"""
People Routes
─────────────
GET /api/people/search?q=Gandhi     — search docs by person name
GET /api/people/{name}              — full contribution profile for one person
GET /api/people                     — list all known contributors
"""

import re
import logging
from datetime import datetime, timezone
from fastapi import APIRouter, Query
from database import get_db

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/people", tags=["people"])

VENDOR_PATTERN = re.compile(r'\[TECH NE\]', re.IGNORECASE)
INTERNAL_PATTERN = re.compile(r'\[TECH\]', re.IGNORECASE)


def _classify_person(name: str) -> str:
    if VENDOR_PATTERN.search(name):
        return "vendor"
    if INTERNAL_PATTERN.search(name):
        return "internal"
    return "unknown"


def _format_date(d) -> str | None:
    if not d:
        return None
    try:
        if isinstance(d, str):
            return d[:10]
        if isinstance(d, datetime):
            return d.strftime("%Y-%m-%d")
        return str(d)[:10]
    except Exception:
        return None


async def _get_person_docs(db, name: str) -> list[dict]:
    """Fetch all docs where person appears as author, assignee, reporter, or committer."""
    name_regex = {"$regex": re.escape(name), "$options": "i"}

    cursor = db.documents.find(
        {"$or": [
            {"author": name_regex},
            {"metadata.assignee": name_regex},
            {"metadata.reporter": name_regex},
            {"metadata.author_name": name_regex},
        ]},
        {"title": 1, "source": 1, "source_type": 1, "url": 1,
         "author": 1, "metadata": 1, "updated_at": 1, "ingested_at": 1, "tags": 1}
    ).limit(500)
    return await cursor.to_list(length=500)


def _build_profile(name: str, docs: list[dict]) -> dict:
    """Build a contribution profile from matched documents."""
    by_source = {}
    roles = set()
    last_active = None
    topics = {}

    for doc in docs:
        st = doc.get("source_type", "unknown")
        by_source[st] = by_source.get(st, 0) + 1

        m = doc.get("metadata") or {}
        author = (doc.get("author") or "").lower()
        name_lower = name.lower()

        # Determine role in this doc
        if st == "jira":
            if name_lower in (m.get("reporter") or "").lower():
                roles.add("Jira Reporter")
            if name_lower in (m.get("assignee") or "").lower():
                roles.add("Jira Assignee")
            if m.get("status", "").lower() in ("done", "resolved", "closed"):
                if name_lower in (m.get("assignee") or "").lower():
                    roles.add("Issue Resolver")
        elif st == "confluence":
            roles.add("Confluence Author")
        elif st == "github":
            ct = m.get("content_type", "")
            if ct == "commit":
                roles.add("Commit Author")
            elif ct == "pull_request":
                roles.add("PR Author")
        elif st == "sharepoint":
            roles.add("SharePoint Author")

        # Track last active
        updated = doc.get("updated_at") or doc.get("ingested_at")
        if updated:
            try:
                if isinstance(updated, str):
                    updated = datetime.fromisoformat(updated.replace("Z", "+00:00"))
                if last_active is None or updated > last_active:
                    last_active = updated
            except Exception:
                pass

        # Topic extraction from tags
        for tag in (doc.get("tags") or []):
            if tag not in ("jira", "confluence", "sharepoint", "github",
                          "commit", "pull_request", "page", "file"):
                topics[tag] = topics.get(tag, 0) + 1

    # Top topics
    top_topics = sorted(topics.items(), key=lambda x: x[1], reverse=True)[:8]

    return {
        "name": name,
        "type": _classify_person(name),
        "total_docs": len(docs),
        "by_source": by_source,
        "roles": sorted(roles),
        "top_topics": [{"topic": t, "count": c} for t, c in top_topics],
        "last_active": _format_date(last_active),
        "recent_docs": [
            {
                "title": d.get("title", ""),
                "source_type": d.get("source_type", ""),
                "source": d.get("source", ""),
                "url": d.get("url", ""),
                "updated_at": _format_date(d.get("updated_at")),
            }
            for d in sorted(
                docs,
                key=lambda x: x.get("updated_at") or x.get("ingested_at") or datetime.min,
                reverse=True
            )[:10]
        ]
    }


@router.get("/search")
async def search_people(q: str = Query(..., min_length=1)):
    """Search for contributors by name across all sources."""
    db = get_db()
    name_regex = {"$regex": re.escape(q), "$options": "i"}

    # Find all docs matching the name
    cursor = db.documents.find(
        {"$or": [
            {"author": name_regex},
            {"metadata.assignee": name_regex},
            {"metadata.reporter": name_regex},
            {"metadata.author_name": name_regex},
        ]},
        {"author": 1, "metadata": 1, "source_type": 1, "updated_at": 1}
    ).limit(1000)
    docs = await cursor.to_list(length=1000)

    # Aggregate unique names
    names: dict[str, dict] = {}
    for doc in docs:
        m = doc.get("metadata") or {}
        candidates = [
            doc.get("author"),
            m.get("assignee"),
            m.get("reporter"),
            m.get("author_name"),
        ]
        for cand in candidates:
            if not cand:
                continue
            cand = cand.strip()
            if not cand or q.lower() not in cand.lower():
                continue
            key = cand.lower()
            if key not in names:
                names[key] = {
                    "name": cand,
                    "type": _classify_person(cand),
                    "doc_count": 0,
                    "sources": set(),
                    "last_active": None,
                }
            names[key]["doc_count"] += 1
            names[key]["sources"].add(doc.get("source_type", ""))

            updated = doc.get("updated_at")
            if updated:
                try:
                    if isinstance(updated, str):
                        updated = datetime.fromisoformat(updated.replace("Z", "+00:00"))
                    cur_la = names[key]["last_active"]
                    if cur_la is None or updated > cur_la:
                        names[key]["last_active"] = updated
                except Exception:
                    pass

    results = []
    for p in names.values():
        results.append({
            "name": p["name"],
            "type": p["type"],
            "doc_count": p["doc_count"],
            "sources": list(p["sources"]),
            "last_active": _format_date(p["last_active"]),
        })

    results.sort(key=lambda x: x["doc_count"], reverse=True)
    return {"query": q, "total": len(results), "results": results[:20]}


@router.get("/{name:path}")
async def get_person_profile(name: str):
    """Full contribution profile for a specific person."""
    db = get_db()
    docs = await _get_person_docs(db, name)
    if not docs:
        return {"name": name, "total_docs": 0, "message": "No contributions found"}
    return _build_profile(name, docs)
