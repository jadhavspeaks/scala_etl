"""
Intelligence Routes
────────────────────
GET /api/intelligence/health        — content freshness, knowledge audit, untouched docs
GET /api/intelligence/risk          — vendor dependency + concentration risk per topic
GET /api/intelligence/onboarding    — top 10 docs for a topic (onboarding path)
"""

import re
import logging
from datetime import datetime, timezone, timedelta
from collections import defaultdict
from fastapi import APIRouter, Query
from database import get_db

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/intelligence", tags=["intelligence"])

VENDOR_PATTERN   = re.compile(r'\[TECH NE\]', re.IGNORECASE)
INTERNAL_PATTERN = re.compile(r'\[TECH\]', re.IGNORECASE)


def _classify(name: str) -> str:
    if VENDOR_PATTERN.search(name or ""):
        return "vendor"
    if INTERNAL_PATTERN.search(name or ""):
        return "internal"
    return "unknown"


def _days_ago(dt) -> int | None:
    if not dt:
        return None
    try:
        if isinstance(dt, str):
            dt = datetime.fromisoformat(dt.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return (datetime.now(timezone.utc) - dt).days
    except Exception:
        return None


# ── Health ────────────────────────────────────────────────────────────────────

@router.get("/health")
async def get_health():
    """
    Content health report:
    - Freshness per source (% docs updated in last 30/90/365 days)
    - Knowledge audit (Jira tickets with/without linked Confluence pages)
    - Untouched docs (ingested but never appeared in a search, oldest first)
    - Stale docs (not updated in 180+ days)
    """
    db = get_db()

    # ── 1. Fetch all docs (lightweight projection) ────────────────────────────
    cursor = db.documents.find(
        {},
        {"source_type": 1, "source": 1, "title": 1, "url": 1,
         "updated_at": 1, "ingested_at": 1, "entities": 1, "tags": 1}
    )
    all_docs = await cursor.to_list(length=50000)

    now = datetime.now(timezone.utc)
    source_stats: dict[str, dict] = {}
    stale_docs = []
    total_by_source: dict[str, int] = {}

    for doc in all_docs:
        st = doc.get("source_type", "unknown")
        total_by_source[st] = total_by_source.get(st, 0) + 1

        if st not in source_stats:
            source_stats[st] = {"fresh_30": 0, "fresh_90": 0, "fresh_365": 0, "total": 0}
        source_stats[st]["total"] += 1

        age = _days_ago(doc.get("updated_at") or doc.get("ingested_at"))
        if age is not None:
            if age <= 30:
                source_stats[st]["fresh_30"] += 1
            if age <= 90:
                source_stats[st]["fresh_90"] += 1
            if age <= 365:
                source_stats[st]["fresh_365"] += 1
            if age > 180:
                stale_docs.append({
                    "title":       doc.get("title", ""),
                    "source_type": st,
                    "source":      doc.get("source", ""),
                    "url":         doc.get("url", ""),
                    "days_old":    age,
                })

    # Freshness summary
    freshness = []
    for st, stats in source_stats.items():
        t = stats["total"] or 1
        freshness.append({
            "source":       st,
            "total":        stats["total"],
            "fresh_30d_pct":  round(stats["fresh_30"] / t * 100),
            "fresh_90d_pct":  round(stats["fresh_90"] / t * 100),
            "fresh_365d_pct": round(stats["fresh_365"] / t * 100),
            "health": (
                "good"    if stats["fresh_90"] / t > 0.7 else
                "warning" if stats["fresh_90"] / t > 0.4 else
                "poor"
            )
        })

    # ── 2. Knowledge audit: Jira ↔ Confluence link % ─────────────────────────
    jira_docs = [d for d in all_docs if d.get("source_type") == "jira"]
    confluence_titles = {
        d.get("title", "").lower().strip()
        for d in all_docs if d.get("source_type") == "confluence"
    }

    linked = 0
    for jdoc in jira_docs:
        entities = jdoc.get("entities") or {}
        jira_tickets = entities.get("jira_tickets", [])
        title = jdoc.get("title", "").lower()
        # Count as linked if any confluence page title contains the Jira key
        # or if entities from this doc appear in confluence content
        for ctitle in confluence_titles:
            for ticket in jira_tickets:
                if ticket.lower() in ctitle:
                    linked += 1
                    break

    jira_total = len(jira_docs) or 1
    audit = {
        "jira_total":      len(jira_docs),
        "confluence_total": len([d for d in all_docs if d.get("source_type") == "confluence"]),
        "linked_count":    linked,
        "linked_pct":      round(linked / jira_total * 100, 1),
        "unlinked_count":  len(jira_docs) - linked,
        "health": (
            "good"    if linked / jira_total > 0.5 else
            "warning" if linked / jira_total > 0.2 else
            "poor"
        )
    }

    # ── 3. Untouched docs: ingested long ago, stale ───────────────────────────
    stale_docs.sort(key=lambda x: x["days_old"], reverse=True)

    # ── 4. Untouched knowledge via search logs ────────────────────────────────
    # Docs that have never matched any search (approximate — by tag)
    search_cursor = db.search_logs.find({}, {"query": 1})
    search_logs   = await search_cursor.to_list(length=10000)
    searched_terms = set()
    for sl in search_logs:
        for word in re.findall(r'\b[a-z]{3,}\b', sl.get("query", "").lower()):
            searched_terms.add(word)

    never_searched = []
    for doc in all_docs:
        title = doc.get("title", "").lower()
        title_words = set(re.findall(r'\b[a-z]{3,}\b', title))
        if title_words and not title_words.intersection(searched_terms):
            age = _days_ago(doc.get("ingested_at"))
            if age and age > 30:
                never_searched.append({
                    "title":       doc.get("title", ""),
                    "source_type": doc.get("source_type", ""),
                    "url":         doc.get("url", ""),
                    "days_ingested": age,
                })
    never_searched.sort(key=lambda x: x["days_ingested"], reverse=True)

    return {
        "total_docs":    len(all_docs),
        "freshness":     freshness,
        "audit":         audit,
        "stale_docs":    stale_docs[:20],
        "never_searched": never_searched[:20],
        "generated_at":  now.isoformat(),
    }


# ── Risk ──────────────────────────────────────────────────────────────────────

@router.get("/risk")
async def get_risk():
    """
    Risk intelligence:
    - Vendor dependency per topic/project (TECH NE signal)
    - Knowledge concentration (1 person owns a topic)
    - Overall risk summary
    """
    db = get_db()

    cursor = db.documents.find(
        {},
        {"source_type": 1, "source": 1, "author": 1, "metadata": 1,
         "tags": 1, "title": 1, "updated_at": 1}
    )
    all_docs = await cursor.to_list(length=50000)

    # ── Per-topic contributor analysis ────────────────────────────────────────
    # Group by project tag (Jira project key, Confluence space, repo name)
    topic_people: dict[str, dict[str, dict]] = defaultdict(lambda: defaultdict(lambda: {
        "type": "unknown", "count": 0, "roles": set()
    }))

    for doc in all_docs:
        st = doc.get("source_type", "")
        m  = doc.get("metadata") or {}

        # Get topic identifier
        topics = []
        for tag in (doc.get("tags") or []):
            if tag not in ("jira", "confluence", "sharepoint", "github",
                          "commit", "pull_request", "page", "file", "document"):
                topics.append(tag)

        people = []
        if st == "jira":
            for role, field in [("Reporter", "reporter"), ("Assignee", "assignee")]:
                person = m.get(field, "")
                if person:
                    people.append((person, role))
        elif st == "confluence":
            person = doc.get("author", "")
            if person:
                people.append((person, "Author"))
        elif st == "github":
            person = m.get("author_name") or doc.get("author", "")
            ct     = m.get("content_type", "")
            if person:
                people.append((person, "Commit Author" if ct == "commit" else "PR Author"))
        elif st == "sharepoint":
            person = doc.get("author", "")
            if person:
                people.append((person, "Author"))

        for person, role in people:
            person = person.strip()
            if not person:
                continue
            for topic in topics[:3]:  # max 3 tags per doc
                topic_people[topic][person]["type"] = _classify(person)
                topic_people[topic][person]["count"] += 1
                topic_people[topic][person]["roles"].add(role)

    # ── Build risk alerts ─────────────────────────────────────────────────────
    risk_topics = []
    for topic, people in topic_people.items():
        if len(people) == 0:
            continue

        total_contribs = sum(p["count"] for p in people.values())
        vendor_contribs  = sum(p["count"] for p in people.values() if p["type"] == "vendor")
        internal_contribs = sum(p["count"] for p in people.values() if p["type"] == "internal")

        vendor_pct   = round(vendor_contribs / total_contribs * 100) if total_contribs else 0
        internal_pct = round(internal_contribs / total_contribs * 100) if total_contribs else 0
        unique_people = len(people)

        # Risk level
        if vendor_pct >= 80 or (vendor_pct > 50 and unique_people <= 2):
            risk_level = "critical"
        elif vendor_pct >= 50 or (vendor_pct > 30 and unique_people <= 3):
            risk_level = "high"
        elif vendor_pct >= 20:
            risk_level = "medium"
        else:
            risk_level = "low"

        # Concentration risk
        if unique_people == 1:
            concentration = "critical"
        elif unique_people == 2:
            concentration = "high"
        elif unique_people <= 4:
            concentration = "medium"
        else:
            concentration = "low"

        top_people = sorted(
            people.items(),
            key=lambda x: x[1]["count"],
            reverse=True
        )[:5]

        risk_topics.append({
            "topic":          topic,
            "total_docs":     total_contribs,
            "unique_people":  unique_people,
            "vendor_pct":     vendor_pct,
            "internal_pct":   internal_pct,
            "risk_level":     risk_level,
            "concentration":  concentration,
            "top_contributors": [
                {
                    "name":  name,
                    "type":  info["type"],
                    "count": info["count"],
                    "roles": list(info["roles"]),
                }
                for name, info in top_people
            ]
        })

    # Sort by risk — critical first, then by vendor %
    risk_order = {"critical": 0, "high": 1, "medium": 2, "low": 3}
    risk_topics.sort(key=lambda x: (risk_order[x["risk_level"]], -x["vendor_pct"]))

    # ── Summary ───────────────────────────────────────────────────────────────
    critical_count = sum(1 for t in risk_topics if t["risk_level"] == "critical")
    high_count     = sum(1 for t in risk_topics if t["risk_level"] == "high")

    # Overall vendor dependency across all docs
    all_authors = defaultdict(lambda: {"type": "unknown", "count": 0})
    for doc in all_docs:
        m = doc.get("metadata") or {}
        for person in [
            doc.get("author"),
            m.get("reporter"),
            m.get("assignee"),
            m.get("author_name"),
        ]:
            if person and person.strip():
                p = person.strip()
                all_authors[p]["type"]  = _classify(p)
                all_authors[p]["count"] += 1

    total_people   = len(all_authors)
    vendor_people  = sum(1 for a in all_authors.values() if a["type"] == "vendor")
    internal_people = sum(1 for a in all_authors.values() if a["type"] == "internal")

    return {
        "summary": {
            "total_topics":     len(risk_topics),
            "critical_topics":  critical_count,
            "high_risk_topics": high_count,
            "total_contributors": total_people,
            "vendor_contributors": vendor_people,
            "internal_contributors": internal_people,
            "vendor_pct": round(vendor_people / total_people * 100) if total_people else 0,
        },
        "topics": risk_topics[:50],
    }


# ── Onboarding ────────────────────────────────────────────────────────────────

@router.get("/onboarding")
async def get_onboarding_path(
    topic: str = Query(..., min_length=1),
    limit: int = Query(10, ge=1, le=20)
):
    """
    Generate an onboarding reading path for a topic.
    Returns the most relevant, freshest documents across all sources.
    """
    from utils.bm25 import rerank_bm25

    db = get_db()

    # Search across all sources for this topic
    cursor = db.documents.find(
        {"$text": {"$search": topic}},
        {"title": 1, "source_type": 1, "source": 1, "url": 1,
         "content": 1, "author": 1, "updated_at": 1, "tags": 1, "metadata": 1}
    ).limit(200)
    docs = await cursor.to_list(length=200)

    if not docs:
        # Fallback: tag match
        cursor = db.documents.find(
            {"tags": {"$regex": re.escape(topic), "$options": "i"}},
            {"title": 1, "source_type": 1, "source": 1, "url": 1,
             "content": 1, "author": 1, "updated_at": 1, "tags": 1}
        ).limit(100)
        docs = await cursor.to_list(length=100)

    if not docs:
        return {"topic": topic, "docs": [], "total": 0}

    # BM25 re-rank
    ranked = rerank_bm25(topic, docs)

    # Deduplicate by source type — pick best from each source
    seen_sources: dict[str, int] = {}
    path = []
    for doc in ranked:
        st = doc.get("source_type", "")
        seen_sources[st] = seen_sources.get(st, 0) + 1

        age = _days_ago(doc.get("updated_at"))
        path.append({
            "title":       doc.get("title", ""),
            "source_type": st,
            "source":      doc.get("source", ""),
            "url":         doc.get("url", ""),
            "author":      doc.get("author"),
            "updated_at":  doc.get("updated_at").isoformat() if isinstance(doc.get("updated_at"), datetime) else str(doc.get("updated_at", "")),
            "days_old":    age,
            "relevance":   round(doc.get("bm25_score", 0), 2),
            "read_order":  len(path) + 1,
        })
        if len(path) >= limit:
            break

    return {
        "topic":  topic,
        "total":  len(path),
        "docs":   path,
    }
