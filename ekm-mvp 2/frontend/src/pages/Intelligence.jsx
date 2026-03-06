import { useState, useEffect, useCallback } from 'react'
import { getAnalyticsStats, getHealthReport, getRiskReport, getOnboardingPath, searchPeople, getPersonProfile, getConfig } from '../api'
import { Spinner, SourceBadge } from '../components/UI'

// ── Helpers ──────────────────────────────────────────────────────────────────
const RISK_COLOR = {
  critical: 'text-red-600 bg-red-50 border-red-200',
  high:     'text-orange-600 bg-orange-50 border-orange-200',
  medium:   'text-yellow-600 bg-yellow-50 border-yellow-200',
  low:      'text-green-600 bg-green-50 border-green-200',
}
const RISK_ICON = { critical: '🔴', high: '🟠', medium: '🟡', low: '🟢' }
const HEALTH_COLOR = { good: 'text-green-600', warning: 'text-yellow-600', poor: 'text-red-600' }
const HEALTH_ICON  = { good: '✅', warning: '⚠️', poor: '❌' }

// Teams deep link — opens a chat with the person
function teamsLink(name, domain) {
  // Convert "Gandhi, Mihir [TECH]" → "mihir.gandhi@citi.com" (best effort)
  const clean = name
    .replace(/\[TECH.*?\]/gi, '')
    .replace(/\(.*?\)/g, '')
    .trim()

  // Try "Lastname, Firstname" format
  const commaMatch = clean.match(/^([^,]+),\s*(.+)$/)
  if (commaMatch) {
    const last  = commaMatch[1].trim().toLowerCase().replace(/\s+/g, '.')
    const first = commaMatch[2].trim().toLowerCase().split(/\s+/)[0]
    return `https://teams.microsoft.com/l/chat/0/0?users=${first}.${last}@${domain}`
  }

  // Fallback: use full name as-is
  const email = clean.toLowerCase().replace(/\s+/g, '.') + '@' + domain
  return `https://teams.microsoft.com/l/chat/0/0?users=${email}`
}

function TeamsButton({ name, domain }) {
  if (!name || !domain) return null
  return (
    <a
      href={teamsLink(name, domain)}
      target="_blank"
      rel="noreferrer"
      onClick={e => e.stopPropagation()}
      className="inline-flex items-center gap-1 text-xs bg-blue-50 text-blue-700 border border-blue-200 px-2 py-0.5 rounded-full hover:bg-blue-100 transition-colors"
      title={`Chat with ${name} on Teams`}
    >
      💬 Teams
    </a>
  )
}

function StatCard({ label, value, sub, color = 'text-navy-800' }) {
  return (
    <div className="card p-4 text-center">
      <div className={`text-3xl font-bold ${color}`}>{value}</div>
      <div className="text-sm font-medium text-gray-700 mt-1">{label}</div>
      {sub && <div className="text-xs text-gray-400 mt-0.5">{sub}</div>}
    </div>
  )
}

function SectionTitle({ children, sub }) {
  return (
    <div className="mb-4">
      <h2 className="text-lg font-bold text-gray-900">{children}</h2>
      {sub && <p className="text-sm text-gray-500">{sub}</p>}
    </div>
  )
}

// ── Analytics Tab ─────────────────────────────────────────────────────────────
function AnalyticsTab() {
  const [data, setData]     = useState(null)
  const [loading, setLoading] = useState(true)
  const [days, setDays]     = useState(30)

  useEffect(() => {
    setLoading(true)
    getAnalyticsStats(days).then(r => { setData(r.data); setLoading(false) })
      .catch(() => setLoading(false))
  }, [days])

  if (loading) return <div className="flex justify-center py-12"><Spinner /></div>
  if (!data)   return <div className="text-gray-500 text-sm">No analytics data yet. Start searching!</div>

  const maxVol = Math.max(...(data.daily_volume?.map(d => d.count) || [1]), 1)

  return (
    <div className="space-y-6">
      {/* Time range */}
      <div className="flex gap-2">
        {[7, 30, 90].map(d => (
          <button key={d} onClick={() => setDays(d)}
            className={`px-3 py-1 text-sm rounded-full border transition-colors ${
              days === d ? 'bg-teal-600 text-white border-teal-600' : 'bg-white text-gray-600 border-gray-200 hover:border-teal-400'
            }`}>
            {d}d
          </button>
        ))}
      </div>

      {/* Summary stats */}
      <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
        <StatCard label="Total Searches"   value={data.total_searches?.toLocaleString()} color="text-teal-600" />
        <StatCard label="Unique Queries"   value={data.unique_queries?.toLocaleString()} color="text-blue-600" />
        <StatCard label="Zero Results"     value={data.zero_result_count} sub={`${data.zero_result_rate}% of searches`} color="text-red-500" />
        <StatCard label="Sources Used"     value={data.source_filter_usage?.length} color="text-purple-600" />
      </div>

      {/* Daily volume chart */}
      {data.daily_volume?.length > 0 && (
        <div className="card p-4">
          <h3 className="text-sm font-semibold text-gray-700 mb-3">Daily Search Volume</h3>
          <div className="flex items-end gap-1 h-24">
            {data.daily_volume.slice(-30).map(d => (
              <div key={d.date} className="flex-1 flex flex-col items-center gap-1 group relative">
                <div
                  className="w-full bg-teal-400 rounded-t hover:bg-teal-600 transition-colors cursor-pointer"
                  style={{ height: `${Math.max(4, (d.count / maxVol) * 88)}px` }}
                />
                <div className="absolute bottom-full mb-1 hidden group-hover:block bg-gray-800 text-white text-xs rounded px-2 py-1 whitespace-nowrap z-10">
                  {d.date}: {d.count} searches
                </div>
              </div>
            ))}
          </div>
        </div>
      )}

      <div className="grid md:grid-cols-2 gap-4">
        {/* Top queries */}
        <div className="card p-4">
          <h3 className="text-sm font-semibold text-gray-700 mb-3">🔥 Top Queries</h3>
          {data.top_queries?.length === 0
            ? <p className="text-xs text-gray-400">No searches yet</p>
            : data.top_queries?.map((q, i) => (
              <div key={q.query} className="flex items-center gap-2 py-1.5 border-b border-gray-50 last:border-0">
                <span className="text-xs text-gray-400 w-4">{i + 1}</span>
                <span className="text-sm text-gray-800 flex-1 truncate">{q.query}</span>
                <span className="text-xs font-medium text-teal-600 bg-teal-50 px-2 py-0.5 rounded-full">{q.count}</span>
              </div>
            ))
          }
        </div>

        {/* Zero result queries */}
        <div className="card p-4">
          <h3 className="text-sm font-semibold text-gray-700 mb-3">⚠️ Zero Result Queries</h3>
          <p className="text-xs text-gray-400 mb-2">Knowledge gaps — these searches found nothing</p>
          {data.zero_result_queries?.length === 0
            ? <p className="text-xs text-green-600">✅ All searches returned results!</p>
            : data.zero_result_queries?.map((q, i) => (
              <div key={q.query} className="flex items-center gap-2 py-1.5 border-b border-gray-50 last:border-0">
                <span className="text-xs text-red-400 w-4">{i + 1}</span>
                <span className="text-sm text-gray-800 flex-1 truncate">{q.query}</span>
                <span className="text-xs font-medium text-red-500 bg-red-50 px-2 py-0.5 rounded-full">{q.count}x</span>
              </div>
            ))
          }
        </div>
      </div>

      {/* Source filter usage */}
      {data.source_filter_usage?.length > 0 && (
        <div className="card p-4">
          <h3 className="text-sm font-semibold text-gray-700 mb-3">📊 Search by Source</h3>
          <div className="flex flex-wrap gap-2">
            {data.source_filter_usage.map(s => (
              <div key={s.source} className="flex items-center gap-2 bg-gray-50 px-3 py-2 rounded-lg">
                <SourceBadge type={s.source === 'all' ? null : s.source} />
                <span className="text-sm font-medium text-gray-700">{s.count} searches</span>
              </div>
            ))}
          </div>
        </div>
      )}
    </div>
  )
}

// ── Health Tab ────────────────────────────────────────────────────────────────
function HealthTab() {
  const [data, setData]     = useState(null)
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    getHealthReport().then(r => { setData(r.data); setLoading(false) })
      .catch(() => setLoading(false))
  }, [])

  if (loading) return <div className="flex justify-center py-12"><Spinner /></div>
  if (!data)   return <div className="text-gray-500 text-sm">Could not load health report.</div>

  return (
    <div className="space-y-6">
      {/* Summary */}
      <div className="grid grid-cols-2 md:grid-cols-3 gap-3">
        <StatCard label="Total Documents" value={data.total_docs?.toLocaleString()} color="text-teal-600" />
        <StatCard label="Jira–Confluence Links" value={`${data.audit?.linked_pct}%`}
          sub={`${data.audit?.linked_count} of ${data.audit?.jira_total} tickets`}
          color={data.audit?.linked_pct > 50 ? 'text-green-600' : 'text-orange-500'} />
        <StatCard label="Stale Docs (180d+)" value={data.stale_docs?.length}
          color={data.stale_docs?.length > 20 ? 'text-red-500' : 'text-yellow-600'} />
      </div>

      {/* Freshness per source */}
      <div className="card p-4">
        <SectionTitle sub="% of documents updated within each time window">Content Freshness by Source</SectionTitle>
        <div className="space-y-3">
          {data.freshness?.map(f => (
            <div key={f.source} className="space-y-1">
              <div className="flex items-center justify-between">
                <div className="flex items-center gap-2">
                  <SourceBadge type={f.source} />
                  <span className="text-sm text-gray-700">{f.total.toLocaleString()} docs</span>
                </div>
                <span className={`text-xs font-medium ${HEALTH_COLOR[f.health]}`}>
                  {HEALTH_ICON[f.health]} {f.health}
                </span>
              </div>
              <div className="flex gap-2 text-xs text-gray-500">
                <span className="text-green-600 font-medium">{f.fresh_30d_pct}% &lt;30d</span>
                <span>·</span>
                <span className="text-blue-600 font-medium">{f.fresh_90d_pct}% &lt;90d</span>
                <span>·</span>
                <span>{f.fresh_365d_pct}% &lt;1yr</span>
              </div>
              <div className="w-full bg-gray-100 rounded-full h-2">
                <div className="bg-teal-500 h-2 rounded-full" style={{ width: `${f.fresh_90d_pct}%` }} />
              </div>
            </div>
          ))}
        </div>
      </div>

      {/* Knowledge audit */}
      <div className="card p-4">
        <SectionTitle sub="How well are Jira tickets documented in Confluence?">Knowledge Audit — Jira ↔ Confluence</SectionTitle>
        <div className="flex items-center gap-4">
          <div className="flex-1">
            <div className="w-full bg-gray-100 rounded-full h-4">
              <div className="bg-teal-500 h-4 rounded-full transition-all"
                style={{ width: `${data.audit?.linked_pct || 0}%` }} />
            </div>
            <div className="flex justify-between text-xs text-gray-500 mt-1">
              <span className="text-teal-600 font-medium">{data.audit?.linked_count} linked</span>
              <span className="text-red-400">{data.audit?.unlinked_count} unlinked</span>
            </div>
          </div>
          <div className={`text-2xl font-bold ${data.audit?.linked_pct > 50 ? 'text-green-600' : 'text-orange-500'}`}>
            {data.audit?.linked_pct}%
          </div>
        </div>
      </div>

      {/* Stale docs */}
      {data.stale_docs?.length > 0 && (
        <div className="card p-4">
          <SectionTitle sub="Documents not updated in 180+ days">Stale Content</SectionTitle>
          <div className="space-y-2">
            {data.stale_docs.slice(0, 10).map((doc, i) => (
              <div key={i} className="flex items-center gap-3 py-1.5 border-b border-gray-50 last:border-0">
                <SourceBadge type={doc.source_type} />
                <a href={doc.url} target="_blank" rel="noreferrer"
                  className="text-sm text-blue-600 hover:underline flex-1 truncate">{doc.title}</a>
                <span className="text-xs text-red-400 shrink-0">{doc.days_old}d old</span>
              </div>
            ))}
          </div>
        </div>
      )}

      {/* Never searched */}
      {data.never_searched?.length > 0 && (
        <div className="card p-4">
          <SectionTitle sub="Indexed content that no one has ever searched for">Untouched Knowledge</SectionTitle>
          <div className="space-y-2">
            {data.never_searched.slice(0, 10).map((doc, i) => (
              <div key={i} className="flex items-center gap-3 py-1.5 border-b border-gray-50 last:border-0">
                <SourceBadge type={doc.source_type} />
                <a href={doc.url} target="_blank" rel="noreferrer"
                  className="text-sm text-blue-600 hover:underline flex-1 truncate">{doc.title}</a>
                <span className="text-xs text-gray-400 shrink-0">{doc.days_ingested}d ago</span>
              </div>
            ))}
          </div>
        </div>
      )}
    </div>
  )
}

// ── Risk Tab ──────────────────────────────────────────────────────────────────
function RiskTab({ teamsDomain }) {
  const [data, setData]       = useState(null)
  const [loading, setLoading] = useState(true)
  const [filter, setFilter]   = useState('all')
  const [expanded, setExpanded] = useState({})

  useEffect(() => {
    getRiskReport().then(r => { setData(r.data); setLoading(false) })
      .catch(() => setLoading(false))
  }, [])

  const toggleExpand = (topic) =>
    setExpanded(prev => ({ ...prev, [topic]: !prev[topic] }))

  if (loading) return <div className="flex justify-center py-12"><Spinner /></div>
  if (!data)   return <div className="text-gray-500 text-sm">Could not load risk report.</div>

  const s = data.summary || {}
  const topics = (data.topics || []).filter(t =>
    filter === 'all' || t.risk_level === filter
  )

  return (
    <div className="space-y-6">
      {/* Summary cards */}
      <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
        <StatCard label="Critical Topics"  value={s.critical_topics}  color="text-red-600" />
        <StatCard label="High Risk Topics" value={s.high_risk_topics} color="text-orange-500" />
        <StatCard label="Vendor Contributors" value={s.vendor_contributors}
          sub={`${s.vendor_pct}% of all contributors`} color="text-purple-600" />
        <StatCard label="Internal Contributors" value={s.internal_contributors} color="text-green-600" />
      </div>

      {/* Vendor dependency banner */}
      {s.vendor_pct > 40 && (
        <div className="bg-red-50 border border-red-200 rounded-lg p-4">
          <div className="flex items-start gap-3">
            <span className="text-2xl">🚨</span>
            <div>
              <h3 className="font-semibold text-red-800">High Vendor Dependency Detected</h3>
              <p className="text-sm text-red-700 mt-1">
                {s.vendor_pct}% of contributors are external vendors [TECH NE].
                Knowledge concentration in vendor resources poses a business continuity risk.
                Consider knowledge transfer programs and internal documentation drives.
              </p>
            </div>
          </div>
        </div>
      )}

      {/* Filter */}
      <div className="flex gap-2 flex-wrap">
        {['all', 'critical', 'high', 'medium', 'low'].map(f => (
          <button key={f} onClick={() => setFilter(f)}
            className={`px-3 py-1 text-xs rounded-full border capitalize transition-colors ${
              filter === f ? 'bg-navy-700 text-white border-navy-700' : 'bg-white text-gray-600 border-gray-200 hover:border-gray-400'
            }`}>
            {f === 'all' ? 'All Topics' : `${RISK_ICON[f]} ${f}`}
          </button>
        ))}
      </div>

      {/* Topics */}
      <div className="space-y-3">
        {topics.length === 0 && <p className="text-sm text-gray-400">No topics match this filter.</p>}
        {topics.map(topic => (
          <div key={topic.topic} className={`card border ${RISK_COLOR[topic.risk_level]}`}>
            {/* Header — always visible */}
            <div className="p-4 cursor-pointer" onClick={() => toggleExpand(topic.topic)}>
              <div className="flex items-start justify-between gap-3 mb-3">
                <div>
                  <div className="flex items-center gap-2">
                    <span className="font-semibold text-gray-900 capitalize">{topic.topic}</span>
                    <span className={`text-xs px-2 py-0.5 rounded-full border font-medium capitalize ${RISK_COLOR[topic.risk_level]}`}>
                      {RISK_ICON[topic.risk_level]} {topic.risk_level}
                    </span>
                  </div>
                  <div className="text-xs text-gray-500 mt-0.5">
                    {topic.unique_people} contributor{topic.unique_people !== 1 ? 's' : ''} · {topic.total_docs} docs
                    · <span className="text-blue-500">{expanded[topic.topic] ? '▲ collapse' : '▼ expand'}</span>
                  </div>
                </div>
                <div className="text-right shrink-0">
                  <div className="text-sm font-bold text-purple-700">{topic.vendor_pct}% vendor</div>
                  <div className="text-xs text-green-700">{topic.internal_pct}% internal</div>
                </div>
              </div>

              {/* Vendor/internal bar */}
              <div className="w-full bg-green-100 rounded-full h-2 mb-3">
                <div className="bg-purple-500 h-2 rounded-full" style={{ width: `${topic.vendor_pct}%` }} />
              </div>

              {/* Contributors with Teams button */}
              <div className="flex flex-wrap gap-2">
                {topic.top_contributors.map(c => (
                  <div key={c.name} className={`flex items-center gap-1.5 text-xs px-2 py-1 rounded-full border ${
                    c.type === 'vendor'   ? 'bg-purple-50 border-purple-200 text-purple-800' :
                    c.type === 'internal' ? 'bg-green-50 border-green-200 text-green-800' :
                                            'bg-gray-50 border-gray-200 text-gray-600'
                  }`}>
                    <span>{c.type === 'vendor' ? '🔵' : c.type === 'internal' ? '🟢' : '⚪'}</span>
                    <span className="font-medium">{c.name}</span>
                    <span className="text-gray-400 mr-1">{c.count}</span>
                    <TeamsButton name={c.name} domain={teamsDomain} />
                  </div>
                ))}
              </div>
            </div>

            {/* Expanded — source documents */}
            {expanded[topic.topic] && topic.top_docs?.length > 0 && (
              <div className="border-t border-gray-100 px-4 py-3 bg-gray-50 rounded-b-lg">
                <p className="text-xs font-semibold text-gray-500 mb-2 uppercase tracking-wide">Source Documents</p>
                <div className="space-y-2">
                  {topic.top_docs.map((doc, i) => (
                    <div key={i} className="flex items-center gap-2">
                      <SourceBadge type={doc.source_type} />
                      {doc.url ? (
                        <a href={doc.url} target="_blank" rel="noreferrer"
                          className="text-sm text-blue-600 hover:underline flex-1 truncate">
                          {doc.title}
                        </a>
                      ) : (
                        <span className="text-sm text-gray-600 flex-1 truncate">{doc.title}</span>
                      )}
                      {doc.updated_at && (
                        <span className="text-xs text-gray-400 shrink-0">
                          {doc.updated_at.slice(0, 10)}
                        </span>
                      )}
                    </div>
                  ))}
                </div>
              </div>
            )}
            {expanded[topic.topic] && (!topic.top_docs || topic.top_docs.length === 0) && (
              <div className="border-t border-gray-100 px-4 py-3 bg-gray-50 rounded-b-lg text-xs text-gray-400">
                No document links available for this topic.
              </div>
            )}
          </div>
        ))}
      </div>
    </div>
  )
}

// ── People Tab ────────────────────────────────────────────────────────────────
function PeopleTab({ teamsDomain }) {
  const [query, setQuery]     = useState('')
  const [results, setResults] = useState(null)
  const [profile, setProfile] = useState(null)
  const [loading, setLoading] = useState(false)

  const handleSearch = async (e) => {
    e.preventDefault()
    if (!query.trim()) return
    setLoading(true)
    setProfile(null)
    try {
      const r = await searchPeople(query)
      setResults(r.data)
    } finally {
      setLoading(false)
    }
  }

  const handlePersonClick = async (name) => {
    setLoading(true)
    try {
      const r = await getPersonProfile(name)
      setProfile(r.data)
    } finally {
      setLoading(false)
    }
  }

  return (
    <div className="space-y-4">
      {/* Search bar */}
      <form onSubmit={handleSearch} className="flex gap-2">
        <input
          value={query}
          onChange={e => setQuery(e.target.value)}
          placeholder="Search by name e.g. Gandhi, Mihir..."
          className="flex-1 border border-gray-200 rounded-lg px-4 py-2 text-sm focus:outline-none focus:border-teal-400"
        />
        <button type="submit" className="btn-primary text-sm px-4">Search</button>
      </form>

      {loading && <div className="flex justify-center py-8"><Spinner /></div>}

      {/* Person profile */}
      {profile && !loading && (
        <div className="card p-5 space-y-4">
          <div className="flex items-start justify-between">
            <div>
              <div className="flex items-center gap-2 flex-wrap">
                <h2 className="text-lg font-bold text-gray-900">{profile.name}</h2>
                <span className={`text-xs px-2 py-0.5 rounded-full border font-medium ${
                  profile.type === 'vendor'   ? 'bg-purple-50 border-purple-200 text-purple-700' :
                  profile.type === 'internal' ? 'bg-green-50 border-green-200 text-green-700' :
                                                 'bg-gray-50 border-gray-200 text-gray-600'
                }`}>
                  {profile.type === 'vendor' ? '🔵 Vendor [TECH NE]' :
                   profile.type === 'internal' ? '🟢 Internal [TECH]' : '⚪ Unknown'}
                </span>
                <TeamsButton name={profile.name} domain={teamsDomain} />
              </div>
              <p className="text-sm text-gray-500 mt-0.5">
                {profile.total_docs} documents · Last active: {profile.last_active || 'unknown'}
              </p>
            </div>
            <button onClick={() => setProfile(null)} className="text-gray-400 hover:text-gray-600 text-sm">✕ Back</button>
          </div>

          {/* Roles */}
          {profile.roles?.length > 0 && (
            <div className="flex flex-wrap gap-2">
              {profile.roles.map(r => (
                <span key={r} className="text-xs bg-teal-50 text-teal-700 border border-teal-200 px-2 py-1 rounded-full">{r}</span>
              ))}
            </div>
          )}

          {/* By source */}
          <div>
            <h3 className="text-sm font-semibold text-gray-700 mb-2">Contributions by Source</h3>
            <div className="flex flex-wrap gap-3">
              {Object.entries(profile.by_source || {}).map(([src, count]) => (
                <div key={src} className="flex items-center gap-1.5 text-sm">
                  <SourceBadge type={src} />
                  <span className="font-medium text-gray-700">{count} docs</span>
                </div>
              ))}
            </div>
          </div>

          {/* Top topics */}
          {profile.top_topics?.length > 0 && (
            <div>
              <h3 className="text-sm font-semibold text-gray-700 mb-2">Top Topics</h3>
              <div className="flex flex-wrap gap-2">
                {profile.top_topics.map(t => (
                  <span key={t.topic} className="text-xs bg-gray-100 text-gray-600 px-2 py-1 rounded-full">
                    {t.topic} ({t.count})
                  </span>
                ))}
              </div>
            </div>
          )}

          {/* Recent docs */}
          {profile.recent_docs?.length > 0 && (
            <div>
              <h3 className="text-sm font-semibold text-gray-700 mb-2">Recent Contributions</h3>
              <div className="space-y-2">
                {profile.recent_docs.map((doc, i) => (
                  <div key={i} className="flex items-center gap-2 py-1.5 border-b border-gray-50 last:border-0">
                    <SourceBadge type={doc.source_type} />
                    {doc.url ? (
                      <a href={doc.url} target="_blank" rel="noreferrer"
                        className="text-sm text-blue-600 hover:underline flex-1 truncate">{doc.title}</a>
                    ) : (
                      <span className="text-sm text-gray-600 flex-1 truncate">{doc.title}</span>
                    )}
                    <span className="text-xs text-gray-400 shrink-0">{doc.updated_at}</span>
                  </div>
                ))}
              </div>
            </div>
          )}
        </div>
      )}

      {/* Search results */}
      {results && !profile && !loading && (
        <div>
          <p className="text-sm text-gray-500 mb-3">
            {results.total} contributor{results.total !== 1 ? 's' : ''} found for "{results.query}"
          </p>
          <div className="grid md:grid-cols-2 gap-3">
            {results.results?.map(person => (
              <div key={person.name}
                onClick={() => handlePersonClick(person.name)}
                className="card p-4 hover:shadow-md cursor-pointer transition-shadow">
                <div className="flex items-start justify-between gap-2 mb-2">
                  <div>
                    <div className="flex items-center gap-2 flex-wrap">
                      <span className="font-medium text-gray-900 text-sm">{person.name}</span>
                      {person.type === 'vendor' && (
                        <span className="text-xs bg-purple-50 text-purple-700 border border-purple-200 px-1.5 py-0.5 rounded-full">🔵 Vendor</span>
                      )}
                      {person.type === 'internal' && (
                        <span className="text-xs bg-green-50 text-green-700 border border-green-200 px-1.5 py-0.5 rounded-full">🟢 Internal</span>
                      )}
                    </div>
                    <p className="text-xs text-gray-400 mt-0.5">Last active: {person.last_active || 'unknown'}</p>
                  </div>
                  <div className="flex items-center gap-2 shrink-0">
                    <span className="text-sm font-bold text-teal-600">{person.doc_count} docs</span>
                    <TeamsButton name={person.name} domain={teamsDomain} />
                  </div>
                </div>
                <div className="flex flex-wrap gap-1">
                  {person.sources?.map(s => <SourceBadge key={s} type={s} />)}
                </div>
              </div>
            ))}
          </div>
        </div>
      )}

      {results?.total === 0 && !loading && (
        <div className="text-center py-8 text-gray-400">No contributors found for "{results.query}"</div>
      )}
    </div>
  )
}

// ── Onboarding Tab ────────────────────────────────────────────────────────────
function OnboardingTab() {
  const [topic, setTopic]     = useState('')
  const [data, setData]       = useState(null)
  const [loading, setLoading] = useState(false)

  const handleSearch = async (e) => {
    e.preventDefault()
    if (!topic.trim()) return
    setLoading(true)
    try {
      const r = await getOnboardingPath(topic)
      setData(r.data)
    } finally {
      setLoading(false)
    }
  }

  const SOURCE_ORDER = { confluence: 1, jira: 2, sharepoint: 3, github: 4 }

  return (
    <div className="space-y-4">
      <div className="bg-blue-50 border border-blue-200 rounded-lg p-4 text-sm text-blue-800">
        <strong>🎓 Onboarding Paths</strong> — Enter a topic, system, or project name to get a curated reading list
        for someone new to that area. EKM surfaces the most relevant docs across all sources in the right order.
      </div>

      <form onSubmit={handleSearch} className="flex gap-2">
        <input
          value={topic}
          onChange={e => setTopic(e.target.value)}
          placeholder="e.g. payments pipeline, BIC ETL, CGME dashboard..."
          className="flex-1 border border-gray-200 rounded-lg px-4 py-2 text-sm focus:outline-none focus:border-teal-400"
        />
        <button type="submit" className="btn-primary text-sm px-4">Generate Path</button>
      </form>

      {loading && <div className="flex justify-center py-8"><Spinner /></div>}

      {data && !loading && (
        <div>
          <p className="text-sm text-gray-500 mb-3">
            {data.total} documents for <strong>"{data.topic}"</strong>
          </p>
          <div className="space-y-3">
            {data.docs?.map((doc, i) => (
              <div key={i} className="card p-4 flex items-start gap-4">
                <div className="flex items-center justify-center w-8 h-8 rounded-full bg-teal-100 text-teal-700 font-bold text-sm shrink-0">
                  {i + 1}
                </div>
                <div className="flex-1 min-w-0">
                  <div className="flex items-center gap-2 mb-1">
                    <SourceBadge type={doc.source_type} />
                    {doc.days_old && (
                      <span className={`text-xs ${doc.days_old > 180 ? 'text-red-400' : 'text-gray-400'}`}>
                        {doc.days_old}d ago
                      </span>
                    )}
                    {doc.relevance > 0 && (
                      <span className="text-xs text-teal-600 ml-auto">score {doc.relevance}</span>
                    )}
                  </div>
                  <a href={doc.url} target="_blank" rel="noreferrer"
                    className="text-sm font-medium text-blue-600 hover:underline line-clamp-1">{doc.title}</a>
                  {doc.author && (
                    <p className="text-xs text-gray-400 mt-0.5">by {doc.author}</p>
                  )}
                </div>
              </div>
            ))}
          </div>
          {data.total === 0 && (
            <div className="text-center py-8 text-gray-400">No documents found for this topic.</div>
          )}
        </div>
      )}
    </div>
  )
}

// ── Main Intelligence Page ────────────────────────────────────────────────────
const TABS = [
  { id: 'analytics',  label: '📊 Analytics' },
  { id: 'risk',       label: '🔴 Risk & Vendors' },
  { id: 'health',     label: '❤️ Health' },
  { id: 'people',     label: '👤 People' },
  { id: 'onboarding', label: '🎓 Onboarding' },
]

export default function Intelligence() {
  const [tab, setTab]             = useState('analytics')
  const [teamsDomain, setTeamsDomain] = useState('citi.com')

  useEffect(() => {
    getConfig().then(r => { if (r.data?.teams_domain) setTeamsDomain(r.data.teams_domain) })
      .catch(() => {})
  }, [])

  return (
    <div className="space-y-4">
      <div>
        <h1 className="text-2xl font-bold text-gray-900">Intelligence</h1>
        <p className="text-gray-500 text-sm mt-0.5">Knowledge analytics, risk signals, and team insights</p>
      </div>

      {/* Tab bar */}
      <div className="flex gap-1 border-b border-gray-200 overflow-x-auto">
        {TABS.map(t => (
          <button key={t.id} onClick={() => setTab(t.id)}
            className={`px-4 py-2 text-sm font-medium whitespace-nowrap border-b-2 transition-colors ${
              tab === t.id
                ? 'border-teal-500 text-teal-700'
                : 'border-transparent text-gray-500 hover:text-gray-700 hover:border-gray-300'
            }`}>
            {t.label}
          </button>
        ))}
      </div>

      {/* Tab content */}
      <div>
        {tab === 'analytics'  && <AnalyticsTab />}
        {tab === 'risk'       && <RiskTab teamsDomain={teamsDomain} />}
        {tab === 'health'     && <HealthTab />}
        {tab === 'people'     && <PeopleTab teamsDomain={teamsDomain} />}
        {tab === 'onboarding' && <OnboardingTab />}
      </div>
    </div>
  )
}
