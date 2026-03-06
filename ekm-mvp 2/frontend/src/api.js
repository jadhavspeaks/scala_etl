import axios from 'axios'

const api = axios.create({
  baseURL: '/api',
  timeout: 30000,
})

export const searchDocs = (q, sourceType, page = 1) =>
  api.get('/search', { params: { q, source_type: sourceType || undefined, page } })

export const getDashboard = () => api.get('/sources')

export const triggerSync = (sourceType) =>
  api.post('/sync', { source_type: sourceType || null })

export const getSyncLogs = () => api.get('/sync/logs')

export const listDocuments = (sourceType, page = 1) =>
  api.get('/documents', { params: { source_type: sourceType || undefined, page } })

export const getDocument = (id) => api.get(`/documents/${id}`)

export const healthCheck = () => api.get('/health')

// People
export const searchPeople = (q) => api.get('/people/search', { params: { q } })
export const getPersonProfile = (name) => api.get(`/people/${encodeURIComponent(name)}`)

// Analytics
export const getAnalyticsStats = (days = 30) => api.get('/analytics/stats', { params: { days } })
export const getSearchHistory = (limit = 50) => api.get('/analytics/searches', { params: { limit } })

// Intelligence
export const getHealthReport = () => api.get('/intelligence/health')
export const getRiskReport = () => api.get('/intelligence/risk')
export const getOnboardingPath = (topic) => api.get('/intelligence/onboarding', { params: { topic } })

export default api
