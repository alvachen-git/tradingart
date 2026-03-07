/**
 * 爱波塔 Mobile API — 所有接口定义
 */
import { request, uploadFile } from '../utils/request'

// ── Auth ──────────────────────────────────────────────────

export const authApi = {
  login: (account: string, password: string) =>
    request<{ token: string; username: string }>('POST', '/api/auth/login', { account, password }),

  loginEmail: (email: string, code: string) =>
    request<{ token: string; username: string }>('POST', '/api/auth/login/email', { email, code }),

  sendCode: (email: string) =>
    request<{ message: string }>('POST', '/api/auth/send-code', { email }),

  logout: () =>
    request<{ message: string }>('POST', '/api/auth/logout'),

  verify: () =>
    request<{ valid: boolean; username: string }>('GET', '/api/auth/verify'),
}

// ── Chat ──────────────────────────────────────────────────

export interface ChatMessage {
  role: 'user' | 'assistant'
  content: string
}

export const chatApi = {
  submit: (prompt: string, history: ChatMessage[] = []) =>
    request<{ task_id: string }>('POST', '/api/chat/submit', { prompt, history }),

  status: (taskId: string) =>
    request<{
      status: 'pending' | 'processing' | 'success' | 'error'
      progress: string
      result: any
      error: string | null
    }>('GET', `/api/chat/status/${taskId}`),
}

// ── Intel ────────────────────────────────────────────────

export interface ReportItem {
  id: number
  title: string
  channel_name: string
  channel_code: string
  summary: string
  published_at: string
}

export const intelApi = {
  reports: (params?: { channel_code?: string; page?: number; page_size?: number }) => {
    const query = new URLSearchParams()
    if (params?.channel_code) query.set('channel_code', params.channel_code)
    if (params?.page) query.set('page', String(params.page))
    if (params?.page_size) query.set('page_size', String(params.page_size))
    const qs = query.toString()
    return request<{ items: ReportItem[]; page: number; has_more: boolean }>(
      'GET', `/api/intel/reports${qs ? '?' + qs : ''}`
    )
  },

  detail: (id: number) =>
    request<{ id: number; title: string; content: string; channel_name: string; published_at: string }>(
      'GET', `/api/intel/report/${id}`
    ),

  subscribe: (channel_code: string) =>
    request<{ message: string }>('POST', '/api/intel/subscribe', { channel_code }),
}

// ── Market ───────────────────────────────────────────────

export const marketApi = {
  snapshot: () =>
    request<{ data: any }>('GET', '/api/market/snapshot'),
}

// ── Portfolio ─────────────────────────────────────────────

export const portfolioApi = {
  upload: (filePath: string) =>
    uploadFile<{ task_id: string; recognized_count: number; message: string }>(
      '/api/portfolio/upload', filePath
    ),

  status: (taskId: string) =>
    request<{
      status: 'pending' | 'processing' | 'success' | 'error'
      progress: string
      result: any
      error: string | null
    }>('GET', `/api/portfolio/status/${taskId}`),

  result: () =>
    request<{ has_data: boolean; snapshot: any }>('GET', '/api/portfolio/result'),
}

// ── User ─────────────────────────────────────────────────

export interface UserProfile {
  username: string
  email: string
  level: number
  risk_preference: string
  focus_assets: string
  subscriptions: Array<{
    channel_name: string
    channel_code: string
    expires_at: string
    is_active: boolean
  }>
}

export const userApi = {
  profile: () => request<UserProfile>('GET', '/api/user/profile'),
}
