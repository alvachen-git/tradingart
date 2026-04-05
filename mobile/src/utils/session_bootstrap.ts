import { authApi } from '../api/index'
import { redirectToDesktop } from './desktop_fallback'

type AuthStoreLike = {
  isLoggedIn: boolean
  setAuth: (token: string, username: string) => void
  clearAuth: () => void
}

interface BootstrapResponse {
  logged_in: boolean
  username?: string
  token?: string
  expire_at?: string
  reason?: string
}

const ENV_ENABLE_BOOTSTRAP = String((import.meta as any)?.env?.VITE_ENABLE_AUTH_BOOTSTRAP || '1').trim()
const ENV_BOOTSTRAP_PATH = String((import.meta as any)?.env?.VITE_H5_BOOTSTRAP_PATH || '').trim()
const ENV_ENABLE_RUNTIME_FALLBACK = String((import.meta as any)?.env?.VITE_ENABLE_H5_RUNTIME_FALLBACK || '1').trim()

let _runtimeFallbackBound = false

function isH5Runtime() {
  return typeof window !== 'undefined'
}

function boolFromEnv(raw: string, defaultValue: boolean): boolean {
  const text = String(raw || '').trim().toLowerCase()
  if (!text) return defaultValue
  return ['1', 'true', 'yes', 'on'].includes(text)
}

function shouldEnableBootstrap() {
  return isH5Runtime() && boolFromEnv(ENV_ENABLE_BOOTSTRAP, true)
}

function shouldEnableRuntimeFallback() {
  return isH5Runtime() && boolFromEnv(ENV_ENABLE_RUNTIME_FALLBACK, true)
}

function getBootstrapUrl() {
  if (!isH5Runtime()) return '/api/auth/session/bootstrap'
  const path = ENV_BOOTSTRAP_PATH || '/api/auth/session/bootstrap'
  return new URL(path, window.location.origin).toString()
}

function shouldIgnoreRuntimeError(errorText: string) {
  const t = String(errorText || '').toLowerCase()
  if (!t) return true
  return t.includes('resizeobserver loop limit exceeded')
}

function isCriticalRuntimeError(errorText: string) {
  const t = String(errorText || '').toLowerCase()
  if (!t) return false
  return (
    t.includes('loading chunk') ||
    t.includes('dynamically imported module') ||
    t.includes('failed to fetch') ||
    t.includes('script error')
  )
}

function requestBootstrap(): Promise<{ statusCode: number; data: BootstrapResponse | null }> {
  const url = getBootstrapUrl()
  return new Promise((resolve, reject) => {
    uni.request({
      url,
      method: 'GET',
      timeout: 3000,
      header: {
        'X-Mobile-H5': '1',
      },
      success(res) {
        resolve({
          statusCode: Number(res.statusCode || 0),
          data: (res.data as BootstrapResponse) || null,
        })
      },
      fail(err) {
        reject(new Error(err?.errMsg || 'bootstrap request failed'))
      },
    })
  })
}

export async function bootstrapAuthSession(authStore: AuthStoreLike) {
  if (!shouldEnableBootstrap()) return
  if (authStore.isLoggedIn) return

  try {
    const { statusCode, data } = await requestBootstrap()
    if (statusCode >= 500) {
      redirectToDesktop('bootstrap_http_5xx')
      return
    }
    if (statusCode < 200 || statusCode >= 300 || !data?.logged_in) {
      return
    }
    const token = String(data.token || '').trim()
    const username = String(data.username || '').trim()
    if (!token || !username) return

    authStore.setAuth(token, username)

    try {
      await authApi.verify()
    } catch {
      authStore.clearAuth()
    }
  } catch (e) {
    console.warn('[mobile-h5] bootstrap failed', e)
  }
}

export function setupH5RuntimeFallback() {
  if (!shouldEnableRuntimeFallback() || _runtimeFallbackBound) return
  _runtimeFallbackBound = true

  window.addEventListener('error', (ev: ErrorEvent) => {
    const text = String(ev?.message || '')
    if (shouldIgnoreRuntimeError(text)) return
    if (isCriticalRuntimeError(text)) {
      redirectToDesktop('runtime_js_error')
    }
  })

  window.addEventListener('unhandledrejection', (ev: PromiseRejectionEvent) => {
    const reasonText = String((ev?.reason as any)?.message || ev?.reason || '')
    if (shouldIgnoreRuntimeError(reasonText)) return
    if (isCriticalRuntimeError(reasonText)) {
      redirectToDesktop('runtime_rejection')
    }
  })
}
