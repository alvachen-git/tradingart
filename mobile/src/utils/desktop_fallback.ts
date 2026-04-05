/**
 * H5 同域分流兜底：用于首期未覆盖页面和运行时故障回退到桌面站。
 */

const ENV_DESKTOP_BASE = String((import.meta as any)?.env?.VITE_DESKTOP_FALLBACK_URL || '').trim()
const DEFAULT_DESKTOP_BASE = '/?force_desktop=1'

const PHASE1_SUPPORTED_PATHS = new Set<string>([
  '/',
  '/pages/login/index',
  '/pages/policy/terms',
  '/pages/policy/privacy',
  '/pages/index/index',
  '/pages/intel/index',
  '/pages/intel/detail',
  '/pages/market/index',
  '/pages/market/detail',
  '/pages/market/broker',
  '/pages/profile/index',
])

const UNSUPPORTED_PATH_FEATURE: Record<string, string> = {
  '/pages/kline/index': 'kline_game',
  '/pages/recharge/index': 'recharge_center',
  '/pages/health/index': 'portfolio_health',
}

function isH5Runtime(): boolean {
  return typeof window !== 'undefined'
}

function forceMobilePinned(): boolean {
  if (!isH5Runtime()) return false
  const qs = new URLSearchParams(window.location.search || '')
  return qs.get('force_mobile') === '1'
}

function getDesktopBaseUrl(): string {
  const raw = ENV_DESKTOP_BASE || DEFAULT_DESKTOP_BASE
  return raw || DEFAULT_DESKTOP_BASE
}

function buildDesktopUrl(reason: string, feature = '', srcPath = ''): string {
  if (!isH5Runtime()) return getDesktopBaseUrl()
  const url = new URL(getDesktopBaseUrl(), window.location.origin)
  url.searchParams.set('force_desktop', '1')
  url.searchParams.set('from_mobile_h5', '1')
  if (reason) url.searchParams.set('reason', reason)
  if (feature) url.searchParams.set('feature', feature)
  if (srcPath) url.searchParams.set('src_path', srcPath)
  return url.toString()
}

export function redirectToDesktop(reason: string, feature = '', srcPath = ''): boolean {
  if (!isH5Runtime() || forceMobilePinned()) return false
  const target = buildDesktopUrl(reason, feature, srcPath)
  try {
    window.location.replace(target)
    return true
  } catch {
    window.location.href = target
    return true
  }
}

export function currentH5Path(): string {
  if (!isH5Runtime()) return ''
  const pathname = String(window.location.pathname || '').trim()
  if (!pathname) return '/'
  return pathname.replace(/\/+$/, '') || '/'
}

export function enforcePhase1ScopeFallback() {
  if (!isH5Runtime()) return
  const path = currentH5Path()
  if (PHASE1_SUPPORTED_PATHS.has(path)) return
  const feature = UNSUPPORTED_PATH_FEATURE[path] || 'unsupported_page'
  redirectToDesktop('phase1_scope', feature, path)
}

export function redirectUnsupportedFeature(feature: string) {
  if (!isH5Runtime()) return
  const path = currentH5Path()
  redirectToDesktop('unsupported_feature', feature, path)
}

