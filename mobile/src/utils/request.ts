/**
 * HTTP request helpers for uni-app mobile client.
 */

const ENV_API_BASE = import.meta.env.VITE_API_BASE || ''
const ENV_MODE = import.meta.env.MODE || ''

function normalizeBase(base: string): string {
  return String(base || '').trim().replace(/\/+$/, '')
}

const DEV_FALLBACK_API_BASE = 'http://localhost:8001'
const PROD_FALLBACK_API_BASE = 'https://api.aiprota.com'
const DEFAULT_API_BASE = ENV_MODE === 'development' ? DEV_FALLBACK_API_BASE : PROD_FALLBACK_API_BASE

export const API_BASE = normalizeBase(ENV_API_BASE) || DEFAULT_API_BASE

function getToken(): string {
  return String(uni.getStorageSync('token') || '').trim()
}

let _unauthorizedRedirecting = false

function shouldForceLogoutFor401(detail: string): boolean {
  const msg = String(detail || '').trim()
  if (!getToken()) return true
  if (!msg) return true
  return /token\s*(无效|已过期|格式错误)|invalid\s*token|expired\s*token/i.test(msg)
}

function handleUnauthorized() {
  if (_unauthorizedRedirecting) return
  _unauthorizedRedirecting = true
  uni.removeStorageSync('token')
  uni.removeStorageSync('username')
  setTimeout(() => {
    uni.reLaunch({ url: '/pages/login/index' })
    _unauthorizedRedirecting = false
  }, 60)
}

export function request<T = any>(
  method: 'GET' | 'POST' | 'PUT' | 'DELETE',
  url: string,
  data?: any,
): Promise<T> {
  return new Promise((resolve, reject) => {
    uni.request({
      url: API_BASE + url,
      method,
      data,
      header: {
        'Content-Type': 'application/json',
        Authorization: `Bearer ${getToken()}`,
      },
      success(res) {
        if (res.statusCode >= 200 && res.statusCode < 300) {
          resolve(res.data as T)
        } else {
          const detail = (res.data as any)?.detail || '请求失败'
          if (res.statusCode === 401 && shouldForceLogoutFor401(detail)) {
            handleUnauthorized()
          }
          reject(new Error(detail))
        }
      },
      fail(err) {
        reject(new Error(err.errMsg || '网络错误，请检查连接'))
      },
    })
  })
}

export function uploadFile<T = any>(url: string, filePath: string): Promise<T> {
  return new Promise((resolve, reject) => {
    uni.uploadFile({
      url: API_BASE + url,
      filePath,
      name: 'file',
      header: {
        Authorization: `Bearer ${getToken()}`,
      },
      success(res) {
        if (res.statusCode >= 200 && res.statusCode < 300) {
          try {
            resolve(JSON.parse(res.data) as T)
          } catch {
            resolve(res.data as any)
          }
        } else {
          try {
            const d = JSON.parse(res.data)
            if (res.statusCode === 401 && shouldForceLogoutFor401(d?.detail || '')) {
              handleUnauthorized()
            }
            reject(new Error(d?.detail || '上传失败'))
          } catch {
            if (res.statusCode === 401) {
              handleUnauthorized()
            }
            reject(new Error('上传失败'))
          }
        }
      },
      fail(err) {
        reject(new Error(err.errMsg || '上传失败'))
      },
    })
  })
}
