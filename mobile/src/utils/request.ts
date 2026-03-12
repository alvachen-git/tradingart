/**
 * HTTP request helpers for uni-app mobile client.
 */

const ENV_API_BASE = (import.meta as any)?.env?.VITE_API_BASE || ''

function normalizeBase(base: string): string {
  return String(base || '').trim().replace(/\/+$/, '')
}

export const API_BASE = normalizeBase(ENV_API_BASE) || 'http://localhost:8001'

function getToken(): string {
  return uni.getStorageSync('token') || ''
}

function handleUnauthorized() {
  uni.removeStorageSync('token')
  uni.removeStorageSync('username')
  uni.reLaunch({ url: '/pages/login/index' })
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
          if (res.statusCode === 401) {
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
            reject(new Error(d?.detail || '上传失败'))
          } catch {
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