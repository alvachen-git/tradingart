import { defineStore } from 'pinia'
import { ref, computed } from 'vue'

export const useAuthStore = defineStore('auth', () => {
  const token = ref('')
  const username = ref('')
  let bootstrapPromise: Promise<void> | null = null

  const isLoggedIn = computed(() => !!token.value && !!username.value)

  function setAuth(t: string, u: string) {
    token.value = t
    username.value = u
    uni.setStorageSync('token', t)
    uni.setStorageSync('username', u)
  }

  function clearAuth() {
    token.value = ''
    username.value = ''
    uni.removeStorageSync('token')
    uni.removeStorageSync('username')
  }

  function restoreFromStorage() {
    const t = uni.getStorageSync('token') || ''
    const u = uni.getStorageSync('username') || ''
    if (t && u) {
      token.value = t
      username.value = u
    }
  }

  function setBootstrapPromise(promise: Promise<void>) {
    bootstrapPromise = promise
  }

  async function waitForBootstrap() {
    if (!bootstrapPromise) return
    try {
      await bootstrapPromise
    } catch {
      // bootstrap 失败时由调用方按未登录处理
    } finally {
      bootstrapPromise = null
    }
  }

  return {
    token,
    username,
    isLoggedIn,
    setAuth,
    clearAuth,
    restoreFromStorage,
    setBootstrapPromise,
    waitForBootstrap,
  }
})
