import { defineStore } from 'pinia'
import { ref, computed } from 'vue'

export const useAuthStore = defineStore('auth', () => {
  const token = ref('')
  const username = ref('')

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

  return { token, username, isLoggedIn, setAuth, clearAuth, restoreFromStorage }
})
