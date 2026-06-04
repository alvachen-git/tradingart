<script setup lang="ts">
import { ref } from 'vue'
import { onShow } from '@dcloudio/uni-app'
import { authApi } from '../../api/index'
import { useAuthStore } from '../../store/auth'

const auth = useAuthStore()
const statusText = ref('自动登录中...')

let initialRedirectDone = false
let loginRedirectDone = false
let verifyStartedForToken = ''

function redirectInitial(url: string) {
  if (initialRedirectDone) return
  initialRedirectDone = true
  uni.reLaunch({ url })
}

function redirectLogin() {
  if (loginRedirectDone) return
  loginRedirectDone = true
  uni.reLaunch({ url: '/pages/login/index' })
}

function isTokenAuthError(error: unknown) {
  const msg = String((error as any)?.message || '').trim()
  return /token\s*(无效|已过期|格式错误)|invalid\s*token|expired\s*token|重新登录/i.test(msg)
}

function verifyInBackground(tokenAtStart: string) {
  if (!tokenAtStart || verifyStartedForToken === tokenAtStart) return
  verifyStartedForToken = tokenAtStart
  authApi.verify().catch((error) => {
    const currentToken = String(auth.token || uni.getStorageSync('token') || '')
    if (currentToken !== tokenAtStart) return
    if (!isTokenAuthError(error)) return
    auth.clearAuth()
    redirectLogin()
  })
}

onShow(() => {
  auth.restoreFromStorage()
  if (!auth.isLoggedIn) {
    statusText.value = '请先登录'
    redirectInitial('/pages/login/index')
    return
  }

  const tokenAtStart = String(auth.token || '')
  verifyInBackground(tokenAtStart)
  redirectInitial('/pages/index/index')
})
</script>

<template>
  <view class="page">
    <view class="brand">
      <text class="logo-text">爱波塔</text>
      <text class="logo-sub">AI 驱动的期权期货分析</text>
    </view>
    <view class="status-box">
      <view class="spinner"></view>
      <text class="status-text">{{ statusText }}</text>
    </view>
  </view>
</template>

<style scoped>
.page {
  min-height: 100vh;
  background: #0b1121;
  color: #f0f0f0;
  display: flex;
  flex-direction: column;
  align-items: center;
  justify-content: center;
  padding: 0 48rpx;
  box-sizing: border-box;
}

.brand {
  text-align: center;
  margin-bottom: 64rpx;
}

.logo-text {
  display: block;
  font-size: 72rpx;
  font-weight: 900;
  color: #f5c518;
  letter-spacing: 4rpx;
  margin-bottom: 12rpx;
}

.logo-sub {
  font-size: 26rpx;
  color: #666666;
}

.status-box {
  display: flex;
  align-items: center;
  justify-content: center;
  min-height: 64rpx;
}

.spinner {
  width: 28rpx;
  height: 28rpx;
  border: 4rpx solid rgba(245, 197, 24, 0.24);
  border-top-color: #f5c518;
  border-radius: 50%;
  margin-right: 16rpx;
  animation: spin 0.9s linear infinite;
}

.status-text {
  color: #8ea4d1;
  font-size: 26rpx;
}

@keyframes spin {
  to {
    transform: rotate(360deg);
  }
}
</style>
