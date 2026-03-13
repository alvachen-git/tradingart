<script setup lang="ts">
import { ref } from 'vue'
import { authApi } from '../../api/index'
import { useAuthStore } from '../../store/auth'

const auth = useAuthStore()
const account = ref('')
const password = ref('')
const agreed = ref(false)
const loading = ref(false)
const error = ref('')

async function handleLogin() {
  error.value = ''
  if (!account.value || !password.value) { error.value = '请填写账号和密码'; return }
  if (!agreed.value) { error.value = '请先同意《用户服务协议》和《隐私政策》'; return }

  loading.value = true
  try {
    const res = await authApi.login(account.value, password.value)
    auth.setAuth(res.token, res.username)
    uni.reLaunch({ url: '/pages/index/index' })
  } catch (e: any) {
    error.value = e.message || '登录失败'
  } finally {
    loading.value = false
  }
}

function toggleAgree() {
  agreed.value = !agreed.value
  if (agreed.value && error.value.includes('同意')) error.value = ''
}

function onAgreeChange(e: any) {
  const values = e?.detail?.value || []
  agreed.value = Array.isArray(values) && values.includes('agreed')
  if (agreed.value && error.value.includes('同意')) error.value = ''
}

function openProtocol(type: 'terms' | 'privacy') {
  uni.navigateTo({ url: `/pages/policy/${type}` })
}
</script>

<template>
  <view class="page">
    <!-- Logo 区域 -->
    <view class="header">
      <text class="logo-text">爱波塔</text>
      <text class="logo-sub">AI 驱动的期权期货分析</text>
    </view>

    <view class="form">
      <view class="field">
        <text class="field-label">账号</text>
        <input
          v-model="account"
          class="field-input"
          placeholder="请输入用户名"
          placeholder-class="placeholder"
          :disabled="loading"
        />
      </view>
      <view class="field">
        <text class="field-label">密码</text>
        <input
          v-model="password"
          class="field-input"
          placeholder="请输入密码"
          placeholder-class="placeholder"
          password
          :disabled="loading"
          @confirm="handleLogin"
        />
      </view>
    </view>

    <view class="agree-row">
      <checkbox-group class="agree-check-group" @change="onAgreeChange">
        <checkbox class="agree-checkbox" value="agreed" :checked="agreed" color="#f5c518" />
      </checkbox-group>
      <view class="agree-text-wrap">
        <text class="agree-text" @tap="toggleAgree">我已阅读并同意</text>
        <text class="link-text" @tap.stop="openProtocol('terms')">《用户服务协议》</text>
        <text class="agree-text"> 和 </text>
        <text class="link-text" @tap.stop="openProtocol('privacy')">《隐私政策》</text>
      </view>
    </view>

    <!-- 错误提示 -->
    <view v-if="error" class="error-msg">{{ error }}</view>

    <!-- 登录按钮 -->
    <button class="btn-primary login-btn" :disabled="loading" @click="handleLogin">
      {{ loading ? '登录中...' : '登录' }}
    </button>

    <view class="footer-tip">
      <text class="muted-text">还没有账号？请在电脑端注册后使用</text>
      <text class="contact-text">登录遇到问题可咨询客服：微信 trader-sec ｜ 电话 17521591756</text>
    </view>
  </view>
</template>

<style scoped>
.page {
  min-height: 100vh;
  background: #0b1121;
  padding: 0 48rpx;
  display: flex;
  flex-direction: column;
}

.header {
  padding-top: 120rpx;
  margin-bottom: 60rpx;
  text-align: center;
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
.form { margin-bottom: 26rpx; }

.field { margin-bottom: 28rpx; }

.field-label {
  display: block;
  font-size: 26rpx;
  color: #aaaaaa;
  margin-bottom: 12rpx;
}

.field-input {
  width: 100%;
  height: 88rpx;
  background: #162035;
  border: 1px solid #1e2d45;
  border-radius: 16rpx;
  padding: 0 28rpx;
  font-size: 30rpx;
  color: #f0f0f0;
  box-sizing: border-box;
}

.placeholder { color: #444444; }

.agree-row {
  display: flex;
  align-items: flex-start;
  gap: 12rpx;
  margin: 8rpx 0 22rpx;
}

.agree-check-group {
  margin-top: 2rpx;
}

.agree-checkbox {
  transform: scale(0.9);
}

.agree-text-wrap {
  flex: 1;
  line-height: 1.5;
}

.agree-text {
  font-size: 24rpx;
  color: #6f7f95;
}

.link-text {
  color: #f5c518;
}

.error-msg {
  color: #e84040;
  font-size: 26rpx;
  margin-bottom: 20rpx;
  text-align: center;
}

.login-btn {
  background: #f5c518 !important;
  color: #0b1121 !important;
  font-weight: 700 !important;
  font-size: 32rpx !important;
  border-radius: 16rpx !important;
  height: 96rpx !important;
  line-height: 96rpx !important;
  text-align: center !important;
  padding: 0 !important;
  border: none !important;
  margin-top: 8rpx;
}

.login-btn[disabled] {
  opacity: 0.5 !important;
}

.footer-tip {
  margin-top: 40rpx;
  text-align: center;
  display: flex;
  flex-direction: column;
  gap: 12rpx;
}

.contact-text {
  font-size: 22rpx;
  color: #6f7f95;
  line-height: 1.5;
}
</style>
