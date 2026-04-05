<script setup lang="ts">
import { ref } from 'vue'
import { onShow } from '@dcloudio/uni-app'
import { authApi } from '../../api/index'
import { useAuthStore } from '../../store/auth'
import { request } from '../../utils/request'

const auth = useAuthStore()
const mode = ref<'login' | 'register'>('login')

const account = ref('')
const password = ref('')
const loginLoading = ref(false)

const regUsername = ref('')
const regPassword = ref('')
const regPasswordConfirm = ref('')
const regStep1Ready = ref(false)
const regPhone = ref('')
const regCode = ref('')
const codeSending = ref(false)
const registerLoading = ref(false)

const agreed = ref(false)
const error = ref('')

onShow(async () => {
  await auth.waitForBootstrap()
  auth.restoreFromStorage()
  if (!auth.isLoggedIn) return
  try {
    await authApi.verify()
    uni.reLaunch({ url: '/pages/index/index' })
  } catch {
    auth.clearAuth()
  }
})

function switchMode(nextMode: 'login' | 'register') {
  mode.value = nextMode
  error.value = ''
}

async function handleLogin() {
  error.value = ''
  if (!account.value || !password.value) { error.value = '请填写账号和密码'; return }
  if (!agreed.value) { error.value = '请先同意《用户服务协议》和《隐私政策》'; return }

  loginLoading.value = true
  try {
    const res = await authApi.login(account.value, password.value)
    auth.setAuth(res.token, res.username)
    uni.reLaunch({ url: '/pages/index/index' })
  } catch (e: any) {
    error.value = e.message || '登录失败'
  } finally {
    loginLoading.value = false
  }
}

function handleRegisterStep1() {
  error.value = ''
  const username = regUsername.value.trim()
  const pwd = regPassword.value
  const pwd2 = regPasswordConfirm.value
  if (!username) { error.value = '请填写账号'; return }
  if (username.length < 3) { error.value = '账号至少3个字符'; return }
  if (!pwd || pwd.length < 6) { error.value = '密码至少6位'; return }
  if (pwd !== pwd2) { error.value = '两次密码不一致'; return }
  regStep1Ready.value = true
}

function editRegisterStep1() {
  regStep1Ready.value = false
  regPhone.value = ''
  regCode.value = ''
  error.value = ''
}

async function sendRegisterCode() {
  error.value = ''
  if (!regPhone.value.trim()) { error.value = '请先输入手机号'; return }
  codeSending.value = true
  try {
    const res = await request<{ message: string }>('POST', '/api/auth/register/send-phone-code', {
      phone: regPhone.value.trim(),
    })
    uni.showToast({ title: res.message || '验证码已发送', icon: 'none' })
  } catch (e: any) {
    error.value = e.message || '验证码发送失败'
  } finally {
    codeSending.value = false
  }
}

async function handleRegister() {
  error.value = ''
  if (!agreed.value) { error.value = '请先同意《用户服务协议》和《隐私政策》'; return }
  if (!regStep1Ready.value) { error.value = '请先完成账号密码步骤'; return }
  if (!regPhone.value.trim()) { error.value = '请填写手机号'; return }
  if (!regCode.value.trim()) { error.value = '请输入短信验证码'; return }

  registerLoading.value = true
  try {
    const res = await request<{ token: string; username: string; message: string }>(
      'POST',
      '/api/auth/register',
      {
      username: regUsername.value.trim(),
      password: regPassword.value,
      password_confirm: regPasswordConfirm.value,
      phone: regPhone.value.trim(),
      sms_code: regCode.value.trim(),
      },
    )
    auth.setAuth(res.token, res.username)
    uni.showToast({ title: '注册成功', icon: 'success' })
    uni.reLaunch({ url: '/pages/index/index' })
  } catch (e: any) {
    error.value = e.message || '注册失败'
  } finally {
    registerLoading.value = false
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

    <view class="mode-switch">
      <view class="mode-btn" :class="{ active: mode === 'login' }" @tap="switchMode('login')">登录</view>
      <view class="mode-btn" :class="{ active: mode === 'register' }" @tap="switchMode('register')">注册</view>
    </view>

    <view v-if="mode === 'login'" class="form">
      <view class="field">
        <text class="field-label">账号</text>
        <input
          v-model="account"
          class="field-input"
          placeholder="请输入用户名"
          placeholder-class="placeholder"
          :disabled="loginLoading"
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
          :disabled="loginLoading"
          @confirm="handleLogin"
        />
      </view>
    </view>

    <view v-else class="form">
      <view class="step-title-wrap">
        <text class="step-title">步骤1：账号与密码</text>
      </view>
      <view class="field">
        <text class="field-label">账号</text>
        <input
          v-model="regUsername"
          class="field-input"
          placeholder="至少3个字符"
          placeholder-class="placeholder"
          :disabled="registerLoading || regStep1Ready"
        />
      </view>
      <view class="field">
        <text class="field-label">设置密码</text>
        <input
          v-model="regPassword"
          class="field-input"
          placeholder="至少6位"
          placeholder-class="placeholder"
          password
          :disabled="registerLoading || regStep1Ready"
        />
      </view>
      <view class="field">
        <text class="field-label">确认密码</text>
        <input
          v-model="regPasswordConfirm"
          class="field-input"
          placeholder="再次输入密码"
          placeholder-class="placeholder"
          password
          :disabled="registerLoading || regStep1Ready"
        />
      </view>
      <button
        v-if="!regStep1Ready"
        class="btn-secondary"
        :disabled="registerLoading"
        @click="handleRegisterStep1"
      >
        下一步
      </button>
      <view v-else class="step-ok-row">
        <text class="step-ok-text">账号步骤已完成：{{ regUsername }}</text>
        <text class="step-edit-btn" @tap="editRegisterStep1">修改</text>
      </view>

      <view v-if="regStep1Ready">
        <view class="step-title-wrap step-gap">
          <text class="step-title">步骤2：手机号验证</text>
        </view>
        <view class="field">
          <text class="field-label">手机号（+86）</text>
          <input
            v-model="regPhone"
            class="field-input"
            placeholder="例如 13800138000"
            placeholder-class="placeholder"
            :disabled="registerLoading"
          />
        </view>
        <view class="field">
          <text class="field-label">短信验证码</text>
          <view class="code-row">
            <input
              v-model="regCode"
              class="field-input code-input"
              placeholder="输入6位验证码"
              placeholder-class="placeholder"
              :disabled="registerLoading"
            />
            <button
              class="code-btn"
              :disabled="registerLoading || codeSending"
              @click="sendRegisterCode"
            >
              {{ codeSending ? '发送中...' : '发验证码' }}
            </button>
          </view>
        </view>
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
    <button
      v-if="mode === 'login'"
      class="btn-primary login-btn"
      :disabled="loginLoading"
      @click="handleLogin"
    >
      {{ loginLoading ? '登录中...' : '登录' }}
    </button>
    <button
      v-else
      class="btn-primary login-btn"
      :disabled="registerLoading || !regStep1Ready"
      @click="handleRegister"
    >
      {{ registerLoading ? '注册中...' : '完成注册' }}
    </button>

    <view class="footer-tip">
      <text class="muted-text" v-if="mode === 'login'">还没有账号？切换到“注册”即可开通</text>
      <text class="muted-text" v-else>注册成功后将自动登录</text>
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

.mode-switch {
  display: flex;
  background: #101a2c;
  border: 1px solid #1e2d45;
  border-radius: 16rpx;
  padding: 6rpx;
  margin-bottom: 28rpx;
}

.mode-btn {
  flex: 1;
  text-align: center;
  font-size: 27rpx;
  color: #8ea4d1;
  padding: 12rpx 0;
  border-radius: 12rpx;
}

.mode-btn.active {
  background: #f5c518;
  color: #0b1121;
  font-weight: 700;
}

.form { margin-bottom: 26rpx; }

.field { margin-bottom: 28rpx; }

.step-title-wrap {
  margin-bottom: 14rpx;
}

.step-title {
  font-size: 24rpx;
  color: #8ea4d1;
}

.step-gap {
  margin-top: 8rpx;
}

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

.btn-secondary {
  background: #162035 !important;
  color: #8ea4d1 !important;
  font-weight: 600 !important;
  border: 1px solid #1e2d45 !important;
  border-radius: 14rpx !important;
  height: 78rpx !important;
  line-height: 78rpx !important;
  font-size: 28rpx !important;
  margin-bottom: 16rpx;
}

.step-ok-row {
  margin-bottom: 16rpx;
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 18rpx;
}

.step-ok-text {
  font-size: 24rpx;
  color: #22c55e;
}

.step-edit-btn {
  font-size: 24rpx;
  color: #f5c518;
}

.code-row {
  display: flex;
  gap: 12rpx;
  align-items: center;
}

.code-input {
  flex: 1;
}

.code-btn {
  width: 190rpx;
  flex-shrink: 0;
  background: #162035 !important;
  color: #f5c518 !important;
  border: 1px solid #f5c518 !important;
  border-radius: 14rpx !important;
  height: 88rpx !important;
  line-height: 88rpx !important;
  font-size: 26rpx !important;
  padding: 0 !important;
}

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
