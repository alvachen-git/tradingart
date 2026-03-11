<script setup lang="ts">
import { ref } from 'vue'
import { authApi } from '../../api/index'
import { useAuthStore } from '../../store/auth'

const auth = useAuthStore()
const mode = ref<'password' | 'email'>('password')

// 密码登录
const account = ref('')
const password = ref('')

// 邮箱登录
const email = ref('')
const code = ref('')
const codeCooldown = ref(0)

const loading = ref(false)
const error = ref('')

async function handleLogin() {
  error.value = ''
  if (mode.value === 'password') {
    if (!account.value || !password.value) { error.value = '请填写账号和密码'; return }
  } else {
    if (!email.value || !code.value) { error.value = '请填写邮箱和验证码'; return }
  }

  loading.value = true
  try {
    let res
    if (mode.value === 'password') {
      res = await authApi.login(account.value, password.value)
    } else {
      res = await authApi.loginEmail(email.value, code.value)
    }
    auth.setAuth(res.token, res.username)
    uni.reLaunch({ url: '/pages/index/index' })
  } catch (e: any) {
    error.value = e.message || '登录失败'
  } finally {
    loading.value = false
  }
}

async function sendCode() {
  if (!email.value) { error.value = '请先填写邮箱'; return }
  error.value = ''
  try {
    await authApi.sendCode(email.value)
    uni.showToast({ title: '验证码已发送', icon: 'none' })
    codeCooldown.value = 60
    const timer = setInterval(() => {
      codeCooldown.value--
      if (codeCooldown.value <= 0) clearInterval(timer)
    }, 1000)
  } catch (e: any) {
    error.value = e.message || '发送失败'
  }
}
</script>

<template>
  <view class="page">
    <!-- Logo 区域 -->
    <view class="header">
      <text class="logo-text">爱波塔</text>
      <text class="logo-sub">AI 驱动的期权期货分析</text>
    </view>

    <!-- 模式切换 -->
    <view class="tab-switch">
      <view
        class="switch-item"
        :class="{ active: mode === 'password' }"
        @tap="mode = 'password'; error = ''"
      >密码登录</view>
      <view
        class="switch-item"
        :class="{ active: mode === 'email' }"
        @tap="mode = 'email'; error = ''"
      >邮箱验证码</view>
    </view>

    <!-- 密码登录表单 -->
    <view v-if="mode === 'password'" class="form">
      <view class="field">
        <text class="field-label">账号</text>
        <input
          v-model="account"
          class="field-input"
          placeholder="用户名或邮箱"
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

    <!-- 邮箱验证码表单 -->
    <view v-else class="form">
      <view class="field">
        <text class="field-label">邮箱</text>
        <input
          v-model="email"
          class="field-input"
          placeholder="请输入注册邮箱"
          placeholder-class="placeholder"
          type="email"
          :disabled="loading"
        />
      </view>
      <view class="field">
        <text class="field-label">验证码</text>
        <view class="code-row">
          <input
            v-model="code"
            class="field-input code-input"
            placeholder="6位验证码"
            placeholder-class="placeholder"
            type="number"
            maxlength="6"
            :disabled="loading"
          />
          <view
            class="send-btn"
            :class="{ disabled: codeCooldown > 0 }"
            @tap="sendCode"
          >
            {{ codeCooldown > 0 ? `${codeCooldown}s` : '发送' }}
          </view>
        </view>
      </view>
    </view>

    <!-- 错误提示 -->
    <view v-if="error" class="error-msg">{{ error }}</view>

    <!-- 登录按钮 -->
    <button class="btn-primary login-btn" :disabled="loading" @tap="handleLogin">
      {{ loading ? '登录中...' : '登录' }}
    </button>

    <view class="footer-tip">
      <text class="muted-text">还没有账号？请在电脑端注册后使用</text>
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

.tab-switch {
  display: flex;
  background: #131c2e;
  border-radius: 16rpx;
  padding: 6rpx;
  margin-bottom: 40rpx;
}

.switch-item {
  flex: 1;
  text-align: center;
  padding: 18rpx 0;
  font-size: 28rpx;
  color: #666666;
  border-radius: 12rpx;
  transition: all 0.2s;
}

.switch-item.active {
  background: #f5c518;
  color: #0b1121;
  font-weight: 700;
}

.form { margin-bottom: 16rpx; }

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

.code-row { display: flex; gap: 16rpx; }

.code-input { flex: 1; }

.send-btn {
  width: 160rpx;
  height: 88rpx;
  background: rgba(245, 197, 24, 0.15);
  border: 1px solid rgba(245, 197, 24, 0.4);
  border-radius: 16rpx;
  display: flex;
  align-items: center;
  justify-content: center;
  color: #f5c518;
  font-size: 26rpx;
  font-weight: 600;
  flex-shrink: 0;
}

.send-btn.disabled {
  opacity: 0.4;
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
  border: none !important;
  margin-top: 8rpx;
}

.login-btn[disabled] {
  opacity: 0.5 !important;
}

.footer-tip {
  margin-top: 40rpx;
  text-align: center;
}
</style>
