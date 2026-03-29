<script setup lang="ts">
import { ref, onMounted } from 'vue'
import { onShow } from '@dcloudio/uni-app'
import { userApi, payApi, type UserProfile, type WalletInfo } from '../../api/index'
import { useAuthStore } from '../../store/auth'
import BottomNav from '../../components/BottomNav.vue'

const auth = useAuthStore()
const profile = ref<UserProfile | null>(null)
const wallet = ref<WalletInfo | null>(null)
const loading = ref(false)

const SERVICE_CHANNELS = [
  { code: 'daily_report', name: '复盘晚报' },
  { code: 'expiry_option_radar', name: '末日期权晚报' },
  { code: 'broker_position_report', name: '期货商持仓晚报' },
  { code: 'fund_flow_report', name: '资金流晚报' },
  { code: 'trade_signal', name: '交易信号' },
]

onShow(() => {
  if (!auth.isLoggedIn) {
    uni.reLaunch({ url: '/pages/login/index' })
    return
  }
  loadAll()
})
onMounted(loadAll)

async function loadAll() {
  loading.value = true
  try {
    const [p, w] = await Promise.all([
      userApi.profile(),
      payApi.wallet().catch(() => null),
    ])
    profile.value = p
    wallet.value = w
  } catch (e: any) {
    uni.showToast({ title: e.message || '加载失败', icon: 'none' })
  } finally {
    loading.value = false
  }
}

function formatDate(raw: string) {
  const text = String(raw || '').trim()
  if (!text) return ''
  if (text.length >= 10) return text.slice(0, 10)
  return text
}

function normCode(raw: string) {
  return String(raw || '').trim().toLowerCase()
}

function getPermissionText(code: string) {
  const target = normCode(code)
  const activeSub = profile.value?.subscriptions?.find(
    s => normCode(s.channel_code) === target && !!s.is_active
  )
  if (!activeSub) return '未开通'
  const dateText = formatDate(activeSub.expires_at)
  return dateText ? `有效期至 ${dateText}` : '有效期至 -'
}

function isPermissionActive(code: string) {
  const target = normCode(code)
  return profile.value?.subscriptions?.some(
    s => normCode(s.channel_code) === target && !!s.is_active
  ) ?? false
}

function goRecharge() {
  uni.navigateTo({ url: '/pages/recharge/index' })
}

function confirmLogout() {
  uni.showModal({
    title: '退出登录',
    content: '确认退出当前账号？',
    success: async (res) => {
      if (res.confirm) {
        try {
          const { authApi } = await import('../../api/index')
          await authApi.logout()
        } catch {
          // 忽略网络错误，本地直接清除
        }
        auth.clearAuth()
        uni.reLaunch({ url: '/pages/login/index' })
      }
    },
  })
}

function getRiskColor(risk: string) {
  const map: Record<string, string> = {
    '激进型': '#e84040',
    '积极型': '#f97316',
    '稳健型': '#f5c518',
    '保守型': '#22c55e',
    '未知': '#666666',
  }
  return map[risk] || '#888888'
}
</script>

<template>
  <view class="page">
    <view class="user-header">
      <view class="avatar">
        <text class="avatar-text">{{ profile?.username?.[0]?.toUpperCase() || '?' }}</text>
      </view>
      <view class="user-info">
        <text class="username">{{ profile?.username || auth.username }}</text>
        <text class="email-text">{{ profile?.email || '' }}</text>
      </view>
      <view class="level-badge">Lv.{{ profile?.level || 1 }}</view>
    </view>

    <view class="card section-card">
      <text class="card-title-text">交易画像</text>
      <view class="profile-row">
        <text class="profile-label">风险偏好</text>
        <text
          class="profile-value"
          :style="{ color: getRiskColor(profile?.risk_preference || '未知') }"
        >{{ profile?.risk_preference || '未知' }}</text>
      </view>
      <view v-if="profile?.focus_assets" class="profile-row">
        <text class="profile-label">关注标的</text>
        <text class="profile-value profile-value-assets">{{ profile.focus_assets }}</text>
      </view>
    </view>

    <view class="card section-card">
      <text class="card-title-text">服务权限</text>
      <view
        v-for="ch in SERVICE_CHANNELS"
        :key="ch.code"
        class="perm-row"
      >
        <view class="perm-left">
          <view class="perm-name">{{ ch.name }}</view>
        </view>
        <view
          class="perm-status"
          :class="{ active: isPermissionActive(ch.code), inactive: !isPermissionActive(ch.code) }"
        >
          {{ getPermissionText(ch.code) }}
        </view>
      </view>
    </view>

    <view class="card section-card recharge-card" @tap="goRecharge">
      <view class="recharge-main">
        <text class="card-title-text recharge-title">充值中心</text>
        <text class="recharge-desc">点数可用于开通晚报、交易信号与情报套餐</text>
        <text class="recharge-update" v-if="wallet?.updated_at">
          最近更新：{{ wallet.updated_at }}
        </text>
      </view>
      <view class="recharge-side">
        <text class="recharge-balance">{{ wallet?.balance ?? 0 }} 点</text>
        <text class="recharge-enter">进入</text>
      </view>
    </view>

    <view class="logout-section">
      <view class="logout-btn" @tap="confirmLogout">
        <text class="logout-text">退出登录</text>
      </view>
    </view>

    <view style="height: 120rpx;" />
    <BottomNav active="profile" />
  </view>
</template>

<style scoped>
.page { background: #0b1121; min-height: 100vh; padding-bottom: 40rpx; }

.user-header {
  display: flex;
  align-items: center;
  gap: 24rpx;
  padding: 40rpx 32rpx 32rpx;
  background: linear-gradient(180deg, #131c2e 0%, #0b1121 100%);
  border-bottom: 1px solid #1e2d45;
}

.avatar {
  width: 100rpx;
  height: 100rpx;
  border-radius: 50%;
  background: linear-gradient(135deg, #c9a227, #f5c518);
  display: flex;
  align-items: center;
  justify-content: center;
  flex-shrink: 0;
}

.avatar-text { font-size: 44rpx; font-weight: 900; color: #0b1121; }

.user-info { flex: 1; }
.username { display: block; font-size: 34rpx; font-weight: 700; color: #f0f0f0; }
.email-text { display: block; font-size: 24rpx; color: #666666; margin-top: 6rpx; }

.level-badge {
  background: rgba(245, 197, 24, 0.15);
  border: 1px solid rgba(245, 197, 24, 0.4);
  color: #f5c518;
  font-size: 24rpx;
  font-weight: 700;
  padding: 8rpx 20rpx;
  border-radius: 20rpx;
}

.section-card {
  margin: 20rpx 24rpx 0;
  background: #131c2e;
  border: 1px solid #1e2d45;
  border-radius: 20rpx;
  padding: 28rpx;
}

.card-title-text {
  display: block;
  font-size: 28rpx;
  font-weight: 700;
  color: #f5c518;
  margin-bottom: 20rpx;
}

.profile-row {
  display: flex;
  justify-content: space-between;
  align-items: flex-start;
  gap: 20rpx;
  padding: 14rpx 0;
  border-bottom: 1px solid #1a2540;
}

.profile-row:last-child { border-bottom: none; }
.profile-label { font-size: 26rpx; color: #888888; }
.profile-value { font-size: 26rpx; font-weight: 600; }
.profile-value-assets { color: #cccccc; text-align: right; line-height: 1.5; }

.perm-row {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 16rpx;
  padding: 18rpx 0;
  border-bottom: 1px solid #1a2540;
}

.perm-row:last-child { border-bottom: none; }

.perm-left { flex: 1; min-width: 0; }
.perm-name { font-size: 28rpx; color: #f0f0f0; font-weight: 600; }

.perm-status {
  flex-shrink: 0;
  font-size: 22rpx;
  padding: 8rpx 14rpx;
  border-radius: 16rpx;
  border: 1px solid transparent;
  text-align: right;
  min-width: 200rpx;
}

.perm-status.active {
  color: #22c55e;
  background: rgba(34, 197, 94, 0.1);
  border-color: rgba(34, 197, 94, 0.3);
}

.perm-status.inactive {
  color: #8b93a9;
  background: #162035;
  border-color: #23314f;
}

.recharge-card {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 20rpx;
}

.recharge-main { flex: 1; min-width: 0; }
.recharge-title { margin-bottom: 10rpx; }
.recharge-desc { display: block; color: #a8b0c4; font-size: 24rpx; line-height: 1.5; }
.recharge-update { display: block; color: #6f7a96; font-size: 22rpx; margin-top: 10rpx; }

.recharge-side { display: flex; flex-direction: column; align-items: flex-end; gap: 10rpx; }
.recharge-balance { color: #f5c518; font-size: 30rpx; font-weight: 700; }
.recharge-enter {
  font-size: 22rpx;
  color: #f5c518;
  background: rgba(245, 197, 24, 0.1);
  border: 1px solid rgba(245, 197, 24, 0.35);
  border-radius: 14rpx;
  padding: 6rpx 16rpx;
}

.logout-section { padding: 40rpx 24rpx 0; }

.logout-btn {
  background: #131c2e;
  border: 1px solid #3a1a1a;
  border-radius: 20rpx;
  padding: 28rpx 0;
  text-align: center;
}

.logout-text { font-size: 30rpx; color: #e84040; font-weight: 600; }
</style>
