<script setup lang="ts">
import { ref, onMounted } from 'vue'
import { onShow } from '@dcloudio/uni-app'
import { userApi, intelApi, type UserProfile } from '../../api/index'
import { useAuthStore } from '../../store/auth'
import BottomNav from '../../components/BottomNav.vue'

const auth = useAuthStore()
const profile = ref<UserProfile | null>(null)
const loading = ref(false)

const FREE_CHANNELS = [
  { code: 'fund_flow_report',       name: '复盘晚报' },
  { code: 'expiry_option_radar',    name: '末日期权晚报' },
  { code: 'broker_position_report', name: '期货商持仓分析晚报' },
]

onShow(() => {
  if (!auth.isLoggedIn) uni.reLaunch({ url: '/pages/login/index' })
})
onMounted(loadProfile)

async function loadProfile() {
  loading.value = true
  try {
    profile.value = await userApi.profile()
  } catch (e: any) {
    uni.showToast({ title: e.message || '加载失败', icon: 'none' })
  } finally {
    loading.value = false
  }
}

function isSubscribed(code: string) {
  return profile.value?.subscriptions?.some(s => s.channel_code === code && s.is_active) ?? false
}

async function subscribe(code: string, name: string) {
  uni.showLoading({ title: '订阅中...' })
  try {
    await intelApi.subscribe(code)
    uni.hideLoading()
    uni.showToast({ title: `已订阅${name}`, icon: 'success' })
    await loadProfile()
  } catch (e: any) {
    uni.hideLoading()
    uni.showToast({ title: e.message || '订阅失败', icon: 'none' })
  }
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
        } catch { /* 忽略网络错误，本地直接清除 */ }
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
    <!-- 用户头部 -->
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

    <!-- 交易画像 -->
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
        <text class="profile-value" style="color: #cccccc;">{{ profile.focus_assets }}</text>
      </view>
    </view>

    <!-- 免费订阅频道 -->
    <view class="card section-card">
      <text class="card-title-text">免费订阅</text>
      <view
        v-for="ch in FREE_CHANNELS"
        :key="ch.code"
        class="channel-row"
      >
        <view class="channel-info">
          <text class="channel-name">{{ ch.name }}</text>
          <text class="channel-desc">每晚 6:30 更新</text>
        </view>
        <view
          v-if="isSubscribed(ch.code)"
          class="sub-status subscribed"
        >
          <text>已订阅</text>
        </view>
        <view
          v-else
          class="sub-status unsubscribed"
          @tap="subscribe(ch.code, ch.name)"
        >
          <text>免费订阅</text>
        </view>
      </view>
    </view>

    <!-- 已订阅列表 -->
    <view v-if="profile?.subscriptions?.length" class="card section-card">
      <text class="card-title-text">我的订阅</text>
      <view
        v-for="sub in profile.subscriptions"
        :key="sub.channel_code"
        class="sub-row"
      >
        <text class="sub-name">{{ sub.channel_name }}</text>
        <text :class="sub.is_active ? 'active-badge' : 'inactive-badge'">
          {{ sub.is_active ? '生效中' : '已到期' }}
        </text>
      </view>
    </view>

    <!-- 退出登录 -->
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

/* 用户头部 */
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

/* 卡片通用 */
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

/* 画像 */
.profile-row {
  display: flex;
  justify-content: space-between;
  align-items: center;
  padding: 14rpx 0;
  border-bottom: 1px solid #1a2540;
}
.profile-row:last-child { border-bottom: none; }
.profile-label { font-size: 26rpx; color: #888888; }
.profile-value { font-size: 26rpx; font-weight: 600; }

/* 频道订阅行 */
.channel-row {
  display: flex;
  align-items: center;
  justify-content: space-between;
  padding: 16rpx 0;
  border-bottom: 1px solid #1a2540;
}
.channel-row:last-child { border-bottom: none; }

.channel-name { display: block; font-size: 28rpx; color: #f0f0f0; font-weight: 600; }
.channel-desc { display: block; font-size: 22rpx; color: #666666; margin-top: 4rpx; }

.sub-status {
  flex-shrink: 0;
  font-size: 24rpx;
  padding: 8rpx 22rpx;
  border-radius: 20rpx;
  font-weight: 600;
}

.subscribed {
  background: rgba(34, 197, 94, 0.12);
  color: #22c55e;
  border: 1px solid rgba(34, 197, 94, 0.3);
}

.unsubscribed {
  background: rgba(245, 197, 24, 0.12);
  color: #f5c518;
  border: 1px solid rgba(245, 197, 24, 0.35);
}

/* 已订阅列表 */
.sub-row {
  display: flex;
  align-items: center;
  justify-content: space-between;
  padding: 14rpx 0;
  border-bottom: 1px solid #1a2540;
}
.sub-row:last-child { border-bottom: none; }
.sub-name { font-size: 26rpx; color: #cccccc; }

.active-badge {
  font-size: 22rpx;
  color: #22c55e;
  background: rgba(34, 197, 94, 0.1);
  padding: 4rpx 16rpx;
  border-radius: 16rpx;
}

.inactive-badge {
  font-size: 22rpx;
  color: #666666;
  background: #162035;
  padding: 4rpx 16rpx;
  border-radius: 16rpx;
}

/* 退出 */
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
