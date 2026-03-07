<script setup lang="ts">
import { ref, onMounted } from 'vue'
import { onShow, onReachBottom } from '@dcloudio/uni-app'
import { intelApi, type ReportItem } from '../../api/index'
import { useAuthStore } from '../../store/auth'
import BottomNav from '../../components/BottomNav.vue'

const auth = useAuthStore()

const channels = [
  { code: '',                          label: '全部' },
  { code: 'fund_flow_report',          label: '复盘晚报' },
  { code: 'expiry_option_report',      label: '末日期权晚报' },
  { code: 'broker_position_report',    label: '期货商持仓' },
]

const activeChannel = ref('')
const reports = ref<ReportItem[]>([])
const page = ref(1)
const hasMore = ref(true)
const loading = ref(false)
const refreshing = ref(false)

onShow(() => {
  if (!auth.isLoggedIn) uni.reLaunch({ url: '/pages/login/index' })
})
onMounted(loadReports)
onReachBottom(loadMore)

async function loadReports(reset = false) {
  if (reset) { page.value = 1; reports.value = []; hasMore.value = true }
  if (loading.value || !hasMore.value) return
  loading.value = true
  try {
    const res = await intelApi.reports({
      channel_code: activeChannel.value || undefined,
      page: page.value,
      page_size: 15,
    })
    if (reset) {
      reports.value = res.items
    } else {
      reports.value.push(...res.items)
    }
    hasMore.value = res.has_more
    page.value++
  } catch (e: any) {
    uni.showToast({ title: e.message || '加载失败', icon: 'none' })
  } finally {
    loading.value = false
    refreshing.value = false
  }
}

async function loadMore() {
  if (!hasMore.value || loading.value) return
  await loadReports()
}

async function switchChannel(code: string) {
  if (activeChannel.value === code) return
  activeChannel.value = code
  await loadReports(true)
}

function toDetail(id: number) {
  uni.navigateTo({ url: `/pages/intel/detail?id=${id}` })
}

function formatDate(s: string) {
  if (!s) return ''
  return s.slice(0, 16).replace('T', ' ')
}
</script>

<template>
  <view class="page">
    <!-- 频道筛选 -->
    <scroll-view class="channel-bar" scroll-x>
      <view class="channel-list">
        <view
          v-for="ch in channels"
          :key="ch.code"
          class="channel-tag"
          :class="{ active: activeChannel === ch.code }"
          @tap="switchChannel(ch.code)"
        >
          {{ ch.label }}
        </view>
      </view>
    </scroll-view>

    <!-- 报告列表 -->
    <view class="list">
      <view
        v-for="item in reports"
        :key="item.id"
        class="report-card"
        @tap="toDetail(item.id)"
      >
        <view class="card-top">
          <view class="channel-badge">{{ item.channel_name }}</view>
          <text class="date-text">{{ formatDate(item.published_at) }}</text>
        </view>
        <text class="card-title">{{ item.title }}</text>
        <text class="card-summary">{{ item.summary }}</text>
      </view>

      <!-- 加载中 -->
      <view v-if="loading" class="center-tip">
        <text class="muted-text">加载中...</text>
      </view>

      <!-- 没有更多 -->
      <view v-else-if="!hasMore && reports.length > 0" class="center-tip">
        <text class="muted-text">— 已全部加载 —</text>
      </view>

      <!-- 空状态 -->
      <view v-else-if="!loading && reports.length === 0" class="empty-state">
        <text class="empty-icon">◉</text>
        <text class="muted-text">暂无情报，稍后再来</text>
      </view>
    </view>

    <!-- 底部占位 -->
    <view style="height: 120rpx;" />
    <BottomNav active="intel" />
  </view>
</template>

<style scoped>
.page { background: #0d0d0d; min-height: 100vh; }

.channel-bar {
  position: sticky;
  top: 0;
  z-index: 10;
  background: #0d0d0d;
  border-bottom: 1px solid #1e1e1e;
  white-space: nowrap;
}

.channel-list {
  display: flex;
  padding: 16rpx 24rpx;
  gap: 16rpx;
}

.channel-tag {
  flex-shrink: 0;
  padding: 12rpx 28rpx;
  border-radius: 32rpx;
  font-size: 26rpx;
  color: #aaaaaa;
  background: #1a1a1a;
  border: 1px solid #2a2a2a;
}

.channel-tag.active {
  background: rgba(245, 197, 24, 0.15);
  color: #f5c518;
  border-color: rgba(245, 197, 24, 0.4);
  font-weight: 600;
}

.list { padding: 20rpx 24rpx; }

.report-card {
  background: #1a1a1a;
  border: 1px solid #2a2a2a;
  border-radius: 20rpx;
  padding: 28rpx;
  margin-bottom: 20rpx;
}

.card-top {
  display: flex;
  align-items: center;
  justify-content: space-between;
  margin-bottom: 16rpx;
}

.channel-badge {
  background: rgba(245, 197, 24, 0.12);
  color: #f5c518;
  font-size: 22rpx;
  padding: 4rpx 16rpx;
  border-radius: 20rpx;
  border: 1px solid rgba(245, 197, 24, 0.3);
}

.date-text { font-size: 22rpx; color: #555555; }

.card-title {
  display: block;
  font-size: 30rpx;
  font-weight: 600;
  color: #f0f0f0;
  margin-bottom: 12rpx;
  line-height: 1.5;
}

.card-summary {
  display: block;
  font-size: 26rpx;
  color: #888888;
  line-height: 1.6;
  overflow: hidden;
  display: -webkit-box;
  -webkit-box-orient: vertical;
  -webkit-line-clamp: 2;
}

.center-tip { text-align: center; padding: 30rpx 0; }

.empty-state {
  display: flex;
  flex-direction: column;
  align-items: center;
  padding: 120rpx 0;
  gap: 24rpx;
}

.empty-icon { font-size: 80rpx; color: #333333; }
</style>
