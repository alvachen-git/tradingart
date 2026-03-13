<script setup lang="ts">
import { ref, onMounted } from 'vue'
import { onShow } from '@dcloudio/uni-app'
import { portfolioApi } from '../../api/index'
import { useAuthStore } from '../../store/auth'
import BottomNav from '../../components/BottomNav.vue'

const auth = useAuthStore()
const snapshot = ref<any>(null)
const hasData = ref(false)
const uploading = ref(false)
const polling = ref(false)
const pollMsg = ref('')
let pollTimer: ReturnType<typeof setInterval> | null = null

onShow(() => {
  if (!auth.isLoggedIn) uni.reLaunch({ url: '/pages/login/index' })
})
onMounted(loadResult)

async function loadResult() {
  try {
    const res = await portfolioApi.result()
    hasData.value = res.has_data
    snapshot.value = res.snapshot
  } catch { /* 忽略，页面会显示上传引导 */ }
}

function chooseAndUpload() {
  uni.chooseImage({
    count: 1,
    sizeType: ['compressed'],
    sourceType: ['album', 'camera'],
    success: async (res) => {
      const filePath = res.tempFilePaths[0]
      uploading.value = true
      uni.showLoading({ title: '识别持仓中...' })
      try {
        const uploadRes = await portfolioApi.upload(filePath)
        uni.hideLoading()
        uni.showToast({ title: uploadRes.message, icon: 'none', duration: 2500 })
        startPolling(uploadRes.task_id)
      } catch (e: any) {
        uni.hideLoading()
        uni.showModal({ title: '识别失败', content: e.message || '请上传清晰的持仓截图', showCancel: false })
        uploading.value = false
      }
    },
  })
}

function startPolling(taskId: string) {
  polling.value = true
  pollMsg.value = '正在生成体检报告...'
  stopPolling()

  pollTimer = setInterval(async () => {
    try {
      const res = await portfolioApi.status(taskId)
      pollMsg.value = res.progress || '分析中...'

      if (res.status === 'success') {
        stopPolling()
        polling.value = false
        uploading.value = false
        uni.showToast({ title: '体检完成', icon: 'success' })
        await loadResult()
      } else if (res.status === 'error') {
        stopPolling()
        polling.value = false
        uploading.value = false
        uni.showModal({ title: '分析失败', content: res.error || '请重试', showCancel: false })
      }
    } catch {
      stopPolling()
      polling.value = false
      uploading.value = false
    }
  }, 3000)
}

function stopPolling() {
  if (pollTimer) { clearInterval(pollTimer); pollTimer = null }
}

function formatVal(v: any) {
  if (v === null || v === undefined) return '-'
  if (typeof v === 'number') {
    if (Math.abs(v) >= 1e8) return (v / 1e8).toFixed(2) + ' 亿'
    if (Math.abs(v) >= 1e4) return (v / 1e4).toFixed(2) + ' 万'
    return v.toFixed(2)
  }
  return String(v)
}
</script>

<template>
  <view class="page">
    <view class="ai-notice">
      <text class="ai-notice-text">本页体检结论与总结为人工智能生成内容，仅供参考，不构成投资建议。</text>
    </view>

    <!-- 进度条（上传/分析中） -->
    <view v-if="polling" class="progress-banner">
      <view class="spinner" />
      <text class="progress-text">{{ pollMsg }}</text>
    </view>

    <!-- 上传区 -->
    <view class="upload-section">
      <view class="upload-card" @tap="chooseAndUpload">
        <text class="upload-icon">⊕</text>
        <text class="upload-label">{{ uploading || polling ? '分析中，请稍候...' : '上传持仓截图' }}</text>
        <text class="upload-hint">支持券商 App 持仓页截图</text>
      </view>
    </view>

    <!-- 无数据引导 -->
    <view v-if="!hasData && !polling" class="empty-state">
      <text class="empty-icon">◎</text>
      <text class="empty-title">尚无体检数据</text>
      <text class="muted-text">上传持仓截图，AI 自动分析</text>
      <text class="muted-text" style="margin-top: 8rpx;">行业占比 · 集中度 · 技术评级</text>
    </view>

    <!-- 体检结果 -->
    <view v-else-if="hasData && snapshot" class="result-wrap">
      <!-- 总结 -->
      <view class="card" style="margin: 0 24rpx 20rpx;">
        <text class="section-title">AI 总结</text>
        <text class="summary-text selectable">【AI生成】{{ snapshot.summary_text || '暂无总结' }}</text>
      </view>

      <!-- KPI 指标 -->
      <view class="kpi-row">
        <view class="kpi-card">
          <text class="kpi-label">持仓数量</text>
          <text class="kpi-value">{{ snapshot.recognized_count ?? '-' }}</text>
        </view>
        <view class="kpi-card">
          <text class="kpi-label">总市值</text>
          <text class="kpi-value">{{ formatVal(snapshot.total_market_value) }}</text>
        </view>
        <view class="kpi-card">
          <text class="kpi-label">最大单仓</text>
          <text class="kpi-value gold-text">{{ snapshot.max_weight ? (snapshot.max_weight * 100).toFixed(1) + '%' : '-' }}</text>
        </view>
      </view>

      <!-- 行业分布 -->
      <view v-if="snapshot.industry_allocation?.length" class="card" style="margin: 0 24rpx 20rpx;">
        <text class="section-title">行业分布</text>
        <view
          v-for="item in snapshot.industry_allocation"
          :key="item.industry"
          class="industry-row"
        >
          <text class="industry-name">{{ item.industry }}</text>
          <view class="bar-wrap">
            <view
              class="bar-fill"
              :style="{ width: (item.weight * 100).toFixed(1) + '%' }"
            />
          </view>
          <text class="industry-pct">{{ (item.weight * 100).toFixed(1) }}%</text>
        </view>
      </view>

      <!-- 更新时间 -->
      <view class="update-info">
        <text class="muted-text">上次更新：{{ snapshot.updated_at?.slice(0, 16) || '-' }}</text>
      </view>
    </view>

    <view style="height: 120rpx;" />
    <BottomNav active="health" />
  </view>
</template>

<style scoped>
.page { background: #0d0d0d; min-height: 100vh; }

.ai-notice {
  margin: 16rpx 24rpx 0;
  padding: 12rpx 14rpx;
  border-radius: 12rpx;
  border: 1px solid rgba(245, 197, 24, 0.35);
  background: rgba(245, 197, 24, 0.1);
}

.ai-notice-text {
  font-size: 22rpx;
  line-height: 1.5;
  color: #f5c518;
}

.progress-banner {
  background: rgba(245, 197, 24, 0.1);
  border-bottom: 1px solid rgba(245, 197, 24, 0.2);
  display: flex;
  align-items: center;
  gap: 16rpx;
  padding: 20rpx 32rpx;
}

.spinner {
  width: 32rpx;
  height: 32rpx;
  border: 3px solid #2a2a2a;
  border-top-color: #f5c518;
  border-radius: 50%;
  animation: spin 0.8s linear infinite;
  flex-shrink: 0;
}
@keyframes spin { to { transform: rotate(360deg); } }

.progress-text { font-size: 26rpx; color: #f5c518; }

.upload-section { padding: 24rpx 24rpx 0; }

.upload-card {
  background: #1a1a1a;
  border: 2px dashed #333333;
  border-radius: 20rpx;
  padding: 40rpx 24rpx;
  text-align: center;
  display: flex;
  flex-direction: column;
  align-items: center;
  gap: 12rpx;
}

.upload-icon { font-size: 72rpx; color: #f5c518; line-height: 1; }
.upload-label { font-size: 30rpx; font-weight: 600; color: #f0f0f0; }
.upload-hint { font-size: 24rpx; color: #666666; }

.empty-state {
  display: flex;
  flex-direction: column;
  align-items: center;
  padding: 80rpx 0 40rpx;
  gap: 16rpx;
}

.empty-icon { font-size: 80rpx; color: #333333; }
.empty-title { font-size: 30rpx; color: #666666; margin-bottom: 4rpx; }

.result-wrap { padding-top: 24rpx; }

.section-title {
  display: block;
  font-size: 28rpx;
  font-weight: 700;
  color: #f5c518;
  margin-bottom: 16rpx;
}

.summary-text {
  display: block;
  font-size: 27rpx;
  color: #cccccc;
  line-height: 1.85;
  white-space: pre-wrap;
}

.kpi-row {
  display: flex;
  gap: 16rpx;
  padding: 0 24rpx;
  margin-bottom: 20rpx;
}

.kpi-card {
  flex: 1;
  background: #1a1a1a;
  border: 1px solid #2a2a2a;
  border-radius: 16rpx;
  padding: 20rpx 16rpx;
  text-align: center;
}

.kpi-label { display: block; font-size: 22rpx; color: #888888; margin-bottom: 10rpx; }
.kpi-value { display: block; font-size: 30rpx; font-weight: 700; color: #f0f0f0; }

.card {
  background: #1a1a1a;
  border: 1px solid #2a2a2a;
  border-radius: 20rpx;
  padding: 28rpx;
}

.industry-row {
  display: flex;
  align-items: center;
  gap: 16rpx;
  padding: 12rpx 0;
  border-bottom: 1px solid #1e1e1e;
}
.industry-row:last-child { border-bottom: none; }

.industry-name { font-size: 26rpx; color: #cccccc; width: 160rpx; flex-shrink: 0; }

.bar-wrap {
  flex: 1;
  height: 12rpx;
  background: #2a2a2a;
  border-radius: 6rpx;
  overflow: hidden;
}

.bar-fill {
  height: 100%;
  background: linear-gradient(90deg, #c9a227, #f5c518);
  border-radius: 6rpx;
  min-width: 4rpx;
}

.industry-pct { font-size: 24rpx; color: #f5c518; width: 80rpx; text-align: right; flex-shrink: 0; }

.update-info { text-align: center; padding: 16rpx 0 8rpx; }
</style>
