<script setup lang="ts">
import { ref, onMounted } from 'vue'
import { onShow } from '@dcloudio/uni-app'
import { marketApi } from '../../api/index'
import { useAuthStore } from '../../store/auth'
import BottomNav from '../../components/BottomNav.vue'

const auth = useAuthStore()
const data = ref<any>(null)
const loading = ref(false)
const lastUpdated = ref('')

onShow(() => {
  if (!auth.isLoggedIn) uni.reLaunch({ url: '/pages/login/index' })
})
onMounted(load)

async function load() {
  loading.value = true
  try {
    const res = await marketApi.snapshot()
    data.value = res.data
    lastUpdated.value = new Date().toLocaleTimeString('zh-CN')
  } catch (e: any) {
    uni.showToast({ title: e.message || '行情加载失败', icon: 'none' })
  } finally {
    loading.value = false
  }
}

// 将对象扁平化为可展示的 key-value 列表
function flatEntries(obj: any, prefix = ''): Array<{ label: string; value: string }> {
  if (!obj || typeof obj !== 'object') return []
  const result: Array<{ label: string; value: string }> = []
  for (const [k, v] of Object.entries(obj)) {
    const label = prefix ? `${prefix} · ${k}` : k
    if (v !== null && typeof v === 'object' && !Array.isArray(v)) {
      result.push(...flatEntries(v, label))
    } else if (Array.isArray(v)) {
      result.push({ label, value: `${v.length} 条` })
    } else {
      result.push({ label, value: String(v ?? '-') })
    }
  }
  return result.slice(0, 60) // 最多展示60条
}

function isPositive(val: string) {
  return val.startsWith('+') || (parseFloat(val) > 0 && !val.startsWith('-'))
}

function isNegative(val: string) {
  return val.startsWith('-') || parseFloat(val) < 0
}
</script>

<template>
  <view class="page">
    <!-- 刷新提示 -->
    <view class="top-bar">
      <text class="top-title">行情快照</text>
      <view class="refresh-btn" :class="{ spinning: loading }" @tap="load">
        <text class="refresh-icon">↻</text>
      </view>
    </view>

    <view v-if="lastUpdated" class="update-tip">
      <text class="muted-text">更新于 {{ lastUpdated }}（建议每分钟刷新）</text>
    </view>

    <!-- 加载中 -->
    <view v-if="loading && !data" class="center">
      <text class="muted-text">行情加载中...</text>
    </view>

    <!-- 数据展示 -->
    <view v-else-if="data" class="data-wrap">
      <!-- 如果后端返回的是分组对象，按组显示 -->
      <template v-if="typeof data === 'object' && !Array.isArray(data)">
        <view v-for="(group, groupKey) in data" :key="groupKey" class="group-card">
          <text class="group-title">{{ groupKey }}</text>
          <view class="kv-list">
            <template v-if="typeof group === 'object' && group !== null">
              <view
                v-for="(val, key) in group"
                :key="key"
                class="kv-row"
              >
                <text class="kv-label">{{ key }}</text>
                <text
                  class="kv-value"
                  :class="{
                    'red-text': isPositive(String(val)),
                    'green-text': isNegative(String(val))
                  }"
                >{{ val ?? '-' }}</text>
              </view>
            </template>
            <view v-else class="kv-row">
              <text class="kv-label">{{ groupKey }}</text>
              <text class="kv-value">{{ group }}</text>
            </view>
          </view>
        </view>
      </template>

      <!-- 如果是数组或简单值 -->
      <view v-else class="group-card">
        <text class="kv-value">{{ JSON.stringify(data, null, 2) }}</text>
      </view>
    </view>

    <!-- 空状态 -->
    <view v-else-if="!loading" class="center">
      <text class="muted-text">暂无行情数据，点击刷新重试</text>
    </view>

    <view style="height: 120rpx;" />
    <BottomNav active="market" />
  </view>
</template>

<style scoped>
.page { background: #0d0d0d; min-height: 100vh; }

.top-bar {
  display: flex;
  align-items: center;
  justify-content: space-between;
  padding: 20rpx 32rpx;
  border-bottom: 1px solid #1e1e1e;
}

.top-title { font-size: 32rpx; font-weight: 700; color: #f5c518; }

.refresh-btn {
  width: 60rpx;
  height: 60rpx;
  display: flex;
  align-items: center;
  justify-content: center;
}

.refresh-icon { font-size: 44rpx; color: #aaaaaa; }

.spinning .refresh-icon {
  animation: spin 0.8s linear infinite;
}

@keyframes spin { to { transform: rotate(360deg); } }

.update-tip {
  padding: 10rpx 32rpx;
  background: #111111;
}

.center {
  display: flex;
  justify-content: center;
  padding-top: 200rpx;
}

.data-wrap { padding: 20rpx 24rpx; }

.group-card {
  background: #1a1a1a;
  border: 1px solid #2a2a2a;
  border-radius: 20rpx;
  padding: 24rpx 28rpx;
  margin-bottom: 20rpx;
}

.group-title {
  display: block;
  font-size: 28rpx;
  font-weight: 700;
  color: #f5c518;
  margin-bottom: 20rpx;
  padding-bottom: 16rpx;
  border-bottom: 1px solid #2a2a2a;
}

.kv-list {}

.kv-row {
  display: flex;
  justify-content: space-between;
  align-items: center;
  padding: 14rpx 0;
  border-bottom: 1px solid #1e1e1e;
}
.kv-row:last-child { border-bottom: none; }

.kv-label { font-size: 26rpx; color: #888888; flex: 1; }

.kv-value {
  font-size: 28rpx;
  color: #f0f0f0;
  font-weight: 500;
  text-align: right;
}
</style>
