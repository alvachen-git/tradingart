<script setup lang="ts">
import { ref, onMounted } from 'vue'
import { onLoad } from '@dcloudio/uni-app'
import { intelApi } from '../../api/index'

const content = ref<any>(null)
const loading = ref(true)
const error = ref('')

onLoad(async (options) => {
  const id = Number(options?.id)
  if (!id) { error.value = '参数错误'; loading.value = false; return }
  try {
    content.value = await intelApi.detail(id)
  } catch (e: any) {
    error.value = e.message || '加载失败'
  } finally {
    loading.value = false
  }
})

function formatDate(s: string) {
  if (!s) return ''
  return s.slice(0, 16).replace('T', ' ')
}
</script>

<template>
  <view class="page">
    <!-- 加载中 -->
    <view v-if="loading" class="center">
      <text class="muted-text">加载中...</text>
    </view>

    <!-- 错误 -->
    <view v-else-if="error" class="center">
      <text class="error-text">{{ error }}</text>
    </view>

    <!-- 内容 -->
    <view v-else-if="content" class="content-wrap">
      <!-- 头部信息 -->
      <view class="meta">
        <view class="channel-badge">{{ content.channel_name }}</view>
        <text class="date-text">{{ formatDate(content.published_at) }}</text>
      </view>

      <text class="article-title">{{ content.title }}</text>
      <view class="divider" />

      <!-- 正文（保留换行） -->
      <text class="article-body selectable">{{ content.content }}</text>
    </view>
  </view>
</template>

<style scoped>
.page {
  background: #0d0d0d;
  min-height: 100vh;
  padding: 32rpx 32rpx 80rpx;
}

.center {
  display: flex;
  justify-content: center;
  align-items: center;
  padding-top: 200rpx;
}

.error-text { color: #e84040; font-size: 28rpx; }

.content-wrap {}

.meta {
  display: flex;
  align-items: center;
  gap: 16rpx;
  margin-bottom: 24rpx;
}

.channel-badge {
  background: rgba(245, 197, 24, 0.12);
  color: #f5c518;
  font-size: 22rpx;
  padding: 6rpx 18rpx;
  border-radius: 20rpx;
  border: 1px solid rgba(245, 197, 24, 0.3);
}

.date-text { font-size: 24rpx; color: #555555; }

.article-title {
  display: block;
  font-size: 36rpx;
  font-weight: 700;
  color: #f0f0f0;
  line-height: 1.5;
  margin-bottom: 20rpx;
}

.divider {
  height: 1px;
  background: #2a2a2a;
  margin: 20rpx 0 30rpx;
}

.article-body {
  display: block;
  font-size: 29rpx;
  color: #cccccc;
  line-height: 1.85;
  white-space: pre-wrap;
}
</style>
