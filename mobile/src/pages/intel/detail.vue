<script setup lang="ts">
import { ref, computed } from 'vue'
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

// 判断是否为 HTML 内容
const isHtml = computed(() => {
  const c = content.value?.content || ''
  return c.trimStart().startsWith('<')
})

function sanitizeHtmlForMp(html: string): string {
  if (!html) return ''
  return html
    .replace(/<!doctype[^>]*>/gi, '')
    .replace(/<head[\s\S]*?<\/head>/gi, '')
    .replace(/<script[\s\S]*?<\/script>/gi, '')
    .replace(/<style[\s\S]*?<\/style>/gi, '')
    .replace(/<meta[^>]*>/gi, '')
    .replace(/<\/?(html|body)[^>]*>/gi, '')
    .trim()
}

function htmlToPlainText(html: string): string {
  if (!html) return ''
  return html
    .replace(/<style[\s\S]*?<\/style>/gi, '')
    .replace(/<script[\s\S]*?<\/script>/gi, '')
    .replace(/<[^>]+>/g, ' ')
    .replace(/&nbsp;/g, ' ')
    .replace(/&lt;/g, '<')
    .replace(/&gt;/g, '>')
    .replace(/&amp;/g, '&')
    .replace(/\s+/g, ' ')
    .trim()
}

const mpRichNodes = computed(() => {
  if (!isHtml.value) return ''
  const cleaned = sanitizeHtmlForMp(content.value?.content || '')
  if (!cleaned) return ''
  return `<div style="color:#cbd5e1;line-height:1.85;word-break:break-word;">${cleaned}</div>`
})

const useMpRichText = computed(() => {
  if (!isHtml.value) return false
  const cleaned = sanitizeHtmlForMp(content.value?.content || '')
  if (!cleaned) return false
  if (cleaned.length > 8000) return false
  if (/<(table|iframe|svg|video|audio|form|canvas)\\b/i.test(cleaned)) return false
  return true
})

const mpPlainFallback = computed(() => {
  const text = htmlToPlainText(content.value?.content || '')
  if (text) return text
  const summary = htmlToPlainText(content.value?.summary || '')
  if (summary) return summary
  return '正文暂不可显示'
})
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

      <!-- HTML 正文：H5 使用 v-html 渲染 -->
      <!-- #ifdef H5 -->
      <view v-if="isHtml" class="html-body" v-html="content.content" />
      <!-- #endif -->

      <!-- 纯文本正文（非 HTML 内容，或小程序环境用 rich-text） -->
      <!-- #ifndef H5 -->
      <rich-text v-if="isHtml && useMpRichText && mpRichNodes" :nodes="mpRichNodes" class="rich-body" />
      <text v-else-if="isHtml" class="article-body selectable">{{ mpPlainFallback }}</text>
      <!-- #endif -->
      <text v-if="!isHtml" class="article-body selectable">{{ content.content || content.summary || '正文暂不可显示' }}</text>
    </view>
  </view>
</template>

<style scoped>
.page {
  background: #0b1121;
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
  background: #1e2d45;
  margin: 20rpx 0 30rpx;
}

.article-body {
  display: block;
  font-size: 29rpx;
  color: #cccccc;
  line-height: 1.85;
  white-space: pre-wrap;
}

.rich-body {
  font-size: 29rpx;
  color: #cccccc;
  line-height: 1.85;
}
</style>

<!-- HTML 渲染深层样式（覆盖晚报自带样式，适配深色主题） -->
<style>
/* #ifdef H5 */
.html-body {
  color: #cccccc;
  font-size: 28rpx;
  line-height: 1.85;
  word-break: break-word;
  overflow-x: hidden;
}

/* 覆盖晚报 HTML 内的白色背景和黑色文字 */
.html-body body,
.html-body .main-container,
.html-body table,
.html-body td,
.html-body th,
.html-body div,
.html-body p,
.html-body span {
  background-color: transparent !important;
  color: inherit !important;
  max-width: 100% !important;
}

.html-body h1, .html-body h2, .html-body h3 {
  color: #f5c518 !important;
  font-size: 1em !important;
  margin: 16px 0 8px !important;
}

.html-body table {
  width: 100% !important;
  border-collapse: collapse !important;
  font-size: 24rpx !important;
}

.html-body td, .html-body th {
  border: 1px solid #1e2d45 !important;
  padding: 8px !important;
  text-align: left !important;
}

.html-body tr:nth-child(even) td {
  background-color: rgba(255,255,255,0.03) !important;
}

.html-body a { color: #f5c518 !important; }

.html-body img { max-width: 100% !important; }
/* #endif */
</style>
