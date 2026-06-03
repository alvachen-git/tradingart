<script setup lang="ts">
import { ref, computed } from 'vue'
import { onLoad } from '@dcloudio/uni-app'
import { intelApi, type ReportDetail, type SafeStockMobileRender } from '../../api/index'
import { formatAiForMobile } from '../../utils/ai_mobile_formatter'

const content = ref<ReportDetail | null>(null)
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

function formatReportTitle(title: string) {
  return String(title || '')
    .replace(/期货商持仓晚报/g, '持仓晚报')
    .replace(/技术突破提醒/g, '技术形态提醒')
}

function formatChannelName(name: string) {
  return String(name || '')
    .replace(/交易信号/g, '突破提示')
    .replace(/期货商持仓/g, '持仓晚报')
}

// 判断是否为 HTML 内容
const isHtml = computed(() => {
  const c = content.value?.content || ''
  return c.trimStart().startsWith('<')
})

const safeStockRender = computed<SafeStockMobileRender | null>(() => {
  const render = content.value?.mobile_render
  if (content.value?.channel_code !== 'safe_stock_report' || render?.type !== 'safe_stock_report') return null
  return render
})

const shouldUseSafeStockLayout = computed(() => !!safeStockRender.value)

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
  const withBreaks = html
    .replace(/<style[\s\S]*?<\/style>/gi, '')
    .replace(/<script[\s\S]*?<\/script>/gi, '')
    .replace(/<br\s*\/?>/gi, '\n')
    .replace(/<\/?(p|div|section|article|h[1-6]|li|ul|ol|tr|table|blockquote)[^>]*>/gi, '\n')
    .replace(/<[^>]+>/g, ' ')
    .replace(/&nbsp;/g, ' ')
    .replace(/&lt;/g, '<')
    .replace(/&gt;/g, '>')
    .replace(/&amp;/g, '&')

  return withBreaks
    .split('\n')
    .map((line) => line.replace(/[ \t]+/g, ' ').trim())
    .filter((line, idx, arr) => line || (idx > 0 && arr[idx - 1] !== ''))
    .join('\n')
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
  // 线上内容较长时也优先使用 rich-text，避免过早降级成一整段纯文本。
  if (cleaned.length > 120000) return false
  return true
})

function normalizePlainText(raw: string): string {
  const { fullText } = formatAiForMobile(raw || '')
  return fullText.replace(/^【AI生成】\s*/, '').trim()
}

const mpPlainFallback = computed(() => {
  const text = normalizePlainText(htmlToPlainText(content.value?.content || ''))
  if (text) return text
  const summary = normalizePlainText(htmlToPlainText(content.value?.summary || ''))
  if (summary) return summary
  return '正文暂不可显示'
})

const plainBodyText = computed(() => {
  const raw = String(content.value?.content || content.value?.summary || '')
  return normalizePlainText(raw) || '正文暂不可显示'
})

function getField(row: Record<string, string>, key: string, fallback = '-') {
  const value = String(row?.[key] ?? '').trim()
  return value || fallback
}

function pctClass(raw: string) {
  const n = Number(String(raw || '').replace('%', ''))
  if (!Number.isFinite(n)) return ''
  if (n > 0) return 'pos'
  if (n < 0) return 'neg'
  return ''
}

function sectionCount(items?: Array<Record<string, string>>) {
  return Array.isArray(items) ? items.length : 0
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
        <view class="channel-badge">{{ formatChannelName(content.channel_name) }}</view>
        <text class="date-text">{{ formatDate(content.published_at) }}</text>
      </view>

      <text class="article-title">{{ formatReportTitle(content.title) }}</text>
      <view class="divider" />

      <view v-if="shouldUseSafeStockLayout && safeStockRender" class="safe-stock-body">
        <view class="safe-hero">
          <text class="safe-title">{{ safeStockRender.hero.title || '小爱选股晚报' }}</text>
          <view class="safe-meta-grid">
            <view class="safe-meta-item">
              <text class="safe-meta-label">交易日</text>
              <text class="safe-meta-value">{{ safeStockRender.hero.trade_date || '-' }}</text>
            </view>
            <view class="safe-meta-item">
              <text class="safe-meta-label">生成时间</text>
              <text class="safe-meta-value">{{ safeStockRender.hero.generated_at || '-' }}</text>
            </view>
          </view>
          <text v-if="safeStockRender.hero.market_note" class="safe-note">{{ safeStockRender.hero.market_note }}</text>
        </view>

        <view class="safe-section">
          <view class="section-head">
            <text class="section-title">资金回流</text>
            <text class="section-count">{{ sectionCount(safeStockRender.sectors) }} 个板块</text>
          </view>
          <view v-if="safeStockRender.sectors.length" class="sector-list">
            <view v-for="row in safeStockRender.sectors" :key="`sector-${getField(row, 'rank')}-${getField(row, 'sector')}`" class="sector-card">
              <view class="rank-pill">#{{ getField(row, 'rank') }}</view>
              <view class="sector-main">
                <view class="sector-top">
                  <text class="sector-name">{{ getField(row, 'sector') }}</text>
                  <text class="sector-type">{{ getField(row, 'type') }}</text>
                </view>
                <view class="metric-grid">
                  <view class="metric">
                    <text class="metric-label">分数</text>
                    <text class="metric-value">{{ getField(row, 'score') }}</text>
                  </view>
                  <view class="metric">
                    <text class="metric-label">资金改善</text>
                    <text class="metric-value">{{ getField(row, 'money_improve') }}</text>
                  </view>
                  <view class="metric">
                    <text class="metric-label">流入天数</text>
                    <text class="metric-value">{{ getField(row, 'inflow_days') }}</text>
                  </view>
                  <view class="metric">
                    <text class="metric-label">近窗涨幅</text>
                    <text class="metric-value" :class="pctClass(getField(row, 'recent_change'))">{{ getField(row, 'recent_change') }}</text>
                  </view>
                </view>
              </view>
            </view>
          </view>
          <text v-else class="empty-text">暂无资金回流板块。</text>
        </view>

        <view class="safe-section">
          <view class="section-head">
            <text class="section-title">可买标的</text>
            <text class="section-count">{{ sectionCount(safeStockRender.buys) }} 个</text>
          </view>
          <view v-if="safeStockRender.buys.length" class="stock-list">
            <view v-for="row in safeStockRender.buys" :key="`buy-${getField(row, 'symbol')}`" class="stock-card buy">
              <view class="stock-top">
                <view>
                  <text class="stock-symbol">{{ getField(row, 'symbol') }}</text>
                  <text class="stock-name">{{ getField(row, 'name') }}</text>
                </view>
                <view class="price-box">
                  <text class="price-label">价格</text>
                  <text class="price-value">{{ getField(row, 'price') }}</text>
                </view>
              </view>
              <view class="stock-tags">
                <text>{{ getField(row, 'sector') }}</text>
                <text>板块排名 {{ getField(row, 'sector_rank') }}</text>
              </view>
              <text class="stock-note">{{ getField(row, 'note', '暂无说明') }}</text>
            </view>
          </view>
          <text v-else class="empty-text">暂无可买标的。</text>
        </view>

        <view class="safe-section">
          <view class="section-head">
            <text class="section-title">观察标的</text>
            <text class="section-count">{{ sectionCount(safeStockRender.watches) }} 个</text>
          </view>
          <view v-if="safeStockRender.watches.length" class="stock-list">
            <view v-for="row in safeStockRender.watches" :key="`watch-${getField(row, 'symbol')}`" class="stock-card watch">
              <view class="stock-top">
                <view>
                  <text class="stock-symbol">{{ getField(row, 'symbol') }}</text>
                  <text class="stock-name">{{ getField(row, 'name') }}</text>
                </view>
                <view class="price-box">
                  <text class="price-label">价格</text>
                  <text class="price-value">{{ getField(row, 'price') }}</text>
                </view>
              </view>
              <view class="stock-tags">
                <text>{{ getField(row, 'sector') }}</text>
                <text>板块排名 {{ getField(row, 'sector_rank') }}</text>
              </view>
              <text class="stock-note">{{ getField(row, 'note', '暂无说明') }}</text>
            </view>
          </view>
          <text v-else class="empty-text">暂无观察标的。</text>
        </view>

        <view class="safe-section">
          <view class="section-head">
            <text class="section-title">已买跟踪</text>
            <text class="section-count">{{ sectionCount(safeStockRender.tracking) }} 个</text>
          </view>
          <view v-if="safeStockRender.tracking.length" class="tracking-list">
            <view v-for="row in safeStockRender.tracking" :key="`tracking-${getField(row, 'symbol')}`" class="tracking-card">
              <view class="stock-top">
                <view>
                  <text class="stock-symbol">{{ getField(row, 'symbol') }}</text>
                  <text class="stock-name">{{ getField(row, 'name') }}</text>
                </view>
                <text class="status-badge">{{ getField(row, 'status') }}</text>
              </view>
              <view class="metric-grid tracking-metrics">
                <view class="metric">
                  <text class="metric-label">持有天数</text>
                  <text class="metric-value">{{ getField(row, 'hold_days') }}</text>
                </view>
                <view class="metric">
                  <text class="metric-label">今日操作</text>
                  <text class="metric-value">{{ getField(row, 'action') }}</text>
                </view>
                <view class="metric">
                  <text class="metric-label">收益</text>
                  <text class="metric-value" :class="pctClass(getField(row, 'return_pct'))">{{ getField(row, 'return_pct') }}</text>
                </view>
              </view>
              <text class="stock-note">{{ getField(row, 'reason', '暂无原因') }}</text>
            </view>
          </view>
          <text v-else class="empty-text">暂无已买跟踪。</text>
        </view>
      </view>

      <!-- HTML 正文：H5 使用 v-html 渲染 -->
      <!-- #ifdef H5 -->
      <view v-if="isHtml && !shouldUseSafeStockLayout" class="html-body" v-html="content.content" />
      <!-- #endif -->

      <!-- 纯文本正文（非 HTML 内容，或小程序环境用 rich-text） -->
      <!-- #ifndef H5 -->
      <rich-text v-if="isHtml && !shouldUseSafeStockLayout && useMpRichText && mpRichNodes" :nodes="mpRichNodes" class="rich-body" />
      <text v-else-if="isHtml && !shouldUseSafeStockLayout" class="article-body selectable">{{ mpPlainFallback }}</text>
      <!-- #endif -->
      <text v-if="!isHtml && !shouldUseSafeStockLayout" class="article-body selectable">{{ plainBodyText }}</text>
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

.safe-stock-body {
  display: flex;
  flex-direction: column;
  gap: 28rpx;
}

.safe-hero {
  border: 1px solid #1e2d45;
  border-radius: 16rpx;
  background: #131c2e;
  padding: 22rpx;
}

.safe-title {
  display: block;
  color: #ecf3ff;
  font-size: 34rpx;
  font-weight: 700;
  line-height: 1.35;
  margin-bottom: 18rpx;
}

.safe-meta-grid {
  display: grid;
  grid-template-columns: repeat(2, minmax(0, 1fr));
  gap: 12rpx;
  margin-bottom: 16rpx;
}

.safe-meta-item {
  border: 1px solid rgba(62, 90, 140, 0.55);
  border-radius: 12rpx;
  padding: 12rpx;
  background: rgba(8, 18, 35, 0.45);
}

.safe-meta-label,
.metric-label,
.price-label {
  display: block;
  color: #7f8fa8;
  font-size: 21rpx;
  margin-bottom: 6rpx;
}

.safe-meta-value,
.metric-value,
.price-value {
  display: block;
  color: #e7eef8;
  font-size: 25rpx;
  font-weight: 600;
  font-variant-numeric: tabular-nums;
}

.safe-note {
  display: block;
  color: #c9d7ee;
  font-size: 28rpx;
  line-height: 1.75;
}

.safe-section {
  display: flex;
  flex-direction: column;
  gap: 14rpx;
}

.section-head {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 16rpx;
}

.section-title {
  color: #f5c518;
  font-size: 34rpx;
  font-weight: 700;
}

.section-count {
  color: #7f8fa8;
  font-size: 22rpx;
}

.sector-list,
.stock-list,
.tracking-list {
  display: flex;
  flex-direction: column;
  gap: 14rpx;
}

.sector-card,
.stock-card,
.tracking-card {
  border: 1px solid #1e2d45;
  border-radius: 16rpx;
  background: #131c2e;
  padding: 18rpx;
}

.sector-card {
  display: flex;
  gap: 16rpx;
}

.rank-pill {
  width: 54rpx;
  height: 54rpx;
  border-radius: 16rpx;
  display: flex;
  align-items: center;
  justify-content: center;
  flex-shrink: 0;
  color: #f5c518;
  background: rgba(245, 197, 24, 0.12);
  border: 1px solid rgba(245, 197, 24, 0.3);
  font-size: 22rpx;
  font-weight: 700;
}

.sector-main {
  flex: 1;
  min-width: 0;
}

.sector-top,
.stock-top {
  display: flex;
  justify-content: space-between;
  gap: 14rpx;
  align-items: flex-start;
  margin-bottom: 14rpx;
}

.sector-name,
.stock-symbol {
  display: block;
  color: #ecf3ff;
  font-size: 28rpx;
  font-weight: 700;
  line-height: 1.35;
}

.sector-type,
.status-badge {
  color: #9bc3ff;
  font-size: 22rpx;
  padding: 6rpx 12rpx;
  border-radius: 999rpx;
  background: rgba(59, 130, 246, 0.14);
  border: 1px solid rgba(96, 165, 250, 0.3);
  white-space: nowrap;
}

.metric-grid {
  display: grid;
  grid-template-columns: repeat(2, minmax(0, 1fr));
  gap: 10rpx;
}

.metric {
  min-width: 0;
  border-radius: 12rpx;
  background: rgba(8, 18, 35, 0.55);
  padding: 12rpx;
}

.stock-card.buy {
  border-color: rgba(245, 197, 24, 0.36);
}

.stock-name {
  display: block;
  color: #c9d7ee;
  font-size: 25rpx;
  line-height: 1.35;
  margin-top: 4rpx;
}

.price-box {
  min-width: 120rpx;
  text-align: right;
}

.stock-tags {
  display: flex;
  flex-wrap: wrap;
  gap: 10rpx;
  margin-bottom: 14rpx;
}

.stock-tags text {
  color: #9ab0cf;
  font-size: 22rpx;
  padding: 6rpx 10rpx;
  border-radius: 999rpx;
  background: rgba(148, 163, 184, 0.12);
}

.stock-note {
  display: block;
  color: #cbd5e1;
  font-size: 26rpx;
  line-height: 1.75;
  word-break: break-word;
}

.tracking-metrics {
  grid-template-columns: repeat(3, minmax(0, 1fr));
  margin-bottom: 14rpx;
}

.pos {
  color: #e84040;
}

.neg {
  color: #22c55e;
}

.empty-text {
  color: #7f8fa8;
  font-size: 26rpx;
  padding: 20rpx;
  border: 1px solid #1e2d45;
  border-radius: 14rpx;
  background: #131c2e;
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
