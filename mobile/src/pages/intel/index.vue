<script setup lang="ts">
import { ref, onMounted, computed, watch, nextTick, getCurrentInstance } from 'vue'
import { onShow, onReachBottom, onPullDownRefresh } from '@dcloudio/uni-app'
import {
  intelApi,
  aiSimApi,
  type ReportItem,
  type AiOverviewPayload,
  type AiReviewPayload,
} from '../../api/index'
import { formatAiForMobile } from '../../utils/ai_mobile_formatter'
import { useAuthStore } from '../../store/auth'
import BottomNav from '../../components/BottomNav.vue'

const auth = useAuthStore()

type MainTab = 'intel' | 'ai'
const activeMainTab = ref<MainTab>('intel')

const channels = [
  { code: '', label: '全部' },
  { code: 'daily_report', label: '复盘晚报' },
  { code: 'trade_signal', label: '盘面观察' },
  { code: 'fund_flow_report', label: '资金流晚报' },
  { code: 'expiry_option_radar', label: '末日晚报' },
  { code: 'broker_position_report', label: '持仓晚报' },
]

// ── 情报列表 ───────────────────────────────────────────────
const activeChannel = ref('')
const reports = ref<ReportItem[]>([])
const page = ref(1)
const hasMore = ref(true)
const reportLoading = ref(false)

// ── AI日记 ─────────────────────────────────────────────────
const AI_OVERVIEW_CACHE_KEY = 'intel_ai_overview_cache_v1'
const AI_OVERVIEW_TTL_MS = 20 * 60 * 1000
const vm = getCurrentInstance()

const aiOverview = ref<AiOverviewPayload | null>(null)
const aiReview = ref<AiReviewPayload | null>(null)
const aiLoading = ref(false)
const aiSyncing = ref(false)
const aiReviewLoading = ref(false)
const aiError = ref('')
const aiCacheTs = ref(0)
const selectedReviewDate = ref('')

const currentChannelLabel = computed(() => {
  const cur = channels.find((c) => c.code === activeChannel.value)
  return cur?.label || '全部'
})

const reviewDates = computed(() => aiOverview.value?.review_dates || [])
const selectedReviewLabel = computed(() => {
  return selectedReviewDate.value ? formatTradeDate(selectedReviewDate.value) : '选择复盘日'
})

const snapshot = computed(() => aiOverview.value?.snapshot || {})
const watchlist = computed(() => {
  const fromReview = aiReview.value?.next_watchlist
  if (Array.isArray(fromReview) && fromReview.length) return fromReview
  return aiOverview.value?.watchlist || []
})

const displayPositions = computed(() => (aiOverview.value?.positions || []).slice(0, 8))
const displayTrades = computed(() => (aiOverview.value?.trades || []).slice(0, 10))

const reviewSummaryText = computed(() => mdToText(aiReview.value?.summary_md || '', '暂无复盘数据。'))
const reviewBuysText = computed(() => mdToText(aiReview.value?.buys_md || '', ''))
const reviewSellsText = computed(() => mdToText(aiReview.value?.sells_md || '', ''))
const reviewRiskText = computed(() => mdToText(aiReview.value?.risk_md || '', ''))

const navChart = computed(() => {
  const rows = aiOverview.value?.nav_series || []
  if (rows.length < 2) return null

  const width = 680
  const height = 220
  const padX = 20
  const padY = 16
  const innerW = width - padX * 2
  const innerH = height - padY * 2
  const initialCap = toNum(snapshot.value.initial_capital, 1_000_000)

  const values = rows.map((r: any) => {
    let nav = toNum(r.nav_norm, NaN)
    if (!Number.isFinite(nav) || nav <= 0) {
      const rawNav = toNum(r.nav, 0)
      nav = initialCap > 0 ? rawNav / initialCap : 0
    }
    return {
      td: String(r.trade_date || ''),
      nav,
      hs: toNum(r.bench_hs300, 1),
      zz: toNum(r.bench_zz1000, 1),
    }
  })

  const all = values.flatMap((v) => [v.nav, v.hs, v.zz]).filter((v) => Number.isFinite(v))
  if (!all.length) return null

  let minV = Math.min(...all)
  let maxV = Math.max(...all)
  if (!Number.isFinite(minV) || !Number.isFinite(maxV)) return null
  if (maxV - minV < 0.02) {
    minV -= 0.01
    maxV += 0.01
  }

  const toPoint = (idx: number, v: number) => {
    const x = padX + (values.length <= 1 ? 0 : (idx * innerW) / (values.length - 1))
    const y = padY + ((maxV - v) / (maxV - minV)) * innerH
    return `${x.toFixed(1)},${y.toFixed(1)}`
  }

  return {
    width,
    height,
    navPoints: values.map((v, i) => toPoint(i, v.nav)).join(' '),
    hsPoints: values.map((v, i) => toPoint(i, v.hs)).join(' '),
    zzPoints: values.map((v, i) => toPoint(i, v.zz)).join(' '),
    min: minV,
    max: maxV,
    start: values[0].td,
    end: values[values.length - 1].td,
    latestNav: values[values.length - 1].nav,
    latestHs: values[values.length - 1].hs,
    latestZz: values[values.length - 1].zz,
  }
})

function parseChartPoints(points: string): Array<{ x: number; y: number }> {
  return String(points || '')
    .split(' ')
    .map((pair) => pair.trim())
    .filter(Boolean)
    .map((pair) => {
      const [x, y] = pair.split(',')
      return { x: Number(x), y: Number(y) }
    })
    .filter((p) => Number.isFinite(p.x) && Number.isFinite(p.y))
}

function drawMpPolyline(ctx: any, points: Array<{ x: number; y: number }>, color: string, width: number) {
  if (!points.length) return
  ctx.beginPath()
  ctx.setStrokeStyle(color)
  ctx.setLineWidth(width)
  points.forEach((p, idx) => {
    if (idx === 0) ctx.moveTo(p.x, p.y)
    else ctx.lineTo(p.x, p.y)
  })
  ctx.stroke()
}

function drawMpNavChart() {
  // #ifndef MP-WEIXIN
  return
  // #endif
  // #ifdef MP-WEIXIN
  const chart = navChart.value
  if (!chart || activeMainTab.value !== 'ai') return
  const ctx = uni.createCanvasContext('aiNavCanvas', vm)
  const w = chart!.width
  const h = chart!.height

  ctx.clearRect(0, 0, w, h)

  ctx.setStrokeStyle('rgba(74, 106, 159, 0.35)')
  ctx.setLineWidth(1)
  for (let i = 0; i <= 3; i += 1) {
    const y = 16 + ((h - 32) * i) / 3
    ctx.beginPath()
    ctx.moveTo(20, y)
    ctx.lineTo(w - 20, y)
    ctx.stroke()
  }

  drawMpPolyline(ctx, parseChartPoints(chart!.navPoints), '#f5c518', 3)
  drawMpPolyline(ctx, parseChartPoints(chart!.hsPoints), '#3cc8ff', 2)
  drawMpPolyline(ctx, parseChartPoints(chart!.zzPoints), '#2ecb88', 2)
  ctx.draw()
  // #endif
}

onShow(async () => {
  await auth.waitForBootstrap()
  if (!auth.isLoggedIn) uni.reLaunch({ url: '/pages/login/index' })
})

onMounted(() => {
  if (auth.isLoggedIn) {
    loadReports(true)
  }
})

onReachBottom(async () => {
  if (activeMainTab.value !== 'intel') return
  await loadMore()
})

onPullDownRefresh(async () => {
  try {
    if (activeMainTab.value === 'intel') {
      await loadReports(true)
    } else {
      await ensureAiOverview(true)
    }
  } finally {
    uni.stopPullDownRefresh()
  }
})

async function toggleAiTab() {
  if (activeMainTab.value === 'ai') {
    activeMainTab.value = 'intel'
    return
  }
  activeMainTab.value = 'ai'
  await ensureAiOverview(false)
}

async function loadReports(reset = false) {
  if (reset) {
    page.value = 1
    reports.value = []
    hasMore.value = true
  }
  if (reportLoading.value || !hasMore.value) return

  reportLoading.value = true
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
    page.value += 1
  } catch (e: any) {
    uni.showToast({ title: e.message || '加载失败', icon: 'none' })
  } finally {
    reportLoading.value = false
  }
}

async function loadMore() {
  if (!hasMore.value || reportLoading.value) return
  await loadReports()
}

async function switchChannel(code: string) {
  if (activeChannel.value === code) return
  activeChannel.value = code
  await loadReports(true)
}

function openChannelPicker() {
  if (activeMainTab.value !== 'intel') {
    activeMainTab.value = 'intel'
  }
  uni.showActionSheet({
    itemList: channels.map((c) => c.label),
    success: async (res) => {
      const idx = Number(res.tapIndex)
      if (Number.isNaN(idx) || idx < 0 || idx >= channels.length) return
      await switchChannel(channels[idx].code)
    },
  })
}

function readAiCache(): { ts: number; data: AiOverviewPayload } | null {
  try {
    const raw = uni.getStorageSync(AI_OVERVIEW_CACHE_KEY)
    if (!raw) return null
    const parsed = typeof raw === 'string' ? JSON.parse(raw) : raw
    if (!parsed || typeof parsed !== 'object') return null
    const ts = Number(parsed.ts || 0)
    if (!ts || !parsed.data) return null
    return { ts, data: parsed.data as AiOverviewPayload }
  } catch {
    return null
  }
}

function writeAiCache(payload: AiOverviewPayload) {
  const entry = { ts: Date.now(), data: payload }
  aiCacheTs.value = entry.ts
  try {
    uni.setStorageSync(AI_OVERVIEW_CACHE_KEY, JSON.stringify(entry))
  } catch {
    // ignore storage failure
  }
}

function isCacheFresh(ts: number): boolean {
  return ts > 0 && Date.now() - ts < AI_OVERVIEW_TTL_MS
}

function applyOverview(payload: AiOverviewPayload) {
  aiOverview.value = payload
  const latest = payload.latest_review
  if (latest && latest.has_data !== false) {
    aiReview.value = latest
    selectedReviewDate.value = String(latest.trade_date || payload.review_dates?.[0] || '')
  } else {
    aiReview.value = null
    selectedReviewDate.value = String(payload.review_dates?.[0] || '')
  }
}

async function fetchAiOverview(silent = false) {
  if (aiLoading.value || aiSyncing.value) return
  if (silent) {
    aiSyncing.value = true
  } else {
    aiLoading.value = true
    aiError.value = ''
  }

  try {
    const payload = await aiSimApi.overview({
      nav_days: 120,
      trades_days: 20,
      positions_limit: 24,
      review_limit: 260,
    })
    applyOverview(payload)
    writeAiCache(payload)
  } catch (e: any) {
    if (!silent) {
      aiError.value = e.message || '加载失败'
      uni.showToast({ title: aiError.value, icon: 'none' })
    }
  } finally {
    if (silent) {
      aiSyncing.value = false
    } else {
      aiLoading.value = false
    }
  }
}

async function ensureAiOverview(force = false) {
  if (force) {
    await fetchAiOverview(false)
    return
  }

  if (aiOverview.value && isCacheFresh(aiCacheTs.value)) {
    void fetchAiOverview(true)
    return
  }

  const cached = readAiCache()
  if (cached && isCacheFresh(cached.ts)) {
    aiCacheTs.value = cached.ts
    applyOverview(cached.data)
    void fetchAiOverview(true)
    return
  }

  await fetchAiOverview(false)
}

function openReviewPicker() {
  if (!reviewDates.value.length) return
  uni.showActionSheet({
    itemList: reviewDates.value.map((d) => formatTradeDate(d)),
    success: async (res) => {
      const idx = Number(res.tapIndex)
      if (Number.isNaN(idx) || idx < 0 || idx >= reviewDates.value.length) return
      const target = reviewDates.value[idx]
      if (!target) return
      if (selectedReviewDate.value === target && aiReview.value?.trade_date === target) return
      selectedReviewDate.value = target
      await loadAiReview(target)
    },
  })
}

async function loadAiReview(tradeDate: string) {
  if (!tradeDate || aiReviewLoading.value) return
  aiReviewLoading.value = true
  try {
    const payload = await aiSimApi.review(tradeDate)
    aiReview.value = payload
  } catch (e: any) {
    uni.showToast({ title: e.message || '复盘加载失败', icon: 'none' })
  } finally {
    aiReviewLoading.value = false
  }
}

function toDetail(id: number) {
  uni.navigateTo({ url: `/pages/intel/detail?id=${id}` })
}

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
    .replace(/交易信号/g, '盘面观察')
    .replace(/期货商持仓/g, '持仓晚报')
}

function formatTradeDate(raw: string) {
  const digits = String(raw || '').replace(/\D/g, '')
  if (digits.length !== 8) return raw || '-'
  return `${digits.slice(0, 4)}-${digits.slice(4, 6)}-${digits.slice(6, 8)}`
}

function stripHtml(html: string): string {
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
    .slice(0, 120)
}

function mdToText(markdown: string, fallback = '暂无数据'): string {
  const { fullText } = formatAiForMobile(markdown || '')
  const cleaned = fullText.replace(/^【AI生成】\s*/, '').trim()
  return cleaned || fallback
}

function toNum(v: any, fallback = 0): number {
  const n = Number(v)
  return Number.isFinite(n) ? n : fallback
}

function fmtMoney(v: any): string {
  const n = toNum(v, 0)
  const abs = Math.abs(n)
  if (abs >= 1e8) return `${(n / 1e8).toFixed(2)}亿`
  if (abs >= 1e4) return `${(n / 1e4).toFixed(2)}万`
  return n.toLocaleString('zh-CN', { maximumFractionDigits: 2 })
}

function fmtPct(v: any): string {
  const n = toNum(v, 0)
  return `${n >= 0 ? '+' : ''}${(n * 100).toFixed(2)}%`
}

function toneClass(v: any): string {
  const n = toNum(v, 0)
  if (n > 0) return 'pos'
  if (n < 0) return 'neg'
  return 'flat'
}

watch(
  [navChart, activeMainTab],
  async ([chart, tab]) => {
    // #ifndef MP-WEIXIN
    return
    // #endif
    // #ifdef MP-WEIXIN
    if (!chart || tab !== 'ai') return
    await nextTick()
    drawMpNavChart()
    // #endif
  },
  { immediate: true },
)
</script>

<template>
  <view class="page">
    <view class="top-actions">
      <view class="filter-trigger" :class="{ active: activeMainTab === 'intel' }" @tap="openChannelPicker">
        <text class="filter-label">{{ currentChannelLabel }}</text>
        <text class="filter-arrow">▾</text>
      </view>
      <view class="ai-tab" :class="{ active: activeMainTab === 'ai' }" @tap="toggleAiTab">
        AI日记
      </view>
    </view>

    <view v-if="activeMainTab === 'intel'">
      <view class="list">
        <view
          v-for="item in reports"
          :key="item.id"
          class="report-card"
          @tap="toDetail(item.id)"
        >
          <view class="card-top">
            <view class="channel-badge">{{ formatChannelName(item.channel_name) }}</view>
            <text class="date-text">{{ formatDate(item.published_at) }}</text>
          </view>
          <text class="card-title">{{ formatReportTitle(item.title) }}</text>
          <text class="card-summary">{{ stripHtml(item.summary) }}</text>
        </view>

        <view v-if="reportLoading" class="center-tip">
          <text class="muted-text">加载中...</text>
        </view>

        <view v-else-if="!hasMore && reports.length > 0" class="center-tip">
          <text class="muted-text">— 已全部加载 —</text>
        </view>

        <view v-else-if="!reportLoading && reports.length === 0" class="empty-state">
          <text class="empty-icon">◉</text>
          <text class="muted-text">暂无情报，稍后再来</text>
        </view>
      </view>
    </view>

    <view v-else class="ai-wrap">
      <view v-if="aiLoading" class="center-tip">
        <text class="muted-text">AI日记加载中...</text>
      </view>

      <view v-else-if="aiError && !aiOverview" class="center-tip">
        <text class="muted-text">{{ aiError }}</text>
      </view>

      <view v-else-if="!aiOverview || !aiOverview.has_data" class="center-tip">
        <text class="muted-text">暂无模拟投资数据</text>
      </view>

      <view v-else>
        <view class="ai-meta">
          <text class="meta-text">千问模型搭配交易汇训练</text>
          <text class="meta-text">更新：{{ aiOverview.fetched_at || '-' }}</text>
          <text v-if="aiSyncing" class="meta-text">同步中...</text>
        </view>

        <view class="kpi-grid">
          <view class="kpi-card">
            <text class="kpi-label">总资金</text>
            <text class="kpi-value">{{ fmtMoney(snapshot.nav) }}</text>
          </view>
          <view class="kpi-card" :class="toneClass(snapshot.daily_return)">
            <text class="kpi-label">当日收益</text>
            <text class="kpi-value">{{ fmtPct(snapshot.daily_return) }}</text>
          </view>
          <view class="kpi-card" :class="toneClass(snapshot.cum_return)">
            <text class="kpi-label">累计收益</text>
            <text class="kpi-value">{{ fmtPct(snapshot.cum_return) }}</text>
          </view>
          <view class="kpi-card">
            <text class="kpi-label">最大回撤</text>
            <text class="kpi-value neg">{{ fmtPct(snapshot.max_drawdown) }}</text>
          </view>
          <view class="kpi-card">
            <text class="kpi-label">现金</text>
            <text class="kpi-value">{{ fmtMoney(snapshot.cash) }}</text>
          </view>
          <view class="kpi-card">
            <text class="kpi-label">持仓市值</text>
            <text class="kpi-value">{{ fmtMoney(snapshot.position_value) }}</text>
          </view>
        </view>

        <view class="ai-card">
          <view class="panel-head">
            <text class="panel-title">复盘日记</text>
            <view class="picker-trigger" @tap="openReviewPicker">
              <text class="picker-label">{{ selectedReviewLabel }}</text>
              <text class="picker-arrow">▾</text>
            </view>
          </view>

          <view v-if="aiReviewLoading" class="center-tip">
            <text class="muted-text">复盘加载中...</text>
          </view>
          <view v-else>
            <view class="md-block">
              <text class="md-title">核心总结</text>
              <text class="md-text">{{ reviewSummaryText }}</text>
            </view>
            <view v-if="reviewBuysText" class="md-block">
              <text class="md-title">买入动作</text>
              <text class="md-text">{{ reviewBuysText }}</text>
            </view>
            <view v-if="reviewSellsText" class="md-block">
              <text class="md-title">卖出动作</text>
              <text class="md-text">{{ reviewSellsText }}</text>
            </view>
            <view v-if="reviewRiskText" class="md-block">
              <text class="md-title">风险提示</text>
              <text class="md-text">{{ reviewRiskText }}</text>
            </view>
          </view>
        </view>

        <view class="ai-card">
          <view class="panel-head">
            <text class="panel-title">净值与基准曲线</text>
            <text class="muted-text" v-if="navChart">{{ formatTradeDate(navChart.start) }} ~ {{ formatTradeDate(navChart.end) }}</text>
          </view>

          <view v-if="navChart" class="chart-wrap">
            <!-- #ifdef MP-WEIXIN -->
            <canvas
              canvas-id="aiNavCanvas"
              class="chart-canvas"
              :width="navChart.width"
              :height="navChart.height"
            />
            <!-- #endif -->
            <!-- #ifndef MP-WEIXIN -->
            <svg class="chart-svg" :viewBox="`0 0 ${navChart.width} ${navChart.height}`" :width="navChart.width" :height="navChart.height" preserveAspectRatio="none">
              <polyline :points="navChart.navPoints" fill="none" stroke="#f5c518" stroke-width="3" stroke-linejoin="round" stroke-linecap="round" />
              <polyline :points="navChart.hsPoints" fill="none" stroke="#3cc8ff" stroke-width="2" stroke-linejoin="round" stroke-linecap="round" />
              <polyline :points="navChart.zzPoints" fill="none" stroke="#2ecb88" stroke-width="2" stroke-linejoin="round" stroke-linecap="round" />
            </svg>
            <!-- #endif -->
            <view class="chart-legend">
              <text class="legend-item">组合 {{ navChart.latestNav.toFixed(3) }}</text>
              <text class="legend-item">沪深300 {{ navChart.latestHs.toFixed(3) }}</text>
              <text class="legend-item">中证1000 {{ navChart.latestZz.toFixed(3) }}</text>
            </view>
          </view>
          <view v-else class="center-tip">
            <text class="muted-text">暂无净值曲线数据</text>
          </view>
        </view>

        <view class="ai-card">
          <text class="panel-title">持仓明细</text>
          <view v-if="displayPositions.length">
            <view v-for="(row, idx) in displayPositions" :key="`${row.symbol}-${idx}`" class="table-row">
              <view class="left-col">
                <text class="row-title">{{ row.symbol }} {{ row.name }}</text>
                <text class="muted-text">权重 {{ (toNum(row.weight, 0) * 100).toFixed(2) }}%</text>
              </view>
              <view class="right-col">
                <text class="row-title">{{ fmtMoney(row.market_value) }}</text>
                <text class="muted-text" :class="toneClass(row.unrealized_pnl)">浮盈亏 {{ fmtMoney(row.unrealized_pnl) }}</text>
              </view>
            </view>
          </view>
          <view v-else class="center-tip">
            <text class="muted-text">暂无持仓明细</text>
          </view>
        </view>

        <view class="ai-card">
          <text class="panel-title">最近交易</text>
          <view v-if="displayTrades.length">
            <view v-for="(row, idx) in displayTrades" :key="`${row.trade_date}-${row.symbol}-${idx}`" class="table-row">
              <view class="left-col">
                <text class="row-title">{{ formatTradeDate(row.trade_date) }} {{ row.symbol }}</text>
                <text class="muted-text">{{ row.side === 'buy' ? '买入' : row.side === 'sell' ? '卖出' : row.side }}</text>
              </view>
              <view class="right-col">
                <text class="row-title">{{ fmtMoney(row.amount) }}</text>
                <text class="muted-text">数量 {{ toNum(row.quantity, 0) }}</text>
              </view>
            </view>
          </view>
          <view v-else class="center-tip">
            <text class="muted-text">暂无交易记录</text>
          </view>
        </view>

        <view class="ai-card">
          <text class="panel-title">次日观察列表</text>
          <view v-if="watchlist.length" class="watch-grid">
            <view v-for="(item, idx) in watchlist.slice(0, 8)" :key="`${item.symbol || 'watch'}-${idx}`" class="watch-item">
              <text class="watch-symbol">{{ item.symbol || '-' }}</text>
              <text class="watch-name">{{ item.name || '未命名' }}</text>
              <text class="watch-score">评分 {{ toNum(item.score, 0).toFixed(1) }}</text>
            </view>
          </view>
          <view v-else class="center-tip">
            <text class="muted-text">暂无观察标的</text>
          </view>
        </view>
      </view>
    </view>

    <view style="height: 120rpx;" />
    <BottomNav active="intel" />
  </view>
</template>

<style scoped>
.page {
  background: #0b1121;
  min-height: 100vh;
}

.top-actions {
  display: flex;
  gap: 14rpx;
  align-items: center;
  padding: 16rpx 24rpx;
  border-bottom: 1px solid #162035;
  position: sticky;
  top: 0;
  z-index: 12;
  background: #0b1121;
}

.filter-trigger {
  display: inline-flex;
  align-items: center;
  gap: 8rpx;
  padding: 12rpx 24rpx;
  border-radius: 28rpx;
  background: #131c2e;
  border: 1px solid #1e2d45;
  flex-shrink: 0;
}

.filter-label {
  color: #9aa8bf;
  font-size: 26rpx;
  font-weight: 500;
}

.filter-arrow {
  color: #9aa8bf;
  font-size: 24rpx;
}

.filter-trigger.active {
  background: rgba(245, 197, 24, 0.15);
  border-color: rgba(245, 197, 24, 0.4);
}

.filter-trigger.active .filter-label,
.filter-trigger.active .filter-arrow {
  color: #f5c518;
  font-weight: 600;
}

.ai-tab {
  padding: 12rpx 24rpx;
  border-radius: 28rpx;
  border: 1px solid #1e2d45;
  background: #131c2e;
  color: #9aa8bf;
  font-size: 26rpx;
  font-weight: 500;
}

.ai-tab.active {
  background: rgba(83, 166, 255, 0.12);
  border-color: rgba(83, 166, 255, 0.45);
  color: #8ec8ff;
  font-weight: 600;
}

.list {
  padding: 16rpx 24rpx 12rpx;
}

.report-card {
  background: #131c2e;
  border: 1px solid #1e2d45;
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

.date-text {
  font-size: 22rpx;
  color: #555555;
}

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

.ai-wrap {
  padding: 12rpx 24rpx;
}

.ai-meta {
  display: flex;
  justify-content: space-between;
  margin-bottom: 12rpx;
}

.meta-text {
  color: #66768f;
  font-size: 22rpx;
}

.kpi-grid {
  display: grid;
  grid-template-columns: repeat(2, minmax(0, 1fr));
  gap: 12rpx;
  margin-bottom: 14rpx;
}

.kpi-card {
  border: 1px solid #1e2d45;
  border-radius: 14rpx;
  background: #131c2e;
  padding: 16rpx;
}

.kpi-label {
  display: block;
  color: #7f8fa8;
  font-size: 22rpx;
  margin-bottom: 8rpx;
}

.kpi-value {
  display: block;
  color: #e7eef8;
  font-size: 30rpx;
  font-weight: 600;
}

.kpi-card.pos .kpi-value,
.pos {
  color: #e84040;
}

.kpi-card.neg .kpi-value,
.neg {
  color: #22c55e;
}

.flat {
  color: #e7eef8;
}

.ai-card {
  border: 1px solid #1e2d45;
  border-radius: 16rpx;
  background: #131c2e;
  padding: 18rpx;
  margin-bottom: 14rpx;
}

.panel-head {
  display: flex;
  justify-content: space-between;
  align-items: center;
  margin-bottom: 10rpx;
  gap: 10rpx;
}

.panel-title {
  display: block;
  color: #ecf3ff;
  font-size: 30rpx;
  font-weight: 600;
  margin-bottom: 10rpx;
}

.picker-trigger {
  display: inline-flex;
  align-items: center;
  gap: 8rpx;
  border: 1px solid #25406a;
  border-radius: 20rpx;
  padding: 6rpx 14rpx;
  background: #102038;
}

.picker-label {
  color: #9bc3ff;
  font-size: 22rpx;
}

.picker-arrow {
  color: #9bc3ff;
  font-size: 20rpx;
}

.md-block {
  margin-bottom: 10rpx;
}

.md-title {
  display: block;
  color: #f5c518;
  font-size: 24rpx;
  margin-bottom: 6rpx;
}

.md-text {
  display: block;
  color: #c9d7ee;
  font-size: 26rpx;
  line-height: 1.65;
  white-space: pre-wrap;
}

.chart-wrap {
  border: 1px solid rgba(62, 90, 140, 0.5);
  border-radius: 12rpx;
  background: rgba(8, 18, 35, 0.65);
  padding: 10rpx;
}

.chart-svg {
  width: 100%;
  height: 220rpx;
}

.chart-canvas {
  width: 100%;
  height: 220rpx;
  display: block;
}

.chart-legend {
  display: flex;
  flex-wrap: wrap;
  gap: 14rpx;
  margin-top: 8rpx;
}

.legend-item {
  color: #9ab0cf;
  font-size: 22rpx;
}

.table-row {
  display: flex;
  justify-content: space-between;
  gap: 14rpx;
  padding: 12rpx 0;
  border-bottom: 1px solid rgba(62, 90, 140, 0.35);
}

.table-row:last-child {
  border-bottom: none;
}

.left-col,
.right-col {
  display: flex;
  flex-direction: column;
  gap: 4rpx;
}

.right-col {
  align-items: flex-end;
}

.row-title {
  color: #ecf3ff;
  font-size: 25rpx;
  font-weight: 600;
}

.watch-grid {
  display: grid;
  grid-template-columns: repeat(2, minmax(0, 1fr));
  gap: 10rpx;
}

.watch-item {
  border: 1px solid rgba(62, 90, 140, 0.45);
  border-radius: 12rpx;
  background: rgba(12, 24, 45, 0.9);
  padding: 10rpx;
}

.watch-symbol {
  display: block;
  color: #f5c518;
  font-size: 24rpx;
  font-weight: 600;
}

.watch-name {
  display: block;
  color: #cddaf0;
  font-size: 23rpx;
  margin-top: 2rpx;
}

.watch-score {
  display: block;
  color: #86a6d2;
  font-size: 22rpx;
  margin-top: 6rpx;
}

.center-tip {
  text-align: center;
  padding: 38rpx 0;
}

.muted-text {
  color: #6f8099;
  font-size: 24rpx;
}

.empty-state {
  display: flex;
  flex-direction: column;
  align-items: center;
  padding: 120rpx 0;
  gap: 24rpx;
}

.empty-icon {
  font-size: 80rpx;
  color: #333333;
}
</style>
