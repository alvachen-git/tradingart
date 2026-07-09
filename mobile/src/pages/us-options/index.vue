<script setup lang="ts">
import { computed, ref } from 'vue'
import { onMounted } from 'vue'
import { onPullDownRefresh, onShareAppMessage, onShareTimeline, onShow } from '@dcloudio/uni-app'
import {
  usOptionsApi,
  type UsOptionAnomaliesPayload,
  type UsOptionDefensePayload,
  type UsOptionOverviewPayload,
  type UsOptionProduct,
  type UsOptionSurfacePayload,
} from '../../api/index'
import { useAuthStore } from '../../store/auth'
import BottomNav from '../../components/BottomNav.vue'

type TabKey = 'overview' | 'surface' | 'defense' | 'anomalies'

type ChartPointInput = {
  x: string
  y: number | null | undefined
}

type ChartLineInput = {
  label: string
  color: string
  points: ChartPointInput[]
}

type ChartSegment = {
  left: number
  top: number
  width: number
  angle: number
  color: string
}

type ChartNode = {
  left: number
  top: number
  color: string
}

type MiniChart = {
  hasData: boolean
  width: number
  height: number
  lines: Array<{ label: string; color: string }>
  segments: ChartSegment[]
  nodes: ChartNode[]
  xLabels: Array<{ left: number; label: string }>
  yLabels: Array<{ top: number; label: string }>
}

const auth = useAuthStore()
const activeTab = ref<TabKey>('overview')
const products = ref<UsOptionProduct[]>([])
const selectedSymbol = ref('SPY')
const overview = ref<UsOptionOverviewPayload | null>(null)
const surface = ref<UsOptionSurfacePayload | null>(null)
const defense = ref<UsOptionDefensePayload | null>(null)
const anomalies = ref<UsOptionAnomaliesPayload | null>(null)
const loading = ref(false)
const productLoading = ref(false)
const surfaceLoading = ref(false)
const defenseLoading = ref(false)
const anomaliesLoading = ref(false)
const showPicker = ref(false)
const searchText = ref('')

const tabs: Array<{ key: TabKey; label: string }> = [
  { key: 'overview', label: '概览' },
  { key: 'surface', label: '波动率' },
  { key: 'defense', label: '持仓防线' },
  { key: 'anomalies', label: '异动观察' },
]

onShareAppMessage(() => ({
  title: '爱波塔 - 美股期权数据观察',
  path: '/pages/us-options/index',
}))

onShareTimeline(() => ({
  title: '爱波塔 - 美股期权数据观察',
  query: '',
}))

onShow(() => {
  if (!auth.isLoggedIn) {
    uni.reLaunch({ url: '/pages/login/index' })
    return
  }
  if (!overview.value) init()
})

onMounted(() => {
  if (auth.isLoggedIn && !overview.value) init()
})

onPullDownRefresh(async () => {
  try {
    await refresh()
  } finally {
    uni.stopPullDownRefresh()
  }
})

async function init() {
  await loadProducts()
  await loadOverview()
}

async function loadProducts() {
  if (productLoading.value) return
  productLoading.value = true
  try {
    const res = await usOptionsApi.products()
    products.value = res.items || []
    if (!selectedSymbol.value) selectedSymbol.value = res.default_symbol || 'SPY'
  } catch (e: any) {
    uni.showToast({ title: e.message || '标的池加载失败', icon: 'none' })
  } finally {
    productLoading.value = false
  }
}

async function loadOverview() {
  if (loading.value) return
  loading.value = true
  try {
    overview.value = await usOptionsApi.overview(selectedSymbol.value)
  } catch (e: any) {
    uni.showToast({ title: e.message || '总览加载失败', icon: 'none' })
  } finally {
    loading.value = false
  }
}

async function loadSurface() {
  if (surfaceLoading.value || surface.value?.symbol === selectedSymbol.value) return
  surfaceLoading.value = true
  try {
    surface.value = await usOptionsApi.surface(selectedSymbol.value)
  } catch (e: any) {
    uni.showToast({ title: e.message || '波动率加载失败', icon: 'none' })
  } finally {
    surfaceLoading.value = false
  }
}

async function loadDefense() {
  if (defenseLoading.value || defense.value?.symbol === selectedSymbol.value) return
  defenseLoading.value = true
  try {
    defense.value = await usOptionsApi.defense(selectedSymbol.value)
  } catch (e: any) {
    uni.showToast({ title: e.message || '持仓防线加载失败', icon: 'none' })
  } finally {
    defenseLoading.value = false
  }
}

async function loadAnomalies() {
  if (anomaliesLoading.value || anomalies.value?.symbol === selectedSymbol.value) return
  anomaliesLoading.value = true
  try {
    anomalies.value = await usOptionsApi.anomalies({ symbol: selectedSymbol.value, limit: 20 })
  } catch (e: any) {
    uni.showToast({ title: e.message || '异动观察加载失败', icon: 'none' })
  } finally {
    anomaliesLoading.value = false
  }
}

async function refresh() {
  if (activeTab.value === 'overview') {
    overview.value = null
    await loadOverview()
  } else if (activeTab.value === 'surface') {
    surface.value = null
    await loadSurface()
  } else if (activeTab.value === 'defense') {
    defense.value = null
    await loadDefense()
  } else {
    anomalies.value = null
    await loadAnomalies()
  }
}

function switchTab(tab: TabKey) {
  activeTab.value = tab
  if (tab === 'surface') loadSurface()
  if (tab === 'defense') loadDefense()
  if (tab === 'anomalies') loadAnomalies()
}

function openPicker() {
  searchText.value = ''
  showPicker.value = true
}

function closePicker() {
  showPicker.value = false
}

async function selectSymbol(symbol: string) {
  const next = String(symbol || '').toUpperCase()
  closePicker()
  if (!next || next === selectedSymbol.value) return
  selectedSymbol.value = next
  overview.value = null
  surface.value = null
  defense.value = null
  anomalies.value = null
  await loadOverview()
  if (activeTab.value === 'surface') loadSurface()
  if (activeTab.value === 'defense') loadDefense()
  if (activeTab.value === 'anomalies') loadAnomalies()
}

const selectedProduct = computed(() => {
  return products.value.find(p => p.symbol === selectedSymbol.value)
    || { symbol: selectedSymbol.value, name: overview.value?.display_name || selectedSymbol.value, asset_type: 'stock', has_data: true }
})

const filteredProducts = computed(() => {
  const q = searchText.value.trim().toUpperCase()
  if (!q) return products.value
  return products.value.filter(p =>
    p.symbol.includes(q) || String(p.name || '').toUpperCase().includes(q)
  )
})

const kpiCards = computed(() => {
  const m = overview.value?.metrics || {}
  return [
    { label: 'ATM IV', value: fmtPctPlain(m.atm_iv_pct), tone: 'blue' },
    { label: 'IV Rank', value: fmtPctPlain(m.iv_rank), tone: rankTone(m.iv_rank) },
    { label: 'IV分位', value: fmtPctPlain(m.iv_percentile), tone: 'gold' },
    { label: 'RV20', value: fmtPctPlain(m.rv20_pct), tone: 'muted' },
    { label: 'IV-RV20', value: fmtSigned(m.iv_rv20_spread, '%'), tone: signedTone(m.iv_rv20_spread) },
    { label: 'Put/Call OI', value: fmtNum(m.put_call_oi, 2), tone: 'muted' },
  ]
})

const ivHistoryChart = computed(() => {
  const rows = overview.value?.iv_history || []
  return buildMiniChart([
    {
      label: 'ATM IV',
      color: '#38bdf8',
      points: rows.map(r => ({ x: compactDate(r.display_date || r.trade_date), y: asNum(r.iv_pct) })),
    },
  ])
})

const coneChart = computed(() => {
  const rows = surface.value?.volatility_cone || []
  return buildMiniChart([
    { label: 'P25', color: '#38bdf8', points: rows.map(r => ({ x: `${r.dte_target}D`, y: asNum(r.p25) })) },
    { label: 'P50', color: '#f5c518', points: rows.map(r => ({ x: `${r.dte_target}D`, y: asNum(r.p50) })) },
    { label: 'P75', color: '#fb7185', points: rows.map(r => ({ x: `${r.dte_target}D`, y: asNum(r.p75) })) },
  ])
})

const otmChart = computed(() => {
  const today = [...(surface.value?.today_otm_curve || [])].sort((a, b) => num0(a.moneyness_pct) - num0(b.moneyness_pct))
  const prev = [...(surface.value?.previous_otm_curve || [])].sort((a, b) => num0(a.moneyness_pct) - num0(b.moneyness_pct))
  return buildMiniChart([
    { label: surface.value?.display_date || '最新', color: '#fb7185', points: today.map(r => ({ x: `${fmtNum(r.moneyness_pct, 0)}%`, y: asNum(r.iv_pct) })) },
    { label: compactDate(surface.value?.previous_trade_date || '') || '前日', color: '#38bdf8', points: prev.map(r => ({ x: `${fmtNum(r.moneyness_pct, 0)}%`, y: asNum(r.iv_pct) })) },
  ])
})

const defenseChart = computed(() => {
  const rows = defense.value?.history || []
  return buildMiniChart([
    { label: 'Call墙', color: '#fb7185', points: rows.map(r => ({ x: compactDate(r.trade_date), y: asNum(r.call_strike) })) },
    { label: '现价', color: '#f5c518', points: rows.map(r => ({ x: compactDate(r.trade_date), y: asNum(r.underlying_close) })) },
    { label: 'Put墙', color: '#38bdf8', points: rows.map(r => ({ x: compactDate(r.trade_date), y: asNum(r.put_strike) })) },
  ])
})

function asNum(v: any, fallback: number | null = null): number | null {
  const n = Number(v)
  return Number.isFinite(n) ? n : fallback
}

function num0(v: any): number {
  return asNum(v, 0) ?? 0
}

function fmtNum(v: any, digits = 1): string {
  const n = asNum(v)
  if (n === null) return '--'
  if (Math.abs(n) >= 1000) return n.toLocaleString(undefined, { maximumFractionDigits: 0 })
  return n.toFixed(digits)
}

function fmtPctPlain(v: any): string {
  const n = asNum(v)
  return n === null ? '--' : `${n.toFixed(1)}%`
}

function fmtSigned(v: any, suffix = ''): string {
  const n = asNum(v)
  if (n === null) return '--'
  return `${n > 0 ? '+' : ''}${n.toFixed(1)}${suffix}`
}

function fmtMoney(v: any): string {
  const n = asNum(v)
  if (n === null) return '--'
  return n >= 1000 ? n.toLocaleString(undefined, { maximumFractionDigits: 2 }) : n.toFixed(2)
}

function compactDate(v: any): string {
  const text = String(v || '').trim()
  if (/^\d{8}$/.test(text)) return `${text.slice(4, 6)}/${text.slice(6)}`
  if (/^\d{4}-\d{2}-\d{2}/.test(text)) return `${text.slice(5, 7)}/${text.slice(8, 10)}`
  return text
}

function sideLabel(v: any): string {
  const text = String(v || '').toUpperCase()
  if (text === 'C') return 'Call'
  if (text === 'P') return 'Put'
  return text || '--'
}

function signedTone(v: any): string {
  const n = asNum(v, 0) || 0
  if (n > 0) return 'red'
  if (n < 0) return 'green'
  return 'muted'
}

function rankTone(v: any): string {
  const n = asNum(v)
  if (n === null) return 'muted'
  if (n >= 70) return 'red'
  if (n <= 20) return 'green'
  return 'gold'
}

function buildMiniChart(inputLines: ChartLineInput[]): MiniChart {
  const width = 620
  const height = 240
  const padL = 42
  const padR = 18
  const padT = 18
  const padB = 38
  const cleanLines = inputLines
    .map(line => ({
      label: line.label,
      color: line.color,
      points: line.points.filter(p => p.x && asNum(p.y) !== null).map(p => ({ x: p.x, y: asNum(p.y) as number })),
    }))
    .filter(line => line.points.length > 0)

  const xValues: string[] = []
  for (const line of cleanLines) {
    for (const p of line.points) {
      if (!xValues.includes(p.x)) xValues.push(p.x)
    }
  }
  const values = cleanLines.flatMap(line => line.points.map(p => p.y))
  if (!xValues.length || !values.length) {
    return { hasData: false, width, height, lines: [], segments: [], nodes: [], xLabels: [], yLabels: [] }
  }
  let minY = Math.min(...values)
  let maxY = Math.max(...values)
  const span = maxY - minY
  const padding = span > 0 ? span * 0.12 : Math.max(Math.abs(maxY || 1) * 0.08, 1)
  minY -= padding
  maxY += padding
  const plotW = width - padL - padR
  const plotH = height - padT - padB
  const xPos = (x: string) => {
    const idx = xValues.indexOf(x)
    if (xValues.length <= 1) return padL + plotW / 2
    return padL + (idx / (xValues.length - 1)) * plotW
  }
  const yPos = (y: number) => padT + ((maxY - y) / Math.max(maxY - minY, 1)) * plotH
  const nodes: ChartNode[] = []
  const segments: ChartSegment[] = []
  const visibleLines: Array<{ label: string; color: string }> = []

  for (const line of cleanLines) {
    visibleLines.push({ label: line.label, color: line.color })
    const pts = line.points.map(p => ({ x: xPos(p.x), y: yPos(p.y) }))
    for (const pt of pts) nodes.push({ left: pt.x, top: pt.y, color: line.color })
    for (let i = 1; i < pts.length; i++) {
      const a = pts[i - 1]
      const b = pts[i]
      const dx = b.x - a.x
      const dy = b.y - a.y
      segments.push({
        left: a.x,
        top: a.y,
        width: Math.sqrt(dx * dx + dy * dy),
        angle: Math.atan2(dy, dx) * 180 / Math.PI,
        color: line.color,
      })
    }
  }

  const yLabels = [maxY, (maxY + minY) / 2, minY].map(v => ({ top: yPos(v), label: fmtNum(v, 1) }))
  const step = Math.max(1, Math.ceil(xValues.length / 4))
  const xLabels = xValues
    .map((label, idx) => ({ label, left: xPos(label), idx }))
    .filter(item => item.idx === 0 || item.idx === xValues.length - 1 || item.idx % step === 0)
    .map(({ label, left }) => ({ label, left }))
  return { hasData: true, width, height, lines: visibleLines, segments, nodes, xLabels, yLabels }
}

function segmentStyle(seg: ChartSegment) {
  return {
    left: `${seg.left}rpx`,
    top: `${seg.top}rpx`,
    width: `${seg.width}rpx`,
    transform: `rotate(${seg.angle}deg)`,
    background: seg.color,
  }
}

function nodeStyle(node: ChartNode) {
  return {
    left: `${node.left}rpx`,
    top: `${node.top}rpx`,
    background: node.color,
  }
}

function axisXStyle(label: { left: number }) {
  return { left: `${label.left}rpx` }
}

function axisYStyle(label: { top: number }) {
  return { top: `${label.top}rpx` }
}
</script>

<template>
  <view class="page">
    <view class="hero">
      <view class="hero-top">
        <view>
          <text class="eyebrow">US OPTIONS</text>
          <text class="hero-title">美股期权</text>
        </view>
        <view class="symbol-trigger" @tap="openPicker">
          <text class="symbol-code">{{ selectedProduct.symbol }}</text>
          <text class="symbol-name">{{ selectedProduct.name }}</text>
          <text class="symbol-arrow">▾</text>
        </view>
      </view>
      <view class="hero-meta">
        <text>最新：{{ overview?.display_date || overview?.trade_date || '--' }}</text>
        <text>收盘：{{ fmtMoney(overview?.underlying_price) }}</text>
      </view>
      <view class="hero-status">
        <text>{{ overview?.status_brief || overview?.message || '数据加载中' }}</text>
      </view>
    </view>

    <view class="tab-bar">
      <view
        v-for="tab in tabs"
        :key="tab.key"
        class="tab-item"
        :class="{ active: activeTab === tab.key }"
        @tap="switchTab(tab.key)"
      >
        <text>{{ tab.label }}</text>
      </view>
      <view class="refresh" @tap="refresh">↻</view>
    </view>

    <view v-if="activeTab === 'overview'" class="content">
      <view v-if="loading" class="center-tip">加载美股期权总览...</view>
      <view v-else>
        <view class="kpi-grid">
          <view v-for="card in kpiCards" :key="card.label" class="kpi-card">
            <text class="kpi-label">{{ card.label }}</text>
            <text class="kpi-value" :class="`tone-${card.tone}`">{{ card.value }}</text>
          </view>
        </view>

        <view class="panel">
          <view class="panel-head">
            <text class="panel-title">IV历史</text>
            <text class="panel-sub">最近样本</text>
          </view>
          <view v-if="ivHistoryChart.hasData" class="chart-box">
            <view v-for="seg in ivHistoryChart.segments" :key="`${seg.left}-${seg.top}-${seg.width}`" class="chart-seg" :style="segmentStyle(seg)" />
            <view v-for="node in ivHistoryChart.nodes" :key="`${node.left}-${node.top}-${node.color}`" class="chart-node" :style="nodeStyle(node)" />
            <text v-for="y in ivHistoryChart.yLabels" :key="`y-${y.label}`" class="axis-y" :style="axisYStyle(y)">{{ y.label }}</text>
            <text v-for="x in ivHistoryChart.xLabels" :key="`x-${x.label}`" class="axis-x" :style="axisXStyle(x)">{{ x.label }}</text>
          </view>
          <view v-else class="empty-chart">暂无 IV 历史曲线</view>
        </view>

        <view class="panel">
          <view class="panel-head">
            <text class="panel-title">数据覆盖</text>
            <text class="panel-sub">{{ overview?.asset_type === 'etf' ? 'ETF' : '股票' }}</text>
          </view>
          <view class="coverage-grid">
            <view class="coverage-row">
              <text>链行数</text>
              <text>{{ overview?.chain_summary?.rows ?? '--' }}</text>
            </view>
            <view class="coverage-row">
              <text>IV行</text>
              <text>{{ (overview?.chain_summary?.provider_iv_rows || 0) + (overview?.chain_summary?.computed_iv_rows || 0) }}</text>
            </view>
            <view class="coverage-row">
              <text>OI行</text>
              <text>{{ overview?.chain_summary?.open_interest_rows ?? '--' }}</text>
            </view>
          </view>
          <view v-if="overview?.gaps?.length" class="gap-list">
            <text v-for="gap in overview.gaps" :key="gap">{{ gap }}</text>
          </view>
        </view>
      </view>
    </view>

    <view v-else-if="activeTab === 'surface'" class="content">
      <view v-if="surfaceLoading" class="center-tip">加载波动率数据...</view>
      <view v-else-if="surface?.has_data">
        <view class="panel">
          <view class="panel-head">
            <text class="panel-title">波动率锥</text>
            <text class="panel-sub">P25 / P50 / P75</text>
          </view>
          <view v-if="coneChart.hasData" class="chart-box">
            <view v-for="seg in coneChart.segments" :key="`${seg.left}-${seg.top}-${seg.width}-${seg.color}`" class="chart-seg" :style="segmentStyle(seg)" />
            <view v-for="node in coneChart.nodes" :key="`${node.left}-${node.top}-${node.color}`" class="chart-node" :style="nodeStyle(node)" />
            <text v-for="y in coneChart.yLabels" :key="`cone-y-${y.label}`" class="axis-y" :style="axisYStyle(y)">{{ y.label }}</text>
            <text v-for="x in coneChart.xLabels" :key="`cone-x-${x.label}`" class="axis-x" :style="axisXStyle(x)">{{ x.label }}</text>
          </view>
          <view class="legend">
            <view v-for="line in coneChart.lines" :key="line.label" class="legend-item">
              <view class="legend-dot" :style="{ background: line.color }" />
              <text>{{ line.label }}</text>
            </view>
          </view>
        </view>

        <view class="panel">
          <view class="panel-head">
            <text class="panel-title">OTM波动率曲线</text>
            <text class="panel-sub">按虚值程度</text>
          </view>
          <view v-if="otmChart.hasData" class="chart-box">
            <view v-for="seg in otmChart.segments" :key="`${seg.left}-${seg.top}-${seg.width}-${seg.color}`" class="chart-seg" :style="segmentStyle(seg)" />
            <view v-for="node in otmChart.nodes" :key="`${node.left}-${node.top}-${node.color}`" class="chart-node" :style="nodeStyle(node)" />
            <text v-for="y in otmChart.yLabels" :key="`otm-y-${y.label}`" class="axis-y" :style="axisYStyle(y)">{{ y.label }}</text>
            <text v-for="x in otmChart.xLabels" :key="`otm-x-${x.label}`" class="axis-x" :style="axisXStyle(x)">{{ x.label }}</text>
          </view>
          <view class="legend">
            <view v-for="line in otmChart.lines" :key="line.label" class="legend-item">
              <view class="legend-dot" :style="{ background: line.color }" />
              <text>{{ line.label }}</text>
            </view>
          </view>
        </view>
      </view>
      <view v-else class="center-tip">{{ surface?.message || '暂无波动率数据' }}</view>
    </view>

    <view v-else-if="activeTab === 'defense'" class="content">
      <view v-if="defenseLoading" class="center-tip">加载持仓防线...</view>
      <view v-else-if="defense?.has_data">
        <view class="defense-card">
          <view class="defense-item red">
            <text>Call压力位</text>
            <text>{{ fmtNum(defense.latest?.call_strike, 2) }}</text>
            <text>距现价 {{ fmtSigned(defense.latest?.call_distance_pct, '%') }}</text>
          </view>
          <view class="defense-item blue">
            <text>Put支撑位</text>
            <text>{{ fmtNum(defense.latest?.put_strike, 2) }}</text>
            <text>距现价 {{ fmtSigned(defense.latest?.put_distance_pct, '%') }}</text>
          </view>
        </view>
        <view class="panel">
          <view class="panel-head">
            <text class="panel-title">近20日防线</text>
            <text class="panel-sub">{{ defense.display_date || defense.trade_date }}</text>
          </view>
          <view v-if="defenseChart.hasData" class="chart-box">
            <view v-for="seg in defenseChart.segments" :key="`${seg.left}-${seg.top}-${seg.width}-${seg.color}`" class="chart-seg" :style="segmentStyle(seg)" />
            <view v-for="node in defenseChart.nodes" :key="`${node.left}-${node.top}-${node.color}`" class="chart-node" :style="nodeStyle(node)" />
            <text v-for="y in defenseChart.yLabels" :key="`def-y-${y.label}`" class="axis-y" :style="axisYStyle(y)">{{ y.label }}</text>
            <text v-for="x in defenseChart.xLabels" :key="`def-x-${x.label}`" class="axis-x" :style="axisXStyle(x)">{{ x.label }}</text>
          </view>
          <view class="legend">
            <view v-for="line in defenseChart.lines" :key="line.label" class="legend-item">
              <view class="legend-dot" :style="{ background: line.color }" />
              <text>{{ line.label }}</text>
            </view>
          </view>
        </view>
      </view>
      <view v-else class="center-tip">{{ defense?.message || '暂无持仓防线数据' }}</view>
    </view>

    <view v-else class="content">
      <view v-if="anomaliesLoading" class="center-tip">加载异动观察...</view>
      <view v-else-if="anomalies?.has_data" class="anomaly-list">
        <view v-for="item in anomalies.items" :key="item.option_ticker" class="anomaly-card">
          <view class="anomaly-top">
            <view>
              <text class="anomaly-symbol">{{ item.underlying }} {{ sideLabel(item.call_put) }}</text>
              <text class="anomaly-contract">{{ fmtNum(item.strike, 2) }} · {{ item.expiration_date }}</text>
            </view>
            <text class="anomaly-score">{{ fmtNum(item.anomaly_score, 0) }}</text>
          </view>
          <view class="anomaly-grid">
            <view><text>成交量</text><text>{{ fmtNum(item.volume, 0) }}</text></view>
            <view><text>OI变化</text><text>{{ fmtSigned(item.oi_change, '') }}</text></view>
            <view><text>IV</text><text>{{ fmtPctPlain(item.iv_pct) }}</text></view>
            <view><text>DTE</text><text>{{ item.dte ?? '--' }}</text></view>
          </view>
        </view>
      </view>
      <view v-else class="center-tip">{{ anomalies?.message || '暂无异动观察数据' }}</view>
    </view>

    <view class="disclaimer">数据仅用于市场观察与学习，不构成操作建议。</view>
    <view style="height: 130rpx;" />

    <view v-if="showPicker" class="picker-mask" @tap="closePicker">
      <view class="picker-panel" @tap.stop>
        <view class="picker-head">
          <text>选择标的</text>
          <text class="picker-close" @tap="closePicker">×</text>
        </view>
        <input v-model="searchText" class="picker-input" placeholder="搜索代码或名称，如 SPY / NVDA" placeholder-class="input-placeholder" />
        <scroll-view scroll-y class="picker-list">
          <view
            v-for="p in filteredProducts"
            :key="p.symbol"
            class="picker-item"
            :class="{ active: p.symbol === selectedSymbol }"
            @tap="selectSymbol(p.symbol)"
          >
            <view>
              <text class="picker-symbol">{{ p.symbol }}</text>
              <text class="picker-name">{{ p.name }}</text>
            </view>
            <text class="picker-state">{{ p.has_data ? '有数据' : '待更新' }}</text>
          </view>
        </scroll-view>
      </view>
    </view>

    <BottomNav active="us" />
  </view>
</template>

<style scoped>
.page {
  min-height: 100vh;
  background: #0b1121;
  color: #f0f4ff;
  padding-bottom: 40rpx;
}

.hero {
  margin: 24rpx;
  padding: 26rpx;
  border: 1px solid #20324f;
  border-radius: 24rpx;
  background:
    radial-gradient(circle at 90% 0%, rgba(245, 197, 24, 0.16), transparent 35%),
    linear-gradient(135deg, #13223a 0%, #101a2d 62%, #121827 100%);
}

.hero-top {
  display: flex;
  justify-content: space-between;
  gap: 18rpx;
  align-items: flex-start;
}

.eyebrow {
  display: block;
  color: #6f86a8;
  font-size: 18rpx;
  letter-spacing: 4rpx;
  font-weight: 800;
}

.hero-title {
  display: block;
  color: #f8fafc;
  font-size: 40rpx;
  font-weight: 900;
  margin-top: 8rpx;
}

.symbol-trigger {
  max-width: 360rpx;
  min-width: 260rpx;
  border: 1px solid #245b7a;
  border-radius: 999rpx;
  padding: 12rpx 20rpx;
  background: rgba(8, 18, 32, 0.82);
  display: flex;
  align-items: center;
  gap: 10rpx;
}

.symbol-code {
  color: #dff3ff;
  font-size: 26rpx;
  font-weight: 900;
}

.symbol-name {
  color: #91a4bf;
  font-size: 22rpx;
  flex: 1;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.symbol-arrow { color: #f5c518; font-size: 22rpx; }

.hero-meta {
  display: flex;
  justify-content: space-between;
  margin-top: 22rpx;
  color: #8ea0ba;
  font-size: 22rpx;
}

.hero-status {
  margin-top: 16rpx;
  color: #d6e4ff;
  font-size: 24rpx;
  line-height: 1.55;
}

.tab-bar {
  position: sticky;
  top: 0;
  z-index: 20;
  display: flex;
  align-items: center;
  gap: 10rpx;
  padding: 10rpx 20rpx 14rpx;
  background: rgba(11, 17, 33, 0.96);
  border-bottom: 1px solid #1e2d45;
}

.tab-item {
  flex: 1;
  text-align: center;
  border: 1px solid #1e2d45;
  border-radius: 999rpx;
  padding: 14rpx 6rpx;
  color: #8b96aa;
  font-size: 24rpx;
  font-weight: 700;
  background: #101a2d;
}

.tab-item.active {
  color: #f5c518;
  border-color: rgba(245, 197, 24, 0.6);
  background: rgba(245, 197, 24, 0.12);
}

.refresh {
  width: 58rpx;
  height: 58rpx;
  line-height: 58rpx;
  text-align: center;
  color: #9fb0c8;
  font-size: 30rpx;
}

.content { padding: 18rpx 24rpx 0; }

.center-tip {
  margin-top: 120rpx;
  text-align: center;
  color: #7f8da5;
  font-size: 26rpx;
}

.kpi-grid {
  display: grid;
  grid-template-columns: repeat(2, 1fr);
  gap: 14rpx;
}

.kpi-card,
.panel,
.defense-card,
.anomaly-card {
  background: #131c2e;
  border: 1px solid #1f3353;
  border-radius: 18rpx;
}

.kpi-card {
  padding: 22rpx;
}

.kpi-label {
  display: block;
  color: #788aa7;
  font-size: 22rpx;
}

.kpi-value {
  display: block;
  margin-top: 10rpx;
  color: #f0f4ff;
  font-size: 34rpx;
  font-weight: 900;
}

.tone-blue { color: #38bdf8; }
.tone-gold { color: #f5c518; }
.tone-red { color: #f87171; }
.tone-green { color: #34d399; }
.tone-muted { color: #d5dced; }

.panel {
  margin-top: 18rpx;
  padding: 20rpx;
}

.panel-head {
  display: flex;
  justify-content: space-between;
  align-items: center;
  margin-bottom: 16rpx;
}

.panel-title {
  color: #f0f4ff;
  font-size: 28rpx;
  font-weight: 900;
}

.panel-sub {
  color: #71809a;
  font-size: 22rpx;
}

.chart-box {
  position: relative;
  width: 620rpx;
  height: 240rpx;
  max-width: 100%;
  overflow: hidden;
  border-radius: 14rpx;
  background: linear-gradient(180deg, rgba(20, 33, 55, 0.86), rgba(10, 18, 32, 0.92));
  border: 1px solid #223653;
}

.chart-box::before {
  content: "";
  position: absolute;
  inset: 18rpx 18rpx 38rpx 42rpx;
  background-image:
    linear-gradient(rgba(148, 163, 184, 0.08) 1px, transparent 1px),
    linear-gradient(90deg, rgba(148, 163, 184, 0.08) 1px, transparent 1px);
  background-size: 64rpx 52rpx;
}

.chart-box::after {
  content: "";
  position: absolute;
  left: 42rpx;
  right: 18rpx;
  bottom: 38rpx;
  border-bottom: 1px solid rgba(203, 213, 225, 0.42);
  box-shadow: -1px -184rpx 0 -0.5px rgba(203, 213, 225, 0.35);
}

.chart-seg {
  position: absolute;
  height: 4rpx;
  border-radius: 4rpx;
  transform-origin: 0 50%;
}

.chart-node {
  position: absolute;
  width: 10rpx;
  height: 10rpx;
  margin-left: -5rpx;
  margin-top: -5rpx;
  border-radius: 50%;
  border: 2rpx solid #0b1121;
}

.axis-y {
  position: absolute;
  right: 12rpx;
  transform: translateY(-50%);
  color: #dce5f5;
  font-size: 18rpx;
  font-weight: 700;
}

.axis-x {
  position: absolute;
  bottom: 10rpx;
  transform: translateX(-50%);
  color: #dce5f5;
  font-size: 18rpx;
  font-weight: 700;
}

.legend {
  display: flex;
  flex-wrap: wrap;
  gap: 16rpx;
  margin-top: 14rpx;
}

.legend-item {
  display: flex;
  align-items: center;
  gap: 8rpx;
  color: #8fa3c1;
  font-size: 22rpx;
}

.legend-dot {
  width: 12rpx;
  height: 12rpx;
  border-radius: 50%;
}

.empty-chart {
  height: 160rpx;
  display: flex;
  align-items: center;
  justify-content: center;
  color: #7f8da5;
  font-size: 24rpx;
}

.coverage-grid {
  display: grid;
  grid-template-columns: repeat(3, 1fr);
  gap: 10rpx;
}

.coverage-row {
  background: #0f192b;
  border-radius: 14rpx;
  padding: 16rpx;
}

.coverage-row text:first-child {
  display: block;
  color: #7e8da6;
  font-size: 20rpx;
}

.coverage-row text:last-child {
  display: block;
  color: #f0f4ff;
  font-size: 28rpx;
  font-weight: 800;
  margin-top: 6rpx;
}

.gap-list {
  display: flex;
  flex-direction: column;
  gap: 8rpx;
  margin-top: 14rpx;
  color: #fbbf24;
  font-size: 22rpx;
}

.defense-card {
  display: grid;
  grid-template-columns: repeat(2, 1fr);
  gap: 12rpx;
  padding: 16rpx;
}

.defense-item {
  border-radius: 16rpx;
  background: #0f192b;
  padding: 18rpx;
}

.defense-item text { display: block; }
.defense-item text:first-child { color: #8998b3; font-size: 22rpx; }
.defense-item text:nth-child(2) { color: #f0f4ff; font-size: 34rpx; font-weight: 900; margin-top: 8rpx; }
.defense-item text:last-child { color: #8ea0ba; font-size: 22rpx; margin-top: 8rpx; }
.defense-item.red { border: 1px solid rgba(248, 113, 113, 0.35); }
.defense-item.blue { border: 1px solid rgba(56, 189, 248, 0.35); }

.anomaly-list {
  display: flex;
  flex-direction: column;
  gap: 16rpx;
}

.anomaly-card {
  padding: 20rpx;
}

.anomaly-top {
  display: flex;
  justify-content: space-between;
  align-items: flex-start;
  gap: 16rpx;
}

.anomaly-symbol {
  display: block;
  color: #f0f4ff;
  font-size: 28rpx;
  font-weight: 900;
}

.anomaly-contract {
  display: block;
  color: #8fa3c1;
  font-size: 22rpx;
  margin-top: 6rpx;
}

.anomaly-score {
  color: #f5c518;
  border: 1px solid rgba(245, 197, 24, 0.45);
  border-radius: 999rpx;
  padding: 8rpx 14rpx;
  font-size: 24rpx;
  font-weight: 900;
}

.anomaly-grid {
  display: grid;
  grid-template-columns: repeat(4, 1fr);
  gap: 10rpx;
  margin-top: 18rpx;
}

.anomaly-grid view {
  background: #0f192b;
  border-radius: 12rpx;
  padding: 12rpx 8rpx;
}

.anomaly-grid text:first-child {
  display: block;
  color: #7588a5;
  font-size: 19rpx;
}

.anomaly-grid text:last-child {
  display: block;
  color: #e6edf9;
  font-size: 22rpx;
  font-weight: 800;
  margin-top: 4rpx;
}

.disclaimer {
  margin: 20rpx 24rpx 0;
  padding: 16rpx 20rpx;
  border: 1px solid rgba(245, 197, 24, 0.28);
  border-radius: 16rpx;
  color: #c9a227;
  background: rgba(245, 197, 24, 0.06);
  font-size: 22rpx;
  line-height: 1.5;
}

.picker-mask {
  position: fixed;
  inset: 0;
  z-index: 1000;
  background: rgba(3, 8, 18, 0.68);
  display: flex;
  align-items: flex-end;
}

.picker-panel {
  width: 100%;
  max-height: 78vh;
  background: #101a2d;
  border-top: 1px solid #223653;
  border-radius: 28rpx 28rpx 0 0;
  padding: 26rpx 24rpx calc(env(safe-area-inset-bottom) + 24rpx);
}

.picker-head {
  display: flex;
  justify-content: space-between;
  align-items: center;
  color: #f0f4ff;
  font-size: 30rpx;
  font-weight: 900;
}

.picker-close {
  color: #7f8da5;
  font-size: 42rpx;
  line-height: 1;
}

.picker-input {
  margin-top: 20rpx;
  height: 72rpx;
  border-radius: 16rpx;
  background: #131f34;
  border: 1px solid #243956;
  color: #e6edf9;
  font-size: 26rpx;
  padding: 0 20rpx;
}

.input-placeholder { color: #687891; }

.picker-list {
  margin-top: 18rpx;
  max-height: 56vh;
}

.picker-item {
  display: flex;
  justify-content: space-between;
  align-items: center;
  gap: 16rpx;
  padding: 20rpx 4rpx;
  border-bottom: 1px solid #1b2a43;
}

.picker-item.active .picker-symbol,
.picker-item.active .picker-state {
  color: #f5c518;
}

.picker-symbol {
  color: #f0f4ff;
  font-size: 28rpx;
  font-weight: 900;
  margin-right: 14rpx;
}

.picker-name {
  color: #9aabc5;
  font-size: 24rpx;
}

.picker-state {
  color: #71809a;
  font-size: 22rpx;
}
</style>
