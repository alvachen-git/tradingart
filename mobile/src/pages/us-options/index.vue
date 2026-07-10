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
  xSort?: number | null | undefined
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

type CandleShape = {
  x: number
  highTop: number
  lowTop: number
  bodyTop: number
  bodyHeight: number
  width: number
  color: string
}

type PriceIvRow = {
  tradeDate: string
  label: string
  open: number
  high: number
  low: number
  close: number
  iv: number | null
}

type PriceIvHoverItem = {
  x: number
  date: string
  open: string
  high: string
  low: string
  close: string
  iv: string
}

type PriceIvChart = {
  hasData: boolean
  width: number
  height: number
  plotLeft: number
  plotWidth: number
  candles: CandleShape[]
  ivSegments: ChartSegment[]
  xLabels: Array<{ left: number; label: string }>
  priceLabels: Array<{ top: number; label: string }>
  ivLabels: Array<{ top: number; label: string }>
  hoverItems: PriceIvHoverItem[]
  latestClose: string
  latestIv: string
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
const showAtmIv = ref(true)
const chartWindow = ref(90)
const chartStart = ref(0)
const chartHoverIndex = ref<number | null>(null)

const PRICE_IV_MIN_WINDOW = 24
const PRICE_IV_MAX_WINDOW = 180
const OVERVIEW_CACHE_PREFIX = 'us_options_overview_cache_v1_'
const OVERVIEW_CACHE_TTL = 10 * 60 * 1000
let touchStartX = 0
let touchStartStart = 0
let pinchStartDistance = 0
let pinchStartWindow = 0
let longPressTimer: ReturnType<typeof setTimeout> | null = null
let chartTouchMode: 'idle' | 'pan' | 'pinch' | 'long' = 'idle'
let chartRect: { left: number; width: number } | null = null
let initPromise: Promise<void> | null = null
let overviewLoadPromise: Promise<void> | null = null
let overviewLoadSymbol = ''
let overviewRequestSeq = 0
let overviewLoadedAt = 0

const tabs: Array<{ key: TabKey; label: string }> = [
  { key: 'overview', label: '概览' },
  { key: 'surface', label: '波动率' },
  { key: 'defense', label: '持仓防线' },
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
  ensureInit()
})

onMounted(() => {
  if (auth.isLoggedIn) ensureInit()
})

onPullDownRefresh(async () => {
  try {
    await refresh()
  } finally {
    uni.stopPullDownRefresh()
  }
})

function ensureInit() {
  const hasFreshOverview = overview.value
    && overview.value.symbol === selectedSymbol.value
    && Date.now() - overviewLoadedAt < OVERVIEW_CACHE_TTL
  if (hasFreshOverview || initPromise) return initPromise
  initPromise = init().finally(() => {
    initPromise = null
  })
  return initPromise
}

async function init() {
  hydrateOverviewCache(selectedSymbol.value)
  await Promise.all([
    loadProducts(),
    loadOverview({ silent: !!overview.value }),
  ])
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

function overviewCacheKey(symbol: string): string {
  return `${OVERVIEW_CACHE_PREFIX}${String(symbol || 'SPY').toUpperCase()}`
}

function hydrateOverviewCache(symbol: string): boolean {
  if (overview.value) return true
  try {
    const cached = uni.getStorageSync(overviewCacheKey(symbol)) as { ts?: number; data?: UsOptionOverviewPayload } | ''
    if (!cached || typeof cached !== 'object') return false
    const ts = Number(cached.ts || 0)
    if (!cached.data || Date.now() - ts > OVERVIEW_CACHE_TTL) return false
    overview.value = cached.data
    overviewLoadedAt = ts
    resetPriceIvWindow()
    return true
  } catch (_e) {
    return false
  }
}

function saveOverviewCache(symbol: string, data: UsOptionOverviewPayload) {
  try {
    uni.setStorageSync(overviewCacheKey(symbol), { ts: Date.now(), data })
  } catch (_e) {
    // Storage can fail in low-space/private contexts; live data should still render.
  }
}

async function loadOverview(options: { force?: boolean; silent?: boolean } = {}) {
  const symbol = selectedSymbol.value
  if (!options.force && overviewLoadPromise && overviewLoadSymbol === symbol) {
    return overviewLoadPromise
  }
  if (!options.force && !overview.value) hydrateOverviewCache(symbol)
  const requestSeq = ++overviewRequestSeq
  loading.value = true
  overviewLoadSymbol = symbol
  overviewLoadPromise = (async () => {
    try {
      const data = await usOptionsApi.overview(symbol)
      if (requestSeq !== overviewRequestSeq || symbol !== selectedSymbol.value) return
      overview.value = data
      overviewLoadedAt = Date.now()
      saveOverviewCache(symbol, data)
      resetPriceIvWindow()
    } catch (e: any) {
      if (!options.silent) uni.showToast({ title: e.message || '总览加载失败', icon: 'none' })
    } finally {
      if (requestSeq === overviewRequestSeq) loading.value = false
      if (overviewLoadSymbol === symbol) {
        overviewLoadPromise = null
        overviewLoadSymbol = ''
      }
    }
  })()
  return overviewLoadPromise
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
    overviewLoadedAt = 0
    await loadOverview({ force: true })
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
  overviewLoadedAt = 0
  surface.value = null
  defense.value = null
  anomalies.value = null
  chartHoverIndex.value = null
  const hadCache = hydrateOverviewCache(next)
  await loadOverview({ silent: hadCache })
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

const snapshotCards = computed(() => {
  const m = overview.value?.metrics || {}
  const putSkew = asNum(m.put_skew_5pct)
  const callSkew = asNum(m.call_skew_5pct)
  const skewGap = asNum(m.put_call_skew_5pct, putSkew !== null && callSkew !== null ? putSkew - callSkew : null)
  const cards = [
    {
      label: '今日 IV 变化',
      sub: String(m.iv_change_1d_direction_label || '较前一交易日'),
      value: fmtSigned(m.iv_change_1d, '%'),
      percentile: asNum(m.iv_change_1d_directional_percentile),
      percentileLabel: String(m.iv_change_1d_direction_label || '变化分位'),
      sample: sampleLabel(m.iv_change_1d_directional_history_count, m.iv_change_1d_min_samples || m.historical_percentile_min_samples),
      tone: ivChangeTone(m.iv_change_1d),
    },
    {
      label: 'IV - RV20',
      sub: '隐波减实际波动',
      value: fmtSigned(m.iv_rv20_spread, '%'),
      percentile: asNum(m.iv_rv20_percentile),
      percentileLabel: '历史分位',
      sample: sampleLabel(m.iv_rv20_spread_history_count, m.iv_rv20_spread_min_samples || m.historical_percentile_min_samples),
      tone: signedTone(m.iv_rv20_spread),
    },
    {
      label: 'IV Rank',
      sub: '月结算 IV 排名',
      value: fmtPctPlain(m.iv_rank),
      percentile: asNum(m.iv_rank),
      percentileLabel: '历史分位',
      sample: sampleLabel(m.iv_history_days, m.historical_percentile_min_samples),
      tone: rankTone(m.iv_rank),
    },
    {
      label: '期限结构',
      sub: termStateLabel(m.term_state),
      value: fmtSigned(m.term_slope_30_60, '%'),
      percentile: asNum(m.term_slope_percentile),
      percentileLabel: '历史分位',
      sample: sampleLabel(m.term_slope_30_60_history_count, m.term_slope_30_60_min_samples || m.historical_percentile_min_samples),
      tone: signedTone(m.term_slope_30_60),
    },
    {
      label: 'Put Skew',
      sub: '下方保护相对 ATM',
      value: fmtSigned(putSkew, '%'),
      percentile: asNum(m.put_skew_5pct_percentile),
      percentileLabel: '历史分位',
      sample: sampleLabel(m.put_skew_5pct_history_count, m.put_skew_5pct_min_samples || m.historical_percentile_min_samples),
      tone: signedTone(putSkew),
    },
    {
      label: 'Call Skew',
      sub: '上方追涨相对 ATM',
      value: fmtSigned(callSkew, '%'),
      percentile: asNum(m.call_skew_5pct_percentile),
      percentileLabel: '历史分位',
      sample: sampleLabel(m.call_skew_5pct_history_count, m.call_skew_5pct_min_samples || m.historical_percentile_min_samples),
      tone: signedTone(callSkew),
    },
    {
      label: 'Put-Call Skew',
      sub: '保护需求减追涨需求',
      value: fmtSigned(skewGap, '%'),
      percentile: asNum(m.put_call_skew_5pct_percentile),
      percentileLabel: '历史分位',
      sample: sampleLabel(m.put_call_skew_5pct_history_count, m.put_call_skew_5pct_min_samples || m.historical_percentile_min_samples),
      tone: signedTone(skewGap),
    },
  ]
  return cards.map(card => ({
    ...card,
    percentileText: card.percentile === null ? '--' : `${card.percentile.toFixed(0)}%`,
    percentileClamped: clampInt(card.percentile ?? 0, 0, 100),
  }))
})

const profileCard = computed(() => overview.value?.profile_card || {})

const profileItems = computed(() => {
  const p = profileCard.value
  return [
    { label: p.intro_label || '标的介绍', text: p.business || '暂无标的介绍' },
    { label: p.style_label || '板块风格', text: p.style || '暂无板块风格' },
    { label: '近期热点', text: p.recent_hotspot || '近期热点待更新' },
  ]
})

const priceIvRows = computed(() => normalizePriceIvRows(
  overview.value?.price_history || [],
  overview.value?.iv_history || [],
))

const priceIvVisibleRows = computed(() => {
  const rows = priceIvRows.value
  const total = rows.length
  if (!total) return []
  const win = clampInt(chartWindow.value, PRICE_IV_MIN_WINDOW, Math.min(PRICE_IV_MAX_WINDOW, total))
  const start = clampInt(chartStart.value, 0, Math.max(total - win, 0))
  return rows.slice(start, start + win)
})

const priceIvChart = computed(() => buildPriceIvChart(
  priceIvVisibleRows.value,
  showAtmIv.value,
))

const priceIvHover = computed(() => {
  const idx = chartHoverIndex.value
  if (idx === null) return null
  const item = priceIvChart.value.hoverItems[idx]
  if (!item) return null
  const tooltipW = 246
  const left = clampInt(Math.round(item.x + 16), 54, priceIvChart.value.width - tooltipW - 10)
  return {
    ...item,
    lineStyle: { left: `${item.x}rpx` },
    tooltipStyle: { left: `${left}rpx` },
  }
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
    { label: surface.value?.display_date || '最新', color: '#fb7185', points: today.map(r => ({ x: `${fmtNum(r.moneyness_pct, 0)}%`, xSort: asNum(r.moneyness_pct), y: asNum(r.iv_pct) })) },
    { label: compactDate(surface.value?.previous_trade_date || '') || '前日', color: '#38bdf8', points: prev.map(r => ({ x: `${fmtNum(r.moneyness_pct, 0)}%`, xSort: asNum(r.moneyness_pct), y: asNum(r.iv_pct) })) },
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

function ivChangeTone(v: any): string {
  const n = asNum(v)
  if (n === null) return 'muted'
  if (n > 0) return 'red'
  if (n < 0) return 'blue'
  return 'muted'
}

function termStateLabel(v: any): string {
  const text = String(v || '')
  if (text === 'Backwardation') return '近月风险溢价偏强'
  if (text === 'Contango') return '远月 IV 高于近月'
  if (text === 'Flat') return '期限结构相对平坦'
  return '期限样本不足'
}

function sampleLabel(count: any, minSamples: any): string {
  const c = asNum(count)
  const m = asNum(minSamples)
  if (c === null && m === null) return '样本待更新'
  if (c !== null && m !== null) return `样本 ${fmtNum(c, 0)}/${fmtNum(m, 0)}`
  if (c !== null) return `样本 ${fmtNum(c, 0)}`
  return `门槛 ${fmtNum(m, 0)}`
}

function clampInt(v: number, min: number, max: number): number {
  if (max < min) return min
  return Math.max(min, Math.min(max, Math.round(v)))
}

function normalizePriceIvRows(priceRowsRaw: Array<Record<string, any>>, ivRowsRaw: Array<Record<string, any>>): PriceIvRow[] {
  const ivMap = new Map<string, number>()
  for (const row of ivRowsRaw || []) {
    const td = String(row.trade_date || '').replace(/-/g, '').slice(0, 8)
    const iv = asNum(row.iv_pct)
    if (td && iv !== null) ivMap.set(td, iv)
  }

  return (priceRowsRaw || [])
    .map(row => {
      const tradeDate = String(row.trade_date || '').replace(/-/g, '').slice(0, 8)
      return {
        tradeDate,
        label: compactDate(row.display_date || tradeDate),
        open: asNum(row.open),
        high: asNum(row.high),
        low: asNum(row.low),
        close: asNum(row.close),
        iv: ivMap.get(tradeDate) ?? null,
      }
    })
    .filter((row): row is PriceIvRow =>
      Boolean(row.tradeDate)
      && row.open !== null
      && row.high !== null
      && row.low !== null
      && row.close !== null
    )
    .sort((a, b) => a.tradeDate.localeCompare(b.tradeDate))
}

function resetPriceIvWindow() {
  const total = priceIvRows.value.length
  if (!total) {
    chartWindow.value = 90
    chartStart.value = 0
    chartHoverIndex.value = null
    return
  }
  const nextWindow = clampInt(Math.min(90, total), Math.min(PRICE_IV_MIN_WINDOW, total), total)
  chartWindow.value = nextWindow
  chartStart.value = Math.max(total - nextWindow, 0)
  chartHoverIndex.value = null
}

function buildPriceIvChart(rows: PriceIvRow[], showIv: boolean): PriceIvChart {
  const width = 620
  const height = 480
  const padL = 42
  const padR = 56
  const padT = 24
  const padB = 42
  const plotW = width - padL - padR
  const plotH = height - padT - padB
  const empty: PriceIvChart = {
    hasData: false,
    width,
    height,
    plotLeft: padL,
    plotWidth: plotW,
    candles: [],
    ivSegments: [],
    xLabels: [],
    priceLabels: [],
    ivLabels: [],
    hoverItems: [],
    latestClose: '--',
    latestIv: '--',
  }

  if (rows.length < 2) return empty

  const priceValues = rows.flatMap(row => [row.high, row.low])
  const ivValues = rows.map(row => row.iv).filter((v): v is number => v !== null)
  if (!priceValues.length) return empty

  let minP = Math.min(...priceValues)
  let maxP = Math.max(...priceValues)
  const spanP = maxP - minP
  const padP = spanP > 0 ? spanP * 0.1 : Math.max(Math.abs(maxP || 1) * 0.05, 1)
  minP -= padP
  maxP += padP

  let minIv = ivValues.length ? Math.min(...ivValues) : 0
  let maxIv = ivValues.length ? Math.max(...ivValues) : 1
  const spanIv = maxIv - minIv
  const padIv = spanIv > 0 ? spanIv * 0.14 : Math.max(Math.abs(maxIv || 1) * 0.08, 1)
  minIv -= padIv
  maxIv += padIv

  const candleW = Math.max(4, Math.min(12, plotW / Math.max(rows.length * 1.65, 1)))
  const xAt = (idx: number) => padL + (idx / Math.max(rows.length - 1, 1)) * plotW
  const yPrice = (value: number) => padT + ((maxP - value) / Math.max(maxP - minP, 1)) * plotH
  const yIv = (value: number) => padT + ((maxIv - value) / Math.max(maxIv - minIv, 1)) * plotH

  const candles: CandleShape[] = rows.map((row, idx) => {
    const open = row.open
    const close = row.close
    const color = close >= open ? '#ef4444' : '#22c55e'
    const top = Math.min(yPrice(open), yPrice(close))
    return {
      x: xAt(idx),
      highTop: yPrice(row.high),
      lowTop: yPrice(row.low),
      bodyTop: top,
      bodyHeight: Math.max(Math.abs(yPrice(open) - yPrice(close)), 3),
      width: candleW,
      color,
    }
  })

  const ivPts = rows
    .map((row, idx) => row.iv === null ? null : ({ x: xAt(idx), y: yIv(row.iv), value: row.iv }))
    .filter((pt): pt is { x: number; y: number; value: number } => !!pt)
  const ivSegments: ChartSegment[] = []
  if (showIv) {
    for (let i = 1; i < ivPts.length; i++) {
      const a = ivPts[i - 1]
      const b = ivPts[i]
      const dx = b.x - a.x
      const dy = b.y - a.y
      ivSegments.push({
        left: a.x,
        top: a.y,
        width: Math.sqrt(dx * dx + dy * dy),
        angle: Math.atan2(dy, dx) * 180 / Math.PI,
        color: '#38bdf8',
      })
    }
  }

  const labelStep = Math.max(1, Math.ceil(rows.length / 4))
  const xLabels = rows
    .map((row, idx) => ({ label: row.label, left: xAt(idx), idx }))
    .filter(item => item.idx === 0 || item.idx === rows.length - 1 || item.idx % labelStep === 0)
    .map(({ label, left }) => ({ label, left }))

  return {
    hasData: true,
    width,
    height,
    plotLeft: padL,
    plotWidth: plotW,
    candles,
    ivSegments,
    xLabels,
    priceLabels: [maxP, (maxP + minP) / 2, minP].map(v => ({ top: yPrice(v), label: fmtNum(v, 1) })),
    ivLabels: showIv && ivValues.length ? [maxIv, minIv].map(v => ({ top: yIv(v), label: fmtNum(v, 1) })) : [],
    hoverItems: rows.map((row, idx) => ({
      x: xAt(idx),
      date: row.label,
      open: fmtMoney(row.open),
      high: fmtMoney(row.high),
      low: fmtMoney(row.low),
      close: fmtMoney(row.close),
      iv: row.iv === null ? '--' : fmtPctPlain(row.iv),
    })),
    latestClose: fmtMoney(rows[rows.length - 1]?.close),
    latestIv: ivPts.length ? fmtPctPlain(ivPts[ivPts.length - 1].value) : '--',
  }
}

function toggleAtmIv() {
  showAtmIv.value = !showAtmIv.value
  chartHoverIndex.value = null
}

function clearLongPressTimer() {
  if (longPressTimer) {
    clearTimeout(longPressTimer)
    longPressTimer = null
  }
}

function chartTouches(e: any): any[] {
  const touches = e?.touches || e?.changedTouches || []
  return Array.isArray(touches) ? touches : Array.from(touches)
}

function touchClientX(touch: any): number {
  return Number(touch?.clientX ?? touch?.pageX ?? 0)
}

function touchDistance(touches: any[]): number {
  if (touches.length < 2) return 0
  const a = touches[0]
  const b = touches[1]
  const ax = Number(a?.clientX ?? a?.pageX ?? 0)
  const ay = Number(a?.clientY ?? a?.pageY ?? 0)
  const bx = Number(b?.clientX ?? b?.pageX ?? 0)
  const by = Number(b?.clientY ?? b?.pageY ?? 0)
  const dx = bx - ax
  const dy = by - ay
  return Math.sqrt(dx * dx + dy * dy)
}

function currentVisibleStart(): number {
  const total = priceIvRows.value.length
  if (!total) return 0
  const win = Math.min(chartWindow.value, total)
  return clampInt(chartStart.value, 0, Math.max(total - win, 0))
}

function setChartStart(nextStart: number) {
  const total = priceIvRows.value.length
  if (!total) return
  const win = Math.min(chartWindow.value, total)
  chartStart.value = clampInt(nextStart, 0, Math.max(total - win, 0))
}

function setChartWindow(nextWindow: number, centerIndex?: number) {
  const total = priceIvRows.value.length
  if (!total) return
  const minWin = Math.min(PRICE_IV_MIN_WINDOW, total)
  const win = clampInt(nextWindow, minWin, Math.min(PRICE_IV_MAX_WINDOW, total))
  const center = centerIndex ?? (currentVisibleStart() + Math.min(chartWindow.value, total) / 2)
  chartWindow.value = win
  chartStart.value = clampInt(center - win / 2, 0, Math.max(total - win, 0))
  chartHoverIndex.value = null
}

function updateChartRect() {
  try {
    uni.createSelectorQuery()
      .select('#priceIvChart')
      .boundingClientRect((rect: any) => {
        if (rect && Number(rect.width) > 0) {
          chartRect = { left: Number(rect.left || 0), width: Number(rect.width || 0) }
        }
      })
      .exec()
  } catch (_e) {
    chartRect = null
  }
}

function nearestHoverIndex(clientX: number): number | null {
  const chart = priceIvChart.value
  const count = chart.hoverItems.length
  if (!count) return null
  let plotLeftPx = chart.plotLeft
  let plotWidthPx = chart.plotWidth
  if (chartRect && chartRect.width > 0) {
    const pxPerRpx = chartRect.width / chart.width
    plotLeftPx = chartRect.left + chart.plotLeft * pxPerRpx
    plotWidthPx = chart.plotWidth * pxPerRpx
  }
  const ratio = Math.max(0, Math.min(1, (clientX - plotLeftPx) / Math.max(plotWidthPx, 1)))
  return clampInt(ratio * (count - 1), 0, count - 1)
}

function hoverGlobalIndexFromClientX(clientX: number): number {
  const localIdx = nearestHoverIndex(clientX)
  const start = currentVisibleStart()
  return start + (localIdx ?? Math.floor(priceIvVisibleRows.value.length / 2))
}

function updateChartHover(clientX: number) {
  const idx = nearestHoverIndex(clientX)
  chartHoverIndex.value = idx
}

function handlePriceIvTouchStart(e: any) {
  if (!priceIvChart.value.hasData) return
  updateChartRect()
  clearLongPressTimer()
  const touches = chartTouches(e)
  if (touches.length >= 2) {
    chartTouchMode = 'pinch'
    pinchStartDistance = touchDistance(touches)
    pinchStartWindow = chartWindow.value
    chartHoverIndex.value = null
    return
  }
  if (!touches.length) return
  chartTouchMode = 'idle'
  touchStartX = touchClientX(touches[0])
  touchStartStart = currentVisibleStart()
  longPressTimer = setTimeout(() => {
    chartTouchMode = 'long'
    updateChartHover(touchStartX)
  }, 420)
}

function handlePriceIvTouchMove(e: any) {
  if (!priceIvChart.value.hasData) return
  const touches = chartTouches(e)
  if (touches.length >= 2) {
    clearLongPressTimer()
    chartTouchMode = 'pinch'
    const distance = touchDistance(touches)
    if (pinchStartDistance > 0 && distance > 0) {
      const centerX = (touchClientX(touches[0]) + touchClientX(touches[1])) / 2
      const centerIndex = hoverGlobalIndexFromClientX(centerX)
      setChartWindow(pinchStartWindow * (pinchStartDistance / distance), centerIndex)
    }
    return
  }
  if (!touches.length) return
  const x = touchClientX(touches[0])
  if (chartTouchMode === 'long') {
    updateChartHover(x)
    return
  }
  const dx = x - touchStartX
  if (Math.abs(dx) < 8) return
  clearLongPressTimer()
  chartTouchMode = 'pan'
  const chart = priceIvChart.value
  const plotPx = chartRect && chartRect.width > 0
    ? chart.plotWidth * (chartRect.width / chart.width)
    : chart.plotWidth
  const visibleCount = Math.max(priceIvVisibleRows.value.length - 1, 1)
  const pxPerRow = Math.max(plotPx / visibleCount, 2)
  const deltaRows = Math.round(dx / pxPerRow)
  setChartStart(touchStartStart - deltaRows)
  chartHoverIndex.value = null
}

function handlePriceIvTouchEnd() {
  clearLongPressTimer()
  chartTouchMode = 'idle'
  pinchStartDistance = 0
  chartHoverIndex.value = null
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
      points: line.points.filter(p => p.x && asNum(p.y) !== null).map(p => ({ x: p.x, xSort: asNum(p.xSort), y: asNum(p.y) as number })),
    }))
    .filter(line => line.points.length > 0)

  const xEntries: Array<{ label: string; sort: number | null; order: number }> = []
  for (const line of cleanLines) {
    for (const p of line.points) {
      if (!xEntries.some(item => item.label === p.x)) {
        xEntries.push({ label: p.x, sort: p.xSort, order: xEntries.length })
      }
    }
  }
  const canSortX = xEntries.length > 0 && xEntries.every(item => item.sort !== null)
  const xValues = [...xEntries]
    .sort((a, b) => canSortX ? (a.sort as number) - (b.sort as number) : a.order - b.order)
    .map(item => item.label)
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
    const pts = line.points
      .map(p => ({ x: xPos(p.x), y: yPos(p.y) }))
      .sort((a, b) => a.x - b.x)
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

function candleWickStyle(candle: CandleShape) {
  return {
    left: `${candle.x}rpx`,
    top: `${candle.highTop}rpx`,
    height: `${Math.max(candle.lowTop - candle.highTop, 1)}rpx`,
    background: candle.color,
  }
}

function candleBodyStyle(candle: CandleShape) {
  return {
    left: `${candle.x}rpx`,
    top: `${candle.bodyTop}rpx`,
    width: `${candle.width}rpx`,
    height: `${candle.bodyHeight}rpx`,
    background: candle.color,
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
      <view v-if="loading && !overview" class="center-tip">加载美股期权总览...</view>
      <view v-else>
        <view class="panel chart-panel-main">
          <view class="panel-head chart-head">
            <view>
              <text class="panel-title">价格与 IV</text>
              <text class="panel-sub chart-sub">收盘 {{ priceIvChart.latestClose }} · IV {{ priceIvChart.latestIv }}</text>
            </view>
            <view class="iv-toggle" :class="{ off: !showAtmIv }" @tap.stop="toggleAtmIv">
              <text>ATM IV {{ showAtmIv ? '显示' : '隐藏' }}</text>
            </view>
          </view>
          <view
            v-if="priceIvChart.hasData"
            id="priceIvChart"
            class="price-iv-chart"
            @touchstart="handlePriceIvTouchStart"
            @touchmove.stop.prevent="handlePriceIvTouchMove"
            @touchend="handlePriceIvTouchEnd"
            @touchcancel="handlePriceIvTouchEnd"
          >
            <view v-for="(c, idx) in priceIvChart.candles" :key="`wick-${idx}`" class="candle-wick" :style="candleWickStyle(c)" />
            <view v-for="(c, idx) in priceIvChart.candles" :key="`body-${idx}`" class="candle-body" :style="candleBodyStyle(c)" />
            <view v-for="seg in priceIvChart.ivSegments" :key="`iv-${seg.left}-${seg.top}-${seg.width}`" class="chart-seg iv-seg" :style="segmentStyle(seg)" />
            <text v-for="y in priceIvChart.priceLabels" :key="`py-${y.label}`" class="axis-y" :style="axisYStyle(y)">{{ y.label }}</text>
            <text v-for="y in priceIvChart.ivLabels" :key="`iy-${y.label}`" class="axis-y iv-axis-y" :style="axisYStyle(y)">{{ y.label }}</text>
            <text v-for="x in priceIvChart.xLabels" :key="`px-${x.label}`" class="axis-x" :style="axisXStyle(x)">{{ x.label }}</text>
            <view v-if="priceIvHover" class="crosshair-line" :style="priceIvHover.lineStyle" />
            <view v-if="priceIvHover" class="crosshair-tip" :style="priceIvHover.tooltipStyle">
              <text class="crosshair-date">{{ priceIvHover.date }}</text>
              <text>收 {{ priceIvHover.close }} · IV {{ priceIvHover.iv }}</text>
              <text>高 {{ priceIvHover.high }} / 低 {{ priceIvHover.low }}</text>
            </view>
            <view class="chart-band-label price-label">K线</view>
            <view v-if="showAtmIv" class="chart-band-label iv-label">ATM IV</view>
            <view class="chart-hint">拖动平移 · 双指缩放 · 长按查看</view>
          </view>
          <view v-else class="empty-chart empty-chart-large">暂无价格与 IV 图表</view>
        </view>

        <view class="panel profile-panel">
          <view class="panel-head">
            <text class="panel-title">{{ profileCard.symbol || selectedProduct.symbol }} · {{ profileCard.name || selectedProduct.name }}</text>
            <text class="panel-sub">{{ profileCard.type_label || (overview?.asset_type === 'etf' ? 'ETF' : '股票') }}</text>
          </view>
          <view class="profile-grid">
            <view v-for="item in profileItems" :key="item.label" class="profile-item">
              <text class="profile-label">{{ item.label }}</text>
              <text class="profile-text">{{ item.text }}</text>
            </view>
          </view>
          <view class="profile-hotline">
            <text>期权数据</text>
            <text>{{ profileCard.option_data || overview?.status_brief || '期权数据待更新' }}</text>
          </view>
        </view>

        <view class="panel metrics-panel">
          <view class="panel-head">
            <text class="panel-title">波动率与持仓速览</text>
            <text class="panel-sub">{{ overview?.display_date || overview?.trade_date || '--' }}</text>
          </view>
          <view class="snapshot-grid">
            <view v-for="card in snapshotCards" :key="card.label" class="snapshot-card">
              <view class="snapshot-top">
                <view>
                  <text class="snapshot-label">{{ card.label }}</text>
                  <text class="snapshot-sub">{{ card.sub }}</text>
                </view>
                <text class="snapshot-value" :class="`tone-${card.tone}`">{{ card.value }}</text>
              </view>
              <view class="thermo-row">
                <text>{{ card.percentileLabel }}</text>
                <text>{{ card.percentileText }}</text>
              </view>
              <view class="thermo-track">
                <view v-if="card.percentile !== null" class="thermo-marker" :style="{ left: `${card.percentileClamped}%` }" />
              </view>
              <text class="snapshot-sample">{{ card.sample }}</text>
            </view>
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
  padding: 22rpx 24rpx 20rpx;
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
  font-size: 38rpx;
  font-weight: 900;
  margin-top: 6rpx;
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
  margin-top: 18rpx;
  color: #8ea0ba;
  font-size: 22rpx;
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

.chart-panel-main {
  margin-top: 0;
  padding: 18rpx;
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

.chart-head {
  align-items: flex-start;
  gap: 16rpx;
}

.chart-sub {
  display: block;
  margin-top: 6rpx;
}

.iv-toggle {
  flex-shrink: 0;
  padding: 8rpx 14rpx;
  border-radius: 999rpx;
  color: #38bdf8;
  font-size: 20rpx;
  font-weight: 800;
  border: 1px solid rgba(56, 189, 248, 0.42);
  background: rgba(56, 189, 248, 0.1);
}

.iv-toggle.off {
  color: #8fa3c1;
  border-color: rgba(143, 163, 193, 0.28);
  background: rgba(15, 23, 42, 0.76);
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

.price-iv-chart {
  position: relative;
  width: 620rpx;
  height: 480rpx;
  max-width: 100%;
  overflow: hidden;
  border-radius: 16rpx;
  background:
    linear-gradient(rgba(148, 163, 184, 0.075) 1px, transparent 1px),
    linear-gradient(90deg, rgba(148, 163, 184, 0.075) 1px, transparent 1px),
    linear-gradient(180deg, rgba(19, 32, 54, 0.94), rgba(8, 16, 30, 0.96));
  background-size: 62rpx 46rpx, 62rpx 46rpx, auto;
  border: 1px solid #223653;
}

.price-iv-chart::before {
  content: "";
  position: absolute;
  left: 42rpx;
  top: 24rpx;
  bottom: 42rpx;
  border-left: 1px solid rgba(203, 213, 225, 0.32);
}

.price-iv-chart::after {
  content: "";
  position: absolute;
  left: 42rpx;
  right: 56rpx;
  bottom: 42rpx;
  border-bottom: 1px solid rgba(203, 213, 225, 0.36);
  box-shadow: 0 -414rpx 0 -0.5px rgba(203, 213, 225, 0.24);
}

.candle-wick {
  position: absolute;
  width: 2rpx;
  margin-left: -1rpx;
  border-radius: 2rpx;
  z-index: 3;
}

.candle-body {
  position: absolute;
  transform: translateX(-50%);
  border-radius: 3rpx;
  z-index: 4;
}

.iv-seg {
  z-index: 7;
  height: 4rpx;
  opacity: 0.9;
}

.chart-band-label {
  position: absolute;
  left: 52rpx;
  color: #7890b3;
  font-size: 20rpx;
  font-weight: 800;
  letter-spacing: 1rpx;
}

.price-label { top: 28rpx; }
.iv-label { top: 58rpx; color: #38bdf8; }

.chart-hint {
  position: absolute;
  right: 16rpx;
  bottom: 12rpx;
  color: #63738f;
  font-size: 18rpx;
  z-index: 9;
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
  z-index: 8;
}

.iv-axis-y {
  right: 70rpx;
  color: #7dd3fc;
}

.axis-x {
  position: absolute;
  bottom: 14rpx;
  transform: translateX(-50%);
  color: #dce5f5;
  font-size: 18rpx;
  font-weight: 700;
  z-index: 8;
}

.crosshair-line {
  position: absolute;
  top: 24rpx;
  bottom: 42rpx;
  width: 2rpx;
  background: rgba(226, 232, 240, 0.55);
  z-index: 10;
}

.crosshair-tip {
  position: absolute;
  top: 28rpx;
  width: 246rpx;
  padding: 10rpx 12rpx;
  border-radius: 12rpx;
  color: #dce7f8;
  font-size: 20rpx;
  line-height: 1.55;
  background: rgba(13, 24, 42, 0.94);
  border: 1px solid rgba(91, 132, 180, 0.58);
  z-index: 11;
}

.crosshair-tip text {
  display: block;
}

.crosshair-date {
  color: #f5c518;
  font-weight: 900;
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

.empty-chart-large {
  height: 480rpx;
}

.profile-panel {
  background:
    radial-gradient(circle at 0% 0%, rgba(56, 189, 248, 0.09), transparent 40%),
    #131c2e;
}

.profile-grid {
  display: grid;
  gap: 14rpx;
}

.profile-item {
  padding: 16rpx;
  border: 1px solid #223653;
  border-radius: 14rpx;
  background: #0f192b;
}

.profile-label {
  display: block;
  color: #f5c518;
  font-size: 22rpx;
  font-weight: 900;
}

.profile-text {
  display: block;
  margin-top: 8rpx;
  color: #d8e2f3;
  font-size: 24rpx;
  line-height: 1.55;
}

.profile-hotline {
  margin-top: 14rpx;
  padding: 16rpx;
  border-radius: 14rpx;
  background: rgba(56, 189, 248, 0.08);
  border: 1px solid rgba(56, 189, 248, 0.22);
}

.profile-hotline text:first-child {
  display: block;
  color: #38bdf8;
  font-size: 22rpx;
  font-weight: 900;
}

.profile-hotline text:last-child {
  display: block;
  margin-top: 8rpx;
  color: #cfe2ff;
  font-size: 23rpx;
  line-height: 1.5;
}

.snapshot-grid {
  display: grid;
  grid-template-columns: 1fr;
  gap: 14rpx;
}

.snapshot-card {
  padding: 18rpx;
  border-radius: 18rpx;
  background:
    radial-gradient(circle at 100% 0%, rgba(56, 189, 248, 0.08), transparent 38%),
    rgba(15, 25, 43, 0.92);
  border: 1px solid #213553;
}

.snapshot-top {
  display: flex;
  justify-content: space-between;
  align-items: flex-start;
  gap: 18rpx;
}

.snapshot-label {
  display: block;
  color: #f0f4ff;
  font-size: 25rpx;
  font-weight: 900;
}

.snapshot-sub {
  display: block;
  margin-top: 5rpx;
  color: #7f8fa9;
  font-size: 20rpx;
  line-height: 1.35;
}

.snapshot-value {
  flex-shrink: 0;
  color: #f0f4ff;
  font-size: 31rpx;
  font-weight: 950;
}

.thermo-row {
  display: flex;
  justify-content: space-between;
  align-items: center;
  margin-top: 16rpx;
  color: #8ea0bb;
  font-size: 20rpx;
}

.thermo-track {
  position: relative;
  height: 12rpx;
  margin-top: 9rpx;
  border-radius: 999rpx;
  background: linear-gradient(90deg, #1d4ed8 0%, #22c55e 34%, #f5c518 64%, #ef4444 100%);
  box-shadow: inset 0 0 0 1px rgba(255, 255, 255, 0.12);
}

.thermo-marker {
  position: absolute;
  top: -8rpx;
  width: 8rpx;
  height: 28rpx;
  margin-left: -4rpx;
  border-radius: 999rpx;
  background: #ffffff;
  box-shadow: 0 0 0 3rpx rgba(255, 255, 255, 0.16), 0 0 14rpx rgba(245, 197, 24, 0.45);
}

.snapshot-sample {
  display: block;
  margin-top: 10rpx;
  color: #657590;
  font-size: 19rpx;
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
