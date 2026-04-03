<script setup lang="ts">
import { ref, computed, watch, nextTick, onUnmounted } from 'vue'
import { onShow, onHide } from '@dcloudio/uni-app'
import { useAuthStore } from '../../store/auth'
import BottomNav from '../../components/BottomNav.vue'
import { klineApi, type KlineBar, type KlineData, type KlineTradeEvent, type LbRow } from '../../api/index'

const auth = useAuthStore()

// ── Module-level cache (survives tab switches & component remounts) ──
// Stores successfully loaded K-line data between navigation events.
// Cleared only once the game actually starts playing.
let _cache: KlineData | null = null
// Promise for the background startRecord call, awaited before finishGame.
let _startRecordPromise: Promise<void> | null = null
// Whether the kline page is currently visible (false while user is on another tab)
let isActive = false
// Prevent double-invocation of startGame (e.g. double-tap)
let _startingGame = false
let _resizeBound = false

const isLandscape = ref(false)

function updateOrientation(windowWidth?: number, windowHeight?: number) {
  let ww = Number(windowWidth || 0)
  let wh = Number(windowHeight || 0)

  if (!ww || !wh) {
    try {
      const info = uni.getSystemInfoSync()
      ww = Number((info as any)?.windowWidth || 0)
      wh = Number((info as any)?.windowHeight || 0)
    } catch (_) {}
  }

  if (ww > 0 && wh > 0) {
    isLandscape.value = ww > wh
  }
}

function handleWindowResize(res: any) {
  const size = res?.size || res || {}
  updateOrientation(size.windowWidth, size.windowHeight)
  // #ifdef MP-WEIXIN
  if (phase.value === 'playing') {
    nextTick(() => syncMpCanvasSize(true))
  }
  // #endif
}

function bindWindowResize() {
  if (_resizeBound) return
  const u = uni as any
  if (typeof u.onWindowResize === 'function') {
    u.onWindowResize(handleWindowResize)
    _resizeBound = true
  }
}

function unbindWindowResize() {
  if (!_resizeBound) return
  const u = uni as any
  if (typeof u.offWindowResize === 'function') {
    u.offWindowResize(handleWindowResize)
  }
  _resizeBound = false
}


onShow(() => {
  bindWindowResize()
  updateOrientation()
  isActive = true
  if (!auth.isLoggedIn) { uni.reLaunch({ url: '/pages/login/index' }); return }
  // Only resume if we were actively loading (user explicitly started loading).
  // Do NOT trigger on phase==='idle': that would auto-start a game with stale
  // _cache left over from a previous concurrent doLoadData call or a remounted component.
  if (_cache && phase.value === 'loading') {
    beginPlaying()
  } else if (phase.value === 'idle') {
    // Safety: clear any stale cache when returning to idle screen
    _cache = null
    loadEntryData()
  } else if (phase.value === 'playing') {
    // #ifdef MP-WEIXIN
    nextTick(() => syncMpCanvasSize(true))
    // #endif
  }
})

onHide(() => {
  isActive = false
  unbindWindowResize()
})

// ── SVG layout ─────────────────────────────────────────────
const SVG_W   = 750
const PAD_L   = 4
const PAD_R   = 60   // y-axis labels
const PAD_T   = 16
const PAD_B   = 24
const PRICE_H = 190  // candle area height
const SUB_H   = 76   // volume / macd pane height
const GAP     = 10   // gap between price and sub pane
const SVG_H   = PAD_T + PRICE_H + GAP + SUB_H + PAD_B  // ≈ 316

const SUB_TOP = PAD_T + PRICE_H + GAP  // y-start of sub pane
const CHART_WINDOW = 60
const mpCanvasW = ref(SVG_W)
const mpCanvasH = ref(SVG_H)

// ── Config selections ──────────────────────────────────────
const SPEED_OPTIONS = [
  { label: '1秒1根', ms: 1000 },
  { label: '3秒1根', ms: 3000 },
  { label: '5秒1根', ms: 5000 },
]
const LOT_OPTIONS = [1, 5, 10, 30]
const selectedSpeedIdx = ref(0)
const selectedLeverage = ref<1 | 10>(1)
const selectedLots = ref(1)

// ── Indicator toggles ──────────────────────────────────────
const showMA   = ref(false)
type SubPane = 'vol' | 'macd' | 'off'
const subPane  = ref<SubPane>('vol')

// ── Game phase ─────────────────────────────────────────────
type Phase = 'idle' | 'penalty' | 'loading' | 'playing' | 'finished'
const phase = ref<Phase>('idle')
const loadError  = ref('')
const tradeMsg   = ref('')
const saving     = ref(false)
const showBottomNav = computed(() => !(phase.value === 'playing' && isLandscape.value))

// ── Penalty state ──────────────────────────────────────────
const penaltyGameId = ref(0)
const penaltySymbol = ref('')
const penaltyAmount = ref(20000)
const abandonLoading = ref(false)

// ── Entry page data (capital + leaderboard) ────────────────
const entryCapital  = ref(0)
const lbData        = ref<{ capital: LbRow[]; max_profit: LbRow[]; streak: LbRow[] }>({ capital: [], max_profit: [], streak: [] })
const activeLbTab   = ref<'capital' | 'max_profit' | 'streak'>('capital')
const lbLoading     = ref(false)

const fmtCapital = computed(() => {
  const c = entryCapital.value
  if (!c) return '--'
  return (c / 10000).toFixed(2) + ' 万'
})
const capitalColorClass = computed(() => {
  const c = entryCapital.value
  if (!c || c === 100000) return 'cap-neutral'
  return c > 100000 ? 'cap-bull' : 'cap-bear'
})
const currentLbList = computed<LbRow[]>(() => lbData.value[activeLbTab.value] || [])

function fmtLbVal(val: number, tab: string) {
  if (tab === 'streak') return val + ' 连胜'
  if (tab === 'max_profit') {
    if (Math.abs(val) >= 10000) return (val >= 0 ? '+' : '') + (val / 10000).toFixed(1) + ' 万'
    return (val >= 0 ? '+' : '') + Math.round(val).toLocaleString() + ' 元'
  }
  return (val / 10000).toFixed(2) + ' 万'
}

async function loadEntryData() {
  lbLoading.value = true
  try {
    const res = await klineApi.getEntry()
    entryCapital.value = res.capital
    lbData.value = res.leaderboard
  } catch (_) {}
  lbLoading.value = false
}

async function acceptPenalty() {
  abandonLoading.value = true
  try { await klineApi.abandonGame(penaltyGameId.value) } catch (_) {}
  abandonLoading.value = false
  await doLoadData()
}

// ── Game data ──────────────────────────────────────────────
const gameId      = ref(0)
const symbol      = ref('')
const symbolName  = ref('')
const symbolType  = ref('')
const capital     = ref(100000)
const allBars     = ref<KlineBar[]>([])
const historyCount = ref(60)
const curIdx      = ref(59)

const totalPlayable = computed(() => allBars.value.length - historyCount.value)
const playedCount   = computed(() => Math.max(0, curIdx.value - (historyCount.value - 1)))
const isDone        = computed(() => curIdx.value >= allBars.value.length - 1)
const progressPct   = computed(() => totalPlayable.value ? playedCount.value / totalPlayable.value * 100 : 0)

// ── Visible bars ───────────────────────────────────────────
const visibleBars = computed(() => allBars.value.slice(0, curIdx.value + 1))
const currentPrice = computed(() => {
  const b = visibleBars.value; return b.length ? b[b.length - 1].c : 0
})
const prevPrice = computed(() => {
  const b = visibleBars.value; return b.length > 1 ? b[b.length - 2].c : currentPrice.value
})
const chartBars = computed(() => {
  const bars = visibleBars.value
  return bars.length <= CHART_WINDOW ? bars : bars.slice(bars.length - CHART_WINDOW)
})
// global index of first chart bar in allBars (for indicator lookup)
const chartStartGlobalIdx = computed(() => {
  const len = visibleBars.value.length
  return len <= CHART_WINDOW ? 0 : len - CHART_WINDOW
})

// ── Position ───────────────────────────────────────────────
interface Pos { direction: 'long' | 'short' | null; lots: number; avgPrice: number; totalCost: number }
const pos         = ref<Pos>({ direction: null, lots: 0, avgPrice: 0, totalCost: 0 })
const cash        = ref(0)
const realizedPnl = ref(0)
const floatingPnl = ref(0)
const maxProfit   = ref(0)
const maxDrawdown = ref(0)
const tradeCount  = ref(0)

const totalPnl    = computed(() => realizedPnl.value + floatingPnl.value)
const totalPnlPct = computed(() => capital.value ? totalPnl.value / capital.value * 100 : 0)
const posValue    = computed(() => pos.value.lots * 1000 * selectedLeverage.value)

function calcPnL() {
  const p = pos.value
  if (!p.direction || !currentPrice.value) { floatingPnl.value = 0; return }
  const lev = selectedLeverage.value
  floatingPnl.value = p.direction === 'long'
    ? (currentPrice.value - p.avgPrice) * p.lots * 1000 / p.avgPrice * lev
    : (p.avgPrice - currentPrice.value) * p.lots * 1000 / p.avgPrice * lev
  const total = realizedPnl.value + floatingPnl.value
  if (total > maxProfit.value) maxProfit.value = total
  const dd = (maxProfit.value - total) / capital.value * 100
  if (dd > maxDrawdown.value) maxDrawdown.value = dd
}

function clonePos(p: Pos): Pos {
  return {
    direction: p.direction,
    lots: Number(p.lots || 0),
    avgPrice: Number(p.avgPrice || 0),
    totalCost: Number(p.totalCost || 0),
  }
}

function pushTradeEvent(action: string, lots: number, price: number, positionBefore: Pos) {
  if (!lots || lots <= 0) return
  const bar = allBars.value[curIdx.value]
  tradeEvents.value.push({
    trade_seq: tradeEvents.value.length + 1,
    action,
    trade_time: new Date().toISOString(),
    bar_index: curIdx.value,
    bar_date: bar?.dt || null,
    price: Number(price || 0),
    lots: Number(lots || 0),
    amount: Number((lots || 0) * 1000),
    leverage: Number(selectedLeverage.value || 1),
    position_before: clonePos(positionBefore),
    position_after: clonePos(pos.value),
    realized_pnl_after: Number(realizedPnl.value || 0),
    floating_pnl_after: Number(floatingPnl.value || 0),
  })
}

async function persistTradesOnce(): Promise<boolean> {
  if (tradePersisted.value) return true
  if (!gameId.value || !auth.username) return false
  if (!tradeEvents.value.length) {
    tradePersisted.value = true
    return true
  }
  if (tradePersistPromise) return tradePersistPromise

  tradePersistPromise = (async () => {
    try {
      await klineApi.saveTradeBatch({
        game_id: gameId.value,
        user_id: auth.username,
        symbol: symbol.value,
        symbol_name: symbolName.value,
        symbol_type: symbolType.value,
        trades: tradeEvents.value,
      })
      tradePersisted.value = true
      return true
    } catch (e) {
      console.error('[kline] saveTradeBatch failed', e)
      return false
    } finally {
      tradePersistPromise = null
    }
  })()
  return tradePersistPromise
}

// ── Trading ────────────────────────────────────────────────
function openPosition(dir: 'long' | 'short') {
  const lots = selectedLots.value
  const price = currentPrice.value
  const posBefore = clonePos(pos.value)
  const marginCost = lots * 1000
  const lev = selectedLeverage.value
  if (marginCost > cash.value) { showMsg(`资金不足，需 ${marginCost.toLocaleString()} 元`); return }
  if (lev === 10) {
    const equity = cash.value + pos.value.totalCost
    if (pos.value.totalCost + marginCost > equity * 0.5) { showMsg('10倍杠杆：保证金不能超过总资金50%'); return }
  }
  if (pos.value.direction && pos.value.direction !== dir) { showMsg(`请先平掉${pos.value.direction === 'long' ? '多' : '空'}仓`); return }
  if (!pos.value.direction) {
    pos.value = { direction: dir, lots, avgPrice: price, totalCost: marginCost }
  } else {
    const total = pos.value.lots + lots
    pos.value.avgPrice = (pos.value.lots * pos.value.avgPrice + lots * price) / total
    pos.value.lots = total
    pos.value.totalCost += marginCost
  }
  cash.value -= marginCost
  tradeCount.value++
  calcPnL()
  const action = posBefore.direction ? (dir === 'long' ? 'add_long' : 'add_short') : (dir === 'long' ? 'open_long' : 'open_short')
  pushTradeEvent(action, lots, price, posBefore)
}

function closePosition(lotsToClose?: number, force = false) {
  const p = pos.value; if (!p.direction) return
  const lots = Math.min(lotsToClose ?? selectedLots.value, p.lots)
  const price = currentPrice.value
  const posBefore = clonePos(p)
  const closingDir = p.direction
  const lev = selectedLeverage.value
  const pnl = p.direction === 'long'
    ? (price - p.avgPrice) * lots * 1000 / p.avgPrice * lev
    : (p.avgPrice - price) * lots * 1000 / p.avgPrice * lev
  cash.value += lots * 1000 + pnl
  realizedPnl.value += pnl
  if (!force) tradeCount.value++
  pos.value.lots -= lots
  pos.value.totalCost -= lots * 1000
  if (pos.value.lots <= 0) pos.value = { direction: null, lots: 0, avgPrice: 0, totalCost: 0 }
  floatingPnl.value = 0; calcPnL()
  const fullyClosed = lots >= (posBefore.lots || 0)
  const action = closingDir === 'long'
    ? (fullyClosed ? 'close_long_all' : 'close_long_partial')
    : (fullyClosed ? 'close_short_all' : 'close_short_partial')
  pushTradeEvent(action, lots, price, posBefore)
}

function closeAll(force = false) {
  if (!pos.value.direction) return
  closePosition(pos.value.lots, force)
}

function showMsg(msg: string) {
  tradeMsg.value = msg; setTimeout(() => tradeMsg.value = '', 2500)
}

// ── Button logic ───────────────────────────────────────────
const leftIsClose  = computed(() => pos.value.direction === 'short')
const rightIsClose = computed(() => pos.value.direction === 'long')
function onLeft()  { leftIsClose.value  ? closePosition() : openPosition('long') }
function onRight() { rightIsClose.value ? closePosition() : openPosition('short') }

// ── Indicator computation (on full allBars for correctness) ──
function calcSMA(closes: number[], period: number) {
  const out: (number | null)[] = new Array(closes.length).fill(null)
  for (let i = period - 1; i < closes.length; i++) {
    let sum = 0; for (let j = 0; j < period; j++) sum += closes[i - j]
    out[i] = sum / period
  }
  return out
}
function calcEMA(closes: number[], period: number) {
  const out: (number | null)[] = new Array(closes.length).fill(null)
  const k = 2 / (period + 1); let prev: number | null = null
  for (let i = 0; i < closes.length; i++) {
    const v = closes[i]; if (!Number.isFinite(v)) continue
    prev = prev === null ? v : v * k + prev * (1 - k)
    out[i] = prev
  }
  return out
}
function calcMACD(closes: number[]) {
  const ema12 = calcEMA(closes, 12)
  const ema26 = calcEMA(closes, 26)
  const dif: (number | null)[] = closes.map((_, i) =>
    ema12[i] != null && ema26[i] != null ? ema12[i]! - ema26[i]! : null)
  const dea = calcEMA(dif.map(v => v ?? NaN), 9)
  const hist = dif.map((d, i) => d != null && dea[i] != null ? 2 * (d - dea[i]!) : null)
  return { dif, dea, hist }
}

const indicatorCache = computed(() => {
  const bars = allBars.value
  if (!bars.length) return null
  const closes = bars.map(b => b.c)
  return {
    ma5:  calcSMA(closes, 5),
    ma20: calcSMA(closes, 20),
    ma60: calcSMA(closes, 60),
    macd: calcMACD(closes),
  }
})

// ── SVG chart paths ────────────────────────────────────────
const svgPaths = computed(() => {
  const bars = chartBars.value
  if (!bars.length) return { candles: [], volumes: [], xLabels: [], yLabels: [], priceY: 0, divY: 0, ma: [], macd: null, subLabel: '' }

  const hi = Math.max(...bars.map(b => b.h))
  const lo = Math.min(...bars.map(b => b.l))
  const priceRange = hi - lo || 1
  const maxVol = Math.max(...bars.map(b => b.v)) || 1
  const bw = (SVG_W - PAD_L - PAD_R) / bars.length
  const cw = Math.max(2, bw * 0.65)
  const startGIdx = chartStartGlobalIdx.value

  function py(price: number) {
    return PAD_T + (1 - (price - lo) / priceRange) * PRICE_H
  }

  // Candles
  const candles = bars.map((b, i) => {
    const x = PAD_L + (i + 0.5) * bw
    const isUp = b.c >= b.o
    const top   = py(Math.max(b.o, b.c))
    const bodyH = Math.max(1, py(Math.min(b.o, b.c)) - top)
    return { x, top, bodyH, wickTop: py(b.h), wickBot: py(b.l), cw, isUp }
  })

  // Volume bars
  const volumes = bars.map((b, i) => {
    const x = PAD_L + (i + 0.5) * bw
    const h = Math.max(1, b.v / maxVol * SUB_H)
    return { x, top: SUB_TOP + SUB_H - h, h, cw, isUp: b.c >= b.o }
  })

  // Y axis (4 labels on right)
  const yLabels: { y: number; label: string }[] = []
  for (let i = 0; i <= 3; i++) {
    const price = lo + (i / 3) * priceRange
    yLabels.push({ y: py(price), label: price >= 1000 ? price.toFixed(0) : price >= 10 ? price.toFixed(1) : price.toFixed(3) })
  }

  // X axis labels every ~10 bars
  const step = Math.max(1, Math.floor(bars.length / 5))
  const xLabels: { x: number; label: string }[] = []
  for (let i = 0; i < bars.length; i += step) {
    const dt = bars[i].dt
    xLabels.push({ x: PAD_L + (i + 0.5) * bw, label: dt ? dt.slice(4, 6) + '/' + dt.slice(6, 8) : '' })
  }

  // Current price Y
  const priceY = py(bars[bars.length - 1].c)

  // Divider line Y
  const divY = PAD_T + PRICE_H + GAP / 2

  // MA lines
  const ic = indicatorCache.value
  const MA_COLORS = ['#60a5fa', '#fbbf24', '#a78bfa']  // ma5 blue, ma20 yellow, ma60 purple
  const ma: { points: string; color: string; label: string }[] = []
  if (showMA.value && ic) {
    const maArrays = [ic.ma5, ic.ma20, ic.ma60]
    const maLabels = ['MA5', 'MA20', 'MA60']
    maArrays.forEach((arr, ai) => {
      const pts: string[] = []
      bars.forEach((_, i) => {
        const gIdx = startGIdx + i
        const v = arr[gIdx]
        if (v != null && v >= lo && v <= hi) {
          const x = PAD_L + (i + 0.5) * bw
          pts.push(`${x.toFixed(1)},${py(v).toFixed(1)}`)
        }
      })
      if (pts.length > 1) ma.push({ points: pts.join(' '), color: MA_COLORS[ai], label: maLabels[ai] })
    })
  }

  // MACD sub pane
  let macdPaths: { hist: { x: number; top: number; h: number; isPos: boolean; cw: number }[]; difPts: string; deaPts: string } | null = null
  if (subPane.value === 'macd' && ic) {
    const { dif, dea, hist } = ic.macd
    // find range for sub pane scaling
    const vals: number[] = []
    bars.forEach((_, i) => {
      const gIdx = startGIdx + i
      if (dif[gIdx] != null) vals.push(dif[gIdx]!)
      if (dea[gIdx] != null) vals.push(dea[gIdx]!)
      if (hist[gIdx] != null) vals.push(hist[gIdx]!)
    })
    if (vals.length) {
      const sMax = Math.max(...vals.map(Math.abs)) || 1
      const sy = (v: number) => SUB_TOP + SUB_H / 2 - (v / sMax) * (SUB_H / 2 - 2)
      const histBars = bars.map((_, i) => {
        const gIdx = startGIdx + i
        const v = hist[gIdx]
        const x = PAD_L + (i + 0.5) * bw
        if (v == null) return null
        const isPos = v >= 0
        const yMid = SUB_TOP + SUB_H / 2
        const h = Math.max(1, Math.abs(v) / sMax * (SUB_H / 2 - 2))
        return { x, top: isPos ? yMid - h : yMid, h, isPos, cw }
      }).filter(Boolean) as { x: number; top: number; h: number; isPos: boolean; cw: number }[]

      const difPts: string[] = []
      const deaPts: string[] = []
      bars.forEach((_, i) => {
        const gIdx = startGIdx + i; const x = PAD_L + (i + 0.5) * bw
        if (dif[gIdx] != null) difPts.push(`${x.toFixed(1)},${sy(dif[gIdx]!).toFixed(1)}`)
        if (dea[gIdx] != null) deaPts.push(`${x.toFixed(1)},${sy(dea[gIdx]!).toFixed(1)}`)
      })
      macdPaths = { hist: histBars, difPts: difPts.join(' '), deaPts: deaPts.join(' ') }
    }
  }

  const subLabel = subPane.value === 'vol' ? '成交量' : subPane.value === 'macd' ? 'MACD(12,26,9)' : ''

  return { candles, volumes, xLabels, yLabels, priceY, divY, ma, macd: macdPaths, subLabel }
})

function parsePointStr(points: string, sx: number, sy: number): Array<{ x: number; y: number }> {
  if (!points) return []
  return points
    .trim()
    .split(/\s+/)
    .map((pair) => {
      const [x, y] = pair.split(',')
      return { x: Number(x) * sx, y: Number(y) * sy }
    })
    .filter((p) => Number.isFinite(p.x) && Number.isFinite(p.y))
}

function drawPolyline(ctx: any, points: string, color: string, lineWidth: number, sx: number, sy: number) {
  const pts = parsePointStr(points, sx, sy)
  if (pts.length < 2) return
  ctx.setStrokeStyle(color)
  ctx.setLineWidth(lineWidth)
  ctx.beginPath()
  ctx.moveTo(pts[0].x, pts[0].y)
  for (let i = 1; i < pts.length; i += 1) ctx.lineTo(pts[i].x, pts[i].y)
  ctx.stroke()
}

function syncMpCanvasSize(drawAfter = false) {
  // #ifdef MP-WEIXIN
  const query = uni.createSelectorQuery()
  query.select('.chart-wrap').boundingClientRect((rect: any) => {
    let width = Math.round(Number(rect?.width || 0))
    let height = Math.round(Number(rect?.height || 0))
    if (!width) {
      try {
        width = Math.round(Number(uni.getSystemInfoSync()?.windowWidth || SVG_W))
      } catch (_) {
        width = SVG_W
      }
    }
    if (!(isLandscape.value && phase.value === 'playing')) {
      height = Math.round((width / SVG_W) * SVG_H)
    } else if (!height) {
      height = Math.round((width / SVG_W) * SVG_H)
    }
    mpCanvasW.value = Math.max(300, width)
    mpCanvasH.value = Math.max(160, height)
    if (drawAfter) drawMpCanvas()
  }).exec()
  // #endif
}

function drawMpCanvas() {
  // #ifdef MP-WEIXIN
  if (phase.value !== 'playing') return
  const w = mpCanvasW.value
  const h = mpCanvasH.value
  const sx = w / SVG_W
  const sy = h / SVG_H
  const paths = svgPaths.value
  const ctx = uni.createCanvasContext('klineGameCanvas')

  ctx.clearRect(0, 0, w, h)
  ctx.setFillStyle('#090f1c')
  ctx.fillRect(0, (PAD_T - 2) * sy, (SVG_W - PAD_R) * sx, (PRICE_H + 4) * sy)
  if (subPane.value !== 'off') {
    ctx.setFillStyle('#07111f')
    ctx.fillRect(0, SUB_TOP * sy, (SVG_W - PAD_R) * sx, SUB_H * sy)
    ctx.setStrokeStyle('#1e2d45')
    ctx.setLineWidth(1)
    ctx.beginPath()
    ctx.moveTo(0, SUB_TOP * sy)
    ctx.lineTo((SVG_W - PAD_R) * sx, SUB_TOP * sy)
    ctx.stroke()
  }

  if (subPane.value === 'vol') {
    paths.volumes.forEach((v) => {
      ctx.setFillStyle(v.isUp ? 'rgba(239,68,68,0.45)' : 'rgba(34,197,94,0.45)')
      ctx.fillRect((v.x - v.cw / 2) * sx, v.top * sy, v.cw * sx, v.h * sy)
    })
  }

  if (subPane.value === 'macd' && paths.macd) {
    ctx.setStrokeStyle('#2a3a55')
    ctx.setLineWidth(1)
    if ((ctx as any).setLineDash) (ctx as any).setLineDash([3, 3], 0)
    ctx.beginPath()
    ctx.moveTo(0, (SUB_TOP + SUB_H / 2) * sy)
    ctx.lineTo((SVG_W - PAD_R) * sx, (SUB_TOP + SUB_H / 2) * sy)
    ctx.stroke()
    if ((ctx as any).setLineDash) (ctx as any).setLineDash([], 0)
    paths.macd.hist.forEach((b) => {
      ctx.setFillStyle(b.isPos ? 'rgba(239,68,68,0.55)' : 'rgba(34,197,94,0.55)')
      ctx.fillRect((b.x - b.cw / 2) * sx, b.top * sy, b.cw * sx, b.h * sy)
    })
    drawPolyline(ctx, paths.macd.difPts, '#60a5fa', 1.5, sx, sy)
    drawPolyline(ctx, paths.macd.deaPts, '#f97316', 1.5, sx, sy)
  }

  paths.candles.forEach((c) => {
    ctx.setStrokeStyle(c.isUp ? '#ef4444' : '#22c55e')
    ctx.setLineWidth(1.2)
    ctx.beginPath()
    ctx.moveTo(c.x * sx, c.wickTop * sy)
    ctx.lineTo(c.x * sx, c.wickBot * sy)
    ctx.stroke()
    ctx.setFillStyle(c.isUp ? '#ef4444' : '#22c55e')
    ctx.fillRect((c.x - c.cw / 2) * sx, c.top * sy, c.cw * sx, Math.max(1, c.bodyH * sy))
  })

  paths.ma.forEach((ma) => {
    drawPolyline(ctx, ma.points, ma.color, 1.4, sx, sy)
  })

  if (paths.priceY) {
    ctx.setStrokeStyle('#f5c518')
    ctx.setLineWidth(1)
    if ((ctx as any).setLineDash) (ctx as any).setLineDash([5, 4], 0)
    ctx.beginPath()
    ctx.moveTo(0, paths.priceY * sy)
    ctx.lineTo((SVG_W - PAD_R + 2) * sx, paths.priceY * sy)
    ctx.stroke()
    if ((ctx as any).setLineDash) (ctx as any).setLineDash([], 0)
  }

  ctx.setFillStyle('#090f1c')
  ctx.fillRect((SVG_W - PAD_R + 2) * sx, 0, (PAD_R - 2) * sx, h)

  if (subPane.value !== 'off') {
    ctx.setFillStyle('#3d5270')
    ctx.setFontSize(Math.max(10, Math.round(16 * sy)))
    ctx.fillText(paths.subLabel, 8 * sx, (SUB_TOP + 14) * sy)
  }

  ctx.setFillStyle('#4a6080')
  ctx.setFontSize(Math.max(10, Math.round(17 * sy)))
  paths.yLabels.forEach((y) => {
    ctx.fillText(y.label, (SVG_W - PAD_R + 6) * sx, (y.y + 4) * sy)
  })

  ctx.setFillStyle('#3d5270')
  ctx.setFontSize(Math.max(10, Math.round(17 * sy)))
  paths.xLabels.forEach((xl) => {
    ctx.fillText(xl.label, (xl.x - 12) * sx, (SVG_H - 4) * sy)
  })

  if (paths.priceY) {
    ctx.setFillStyle('#1e3a20')
    ctx.fillRect((SVG_W - PAD_R + 2) * sx, (paths.priceY - 10) * sy, (PAD_R - 2) * sx, 20 * sy)
    ctx.setFillStyle('#f5c518')
    ctx.setFontSize(Math.max(10, Math.round(16 * sy)))
    ctx.fillText(fmtN(currentPrice.value, pDec(currentPrice.value)), (SVG_W - PAD_R + 6) * sx, (paths.priceY + 5) * sy)
  }

  ctx.draw()
  // #endif
}

watch([svgPaths, phase, isLandscape], () => {
  // #ifdef MP-WEIXIN
  if (phase.value !== 'playing') return
  nextTick(() => syncMpCanvasSize(true))
  // #endif
}, { deep: true })

// ── Auto-play ──────────────────────────────────────────────
let autoTimer: ReturnType<typeof setInterval> | null = null

function startTimer() {
  stopTimer()
  const ms = SPEED_OPTIONS[selectedSpeedIdx.value].ms
  autoTimer = setInterval(() => {
    if (isDone.value) { finishGame(); return }
    curIdx.value++; calcPnL()
    if (isDone.value) finishGame()
  }, ms)
}
function stopTimer() { if (autoTimer) { clearInterval(autoTimer); autoTimer = null } }
onUnmounted(() => {
  stopTimer()
  unbindWindowResize()
})

// ── Game lifecycle ─────────────────────────────────────────
// ── Step 1: user taps "开始游戏" → check for abandoned game → load data ──
async function startGame() {
  if (_startingGame) return   // prevent double-tap / concurrent invocation
  _startingGame = true
  loadError.value = ''
  stopTimer()
  // Check for abandoned game BEFORE loading (no DB record exists yet during loading)
  try {
    const check = await klineApi.checkUnfinished()
    if (check.has_unfinished && check.game_id) {
      penaltyGameId.value = check.game_id
      penaltySymbol.value = check.symbol_name || '???'
      penaltyAmount.value = check.penalty || 20000
      phase.value = 'penalty'
      _startingGame = false; return
    }
  } catch (_) {}
  await doLoadData()
  _startingGame = false
}

// ── Step 2: fetch K-line data (no DB record created here) ──
async function doLoadData() {
  phase.value = 'loading'
  _cache = null
  try {
    const res = await klineApi.getData()   // pure data, no game_id
    _cache = res
    // Only begin playing immediately if the user is still on this page.
    // If user navigated away, onShow() will call beginPlaying() on return.
    if (phase.value === 'loading' && isActive) {
      beginPlaying()
    }
  } catch (e: any) {
    _cache = null
    loadError.value = e?.message || '加载失败，请重试'
    phase.value = 'idle'
  }
}

// ── Step 3: apply cached data to component and start the game ──
function beginPlaying() {
  const data = _cache
  if (!data) return
  _cache = null   // consumed

  // Reset game state
  allBars.value      = data.bars
  symbol.value       = data.symbol
  symbolName.value   = data.symbol_name
  symbolType.value   = data.symbol_type
  capital.value      = data.capital
  historyCount.value = data.history_count
  curIdx.value       = data.history_count - 1
  pos.value          = { direction: null, lots: 0, avgPrice: 0, totalCost: 0 }
  cash.value         = data.capital
  realizedPnl.value  = 0; floatingPnl.value = 0
  maxProfit.value    = 0; maxDrawdown.value  = 0; tradeCount.value = 0
  selectedLots.value = 1
  gameId.value       = 0   // will be set after startRecord resolves
  tradeEvents.value  = []
  tradePersisted.value = false
  tradePersistPromise = null

  phase.value = 'playing'
  startTimer()
  // #ifdef MP-WEIXIN
  nextTick(() => syncMpCanvasSize(true))
  // #endif

  // NOW create the DB record (game is officially running from this point)
  // Any future navigation-away is a real abandon and deserves penalty
  const lev = selectedLeverage.value
  const speedSec = Math.round(SPEED_OPTIONS[selectedSpeedIdx.value].ms / 1000)
  _startRecordPromise = klineApi
    .startRecord({ symbol: data.symbol, symbol_name: data.symbol_name, symbol_type: data.symbol_type, capital: data.capital, leverage: lev, speed: speedSec })
    .then(r => { gameId.value = r.game_id })
    .catch(() => {})
}

const finalProfit = ref(0)
const finalRate   = ref(0)
const tradeEvents = ref<KlineTradeEvent[]>([])
const tradePersisted = ref(false)
let tradePersistPromise: Promise<boolean> | null = null

async function finishGame() {
  stopTimer()
  if (pos.value.direction) closeAll(true)
  finalProfit.value = realizedPnl.value
  finalRate.value   = finalProfit.value / capital.value * 100
  phase.value = 'finished'; saving.value = true
  // Ensure the startRecord call completed so we have a valid game_id
  if (_startRecordPromise) { await _startRecordPromise; _startRecordPromise = null }
  try {
    const tradesOk = await persistTradesOnce()
    if (!tradesOk) {
      uni.showToast({ title: '交易明细保存失败，请重试本局结算', icon: 'none' })
      saving.value = false
      return
    }
    await klineApi.saveGame({ game_id: gameId.value, profit: finalProfit.value, profit_rate: finalRate.value, trade_count: tradeCount.value, max_drawdown: maxDrawdown.value, capital: capital.value })
    await loadEntryData()
  } catch (_) {}
  saving.value = false
}

function resetGame() {
  stopTimer()
  _cache = null
  _startRecordPromise = null
  phase.value = 'idle'
  allBars.value = []
  symbol.value = ''
  pos.value = { direction: null, lots: 0, avgPrice: 0, totalCost: 0 }
  tradeEvents.value = []
  tradePersisted.value = false
  tradePersistPromise = null
  loadEntryData()
}

// ── Helpers ────────────────────────────────────────────────
function fmtN(n: number, d = 2) { return n.toFixed(d) }
function fmtPnl(n: number) { return (n >= 0 ? '+' : '') + Math.round(n).toLocaleString() }
function fmtPct(n: number) { return (n >= 0 ? '+' : '') + fmtN(n, 2) + '%' }
function pDec(p: number) { return p < 10 ? 3 : p < 1000 ? 2 : 0 }
</script>

<template>
  <view class="page" :class="{ landscape: isLandscape, 'playing-landscape': isLandscape && phase === 'playing' }">

    <!-- ══ PENALTY ════════════════════════════════════════════ -->
    <view v-if="phase === 'penalty'" class="penalty-page">
      <text class="penalty-icon">⚠️</text>
      <text class="penalty-title">检测到未完成游戏</text>
      <text class="penalty-desc">上次游戏中途离开，需接受处罚才能开始新游戏</text>
      <view class="penalty-card">
        <view class="penalty-row"><text class="pk">上局品种</text><text class="pv">{{ penaltySymbol }}</text></view>
        <view class="penalty-row"><text class="pk">惩罚金额</text><text class="pv bear">-{{ penaltyAmount.toLocaleString() }} 元</text></view>
      </view>
      <text class="penalty-rule">中途离开游戏视为放弃，固定扣除 2 万元作为惩罚</text>
      <view class="penalty-btn" :class="{ 'op30': abandonLoading }" @click="acceptPenalty">
        <text class="penalty-btn-text">{{ abandonLoading ? '处理中…' : '确认接受处罚，开始新游戏' }}</text>
      </view>
    </view>

    <!-- ══ IDLE ══════════════════════════════════════════════ -->
    <view v-else-if="phase === 'idle'" class="start-page">

      <!-- Hero -->
      <view class="hero-wrap">
        <!-- Candlestick SVG icon -->
        <svg width="64" height="48" viewBox="0 0 64 48" fill="none" xmlns="http://www.w3.org/2000/svg" style="display:block;margin:0 auto 16rpx;">
          <defs>
            <filter id="glow-r"><feGaussianBlur stdDeviation="1.5" result="blur"/><feMerge><feMergeNode in="blur"/><feMergeNode in="SourceGraphic"/></feMerge></filter>
            <filter id="glow-g"><feGaussianBlur stdDeviation="1.5" result="blur"/><feMerge><feMergeNode in="blur"/><feMergeNode in="SourceGraphic"/></feMerge></filter>
          </defs>
          <!-- candle 1: bear/green -->
          <line x1="12" y1="5" x2="12" y2="12" stroke="#22c55e" stroke-width="1.5" stroke-linecap="round" filter="url(#glow-g)"/>
          <rect x="9" y="12" width="6" height="18" fill="#22c55e" rx="1" filter="url(#glow-g)"/>
          <line x1="12" y1="30" x2="12" y2="38" stroke="#22c55e" stroke-width="1.5" stroke-linecap="round"/>
          <!-- candle 2: bull/red, tallest -->
          <line x1="32" y1="2" x2="32" y2="10" stroke="#ef4444" stroke-width="1.5" stroke-linecap="round" filter="url(#glow-r)"/>
          <rect x="29" y="10" width="6" height="26" fill="#ef4444" rx="1" filter="url(#glow-r)"/>
          <line x1="32" y1="36" x2="32" y2="44" stroke="#ef4444" stroke-width="1.5" stroke-linecap="round"/>
          <!-- candle 3: bull/red -->
          <line x1="52" y1="10" x2="52" y2="18" stroke="#ef4444" stroke-width="1.5" stroke-linecap="round" filter="url(#glow-r)"/>
          <rect x="49" y="18" width="6" height="16" fill="#ef4444" rx="1" filter="url(#glow-r)"/>
          <line x1="52" y1="34" x2="52" y2="42" stroke="#ef4444" stroke-width="1.5" stroke-linecap="round"/>
        </svg>
        <text class="hero-title">K线游戏</text>
        <text class="hero-sub">KLINE TRAINING SIMULATOR</text>

        <!-- Capital display -->
        <view class="capital-card">
          <text class="cap-label">当前积分</text>
          <text :class="['cap-value', capitalColorClass]">
            {{ lbLoading ? '加载中…' : fmtCapital }}
          </text>
        </view>
      </view>

      <!-- Config -->
      <view class="config-card">
        <picker class="cfg-picker" mode="selector"
          :range="SPEED_OPTIONS" range-key="label"
          :value="selectedSpeedIdx"
          @change="(e: any) => selectedSpeedIdx = +e.detail.value">
          <view class="cfg-row">
            <text class="cfg-key">播放速度</text>
            <view class="cfg-val-wrap">
              <text class="cfg-val">{{ SPEED_OPTIONS[selectedSpeedIdx].label }}</text>
              <text class="cfg-arrow">›</text>
            </view>
          </view>
        </picker>
        <view class="cfg-sep" />
        <picker class="cfg-picker" mode="selector"
          :range="['1倍', '10倍']"
          :value="selectedLeverage === 1 ? 0 : 1"
          @change="(e: any) => selectedLeverage = +e.detail.value === 0 ? 1 : 10">
          <view class="cfg-row">
            <text class="cfg-key">倍数</text>
            <view class="cfg-val-wrap">
              <text class="cfg-val">{{ selectedLeverage === 1 ? '1倍' : '10倍' }}</text>
              <text class="cfg-arrow">›</text>
            </view>
          </view>
        </picker>
      </view>

      <!-- Penalty warning -->
      <view class="penalty-tip">
        <text class="penalty-tip-icon">⚠</text>
        <text class="penalty-tip-text">中途非正常退出游戏将被扣除 2 万元惩罚</text>
      </view>

      <view v-if="loadError" class="err-box"><text class="err-text">{{ loadError }}</text></view>
      <view class="start-btn" @click="startGame">
        <text class="start-btn-text">开始游戏</text>
      </view>

      <!-- Leaderboard -->
      <view class="lb-wrap">
        <text class="lb-heading">排行榜</text>
        <view class="lb-tab-bar">
          <view class="lb-tab" :class="{ 'lb-tab-active': activeLbTab === 'capital' }" @click="activeLbTab = 'capital'">
            <text class="lb-tab-text">积分榜</text>
          </view>
          <view class="lb-tab" :class="{ 'lb-tab-active': activeLbTab === 'max_profit' }" @click="activeLbTab = 'max_profit'">
            <text class="lb-tab-text">最高得分</text>
          </view>
          <view class="lb-tab" :class="{ 'lb-tab-active': activeLbTab === 'streak' }" @click="activeLbTab = 'streak'">
            <text class="lb-tab-text">连胜榜</text>
          </view>
        </view>
        <view v-if="lbLoading" class="lb-loading"><text class="lb-loading-text">加载中…</text></view>
        <view v-else-if="!currentLbList.length" class="lb-loading"><text class="lb-loading-text">暂无数据</text></view>
        <view v-else class="lb-list">
          <view v-for="(item, i) in currentLbList" :key="item.user_id" class="lb-row">
            <text :class="['lb-rank', i === 0 ? 'rank-gold' : i === 1 ? 'rank-silver' : i === 2 ? 'rank-bronze' : 'rank-normal']">
              {{ i + 1 }}
            </text>
            <text class="lb-name">{{ item.user_id }}</text>
            <text :class="['lb-val', activeLbTab === 'max_profit' && item.value > 0 ? 'bull' : activeLbTab === 'max_profit' && item.value < 0 ? 'bear' : '']">
              {{ fmtLbVal(item.value, activeLbTab) }}
            </text>
          </view>
        </view>
      </view>

      <view class="compliance-note-wrap">
        <text class="compliance-note-text">本功能仅用于历史数据学习与练习，不构成操作建议。</text>
      </view>

    </view>

    <!-- ══ LOADING ════════════════════════════════════════════ -->
    <view v-else-if="phase === 'loading'" class="loading-page">
      <view class="spinner" />
      <text class="loading-text">正在加载K线数据…</text>
      <text class="loading-sub">随机抽取历史行情，请稍候</text>
    </view>

    <!-- ══ PLAYING ════════════════════════════════════════════ -->
    <view v-else-if="phase === 'playing'" class="game-wrap">

      <!-- Top bar -->
      <view class="game-topbar">
        <view class="topbar-price">
          <text :class="['cur-price', currentPrice >= prevPrice ? 'bull' : 'bear']">
            {{ fmtN(currentPrice, pDec(currentPrice)) }}
          </text>
          <text :class="['price-chg', currentPrice >= prevPrice ? 'bull' : 'bear']">
            {{ currentPrice >= prevPrice ? '+' : '' }}{{ fmtN((currentPrice - prevPrice) / prevPrice * 100, 2) }}%
          </text>
        </view>
        <view class="progress-wrap">
          <view class="progress-track">
            <view class="progress-fill" :style="{ width: progressPct + '%' }" />
          </view>
          <text class="progress-text">{{ playedCount }}/{{ totalPlayable }}</text>
        </view>
        <view class="topbar-right">
          <text
            v-if="isLandscape"
            :class="[
              'mini-pnl',
              pos.direction ? (floatingPnl >= 0 ? 'bull' : 'bear') : 'mini-pnl-empty'
            ]"
          >
            浮{{ pos.direction ? fmtPnl(floatingPnl) : '--' }}
          </text>
          <text class="lev-badge">{{ selectedLeverage }}x</text>
          <view class="end-btn" @click="finishGame"><text class="end-text">结束</text></view>
        </view>
      </view>

      <!-- Account strip -->
      <view class="acct-strip">
        <view class="acct-col">
          <text class="acct-label">可用资金</text>
          <text class="acct-val">{{ Math.round(cash).toLocaleString() }}</text>
        </view>
        <view class="acct-col">
          <text class="acct-label">持仓市值</text>
          <text class="acct-val">{{ posValue.toLocaleString() }}</text>
        </view>
        <view class="acct-col">
          <text class="acct-label">浮动盈亏</text>
          <text :class="['acct-val', floatingPnl >= 0 ? 'bull' : 'bear']">{{ fmtPnl(floatingPnl) }}</text>
        </view>
        <view class="acct-col">
          <text class="acct-label">已实现</text>
          <text :class="['acct-val', realizedPnl >= 0 ? 'bull' : 'bear']">{{ fmtPnl(realizedPnl) }}</text>
        </view>
      </view>

      <!-- Pos row -->
      <view v-if="pos.direction" class="pos-row">
        <text :class="['pos-dir', pos.direction === 'long' ? 'bull' : 'bear']">
          {{ pos.direction === 'long' ? '多头' : '空头' }}
        </text>
        <text class="pos-detail">{{ pos.lots }}手 · 均价 {{ fmtN(pos.avgPrice, pDec(pos.avgPrice)) }}</text>
        <text :class="['pos-pct', totalPnl >= 0 ? 'bull' : 'bear']">{{ fmtPct(totalPnlPct) }}</text>
      </view>

      <!-- Indicator toolbar -->
      <view class="indicator-bar">
        <view class="ind-toggle" :class="{ 'ind-active': showMA }" @click="showMA = !showMA">
          <text class="ind-text">MA</text>
        </view>
        <text class="ind-sep">|</text>
        <view class="ind-toggle" :class="{ 'ind-active': subPane === 'vol' }" @click="subPane = 'vol'">
          <text class="ind-text">成交量</text>
        </view>
        <view class="ind-toggle" :class="{ 'ind-active': subPane === 'macd' }" @click="subPane = 'macd'">
          <text class="ind-text">MACD</text>
        </view>
        <view class="ind-toggle" :class="{ 'ind-active': subPane === 'off' }" @click="subPane = 'off'">
          <text class="ind-text">关闭</text>
        </view>
        <!-- MA legend -->
        <view v-if="showMA" class="ma-legend">
          <view class="ma-dot" style="background:#60a5fa;" /><text class="ma-leg-text">5</text>
          <view class="ma-dot" style="background:#fbbf24;" /><text class="ma-leg-text">20</text>
          <view class="ma-dot" style="background:#a78bfa;" /><text class="ma-leg-text">60</text>
        </view>
      </view>
      <view class="play-main">
        <view class="chart-col">
      <!-- Chart SVG -->
      <view class="chart-wrap">
        <!-- #ifdef MP-WEIXIN -->
        <canvas
          canvas-id="klineGameCanvas"
          class="chart-canvas"
          :style="{ width: mpCanvasW + 'px', height: mpCanvasH + 'px' }"
          :width="mpCanvasW"
          :height="mpCanvasH"
        />
        <!-- #endif -->
        <!-- #ifndef MP-WEIXIN -->
        <svg
          :width="SVG_W" :height="SVG_H"
          :viewBox="`0 0 ${SVG_W} ${SVG_H}`"
          :preserveAspectRatio="isLandscape && phase === 'playing' ? 'none' : 'xMidYMid meet'"
          xmlns="http://www.w3.org/2000/svg"
          :style="isLandscape && phase === 'playing' ? 'width:100%;height:100%;display:block;' : 'width:100%;display:block;'"
        >
          <!-- ── Price area background ── -->
          <rect x="0" :y="PAD_T - 2" :width="SVG_W - PAD_R" :height="PRICE_H + 4" fill="#090f1c" />

          <!-- ── Sub pane background ── -->
          <rect v-if="subPane !== 'off'"
            x="0" :y="SUB_TOP" :width="SVG_W - PAD_R" :height="SUB_H"
            fill="#07111f"
          />
          <!-- Sub pane top divider -->
          <line v-if="subPane !== 'off'"
            x1="0" :x2="SVG_W - PAD_R"
            :y1="SUB_TOP" :y2="SUB_TOP"
            stroke="#1e2d45" stroke-width="1.5"
          />
          <!-- Sub label -->
          <text v-if="subPane !== 'off'"
            x="8" :y="SUB_TOP + 14"
            font-size="16" fill="#3d5270"
          >{{ svgPaths.subLabel }}</text>
          <!-- MACD zero line -->
          <line v-if="subPane === 'macd'"
            x1="0" :x2="SVG_W - PAD_R"
            :y1="SUB_TOP + SUB_H / 2" :y2="SUB_TOP + SUB_H / 2"
            stroke="#2a3a55" stroke-width="1" stroke-dasharray="3 3"
          />

          <!-- ── Volume bars ── -->
          <template v-if="subPane === 'vol'">
            <rect v-for="(v,i) in svgPaths.volumes" :key="'vol'+i"
              :x="v.x - v.cw/2" :y="v.top" :width="v.cw" :height="v.h"
              :fill="v.isUp ? 'rgba(239,68,68,0.45)' : 'rgba(34,197,94,0.45)'"
            />
          </template>

          <!-- ── MACD ── -->
          <template v-if="subPane === 'macd' && svgPaths.macd">
            <rect v-for="(h,i) in svgPaths.macd.hist" :key="'mh'+i"
              :x="h.x - h.cw/2" :y="h.top" :width="h.cw" :height="h.h"
              :fill="h.isPos ? 'rgba(239,68,68,0.55)' : 'rgba(34,197,94,0.55)'"
            />
            <polyline v-if="svgPaths.macd.difPts"
              :points="svgPaths.macd.difPts" fill="none" stroke="#60a5fa" stroke-width="1.5"
            />
            <polyline v-if="svgPaths.macd.deaPts"
              :points="svgPaths.macd.deaPts" fill="none" stroke="#f97316" stroke-width="1.5"
            />
          </template>

          <!-- ── Candle wicks ── -->
          <line v-for="(c,i) in svgPaths.candles" :key="'wk'+i"
            :x1="c.x" :y1="c.wickTop" :x2="c.x" :y2="c.wickBot"
            :stroke="c.isUp ? '#ef4444' : '#22c55e'" stroke-width="1.5"
          />
          <!-- ── Candle bodies ── -->
          <rect v-for="(c,i) in svgPaths.candles" :key="'cd'+i"
            :x="c.x - c.cw/2" :y="c.top" :width="c.cw" :height="c.bodyH"
            :fill="c.isUp ? '#ef4444' : '#22c55e'"
          />

          <!-- ── MA lines ── -->
          <polyline v-for="(ma, i) in svgPaths.ma" :key="'ma'+i"
            :points="ma.points" fill="none" :stroke="ma.color" stroke-width="1.5" opacity="0.85"
          />

          <!-- ── Current price dashed line ── -->
          <line v-if="svgPaths.priceY"
            x1="0" :x2="SVG_W - PAD_R + 2"
            :y1="svgPaths.priceY" :y2="svgPaths.priceY"
            stroke="#f5c518" stroke-width="1" stroke-dasharray="5 4" opacity="0.5"
          />

          <!-- ── Y-axis label background strip ── -->
          <rect :x="SVG_W - PAD_R + 2" y="0" :width="PAD_R - 2" :height="SVG_H" fill="#090f1c" />

          <!-- ── Y-axis labels ── -->
          <text v-for="(y,i) in svgPaths.yLabels" :key="'yl'+i"
            :x="SVG_W - PAD_R + 6" :y="y.y + 4"
            text-anchor="start" font-size="17" fill="#4a6080"
          >{{ y.label }}</text>

          <!-- ── X-axis labels ── -->
          <text v-for="(xl,i) in svgPaths.xLabels" :key="'xl'+i"
            :x="xl.x" :y="SVG_H - 4"
            text-anchor="middle" font-size="17" fill="#3d5270"
          >{{ xl.label }}</text>

          <!-- ── Price tag on axis ── -->
          <rect v-if="svgPaths.priceY"
            :x="SVG_W - PAD_R + 2" :y="svgPaths.priceY - 10"
            :width="PAD_R - 2" height="20" fill="#1e3a20" rx="3"
          />
          <text v-if="svgPaths.priceY"
            :x="SVG_W - PAD_R + 6" :y="svgPaths.priceY + 5"
            font-size="17" fill="#f5c518" font-weight="bold"
          >{{ fmtN(currentPrice, pDec(currentPrice)) }}</text>
        </svg>
        <!-- #endif -->
      </view>

      <!-- Trade error message -->
      <view v-if="tradeMsg" class="trade-msg">
        <text class="trade-msg-text">{{ tradeMsg }}</text>
      </view>
        </view>

        <view class="trade-col">
      <!-- Lot selector -->
      <view class="lot-row">
        <text class="lot-label">手数</text>
        <view v-for="n in LOT_OPTIONS" :key="n"
          class="lot-chip" :class="{ 'lot-active': selectedLots === n }"
          @click="selectedLots = n">
          <text class="lot-chip-text">{{ n }}</text>
        </view>
      </view>

      <!-- ═══ TRADING BUTTONS ═══ -->
      <view class="trade-panel">
        <!-- 做多 / 平空 -->
        <view class="trade-btn-wrap" :class="{ 'op30': pos.direction === 'short' && !leftIsClose }" @click="onLeft">
          <view :class="['trade-big-btn', leftIsClose ? 'btn-close-short' : 'btn-long']">
            <text class="trade-big-label">{{ leftIsClose ? '平空' : '做多' }}</text>
            <text class="trade-big-lots">{{ selectedLots }}手</text>
            <text class="trade-big-hint">{{ leftIsClose ? '+ ' + fmtPnl(floatingPnl) : '买入做多' }}</text>
          </view>
        </view>

        <!-- 全部平仓 (middle) -->
        <view class="trade-btn-wrap trade-mid" @click="closeAll()">
          <view class="trade-big-btn btn-closeall" :class="{ 'op30': !pos.direction }">
            <text class="trade-closeall-label">全部平仓</text>
            <text class="trade-big-lots">{{ pos.lots }}手</text>
          </view>
        </view>

        <!-- 做空 / 平多 -->
        <view class="trade-btn-wrap" :class="{ 'op30': pos.direction === 'long' && !rightIsClose }" @click="onRight">
          <view :class="['trade-big-btn', rightIsClose ? 'btn-close-long' : 'btn-short']">
            <text class="trade-big-label">{{ rightIsClose ? '平多' : '做空' }}</text>
            <text class="trade-big-lots">{{ selectedLots }}手</text>
            <text class="trade-big-hint">{{ rightIsClose ? fmtPnl(floatingPnl) + ' →' : '卖出做空' }}</text>
          </view>
        </view>
      </view>

        </view>
      </view>
    </view>

    <!-- ══ FINISHED ═══════════════════════════════════════════ -->
    <view v-else-if="phase === 'finished'" class="result-page">
      <text class="result-sym">{{ symbolName }} · {{ symbolType === 'future' ? '期货' : symbolType === 'index' ? '指数' : '股票' }}</text>
      <text class="result-label">本局盈亏</text>
      <text :class="['result-pnl', finalProfit >= 0 ? 'bull' : 'bear']">{{ fmtPnl(finalProfit) }} 元</text>
      <text :class="['result-rate', finalProfit >= 0 ? 'bull' : 'bear']">{{ fmtPct(finalRate) }}</text>
      <view class="result-stats">
        <view class="stat-row"><text class="stat-k">初始资金</text><text class="stat-v">{{ (capital/10000).toFixed(2) }} 万</text></view>
        <view class="stat-row">
          <text class="stat-k">最终资金</text>
          <text :class="['stat-v', (capital+finalProfit)>=capital?'bull':'bear']">{{ ((capital+finalProfit)/10000).toFixed(2) }} 万</text>
        </view>
        <view class="stat-row"><text class="stat-k">倍数</text><text class="stat-v">{{ selectedLeverage }}倍</text></view>
        <view class="stat-row"><text class="stat-k">交易次数</text><text class="stat-v">{{ tradeCount }}</text></view>
        <view class="stat-row"><text class="stat-k">最大回撤</text><text class="stat-v bear">{{ fmtN(maxDrawdown, 2) }}%</text></view>
      </view>
      <text v-if="saving" class="saving-text">正在保存记录…</text>
      <view class="again-btn" @click="startGame"><text class="again-text">再来一局</text></view>
      <view class="back-btn" @click="resetGame"><text class="back-text">返回入口</text></view>
    </view>

    <view v-if="showBottomNav" style="height: 120rpx;" />
    <BottomNav v-if="showBottomNav" active="kline" />
  </view>
</template>

<style scoped>
.page { background: #0b1121; min-height: 100vh; color: #e8eaf0; }
.op30 { opacity: 0.3; }

/* ══ PENALTY ════════════════════════════════════════════════ */
.penalty-page { display:flex;flex-direction:column;align-items:center;padding:60rpx 44rpx;gap:28rpx; }
.penalty-icon  { font-size:80rpx;margin-top:20rpx; }
.penalty-title { font-size:40rpx;font-weight:800;color:#fca5a5; }
.penalty-desc  { font-size:24rpx;color:#94a3b8;text-align:center; }
.penalty-card  { width:100%;background:rgba(239,68,68,0.08);border:1px solid rgba(239,68,68,0.3);border-radius:16rpx;padding:24rpx 32rpx;display:flex;flex-direction:column;gap:18rpx; }
.penalty-row   { display:flex;justify-content:space-between; }
.pk { font-size:26rpx;color:#94a3b8; } .pv { font-size:28rpx;font-weight:700;color:#e8eaf0; }
.penalty-rule  { font-size:22rpx;color:#475569;text-align:center; }
.penalty-btn   { width:100%;padding:28rpx 0;border-radius:16rpx;background:linear-gradient(135deg,#dc2626,#b91c1c);text-align:center; }
.penalty-btn-text { font-size:28rpx;font-weight:700;color:#fff; }

/* ══ START ══════════════════════════════════════════════════ */
.start-page { display:flex;flex-direction:column;padding:36rpx 36rpx 0;gap:28rpx; }

/* Hero */
.hero-wrap {
  display:flex;flex-direction:column;align-items:center;
  padding:36rpx 32rpx 32rpx;
  background:linear-gradient(160deg,#0d1e38 0%,#061325 100%);
  border:1px solid #1e3a5f;
  border-radius:24rpx;
  gap:0;
  position:relative;
  overflow:hidden;
}
.hero-wrap::before {
  content:'';position:absolute;inset:0;
  background-image:
    linear-gradient(rgba(59,130,246,0.04) 1px, transparent 1px),
    linear-gradient(90deg, rgba(59,130,246,0.04) 1px, transparent 1px);
  background-size:40rpx 40rpx;
}
.hero-title {
  font-size:52rpx;font-weight:900;letter-spacing:6rpx;
  background:linear-gradient(135deg,#f5c518 0%,#ffffff 60%,#93c5fd 100%);
  -webkit-background-clip:text;-webkit-text-fill-color:transparent;
  background-clip:text;
  margin-top:4rpx;
}
.hero-sub {
  font-size:18rpx;letter-spacing:4rpx;color:#2a4a6e;
  font-weight:600;margin-top:4rpx;margin-bottom:24rpx;
}
.capital-card {
  width:100%;background:rgba(15,30,55,0.8);
  border:1px solid #1e3a5f;border-radius:16rpx;
  padding:20rpx 28rpx;
  display:flex;align-items:center;justify-content:space-between;
  margin-top:4rpx;
}
.cap-label { font-size:24rpx;color:#4a6a8a;font-weight:500; }
.cap-value { font-size:36rpx;font-weight:800; }
.cap-neutral { color:#e2e8f0; }
.cap-bull    { color:#ef4444; }
.cap-bear    { color:#22c55e; }

/* Config */
.config-card { background:#0e1929;border:1px solid #1e2d45;border-radius:16rpx;overflow:hidden; }
.cfg-picker { display:block; }
.cfg-row { display:flex;align-items:center;justify-content:space-between;padding:22rpx 28rpx; }
.cfg-key { font-size:26rpx;color:#94a3b8; }
.cfg-val-wrap { display:flex;align-items:center;gap:8rpx; }
.cfg-val { font-size:26rpx;font-weight:600;color:#f5c518; }
.cfg-arrow { font-size:28rpx;color:#334155;font-weight:300; }
.cfg-sep { height:1px;background:#1e2d45;margin:0 28rpx; }

/* Penalty warning */
.penalty-tip {
  display:flex;align-items:center;gap:12rpx;
  background:rgba(220,38,38,0.08);border:1px solid rgba(220,38,38,0.2);
  border-radius:12rpx;padding:16rpx 24rpx;
}
.penalty-tip-icon { font-size:24rpx;color:#f87171; }
.penalty-tip-text { font-size:22rpx;color:#f87171;line-height:1.5; }

.err-box { background:rgba(239,68,68,0.1);border:1px solid rgba(239,68,68,0.3);border-radius:12rpx;padding:16rpx 24rpx; }
.err-text { font-size:24rpx;color:#ef4444; }
.start-btn {
  background:linear-gradient(135deg,#f5c518,#e0a800);
  border-radius:18rpx;padding:28rpx;text-align:center;
  box-shadow:0 4rpx 24rpx rgba(245,197,24,0.35);
}
.start-btn-text { font-size:34rpx;font-weight:800;color:#0b1121; }

/* Leaderboard */
.lb-wrap { display:flex;flex-direction:column;gap:0;padding-bottom:40rpx; }
.lb-heading { font-size:26rpx;font-weight:700;color:#4a6080;letter-spacing:2rpx;margin-bottom:16rpx; }
.lb-tab-bar { display:flex;background:#0e1929;border-radius:12rpx;padding:6rpx;gap:4rpx;margin-bottom:12rpx; }
.lb-tab { flex:1;padding:14rpx 0;border-radius:8rpx;text-align:center; }
.lb-tab-active { background:#1e2d45; }
.lb-tab-text { font-size:22rpx;color:#4a6080; }
.lb-tab-active .lb-tab-text { color:#93c5fd;font-weight:700; }
.lb-loading { padding:32rpx 0;text-align:center; }
.lb-loading-text { font-size:24rpx;color:#334155; }
.lb-list { display:flex;flex-direction:column;gap:2rpx; }
.lb-row {
  display:flex;align-items:center;gap:0;
  padding:16rpx 16rpx;
  background:#080f1d;border-radius:10rpx;
}
.lb-rank { width:52rpx;font-size:24rpx;font-weight:800;text-align:center; }
.rank-gold   { color:#f5c518; }
.rank-silver { color:#94a3b8; }
.rank-bronze { color:#cd7c2c; }
.rank-normal { color:#2a3a55; }
.lb-name { flex:1;font-size:24rpx;color:#64748b;padding:0 12rpx; }
.lb-val  { font-size:24rpx;font-weight:700;color:#cbd5e1; }

.compliance-note-wrap {
  margin-top: 6rpx;
  padding: 0 8rpx 28rpx;
}

.compliance-note-text {
  display: block;
  text-align: center;
  font-size: 22rpx;
  line-height: 1.6;
  color: #556070;
}

/* ══ LOADING ═════════════════════════════════════════════════ */
.loading-page { display:flex;flex-direction:column;align-items:center;justify-content:center;padding:160rpx 60rpx;gap:28rpx; }
.spinner { width:80rpx;height:80rpx;border-radius:50%;border:6rpx solid rgba(148,163,184,0.2);border-top-color:#3b82f6;animation:spin 0.9s linear infinite; }
@keyframes spin { to { transform:rotate(360deg); } }
.loading-text { font-size:30rpx;color:#e8eaf0;font-weight:600; } .loading-sub { font-size:24rpx;color:#64748b; }

/* Game */
.game-wrap { display:flex;flex-direction:column;min-height:100vh; }
.play-main { display:flex;flex-direction:column; }
.chart-col { width:100%;min-width:0; }
.trade-col { width:100%;min-width:0; }

/* Top bar */
.game-topbar { display:flex;align-items:center;justify-content:space-between;padding:14rpx 20rpx;background:#0e1929;border-bottom:1px solid #1e2d45;gap:12rpx; }
.topbar-price { display:flex;align-items:baseline;gap:10rpx; }
.cur-price { font-size:36rpx;font-weight:800; } .price-chg { font-size:22rpx;font-weight:600; }
.progress-wrap { flex:1;display:flex;align-items:center;gap:10rpx; }
.progress-track { flex:1;height:6rpx;background:#1e2d45;border-radius:3rpx;overflow:hidden; }
.progress-fill  { height:100%;background:#3b82f6;transition:width 0.2s linear; }
.progress-text  { font-size:20rpx;color:#4a6080;white-space:nowrap; }
.topbar-right { display:flex;align-items:center;gap:12rpx; }
.mini-pnl { font-size:18rpx;font-weight:700;white-space:nowrap;text-shadow:0 1px 2px rgba(0,0,0,0.35); }
.mini-pnl-empty { color:#64748b; }
.lev-badge { font-size:20rpx;color:#f5c518;background:rgba(245,197,24,0.12);border:1px solid rgba(245,197,24,0.3);border-radius:8rpx;padding:4rpx 12rpx; }
.end-btn { padding:10rpx 20rpx;border-radius:10rpx;background:rgba(100,116,139,0.2);border:1px solid #334155; }
.end-text { font-size:22rpx;color:#94a3b8; }

/* Account strip */
.acct-strip { display:flex;justify-content:space-around;padding:12rpx 4rpx;background:#131c2e;border-bottom:1px solid #1e2d45; }
.acct-col { display:flex;flex-direction:column;align-items:center;gap:2rpx; }
.acct-label { font-size:18rpx;color:#3d5270; } .acct-val { font-size:22rpx;font-weight:600;color:#cbd5e1; }

/* Pos row */
.pos-row { display:flex;align-items:center;gap:14rpx;padding:10rpx 20rpx;background:rgba(14,25,41,0.7);border-bottom:1px solid #1e2d45; }
.pos-dir { font-size:24rpx;font-weight:800; } .pos-detail { font-size:20rpx;color:#64748b;flex:1; } .pos-pct { font-size:22rpx;font-weight:700; }

/* Indicator bar */
.indicator-bar { display:flex;align-items:center;gap:0;padding:8rpx 16rpx;background:#0d1626;border-bottom:1px solid #1a2840;flex-wrap:wrap; }
.ind-toggle { padding:8rpx 18rpx;border-radius:8rpx; }
.ind-active { background:rgba(59,130,246,0.2);border:1px solid rgba(59,130,246,0.4); }
.ind-text { font-size:22rpx;color:#64748b; } .ind-active .ind-text { color:#93c5fd;font-weight:600; }
.ind-sep { font-size:22rpx;color:#1e2d45;margin:0 6rpx; }
.ma-legend { display:flex;align-items:center;gap:8rpx;margin-left:16rpx; }
.ma-dot { width:16rpx;height:6rpx;border-radius:3rpx; }
.ma-leg-text { font-size:19rpx;color:#64748b; }

/* Chart */
.chart-wrap { background:#090f1c; }
.chart-canvas { display:block; }

/* Trade msg */
.trade-msg { background:rgba(239,68,68,0.1);border-bottom:1px solid rgba(239,68,68,0.2);padding:10rpx 20rpx;text-align:center; }
.trade-msg-text { font-size:22rpx;color:#fca5a5; }

/* Lot row */
.lot-row { display:flex;align-items:center;gap:12rpx;padding:12rpx 20rpx;background:#0e1929;border-bottom:1px solid #1e2d45; }
.lot-label { font-size:22rpx;color:#4a6080; }
.lot-chip { padding:10rpx 24rpx;border-radius:8rpx;background:#1a2540;border:1px solid #2a3a55; }
.lot-active { background:#1e3a5f;border-color:#3b82f6; }
.lot-chip-text { font-size:24rpx;color:#64748b; } .lot-active .lot-chip-text { color:#93c5fd;font-weight:700; }

/* Trade Buttons */
.trade-panel { display:flex;gap:10rpx;padding:16rpx 16rpx 20rpx;background:#07101f; }
.trade-btn-wrap { flex:1;display:flex; }
.trade-mid { flex:0.65; }

.trade-big-btn {
  flex:1;border-radius:18rpx;display:flex;flex-direction:column;
  align-items:center;justify-content:center;gap:6rpx;padding:22rpx 10rpx;
  position:relative;overflow:hidden;
}
/* long: red gradient */
.btn-long {
  background: linear-gradient(160deg, #dc2626 0%, #991b1b 100%);
  box-shadow: 0 4rpx 20rpx rgba(220,38,38,0.45), inset 0 1px 0 rgba(255,255,255,0.12);
  border: 1.5px solid rgba(239,68,68,0.6);
}
/* short: green gradient */
.btn-short {
  background: linear-gradient(160deg, #16a34a 0%, #14532d 100%);
  box-shadow: 0 4rpx 20rpx rgba(22,163,74,0.45), inset 0 1px 0 rgba(255,255,255,0.12);
  border: 1.5px solid rgba(34,197,94,0.6);
}
/* close short: amber accent */
.btn-close-short {
  background: linear-gradient(160deg, #b45309 0%, #92400e 100%);
  box-shadow: 0 4rpx 16rpx rgba(245,158,11,0.4), inset 0 1px 0 rgba(255,255,255,0.1);
  border: 1.5px solid rgba(251,191,36,0.5);
}
/* close long: teal accent */
.btn-close-long {
  background: linear-gradient(160deg, #0e7490 0%, #164e63 100%);
  box-shadow: 0 4rpx 16rpx rgba(6,182,212,0.4), inset 0 1px 0 rgba(255,255,255,0.1);
  border: 1.5px solid rgba(34,211,238,0.5);
}
/* close all */
.btn-closeall {
  background: linear-gradient(160deg, #374151 0%, #1f2937 100%);
  box-shadow: 0 4rpx 12rpx rgba(0,0,0,0.4), inset 0 1px 0 rgba(255,255,255,0.06);
  border: 1.5px solid #334155;
}

.trade-big-label { font-size:36rpx;font-weight:900;color:#fff;letter-spacing:2rpx; }
.trade-big-lots  { font-size:26rpx;font-weight:700;color:rgba(255,255,255,0.75); }
.trade-big-hint  { font-size:19rpx;color:rgba(255,255,255,0.45); }
.trade-closeall-label { font-size:28rpx;font-weight:800;color:#9ca3af;letter-spacing:2rpx; }

/* Landscape playing mode */
.page.playing-landscape {
  height: 100vh;
  height: 100dvh;
  overflow: hidden;
}
.page.playing-landscape .game-wrap {
  height: 100%;
  min-height: 0;
  overflow: hidden;
  padding-bottom: env(safe-area-inset-bottom);
  box-sizing: border-box;
}
.page.playing-landscape .game-topbar {
  padding: 2rpx 8rpx;
  gap: 6rpx;
}
.page.playing-landscape .game-topbar,
.page.playing-landscape .acct-strip,
.page.playing-landscape .pos-row,
.page.playing-landscape .indicator-bar {
  flex-shrink: 0;
}
.page.playing-landscape .topbar-price { gap: 6rpx; }
.page.playing-landscape .cur-price { font-size:24rpx; }
.page.playing-landscape .price-chg { font-size:15rpx; }
.page.playing-landscape .progress-track { height: 4rpx; }
.page.playing-landscape .progress-text { font-size: 14rpx; }
.page.playing-landscape .topbar-right { gap: 8rpx; flex-shrink: 0; }
.page.playing-landscape .mini-pnl {
  font-size: 14rpx;
  max-width: 120rpx;
  overflow: hidden;
  text-overflow: ellipsis;
}
.page.playing-landscape .lev-badge { font-size: 14rpx; padding: 2rpx 7rpx; }
.page.playing-landscape .end-btn { padding: 2rpx 9rpx; border-radius: 8rpx; }
.page.playing-landscape .end-text { font-size: 16rpx; }
.page.playing-landscape .acct-strip {
  padding: 1rpx 6rpx;
  gap: 6rpx;
  justify-content: space-between;
}
.page.playing-landscape .acct-col {
  flex: 1;
  flex-direction: row;
  align-items: baseline;
  gap: 2rpx;
  min-width: 0;
}
.page.playing-landscape .acct-label { font-size:12rpx;white-space:nowrap; }
.page.playing-landscape .acct-val { font-size:15rpx;white-space:nowrap; }
.page.playing-landscape .pos-row { padding:1rpx 8rpx; }
.page.playing-landscape .pos-dir { font-size: 15rpx; }
.page.playing-landscape .pos-detail { font-size:13rpx;white-space:nowrap;overflow:hidden;text-overflow:ellipsis; }
.page.playing-landscape .pos-pct { font-size: 15rpx; }
.page.playing-landscape .indicator-bar {
  padding:2rpx 6rpx;
  flex-wrap: nowrap;
  overflow-x: auto;
  -webkit-overflow-scrolling: touch;
}
.page.playing-landscape .indicator-bar::-webkit-scrollbar { display: none; }
.page.playing-landscape .ind-toggle {
  padding:3rpx 9rpx;
  flex-shrink: 0;
}
.page.playing-landscape .ind-text { font-size:15rpx; }
.page.playing-landscape .ma-legend {
  margin-left: 8rpx;
  transform: scale(0.9);
  transform-origin: left center;
}
.page.playing-landscape .play-main {
  flex: 1;
  min-height: 0;
  flex-direction: row;
  align-items: stretch;
}
.page.playing-landscape .chart-col {
  flex: 1;
  min-width: 0;
  min-height: 0;
  display: flex;
  flex-direction: column;
  position: relative;
}
.page.playing-landscape .chart-wrap {
  flex: 1;
  min-height: 0;
  display: flex;
}
.page.playing-landscape .chart-wrap svg {
  width: 100%;
  height: 100%;
  display: block;
}
.page.playing-landscape .chart-wrap .chart-canvas {
  width: 100% !important;
  height: 100% !important;
}
.page.playing-landscape .trade-col {
  width: 280rpx;
  max-width: 31vw;
  min-width: 236rpx;
  min-height: 0;
  overflow: hidden;
  display: flex;
  flex-direction: column;
  border-left: 1px solid #1e2d45;
  background: linear-gradient(180deg, #091426 0%, #050b16 100%);
}
.page.playing-landscape .trade-msg {
  position: absolute;
  left: 10rpx;
  right: 10rpx;
  bottom: 10rpx;
  z-index: 3;
  padding: 5rpx 8rpx;
  border-radius: 10rpx;
  background: rgba(239, 68, 68, 0.18);
  border: 1px solid rgba(239, 68, 68, 0.35);
}
.page.playing-landscape .trade-msg-text { font-size:14rpx; }
.page.playing-landscape .lot-row {
  display: flex;
  align-items: center;
  gap: 3rpx;
  padding: 2rpx 5rpx;
  border-bottom: 1px solid #1e2d45;
}
.page.playing-landscape .lot-label {
  width: auto;
  font-size: 12rpx;
  white-space: nowrap;
  margin-right: 2rpx;
}
.page.playing-landscape .lot-chip {
  flex: 1;
  min-width: 0;
  min-height: 30rpx;
  display: flex;
  align-items: center;
  justify-content: center;
  text-align: center;
  padding: 1rpx 0;
  border-radius: 8rpx;
}
.page.playing-landscape .lot-chip-text { font-size: 13rpx; }
.page.playing-landscape .trade-panel {
  flex: 1;
  min-height: 0;
  display: grid;
  grid-template-rows: minmax(0, 0.9fr) minmax(0, 0.58fr) minmax(0, 0.9fr);
  gap: 7rpx;
  padding: 3rpx 6rpx 5rpx;
  background: transparent;
}
.page.playing-landscape .trade-btn-wrap,
.page.playing-landscape .trade-mid {
  flex: none;
  min-height: 0;
}
.page.playing-landscape .trade-big-btn {
  height: 100%;
  min-height: 0;
  padding: 4rpx 5rpx;
  gap: 0;
  border-radius: 12rpx;
  box-shadow: 0 7rpx 18rpx rgba(0, 0, 0, 0.34);
  transition: transform 0.08s ease, box-shadow 0.12s ease;
}
.page.playing-landscape .trade-btn-wrap:active .trade-big-btn {
  transform: scale(0.965);
  box-shadow: 0 4rpx 12rpx rgba(0, 0, 0, 0.4);
}
.page.playing-landscape .trade-big-label {
  font-size: 22rpx;
  line-height: 1.02;
  text-shadow: 0 1px 4px rgba(0,0,0,0.35);
}
.page.playing-landscape .trade-big-lots {
  font-size: 13rpx;
  line-height: 1;
}
.page.playing-landscape .trade-big-hint { display:none; }
.page.playing-landscape .trade-closeall-label {
  font-size: 16rpx;
  line-height: 1.05;
  letter-spacing: 0;
  white-space: nowrap;
}

@media (max-height: 500px) {
  .page.playing-landscape .trade-col {
    width: 256rpx;
    min-width: 216rpx;
    max-width: 31vw;
  }
  .page.playing-landscape .lot-row { padding: 1rpx 4rpx; }
  .page.playing-landscape .lot-chip { min-height: 28rpx; }
  .page.playing-landscape .lot-chip-text { font-size: 12rpx; }
  .page.playing-landscape .trade-panel {
    grid-template-rows: minmax(0, 0.82fr) minmax(0, 0.5fr) minmax(0, 0.82fr);
    gap: 4rpx;
    padding: 2rpx 4rpx 4rpx;
  }
  .page.playing-landscape .trade-big-btn { padding: 3rpx 4rpx; }
  .page.playing-landscape .trade-big-label { font-size: 20rpx; }
  .page.playing-landscape .trade-big-lots { font-size: 12rpx; }
  .page.playing-landscape .trade-closeall-label { font-size: 14rpx; }
}

@media (max-height: 430px) {
  .page.playing-landscape .acct-strip,
  .page.playing-landscape .pos-row {
    display: none;
  }
  .page.playing-landscape .indicator-bar {
    padding: 1rpx 4rpx;
  }
  .page.playing-landscape .ind-toggle {
    padding: 1rpx 6rpx;
  }
  .page.playing-landscape .ind-text {
    font-size: 14rpx;
  }
  .page.playing-landscape .trade-col {
    width: 240rpx;
    min-width: 204rpx;
    max-width: 30vw;
    min-height: 0;
  }
  .page.playing-landscape .lot-row { padding: 2rpx 4rpx; }
  .page.playing-landscape .lot-chip { min-height: 30rpx; }
  .page.playing-landscape .lot-chip-text { font-size: 13rpx; }
  .page.playing-landscape .trade-panel { gap: 4rpx; padding: 3rpx 4rpx 4rpx; }
  .page.playing-landscape .trade-big-label { font-size: 21rpx; }
  .page.playing-landscape .trade-big-lots { font-size: 13rpx; }
}

/* Result */
.result-page { display:flex;flex-direction:column;align-items:center;padding:60rpx 44rpx;gap:16rpx; }
.result-sym   { font-size:28rpx;color:#64748b; } .result-label { font-size:24rpx;color:#94a3b8;margin-top:8rpx; }
.result-pnl   { font-size:68rpx;font-weight:900; } .result-rate  { font-size:38rpx;font-weight:700; }
.result-stats { width:100%;background:#131c2e;border:1px solid #1e2d45;border-radius:16rpx;padding:24rpx 32rpx;display:flex;flex-direction:column;gap:18rpx;margin-top:12rpx; }
.stat-row { display:flex;justify-content:space-between;align-items:center; }
.stat-k { font-size:26rpx;color:#94a3b8; } .stat-v { font-size:28rpx;font-weight:600;color:#e8eaf0; }
.saving-text { font-size:22rpx;color:#4a6080; }
.again-btn { width:100%;padding:28rpx 0;border-radius:16rpx;background:linear-gradient(135deg,#f5c518,#e0a800);text-align:center;margin-top:16rpx; }
.again-text { font-size:32rpx;font-weight:800;color:#0b1121; }
.back-btn  { width:100%;padding:20rpx 0;border-radius:16rpx;background:transparent;border:1px solid #334155;text-align:center; }
.back-text { font-size:28rpx;color:#64748b; }

/* Colors */
.bull { color:#ef4444; } .bear { color:#22c55e; }
</style>
