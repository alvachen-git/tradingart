<script setup lang="ts">
import { ref, computed, watch, nextTick, getCurrentInstance } from 'vue'
import { onShow, onHide, onShareAppMessage, onShareTimeline } from '@dcloudio/uni-app'
import {
  marketApi,
  type OptionItem,
  type ContractLiveItem,
  type ChaosSnapshotPayload,
  type TermProductItem,
  type TermWindowItem,
  type TermStructurePayload,
  type TermStructureBlock,
  type TermLongBlock,
} from '../../api/index'
import { useAuthStore } from '../../store/auth'
import BottomNav from '../../components/BottomNav.vue'

const auth = useAuthStore()
const SHARE_TITLE = '爱波塔 - 市场数据学习工具'
const SHARE_PATH = '/pages/login/index'

// ── Tab ──────────────────────────────────────────────────
type Tab = 'options' | 'holding' | 'chaos' | 'term'
const activeTab = ref<Tab>('options')

// ── 商品期权数据 ──────────────────────────────────────────
const options = ref<OptionItem[]>([])
const optUpdatedAt = ref('')
const optLoading = ref(false)

// ── 实时合约行情（方案B缓存，轮询融合进期权列表）────────────
// key = 大写合约代码，如 SR2605
const liveContracts = ref<Record<string, ContractLiveItem>>({})
// 品种级别回退：key = 品种代码 (cu/ni/rb)，value = 主力合约价格
const livePriceByProduct = ref<Record<string, number>>({})
const liveTrading   = ref(false)
const liveAt        = ref('')
let _liveTimer: ReturnType<typeof setInterval> | null = null

async function fetchLive() {
  try {
    const res = await marketApi.prices()
    liveContracts.value = res.contracts ?? {}
    // 品种级别回退 map：product_code → 主力合约价格
    const byProd: Record<string, number> = {}
    for (const it of (res.items ?? [])) {
      if (it.price > 0) byProd[it.code] = it.price
    }
    livePriceByProduct.value = byProd
    liveTrading.value   = res.is_trading
    liveAt.value        = res.refreshed_at
  } catch (_) {}
}

function startLivePolling() {
  if (_liveTimer) return
  fetchLive()
  _liveTimer = setInterval(fetchLive, 10000)
}

function stopLivePolling() {
  if (_liveTimer) { clearInterval(_liveTimer); _liveTimer = null }
}

/** 从 item.name (如 "sr2605 (白糖)") 提取大写合约代码 SR2605 */
function extractContractCode(name: string): string {
  return (name.match(/^([a-z]+\d+)/i)?.[1] ?? '').toUpperCase()
}

/** 获取某条期权行的实时涨跌幅（交易时段且有数据时返回，否则返回 null）*/
function livePct(item: OptionItem): number | null {
  if (!liveTrading.value) return null
  const code = extractContractCode(item.name)
  return liveContracts.value[code]?.pct ?? null
}

/** 显示价格：优先实时，回退到 DB 收盘价 */
function displayPrice(item: OptionItem): string | null {
  // 优先实时缓存（含收盘后最后一笔）
  const contractCode = extractContractCode(item.name)
  const liveP = liveContracts.value[contractCode]?.price
              ?? livePriceByProduct.value[item.product_code]
              ?? 0
  if (liveP > 0) return liveP >= 1000 ? liveP.toLocaleString() : liveP.toFixed(2)
  // DB 收盘价（始终可用）
  const p = item.cur_price ?? 0
  if (!p) return null
  return p >= 1000 ? p.toLocaleString() : p.toFixed(2)
}

// ── 持仓分析 ──────────────────────────────────────────────
const holdingProduct = ref('')
const holdingBrokers = ref<any[]>([])
const holdingDate = ref('')
const holdingLoading = ref(false)

// ── 混乱指数 ───────────────────────────────────────────────
const chaosData = ref<ChaosSnapshotPayload | null>(null)
const chaosLoading = ref(false)
const chaosError = ref('')

// ── 期限结构 ───────────────────────────────────────────────
const termProducts = ref<TermProductItem[]>([])
const termWindows = ref<TermWindowItem[]>([
  { key: '3d', label: '3交易日' },
  { key: '1w', label: '1周' },
  { key: '2w', label: '2周' },
  { key: '1m', label: '1月' },
])
const termProduct = ref('IH')
const termWindow = ref('3d')
const termData = ref<TermStructurePayload | null>(null)
const termLoading = ref(false)
const termProductsLoading = ref(false)
const termError = ref('')
const showTermPicker = ref(false)
const termSearch = ref('')
const showTermWindowPicker = ref(false)
const termMainZoom = ref(1)
const termBasisZoom = ref(1)
const termLongZoom = ref(1)
const termTooltip = ref<TermTooltipState | null>(null)
const termChartRects = ref<Record<TermChartKey, { left: number; top: number; width: number; height: number } | null>>({
  main: null,
  basis: null,
  long: null,
})
const pageInstance = getCurrentInstance()
let termLongPressTimer: ReturnType<typeof setTimeout> | null = null
let termPressKey: TermChartKey | null = null
let termPressActive = false
let termPressStartX = 0
let termPressStartY = 0
let termPinchKey: TermChartKey | null = null
let termPinchStartDistance = 0
let termPinchStartZoom = 1

// 与网站版 03_商品持仓.py COMMODITIES 保持相同顺序
const HOLDING_PRODUCTS = [
  { code: 'ih', name: '上证50' },
  { code: 'if', name: '沪深300' },
  { code: 'ic', name: '中证500' },
  { code: 'im', name: '中证1000' },
  { code: 'ts', name: '2年期国债' },
  { code: 't',  name: '10年期国债' },
  { code: 'tl', name: '30年期国债' },
  { code: 'lc', name: '碳酸锂' },
  { code: 'si', name: '工业硅' },
  { code: 'ps', name: '多晶硅' },
  { code: 'pt', name: '铂金' },
  { code: 'pd', name: '钯金' },
  { code: 'au', name: '黄金' },
  { code: 'ag', name: '白银' },
  { code: 'cu', name: '沪铜' },
  { code: 'al', name: '沪铝' },
  { code: 'zn', name: '沪锌' },
  { code: 'ni', name: '沪镍' },
  { code: 'sn', name: '沪锡' },
  { code: 'pb', name: '沪铅' },
  { code: 'ru', name: '橡胶' },
  { code: 'br', name: 'BR橡胶' },
  { code: 'i',  name: '铁矿石' },
  { code: 'jm', name: '焦煤' },
  { code: 'j',  name: '焦炭' },
  { code: 'rb', name: '螺纹钢' },
  { code: 'hc', name: '热卷' },
  { code: 'sp', name: '纸浆' },
  { code: 'lg', name: '原木' },
  { code: 'ao', name: '氧化铝' },
  { code: 'sh', name: '烧碱' },
  { code: 'fg', name: '玻璃' },
  { code: 'sa', name: '纯碱' },
  { code: 'm',  name: '豆粕' },
  { code: 'a',  name: '豆一' },
  { code: 'b',  name: '豆二' },
  { code: 'c',  name: '玉米' },
  { code: 'lh', name: '生猪' },
  { code: 'jd', name: '鸡蛋' },
  { code: 'cj', name: '红枣' },
  { code: 'p',  name: '棕榈油' },
  { code: 'y',  name: '豆油' },
  { code: 'oi', name: '菜油' },
  { code: 'l',  name: '塑料' },
  { code: 'pk', name: '花生' },
  { code: 'rm', name: '菜粕' },
  { code: 'ma', name: '甲醇' },
  { code: 'ta', name: 'PTA' },
  { code: 'pr', name: '瓶片' },
  { code: 'pp', name: '聚丙烯' },
  { code: 'v',  name: 'PVC' },
  { code: 'eb', name: '苯乙烯' },
  { code: 'eg', name: '乙二醇' },
  { code: 'ss', name: '不锈钢' },
  { code: 'ad', name: '铝合金' },
  { code: 'bu', name: '沥青' },
  { code: 'fu', name: '燃料油' },
  { code: 'ec', name: '集运欧线' },
  { code: 'ur', name: '尿素' },
  { code: 'sr', name: '白糖' },
  { code: 'cf', name: '棉花' },
  { code: 'ap', name: '苹果' },
]

const showHoldingPicker = ref(false)
const holdingSearch = ref('')

const filteredHoldingProducts = computed(() => {
  const q = holdingSearch.value.trim().toLowerCase()
  if (!q) return HOLDING_PRODUCTS
  return HOLDING_PRODUCTS.filter(p =>
    p.code.toLowerCase().includes(q) || p.name.includes(q)
  )
})

const fallbackTermProducts = computed<TermProductItem[]>(() =>
  HOLDING_PRODUCTS.map(p => ({
    code: p.code.toUpperCase(),
    name: p.name,
    is_index: ['ih', 'if', 'ic', 'im'].includes(p.code),
  }))
)

const termProductList = computed(() => termProducts.value.length ? termProducts.value : fallbackTermProducts.value)

const filteredTermProducts = computed(() => {
  const q = termSearch.value.trim().toLowerCase()
  const src = termProductList.value
  if (!q) return src
  return src.filter(p =>
    p.code.toLowerCase().includes(q) || p.name.includes(q)
  )
})

const holdingProductName = computed(() => {
  const p = HOLDING_PRODUCTS.find(p => p.code === holdingProduct.value)
  return p ? `${p.name} (${p.code.toUpperCase()})` : holdingProduct.value.toUpperCase()
})

const selectedTermProductInfo = computed(() => {
  return termProductList.value.find(p => p.code === termProduct.value)
    || { code: termProduct.value, name: termProduct.value, is_index: false }
})

const termProductName = computed(() => {
  const p = selectedTermProductInfo.value
  return `${p.name} (${p.code})`
})

const selectedTermWindowLabel = computed(() => {
  return termWindows.value.find(w => w.key === termWindow.value)?.label
    || termData.value?.window_label
    || '3交易日'
})

const termTableSeries = computed(() => {
  return (termData.value?.main?.series || []).map(s => ({
    key: s.label,
    label: compactDisplayDate(s.display_date || s.trade_date) || s.label,
  }))
})

function openHoldingPicker() {
  holdingSearch.value = ''
  showHoldingPicker.value = true
}

function closeHoldingPicker() {
  showHoldingPicker.value = false
}

function selectHoldingFromPicker(code: string) {
  closeHoldingPicker()
  if (holdingProduct.value !== code) {
    holdingProduct.value = code
    loadHolding(code)
  }
}

function openTermPicker() {
  termSearch.value = ''
  showTermPicker.value = true
}

function closeTermPicker() {
  showTermPicker.value = false
}

function selectTermFromPicker(code: string) {
  closeTermPicker()
  if (termProduct.value !== code) {
    termProduct.value = code
    loadTermStructure()
  }
}

function openTermWindowPicker() {
  showTermWindowPicker.value = true
}

function closeTermWindowPicker() {
  showTermWindowPicker.value = false
}

function selectTermWindowFromPicker(key: string) {
  closeTermWindowPicker()
  if (termWindow.value === key) return
  termWindow.value = key
  loadTermStructure()
}

// 持仓分析排序
type HoldingSortKey = 'score' | 'net_vol'
const holdingSortKey  = ref<HoldingSortKey>('score')
const holdingSortDesc = ref(true)

function toggleHoldingSort(key: HoldingSortKey) {
  if (holdingSortKey.value === key) {
    holdingSortDesc.value = !holdingSortDesc.value
  } else {
    holdingSortKey.value  = key
    holdingSortDesc.value = true
  }
}

function holdingSortIcon(key: HoldingSortKey): string {
  if (holdingSortKey.value !== key) return '⇅'
  return holdingSortDesc.value ? '↓' : '↑'
}

const sortedHoldingBrokers = computed(() => {
  const list = [...holdingBrokers.value]
  list.sort((a, b) => {
    const av = a[holdingSortKey.value] as number
    const bv = b[holdingSortKey.value] as number
    return holdingSortDesc.value ? bv - av : av - bv
  })
  return list
})

const chaosCategoryMax = computed(() => {
  const rows = chaosData.value?.category_breakdown ?? []
  const maxVal = rows.reduce((acc, item) => Math.max(acc, item.total || 0), 0)
  return maxVal > 0 ? maxVal : 1
})

function toHoldingBrokerDetail(b: any) {
  uni.navigateTo({
    url: `/pages/market/broker?product=${holdingProduct.value}&broker=${encodeURIComponent(b.broker)}&product_name=${encodeURIComponent(holdingProductName.value)}`,
  })
}

// ── 品种分类 ──────────────────────────────────────────────
const CATEGORIES = [
  { key: 'all',        label: '全部' },
  { key: 'stockidx',   label: '股指' },
  { key: 'petro',      label: '化工' },
  { key: 'agri',       label: '农产品' },
  { key: 'black',      label: '工业品' },
  { key: 'nonferrous', label: '有色' },
  { key: 'precious',   label: '贵金属' },
  { key: 'newenergy',  label: '新能源' },
]

const PRODUCT_CAT: Record<string, string> = {
  // 化工
  ta:'petro', ma:'petro', pp:'petro', l:'petro', v:'petro',
  eb:'petro', bu:'petro', ru:'petro', nr:'petro', sc:'petro', sh:'petro',
  lu:'petro', pg:'petro', eg:'petro', fu:'petro',
  // 农产品
  m:'agri', y:'agri', p:'agri', oi:'agri', rm:'agri',
  cf:'agri', sr:'agri', jd:'agri', ap:'agri', cj:'agri',
  lh:'agri', cs:'agri', c:'agri', a:'agri', b:'agri',
  // 工业品（黑色系）
  j:'black', jm:'black', i:'black', rb:'black', hc:'black',
  sf:'black', sm:'black', zc:'black', ss:'black', fg:'black',
  sa:'black', ur:'black',
  // 有色（BC=国际铜，属有色；SI已移至新能源）
  cu:'nonferrous', al:'nonferrous', zn:'nonferrous', pb:'nonferrous',
  ni:'nonferrous', sn:'nonferrous', bc:'nonferrous',
  // 贵金属（AU黄金、AG白银、PT铂金、PD钯金）
  au:'precious', ag:'precious', pt:'precious', pd:'precious',
  // 新能源（工业硅、多晶硅、碳酸锂）
  si:'newenergy', ps:'newenergy', lc:'newenergy',
  // 股指期货（IF沪深300、IC中证500、IH上证50、IM中证1000）
  'if':'stockidx', ic:'stockidx', ih:'stockidx', im:'stockidx',
}

const selectedCat = ref('all')    // 一级分类
const selectedProd = ref('')      // 二级：具体品种代码，'' = 未选

// 品种 chip 点击后加载的全部月份合约
const productContracts = ref<OptionItem[]>([])
const prodLoading = ref(false)

// 当前分类下的品种列表（用于二级 chips）
const catProducts = computed(() => {
  const src = selectedCat.value === 'all'
    ? options.value
    : options.value.filter(it => PRODUCT_CAT[it.product_code] === selectedCat.value)
  const seen = new Set<string>()
  const list: Array<{ code: string; name: string }> = []
  for (const it of src) {
    if (!seen.has(it.product_code)) {
      seen.add(it.product_code)
      const m = it.name.match(/\(([^)]+)\)/)
      list.push({ code: it.product_code, name: m ? m[1] : it.product_code.toUpperCase() })
    }
  }
  return list
})

// ── 排序（列头点击）──────────────────────────────────────
type SortKey = 'iv_rank' | 'iv' | 'pct_1d' | 'iv_chg_1d'
const sortKey  = ref<SortKey>('iv_rank')
const sortDesc = ref(true)

function toggleSort(key: SortKey) {
  if (sortKey.value === key) {
    sortDesc.value = !sortDesc.value
  } else {
    sortKey.value = key
    sortDesc.value = true
  }
}

function sortIcon(key: SortKey): string {
  if (sortKey.value !== key) return '⇅'
  return sortDesc.value ? '↓' : '↑'
}

// ── 筛选 + 排序后的列表 ───────────────────────────────────
const displayedOptions = computed(() => {
  // 选中具体品种时，直接用后端返回的全部月份合约
  if (selectedProd.value) {
    const list = [...productContracts.value]
    list.sort((a, b) => {
      const av = a[sortKey.value] as number
      const bv = b[sortKey.value] as number
      if (sortKey.value === 'iv_rank') {
        if (av < 0 && bv >= 0) return 1
        if (bv < 0 && av >= 0) return -1
      }
      return sortDesc.value ? bv - av : av - bv
    })
    return list
  }

  // 未选具体品种：从缓存的主力合约里按分类过滤，每品种保留主力一条
  let list = [...options.value]
  if (selectedCat.value !== 'all') {
    list = list.filter(it => PRODUCT_CAT[it.product_code] === selectedCat.value)
  }
  const best = new Map<string, OptionItem>()
  for (const it of list) {
    const cur = best.get(it.product_code)
    if (!cur) { best.set(it.product_code, it); continue }
    const curBetter = cur.iv_rank >= 0
      ? (it.iv_rank >= 0 && it.iv_rank > cur.iv_rank)
      : it.iv_rank >= 0
    if (curBetter) best.set(it.product_code, it)
  }
  list = Array.from(best.values())

  list.sort((a, b) => {
    const av = a[sortKey.value] as number
    const bv = b[sortKey.value] as number
    if (sortKey.value === 'iv_rank') {
      if (av < 0 && bv >= 0) return 1
      if (bv < 0 && av >= 0) return -1
    }
    return sortDesc.value ? bv - av : av - bv
  })
  return list
})

type TermChart = {
  width: number
  height: number
  xLabels: Array<{ x: number; label: string }>
  yLabels: Array<{ y: number; label: string }>
  lines: Array<{
    label: string
    color: string
    points: string
    samples: Array<{ index: number; x: number; y: number; display: string }>
  }>
  sampleXs: number[]
  tooltipLabels: string[]
  empty: boolean
}

type TermChartKey = 'main' | 'basis' | 'long'

type TermTooltipState = {
  key: TermChartKey
  title: string
  leftPct: number
  topPct: number
  guideLeftPct: number
  align: 'left' | 'right'
  vertical: 'above' | 'below'
  dots: Array<{ leftPct: number; topPct: number; color: string }>
  rows: Array<{ label: string; color: string; value: string }>
}

const TERM_W = 660
const TERM_H = 300
const TERM_PAD_L = 24
const TERM_PAD_R = 12
const TERM_PAD_T = 16
const TERM_PAD_B = 26
const TERM_COLORS: Record<string, string> = {
  '窗口起点': '#38bdf8',
  '窗口中点': '#f59e0b',
  '最新': '#fb7185',
}

function safeNum(v: any): number | null {
  const n = Number(v)
  return Number.isFinite(n) ? n : null
}

function fmtTermNum(v: any, digits = 2): string {
  const n = safeNum(v)
  if (n === null) return '--'
  if (Math.abs(n) >= 1000) return n.toLocaleString(undefined, { maximumFractionDigits: 0 })
  return n.toFixed(digits)
}

function fmtTermPct(v: any): string {
  const n = safeNum(v)
  if (n === null) return '--'
  return `${(n * 100).toFixed(2)}%`
}

function termStructureLabel(v?: string): string {
  if (v === 'Contango') return '升水'
  if (v === 'Backwardation') return '贴水'
  if (v === 'Flat') return '平水'
  if (v === 'InsufficientData') return '不足'
  return v || '--'
}

function termStructureClass(v?: string): string {
  if (v === 'Contango') return 'term-up'
  if (v === 'Backwardation') return 'term-down'
  return 'term-flat'
}

function compactDisplayDate(v?: string): string {
  const text = String(v || '').trim()
  if (!text) return ''
  if (/^\d{8}$/.test(text)) return `${text.slice(4, 6)}/${text.slice(6, 8)}`
  const m = text.match(/^(\d{4})-(\d{2})-(\d{2})/)
  if (m) return `${m[2]}/${m[3]}`
  return text
}

function buildBlockChart(block?: TermStructureBlock | null, valueKey: 'close_price' | 'basis' = 'close_price'): TermChart | null {
  if (!block?.contracts?.length || !block?.series?.length) return null
  const normalizedSeries = block.series.map((s, idx) => {
    const samples: Array<{ index: number; value: number }> = []
    for (let i = 0; i < block.contracts.length; i++) {
      const point = (s.points || [])[i]
      const val = safeNum((point as any)?.[valueKey])
      if (val !== null) samples.push({ index: i, value: val })
    }
    const isMissingBasisSeries = valueKey === 'basis'
      && samples.length > 0
      && samples.every(sample => Math.abs(sample.value) < 1e-9)
    return {
      idx,
      source: s,
      samples,
      isMissingBasisSeries,
    }
  }).filter(item => item.samples.length && !item.isMissingBasisSeries)
  const values = normalizedSeries.flatMap(item => item.samples.map(sample => sample.value))
  if (values.length < 2) return null
  const min = Math.min(...values)
  const max = Math.max(...values)
  const span = max - min || 1
  const innerW = TERM_W - TERM_PAD_L - TERM_PAD_R
  const innerH = TERM_H - TERM_PAD_T - TERM_PAD_B
  const toX = (i: number) => TERM_PAD_L + (block.contracts.length <= 1 ? 0 : (i / (block.contracts.length - 1)) * innerW)
  const toY = (v: number) => TERM_PAD_T + innerH - ((v - min) / span) * innerH
  const sampleXs = block.contracts.map((_, i) => toX(i))
  const lines = normalizedSeries.map(({ source: s, idx }) => {
    const pts: string[] = []
    const samples: Array<{ index: number; x: number; y: number; display: string }> = []
    for (let i = 0; i < block.contracts.length; i++) {
      const point = (s.points || [])[i]
      const val = safeNum((point as any)?.[valueKey])
      if (val !== null) {
        const x = toX(i)
        const y = toY(val)
        pts.push(`${x.toFixed(1)},${y.toFixed(1)}`)
        samples.push({ index: i, x, y, display: fmtTermNum(val) })
      }
    }
    return {
      label: compactDisplayDate(s.display_date || s.trade_date) || s.label,
      color: TERM_COLORS[s.label] || ['#38bdf8', '#f59e0b', '#fb7185'][idx % 3],
      points: pts.join(' '),
      samples,
    }
  }).filter(line => line.points)
  const tickIndexes = block.contracts.length <= 7
    ? block.contracts.map((_, i) => i)
    : Array.from(new Set([0, Math.floor((block.contracts.length - 1) / 2), block.contracts.length - 1]))
  return {
    width: TERM_W,
    height: TERM_H,
    xLabels: tickIndexes.map(i => ({ x: toX(i), label: block.contracts[i] })),
    yLabels: [
      { y: toY(max), label: fmtTermNum(max) },
      { y: toY((max + min) / 2), label: fmtTermNum((max + min) / 2) },
      { y: toY(min), label: fmtTermNum(min) },
    ],
    lines,
    sampleXs,
    tooltipLabels: block.contracts.slice(),
    empty: !lines.length,
  }
}

function buildLongChart(block?: TermLongBlock | null): TermChart | null {
  const raw = block?.points || []
  const points = raw.filter(p => safeNum(p.basis) !== null)
  if (points.length < 2) return null
  const values = points.map(p => Number(p.basis))
  const min = Math.min(...values)
  const max = Math.max(...values)
  const span = max - min || 1
  const innerW = TERM_W - TERM_PAD_L - TERM_PAD_R
  const innerH = TERM_H - TERM_PAD_T - TERM_PAD_B
  const toX = (i: number) => TERM_PAD_L + (i / (points.length - 1)) * innerW
  const toY = (v: number) => TERM_PAD_T + innerH - ((v - min) / span) * innerH
  const sampleXs = points.map((_, i) => toX(i))
  const samples = points.map((p, i) => ({
    index: i,
    x: toX(i),
    y: toY(Number(p.basis)),
    display: fmtTermNum(p.basis),
  }))
  const linePoints = samples.map(p => `${p.x.toFixed(1)},${p.y.toFixed(1)}`).join(' ')
  const tickIndexes = Array.from(new Set([0, Math.floor((points.length - 1) / 2), points.length - 1]))
  return {
    width: TERM_W,
    height: TERM_H,
    xLabels: tickIndexes.map(i => ({ x: toX(i), label: (points[i].display_date || points[i].trade_date || '').slice(5) })),
    yLabels: [
      { y: toY(max), label: fmtTermNum(max) },
      { y: toY((max + min) / 2), label: fmtTermNum((max + min) / 2) },
      { y: toY(min), label: fmtTermNum(min) },
    ],
    lines: [{ label: '近月升贴水', color: '#22d3ee', points: linePoints, samples }],
    sampleXs,
    tooltipLabels: points.map(p => p.display_date || p.trade_date || '--'),
    empty: false,
  }
}

const termMainChart = computed(() => buildBlockChart(termData.value?.main, 'close_price'))
const termBasisChart = computed(() => buildBlockChart(termData.value?.basis_anchor, 'basis'))
const termLongChart = computed(() => buildLongChart(termData.value?.basis_longterm))
const termTableRows = computed(() => {
  const block = termData.value?.main
  if (!block?.contracts?.length) return []
  return block.contracts.map((contract, idx) => {
    const row: Record<string, string> = { contract }
    for (const s of block.series || []) {
      row[s.label] = fmtTermNum((s.points || [])[idx]?.close_price)
    }
    return row
  })
})

function termAxisXStyle(x: number) {
  const pct = Math.max(5, Math.min(92, (Number(x) / TERM_W) * 100))
  return {
    left: `${pct}%`,
    bottom: '-2rpx',
    transform: 'translateX(-50%)',
  }
}

function termAxisYStyle(y: number) {
  const pct = Math.max(9, Math.min(82, (Number(y) / TERM_H) * 100))
  return {
    top: `${pct}%`,
    right: '4rpx',
    transform: 'translateY(-50%)',
  }
}

function termZoomValue(key: TermChartKey): number {
  if (key === 'basis') return termBasisZoom.value
  if (key === 'long') return termLongZoom.value
  return termMainZoom.value
}

function termChartBoxStyle(key: TermChartKey) {
  const height = Math.round(300 * termZoomValue(key))
  return { height: `${height}rpx` }
}

function setTermZoom(key: TermChartKey, value: number) {
  const next = Math.max(1, Math.min(1.7, Number(value) || 1))
  const rounded = Math.round(next * 100) / 100
  if (key === 'basis') termBasisZoom.value = rounded
  else if (key === 'long') termLongZoom.value = rounded
  else termMainZoom.value = rounded
  nextTick(() => refreshTermChartRects())
}

function refreshTermChartRects() {
  if (activeTab.value !== 'term') return
  const proxy = pageInstance?.proxy
  if (!proxy || typeof uni.createSelectorQuery !== 'function') return
  const query = uni.createSelectorQuery().in(proxy)
  query.select('#term-chart-main').boundingClientRect()
  query.select('#term-chart-basis').boundingClientRect()
  query.select('#term-chart-long').boundingClientRect()
  query.exec((res: any[]) => {
    const [main, basis, long] = res || []
    termChartRects.value = {
      main: main?.width ? main : null,
      basis: basis?.width ? basis : null,
      long: long?.width ? long : null,
    }
  })
}

function eventPagePoint(e: any): { x: number; y: number } | null {
  const touch = e?.touches?.[0] || e?.changedTouches?.[0]
  if (touch) {
    const x = Number(touch.clientX ?? touch.pageX ?? touch.x)
    const y = Number(touch.clientY ?? touch.pageY ?? touch.y)
    return Number.isFinite(x) && Number.isFinite(y) ? { x, y } : null
  }
  const detail = e?.detail
  if (detail && Number.isFinite(Number(detail.x)) && Number.isFinite(Number(detail.y))) {
    return { x: Number(detail.x), y: Number(detail.y) }
  }
  const mouseX = Number(e?.clientX)
  const mouseY = Number(e?.clientY)
  if (Number.isFinite(mouseX) && Number.isFinite(mouseY)) return { x: mouseX, y: mouseY }
  return null
}

function chartByKey(key: TermChartKey): TermChart | null {
  if (key === 'basis') return termBasisChart.value
  if (key === 'long') return termLongChart.value
  return termMainChart.value
}

function buildTermTooltip(key: TermChartKey, chart: TermChart, pageX: number): TermTooltipState | null {
  const rect = termChartRects.value[key]
  if (!rect?.width || !chart.sampleXs.length) return null
  const ratio = Math.max(0, Math.min(1, (pageX - rect.left) / rect.width))
  const chartX = TERM_PAD_L + ratio * (TERM_W - TERM_PAD_L - TERM_PAD_R)
  let targetIndex = 0
  let bestDist = Number.POSITIVE_INFINITY
  chart.sampleXs.forEach((x, idx) => {
    const dist = Math.abs(x - chartX)
    if (dist < bestDist) {
      bestDist = dist
      targetIndex = idx
    }
  })
  const rows = chart.lines.map(line => {
    const sample = line.samples.find(item => item.index === targetIndex)
    if (!sample) return null
    return {
      label: line.label,
      color: line.color,
      value: sample.display,
      x: sample.x,
      y: sample.y,
    }
  }).filter(Boolean) as Array<{ label: string; color: string; value: string; x: number; y: number }>
  if (!rows.length) return null
  const anchorX = rows[0].x
  const topY = Math.min(...rows.map(row => row.y))
  const align = anchorX > TERM_W * 0.7 ? 'right' : 'left'
  const vertical = topY < TERM_H * 0.24 ? 'below' : 'above'
  return {
    key,
    title: chart.tooltipLabels[targetIndex] || '--',
    leftPct: Math.max(8, Math.min(94, (anchorX / TERM_W) * 100)),
    topPct: vertical === 'below'
      ? Math.max(6, Math.min(76, ((topY + 8) / TERM_H) * 100))
      : Math.max(10, Math.min(58, ((topY - 18) / TERM_H) * 100)),
    guideLeftPct: Math.max(4, Math.min(96, (anchorX / TERM_W) * 100)),
    align,
    vertical,
    dots: rows.map(row => ({
      leftPct: Math.max(4, Math.min(96, (row.x / TERM_W) * 100)),
      topPct: Math.max(6, Math.min(92, (row.y / TERM_H) * 100)),
      color: row.color,
    })),
    rows: rows.map(row => ({ label: row.label, color: row.color, value: row.value })),
  }
}

function updateTermTooltip(key: TermChartKey, chart: TermChart | null, e: any) {
  const point = eventPagePoint(e)
  if (!chart || !point) return
  const next = buildTermTooltip(key, chart, point.x)
  if (next) termTooltip.value = next
}

function clearTermLongPressTimer() {
  if (termLongPressTimer) {
    clearTimeout(termLongPressTimer)
    termLongPressTimer = null
  }
}

function touchDistance(e: any): number | null {
  const touches = e?.touches || []
  if (touches.length < 2) return null
  const [a, b] = touches
  const ax = Number(a?.clientX ?? a?.pageX ?? a?.x)
  const ay = Number(a?.clientY ?? a?.pageY ?? a?.y)
  const bx = Number(b?.clientX ?? b?.pageX ?? b?.x)
  const by = Number(b?.clientY ?? b?.pageY ?? b?.y)
  if (![ax, ay, bx, by].every(Number.isFinite)) return null
  return Math.hypot(ax - bx, ay - by)
}

function startTermPress(key: TermChartKey, e: any) {
  const distance = touchDistance(e)
  if (distance && distance > 0) {
    clearTermLongPressTimer()
    termTooltip.value = null
    termPressKey = null
    termPressActive = false
    termPinchKey = key
    termPinchStartDistance = distance
    termPinchStartZoom = termZoomValue(key)
    return
  }
  const point = eventPagePoint(e)
  const chart = chartByKey(key)
  if (!point || !chart) return
  clearTermLongPressTimer()
  termPressKey = key
  termPressActive = false
  termPressStartX = point.x
  termPressStartY = point.y
  termLongPressTimer = setTimeout(() => {
    termPressActive = true
    updateTermTooltip(key, chart, e)
  }, 260)
}

function moveTermPress(key: TermChartKey, e: any) {
  const distance = touchDistance(e)
  if (distance && distance > 0 && termPinchKey === key && termPinchStartDistance > 0) {
    clearTermLongPressTimer()
    termTooltip.value = null
    termPressActive = false
    setTermZoom(key, termPinchStartZoom * (distance / termPinchStartDistance))
    return
  }
  const point = eventPagePoint(e)
  const chart = chartByKey(key)
  if (!point || !chart || termPressKey !== key) return
  if (!termPressActive) {
    if (Math.abs(point.x - termPressStartX) > 10 || Math.abs(point.y - termPressStartY) > 10) {
      clearTermLongPressTimer()
      termPressKey = null
    }
    return
  }
  updateTermTooltip(key, chart, e)
}

function endTermPress() {
  clearTermLongPressTimer()
  termPressKey = null
  if (termPressActive) termTooltip.value = null
  termPressActive = false
  termPinchKey = null
  termPinchStartDistance = 0
}

function parseChartPoints(points: string): Array<{ x: number; y: number }> {
  return String(points || '').split(/\s+/).map(item => {
    const [x, y] = item.split(',').map(Number)
    return { x, y }
  }).filter(p => Number.isFinite(p.x) && Number.isFinite(p.y))
}

function drawTermChart(canvasId: string, chart: TermChart | null) {
  if (typeof uni.createCanvasContext !== 'function') return
  const ctx = uni.createCanvasContext(canvasId)
  const w = TERM_W
  const h = TERM_H
  ctx.clearRect(0, 0, w, h)
  ctx.setFillStyle('#0b1528')
  ctx.fillRect(0, 0, w, h)
  if (!chart || chart.empty) {
    ctx.setFillStyle('#7f8ea8')
    ctx.setFontSize(22)
    ctx.fillText('暂无曲线数据', 250, 150)
    ctx.draw()
    return
  }
  ctx.setStrokeStyle('rgba(148,163,184,0.18)')
  ctx.setLineWidth(1)
  for (const y of chart.yLabels) {
    ctx.beginPath()
    ctx.moveTo(TERM_PAD_L, y.y)
    ctx.lineTo(w - TERM_PAD_R, y.y)
    ctx.stroke()
    ctx.setFillStyle('#dbeafe')
    ctx.setFontSize(20)
    ;(ctx as any).setTextAlign?.('right')
    ctx.fillText(y.label, w - TERM_PAD_R - 4, y.y - 5)
  }
  ctx.setStrokeStyle('rgba(203,213,225,0.46)')
  ctx.setLineWidth(1.5)
  ctx.beginPath()
  ctx.moveTo(TERM_PAD_L, TERM_PAD_T)
  ctx.lineTo(TERM_PAD_L, h - TERM_PAD_B)
  ctx.lineTo(w - TERM_PAD_R, h - TERM_PAD_B)
  ctx.stroke()
  for (const line of chart.lines) {
    const pts = parseChartPoints(line.points)
    if (pts.length < 2) continue
    ctx.beginPath()
    pts.forEach((p, idx) => {
      if (idx === 0) ctx.moveTo(p.x, p.y)
      else ctx.lineTo(p.x, p.y)
    })
    ctx.setStrokeStyle(line.color)
    ctx.setLineWidth(4)
    ctx.stroke()
    for (const p of pts) {
      ctx.beginPath()
      ctx.arc(p.x, p.y, 4, 0, Math.PI * 2)
      ctx.setFillStyle(line.color)
      ctx.fill()
    }
  }
  ctx.setFillStyle('#dbeafe')
  ctx.setFontSize(20)
  ;(ctx as any).setTextAlign?.('center')
  for (const x of chart.xLabels) {
    ctx.fillText(x.label, x.x, h - 7)
  }
  ;(ctx as any).setTextAlign?.('left')
  ctx.draw()
}

function drawTermCharts() {
  if (activeTab.value !== 'term') return
  nextTick(() => {
    drawTermChart('termMainCanvas', termMainChart.value)
    drawTermChart('termBasisCanvas', termBasisChart.value)
    drawTermChart('termLongCanvas', termLongChart.value)
    refreshTermChartRects()
  })
}

watch([termMainChart, termBasisChart, termLongChart, activeTab, termMainZoom, termBasisZoom, termLongZoom], drawTermCharts)

// ── 事件处理 ──────────────────────────────────────────────
onShow(() => {
  if (!auth.isLoggedIn) { uni.reLaunch({ url: '/pages/login/index' }); return }
  if (activeTab.value === 'options') {
    if (options.value.length === 0) loadOptions()
    startLivePolling()
  } else if (activeTab.value === 'holding') {
    if (!holdingProduct.value) {
      holdingProduct.value = HOLDING_PRODUCTS[0].code
      loadHolding(holdingProduct.value)
    }
    stopLivePolling()
  } else if (activeTab.value === 'chaos') {
    if (!chaosData.value) loadChaos()
    stopLivePolling()
  } else if (activeTab.value === 'term') {
    if (!termData.value) loadTermStructure()
    stopLivePolling()
  }
})

onHide(() => {
  stopLivePolling()
})

async function switchTab(t: Tab) {
  endTermPress()
  activeTab.value = t
  if (t === 'options') {
    if (options.value.length === 0) loadOptions()
    startLivePolling()
  } else {
    stopLivePolling()
  }
  if (t === 'holding' && !holdingProduct.value) {
    holdingProduct.value = HOLDING_PRODUCTS[0].code
    loadHolding(holdingProduct.value)
  }
  if (t === 'chaos' && !chaosData.value) loadChaos()
  if (t === 'term') {
    if (!termProducts.value.length) loadTermProducts()
    if (!termData.value) loadTermStructure()
    else drawTermCharts()
  }
}

async function loadOptions() {
  optLoading.value = true
  try {
    const res = await marketApi.options()
    options.value = res.items
    optUpdatedAt.value = res.updated_at
  } catch (e: any) {
    uni.showToast({ title: e.message || '加载失败', icon: 'none' })
  } finally {
    optLoading.value = false
  }
}

async function loadHolding(product: string) {
  holdingLoading.value = true
  holdingBrokers.value = []
  try {
    const res = await marketApi.holding(product)
    holdingBrokers.value = res.brokers
    holdingDate.value = res.trade_date
  } catch (e: any) {
    uni.showToast({ title: e.message || '加载失败', icon: 'none' })
  } finally {
    holdingLoading.value = false
  }
}

async function loadChaos() {
  chaosLoading.value = true
  chaosError.value = ''
  try {
    const res = await marketApi.chaos()
    chaosData.value = res
  } catch (e: any) {
    chaosError.value = e.message || '加载失败'
    uni.showToast({ title: chaosError.value, icon: 'none' })
  } finally {
    chaosLoading.value = false
  }
}

async function loadTermProducts() {
  if (termProductsLoading.value) return
  termProductsLoading.value = true
  try {
    const res = await marketApi.termProducts()
    termProducts.value = res.items || []
    if (res.windows?.length) termWindows.value = res.windows
    if (!termProduct.value) termProduct.value = res.default_product || 'IH'
    if (!termWindow.value) termWindow.value = res.default_window || '3d'
  } catch (_) {
    // 产品列表失败时使用本地 HOLDING_PRODUCTS 兜底，不打断页面。
  } finally {
    termProductsLoading.value = false
  }
}

async function loadTermStructure() {
  termLoading.value = true
  termError.value = ''
  termTooltip.value = null
  try {
    await loadTermProducts()
    const res = await marketApi.termStructure({
      product: termProduct.value,
      window: termWindow.value,
      slots: 7,
    })
    termData.value = res
    if (res.windows?.length) termWindows.value = res.windows
    termProduct.value = res.product || termProduct.value
    termWindow.value = res.window || termWindow.value
    drawTermCharts()
  } catch (e: any) {
    const msg = e.message || '加载失败'
    termError.value = msg === 'Not Found' ? '期限结构服务更新中，请稍后刷新' : msg
    uni.showToast({ title: termError.value, icon: 'none' })
  } finally {
    termLoading.value = false
  }
}

function selectTermWindow(key: string) {
  if (termWindow.value === key) return
  termWindow.value = key
  loadTermStructure()
}

async function selectHoldingProduct(code: string) {
  if (holdingProduct.value === code) return
  holdingProduct.value = code
  await loadHolding(code)
}

function selectCat(key: string) {
  selectedCat.value = key
  selectedProd.value = ''   // 切换一级时清空二级
}

async function selectProd(code: string) {
  if (selectedProd.value === code) {
    // 再次点击取消选中
    selectedProd.value = ''
    productContracts.value = []
    return
  }
  selectedProd.value = code
  productContracts.value = []
  prodLoading.value = true
  try {
    const res = await marketApi.contracts(code)
    productContracts.value = res.items
  } catch (e: any) {
    uni.showToast({ title: e.message || '加载失败', icon: 'none' })
  } finally {
    prodLoading.value = false
  }
}

function refresh() {
  if (activeTab.value === 'options') {
    options.value = []
    loadOptions()
    fetchLive()
  } else if (activeTab.value === 'holding') {
    if (holdingProduct.value) loadHolding(holdingProduct.value)
  } else if (activeTab.value === 'chaos') {
    loadChaos()
  } else if (activeTab.value === 'term') {
    loadTermStructure()
  }
}

function toDetail(item: OptionItem) {
  const contract = extractContractCode(item.name)
  const contractQuery = contract ? `&contract=${encodeURIComponent(contract)}` : ''
  uni.navigateTo({
    url: `/pages/market/detail?product=${item.product_code}${contractQuery}&name=${encodeURIComponent(item.name)}&iv_rank=${item.iv_rank}&iv=${item.iv}`,
  })
}

// ── 样式辅助 ──────────────────────────────────────────────
function ivRankColor(rank: number): string {
  if (rank === -2) return '#6b7280' // 无期权
  if (rank === -3) return '#76839a' // 有期权但缺IV数据
  if (rank < 0) return '#555555'
  if (rank >= 80) return '#e84040'
  if (rank >= 60) return '#f97316'
  if (rank >= 40) return '#f5c518'
  if (rank >= 20) return '#22c55e'
  return '#3b82f6'
}
function ivRankLabel(rank: number): string {
  if (rank === -2) return '无'
  if (rank === -3) return '缺'
  if (rank < 0) return '到期'
  return String(Math.round(rank))
}
function pctColor(v: number): string {
  if (v > 0) return '#e84040'
  if (v < 0) return '#22c55e'
  return '#888888'
}
function fmtPct(v: number): string {
  if (v === 0) return '0%'
  return (v > 0 ? '+' : '') + v.toFixed(2) + '%'
}
function fmtIvChg(v: number, rank?: number): string {
  if (rank === -2) return '无'
  if (rank === -3) return '-'
  if (v === 0) return '─'
  return (v > 0 ? '+' : '') + v.toFixed(1)
}
function posIcon(v: number): string { return v > 0 ? '▲' : v < 0 ? '▼' : '─' }
function posColor(v: number): string { return v > 0 ? '#e84040' : v < 0 ? '#22c55e' : '#555555' }
function scoreColor(v: number): string { return v > 0 ? '#e84040' : v < 0 ? '#22c55e' : '#888888' }
function dirColor(d: string): string { return d === '多' ? '#e84040' : d === '空' ? '#22c55e' : '#888888' }
function chaosBandColor(band: string): string {
  if (band === 'things_are_happening') return '#ef4444'
  if (band === 'something_is_brewing') return '#f97316'
  if (band === 'something_might_happen') return '#f5c518'
  return '#22c55e'
}
function chaosPct(v: number): string {
  return `${(v * 100).toFixed(1)}%`
}
function chaosDelta(v: number): string {
  if (!v) return '0.0%'
  return `${v > 0 ? '+' : ''}${(v * 100).toFixed(1)}%`
}
function chaosDeltaColor(v: number): string {
  if (v > 0) return '#f97316'
  if (v < 0) return '#22c55e'
  return '#94a3b8'
}
function chaosBarWidth(v: number): string {
  const pct = Math.max(0, Math.min(100, (v / chaosCategoryMax.value) * 100))
  return `${pct}%`
}
function chaosGaugeAngle(score: number): number {
  const clamped = Math.max(0, Math.min(100, Number(score) || 0))
  return -90 + clamped * 1.8
}

onShareAppMessage(() => ({
  title: SHARE_TITLE,
  path: SHARE_PATH,
}))

onShareTimeline(() => ({
  title: SHARE_TITLE,
  query: 'from=timeline&page=market',
}))
</script>

<template>
  <view class="page">
    <!-- Tab（含刷新）-->
    <view class="tab-bar">
      <scroll-view class="tab-scroll" scroll-x>
        <view class="tab-row">
          <view class="tab-item" :class="{ active: activeTab === 'options' }" @tap="switchTab('options')">
            <text>市场数据</text>
          </view>
          <view class="tab-item" :class="{ active: activeTab === 'holding' }" @tap="switchTab('holding')">
            <text>仓位变化</text>
          </view>
          <view class="tab-item" :class="{ active: activeTab === 'term' }" @tap="switchTab('term')">
            <text>期限结构</text>
          </view>
          <view class="tab-item" :class="{ active: activeTab === 'chaos' }" @tap="switchTab('chaos')">
            <text>混乱指数</text>
          </view>
        </view>
      </scroll-view>
      <!-- 实时状态指示点 -->
      <view v-if="activeTab === 'options' && liveTrading" class="live-dot-wrap">
        <view class="live-dot" /><text class="live-dot-text">{{ liveAt }}</text>
      </view>
      <view class="tab-refresh" @tap="refresh">
        <text class="refresh-icon" :class="{ spinning: optLoading || holdingLoading || chaosLoading || termLoading }">↻</text>
      </view>
    </view>

    <!-- ══ 商品期权 Tab ══ -->
    <view v-if="activeTab === 'options'">

      <!-- 一级分类 chips -->
      <scroll-view class="chips-bar" scroll-x>
        <view class="chips-row">
          <view
            v-for="cat in CATEGORIES" :key="cat.key"
            class="chip" :class="{ 'chip-active': selectedCat === cat.key }"
            @tap="selectCat(cat.key)"
          >{{ cat.label }}</view>
        </view>
      </scroll-view>

      <!-- 二级品种 chips（选中某分类后出现）-->
      <view v-if="selectedCat !== 'all' || selectedProd">
        <scroll-view class="chips-bar chips-bar-sub" scroll-x>
          <view class="chips-row">
            <view
              v-for="p in catProducts" :key="p.code"
              class="chip chip-sub" :class="{ 'chip-active': selectedProd === p.code }"
              @tap="selectProd(p.code)"
            >{{ p.name }}</view>
          </view>
        </scroll-view>
      </view>

      <!-- 列表头（可点击排序）-->
      <view class="list-header">
        <text class="th th-name">
          {{ selectedProd ? '合约' : '品种' }}
        </text>
        <view class="th th-chg sort-th" @tap="toggleSort('pct_1d')">
          <text>涨跌</text>
          <text class="sort-icon" :class="{ 'sort-active': sortKey === 'pct_1d' }">{{ sortIcon('pct_1d') }}</text>
        </view>
        <view class="th th-iv sort-th" @tap="toggleSort('iv')">
          <text>IV%</text>
          <text class="sort-icon" :class="{ 'sort-active': sortKey === 'iv' }">{{ sortIcon('iv') }}</text>
        </view>
        <view class="th th-ivchg sort-th" @tap="toggleSort('iv_chg_1d')">
          <text>IV变动</text>
          <text class="sort-icon" :class="{ 'sort-active': sortKey === 'iv_chg_1d' }">{{ sortIcon('iv_chg_1d') }}</text>
        </view>
        <view class="th th-rank sort-th" @tap="toggleSort('iv_rank')">
          <text>Rank</text>
          <text class="sort-icon" :class="{ 'sort-active': sortKey === 'iv_rank' }">{{ sortIcon('iv_rank') }}</text>
        </view>
      </view>

      <!-- 加载中 -->
      <view v-if="optLoading || prodLoading" class="center-tip">
        <text class="muted-text">加载中...</text>
      </view>

      <!-- 列表 -->
      <view v-else-if="!prodLoading && displayedOptions.length" class="opt-list">
        <view
          v-for="item in displayedOptions"
          :key="item.name"
          class="opt-row"
          @tap="toDetail(item)"
        >
          <!-- 合约名 + 实时价格 -->
          <view class="col-name">
            <text class="opt-name">{{ item.name.split('(')[0].trim() }}</text>
            <text class="opt-sub" v-if="item.name.includes('(')">{{ item.name.match(/\(([^)]+)\)/)?.[1] }}</text>
            <text v-if="displayPrice(item)" class="opt-price">{{ displayPrice(item) }}</text>
          </view>

          <!-- 涨跌（交易时段显示实时，否则显示昨日收盘）-->
          <view class="col-chg">
            <view class="pct-row">
              <text class="opt-pct" :style="{ color: pctColor(livePct(item) ?? item.pct_1d) }">
                {{ fmtPct(livePct(item) ?? item.pct_1d) }}
              </text>
              <view v-if="liveTrading && livePct(item) !== null" class="live-badge"><text class="live-badge-text">●</text></view>
            </view>
            <text class="opt-pct5" :style="{ color: pctColor(item.pct_5d) }">{{ fmtPct(item.pct_5d) }} 5日</text>
          </view>

          <!-- IV% -->
          <view class="col-iv">
            <text class="opt-iv">{{ item.iv > 0 ? item.iv.toFixed(1) : (item.iv_rank === -2 ? '无' : '-') }}</text>
          </view>

          <!-- IV变动 -->
          <view class="col-ivchg">
            <text class="ivchg-val" :style="{ color: pctColor(item.iv_chg_1d) }">{{ fmtIvChg(item.iv_chg_1d, item.iv_rank) }}</text>
          </view>

          <!-- Rank -->
          <view class="col-rank">
            <view class="rank-badge" :style="{ borderColor: ivRankColor(item.iv_rank), color: ivRankColor(item.iv_rank) }">
              <text class="rank-num">{{ ivRankLabel(item.iv_rank) }}</text>
            </view>
          </view>
        </view>
      </view>

      <view v-else-if="!optLoading" class="center-tip">
        <text class="muted-text">暂无数据，点刷新重试</text>
      </view>
    </view>

    <!-- ══ 期限结构 Tab ══ -->
    <view v-else-if="activeTab === 'term'" class="term-wrap">
      <view class="term-hero">
        <view class="term-filter-row">
          <view class="term-filter-trigger" @tap="openTermWindowPicker">
            <text>{{ selectedTermWindowLabel }}</text>
            <text class="picker-arrow">▾</text>
          </view>
          <view class="term-filter-trigger term-product-trigger" @tap="openTermPicker">
            <text>{{ termProductName }}</text>
            <text class="picker-arrow">▾</text>
          </view>
        </view>
        <view class="term-meta-line">
          <text>最新：{{ termData?.main?.meta?.latest_trade_date || '--' }}</text>
          <text>双指捏合缩放 · 长按看数值</text>
        </view>
      </view>

      <view v-if="termLoading" class="center-tip">
        <text class="muted-text">加载期限结构...</text>
      </view>

      <view v-else-if="termData?.has_data" class="term-content">
        <view class="term-metrics">
          <view class="term-metric">
            <text class="metric-k">结构</text>
            <text class="metric-v" :class="termStructureClass(termData.main?.summary?.structure_type)">
              {{ termStructureLabel(termData.main?.summary?.structure_type) }}
            </text>
          </view>
          <view class="term-metric">
            <text class="metric-k">近远价差</text>
            <text class="metric-v">{{ fmtTermNum(termData.main?.summary?.spread_abs) }}</text>
          </view>
          <view class="term-metric">
            <text class="metric-k">价差%</text>
            <text class="metric-v">{{ fmtTermPct(termData.main?.summary?.spread_pct) }}</text>
          </view>
          <view class="term-metric">
            <text class="metric-k">每档斜率</text>
            <text class="metric-v">{{ fmtTermNum(termData.main?.summary?.slope_per_step) }}</text>
          </view>
        </view>

        <view class="term-card">
          <view class="term-card-head">
            <text class="term-card-title">期限结构曲线</text>
            <view class="term-card-actions">
              <text class="term-card-sub">{{ termData.product }} · {{ termData.product_name }}</text>
            </view>
          </view>
          <view
            v-if="termMainChart"
            id="term-chart-main"
            class="term-chart-box"
            :style="termChartBoxStyle('main')"
            @touchstart="startTermPress('main', $event)"
            @touchmove="moveTermPress('main', $event)"
            @touchend="endTermPress"
            @touchcancel="endTermPress"
            @mousedown="startTermPress('main', $event)"
            @mousemove="moveTermPress('main', $event)"
            @mouseup="endTermPress"
            @mouseleave="endTermPress"
          >
            <!-- #ifdef MP-WEIXIN -->
            <canvas canvas-id="termMainCanvas" class="term-canvas" :style="termChartBoxStyle('main')"></canvas>
            <!-- #endif -->
            <!-- #ifndef MP-WEIXIN -->
            <svg class="term-svg" :viewBox="`0 0 ${termMainChart.width} ${termMainChart.height}`" preserveAspectRatio="none">
              <line v-for="y in termMainChart.yLabels" :key="`main-y-${y.label}`" :x1="TERM_PAD_L" :y1="y.y" :x2="TERM_W - TERM_PAD_R" :y2="y.y" stroke="rgba(148,163,184,0.18)" stroke-width="1" />
              <line :x1="TERM_PAD_L" :y1="TERM_PAD_T" :x2="TERM_PAD_L" :y2="TERM_H - TERM_PAD_B" stroke="rgba(203,213,225,0.46)" stroke-width="1.5" />
              <line :x1="TERM_PAD_L" :y1="TERM_H - TERM_PAD_B" :x2="TERM_W - TERM_PAD_R" :y2="TERM_H - TERM_PAD_B" stroke="rgba(203,213,225,0.46)" stroke-width="1.5" />
              <polyline v-for="line in termMainChart.lines" :key="line.label" :points="line.points" fill="none" :stroke="line.color" stroke-width="4" stroke-linejoin="round" stroke-linecap="round" />
            </svg>
            <view class="term-axis-overlay">
              <text v-for="y in termMainChart.yLabels" :key="`main-y-label-${y.label}`" class="term-axis-tag term-axis-tag-y" :style="termAxisYStyle(y.y)">{{ y.label }}</text>
              <text v-for="x in termMainChart.xLabels" :key="`main-x-${x.label}`" class="term-axis-tag term-axis-tag-x" :style="termAxisXStyle(x.x)">{{ x.label }}</text>
            </view>
            <!-- #endif -->
            <view v-if="termTooltip?.key === 'main'" class="term-tooltip-layer">
              <view class="term-tooltip-guide" :style="{ left: `${termTooltip.guideLeftPct}%` }"></view>
              <view v-for="dot in termTooltip.dots" :key="`${dot.leftPct}-${dot.topPct}-${dot.color}`" class="term-tooltip-dot" :style="{ left: `${dot.leftPct}%`, top: `${dot.topPct}%`, background: dot.color }"></view>
              <view class="term-tooltip-box" :class="{ 'term-tooltip-box-right': termTooltip.align === 'right', 'term-tooltip-box-below': termTooltip.vertical === 'below' }" :style="{ left: `${termTooltip.leftPct}%`, top: `${termTooltip.topPct}%` }">
                <text class="term-tooltip-title">{{ termTooltip.title }}</text>
                <view v-for="row in termTooltip.rows" :key="row.label" class="term-tooltip-row">
                  <view class="term-tooltip-row-left">
                    <view class="legend-dot" :style="{ background: row.color }"></view>
                    <text>{{ row.label }}</text>
                  </view>
                  <text class="term-tooltip-val">{{ row.value }}</text>
                </view>
              </view>
            </view>
          </view>
          <view v-else class="term-empty">暂无可绘制曲线</view>
          <view class="term-legend">
            <view v-for="line in termMainChart?.lines || []" :key="line.label" class="legend-item">
              <view class="legend-dot" :style="{ background: line.color }"></view>
              <text>{{ line.label }}</text>
            </view>
          </view>
        </view>

        <view v-if="termData.is_index" class="term-card">
          <view class="term-card-head">
            <text class="term-card-title">升贴水期限结构</text>
            <view class="term-card-actions">
              <text class="term-card-sub">指数参考差</text>
            </view>
          </view>
          <view
            v-if="termBasisChart"
            id="term-chart-basis"
            class="term-chart-box"
            :style="termChartBoxStyle('basis')"
            @touchstart="startTermPress('basis', $event)"
            @touchmove="moveTermPress('basis', $event)"
            @touchend="endTermPress"
            @touchcancel="endTermPress"
            @mousedown="startTermPress('basis', $event)"
            @mousemove="moveTermPress('basis', $event)"
            @mouseup="endTermPress"
            @mouseleave="endTermPress"
          >
            <!-- #ifdef MP-WEIXIN -->
            <canvas canvas-id="termBasisCanvas" class="term-canvas" :style="termChartBoxStyle('basis')"></canvas>
            <!-- #endif -->
            <!-- #ifndef MP-WEIXIN -->
            <svg class="term-svg" :viewBox="`0 0 ${termBasisChart.width} ${termBasisChart.height}`" preserveAspectRatio="none">
              <line v-for="y in termBasisChart.yLabels" :key="`basis-y-${y.label}`" :x1="TERM_PAD_L" :y1="y.y" :x2="TERM_W - TERM_PAD_R" :y2="y.y" stroke="rgba(148,163,184,0.18)" stroke-width="1" />
              <line :x1="TERM_PAD_L" :y1="TERM_PAD_T" :x2="TERM_PAD_L" :y2="TERM_H - TERM_PAD_B" stroke="rgba(203,213,225,0.46)" stroke-width="1.5" />
              <line :x1="TERM_PAD_L" :y1="TERM_H - TERM_PAD_B" :x2="TERM_W - TERM_PAD_R" :y2="TERM_H - TERM_PAD_B" stroke="rgba(203,213,225,0.46)" stroke-width="1.5" />
              <polyline v-for="line in termBasisChart.lines" :key="line.label" :points="line.points" fill="none" :stroke="line.color" stroke-width="4" stroke-linejoin="round" stroke-linecap="round" />
            </svg>
            <view class="term-axis-overlay">
              <text v-for="y in termBasisChart.yLabels" :key="`basis-y-label-${y.label}`" class="term-axis-tag term-axis-tag-y" :style="termAxisYStyle(y.y)">{{ y.label }}</text>
              <text v-for="x in termBasisChart.xLabels" :key="`basis-x-${x.label}`" class="term-axis-tag term-axis-tag-x" :style="termAxisXStyle(x.x)">{{ x.label }}</text>
            </view>
            <!-- #endif -->
            <view v-if="termTooltip?.key === 'basis'" class="term-tooltip-layer">
              <view class="term-tooltip-guide" :style="{ left: `${termTooltip.guideLeftPct}%` }"></view>
              <view v-for="dot in termTooltip.dots" :key="`${dot.leftPct}-${dot.topPct}-${dot.color}`" class="term-tooltip-dot" :style="{ left: `${dot.leftPct}%`, top: `${dot.topPct}%`, background: dot.color }"></view>
              <view class="term-tooltip-box" :class="{ 'term-tooltip-box-right': termTooltip.align === 'right', 'term-tooltip-box-below': termTooltip.vertical === 'below' }" :style="{ left: `${termTooltip.leftPct}%`, top: `${termTooltip.topPct}%` }">
                <text class="term-tooltip-title">{{ termTooltip.title }}</text>
                <view v-for="row in termTooltip.rows" :key="row.label" class="term-tooltip-row">
                  <view class="term-tooltip-row-left">
                    <view class="legend-dot" :style="{ background: row.color }"></view>
                    <text>{{ row.label }}</text>
                  </view>
                  <text class="term-tooltip-val">{{ row.value }}</text>
                </view>
              </view>
            </view>
          </view>
          <view v-else class="term-empty">暂无升贴水数据</view>
        </view>

        <view v-if="termData.is_index" class="term-card">
          <view class="term-card-head">
            <text class="term-card-title">近月升贴水</text>
            <view class="term-card-actions">
              <text class="term-card-sub">最近1年</text>
            </view>
          </view>
          <view
            v-if="termLongChart"
            id="term-chart-long"
            class="term-chart-box"
            :style="termChartBoxStyle('long')"
            @touchstart="startTermPress('long', $event)"
            @touchmove="moveTermPress('long', $event)"
            @touchend="endTermPress"
            @touchcancel="endTermPress"
            @mousedown="startTermPress('long', $event)"
            @mousemove="moveTermPress('long', $event)"
            @mouseup="endTermPress"
            @mouseleave="endTermPress"
          >
            <!-- #ifdef MP-WEIXIN -->
            <canvas canvas-id="termLongCanvas" class="term-canvas" :style="termChartBoxStyle('long')"></canvas>
            <!-- #endif -->
            <!-- #ifndef MP-WEIXIN -->
            <svg class="term-svg" :viewBox="`0 0 ${termLongChart.width} ${termLongChart.height}`" preserveAspectRatio="none">
              <line v-for="y in termLongChart.yLabels" :key="`long-y-${y.label}`" :x1="TERM_PAD_L" :y1="y.y" :x2="TERM_W - TERM_PAD_R" :y2="y.y" stroke="rgba(148,163,184,0.18)" stroke-width="1" />
              <line :x1="TERM_PAD_L" :y1="TERM_PAD_T" :x2="TERM_PAD_L" :y2="TERM_H - TERM_PAD_B" stroke="rgba(203,213,225,0.46)" stroke-width="1.5" />
              <line :x1="TERM_PAD_L" :y1="TERM_H - TERM_PAD_B" :x2="TERM_W - TERM_PAD_R" :y2="TERM_H - TERM_PAD_B" stroke="rgba(203,213,225,0.46)" stroke-width="1.5" />
              <polyline v-for="line in termLongChart.lines" :key="line.label" :points="line.points" fill="none" :stroke="line.color" stroke-width="4" stroke-linejoin="round" stroke-linecap="round" />
            </svg>
            <view class="term-axis-overlay">
              <text v-for="y in termLongChart.yLabels" :key="`long-y-label-${y.label}`" class="term-axis-tag term-axis-tag-y" :style="termAxisYStyle(y.y)">{{ y.label }}</text>
              <text v-for="x in termLongChart.xLabels" :key="`long-x-${x.label}`" class="term-axis-tag term-axis-tag-x" :style="termAxisXStyle(x.x)">{{ x.label }}</text>
            </view>
            <!-- #endif -->
            <view v-if="termTooltip?.key === 'long'" class="term-tooltip-layer">
              <view class="term-tooltip-guide" :style="{ left: `${termTooltip.guideLeftPct}%` }"></view>
              <view v-for="dot in termTooltip.dots" :key="`${dot.leftPct}-${dot.topPct}-${dot.color}`" class="term-tooltip-dot" :style="{ left: `${dot.leftPct}%`, top: `${dot.topPct}%`, background: dot.color }"></view>
              <view class="term-tooltip-box" :class="{ 'term-tooltip-box-right': termTooltip.align === 'right', 'term-tooltip-box-below': termTooltip.vertical === 'below' }" :style="{ left: `${termTooltip.leftPct}%`, top: `${termTooltip.topPct}%` }">
                <text class="term-tooltip-title">{{ termTooltip.title }}</text>
                <view v-for="row in termTooltip.rows" :key="row.label" class="term-tooltip-row">
                  <view class="term-tooltip-row-left">
                    <view class="legend-dot" :style="{ background: row.color }"></view>
                    <text>{{ row.label }}</text>
                  </view>
                  <text class="term-tooltip-val">{{ row.value }}</text>
                </view>
              </view>
            </view>
          </view>
          <view v-else class="term-empty">暂无一年趋势数据</view>
        </view>

        <view class="term-table-card">
          <view class="term-card-head">
            <text class="term-card-title">月份明细</text>
            <text class="term-card-sub">收盘价</text>
          </view>
          <scroll-view scroll-x>
            <view class="term-table">
              <view class="term-tr term-th">
                <text class="term-td term-contract">月份</text>
                <text v-for="col in termTableSeries" :key="`term-th-${col.key}`" class="term-td">{{ col.label }}</text>
              </view>
              <view v-for="row in termTableRows" :key="row.contract" class="term-tr">
                <text class="term-td term-contract">{{ row.contract }}</text>
                <text v-for="col in termTableSeries" :key="`term-row-${row.contract}-${col.key}`" class="term-td">{{ row[col.key] || '--' }}</text>
              </view>
            </view>
          </scroll-view>
        </view>
      </view>

      <view v-else class="center-tip">
        <text class="muted-text">{{ termData?.main?.error ? '该品种暂无期限结构数据，换个品种或窗口试试' : (termError || '暂无期限结构数据，点刷新重试') }}</text>
      </view>
    </view>

    <!-- ══ 持仓分析 Tab ══ -->
    <view v-else-if="activeTab === 'holding'" class="holding-wrap">
      <!-- 品种选择器触发按钮 -->
      <view class="picker-trigger" @tap="openHoldingPicker">
        <text class="picker-label">{{ holdingProductName || '选择品种' }}</text>
        <text class="picker-arrow">▾</text>
      </view>

      <view v-if="!holdingProduct" class="center-tip">
        <text class="muted-text">请选择品种</text>
      </view>

      <view v-else-if="holdingLoading" class="center-tip">
        <text class="muted-text">加载中...</text>
      </view>

      <view v-else-if="holdingBrokers.length">
        <view class="holding-date-bar">
          <text class="muted-text">{{ holdingDate ? '数据日期：' + holdingDate : '' }}</text>
        </view>

        <view class="holding-header">
          <text class="hth hth-broker">机构</text>
          <text class="hth hth-dir">方向</text>
          <view class="hth hth-score sort-th" @tap="toggleHoldingSort('score')">
            <text>累计得分</text>
            <text class="sort-icon" :class="{ 'sort-active': holdingSortKey === 'score' }">{{ holdingSortIcon('score') }}</text>
          </view>
          <view class="hth hth-vol sort-th" @tap="toggleHoldingSort('net_vol')">
            <text>净持仓</text>
            <text class="sort-icon" :class="{ 'sort-active': holdingSortKey === 'net_vol' }">{{ holdingSortIcon('net_vol') }}</text>
          </view>
          <text class="hth hth-go"></text>
        </view>

        <view class="holding-list">
          <view
            v-for="(b, idx) in sortedHoldingBrokers"
            :key="b.broker"
            class="holding-row"
            @tap="toHoldingBrokerDetail(b)"
          >
            <text class="h-rank" :class="{ 'gold': idx < 3 }">{{ idx + 1 }}</text>
            <text class="h-broker">{{ b.broker }}</text>
            <view class="h-dir-badge" :style="{ borderColor: dirColor(b.direction), color: dirColor(b.direction) }">
              <text>{{ b.direction }}</text>
            </view>
            <text class="h-score" :style="{ color: scoreColor(b.score) }">
              {{ b.score > 0 ? '+' : '' }}{{ Math.round(b.score) }}
            </text>
            <text class="h-vol" :style="{ color: posColor(b.net_vol) }">
              {{ b.net_vol > 0 ? '+' : '' }}{{ b.net_vol }}
            </text>
            <text class="h-go">›</text>
          </view>
        </view>

        <view class="holding-note">
          <text class="muted-text">得分 = 净仓位方向与市场变化相关性（近150天），正分=方向一致。点击机构查看明细。</text>
        </view>
      </view>

      <view v-else-if="!holdingLoading" class="center-tip">
        <text class="muted-text">该品种暂无持仓数据</text>
      </view>
    </view>

    <!-- ══ 混乱指数 Tab ══ -->
    <view v-else-if="activeTab === 'chaos'" class="chaos-wrap">
      <view v-if="chaosLoading" class="center-tip">
        <text class="muted-text">加载中...</text>
      </view>

      <view v-else-if="chaosData?.has_data" class="chaos-content">
        <view class="chaos-card chaos-core">
          <view class="chaos-core-head">
            <text class="chaos-core-title">混乱指数</text>
            <text class="chaos-core-time">{{ chaosData?.updated_time_text || '--:--:--' }}</text>
          </view>
          <view class="chaos-gauge-wrap">
            <view class="chaos-gauge-track">
              <view class="chaos-gauge-seg chaos-gauge-seg-1"></view>
              <view class="chaos-gauge-seg chaos-gauge-seg-2"></view>
              <view class="chaos-gauge-seg chaos-gauge-seg-3"></view>
              <view class="chaos-gauge-seg chaos-gauge-seg-4"></view>
            </view>
            <view class="chaos-gauge-inner"></view>
            <view class="chaos-gauge-needle-wrap" :style="{ transform: `translateX(-50%) rotate(${chaosGaugeAngle(chaosData?.score_raw || 0)}deg)` }">
              <view class="chaos-gauge-needle"></view>
            </view>
            <view class="chaos-gauge-cap"></view>
            <text class="chaos-gauge-tick chaos-gauge-tick-left">0</text>
            <text class="chaos-gauge-tick chaos-gauge-tick-mid">50</text>
            <text class="chaos-gauge-tick chaos-gauge-tick-right">100</text>
          </view>
          <view class="chaos-core-main">
            <text class="chaos-score">{{ chaosData?.score_raw.toFixed(1) }}</text>
            <text class="chaos-band" :style="{ color: chaosBandColor(chaosData?.band || '') }">{{ chaosData?.band_label }}</text>
          </view>
          <view class="chaos-meta-row">
            <view class="chaos-meta-item">
              <text class="chaos-meta-label">持续基础分</text>
              <text class="chaos-meta-val">{{ chaosData?.components.ongoing_baseline.toFixed(1) }}</text>
            </view>
            <view class="chaos-meta-item">
              <text class="chaos-meta-label">升级风险分</text>
              <text class="chaos-meta-val">{{ chaosData?.components.escalation_pressure.toFixed(1) }}</text>
            </view>
            <view class="chaos-meta-item">
              <text class="chaos-meta-label">联动加成</text>
              <text class="chaos-meta-val">{{ chaosData?.components.contagion_bonus.toFixed(1) }}</text>
            </view>
          </view>
        </view>

        <view class="chaos-card">
          <view class="chaos-card-head">
            <text class="chaos-card-title">监控市场</text>
            <text class="chaos-card-sub">Top {{ chaosData?.monitored_markets.length || 0 }}</text>
          </view>
          <view class="chaos-market-list">
            <view v-for="m in chaosData?.monitored_markets || []" :key="`${m.rank}-${m.display_title}`" class="chaos-market-row">
              <text class="chaos-rank">{{ m.rank }}</text>
              <view class="chaos-market-main">
                <view class="chaos-market-title-line">
                  <text class="chaos-market-title">{{ m.display_title }}</text>
                  <text v-if="m.trend_arrows" class="chaos-trend" :class="m.trend_direction === 'up' ? 'chaos-trend-up' : 'chaos-trend-down'">{{ m.trend_arrows }}</text>
                  <text v-if="m.trend_flames" class="chaos-trend-heat">{{ m.trend_flames }}</text>
                </view>
                <text class="chaos-market-meta">{{ m.region_label }} · {{ m.pair_tag }}</text>
              </view>
              <view class="chaos-market-right">
                <text class="chaos-market-prob">{{ chaosPct(m.probability) }}</text>
                <text class="chaos-market-delta" :style="{ color: chaosDeltaColor(m.delta_24h) }">{{ chaosDelta(m.delta_24h) }}</text>
              </view>
            </view>
          </view>
        </view>

        <view class="chaos-card">
          <view class="chaos-card-head">
            <text class="chaos-card-title">主要推升项</text>
            <text class="chaos-card-sub">Top {{ chaosData?.top_drivers.length || 0 }}</text>
          </view>
          <view class="chaos-driver-head">
            <text class="chaos-driver-col chaos-driver-title-col">事件</text>
            <text class="chaos-driver-col">概率</text>
            <text class="chaos-driver-col">24h</text>
          </view>
          <view v-for="item in chaosData?.top_drivers || []" :key="item.display_title" class="chaos-driver-row">
            <view class="chaos-driver-title-col">
              <text class="chaos-driver-title">{{ item.display_title }}</text>
              <text class="chaos-driver-meta">{{ item.region_label }}</text>
            </view>
            <text class="chaos-driver-col">{{ chaosPct(item.probability) }}</text>
            <text class="chaos-driver-col" :style="{ color: chaosDeltaColor(item.delta_24h) }">{{ chaosDelta(item.delta_24h) }}</text>
          </view>
        </view>

        <view class="chaos-card">
          <view class="chaos-card-head">
            <text class="chaos-card-title">风险来源分布</text>
            <text class="chaos-card-sub">类别构成</text>
          </view>
          <view v-for="item in chaosData?.category_breakdown || []" :key="item.key" class="chaos-cat-row">
            <view class="chaos-cat-top">
              <text class="chaos-cat-label">{{ item.label }}</text>
              <text class="chaos-cat-total">{{ item.total.toFixed(1) }}</text>
            </view>
            <view class="chaos-bar-wrap">
              <view class="chaos-bar-base" :style="{ width: chaosBarWidth(item.baseline) }"></view>
              <view class="chaos-bar-esca" :style="{ width: chaosBarWidth(item.escalation) }"></view>
            </view>
          </view>
        </view>
      </view>

      <view v-else class="center-tip">
        <text class="muted-text">{{ chaosError || '暂无数据，点刷新重试' }}</text>
      </view>
    </view>

    <!-- 品种选择弹窗（覆盖整个页面）-->
    <view v-if="showHoldingPicker" class="picker-overlay" @tap="closeHoldingPicker">
      <view class="picker-sheet" @tap.stop>
        <view class="picker-sheet-header">
          <text class="picker-sheet-title">选择品种</text>
          <text class="picker-sheet-close" @tap.stop="closeHoldingPicker">✕</text>
        </view>
        <view class="picker-search-bar" @tap.stop>
          <input
            class="picker-search-input"
            v-model="holdingSearch"
            type="text"
            confirm-type="search"
            :adjust-position="false"
            :cursor-spacing="120"
            @tap.stop
            placeholder="搜索品种名称或代码..."
            placeholder-style="color:#556070"
          />
        </view>
        <scroll-view class="picker-list" scroll-y>
          <view
            v-for="p in filteredHoldingProducts"
            :key="p.code"
            class="picker-item"
            :class="{ 'picker-item-active': holdingProduct === p.code }"
            @tap="selectHoldingFromPicker(p.code)"
          >
            <text class="picker-item-code">{{ p.code.toUpperCase() }}</text>
            <text class="picker-item-name">{{ p.name }}</text>
            <text v-if="holdingProduct === p.code" class="picker-item-check">✓</text>
          </view>
        </scroll-view>
      </view>
    </view>

    <view v-if="showTermPicker" class="picker-overlay" @tap="closeTermPicker">
      <view class="picker-sheet" @tap.stop>
        <view class="picker-sheet-header">
          <text class="picker-sheet-title">选择品种</text>
          <text class="picker-sheet-close" @tap.stop="closeTermPicker">✕</text>
        </view>
        <view class="picker-search-bar" @tap.stop>
          <input
            class="picker-search-input"
            v-model="termSearch"
            type="text"
            confirm-type="search"
            :adjust-position="false"
            :cursor-spacing="120"
            @tap.stop
            placeholder="搜索品种名称或代码..."
            placeholder-style="color:#556070"
          />
        </view>
        <scroll-view class="picker-list" scroll-y>
          <view
            v-for="p in filteredTermProducts"
            :key="p.code"
            class="picker-item"
            :class="{ 'picker-item-active': termProduct === p.code }"
            @tap="selectTermFromPicker(p.code)"
          >
            <text class="picker-item-code">{{ p.code }}</text>
            <text class="picker-item-name">{{ p.name }}</text>
            <text v-if="p.is_index" class="picker-item-tag">指数</text>
            <text v-if="termProduct === p.code" class="picker-item-check">✓</text>
          </view>
        </scroll-view>
      </view>
    </view>

    <view v-if="showTermWindowPicker" class="picker-overlay" @tap="closeTermWindowPicker">
      <view class="picker-sheet picker-sheet-compact" @tap.stop>
        <view class="picker-sheet-header">
          <text class="picker-sheet-title">选择周期</text>
          <text class="picker-sheet-close" @tap.stop="closeTermWindowPicker">✕</text>
        </view>
        <scroll-view class="picker-list" scroll-y>
          <view
            v-for="w in termWindows"
            :key="w.key"
            class="picker-item"
            :class="{ 'picker-item-active': termWindow === w.key }"
            @tap="selectTermWindowFromPicker(w.key)"
          >
            <text class="picker-item-name">{{ w.label }}</text>
            <text v-if="termWindow === w.key" class="picker-item-check">✓</text>
          </view>
        </scroll-view>
      </view>
    </view>

    <view style="height: 120rpx;" />
    <BottomNav active="market" />
  </view>
</template>

<style scoped>
.page { background: #0b1121; min-height: 100vh; }

/* Tab */
.tab-bar { display: flex; align-items: center; border-bottom: 1px solid #162035; }
.tab-scroll { flex: 1; min-width: 0; white-space: nowrap; }
.tab-row { display: flex; min-width: 672rpx; }
.tab-item { width: 168rpx; flex-shrink: 0; text-align: center; padding: 22rpx 0; font-size: 28rpx; color: #666666; }
.tab-item.active { color: #f5c518; border-bottom: 3rpx solid #f5c518; font-weight: 700; }
.tab-refresh { width: 72rpx; display: flex; align-items: center; justify-content: center; padding: 22rpx 0; flex-shrink: 0; }
.refresh-icon { font-size: 36rpx; color: #aaaaaa; }
.spinning { animation: spin 0.8s linear infinite; }
@keyframes spin { to { transform: rotate(360deg); } }

/* Chips */
.chips-bar { white-space: nowrap; border-bottom: 1px solid #131c2e; }
.chips-bar-sub { background: #080f1c; }
.chips-row { display: flex; padding: 12rpx 20rpx; gap: 12rpx; }

.chip {
  flex-shrink: 0; padding: 8rpx 22rpx; border-radius: 28rpx;
  font-size: 24rpx; color: #888888; background: #131c2e; border: 1px solid #1e2d45;
}
.chip.chip-active {
  color: #f5c518; background: rgba(245,197,24,0.1);
  border-color: rgba(245,197,24,0.4); font-weight: 600;
}
.chip-sub { font-size: 22rpx; padding: 6rpx 18rpx; }

/* 列表头 */
.list-header {
  display: flex; align-items: center; padding: 10rpx 24rpx;
  background: #0d1829; border-bottom: 1px solid #131c2e;
}
.th { font-size: 22rpx; color: #666666; display: flex; align-items: center; gap: 4rpx; }
.th-name  { flex: 2.2; }
.th-iv    { flex: 1.1; justify-content: flex-end; }
.th-rank  { flex: 1;   justify-content: center; }
.th-chg   { flex: 1.4; justify-content: flex-end; }
.th-ivchg { flex: 1.1; justify-content: flex-end; }

.sort-th  { cursor: pointer; }
.sort-icon { font-size: 18rpx; color: #444444; }
.sort-icon.sort-active { color: #f5c518; }

/* 期权行 */
.opt-list { padding: 0 16rpx; }
.opt-row {
  display: flex; align-items: center; padding: 18rpx 8rpx;
  border-bottom: 1px solid #131c2e;
}
.col-name { flex: 2.2; }
.opt-name  { display: block; font-size: 26rpx; color: #f0f0f0; font-weight: 600; }
.opt-sub   { display: block; font-size: 20rpx; color: #666666; margin-top: 3rpx; }
.opt-price { display: block; font-size: 22rpx; color: #94a3b8; margin-top: 4rpx; font-variant-numeric: tabular-nums; }

.col-iv { flex: 1.1; text-align: right; }
.opt-iv { font-size: 26rpx; color: #f0f0f0; font-weight: 600; }

.col-rank { flex: 1; display: flex; justify-content: center; align-items: center; }
.rank-badge {
  width: 58rpx; height: 58rpx; border-radius: 50%; border: 2px solid;
  display: flex; align-items: center; justify-content: center;
}
.rank-num { font-size: 20rpx; font-weight: 700; }

.col-chg { flex: 1.4; text-align: right; }
.opt-pct  { display: block; font-size: 24rpx; font-weight: 600; }
.opt-pct5 { display: block; font-size: 18rpx; color: #555555; margin-top: 3rpx; }

.col-ivchg { flex: 1.1; text-align: right; }
.ivchg-val { font-size: 24rpx; font-weight: 600; }

/* 持仓分析 */
.holding-wrap { padding-bottom: 20rpx; }
.holding-date-bar { padding: 10rpx 24rpx; background: #0d1829; }

.holding-header {
  display: flex; align-items: center;
  padding: 12rpx 24rpx; background: #0d1829; border-bottom: 1px solid #131c2e;
}
.hth { font-size: 22rpx; color: #555555; }
.hth-broker { flex: 2; }
.hth-dir    { flex: 0.7; text-align: center; }
.hth-score  { flex: 1.2; display: flex; align-items: center; justify-content: flex-end; gap: 4rpx; }
.hth-vol    { flex: 1.2; display: flex; align-items: center; justify-content: flex-end; gap: 4rpx; }
.hth-go     { width: 28rpx; flex-shrink: 0; }

.holding-list { padding: 0 16rpx; }
.holding-row {
  display: flex; align-items: center; padding: 18rpx 8rpx;
  border-bottom: 1px solid #131c2e;
}
.h-rank { width: 44rpx; font-size: 22rpx; color: #666; font-weight: 700; flex-shrink: 0; }
.h-rank.gold { color: #f5c518; }
.h-broker { flex: 2; font-size: 26rpx; color: #f0f0f0; }
.h-dir-badge {
  flex: 0.7; height: 40rpx; border-radius: 8rpx; border: 1px solid;
  display: flex; align-items: center; justify-content: center;
  font-size: 22rpx; font-weight: 700;
}
.h-score { flex: 1.2; font-size: 26rpx; font-weight: 700; text-align: right; }
.h-vol   { flex: 1.2; font-size: 22rpx; color: #aaaaaa; text-align: right; }
.h-go    { width: 28rpx; font-size: 30rpx; color: #445566; flex-shrink: 0; text-align: right; }
.holding-note { padding: 20rpx 24rpx; }

/* 品种下拉选择器 */
.picker-trigger {
  display: flex; align-items: center; justify-content: space-between;
  padding: 20rpx 24rpx; background: #0d1829; border-bottom: 1px solid #131c2e;
}
.picker-label { font-size: 28rpx; color: #f0f0f0; font-weight: 600; }
.picker-arrow { font-size: 28rpx; color: #888888; }

.picker-overlay {
  position: fixed; top: 0; left: 0; right: 0; bottom: 0;
  background: rgba(0, 0, 0, 0.65); z-index: 1000;
  display: flex; align-items: flex-end;
}
.picker-sheet {
  width: 100%; background: #0e1828;
  border-radius: 28rpx 28rpx 0 0;
  max-height: 72vh; display: flex; flex-direction: column;
}
.picker-sheet-header {
  display: flex; align-items: center; justify-content: space-between;
  padding: 24rpx 32rpx 8rpx; flex-shrink: 0;
}
.picker-sheet-title { font-size: 30rpx; color: #f0f0f0; font-weight: 700; }
.picker-sheet-close { font-size: 30rpx; color: #666666; padding: 8rpx; }

.picker-search-bar { padding: 12rpx 24rpx 20rpx; flex-shrink: 0; }
.picker-search-input {
  width: 100%;
  display: block;
  height: 76rpx;
  line-height: 76rpx;
  background: #131c2e;
  border: 1px solid #1e2d45;
  border-radius: 14rpx;
  padding: 0 22rpx;
  font-size: 30rpx;
  color: #f0f0f0;
  box-sizing: border-box;
}

.picker-list { flex: 1; overflow-y: auto; }
.picker-item {
  display: flex; align-items: center; padding: 20rpx 32rpx;
  border-bottom: 1px solid #131c2e; gap: 16rpx;
}
.picker-item-active { background: rgba(245, 197, 24, 0.06); }
.picker-item-code  { width: 72rpx; font-size: 22rpx; color: #888888; font-weight: 600; flex-shrink: 0; }
.picker-item-name  { flex: 1; font-size: 28rpx; color: #f0f0f0; }
.picker-item-tag {
  flex-shrink: 0;
  padding: 3rpx 10rpx;
  border-radius: 999rpx;
  border: 1px solid rgba(56, 189, 248, 0.35);
  background: rgba(56, 189, 248, 0.10);
  color: #7dd3fc;
  font-size: 18rpx;
}
.picker-item-check { font-size: 26rpx; color: #f5c518; flex-shrink: 0; }

/* 期限结构 */
.term-wrap {
  padding: 18rpx 18rpx 28rpx;
}
.term-hero {
  position: relative;
  overflow: hidden;
  border-radius: 26rpx;
  border: 1px solid #243652;
  background:
    radial-gradient(circle at 12% 0%, rgba(56, 189, 248, 0.18), transparent 36%),
    radial-gradient(circle at 100% 28%, rgba(245, 197, 24, 0.16), transparent 34%),
    linear-gradient(145deg, #101d31, #0d1729 58%, #0a1324);
  padding: 24rpx;
  box-shadow: 0 14rpx 40rpx rgba(0, 0, 0, 0.22);
}
.term-filter-row {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 14rpx;
}
.term-filter-trigger {
  min-height: 58rpx;
  padding: 0 18rpx;
  border-radius: 999rpx;
  border: 1px solid rgba(125, 211, 252, 0.36);
  background: rgba(9, 18, 33, 0.72);
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 10rpx;
  color: #dbeafe;
  font-size: 23rpx;
  font-weight: 700;
}
.term-filter-trigger text:first-child {
  overflow: hidden;
  white-space: nowrap;
  text-overflow: ellipsis;
}
.term-product-trigger {
  flex: 1;
  max-width: 330rpx;
}
.term-filter-row .term-filter-trigger:first-child {
  min-width: 196rpx;
  max-width: 220rpx;
}
.term-product-trigger {
  min-height: 58rpx;
  justify-content: space-between;
}
.term-meta-line {
  margin-top: 14rpx;
  display: flex;
  align-items: center;
  justify-content: space-between;
  color: #7d8ba4;
  font-size: 21rpx;
  font-variant-numeric: tabular-nums;
}
.term-content {
  margin-top: 18rpx;
  display: flex;
  flex-direction: column;
  gap: 18rpx;
}
.term-metrics {
  display: flex;
  flex-wrap: wrap;
  gap: 12rpx;
}
.term-metric {
  width: calc(50% - 6rpx);
  min-height: 104rpx;
  border-radius: 18rpx;
  border: 1px solid #223452;
  background: linear-gradient(180deg, #101d31, #0d1729);
  padding: 16rpx;
  box-sizing: border-box;
}
.metric-k {
  display: block;
  color: #71829b;
  font-size: 21rpx;
}
.metric-v {
  display: block;
  margin-top: 8rpx;
  color: #f8fafc;
  font-size: 31rpx;
  font-weight: 800;
  font-variant-numeric: tabular-nums;
}
.term-up { color: #f87171; }
.term-down { color: #34d399; }
.term-flat { color: #f5c518; }
.term-card,
.term-table-card {
  border-radius: 22rpx;
  border: 1px solid #233656;
  background: #101b2f;
  padding: 18rpx;
}
.term-card-head {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 12rpx;
  margin-bottom: 14rpx;
}
.term-card-actions {
  display: flex;
  align-items: center;
  justify-content: flex-end;
  gap: 12rpx;
  min-width: 0;
}
.term-card-title {
  color: #f8fafc;
  font-size: 28rpx;
  font-weight: 800;
}
.term-card-sub {
  max-width: 360rpx;
  color: #71829b;
  font-size: 20rpx;
  overflow: hidden;
  white-space: nowrap;
  text-overflow: ellipsis;
}
.term-chart-box {
  position: relative;
  border-radius: 18rpx;
  border: 1px solid #1e3150;
  background:
    linear-gradient(rgba(148, 163, 184, 0.045) 1px, transparent 1px),
    linear-gradient(90deg, rgba(148, 163, 184, 0.045) 1px, transparent 1px),
    #0b1528;
  background-size: 44rpx 44rpx;
  overflow: hidden;
  transition: height 0.16s ease;
}
.term-canvas,
.term-svg {
  width: 100%;
  height: 300rpx;
  display: block;
}
.term-axis-label {
  fill: #dbeafe;
  font-size: 20px;
  font-weight: 800;
  font-variant-numeric: tabular-nums;
}
.term-axis-y {
  fill: #f8fafc;
}
.term-axis-overlay {
  position: absolute;
  left: 0;
  right: 0;
  top: 0;
  bottom: 0;
  pointer-events: none;
}
.term-axis-tag {
  position: absolute;
  z-index: 2;
  padding: 1rpx 4rpx;
  border-radius: 6rpx;
  background: rgba(11, 21, 40, 0.64);
  color: #eaf2ff;
  font-size: 20rpx;
  line-height: 1.1;
  font-weight: 800;
  text-shadow: 0 1rpx 4rpx rgba(0, 0, 0, 0.9);
  font-variant-numeric: tabular-nums;
}
.term-axis-tag-y {
  color: #ffffff;
  font-size: 18rpx;
  font-weight: 700;
}
.term-axis-tag-x {
  color: #dbeafe;
  min-width: 42rpx;
  text-align: center;
}
.term-tooltip-layer {
  position: absolute;
  inset: 0;
  pointer-events: none;
  z-index: 3;
}
.term-tooltip-guide {
  position: absolute;
  top: 12rpx;
  bottom: 18rpx;
  width: 2rpx;
  background: rgba(226, 232, 240, 0.34);
  transform: translateX(-50%);
}
.term-tooltip-dot {
  position: absolute;
  width: 14rpx;
  height: 14rpx;
  border-radius: 999rpx;
  border: 2rpx solid rgba(255, 255, 255, 0.9);
  box-shadow: 0 0 0 4rpx rgba(11, 21, 40, 0.45);
  transform: translate(-50%, -50%);
}
.term-tooltip-box {
  position: absolute;
  min-width: 176rpx;
  max-width: 260rpx;
  padding: 12rpx 14rpx;
  border-radius: 16rpx;
  border: 1px solid rgba(125, 211, 252, 0.28);
  background: rgba(7, 14, 27, 0.92);
  box-shadow: 0 12rpx 36rpx rgba(0, 0, 0, 0.28);
  transform: translate(-10%, -100%);
}
.term-tooltip-box-right {
  transform: translate(calc(-100% - 14rpx), -100%);
}
.term-tooltip-box-below {
  transform: translate(-10%, 14rpx);
}
.term-tooltip-box-right.term-tooltip-box-below {
  transform: translate(calc(-100% - 14rpx), 14rpx);
}
.term-tooltip-title {
  display: block;
  color: #f8fafc;
  font-size: 22rpx;
  font-weight: 800;
  font-variant-numeric: tabular-nums;
}
.term-tooltip-row {
  margin-top: 8rpx;
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 12rpx;
  color: #dbeafe;
  font-size: 20rpx;
}
.term-tooltip-row-left {
  display: flex;
  align-items: center;
  gap: 8rpx;
}
.term-tooltip-val {
  color: #f8fafc;
  font-size: 20rpx;
  font-weight: 700;
  font-variant-numeric: tabular-nums;
}
.term-legend {
  display: flex;
  flex-wrap: wrap;
  gap: 10rpx 18rpx;
  margin-top: 12rpx;
}
.legend-item {
  display: flex;
  align-items: center;
  gap: 8rpx;
  color: #9fb0cd;
  font-size: 21rpx;
}
.legend-dot {
  width: 14rpx;
  height: 14rpx;
  border-radius: 999rpx;
  flex-shrink: 0;
}
.term-empty {
  height: 180rpx;
  border-radius: 18rpx;
  border: 1px dashed #263854;
  color: #71829b;
  font-size: 23rpx;
  display: flex;
  align-items: center;
  justify-content: center;
}
.picker-sheet-compact {
  max-height: 560rpx;
}
.term-table {
  min-width: 760rpx;
}
.term-tr {
  display: flex;
  align-items: center;
  min-height: 62rpx;
  border-bottom: 1px solid #17263e;
}
.term-tr:last-child { border-bottom: none; }
.term-th {
  min-height: 54rpx;
  background: rgba(14, 28, 49, 0.8);
  border-radius: 12rpx;
  border-bottom: none;
  margin-bottom: 6rpx;
}
.term-td {
  width: 170rpx;
  padding: 0 12rpx;
  box-sizing: border-box;
  color: #cbd5e1;
  font-size: 22rpx;
  font-variant-numeric: tabular-nums;
  text-align: right;
}
.term-th .term-td {
  color: #7d8ba4;
  font-size: 20rpx;
  font-weight: 700;
}
.term-contract {
  width: 150rpx;
  text-align: left;
  color: #f8fafc;
  font-weight: 700;
}

/* 实时点（tab 栏内）*/
.live-dot-wrap { display: flex; align-items: center; gap: 6rpx; padding: 0 14rpx; }
.live-dot { width: 10rpx; height: 10rpx; border-radius: 50%; background: #22c55e; box-shadow: 0 0 6rpx #22c55e; animation: pulse 2s infinite; flex-shrink: 0; }
.live-dot-text { font-size: 18rpx; color: #334155; white-space: nowrap; }
@keyframes pulse { 0%,100%{opacity:1} 50%{opacity:0.4} }

/* 实时涨跌徽标 */
.pct-row { display: flex; align-items: center; gap: 6rpx; justify-content: flex-end; }
.live-badge { display: flex; align-items: center; }
.live-badge-text { font-size: 14rpx; color: #22c55e; line-height: 1; }

/* 混乱指数 */
.chaos-wrap { padding: 16rpx; }
.chaos-content { display: flex; flex-direction: column; gap: 16rpx; }
.chaos-card {
  background: #0f1b2e;
  border: 1px solid #1f2f4c;
  border-radius: 16rpx;
  padding: 18rpx;
}
.chaos-core-head {
  display: flex;
  align-items: center;
  justify-content: space-between;
}
.chaos-core-title { font-size: 28rpx; color: #f0f0f0; font-weight: 700; }
.chaos-core-time { font-size: 22rpx; color: #94a3b8; font-variant-numeric: tabular-nums; }
.chaos-gauge-wrap {
  position: relative;
  height: 232rpx;
  margin-top: 8rpx;
}
.chaos-gauge-track {
  position: absolute;
  left: 50%;
  bottom: 16rpx;
  transform: translateX(-50%);
  width: 420rpx;
  height: 210rpx;
  border-radius: 210rpx 210rpx 0 0;
  overflow: hidden;
  display: flex;
  border: 2rpx solid #223451;
  border-bottom: none;
  box-sizing: border-box;
}
.chaos-gauge-seg { flex: 1; }
.chaos-gauge-seg-1 { background: linear-gradient(180deg, #22c55e, #16a34a); }
.chaos-gauge-seg-2 { background: linear-gradient(180deg, #facc15, #eab308); }
.chaos-gauge-seg-3 { background: linear-gradient(180deg, #fb923c, #f97316); }
.chaos-gauge-seg-4 { background: linear-gradient(180deg, #ef4444, #dc2626); }
.chaos-gauge-inner {
  position: absolute;
  left: 50%;
  bottom: 16rpx;
  transform: translateX(-50%);
  width: 326rpx;
  height: 163rpx;
  border-radius: 163rpx 163rpx 0 0;
  background: #0f1b2e;
  border: 2rpx solid #223451;
  border-bottom: none;
  box-sizing: border-box;
}
.chaos-gauge-needle-wrap {
  position: absolute;
  left: 50%;
  bottom: 16rpx;
  width: 0;
  height: 0;
  transform-origin: 50% 100%;
  z-index: 2;
}
.chaos-gauge-needle {
  width: 6rpx;
  height: 132rpx;
  background: linear-gradient(180deg, #fde68a, #f59e0b);
  border-radius: 999rpx;
  transform: translate(-50%, -100%);
  box-shadow: 0 0 10rpx rgba(245, 158, 11, 0.45);
}
.chaos-gauge-cap {
  position: absolute;
  left: 50%;
  bottom: 8rpx;
  transform: translateX(-50%);
  width: 24rpx;
  height: 24rpx;
  border-radius: 50%;
  background: #f59e0b;
  border: 3rpx solid #fde68a;
  z-index: 3;
}
.chaos-gauge-tick {
  position: absolute;
  bottom: 0;
  font-size: 20rpx;
  color: #94a3b8;
  font-variant-numeric: tabular-nums;
}
.chaos-gauge-tick-left { left: 12rpx; }
.chaos-gauge-tick-mid {
  left: 50%;
  transform: translateX(-50%);
}
.chaos-gauge-tick-right { right: 12rpx; }
.chaos-core-main {
  margin-top: 10rpx;
  display: flex;
  align-items: baseline;
  gap: 16rpx;
}
.chaos-score {
  font-size: 68rpx;
  font-weight: 800;
  line-height: 1;
  color: #f5c518;
  font-variant-numeric: tabular-nums;
}
.chaos-band { font-size: 28rpx; font-weight: 700; }
.chaos-meta-row { display: flex; gap: 12rpx; margin-top: 12rpx; }
.chaos-meta-item {
  flex: 1;
  border-radius: 12rpx;
  border: 1px solid #233656;
  background: #101f36;
  padding: 10rpx 12rpx;
}
.chaos-meta-label { display: block; font-size: 20rpx; color: #7e95b8; }
.chaos-meta-val {
  display: block;
  margin-top: 4rpx;
  font-size: 30rpx;
  color: #f0f0f0;
  font-weight: 700;
  font-variant-numeric: tabular-nums;
}
.chaos-card-head {
  display: flex;
  align-items: center;
  justify-content: space-between;
  margin-bottom: 10rpx;
}
.chaos-card-title { font-size: 26rpx; color: #f0f0f0; font-weight: 700; }
.chaos-card-sub { font-size: 20rpx; color: #64748b; }
.chaos-market-row {
  display: flex;
  align-items: center;
  gap: 12rpx;
  padding: 12rpx 0;
  border-bottom: 1px solid #15243c;
}
.chaos-market-row:last-child { border-bottom: none; }
.chaos-rank {
  width: 34rpx;
  text-align: center;
  font-size: 20rpx;
  color: #94a3b8;
  font-variant-numeric: tabular-nums;
}
.chaos-market-main { flex: 1; min-width: 0; }
.chaos-market-title-line { display: flex; align-items: center; gap: 8rpx; min-width: 0; }
.chaos-market-title {
  display: inline-block;
  font-size: 24rpx;
  color: #f0f0f0;
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
}
.chaos-trend {
  font-size: 18rpx;
  font-variant-numeric: tabular-nums;
  padding: 2rpx 8rpx;
  border-radius: 999rpx;
  border: 1px solid #334155;
  line-height: 1.1;
  flex-shrink: 0;
}
.chaos-trend-up {
  color: #f87171;
  background: rgba(248, 113, 113, 0.12);
  border-color: rgba(248, 113, 113, 0.34);
}
.chaos-trend-down {
  color: #34d399;
  background: rgba(52, 211, 153, 0.12);
  border-color: rgba(52, 211, 153, 0.34);
}
.chaos-trend-heat {
  font-size: 19rpx;
  line-height: 1;
  flex-shrink: 0;
}
.chaos-market-meta { display: block; margin-top: 3rpx; font-size: 20rpx; color: #64748b; }
.chaos-market-right { width: 150rpx; text-align: right; }
.chaos-market-prob {
  display: block;
  font-size: 24rpx;
  color: #f5c518;
  font-variant-numeric: tabular-nums;
}
.chaos-market-delta {
  display: block;
  margin-top: 3rpx;
  font-size: 20rpx;
  font-variant-numeric: tabular-nums;
}
.chaos-driver-head,
.chaos-driver-row {
  display: flex;
  align-items: center;
  border-bottom: 1px solid #15243c;
}
.chaos-driver-head { padding: 8rpx 0; }
.chaos-driver-row { padding: 12rpx 0; }
.chaos-driver-row:last-child { border-bottom: none; }
.chaos-driver-col {
  width: 120rpx;
  text-align: right;
  font-size: 22rpx;
  color: #cbd5e1;
  font-variant-numeric: tabular-nums;
}
.chaos-driver-title-col { flex: 1; text-align: left; width: auto; min-width: 0; }
.chaos-driver-title {
  display: block;
  font-size: 23rpx;
  color: #f0f0f0;
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
}
.chaos-driver-meta { display: block; margin-top: 3rpx; font-size: 19rpx; color: #64748b; }
.chaos-cat-row { margin-bottom: 14rpx; }
.chaos-cat-row:last-child { margin-bottom: 0; }
.chaos-cat-top {
  display: flex;
  align-items: center;
  justify-content: space-between;
  margin-bottom: 8rpx;
}
.chaos-cat-label { font-size: 22rpx; color: #dbeafe; }
.chaos-cat-total {
  font-size: 22rpx;
  color: #f8fafc;
  font-variant-numeric: tabular-nums;
}
.chaos-bar-wrap {
  height: 16rpx;
  background: #13223a;
  border-radius: 999rpx;
  border: 1px solid #1f2f4c;
  overflow: hidden;
  display: flex;
}
.chaos-bar-base { height: 100%; background: linear-gradient(90deg, #22c55e, #16a34a); }
.chaos-bar-esca { height: 100%; background: linear-gradient(90deg, #38bdf8, #3b82f6); }

/* 通用 */
.center-tip { text-align: center; padding: 60rpx 0; }
.muted-text { font-size: 24rpx; color: #555555; }
</style>
