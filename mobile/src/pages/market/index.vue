<script setup lang="ts">
import { ref, computed } from 'vue'
import { onShow, onHide, onShareAppMessage, onShareTimeline } from '@dcloudio/uni-app'
import { marketApi, type OptionItem, type ContractLiveItem } from '../../api/index'
import { useAuthStore } from '../../store/auth'
import BottomNav from '../../components/BottomNav.vue'

const auth = useAuthStore()
const SHARE_TITLE = '爱波塔 - 期货期权行情分析'
const SHARE_PATH = '/pages/login/index'

// ── Tab ──────────────────────────────────────────────────
type Tab = 'options' | 'holding'
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

const holdingProductName = computed(() => {
  const p = HOLDING_PRODUCTS.find(p => p.code === holdingProduct.value)
  return p ? `${p.name} (${p.code.toUpperCase()})` : holdingProduct.value.toUpperCase()
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
  eb:'petro', bu:'petro', ru:'petro', nr:'petro', sc:'petro',
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

// ── 事件处理 ──────────────────────────────────────────────
onShow(() => {
  if (!auth.isLoggedIn) { uni.reLaunch({ url: '/pages/login/index' }); return }
  if (options.value.length === 0) loadOptions()
  startLivePolling()
})

onHide(() => {
  stopLivePolling()
})

async function switchTab(t: Tab) {
  activeTab.value = t
  if (t === 'options' && options.value.length === 0) loadOptions()
  if (t === 'holding' && !holdingProduct.value) {
    holdingProduct.value = HOLDING_PRODUCTS[0].code
    loadHolding(holdingProduct.value)
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
  } else if (activeTab.value === 'holding') {
    if (holdingProduct.value) loadHolding(holdingProduct.value)
  }
  fetchLive()
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
      <view class="tab-item" :class="{ active: activeTab === 'options' }" @tap="switchTab('options')">
        <text>价格分析</text>
      </view>
      <view class="tab-item" :class="{ active: activeTab === 'holding' }" @tap="switchTab('holding')">
        <text>持仓分析</text>
      </view>
      <!-- 实时状态指示点 -->
      <view v-if="liveTrading" class="live-dot-wrap">
        <view class="live-dot" /><text class="live-dot-text">{{ liveAt }}</text>
      </view>
      <view class="tab-refresh" @tap="refresh">
        <text class="refresh-icon" :class="{ spinning: optLoading || holdingLoading }">↻</text>
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
          <text class="hth hth-broker">期货商</text>
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
          <text class="muted-text">得分 = 净持仓方向与行情相关性（近150天），正分=做对方向。点击期货商查看明细。</text>
        </view>
      </view>

      <view v-else-if="!holdingLoading" class="center-tip">
        <text class="muted-text">该品种暂无持仓数据</text>
      </view>
    </view>

    <!-- 品种选择弹窗（覆盖整个页面）-->
    <view v-if="showHoldingPicker" class="picker-overlay" @tap.self="closeHoldingPicker">
      <view class="picker-sheet">
        <view class="picker-sheet-header">
          <text class="picker-sheet-title">选择品种</text>
          <text class="picker-sheet-close" @tap="closeHoldingPicker">✕</text>
        </view>
        <view class="picker-search-bar">
          <input
            class="picker-search-input"
            v-model="holdingSearch"
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

    <view style="height: 120rpx;" />
    <BottomNav active="market" />
  </view>
</template>

<style scoped>
.page { background: #0b1121; min-height: 100vh; }

/* Tab */
.tab-bar { display: flex; align-items: center; border-bottom: 1px solid #162035; }
.tab-item { flex: 1; text-align: center; padding: 22rpx 0; font-size: 28rpx; color: #666666; }
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

.picker-search-bar { padding: 8rpx 24rpx 16rpx; flex-shrink: 0; }
.picker-search-input {
  width: 100%; background: #131c2e; border: 1px solid #1e2d45;
  border-radius: 14rpx; padding: 14rpx 20rpx; font-size: 26rpx; color: #f0f0f0;
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
.picker-item-check { font-size: 26rpx; color: #f5c518; flex-shrink: 0; }

/* 实时点（tab 栏内）*/
.live-dot-wrap { display: flex; align-items: center; gap: 6rpx; padding: 0 14rpx; }
.live-dot { width: 10rpx; height: 10rpx; border-radius: 50%; background: #22c55e; box-shadow: 0 0 6rpx #22c55e; animation: pulse 2s infinite; flex-shrink: 0; }
.live-dot-text { font-size: 18rpx; color: #334155; white-space: nowrap; }
@keyframes pulse { 0%,100%{opacity:1} 50%{opacity:0.4} }

/* 实时涨跌徽标 */
.pct-row { display: flex; align-items: center; gap: 6rpx; justify-content: flex-end; }
.live-badge { display: flex; align-items: center; }
.live-badge-text { font-size: 14rpx; color: #22c55e; line-height: 1; }

/* 通用 */
.center-tip { text-align: center; padding: 60rpx 0; }
.muted-text { font-size: 24rpx; color: #555555; }
</style>
