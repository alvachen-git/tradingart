<script setup lang="ts">
import { ref, computed } from 'vue'
import { onLoad } from '@dcloudio/uni-app'
import { marketApi, type BrokerDetailRow } from '../../api/index'

const product     = ref('')
const broker      = ref('')
const productName = ref('')
const rows        = ref<BrokerDetailRow[]>([])
const totalScore  = ref(0)
const loading     = ref(false)
const errMsg      = ref('')

onLoad((options: any) => {
  product.value     = options?.product ?? ''
  broker.value      = decodeURIComponent(options?.broker ?? '')
  productName.value = decodeURIComponent(options?.product_name ?? '')
  fetchDetail()
})

async function fetchDetail() {
  loading.value = true
  errMsg.value  = ''
  try {
    const res = await marketApi.brokerDetail(product.value, broker.value)
    rows.value       = res.rows
    totalScore.value = res.total_score
  } catch (e: any) {
    errMsg.value = e.message || '加载失败'
  } finally {
    loading.value = false
  }
}

// ── SVG cumulative-score chart ────────────────────────────
const SVG_W  = 660, SVG_H  = 180
const PAD_L  = 42,  PAD_R  = 12, PAD_T = 16, PAD_B = 28

const chartRows = computed(() =>
  [...rows.value].sort((a, b) => a.dt.localeCompare(b.dt))
)

const svgData = computed(() => {
  const data = chartRows.value
  if (data.length < 2) return null

  const innerW = SVG_W - PAD_L - PAD_R
  const innerH = SVG_H - PAD_T - PAD_B
  const scores = data.map(r => r.cum_score)
  const minS   = Math.min(...scores, 0)
  const maxS   = Math.max(...scores, 0)
  const range  = maxS - minS || 1

  const toX = (i: number) => PAD_L + (i / (data.length - 1)) * innerW
  const toY = (v: number) => PAD_T + (1 - (v - minS) / range) * innerH

  const pts = data.map((r, i) => ({ x: toX(i), y: toY(r.cum_score) }))
  const linePath = 'M' + pts.map(p => `${p.x.toFixed(1)},${p.y.toFixed(1)}`).join('L')

  const zeroY   = toY(0)
  const clipY   = Math.min(SVG_H - PAD_B, Math.max(PAD_T, zeroY))
  const areaPath = linePath
    + `L${pts[pts.length - 1].x.toFixed(1)},${clipY.toFixed(1)}`
    + `L${pts[0].x.toFixed(1)},${clipY.toFixed(1)}Z`

  // X ticks (~5 labels)
  const tickCount = Math.min(5, data.length)
  const xTicks = Array.from({ length: tickCount }, (_, i) => {
    const idx   = Math.round(i * (data.length - 1) / (tickCount - 1))
    const label = data[idx].dt.slice(5)   // MM-DD
    return { x: toX(idx).toFixed(1), label }
  })

  // Y labels: minS, 0, maxS (deduplicated)
  const yVals = [...new Set([minS, 0, maxS].map(v => Math.round(v)))]
  const yLabels = yVals.map(v => ({
    y: toY(v).toFixed(1),
    label: v > 0 ? '+' + v : String(v),
  }))

  return { linePath, areaPath, zeroY: zeroY.toFixed(1), xTicks, yLabels }
})

const lineColor = computed(() => totalScore.value >= 0 ? '#e84040' : '#22c55e')
const areaFill  = computed(() => totalScore.value >= 0 ? 'rgba(232,64,64,0.09)' : 'rgba(34,197,94,0.09)')

// ── Helpers ───────────────────────────────────────────────
function scoreColor(v: number) { return v > 0 ? '#e84040' : v < 0 ? '#22c55e' : '#888888' }
function fmtScore(v: number)   { return (v > 0 ? '+' : '') + Math.round(v) }
function fmtVol(v: number)     { return (v > 0 ? '+' : '') + v }
function fmtPct(v: number)     { return (v > 0 ? '+' : '') + v.toFixed(2) + '%' }
</script>

<template>
  <view class="page">

    <!-- Header -->
    <view class="broker-header">
      <view class="title-row">
        <text class="broker-name">{{ broker }}</text>
        <text class="product-tag">{{ productName }} ({{ product.toUpperCase() }})</text>
      </view>
      <view class="score-row">
        <text class="score-label">150天累计得分</text>
        <text class="score-val" :style="{ color: scoreColor(totalScore) }">
          {{ fmtScore(totalScore) }}
        </text>
      </view>
    </view>

    <!-- Loading / Error -->
    <view v-if="loading" class="center-tip"><text class="muted">加载中...</text></view>
    <view v-else-if="errMsg" class="center-tip"><text class="muted">{{ errMsg }}</text></view>

    <template v-else-if="rows.length">

      <!-- Cumulative score chart -->
      <view class="chart-section">
        <text class="section-title">累计得分走势</text>
        <view class="chart-wrap">
          <svg
            v-if="svgData"
            :width="SVG_W" :height="SVG_H"
            :viewBox="`0 0 ${SVG_W} ${SVG_H}`"
            preserveAspectRatio="none"
            style="width:100%;height:180px;display:block"
          >
            <!-- Zero dashed line -->
            <line
              :x1="PAD_L" :y1="svgData.zeroY"
              :x2="SVG_W - PAD_R" :y2="svgData.zeroY"
              stroke="#2a3a52" stroke-width="1" stroke-dasharray="4,3"
            />
            <!-- Area fill -->
            <path :d="svgData.areaPath" :fill="areaFill" />
            <!-- Score line -->
            <path
              :d="svgData.linePath" fill="none"
              :stroke="lineColor" stroke-width="2"
              stroke-linejoin="round" stroke-linecap="round"
            />
            <!-- Y labels -->
            <text
              v-for="l in svgData.yLabels" :key="l.y"
              :x="PAD_L - 4" :y="Number(l.y) + 4"
              text-anchor="end" font-size="10" fill="#556070"
            >{{ l.label }}</text>
            <!-- X tick labels -->
            <text
              v-for="t in svgData.xTicks" :key="t.x"
              :x="t.x" :y="SVG_H - 6"
              text-anchor="middle" font-size="10" fill="#556070"
            >{{ t.label }}</text>
          </svg>
        </view>
      </view>

      <!-- Daily detail table (newest first, max 40 rows) -->
      <view class="table-section">
        <text class="section-title">近期持仓明细</text>
        <view class="table-header">
          <text class="c-dt">日期</text>
          <text class="c-vol">净持仓</text>
          <text class="c-pct">价格涨跌</text>
          <text class="c-score">当日得分</text>
          <text class="c-cum">累计</text>
        </view>
        <view class="table-body">
          <view
            v-for="r in [...chartRows].reverse().slice(0, 40)"
            :key="r.dt"
            class="table-row"
          >
            <text class="c-dt">{{ r.dt.slice(5) }}</text>
            <text class="c-vol" :style="{ color: scoreColor(r.net_vol) }">{{ fmtVol(r.net_vol) }}</text>
            <text class="c-pct" :style="{ color: scoreColor(r.pct_chg) }">{{ fmtPct(r.pct_chg) }}</text>
            <text class="c-score fw" :style="{ color: scoreColor(r.score) }">{{ fmtScore(r.score) }}</text>
            <text class="c-cum fw" :style="{ color: scoreColor(r.cum_score) }">{{ fmtScore(r.cum_score) }}</text>
          </view>
        </view>
      </view>

    </template>

    <view v-else-if="!loading" class="center-tip">
      <text class="muted">该期货商暂无持仓数据</text>
    </view>

    <view style="height: 60rpx;" />
  </view>
</template>

<style scoped>
.page { background: #0b1121; min-height: 100vh; }

/* Header */
.broker-header { padding: 28rpx 24rpx 20rpx; border-bottom: 1px solid #162035; }
.title-row { display: flex; align-items: baseline; flex-wrap: wrap; gap: 14rpx; margin-bottom: 14rpx; }
.broker-name { font-size: 36rpx; color: #f0f0f0; font-weight: 700; }
.product-tag { font-size: 22rpx; color: #888888; }
.score-row   { display: flex; align-items: center; gap: 16rpx; }
.score-label { font-size: 24rpx; color: #666666; }
.score-val   { font-size: 42rpx; font-weight: 700; font-variant-numeric: tabular-nums; }

/* Chart */
.chart-section { padding: 20rpx 0 0; }
.section-title { display: block; font-size: 24rpx; color: #666666; padding: 0 24rpx 10rpx; }
.chart-wrap { padding: 0 6rpx 6rpx; }

/* Table */
.table-section { padding: 20rpx 0 0; }
.table-header {
  display: flex; padding: 10rpx 24rpx;
  background: #0d1829; border-bottom: 1px solid #131c2e;
}
.table-header text { font-size: 20rpx; color: #555555; }
.table-body { padding: 0 16rpx; }
.table-row {
  display: flex; align-items: center; padding: 16rpx 8rpx;
  border-bottom: 1px solid #131c2e;
}
.table-row text { font-size: 22rpx; font-variant-numeric: tabular-nums; }

.c-dt    { flex: 1.6; color: #888888; }
.c-vol   { flex: 1.5; text-align: right; }
.c-pct   { flex: 1.4; text-align: right; }
.c-score { flex: 1.2; text-align: right; }
.c-cum   { flex: 1.2; text-align: right; }
.fw      { font-weight: 600; }

/* Misc */
.center-tip { text-align: center; padding: 80rpx 0; }
.muted      { font-size: 26rpx; color: #555555; }
</style>
