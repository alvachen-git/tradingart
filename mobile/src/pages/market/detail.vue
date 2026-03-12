<script setup lang="ts">
import { ref, computed, watch } from 'vue'
import { onLoad, onHide, onUnload } from '@dcloudio/uni-app'
import { marketApi } from '../../api/index'

// 把 buildLine 输出的 "x,y ..." 中的 y 减去 offsetY（每个子图 SVG 从0起算）
function rebasePoints(pts: string, offsetY: number): string {
  return pts.replace(/(-?\d+\.?\d*),(-?\d+\.?\d*)/g, (_m, x, y) =>
    `${x},${(parseFloat(y) - offsetY).toFixed(1)}`
  )
}

const chartData  = ref<any>(null)
const loading    = ref(true)
const error      = ref('')
const initName   = ref('')
const initIvRank = ref(-1)
const initIv     = ref(0)

// ── 今日实时K线 ───────────────────────────────────────────
const liveCandle = ref<{dt:string;o:number;h:number;l:number;c:number;pct:number} | null>(null)
const isTrading   = ref(false)   // 当前是否交易时段（用于头部显示实时标签）
let _pollTimer: ReturnType<typeof setInterval> | null = null

function _todayStr() {
  const d = new Date()
  return d.getFullYear().toString()
    + String(d.getMonth()+1).padStart(2,'0')
    + String(d.getDate()).padStart(2,'0')
}

function normalizeTradeDay(v: any): string {
  if (typeof v !== 'string') return ''
  const s = v.replace(/-/g, '').trim()
  return /^\d{8}$/.test(s) ? s : ''
}

const NIGHT_SESSION_PRODUCTS = new Set([
  'au','ag','cu','al','zn','pb','ni','sn','rb','hc','ss','fu','bu','ru','sp','sc','lu','bc','ao',
  'a','b','m','y','p','c','cs','jd','l','pp','v','eb','eg','j','jm','i','rr','pg','lh',
  'sr','cf','ta','ma','rm','oi','zc','fg','sa','ur','ap','cj','lc','si','ps','pr','sf','sm','pf','cy',
])

function getProductFromContract(contract: string): string {
  const m = (contract || '').toUpperCase().match(/^([A-Z]+)\d+$/)
  return m ? m[1].toLowerCase() : ''
}

function nextTradingDayApprox(dayStr: string): string {
  const d = new Date(`${dayStr.slice(0,4)}-${dayStr.slice(4,6)}-${dayStr.slice(6,8)}T00:00:00`)
  d.setDate(d.getDate() + 1)
  while (d.getDay() === 0 || d.getDay() === 6) d.setDate(d.getDate() + 1)
  return d.getFullYear().toString()
    + String(d.getMonth() + 1).padStart(2, '0')
    + String(d.getDate()).padStart(2, '0')
}

function resolveLiveTradingDay(contract: string): string {
  const today = _todayStr()
  const product = getProductFromContract(contract)
  if (!NIGHT_SESSION_PRODUCTS.has(product)) return today

  const now = new Date()
  const hhmm = now.getHours() * 60 + now.getMinutes()
  if (hhmm >= 21 * 60) return nextTradingDayApprox(today)
  return today
}

async function fetchLiveCandle() {
  if (!chartData.value?.main_contract) return
  try {
    const res = await marketApi.prices()
    isTrading.value = res.is_trading ?? false
    if (!res.is_trading || !res.contracts) {
      // 收盘后：不清空 liveCandle，保留最后实时价格直到 DB 更新
      return
    }
    const code = chartData.value.main_contract.toUpperCase()
    const item = res.contracts[code]
    if (!item || item.price <= 0) return

    const tradingDay = normalizeTradeDay((item as any).trading_day) || resolveLiveTradingDay(code)
    if (liveCandle.value && liveCandle.value.dt !== tradingDay) {
      liveCandle.value = null
    }
    liveCandle.value = {
      dt: tradingDay,
      o: item.open,
      h: item.high,
      l: item.low,
      c: item.price,
      pct: item.pct,
    }
  } catch (_) {}
}

function startLivePoll() {
  if (_pollTimer) return
  fetchLiveCandle()
  _pollTimer = setInterval(fetchLiveCandle, 10000)
}

function stopLivePoll() {
  if (_pollTimer) { clearInterval(_pollTimer); _pollTimer = null }
}

onHide(() => stopLivePoll())
onUnload(() => stopLivePoll())

onLoad(async (options) => {
  const product = options?.product || ''
  initName.value   = decodeURIComponent(options?.name || product)
  initIvRank.value = Number(options?.iv_rank ?? -1)
  initIv.value     = Number(options?.iv ?? 0)
  if (!product) { error.value = '参数错误'; loading.value = false; return }
  try {
    chartData.value = await marketApi.chart(product)
    startLivePoll()
  } catch (e: any) {
    error.value = e.message || '加载失败'
  } finally {
    loading.value = false
  }
})

// ── 视窗控制 ──────────────────────────────────────────────
const startIdx    = ref(0)
const barCount    = ref(63)
const activeRange = ref('3M')

// Historical OHLC + live daily bar merged by trading day (not natural day).
const allOhlc = computed(() => {
  const hist: any[] = chartData.value?.ohlc ?? []
  const live = liveCandle.value
  if (!live) return hist

  const liveBar = { ...live, _live: true }
  if (!hist.length) return [liveBar]

  const last = hist[hist.length - 1]
  if (live.dt === last.dt) return [...hist.slice(0, -1), liveBar]
  if (live.dt > last.dt) return [...hist, liveBar]
  return hist
})
const allIv      = computed(() => chartData.value?.iv       || [])
const allDumb    = computed(() => chartData.value?.dumb     || [])
const allSmart   = computed(() => chartData.value?.smart    || [])
const allTotalOi = computed(() => chartData.value?.total_oi || [])

watch(allOhlc, (arr) => { if (arr.length) applyRange('3M') }, { immediate: true })

const RANGE_BARS: Record<string, number> = { '1M': 21, '3M': 63, '6M': 126, '1Y': 252 }

function applyRange(label: string) {
  activeRange.value = label
  const n = allOhlc.value.length
  if (!n) return
  if (label === '全部') { barCount.value = n; startIdx.value = 0 }
  else { barCount.value = Math.min(RANGE_BARS[label] || 63, n); startIdx.value = Math.max(0, n - barCount.value) }
}

const visOhlc = computed(() => {
  const end = Math.min(startIdx.value + barCount.value, allOhlc.value.length)
  return allOhlc.value.slice(startIdx.value, end)
})
function sliceByDates(arr: any[]) {
  if (!arr.length || !visOhlc.value.length) return []
  const s = visOhlc.value[0].dt, e = visOhlc.value[visOhlc.value.length-1].dt
  return arr.filter((d: any) => d.dt >= s && d.dt <= e)
}
const visIv      = computed(() => sliceByDates(allIv.value))
const visDumb    = computed(() => sliceByDates(allDumb.value))
const visSmart   = computed(() => sliceByDates(allSmart.value))
const visTotalOi = computed(() => sliceByDates(allTotalOi.value))

// ── SVG 尺寸常量 ──────────────────────────────────────────
const SVG_W   = 660
const PAD_L   = 6
const PAD_R   = 6
const PAD_TOP = 8

// 各区域在 SVG 中的 Y 起始和高度
const CANDLE_H = 200
const DIV      = 2    // 细分割线
const IV_H     = 70
const DUMB_H   = 70
const SMART_H  = 70
const OI_H     = 70

const IV_Y0    = CANDLE_H + DIV
const DUMB_Y0  = IV_Y0 + IV_H + DIV
const SMART_Y0 = DUMB_Y0 + DUMB_H + DIV
const OI_Y0    = SMART_Y0 + SMART_H + DIV
const totalH   = OI_Y0 + OI_H

// 百分比坐标转换（用于 HTML 叠加层定位）
const pctX = (x: number) => (x / SVG_W * 100).toFixed(2) + '%'
const pctY = (y: number) => (y / totalH * 100).toFixed(2) + '%'

// ── K线图计算 ─────────────────────────────────────────────
const candleChart = computed(() => {
  const ohlc = visOhlc.value
  if (!ohlc || ohlc.length < 2) return null
  const highs = ohlc.map((d:any)=>d.h), lows = ohlc.map((d:any)=>d.l)
  const maxV = Math.max(...highs), minV = Math.min(...lows), range = maxV - minV || 1
  const usableW = SVG_W - PAD_L - PAD_R
  const usableH = CANDLE_H - PAD_TOP - 14
  const n = ohlc.length, slot = usableW / n, barW = Math.max(1, slot * 0.72)
  const toY = (v:number) => PAD_TOP + usableH - ((v - minV) / range) * usableH
  const toX = (i:number) => PAD_L + (i + 0.5) * slot
  const candles = ohlc.map((d:any,i:number) => {
    const oy=toY(d.o), cy=toY(d.c), up=d.c>=d.o
    return { x:toX(i), barW, hy:toY(d.h), ly:toY(d.l), up, bodyTop:Math.min(oy,cy), bodyH:Math.max(Math.abs(cy-oy),1), live:!!d._live }
  })
  const midV = (maxV+minV)/2
  // X轴刻度（首/25%/中/75%/尾）
  const idxs = [0, Math.floor(n*0.25), Math.floor(n*0.5), Math.floor(n*0.75), n-1]
  const seen = new Set<number>()
  const xTicks = idxs.filter(i=>{ if(seen.has(i)) return false; seen.add(i); return true })
    .map(i => ({ x: toX(i), label: ohlc[i].dt.slice(4,6)+'/'+ohlc[i].dt.slice(6,8) }))
  const xAxisY = CANDLE_H - 10  // x轴标签在SVG中的Y位置（用于HTML叠加）
  return {
    candles, xTicks, xAxisY,
    maxLabel: fmtNum(maxV), midLabel: fmtNum(midV), minLabel: fmtNum(minV),
    maxY: toY(maxV), midY: toY(midV), minY: toY(minV),
  }
})

// ── 通用折线图 ────────────────────────────────────────────
function buildLine(arr: any[], key: string, offsetY: number, panelH: number) {
  if (!arr || arr.length < 2) return null
  const vals = arr.map((d:any)=>d[key])
  const maxV = Math.max(...vals), minV = Math.min(...vals), range = maxV-minV||1
  const usableW = SVG_W - PAD_L - PAD_R, usableH = panelH - 4
  const toX = (i:number) => PAD_L + (i/(arr.length-1))*usableW
  const toY = (v:number) => offsetY + (usableH - ((v-minV)/range)*usableH)
  const pts = arr.map((d:any,i:number)=>`${toX(i).toFixed(1)},${toY(d[key]).toFixed(1)}`).join(' ')
  const lastX=toX(arr.length-1), lastY=toY(arr[arr.length-1][key])
  const fillPts = pts+` ${toX(arr.length-1).toFixed(1)},${(offsetY+usableH).toFixed(1)} ${PAD_L},${(offsetY+usableH).toFixed(1)}`
  const zeroY = (minV<0&&maxV>0) ? toY(0) : null
  return {
    pts, fillPts, zeroY,
    lastX: lastX.toFixed(1), lastY: lastY.toFixed(1),
    lastVal: arr[arr.length-1][key],
    maxLabel: fmtCompact(maxV), minLabel: fmtCompact(minV),
    maxY: toY(maxV), minY: toY(minV),
  }
}
const ivLine    = computed(() => buildLine(visIv.value,      'v',   IV_Y0,    IV_H))
const dumbLine  = computed(() => buildLine(visDumb.value,    'net', DUMB_Y0,  DUMB_H))
const smartLine = computed(() => buildLine(visSmart.value,   'net', SMART_Y0, SMART_H))
const oiLine    = computed(() => buildLine(visTotalOi.value, 'v',   OI_Y0,    OI_H))

// ── 长按 OHLC 浮框 ────────────────────────────────────────
const tooltip = ref<{visible:boolean;x:number;leftPct:number;dt:string;o:number;h:number;l:number;c:number;pct:number;iv:number|null}>
  ({visible:false,x:0,leftPct:0,dt:'',o:0,h:0,l:0,c:0,pct:0,iv:null})

let lpTimer:any=null, lpStartX=0, lpStartY=0
let t0X=0, t0Idx=0, t0Dist=0, t0Count=0

function getW() { try { return (uni.getSystemInfoSync().windowWidth||375)-48 } catch { return 327 } }

function onTouchStart(e:any) {
  if(e.touches.length===1) {
    lpStartX=e.touches[0].clientX; lpStartY=e.touches[0].clientY
    t0X=e.touches[0].clientX; t0Idx=startIdx.value
    lpTimer=setTimeout(()=>doTooltip(e.touches[0].clientX), 450)
  } else if(e.touches.length===2) {
    clearTimeout(lpTimer)
    const dx=e.touches[1].clientX-e.touches[0].clientX, dy=e.touches[1].clientY-e.touches[0].clientY
    t0Dist=Math.sqrt(dx*dx+dy*dy); t0Count=barCount.value; t0Idx=startIdx.value
  }
}
function doTooltip(cx:number) {
  const ohlc=visOhlc.value; if(!ohlc.length) return
  let idx=Math.round(cx/getW()*(ohlc.length-1)); idx=Math.max(0,Math.min(ohlc.length-1,idx))
  const d=ohlc[idx]
  const svgX=PAD_L+(idx+0.5)*((SVG_W-PAD_L-PAD_R)/ohlc.length)
  const leftPct=svgX/SVG_W*100
  const ivEntry=visIv.value.find((v:any)=>v.dt===d.dt)
  tooltip.value={visible:true,x:svgX,leftPct,dt:d.dt.slice(0,4)+'/'+d.dt.slice(4,6)+'/'+d.dt.slice(6,8),o:d.o,h:d.h,l:d.l,c:d.c,pct:d.pct??0,iv:ivEntry?ivEntry.v:null}
}
function onTouchMove(e:any) {
  e.preventDefault()
  const n=allOhlc.value.length; if(!n) return
  if(e.touches.length===1) {
    const dx=Math.abs(e.touches[0].clientX-lpStartX), dy=Math.abs(e.touches[0].clientY-lpStartY)
    if(dx>8||dy>8){clearTimeout(lpTimer);tooltip.value.visible=false}
    if(!tooltip.value.visible) {
      const shift=Math.round(-(e.touches[0].clientX-t0X)*barCount.value/getW())
      startIdx.value=Math.max(0,Math.min(n-barCount.value,t0Idx+shift)); activeRange.value=''
    }
  } else if(e.touches.length===2) {
    const dx=e.touches[1].clientX-e.touches[0].clientX, dy=e.touches[1].clientY-e.touches[0].clientY
    const dist=Math.sqrt(dx*dx+dy*dy)
    let nc=Math.round(t0Count*(t0Dist/(dist||1))); nc=Math.max(10,Math.min(n,nc))
    const center=t0Idx+Math.floor(t0Count/2)
    const ns=Math.max(0,Math.min(n-nc,center-Math.floor(nc/2)))
    barCount.value=nc; startIdx.value=ns; activeRange.value=''
  }
}
function onTouchEnd() {
  clearTimeout(lpTimer)
  if(tooltip.value.visible) setTimeout(()=>{tooltip.value.visible=false},2500)
}

// ── 格式化 ────────────────────────────────────────────────
function fmtNum(v:number):string {
  if(Math.abs(v)>=10000) return (v/10000).toFixed(1)+'w'
  if(Math.abs(v)>=1000)  return v.toFixed(0)
  if(Math.abs(v)>=100)   return v.toFixed(1)
  return v.toFixed(2)
}
function fmtCompact(v:number):string {
  const a=Math.abs(v), s=v<0?'-':''
  if(a>=100000) return s+(a/10000).toFixed(0)+'w'
  if(a>=10000)  return s+(a/10000).toFixed(1)+'w'
  if(a>=1000)   return s+(a/1000).toFixed(1)+'k'
  return v.toFixed(0)
}

function ivRankColor(r:number):string {
  if(r<0) return '#555'; if(r>=80) return '#e84040'; if(r>=60) return '#f97316'
  if(r>=40) return '#f5c518'; if(r>=20) return '#22c55e'; return '#3b82f6'
}
function pctColor(v:number|null):string { if(v==null) return '#888'; return v>0?'#e84040':v<0?'#22c55e':'#888' }
function dumbChgColor(v:number|null):string { if(v==null) return '#888'; return v<0?'#22c55e':v>0?'#e84040':'#888' }
const tooltipColor = computed(()=>tooltip.value.pct>0?'#e84040':tooltip.value.pct<0?'#22c55e':'#888')
</script>

<template>
  <view class="page">
    <view v-if="loading"  class="center"><text class="muted-text">加载中...</text></view>
    <view v-else-if="error" class="center"><text class="err-text">{{ error }}</text></view>

    <view v-else class="content">
      <!-- 合约头部 -->
      <view class="header-card">
        <view class="header-top">
          <view>
            <text class="c-name">{{ chartData?.cn_name || initName.split('(')[0].trim() }}</text>
            <text class="c-code">{{ chartData?.main_contract || initName }}</text>
          </view>
          <view class="rank-circle" :style="{borderColor:ivRankColor(initIvRank),color:ivRankColor(initIvRank)}">
            <text class="rank-num">{{ initIvRank>=0 ? Math.round(initIvRank) : '-' }}</text>
            <text class="rank-lbl">Rank</text>
          </view>
        </view>
        <view class="kpi-grid">
          <view class="kpi-item">
            <text class="kpi-lbl">{{ liveCandle ? '实时价格' : '当前价格' }}</text>
            <text class="kpi-val">{{ (liveCandle?.c ?? chartData?.cur_price) != null ? (liveCandle?.c ?? chartData.cur_price).toLocaleString() : '-' }}</text>
            <text class="kpi-delta" :style="{color:pctColor(liveCandle?.pct ?? chartData?.cur_pct)}">
              {{ (liveCandle?.pct ?? chartData?.cur_pct) != null ? ((liveCandle?.pct ?? chartData.cur_pct)>0?'+':'') + (liveCandle?.pct ?? chartData.cur_pct) + '%' : '' }}
            </text>
          </view>
          <view class="kpi-item">
            <text class="kpi-lbl">当前 IV%</text>
            <text class="kpi-val" :style="{color:ivRankColor(initIvRank)}">
              {{ chartData?.cur_iv!=null ? chartData.cur_iv.toFixed(1)+'%' : (initIv>0?initIv.toFixed(1)+'%':'-') }}
            </text>
          </view>
          <view class="kpi-item">
            <text class="kpi-lbl">IV Rank</text>
            <text class="kpi-val" :style="{color:ivRankColor(initIvRank)}">
              {{ initIvRank>=0 ? Math.round(initIvRank) : '到期' }}
            </text>
          </view>
          <view class="kpi-item">
            <text class="kpi-lbl">反指当日变化</text>
            <text class="kpi-val" :style="{color:dumbChgColor(chartData?.dumb_chg_1d)}">
              {{ chartData?.dumb_chg_1d!=null ? (chartData.dumb_chg_1d>0?'+':'')+chartData.dumb_chg_1d.toLocaleString() : '-' }}
            </text>
          </view>
        </view>
      </view>

      <!-- 图表卡片 -->
      <view v-if="allOhlc.length>0" class="chart-card">
        <view class="chart-top">
          <text class="chart-title">K线 · IV · 持仓</text>
          <text class="chart-sub">{{ visOhlc.length }}/{{ allOhlc.length }}天</text>
        </view>
        <view class="range-bar">
          <view v-for="r in ['1M','3M','6M','1Y','全部']" :key="r"
            class="range-btn" :class="{active:activeRange===r}" @tap="applyRange(r)">
            <text class="range-text">{{ r }}</text>
          </view>
        </view>
        <view class="gesture-hint"><text class="hint-text">拖动平移 | 双指缩放 | 长按查看OHLC</text></view>

        <!-- #ifdef H5 -->
        <!-- ① K线子图 -->
        <view class="sub-panel">
          <view class="sub-header">
            <text class="sub-title" style="color:#f5c518">K线</text>
            </view>
          <view class="svg-wrap" @touchstart="onTouchStart" @touchmove.prevent="onTouchMove" @touchend="onTouchEnd">
              <svg class="chart-svg" :width="SVG_W" :height="CANDLE_H"
                :viewBox="`0 0 ${SVG_W} ${CANDLE_H}`" preserveAspectRatio="none">
                <template v-if="candleChart">
                  <line :x1="PAD_L" :y1="candleChart.maxY" :x2="SVG_W-PAD_R" :y2="candleChart.maxY" stroke="#252525" stroke-width="1"/>
                  <line :x1="PAD_L" :y1="candleChart.midY" :x2="SVG_W-PAD_R" :y2="candleChart.midY" stroke="#252525" stroke-width="1" stroke-dasharray="4,4"/>
                  <line :x1="PAD_L" :y1="candleChart.minY" :x2="SVG_W-PAD_R" :y2="candleChart.minY" stroke="#252525" stroke-width="1"/>
                  <template v-for="(c,i) in candleChart.candles" :key="i">
                    <line :x1="c.x" :y1="c.hy" :x2="c.x" :y2="c.ly"
                      :stroke="c.up?'#e84040':'#22c55e'" stroke-width="1.2"
                      :opacity="c.live?0.7:1" :stroke-dasharray="c.live?'3,2':undefined"/>
                    <rect :x="c.x-c.barW/2" :y="c.bodyTop" :width="c.barW" :height="c.bodyH"
                      :fill="c.up?'#e84040':'#22c55e'"
                      :opacity="c.live?0.55:1" :stroke="c.live?(c.up?'#e84040':'#22c55e'):undefined"
                      :stroke-dasharray="c.live?'3,2':undefined" :fill-opacity="c.live?0.55:1"/>
                  </template>
                  <line v-if="tooltip.visible" :x1="tooltip.x" :y1="PAD_TOP" :x2="tooltip.x" :y2="CANDLE_H"
                    stroke="#fff" stroke-width="0.8" stroke-dasharray="3,3" opacity="0.4"/>
                </template>
              </svg>
              <template v-if="candleChart">
                <text class="y-max">{{ candleChart.maxLabel }}</text>
                <text class="y-mid">{{ candleChart.midLabel }}</text>
                <text class="y-min">{{ candleChart.minLabel }}</text>
              </template>
              <!-- 长按浮框：在 svg-wrap 内，position:absolute 相对此容器定位 -->
              <view v-if="tooltip.visible" class="tooltip-box"
                :style="tooltip.leftPct > 55
                  ? 'right:8rpx;left:auto;top:4rpx;'
                  : 'left:8rpx;right:auto;top:4rpx;'">
                <text class="tt-date">{{ tooltip.dt }}</text>
                <view class="tt-row"><text class="tt-lbl">开</text><text class="tt-val">{{ tooltip.o.toLocaleString() }}</text></view>
                <view class="tt-row"><text class="tt-lbl">高</text><text class="tt-val" style="color:#e84040">{{ tooltip.h.toLocaleString() }}</text></view>
                <view class="tt-row"><text class="tt-lbl">低</text><text class="tt-val" style="color:#22c55e">{{ tooltip.l.toLocaleString() }}</text></view>
                <view class="tt-row"><text class="tt-lbl">收</text><text class="tt-val" :style="{color:tooltipColor}">{{ tooltip.c.toLocaleString() }}</text></view>
                <view class="tt-row"><text class="tt-lbl">涨跌</text><text class="tt-val" :style="{color:tooltipColor}">{{ tooltip.pct>0?'+':'' }}{{ tooltip.pct }}%</text></view>
                <view v-if="tooltip.iv!=null" class="tt-row"><text class="tt-lbl">IV</text><text class="tt-val" style="color:#3b82f6">{{ tooltip.iv.toFixed(1) }}%</text></view>
              </view>
          </view>
          <!-- X轴日期 -->
          <view v-if="candleChart" class="x-axis-row">
            <view class="x-axis-inner">
              <text v-for="tick in candleChart.xTicks" :key="tick.label"
                class="x-tick"
                :style="{left: ((tick.x-PAD_L)/(SVG_W-PAD_L-PAD_R)*100).toFixed(1)+'%'}">
                {{ tick.label }}
              </text>
            </view>
          </view>
        </view>

        <!-- ② IV 子图 -->
        <view class="sub-panel">
          <view class="sub-header">
            <text class="sub-title" style="color:#3b82f6">隐含波动率 IV%</text>
            <text v-if="ivLine" class="sub-cur" style="color:#3b82f6">当前 {{ ivLine.lastVal.toFixed(1) }}%</text>
            <text v-else class="no-data">暂无数据</text>
          </view>
          <view class="svg-wrap">
            <svg class="chart-svg" :width="SVG_W" :height="IV_H"
              :viewBox="`0 0 ${SVG_W} ${IV_H}`" preserveAspectRatio="none">
              <defs>
                <linearGradient id="ivgd" x1="0" y1="0" x2="0" y2="1">
                  <stop offset="0%" stop-color="#3b82f6" stop-opacity="0.3"/>
                  <stop offset="100%" stop-color="#3b82f6" stop-opacity="0"/>
                </linearGradient>
              </defs>
              <template v-if="ivLine">
                <polygon :points="rebasePoints(ivLine.fillPts, IV_Y0)" fill="url(#ivgd)"/>
                <polyline :points="rebasePoints(ivLine.pts, IV_Y0)" fill="none" stroke="#3b82f6" stroke-width="2" stroke-linejoin="round" stroke-linecap="round"/>
                <circle :cx="ivLine.lastX" :cy="(parseFloat(ivLine.lastY)-IV_Y0).toFixed(1)" r="3" fill="#3b82f6"/>
              </template>
            </svg>
            <template v-if="ivLine">
              <text class="y-max" style="color:#3b82f6">{{ ivLine.maxLabel }}</text>
              <text class="y-min" style="color:#3b82f6">{{ ivLine.minLabel }}</text>
            </template>
          </view>
        </view>

        <!-- ③ 总持仓量 子图（紧接IV之后）-->
        <view class="sub-panel">
          <view class="sub-header">
            <text class="sub-title" style="color:#f5c518">总持仓量</text>
            <text v-if="oiLine" class="sub-cur" style="color:#f5c518">{{ fmtCompact(oiLine.lastVal) }}</text>
            <text v-else class="no-data">暂无数据</text>
          </view>
          <view class="svg-wrap">
            <svg class="chart-svg" :width="SVG_W" :height="OI_H"
              :viewBox="`0 0 ${SVG_W} ${OI_H}`" preserveAspectRatio="none">
              <defs>
                <linearGradient id="oigd" x1="0" y1="0" x2="0" y2="1">
                  <stop offset="0%" stop-color="#f5c518" stop-opacity="0.2"/>
                  <stop offset="100%" stop-color="#f5c518" stop-opacity="0"/>
                </linearGradient>
              </defs>
              <template v-if="oiLine">
                <polygon :points="rebasePoints(oiLine.fillPts, OI_Y0)" fill="url(#oigd)"/>
                <polyline :points="rebasePoints(oiLine.pts, OI_Y0)" fill="none" stroke="#f5c518" stroke-width="2" stroke-linejoin="round" stroke-linecap="round"/>
                <circle :cx="oiLine.lastX" :cy="(parseFloat(oiLine.lastY)-OI_Y0).toFixed(1)" r="3" fill="#f5c518"/>
              </template>
            </svg>
            <template v-if="oiLine">
              <text class="y-max" style="color:#f5c518">{{ oiLine.maxLabel }}</text>
              <text class="y-min" style="color:#f5c518">{{ oiLine.minLabel }}</text>
            </template>
          </view>
        </view>

        <!-- ④ 反指标 子图 -->
        <view class="sub-panel">
          <view class="sub-header">
            <text class="sub-title" style="color:#e84040">反指标净持仓（手）</text>
            <text v-if="dumbLine" class="sub-cur" style="color:#e84040">{{ fmtCompact(dumbLine.lastVal) }}</text>
            <text v-else class="no-data">暂无数据</text>
          </view>
          <view class="svg-wrap">
            <svg class="chart-svg" :width="SVG_W" :height="DUMB_H"
              :viewBox="`0 0 ${SVG_W} ${DUMB_H}`" preserveAspectRatio="none">
              <defs>
                <linearGradient id="dumbgd" x1="0" y1="0" x2="0" y2="1">
                  <stop offset="0%" stop-color="#e84040" stop-opacity="0.25"/>
                  <stop offset="100%" stop-color="#e84040" stop-opacity="0"/>
                </linearGradient>
              </defs>
              <template v-if="dumbLine">
                <polygon :points="rebasePoints(dumbLine.fillPts, DUMB_Y0)" fill="url(#dumbgd)"/>
                <line v-if="dumbLine.zeroY" :x1="PAD_L" :y1="(dumbLine.zeroY-DUMB_Y0).toFixed(1)" :x2="SVG_W-PAD_R" :y2="(dumbLine.zeroY-DUMB_Y0).toFixed(1)"
                  stroke="#e84040" stroke-width="0.8" opacity="0.4" stroke-dasharray="4,4"/>
                <polyline :points="rebasePoints(dumbLine.pts, DUMB_Y0)" fill="none" stroke="#e84040" stroke-width="2" stroke-linejoin="round" stroke-linecap="round"/>
                <circle :cx="dumbLine.lastX" :cy="(parseFloat(dumbLine.lastY)-DUMB_Y0).toFixed(1)" r="3" fill="#e84040"/>
              </template>
            </svg>
            <template v-if="dumbLine">
              <text class="y-max" style="color:#e84040">{{ dumbLine.maxLabel }}</text>
              <text class="y-min" style="color:#e84040">{{ dumbLine.minLabel }}</text>
            </template>
          </view>
        </view>

        <!-- ⑤ 正指标 子图 -->
        <view class="sub-panel" style="margin-bottom:0">
          <view class="sub-header">
            <text class="sub-title" style="color:#22c55e">正指标净持仓（手）</text>
            <text v-if="smartLine" class="sub-cur" style="color:#22c55e">{{ fmtCompact(smartLine.lastVal) }}</text>
            <text v-else class="no-data">暂无数据</text>
          </view>
          <view class="svg-wrap">
            <svg class="chart-svg" :width="SVG_W" :height="SMART_H"
              :viewBox="`0 0 ${SVG_W} ${SMART_H}`" preserveAspectRatio="none">
              <defs>
                <linearGradient id="smartgd" x1="0" y1="0" x2="0" y2="1">
                  <stop offset="0%" stop-color="#22c55e" stop-opacity="0.25"/>
                  <stop offset="100%" stop-color="#22c55e" stop-opacity="0"/>
                </linearGradient>
              </defs>
              <template v-if="smartLine">
                <polygon :points="rebasePoints(smartLine.fillPts, SMART_Y0)" fill="url(#smartgd)"/>
                <line v-if="smartLine.zeroY" :x1="PAD_L" :y1="(smartLine.zeroY-SMART_Y0).toFixed(1)" :x2="SVG_W-PAD_R" :y2="(smartLine.zeroY-SMART_Y0).toFixed(1)"
                  stroke="#22c55e" stroke-width="0.8" opacity="0.4" stroke-dasharray="4,4"/>
                <polyline :points="rebasePoints(smartLine.pts, SMART_Y0)" fill="none" stroke="#22c55e" stroke-width="2" stroke-linejoin="round" stroke-linecap="round"/>
                <circle :cx="smartLine.lastX" :cy="(parseFloat(smartLine.lastY)-SMART_Y0).toFixed(1)" r="3" fill="#22c55e"/>
              </template>
            </svg>
            <template v-if="smartLine">
              <text class="y-max" style="color:#22c55e">{{ smartLine.maxLabel }}</text>
              <text class="y-min" style="color:#22c55e">{{ smartLine.minLabel }}</text>
            </template>
          </view>
        </view>
        <!-- #endif -->

        <view v-if="chartData?.cur_iv" class="iv-tag">
          <text class="iv-tag-text">IV {{ chartData.cur_iv.toFixed(1) }}%  |  Rank {{ initIvRank>=0?Math.round(initIvRank):'-' }}</text>
        </view>
      </view>

      <view v-if="allOhlc.length===0&&!loading" class="center-tip">
        <text class="muted-text">该品种暂无历史数据</text>
      </view>
    </view>
    <view style="height:60rpx;"/>
  </view>
</template>


<style scoped>
.page { background: #0b1121; min-height: 100vh; padding: 24rpx 24rpx 0; }
.center { display: flex; justify-content: center; padding-top: 200rpx; }
.err-text { color: #e84040; font-size: 28rpx; }

.header-card { background:#131c2e; border:1px solid #1e2d45; border-radius:20rpx; padding:28rpx; margin-bottom:20rpx; }
.header-top  { display:flex; justify-content:space-between; align-items:flex-start; margin-bottom:24rpx; }
.c-name { display:block; font-size:38rpx; font-weight:900; color:#f0f0f0; }
.c-code { display:block; font-size:22rpx; color:#666; margin-top:6rpx; }
.rank-circle { width:80rpx; height:80rpx; border-radius:50%; border:3px solid; display:flex; flex-direction:column; align-items:center; justify-content:center; flex-shrink:0; }
.rank-num { font-size:26rpx; font-weight:900; line-height:1; }
.rank-lbl { font-size:18rpx; margin-top:2rpx; }

.kpi-grid { display:grid; grid-template-columns:1fr 1fr 1fr 1fr; gap:12rpx; }
.kpi-item { background:#0d1829; border-radius:12rpx; padding:14rpx 10rpx; text-align:center; }
.kpi-lbl  { display:block; font-size:18rpx; color:#555; margin-bottom:6rpx; }
.kpi-val  { display:block; font-size:24rpx; font-weight:700; color:#f0f0f0; }
.kpi-delta{ display:block; font-size:18rpx; margin-top:4rpx; }

.chart-card { background:#131c2e; border:1px solid #1e2d45; border-radius:20rpx; padding:20rpx; margin-bottom:20rpx; }
.chart-top  { display:flex; justify-content:space-between; align-items:baseline; margin-bottom:12rpx; }
.chart-title{ font-size:26rpx; font-weight:700; color:#f5c518; }
.chart-sub  { font-size:18rpx; color:#555; }

.range-bar { display:flex; gap:10rpx; margin-bottom:10rpx; }
.range-btn { flex:1; background:#0d1829; border:1px solid #1e2d45; border-radius:8rpx; padding:8rpx 0; text-align:center; }
.range-btn.active { background:#f5c518; border-color:#f5c518; }
.range-text { font-size:22rpx; color:#888; }
.range-btn.active .range-text { color:#000; font-weight:700; }

.gesture-hint { text-align:center; margin-bottom:10rpx; }
.hint-text { font-size:18rpx; color:#444; }

/* 子图面板 */
.sub-panel { margin-bottom:16rpx; }

.sub-header {
  display:flex; justify-content:space-between; align-items:center;
  padding:6rpx 0 8rpx; border-bottom:1px solid #222;
  margin-bottom:6rpx;
}
.sub-title { font-size:22rpx; font-weight:700; }
.sub-cur   { font-size:20rpx; font-weight:600; }
.no-data   { font-size:18rpx; color:#444; }

/* SVG 容器：相对定位，供 Y 轴标签绝对定位 */
.svg-wrap { position:relative; width:100%; }
.chart-svg { display:block; width:100%; height:auto; }

/* Y轴标签：绝对定位在 SVG 右上/右下角 */
.y-max {
  position:absolute; right:4rpx; top:2rpx;
  font-size:18rpx; color:#aaa; line-height:1;
  background:rgba(13,13,13,0.7); padding:0 4rpx; border-radius:4rpx;
}
.y-mid {
  position:absolute; right:4rpx; top:50%; transform:translateY(-50%);
  font-size:16rpx; color:#666; line-height:1;
}
.y-min {
  position:absolute; right:4rpx; bottom:2rpx;
  font-size:18rpx; color:#aaa; line-height:1;
  background:rgba(13,13,13,0.7); padding:0 4rpx; border-radius:4rpx;
}

/* X轴日期行 */
.x-axis-row { position:relative; height:32rpx; margin-top:2rpx; }
.x-axis-inner { position:absolute; left:0; right:0; top:0; height:32rpx; }
.x-tick {
  position:absolute; transform:translateX(-50%);
  font-size:20rpx; color:#aaaaaa; white-space:nowrap;
}

/* 长按浮框 */
.tooltip-box {
  position:absolute; top:4rpx; left:8rpx; z-index:20;
  background:rgba(20,20,20,0.95); border:1px solid #333;
  border-radius:10rpx; padding:10rpx 16rpx; min-width:150rpx;
}
.tt-date { display:block; font-size:18rpx; color:#999; margin-bottom:6rpx; }
.tt-row  { display:flex; justify-content:space-between; gap:20rpx; margin-top:4rpx; }
.tt-lbl  { font-size:18rpx; color:#666; }
.tt-val  { font-size:18rpx; color:#f0f0f0; font-weight:600; }

.iv-tag { margin-top:14rpx; background:rgba(59,130,246,0.1); border-radius:10rpx; padding:8rpx 16rpx; text-align:center; }
.iv-tag-text { font-size:20rpx; color:#3b82f6; font-weight:600; }

.center-tip { text-align:center; padding:60rpx 0; }
.muted-text { font-size:24rpx; color:#555; }
</style>
