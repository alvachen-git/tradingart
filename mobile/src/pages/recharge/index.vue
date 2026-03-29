<script setup lang="ts">
import { ref, onMounted } from 'vue'
import { onShow } from '@dcloudio/uni-app'
import {
  payApi,
  userApi,
  type WalletInfo,
  type TopupPackage,
  type PaidProduct,
  type PayConfig,
  type UserProfile,
} from '../../api/index'
import { useAuthStore } from '../../store/auth'

const auth = useAuthStore()
const loading = ref(false)
const wallet = ref<WalletInfo | null>(null)
const packages = ref<TopupPackage[]>([])
const products = ref<PaidProduct[]>([])
const config = ref<PayConfig | null>(null)
const profile = ref<UserProfile | null>(null)
const monthMap = ref<Record<string, number>>({})
const purchasingKey = ref('')

onShow(() => {
  if (!auth.isLoggedIn) {
    uni.reLaunch({ url: '/pages/login/index' })
    return
  }
  loadAll()
})
onMounted(loadAll)

function productKey(p: PaidProduct) {
  return `${p.product_type}:${p.code}`
}

function defaultMonths(p: PaidProduct) {
  return Number((p.months_options && p.months_options[0]) || 1)
}

function selectedMonths(p: PaidProduct) {
  return Number(monthMap.value[productKey(p)] || defaultMonths(p))
}

function setMonths(p: PaidProduct, m: number) {
  monthMap.value[productKey(p)] = Number(m)
}

function formatDate(raw: string) {
  const text = String(raw || '').trim()
  if (!text) return ''
  return text.length >= 10 ? text.slice(0, 10) : text
}

function subscriptionStatus(code: string) {
  const sub = profile.value?.subscriptions?.find(
    s => s.channel_code === code && s.is_active
  )
  if (!sub) return '未开通'
  const exp = formatDate(sub.expires_at)
  return exp ? `有效期至 ${exp}` : '有效期至 -'
}

async function loadAll() {
  loading.value = true
  try {
    const [w, pkgRes, prodRes, cfg, userProfile] = await Promise.all([
      payApi.wallet(),
      payApi.packages(),
      payApi.products(),
      payApi.config(),
      userApi.profile(),
    ])
    wallet.value = w
    packages.value = pkgRes.items || []
    products.value = prodRes.items || []
    config.value = cfg
    profile.value = userProfile

    for (const p of products.value) {
      const key = productKey(p)
      if (!monthMap.value[key]) monthMap.value[key] = defaultMonths(p)
    }
  } catch (e: any) {
    uni.showToast({ title: e.message || '加载失败', icon: 'none' })
  } finally {
    loading.value = false
  }
}

function pointsCost(p: PaidProduct) {
  return Math.max(0, Number(p.points_monthly || 0) * selectedMonths(p))
}

async function buyProduct(p: PaidProduct) {
  const months = selectedMonths(p)
  const totalCost = pointsCost(p)
  if (months <= 0) {
    uni.showToast({ title: '购买月数无效', icon: 'none' })
    return
  }

  const key = productKey(p)
  const balance = Number(wallet.value?.balance || 0)
  if (balance < totalCost) {
    uni.showModal({
      title: '余额不足',
      content: `当前余额 ${balance} 点，购买需要 ${totalCost} 点，请先充值。`,
      showCancel: false,
    })
    return
  }

  uni.showModal({
    title: '确认购买',
    content: `${p.name} ${months} 个月，共 ${totalCost} 点`,
    success: async (res) => {
      if (!res.confirm) return
      purchasingKey.value = key
      uni.showLoading({ title: '开通中...' })
      try {
        const ret = await payApi.purchase({
          product_type: p.product_type,
          code: p.code,
          months,
        })
        uni.hideLoading()
        uni.showToast({ title: ret.message || '开通成功', icon: 'success' })
        await loadAll()
      } catch (e: any) {
        uni.hideLoading()
        uni.showToast({ title: e.message || '开通失败', icon: 'none' })
      } finally {
        purchasingKey.value = ''
      }
    },
  })
}

function openRecharge() {
  const url = (config.value?.recharge_url || 'https://www.aiprota.com').trim()
  if (!url) {
    uni.showToast({ title: '充值链接未配置', icon: 'none' })
    return
  }

  // #ifdef H5
  try {
    ;(globalThis as any).location.href = url
    return
  } catch {
    // ignore and fallback to copy
  }
  // #endif

  uni.setClipboardData({
    data: url,
    success: () => {
      uni.showModal({
        title: '已复制充值链接',
        content: `请在浏览器打开链接完成充值：${url}`,
        showCancel: false,
      })
    },
    fail: () => {
      uni.showModal({
        title: '无法直接打开',
        content: `请联系客服充值：微信 ${config.value?.service_wechat || 'trader-sec'} ｜ 电话 ${config.value?.service_phone || '17521591756'}`,
        showCancel: false,
      })
    },
  })
}
</script>

<template>
  <view class="page">
    <view class="card wallet-card">
      <text class="section-title">点数钱包</text>
      <view class="wallet-balance-row">
        <text class="wallet-balance">{{ wallet?.balance ?? 0 }}</text>
        <text class="wallet-unit">点</text>
      </view>
      <view class="wallet-metrics">
        <view class="metric-item">
          <text class="metric-k">累计充值</text>
          <text class="metric-v">{{ wallet?.total_earned ?? 0 }}</text>
        </view>
        <view class="metric-item">
          <text class="metric-k">累计消费</text>
          <text class="metric-v">{{ wallet?.total_spent ?? 0 }}</text>
        </view>
      </view>
      <text class="wallet-updated" v-if="wallet?.updated_at">最近更新：{{ wallet.updated_at }}</text>
    </view>

    <view class="card section-card">
      <view class="section-head">
        <text class="section-title">充值商城</text>
        <text class="section-sub">外部网页充值，到账后可在本页开通权限</text>
      </view>
      <view v-if="!wallet?.payment_enabled" class="warn-text">
        支付功能当前未开启，请联系客服人工开通。
      </view>
      <view v-for="pkg in packages" :key="pkg.name" class="pkg-row">
        <view class="pkg-left">
          <text class="pkg-name">{{ pkg.name }}</text>
          <text class="pkg-desc">
            ￥{{ pkg.rmb }} · {{ pkg.points }} 点
            <text v-if="pkg.bonus_points > 0">（赠送 {{ pkg.bonus_points }} 点）</text>
          </text>
        </view>
        <view class="action-btn" @tap="openRecharge">
          <text class="action-text">去充值</text>
        </view>
      </view>
      <text class="risk-text">点数为虚拟权益，不可提现。</text>
    </view>

    <view class="card section-card">
      <view class="section-head">
        <text class="section-title">付费产品</text>
        <text class="section-sub">使用点数直接开通频道或套餐权限</text>
      </view>
      <view v-if="!products.length" class="empty-text">暂无可购买产品</view>
      <view v-for="p in products" :key="productKey(p)" class="product-row">
        <view class="product-top">
          <view class="product-main">
            <text class="product-name">{{ p.icon ? `${p.icon} ` : '' }}{{ p.name }}</text>
            <text class="product-meta">{{ p.points_monthly }} 点 / 月</text>
            <text
              v-if="p.product_type === 'channel'"
              class="product-status"
            >{{ subscriptionStatus(p.code) }}</text>
            <text
              v-else-if="p.includes_names && p.includes_names.length"
              class="product-status"
            >包含：{{ p.includes_names.join('、') }}</text>
          </view>
          <view class="buy-side">
            <text class="buy-total">{{ pointsCost(p) }} 点</text>
            <view
              class="buy-btn"
              :class="{ disabled: purchasingKey === productKey(p) }"
              @tap="buyProduct(p)"
            >
              <text class="buy-btn-text">{{ purchasingKey === productKey(p) ? '开通中' : '用点数开通' }}</text>
            </view>
          </view>
        </view>
        <view class="month-row">
          <view
            v-for="m in (p.months_options || [1, 3, 6, 12])"
            :key="`${productKey(p)}:${m}`"
            class="month-chip"
            :class="{ active: selectedMonths(p) === m }"
            @tap="setMonths(p, m)"
          >
            <text class="month-text">{{ m }}月</text>
          </view>
        </view>
      </view>
      <text class="contact-text">
        充值或开通问题请咨询客服：微信 {{ config?.service_wechat || 'trader-sec' }} ｜ 电话 {{ config?.service_phone || '17521591756' }}
      </text>
    </view>
  </view>
</template>

<style scoped>
.page {
  min-height: 100vh;
  background: #0b1121;
  padding: 20rpx 24rpx 40rpx;
}

.card {
  background: #131c2e;
  border: 1px solid #1e2d45;
  border-radius: 20rpx;
  padding: 24rpx;
  margin-bottom: 20rpx;
}

.section-title {
  font-size: 30rpx;
  font-weight: 700;
  color: #f5c518;
}

.section-sub {
  display: block;
  margin-top: 8rpx;
  font-size: 22rpx;
  color: #7f8aa8;
}

.wallet-card {
  background: linear-gradient(180deg, #13213a 0%, #101a2c 100%);
}

.wallet-balance-row {
  margin-top: 14rpx;
  display: flex;
  align-items: baseline;
  gap: 10rpx;
}

.wallet-balance {
  font-size: 56rpx;
  color: #f5c518;
  font-weight: 800;
  line-height: 1;
}

.wallet-unit {
  font-size: 26rpx;
  color: #d2d8ea;
}

.wallet-metrics {
  margin-top: 20rpx;
  display: flex;
  gap: 16rpx;
}

.metric-item {
  flex: 1;
  border: 1px solid #263657;
  border-radius: 14rpx;
  background: #0f182a;
  padding: 14rpx;
}

.metric-k {
  display: block;
  font-size: 22rpx;
  color: #7f8aa8;
}

.metric-v {
  display: block;
  margin-top: 6rpx;
  font-size: 28rpx;
  color: #d9e1f5;
  font-weight: 700;
}

.wallet-updated {
  display: block;
  margin-top: 14rpx;
  font-size: 22rpx;
  color: #6f7a96;
}

.section-head {
  margin-bottom: 12rpx;
}

.warn-text {
  font-size: 24rpx;
  color: #f97316;
  margin-bottom: 12rpx;
}

.pkg-row {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 20rpx;
  padding: 18rpx 0;
  border-bottom: 1px solid #1a2540;
}

.pkg-row:last-of-type { border-bottom: none; }

.pkg-left { flex: 1; min-width: 0; }
.pkg-name { display: block; color: #f0f0f0; font-size: 28rpx; font-weight: 600; }
.pkg-desc { display: block; margin-top: 6rpx; color: #8f99b2; font-size: 22rpx; }

.action-btn {
  flex-shrink: 0;
  border: 1px solid rgba(245, 197, 24, 0.45);
  background: rgba(245, 197, 24, 0.1);
  border-radius: 18rpx;
  padding: 8rpx 18rpx;
}

.action-text { color: #f5c518; font-size: 24rpx; font-weight: 600; }

.risk-text {
  display: block;
  margin-top: 12rpx;
  color: #6f7a96;
  font-size: 22rpx;
}

.empty-text {
  color: #8f99b2;
  font-size: 24rpx;
  padding: 10rpx 0;
}

.product-row {
  border-top: 1px solid #1a2540;
  padding: 18rpx 0;
}

.product-top {
  display: flex;
  align-items: flex-start;
  justify-content: space-between;
  gap: 14rpx;
}

.product-main { flex: 1; min-width: 0; }
.product-name { display: block; color: #f0f0f0; font-size: 28rpx; font-weight: 600; }
.product-meta { display: block; margin-top: 6rpx; color: #f5c518; font-size: 24rpx; }
.product-status { display: block; margin-top: 8rpx; color: #8f99b2; font-size: 22rpx; line-height: 1.4; }

.buy-side {
  flex-shrink: 0;
  display: flex;
  flex-direction: column;
  align-items: flex-end;
  gap: 10rpx;
}

.buy-total {
  color: #f5c518;
  font-size: 24rpx;
  font-weight: 700;
}

.buy-btn {
  background: rgba(34, 197, 94, 0.16);
  border: 1px solid rgba(34, 197, 94, 0.35);
  border-radius: 14rpx;
  padding: 8rpx 16rpx;
}

.buy-btn.disabled {
  opacity: 0.6;
}

.buy-btn-text {
  color: #22c55e;
  font-size: 22rpx;
  font-weight: 600;
}

.month-row {
  margin-top: 14rpx;
  display: flex;
  gap: 12rpx;
  flex-wrap: wrap;
}

.month-chip {
  border: 1px solid #2a3a5d;
  background: #101a2c;
  border-radius: 999rpx;
  padding: 8rpx 20rpx;
}

.month-chip.active {
  border-color: rgba(245, 197, 24, 0.45);
  background: rgba(245, 197, 24, 0.15);
}

.month-text {
  color: #9ba6c2;
  font-size: 22rpx;
}

.month-chip.active .month-text {
  color: #f5c518;
  font-weight: 600;
}

.contact-text {
  display: block;
  margin-top: 16rpx;
  font-size: 22rpx;
  color: #7f8aa8;
  line-height: 1.5;
}
</style>
