<script setup lang="ts">
const props = defineProps<{ active: string }>()

const tabs = [
  { key: 'index',   label: 'AI问答',  icon: '◈', path: '/pages/index/index' },
  { key: 'intel',   label: '情报站',  icon: '◉', path: '/pages/intel/index' },
  { key: 'market',  label: '行情',    icon: '◆', path: '/pages/market/index' },
  { key: 'kline',   label: 'K线训练',  icon: '◑', path: '/pages/kline/index' },
  { key: 'profile', label: '我的',    icon: '◍', path: '/pages/profile/index' },
]

function navigate(path: string) {
  uni.reLaunch({ url: path })
}
</script>

<template>
  <view class="bottom-nav">
    <view
      v-for="tab in tabs"
      :key="tab.key"
      class="tab-item"
      :class="{ active: active === tab.key }"
      @tap="navigate(tab.path)"
    >
      <text class="tab-icon">{{ tab.icon }}</text>
      <text class="tab-label">{{ tab.label }}</text>
    </view>
  </view>
</template>

<style scoped>
.bottom-nav {
  position: fixed;
  bottom: 0;
  left: 0;
  right: 0;
  height: 100rpx;
  background: #0d1829;
  border-top: 1px solid #1e2d45;
  display: flex;
  align-items: center;
  padding-bottom: env(safe-area-inset-bottom);
  z-index: 999;
}

.tab-item {
  flex: 1;
  display: flex;
  flex-direction: column;
  align-items: center;
  justify-content: center;
  padding: 8rpx 0;
  opacity: 0.45;
  transition: opacity 0.15s;
}

.tab-item.active {
  opacity: 1;
}

.tab-icon {
  font-size: 36rpx;
  color: #aaaaaa;
  line-height: 1;
  margin-bottom: 4rpx;
}

.tab-item.active .tab-icon {
  color: #f5c518;
}

.tab-label {
  font-size: 20rpx;
  color: #aaaaaa;
}

.tab-item.active .tab-label {
  color: #f5c518;
  font-weight: 600;
}
</style>
