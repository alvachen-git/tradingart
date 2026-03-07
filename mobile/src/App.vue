<script setup lang="ts">
import { onLaunch } from '@dcloudio/uni-app'
import { useAuthStore } from './store/auth'

onLaunch(() => {
  const auth = useAuthStore()
  auth.restoreFromStorage()
})
</script>

<style>
/* ── 全局重置 ── */
page {
  background-color: #0d0d0d;
  color: #f0f0f0;
  font-family: -apple-system, 'PingFang SC', 'Helvetica Neue', 'Microsoft YaHei', sans-serif;
  font-size: 28rpx;
}

/* ── 设计 Token ── */
:root {
  --bg-base:       #0d0d0d;
  --bg-card:       #1a1a1a;
  --bg-input:      #222222;
  --gold:          #f5c518;
  --gold-dim:      #c9a227;
  --gold-bg:       rgba(245, 197, 24, 0.12);
  --text-primary:  #f0f0f0;
  --text-secondary:#aaaaaa;
  --text-muted:    #666666;
  --border:        #2a2a2a;
  --red:           #e84040;
  --green:         #22c55e;
  --radius:        16rpx;
  --radius-sm:     10rpx;
}

/* ── 通用组件 ── */
.card {
  background: var(--bg-card);
  border: 1px solid var(--border);
  border-radius: var(--radius);
  padding: 28rpx;
  margin-bottom: 20rpx;
}

.gold-text { color: var(--gold); }
.muted-text { color: var(--text-secondary); font-size: 24rpx; }
.red-text { color: var(--red); }
.green-text { color: var(--green); }

.btn-primary {
  background: var(--gold);
  color: #0d0d0d;
  font-weight: 700;
  font-size: 30rpx;
  border-radius: var(--radius);
  padding: 24rpx 0;
  text-align: center;
  display: block;
  width: 100%;
  border: none;
}

.btn-ghost {
  background: transparent;
  color: var(--gold);
  font-weight: 600;
  font-size: 28rpx;
  border: 1px solid var(--gold);
  border-radius: var(--radius);
  padding: 20rpx 0;
  text-align: center;
  display: block;
  width: 100%;
}

.divider {
  height: 1px;
  background: var(--border);
  margin: 24rpx 0;
}

.tag {
  display: inline-block;
  background: var(--gold-bg);
  color: var(--gold);
  font-size: 22rpx;
  padding: 4rpx 14rpx;
  border-radius: 20rpx;
  border: 1px solid rgba(245, 197, 24, 0.3);
}
</style>
