<script setup lang="ts">
import { ref, nextTick, computed } from 'vue'
import { onShow } from '@dcloudio/uni-app'
import { chatApi, type ChatMessage } from '../../api/index'
import { useAuthStore } from '../../store/auth'
import BottomNav from '../../components/BottomNav.vue'

const auth = useAuthStore()

interface UIMessage {
  id: number
  role: 'user' | 'assistant' | 'loading'
  content: string
}

const messages = ref<UIMessage[]>([])
const input = ref('')
const sending = ref(false)
let msgCounter = 0
let pollTimer: ReturnType<typeof setInterval> | null = null

const HISTORY_KEY = computed(() => `chat_history_${auth.username}`)
const PENDING_KEY = computed(() => `chat_pending_${auth.username}`)

// ── 初始化：onShow 统一处理，避免 onMounted/onShow 时序竞争 ──
onShow(async () => {
  if (!auth.isLoggedIn) { uni.reLaunch({ url: '/pages/login/index' }); return }
  loadHistory()
  await nextTick()
  resumePendingTask()
})

function loadHistory() {
  try {
    const saved = uni.getStorageSync(HISTORY_KEY.value)
    if (saved) {
      const loaded: UIMessage[] = JSON.parse(saved)
      messages.value = loaded
      // 从已加载消息中初始化 msgCounter，避免新消息 ID 与历史 ID 重复导致 v-for 渲染错乱
      msgCounter = loaded.reduce((max, m) => Math.max(max, m.id || 0), 0)
    }
  } catch { /* 忽略 */ }
  if (messages.value.length === 0) {
    messages.value = [{
      id: ++msgCounter,
      role: 'assistant',
      content: '你好！我是爱波塔 AI，专注 A 股期权期货分析。\n\n你可以问我：\n• 沪铜近期多空格局\n• 300ETF 隐含波动率分析\n• 当前适合做哪个期权策略',
    }]
  }
}

function saveHistory() {
  const toSave = messages.value.filter(m => m.role !== 'loading').slice(-40)
  uni.setStorageSync(HISTORY_KEY.value, JSON.stringify(toSave))
}

// ── 发送消息 ──────────────────────────────────────
async function send() {
  const text = input.value.trim()
  if (!text || sending.value) return

  input.value = ''
  sending.value = true

  messages.value.push({ id: ++msgCounter, role: 'user', content: text })
  saveHistory() // 立即保存用户消息，切页后不会丢失
  const loadingId = ++msgCounter
  messages.value.push({ id: loadingId, role: 'loading', content: '' })
  scrollToBottom()

  // 构建历史（最近 10 轮）
  const history: ChatMessage[] = messages.value
    .filter(m => m.role !== 'loading')
    .slice(-20)
    .map(m => ({ role: m.role as 'user' | 'assistant', content: m.content }))

  try {
    const { task_id } = await chatApi.submit(text, history)
    // 存储待处理任务，切换页面后可恢复
    uni.setStorageSync(PENDING_KEY.value, JSON.stringify({ task_id }))
    pollStatus(task_id, loadingId)
  } catch (e: any) {
    replaceLoading(loadingId, `请求失败：${e.message}`)
    sending.value = false
  }
}

// 回到页面时恢复未完成的轮询
function resumePendingTask() {
  if (sending.value || pollTimer) return
  try {
    const raw = uni.getStorageSync(PENDING_KEY.value)
    if (!raw) return
    const { task_id } = JSON.parse(raw)
    // 重新加一个 loading 气泡（切页后原气泡已丢失）
    const newLoadingId = ++msgCounter
    messages.value.push({ id: newLoadingId, role: 'loading', content: '' })
    sending.value = true
    scrollToBottom()
    pollStatus(task_id, newLoadingId)
  } catch {
    uni.removeStorageSync(PENDING_KEY.value)
  }
}

function pollStatus(taskId: string, loadingId: number) {
  stopPoll()
  pollTimer = setInterval(async () => {
    try {
      const res = await chatApi.status(taskId)
      if (res.status === 'success') {
        stopPoll()
        uni.removeStorageSync(PENDING_KEY.value)
        const aiText = res.result?.response || res.result?.answer || JSON.stringify(res.result)
        replaceLoading(loadingId, aiText)
        saveHistory()
        sending.value = false
      } else if (res.status === 'error') {
        stopPoll()
        uni.removeStorageSync(PENDING_KEY.value)
        replaceLoading(loadingId, `分析失败：${res.error || '请稍后重试'}`)
        sending.value = false
      }
    } catch {
      stopPoll()
      uni.removeStorageSync(PENDING_KEY.value)
      replaceLoading(loadingId, '网络异常，请稍后重试')
      sending.value = false
    }
  }, 1000)
}

function stopPoll() {
  if (pollTimer) { clearInterval(pollTimer); pollTimer = null }
}

function replaceLoading(loadingId: number, content: string) {
  const idx = messages.value.findIndex(m => m.id === loadingId)
  if (idx !== -1) {
    messages.value[idx] = { id: loadingId, role: 'assistant', content }
  }
  scrollToBottom()
}

function scrollToBottom() {
  nextTick(() => {
    uni.pageScrollTo({ scrollTop: 999999, duration: 200 })
  })
}

function clearChat() {
  uni.showModal({
    title: '清空对话',
    content: '确认清空当前对话记录？',
    success(res) {
      if (res.confirm) {
        messages.value = []
        uni.removeStorageSync(HISTORY_KEY.value)
        loadHistory()
      }
    },
  })
}

</script>

<template>
  <view class="page">
    <!-- 顶部操作栏 -->
    <view class="top-bar">
      <text class="top-title">AI 问答</text>
      <text class="clear-btn" @tap="clearChat">清空</text>
    </view>

    <!-- 消息列表 -->
    <view class="msg-list">
      <view
        v-for="msg in messages"
        :key="msg.id"
        class="msg-row"
        :class="msg.role"
      >
        <!-- 加载动画 -->
        <view v-if="msg.role === 'loading'" class="bubble loading-bubble">
          <view class="dot-wave">
            <view class="dot" />
            <view class="dot" />
            <view class="dot" />
          </view>
        </view>

        <!-- AI 消息 -->
        <view v-else-if="msg.role === 'assistant'" class="bubble ai-bubble">
          <text class="msg-text selectable">{{ msg.content }}</text>
        </view>

        <!-- 用户消息 -->
        <view v-else class="bubble user-bubble">
          <text class="msg-text selectable">{{ msg.content }}</text>
        </view>
      </view>

      <!-- 底部占位（留出 BottomNav 和输入框的高度） -->
      <view style="height: 280rpx;" />
    </view>

    <!-- 输入栏 -->
    <view class="input-bar">
      <textarea
        v-model="input"
        class="input-area"
        placeholder="问我任何期权期货问题..."
        placeholder-class="input-placeholder"
        :disabled="sending"
        :auto-height="true"
        :max-height="160"
        @confirm.prevent
      />
      <view
        class="send-btn"
        :class="{ active: input.trim() && !sending, disabled: sending }"
        @tap="send"
      >
        <text class="send-icon">{{ sending ? '…' : '↑' }}</text>
      </view>
    </view>

    <BottomNav active="index" />
  </view>
</template>

<style scoped>
.page {
  background: #0b1121;
  min-height: 100vh;
  padding-bottom: 0;
}

.top-bar {
  position: sticky;
  top: 0;
  z-index: 10;
  background: #0b1121;
  border-bottom: 1px solid #162035;
  display: flex;
  align-items: center;
  justify-content: space-between;
  padding: 20rpx 32rpx;
}

.top-title {
  font-size: 32rpx;
  font-weight: 700;
  color: #f5c518;
}

.clear-btn {
  font-size: 26rpx;
  color: #666666;
}

.msg-list {
  padding: 20rpx 24rpx 0;
}

.msg-row {
  display: flex;
  margin-bottom: 24rpx;
}

.msg-row.user { justify-content: flex-end; }
.msg-row.assistant, .msg-row.loading { justify-content: flex-start; }

.bubble {
  max-width: 80%;
  border-radius: 24rpx;
  padding: 20rpx 24rpx;
  word-break: break-all;
}

.ai-bubble {
  background: #162035;
  border: 1px solid #1e2d45;
  border-bottom-left-radius: 6rpx;
}

.user-bubble {
  background: #f5c518;
  border-bottom-right-radius: 6rpx;
}

.user-bubble .msg-text { color: #0b1121; font-weight: 600; }
.ai-bubble .msg-text { color: #f0f0f0; line-height: 1.7; white-space: pre-wrap; }

.loading-bubble {
  background: #162035;
  border: 1px solid #1e2d45;
  border-bottom-left-radius: 6rpx;
  padding: 24rpx 28rpx;
}

.dot-wave { display: flex; gap: 10rpx; align-items: center; }

.dot {
  width: 12rpx;
  height: 12rpx;
  border-radius: 50%;
  background: #f5c518;
  animation: bounce 1.2s ease-in-out infinite;
}
.dot:nth-child(2) { animation-delay: 0.2s; }
.dot:nth-child(3) { animation-delay: 0.4s; }

@keyframes bounce {
  0%, 80%, 100% { transform: translateY(0); opacity: 0.4; }
  40% { transform: translateY(-10rpx); opacity: 1; }
}

/* 输入栏 */
.input-bar {
  position: fixed;
  bottom: 100rpx;
  left: 0;
  right: 0;
  background: #0d1829;
  border-top: 1px solid #1e2d45;
  display: flex;
  align-items: flex-end;
  padding: 16rpx 20rpx;
  padding-bottom: calc(16rpx + env(safe-area-inset-bottom));
  gap: 16rpx;
  z-index: 100;
}

.input-area {
  flex: 1;
  background: #162035;
  border: 1px solid #1e2d45;
  border-radius: 20rpx;
  padding: 18rpx 24rpx;
  font-size: 28rpx;
  color: #f0f0f0;
  min-height: 72rpx;
  max-height: 160rpx;
}

.input-placeholder { color: #444444; }

.send-btn {
  width: 72rpx;
  height: 72rpx;
  border-radius: 50%;
  background: #1e2d45;
  display: flex;
  align-items: center;
  justify-content: center;
  flex-shrink: 0;
  margin-bottom: 4rpx;
}

.send-btn.active { background: #f5c518; }

.send-icon {
  font-size: 36rpx;
  color: #666666;
  font-weight: 700;
}

.send-btn.active .send-icon { color: #0b1121; }
</style>
