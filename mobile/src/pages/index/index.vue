<script setup lang="ts">
import { ref, nextTick, computed } from 'vue'
import { onShow, onHide, onUnload, onShareAppMessage, onShareTimeline } from '@dcloudio/uni-app'
import { chatApi, type ChatMessage, type ChatPendingResponse, type ChatStatusResponse } from '../../api/index'
import { useAuthStore } from '../../store/auth'
import BottomNav from '../../components/BottomNav.vue'
import { formatAiForMobile, type MobileAiFormatted } from '../../utils/ai_mobile_formatter'

const auth = useAuthStore()

interface UIMessage {
  id: number
  role: 'user' | 'assistant' | 'loading'
  content: string
}

interface PendingTaskRecord {
  task_id: string
  submitted_at_ms: number
  question_text?: string
}

const messages = ref<UIMessage[]>([])
const input = ref('')
const sending = ref(false)
const expandedMap = ref<Record<number, boolean>>({})
let msgCounter = 0
let pollTimer: ReturnType<typeof setInterval> | null = null
let pollingTaskId = ''
let pollingStartedAtMs = 0
let pollingSessionVersion = 0
let pollingRequestInFlight = false
let uiSessionVersion = 0

const CHAT_HISTORY_VERSION = 'v2'
const HISTORY_KEY = computed(() => `chat_history_${CHAT_HISTORY_VERSION}_${auth.username}`)
const LEGACY_HISTORY_KEYS = computed(() => [`chat_history_${auth.username}`])
const PENDING_KEY = computed(() => `chat_pending_${auth.username}`)
const POLL_INTERVAL_MS = 3000
const POLL_TIMEOUT_MS = 400 * 1000
const SHARE_TITLE = '爱波塔-懂期权的AI'
const SHARE_PATH = '/pages/login/index'
const DEFAULT_GREETING_BODY =
  '你好！我是爱波塔 AI。\n\n你可以问我：\n• 中证1000技术面怎么看\n• 300ETF隐含波动率如何\n• 什么是牛市价差策略\n\n内容仅供参考，不构成投资建议。'

function sanitizeComplianceCopy(text: string) {
  return String(text || '').replace(
    /你好！我是爱波塔 AI，专注[^。\n]*。/g,
    '你好！我是爱波塔 AI。',
  )
}

function markAiContent(content: string) {
  const text = sanitizeComplianceCopy(content).trim()
  if (!text) return '【AI生成】'
  return text.startsWith('【AI生成】') ? text : `【AI生成】\n${text}`
}

function normalizeAssistantHistory(list: UIMessage[]) {
  let changed = false
  const normalized = list.map((m) => {
    if (m.role !== 'assistant') return m
    const next = markAiContent(m.content)
    if (next !== m.content) changed = true
    return { ...m, content: next }
  })
  return { normalized, changed }
}

function isBootstrapAssistantContent(content: string): boolean {
  const raw = String(content || '').trim()
  if (!raw) return false
  const normalized = raw.replace(/^【AI生成】\s*/, '').trim()
  return normalized === DEFAULT_GREETING_BODY
}

function normalizeHistoryContent(role: UIMessage['role'], content: string): string {
  const raw = String(content || '').trim()
  if (!raw) return ''
  if (role === 'assistant') return raw.replace(/^【AI生成】\s*/, '').trim()
  return raw
}

const formattedAssistantMap = computed<Record<number, MobileAiFormatted>>(() => {
  const out: Record<number, MobileAiFormatted> = {}
  for (const msg of messages.value) {
    if (msg.role !== 'assistant') continue
    out[msg.id] = formatAiForMobile(msg.content)
  }
  return out
})

function isAssistantExpanded(msgId: number): boolean {
  // 默认展开全文，保证与网页端一致的专业回答密度
  return expandedMap.value[msgId] ?? true
}

function toggleAssistantExpand(msgId: number) {
  const current = expandedMap.value[msgId] ?? true
  expandedMap.value = {
    ...expandedMap.value,
    [msgId]: !current,
  }
}

function readPendingTask(): PendingTaskRecord | null {
  try {
    const raw = uni.getStorageSync(PENDING_KEY.value)
    if (!raw) return null
    const parsed = JSON.parse(raw)
    const taskId = String(parsed?.task_id || '').trim()
    if (!taskId) return null
    const submittedAt = Number(parsed?.submitted_at_ms || 0)
    return {
      task_id: taskId,
      submitted_at_ms: submittedAt > 0 ? submittedAt : Date.now(),
      question_text: String(parsed?.question_text || ''),
    }
  } catch {
    return null
  }
}

function writePendingTask(task: PendingTaskRecord) {
  uni.setStorageSync(PENDING_KEY.value, JSON.stringify(task))
}

function clearPendingTask() {
  uni.removeStorageSync(PENDING_KEY.value)
  pollingTaskId = ''
  pollingStartedAtMs = 0
}

function isStaleSession(sessionVersion: number) {
  return sessionVersion !== uiSessionVersion
}

// ── 初始化：onShow 统一处理，避免 onMounted/onShow 时序竞争 ──
onShow(async () => {
  if (!auth.isLoggedIn) { uni.reLaunch({ url: '/pages/login/index' }); return }
  loadHistory()
  await nextTick()
  await resumePendingTask()
})

// 页面切走/进后台时暂停轮询；保留 pending task，回到页面自动续轮询
onHide(() => {
  pausePollingForBackground()
})

onUnload(() => {
  pausePollingForBackground()
})

function loadHistory() {
  try {
    let saved = uni.getStorageSync(HISTORY_KEY.value)
    let loadedFromLegacy = false
    if (!saved) {
      for (const key of LEGACY_HISTORY_KEYS.value) {
        const legacy = uni.getStorageSync(key)
        if (legacy) {
          saved = legacy
          loadedFromLegacy = true
          break
        }
      }
    }
    if (saved) {
      const loaded: UIMessage[] = JSON.parse(saved)
      const { normalized, changed } = normalizeAssistantHistory(loaded)
      messages.value = normalized
      // 从已加载消息中初始化 msgCounter，避免新消息 ID 与历史 ID 重复导致 v-for 渲染错乱
      msgCounter = loaded.reduce((max, m) => Math.max(max, m.id || 0), 0)
      if (loadedFromLegacy || changed) {
        uni.setStorageSync(HISTORY_KEY.value, JSON.stringify(normalized.slice(-40)))
        for (const key of LEGACY_HISTORY_KEYS.value) uni.removeStorageSync(key)
      }
    }
  } catch { /* 忽略 */ }
  if (messages.value.length === 0) {
    messages.value = [{
      id: ++msgCounter,
      role: 'assistant',
      content: markAiContent(DEFAULT_GREETING_BODY),
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
  const sessionVersion = uiSessionVersion

  // 与网页端对齐：history 只包含“当前提问之前”的上下文
  const historySource = messages.value
    .filter(m => m.role !== 'loading')
    .filter(m => !(m.role === 'assistant' && isBootstrapAssistantContent(m.content)))
    .slice(-12)

  input.value = ''
  sending.value = true

  messages.value.push({ id: ++msgCounter, role: 'user', content: text })
  saveHistory() // 立即保存用户消息，切页后不会丢失
  const loadingId = ++msgCounter
  messages.value.push({ id: loadingId, role: 'loading', content: '' })
  scrollToBottom()

  // 构建历史：不包含当前这一条用户输入（当前输入由 submit 的 prompt 单独传）
  const history: ChatMessage[] = historySource
    .map(m => ({
      role: m.role as 'user' | 'assistant',
      content: normalizeHistoryContent(m.role, m.content),
    }))
    .filter(m => !!m.content)

  try {
    const { task_id } = await chatApi.submit(text, history)
    if (isStaleSession(sessionVersion)) {
      try {
        await chatApi.cancel(task_id, 'clear')
      } catch {
        // ignore
      }
      return
    }
    const pending: PendingTaskRecord = {
      task_id,
      submitted_at_ms: Date.now(),
      question_text: text,
    }
    writePendingTask(pending)
    pollStatus(task_id, loadingId, pending.submitted_at_ms, sessionVersion)
  } catch (e: any) {
    replaceLoading(loadingId, `请求失败：${e.message}`)
    sending.value = false
  }
}

function ensureLoadingBubble(): number {
  const existing = messages.value.find(m => m.role === 'loading')
  if (existing) return existing.id
  const loadingId = ++msgCounter
  messages.value.push({ id: loadingId, role: 'loading', content: '' })
  return loadingId
}

function applyTaskSuccess(taskId: string, loadingId: number, result: any, sessionVersion: number) {
  if (isStaleSession(sessionVersion)) return
  stopPoll()
  clearPendingTask()
  void taskId
  const aiText = result?.response || result?.answer || JSON.stringify(result || {})
  replaceLoading(loadingId, markAiContent(aiText))
  saveHistory()
  sending.value = false
}

function appendAssistantMessage(content: string) {
  messages.value.push({ id: ++msgCounter, role: 'assistant', content: markAiContent(content) })
  scrollToBottom()
}

function applyTaskError(taskId: string, loadingId: number, errorText: string | undefined, sessionVersion: number) {
  if (isStaleSession(sessionVersion)) return
  stopPoll()
  clearPendingTask()
  void taskId
  const msg = String(errorText || '').trim() || 'AI思考太久，请重新提问。'
  const idx = messages.value.findIndex(m => m.id === loadingId && m.role === 'loading')
  if (idx !== -1) {
    replaceLoading(loadingId, markAiContent(msg))
  } else {
    appendAssistantMessage(msg)
  }
  saveHistory()
  sending.value = false
}

function normalizePendingSnapshot(snapshot: ChatPendingResponse): ChatStatusResponse {
  const status = String(snapshot?.status || 'pending').toLowerCase()
  if (status === 'success') {
    return {
      status: 'success',
      progress: '已完成',
      result: snapshot?.result || {},
      error: null,
    }
  }
  if (status === 'error' || status === 'canceled' || status === 'timeout') {
    return {
      status: 'error',
      progress: '任务失败',
      result: null,
      error: snapshot?.error || (status === 'timeout' ? 'AI思考太久，请重新提问。' : '任务失败'),
      code: status === 'timeout' ? 'task_timeout' : status === 'canceled' ? 'task_canceled' : undefined,
    }
  }
  return {
    status: status === 'processing' ? 'processing' : 'pending',
    progress: '处理中',
    result: null,
    error: null,
  }
}

async function resolveTaskSnapshot(
  taskId: string,
  loadingId: number,
  snapshot: ChatStatusResponse,
  startedAtMs: number,
  sessionVersion: number,
) {
  if (isStaleSession(sessionVersion)) return
  if (snapshot.status === 'success') {
    applyTaskSuccess(taskId, loadingId, snapshot.result, sessionVersion)
    return
  }
  if (snapshot.status === 'error') {
    applyTaskError(taskId, loadingId, snapshot.error || 'AI思考太久，请重新提问。', sessionVersion)
    return
  }
  pollStatus(taskId, loadingId, startedAtMs, sessionVersion)
}

// 回到页面时恢复未完成任务（优先以服务端状态为准）
async function resumePendingTask() {
  const sessionVersion = uiSessionVersion
  stopPoll()
  const localPending = readPendingTask()
  let remotePending: ChatPendingResponse | null = null
  try {
    remotePending = await chatApi.pending()
  } catch {
    remotePending = null
  }
  if (isStaleSession(sessionVersion)) return

  let taskId = ''
  let startedAtMs = Date.now()

  if (remotePending?.has_task && remotePending.task_id) {
    taskId = String(remotePending.task_id)
    startedAtMs = localPending?.task_id === taskId
      ? Number(localPending?.submitted_at_ms || Date.now())
      : Date.now()
    writePendingTask({
      task_id: taskId,
      submitted_at_ms: startedAtMs,
      question_text: localPending?.question_text || '',
    })

    const loadingId = ensureLoadingBubble()
    sending.value = true
    scrollToBottom()
    await resolveTaskSnapshot(taskId, loadingId, normalizePendingSnapshot(remotePending), startedAtMs, sessionVersion)
    return
  }

  if (!localPending?.task_id) {
    sending.value = false
    clearPendingTask()
    return
  }

  taskId = localPending.task_id
  startedAtMs = Number(localPending.submitted_at_ms || Date.now())
  const loadingId = ensureLoadingBubble()
  sending.value = true
  scrollToBottom()
  try {
    const status = await chatApi.status(taskId)
    if (isStaleSession(sessionVersion)) return
    await resolveTaskSnapshot(taskId, loadingId, status, startedAtMs, sessionVersion)
  } catch {
    applyTaskError(taskId, loadingId, '网络异常，请稍后重试', sessionVersion)
  }
}

function pollStatus(taskId: string, loadingId: number, startedAtMs: number, sessionVersion: number) {
  if (isStaleSession(sessionVersion)) return
  stopPoll()
  pollingTaskId = taskId
  pollingStartedAtMs = startedAtMs
  pollingSessionVersion = sessionVersion
  pollingRequestInFlight = false
  pollTimer = setInterval(async () => {
    if (isStaleSession(sessionVersion)) {
      stopPoll(sessionVersion)
      return
    }
    if (Date.now() - pollingStartedAtMs >= POLL_TIMEOUT_MS) {
      try {
        await chatApi.cancel(taskId, 'timeout')
      } catch {
        // ignore
      }
      if (isStaleSession(sessionVersion)) return
      applyTaskError(taskId, loadingId, 'AI思考太久，请重新提问。', sessionVersion)
      return
    }

    if (pollingRequestInFlight) return
    pollingRequestInFlight = true
    try {
      const res = await chatApi.status(taskId)
      if (isStaleSession(sessionVersion)) return
      if (res.status === 'success') {
        applyTaskSuccess(taskId, loadingId, res.result, sessionVersion)
      } else if (res.status === 'error') {
        applyTaskError(taskId, loadingId, res.error || 'AI思考太久，请重新提问。', sessionVersion)
      }
    } catch {
      if (isStaleSession(sessionVersion)) return
      applyTaskError(taskId, loadingId, '网络异常，请稍后重试', sessionVersion)
    } finally {
      pollingRequestInFlight = false
    }
  }, POLL_INTERVAL_MS)
}

function stopPoll(expectedSessionVersion?: number) {
  if (typeof expectedSessionVersion === 'number' && expectedSessionVersion !== pollingSessionVersion) {
    return
  }
  if (pollTimer) { clearInterval(pollTimer); pollTimer = null }
  pollingTaskId = ''
  pollingStartedAtMs = 0
  pollingSessionVersion = 0
  pollingRequestInFlight = false
}

function pausePollingForBackground() {
  const hasPending = !!readPendingTask()
  if (!hasPending) return
  stopPoll()
  // 让 onShow 可以正常触发 resume，不被旧 sending 状态拦住
  sending.value = false
}

function replaceLoading(loadingId: number, content: string) {
  const idx = messages.value.findIndex(m => m.id === loadingId)
  if (idx !== -1) {
    messages.value[idx] = { id: loadingId, role: 'assistant', content }
  } else {
    messages.value.push({ id: ++msgCounter, role: 'assistant', content })
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
    success: async (res) => {
      if (res.confirm) {
        const pending = readPendingTask()
        const taskIdToCancel = pending?.task_id || pollingTaskId
        uiSessionVersion += 1
        stopPoll()
        clearPendingTask()
        sending.value = false
        messages.value = []
        expandedMap.value = {}
        uni.removeStorageSync(HISTORY_KEY.value)
        loadHistory()
        if (taskIdToCancel) {
          try {
            await chatApi.cancel(taskIdToCancel, 'clear')
          } catch {
            // ignore
          }
        }
      }
    },
  })
}

onShareAppMessage(() => ({
  title: SHARE_TITLE,
  path: SHARE_PATH,
}))

onShareTimeline(() => ({
  title: SHARE_TITLE,
  query: 'from=timeline',
}))

</script>

<template>
  <view class="page">
    <!-- 顶部操作栏 -->
    <view class="top-bar">
      <text class="top-title">爱波塔</text>
      <text class="clear-btn" @tap="clearChat">清空</text>
    </view>
    <view class="ai-notice">
      <text class="ai-notice-text">以下回复均为人工智能生成内容，请谨慎甄别，仅供参考，不构成投资建议。</text>
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
          <text class="msg-text selectable">
            {{
              isAssistantExpanded(msg.id)
                ? (formattedAssistantMap[msg.id]?.fullText || msg.content)
                : (formattedAssistantMap[msg.id]?.previewText || msg.content)
            }}
          </text>
          <view
            v-if="formattedAssistantMap[msg.id]?.hasMore"
            class="expand-toggle"
            @tap="toggleAssistantExpand(msg.id)"
          >
            <text class="expand-toggle-text">{{ isAssistantExpanded(msg.id) ? '收起' : '展开全文' }}</text>
          </view>
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
        placeholder="问我任何市场问题..."
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

.ai-notice {
  margin: 14rpx 24rpx 0;
  padding: 10rpx 14rpx;
  border-radius: 12rpx;
  border: 1px solid rgba(245, 197, 24, 0.3);
  background: rgba(245, 197, 24, 0.08);
}

.ai-notice-text {
  font-size: 22rpx;
  color: #f5c518;
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
  max-width: 86%;
  border-radius: 24rpx;
  padding: 20rpx 24rpx;
  word-break: break-word;
  overflow-wrap: anywhere;
}

.ai-bubble {
  display: flex;
  flex-direction: column;
  gap: 8rpx;
  background: #162035;
  border: 1px solid #1e2d45;
  border-bottom-left-radius: 6rpx;
}

.user-bubble {
  background: #f5c518;
  border-bottom-right-radius: 6rpx;
}

.user-bubble .msg-text { color: #0b1121; font-weight: 600; }
.msg-text {
  white-space: pre-wrap;
  line-height: 1.82;
}

.ai-bubble .msg-text { color: #f0f0f0; }

.expand-toggle {
  align-self: flex-end;
}

.expand-toggle-text {
  font-size: 22rpx;
  color: #8ea4d1;
}
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
