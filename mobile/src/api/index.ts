/**
 * 爱波塔 Mobile API — 所有接口定义
 */
import { request, uploadFile } from '../utils/request'

function toQuery(params: Record<string, string | number | null | undefined>): string {
  const pairs: string[] = []
  for (const [k, v] of Object.entries(params)) {
    if (v === null || v === undefined) continue
    const value = String(v)
    if (!value) continue
    pairs.push(`${encodeURIComponent(k)}=${encodeURIComponent(value)}`)
  }
  return pairs.length ? `?${pairs.join('&')}` : ''
}

// ── Auth ──────────────────────────────────────────────────

export const authApi = {
  login: (account: string, password: string) =>
    request<{ token: string; username: string }>('POST', '/api/auth/login', { account, password }),

  logout: () =>
    request<{ message: string }>('POST', '/api/auth/logout'),

  verify: () =>
    request<{ valid: boolean; username: string }>('GET', '/api/auth/verify'),
}

// ── Chat ──────────────────────────────────────────────────

export interface ChatMessage {
  role: 'user' | 'assistant'
  content: string
}

export const chatApi = {
  submit: (prompt: string, history: ChatMessage[] = []) =>
    request<{ task_id: string }>('POST', '/api/chat/submit', { prompt, history }),

  status: (taskId: string) =>
    request<{
      status: 'pending' | 'processing' | 'success' | 'error'
      progress: string
      result: any
      error: string | null
    }>('GET', `/api/chat/status/${taskId}`),
}

// ── Intel ────────────────────────────────────────────────

export interface ReportItem {
  id: number
  title: string
  channel_name: string
  channel_code: string
  summary: string
  published_at: string
}

export const intelApi = {
  reports: (params?: { channel_code?: string; page?: number; page_size?: number }) => {
    const qs = toQuery({
      channel_code: params?.channel_code,
      page: params?.page,
      page_size: params?.page_size,
    })
    return request<{ items: ReportItem[]; page: number; has_more: boolean }>(
      'GET', `/api/intel/reports${qs}`
    )
  },

  detail: (id: number) =>
    request<{ id: number; title: string; content: string; channel_name: string; published_at: string }>(
      'GET', `/api/intel/report/${id}`
    ),

  subscribe: (channel_code: string) =>
    request<{ message: string }>('POST', '/api/intel/subscribe', { channel_code }),
}

// ── Market ───────────────────────────────────────────────

export interface OptionItem {
  name: string
  product_code: string  // 品种代码，如 m / rb / cu
  iv: number            // 当前IV%
  iv_rank: number       // IV Rank 0-100，-1 表示快到期
  iv_chg_1d: number     // 当日IV变动（百分点）
  pct_1d: number        // 标的当日涨跌%
  pct_5d: number        // 标的5日涨跌%
  retail_chg: number
  inst_chg: number
  cur_price: number     // 最新收盘价（DB）
}

export interface BrokerRankItem {
  rank: number
  broker: string
  score: number
}

export interface PriceItem {
  code: string       // 品种代码，如 cu / rb
  name: string       // 中文名，如 沪铜
  price: number      // 当前价
  pct: number        // 涨跌幅 %（正=涨，负=跌）
  volume: number     // 成交量
  updated_at: string // 最后更新时间（ticktime）
}

export interface ContractLiveItem {
  open: number
  high: number
  low: number
  price: number      // 当前价（trade）
  pct: number        // 涨跌幅 %
  volume: number
  updated_at: string
  trading_day?: string // YYYYMMDD, night-session trading day
}

export interface BrokerDetailRow {
  dt: string       // YYYY-MM-DD
  net_vol: number  // 净持仓手数
  pct_chg: number  // 当日价格涨跌幅%
  score: number    // 当日得分
  cum_score: number // 累计得分
}

export const marketApi = {
  snapshot: () =>
    request<{ data: any }>('GET', '/api/market/snapshot'),

  options: () =>
    request<{ items: OptionItem[]; updated_at: string }>('GET', '/api/market/options'),

  holding: (product: string) =>
    request<{
      product: string
      brokers: Array<{ broker: string; score: number; net_vol: number; direction: string }>
      trade_date: string
    }>('GET', `/api/market/holding/${product}`),

  contracts: (product: string) =>
    request<{ items: OptionItem[] }>('GET', `/api/market/contracts/${product}`),

  prices: () =>
    request<{
      items: PriceItem[]
      is_trading: boolean
      refreshed_at: string
      contracts: Record<string, ContractLiveItem>
    }>('GET', '/api/market/prices'),

  brokerDetail: (product: string, broker: string) =>
    request<{
      product: string
      broker: string
      total_score: number
      rows: BrokerDetailRow[]
    }>('GET', `/api/market/broker/${product}?broker=${encodeURIComponent(broker)}`),

  chart: (product: string, contract?: string) =>
    request<{
      product: string
      cn_name: string
      main_contract: string
      cur_price: number | null
      cur_pct: number | null
      cur_iv: number | null
      dumb_chg_1d: number | null
      ohlc: Array<{ dt: string; o: number; h: number; l: number; c: number; pct: number }>
      iv: Array<{ dt: string; v: number }>
      dumb: Array<{ dt: string; net: number; chg: number }>
      smart: Array<{ dt: string; net: number; chg: number }>
      total_oi: Array<{ dt: string; v: number }>
    }>('GET', `/api/market/chart/${product}${contract ? `?contract=${encodeURIComponent(contract)}` : ''}`),
}

// ── Portfolio ─────────────────────────────────────────────

export const portfolioApi = {
  upload: (filePath: string) =>
    uploadFile<{ task_id: string; recognized_count: number; message: string }>(
      '/api/portfolio/upload', filePath
    ),

  status: (taskId: string) =>
    request<{
      status: 'pending' | 'processing' | 'success' | 'error'
      progress: string
      result: any
      error: string | null
    }>('GET', `/api/portfolio/status/${taskId}`),

  result: () =>
    request<{ has_data: boolean; snapshot: any }>('GET', '/api/portfolio/result'),
}

// ── User ─────────────────────────────────────────────────

export interface UserProfile {
  username: string
  email: string
  level: number
  risk_preference: string
  focus_assets: string
  subscriptions: Array<{
    channel_name: string
    channel_code: string
    expires_at: string
    is_active: boolean
  }>
}

export const userApi = {
  profile: () => request<UserProfile>('GET', '/api/user/profile'),
}

// ── Payment ──────────────────────────────────────────────

export interface WalletInfo {
  balance: number
  total_earned: number
  total_spent: number
  updated_at: string
  payment_enabled: boolean
}

export interface TopupPackage {
  name: string
  rmb: number
  points: number
  bonus_points: number
}

export interface PaidProduct {
  product_type: 'channel' | 'package'
  code: string
  id: number | null
  name: string
  icon: string
  points_monthly: number
  months_options: number[]
  includes: string[]
  includes_names: string[]
}

export interface PurchaseRequest {
  product_type: 'channel' | 'package'
  code: string
  months: number
}

export interface PurchaseResponse {
  ok: boolean
  message: string
}

export interface PayConfig {
  recharge_url: string
  service_wechat: string
  service_phone: string
}

export const payApi = {
  wallet: () =>
    request<WalletInfo>('GET', '/api/pay/wallet'),

  packages: () =>
    request<{ items: TopupPackage[] }>('GET', '/api/pay/packages'),

  products: () =>
    request<{ items: PaidProduct[] }>('GET', '/api/pay/products'),

  purchase: (body: PurchaseRequest) =>
    request<PurchaseResponse>('POST', '/api/pay/purchase', body),

  config: () =>
    request<PayConfig>('GET', '/api/pay/config'),
}

// ── Kline Training ────────────────────────────────────────

export interface KlineBar {
  dt: string
  o: number
  h: number
  l: number
  c: number
  v: number
}

export interface KlineGame {
  game_id: number
  symbol: string
  symbol_name: string
  symbol_type: string
  capital: number
  history_count: number
  bars: KlineBar[]
}

export interface KlineData {
  symbol: string
  symbol_name: string
  symbol_type: string
  capital: number
  history_count: number
  bars: KlineBar[]
}

export interface LbRow { user_id: string; value: number }
export interface KlineEntryData {
  capital: number
  leaderboard: { capital: LbRow[]; max_profit: LbRow[]; streak: LbRow[] }
}

export const klineApi = {
  getEntry: () =>
    request<KlineEntryData>('GET', '/api/kline/entry'),

  // Step 1: load data only (no DB record created — leaving now won't be penalized)
  getData: () =>
    request<KlineData>('GET', '/api/kline/data'),

  // Step 2: create game record once K-line is actually playing
  startRecord: (body: { symbol: string; symbol_name: string; symbol_type: string; capital: number; leverage: number; speed: number }) =>
    request<{ game_id: number }>('POST', '/api/kline/start', body),

  checkUnfinished: () =>
    request<{
      has_unfinished: boolean
      game_id?: number
      symbol_name?: string
      capital?: number
      penalty?: number
    }>('GET', '/api/kline/check'),

  abandonGame: (game_id: number) =>
    request<{ ok: boolean; penalty: number }>('POST', '/api/kline/abandon', { game_id }),

  saveGame: (body: {
    game_id: number
    profit: number
    profit_rate: number
    trade_count: number
    max_drawdown: number
    capital: number
  }) =>
    request<{ ok: boolean; profit: number; profit_rate: number }>('POST', '/api/kline/save', body),
}
