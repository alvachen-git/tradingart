"""
K线训练 - 支持手数交易的版本
特性：
1. 每手 = 1000元
2. 支持加仓 1手、5手、30手
3. 支持部分平仓或全部平仓
4. K线图为主画面

【修复内容】
1. 移除 @st.cache_resource 装饰器，解决 CachedWidgetWarning
2. 修复游戏正常结束却被判定"未完成"的问题
3. K线颜色改为中国标准：红涨绿跌
"""

import streamlit as st
import streamlit.components.v1 as components
import pandas as pd
import json
from datetime import datetime, timedelta
import sys
import os
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import kline_game as kg
import auth_utils as auth
import extra_streamlit_components as stx

# 页面配置
st.set_page_config(
    page_title="K线交易训练",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# 样式
st.markdown("""
<style>
    .stApp { background-color: #0b1121 !important; color: white !important; }
    .block-container { padding: 0.5rem 1rem !important; max-width: 100% !important; }
    #MainMenu, footer, [data-testid="stDecoration"] { display: none !important; }
    .stButton > button {
        background: linear-gradient(135deg, #3b82f6, #1d4ed8) !important;
        color: white !important; border: none !important;
        padding: 12px 24px !important; border-radius: 8px !important;
    }
    .game-setup-card {
        background: linear-gradient(135deg, #1a1f2e, #2a3441);
        border: 2px solid #3b4252; border-radius: 16px;
        padding: 24px; margin: 16px 0;
    }
</style>
""", unsafe_allow_html=True)


# 🔧 【修复1】Cookie管理 - 移除 @st.cache_resource 装饰器
# 因为 CookieManager 是一个 widget，不能在缓存函数中创建
def get_cookie_manager():
    return stx.CookieManager(key="kline_lot_trading_v1")


cookie_manager = get_cookie_manager()

if 'is_logged_in' not in st.session_state:
    st.session_state['is_logged_in'] = False
    st.session_state['user_id'] = None
if 'game_started' not in st.session_state:
    st.session_state['game_started'] = False

# 🔧 【修复2】新增：用于记录刚结束的游戏ID，避免误判
if 'just_finished_game_id' not in st.session_state:
    st.session_state['just_finished_game_id'] = None

# 恢复登录
if not st.session_state.get('is_logged_in'):
    try:
        cookies = cookie_manager.get_all() or {}
        if cookies.get("username") and cookies.get("token"):
            if auth.check_token(str(cookies["username"]), cookies["token"]):
                st.session_state['is_logged_in'] = True
                st.session_state['user_id'] = str(cookies["username"])
    except:
        pass

# 侧边栏
with st.sidebar:
    st.markdown("### 🎮 K线训练场")
    if st.session_state.get('is_logged_in'):
        st.success(f"👤 {st.session_state.get('user_id')}")

# 🔧 【修复2】处理游戏结果 - 先处理结果，再检查未完成游戏
game_done = st.query_params.get('game_done', '')
if game_done == '1':
    profit = int(st.query_params.get('profit', '0') or '0')
    profit_rate = float(st.query_params.get('rate', '0') or '0')
    trade_count = int(st.query_params.get('trades', '0') or '0')
    max_drawdown = float(st.query_params.get('drawdown', '0') or '0')
    game_id = int(st.query_params.get('game_id', '0') or '0') or None
    symbol = st.query_params.get('symbol', '')
    symbol_name = st.query_params.get('symbol_name', '未知')
    symbol_type = st.query_params.get('symbol_type', 'stock')
    capital_before = int(float(st.query_params.get('capital', '1000000') or '1000000'))

    st.query_params.clear()
    st.session_state['game_started'] = False
    if 'game_data' in st.session_state: del st.session_state['game_data']

    # 🔧 记录刚结束的游戏ID
    st.session_state['just_finished_game_id'] = game_id

    try:
        if game_id:
            game_info = kg.get_game_info(game_id)
            if game_info:
                kg.end_game(game_id, game_info['user_id'], 'finished', 'completed',
                            profit, profit_rate, capital_before + profit, trade_count, max_drawdown)
    except Exception as e:
        print(f"结算游戏失败: {e}")

    st.markdown("<h1 style='text-align:center;color:#e5e7eb;'>🎯 游戏结束</h1>", unsafe_allow_html=True)
    col1, col2, col3 = st.columns([1, 2, 1])
    with col2:
        # 🔧 【修复3】盈利显示也改为红涨绿跌
        profit_color = '#ef4444' if profit > 0 else '#22c55e' if profit < 0 else '#e5e7eb'
        st.markdown(f"""
        <div class="game-setup-card" style="text-align:center;">
            <div style="color:#9ca3af;">揭晓品种</div>
            <div style="font-size:32px;font-weight:700;color:#e5e7eb;">{symbol_name}</div>
            <div style="color:#6b7280;margin-bottom:20px;">{symbol}</div>
            <div style="display:flex;justify-content:space-around;">
                <div><div style="color:#9ca3af;">盈亏</div><div style="color:{profit_color};font-size:24px;font-weight:bold;">{'+' if profit >= 0 else ''}{profit:,.0f}</div></div>
                <div><div style="color:#9ca3af;">收益率</div><div style="color:{profit_color};font-size:24px;font-weight:bold;">{'+' if profit_rate >= 0 else ''}{profit_rate * 100:.2f}%</div></div>
                <div><div style="color:#9ca3af;">交易次数</div><div style="color:#e5e7eb;font-size:24px;font-weight:bold;">{trade_count}</div></div>
            </div>
        </div>
        """, unsafe_allow_html=True)
        if st.button("🎮 再来一局", type="primary", use_container_width=True):
            # 清除刚结束的游戏ID标记
            st.session_state['just_finished_game_id'] = None
            st.rerun()
    st.stop()

# 登录检查
if not st.session_state.get('is_logged_in'):
    st.warning("请先在首页登录")
    st.stop()

user_id = st.session_state.get('user_id')

# 🔧 【修复2】检查未完成游戏 - 排除刚结束的游戏
if not st.session_state.get('game_started'):
    try:
        last_unfinished = kg.check_unfinished_game(user_id)
        if last_unfinished:
            unfinished_game_id = last_unfinished.get('id')
            game_start_time = last_unfinished.get('game_start_time')

            # 🔧 核心修复：排除刚结束的游戏
            just_finished_id = st.session_state.get('just_finished_game_id')
            if just_finished_id and unfinished_game_id == just_finished_id:
                # 这是刚结束的游戏，跳过惩罚
                st.session_state['just_finished_game_id'] = None
            else:
                # 检查是否是最近10秒内开始的游戏（正常情况）
                is_recent = game_start_time and isinstance(game_start_time, datetime) and (
                        datetime.now() - game_start_time).total_seconds() < 10
                if not is_recent:
                    st.error("⚠️ 检测到未完成游戏")
                    col1, col2, col3 = st.columns([1, 2, 1])
                    with col2:
                        st.markdown(f"""<div class="game-setup-card" style="text-align:center;border-color:#dc2626;">
                            <p style="color:#fca5a5;">品种：{last_unfinished.get('symbol_name', '???')}</p>
                            <p style="color:#fca5a5;">惩罚：-50,000 元</p>
                        </div>""", unsafe_allow_html=True)
                        if st.button("确认并重新开始", type="primary", use_container_width=True):
                            kg.settle_abandoned_game(user_id, last_unfinished['id'])
                            time.sleep(1)
                            st.rerun()
                    st.stop()
    except:
        pass

user_capital = kg.get_user_capital(user_id) or 1000000

# 游戏设置页面
if not st.session_state.get('game_started'):
    st.markdown("<h1 style='text-align:center;color:#e5e7eb;'>📈 K线交易训练</h1>", unsafe_allow_html=True)
    col1, col2, col3 = st.columns([1, 2, 1])
    with col2:
        st.markdown(f"""<div style="background:#2a3441;padding:16px;border-radius:10px;text-align:center;margin:16px 0;">
            <div style="color:#9ca3af;">账户资金</div>
            <div style="font-size:32px;font-weight:700;color:#ef4444;">{user_capital:,.0f}</div>
            <div style="color:#64748b;font-size:14px;margin-top:8px;">每手 = 1,000元</div>
        </div>""", unsafe_allow_html=True)

        col_a, col_b = st.columns(2)
        with col_a:
            speed = st.selectbox("播放速度", ["1秒/根", "5秒/根"], index=0)
            speed_ms = {"1秒/根": 1000, "5秒/根": 5000}[speed]
            speed_sec = {"1秒/根": 1, "5秒/根": 5}[speed]
        with col_b:
            leverage = st.selectbox("杠杆倍数", ["1倍", "5倍", "10倍"], index=0)
            leverage_val = {"1倍": 1, "5倍": 5, "10倍": 10}[leverage]

        if st.button("🎮 开始游戏", type="primary", use_container_width=True):
            with st.spinner("加载K线数据..."):
                symbol, symbol_name, symbol_type, df = kg.get_random_kline_data(bars=100, history_bars=60)
                if df is None or len(df) < 160:
                    st.error("数据加载失败")
                    st.stop()

                kline_data = [{'open': float(r['open_price']), 'high': float(r['high_price']),
                               'low': float(r['low_price']), 'close': float(r['close_price'])} for _, r in
                              df.iterrows()]

                game_id = kg.start_game(user_id, symbol, symbol_name, symbol_type, user_capital, leverage_val, speed_sec)
                if not game_id:
                    st.error("游戏创建失败")
                    st.stop()

                st.session_state['game_started'] = True
                st.session_state['game_data'] = {
                    'kline_data': kline_data,
                    'config': {'symbol': symbol, 'symbolName': symbol_name, 'symbolType': symbol_type,
                               'capital': user_capital, 'leverage': leverage_val, 'speed': speed_ms, 'gameId': game_id}
                }
                st.rerun()

        with st.expander("📋 交易规则"):
            st.markdown("""
            - **每手 = 1,000元**，可自由选择加仓手数
            - 支持**做多**和**做空**
            - 可以多次**加仓**，累计持仓
            - 可以选择**平仓手数**或一键全平
            - 杠杆放大盈亏，注意风险
            """)
    st.stop()

# ==========================================
# 游戏界面
# ==========================================
if st.session_state.get('game_started') and 'game_data' in st.session_state:
    game_data = st.session_state['game_data']
    kline_data = game_data['kline_data']
    config = game_data['config']

    # 🔧 【修复3】K线颜色改为中国标准：红涨绿跌
    trading_html = f'''
<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <script src="https://unpkg.com/lightweight-charts@4.0.1/dist/lightweight-charts.standalone.production.js"></script>
    <style>
        * {{ margin: 0; padding: 0; box-sizing: border-box; }}
        body {{ 
            background: #0a0e1a;
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
            color: #e5e7eb;
            height: 100vh;
            display: flex;
            flex-direction: column;
            overflow: hidden;
        }}

        /* 顶部信息栏 */
        .top-bar {{
            display: flex;
            justify-content: space-between;
            align-items: center;
            padding: 10px 20px;
            background: rgba(30, 41, 59, 0.95);
            border-bottom: 1px solid #334155;
            flex-shrink: 0;
        }}
        .price-section {{ display: flex; align-items: center; gap: 20px; }}
        .price-label {{ color: #64748b; font-size: 13px; }}
        .price-value {{ font-size: 24px; font-weight: 700; }}
        .price-up {{ color: #ef4444; }}
        .price-down {{ color: #22c55e; }}
        .progress-section {{ display: flex; align-items: center; gap: 12px; }}
        .progress-bar {{ width: 180px; height: 6px; background: #1e293b; border-radius: 3px; overflow: hidden; }}
        .progress-fill {{ height: 100%; background: linear-gradient(90deg, #3b82f6, #ef4444); transition: width 0.3s; }}
        .info-badge {{ background: #1e293b; padding: 6px 12px; border-radius: 6px; font-size: 13px; }}
        .info-badge span {{ color: #f59e0b; font-weight: 600; }}

        /* K线图区域 */
        .chart-area {{ flex: 1; background: #0f172a; min-height: 0; }}
        #chart {{ width: 100%; height: 100%; }}

        /* 底部交易面板 */
        .trade-panel {{
            background: rgba(30, 41, 59, 0.98);
            border-top: 1px solid #334155;
            padding: 12px 16px;
            flex-shrink: 0;
        }}
        .panel-row {{
            display: flex;
            align-items: center;
            gap: 20px;
        }}

        /* 账户信息 */
        .account-info {{
            display: flex;
            gap: 24px;
            padding-right: 20px;
            border-right: 1px solid #334155;
        }}
        .account-item {{ text-align: center; }}
        .account-label {{ font-size: 11px; color: #64748b; margin-bottom: 2px; }}
        .account-value {{ font-size: 15px; font-weight: 600; }}
        .profit {{ color: #ef4444; }}
        .loss {{ color: #22c55e; }}

        /* 持仓信息 */
        .position-info {{
            display: flex;
            gap: 20px;
            padding: 0 20px;
            border-right: 1px solid #334155;
        }}
        .pos-item {{ text-align: center; min-width: 70px; }}
        .pos-label {{ font-size: 11px; color: #64748b; }}
        .pos-value {{ font-size: 14px; font-weight: 600; }}

        /* 交易控制 */
        .trade-controls {{
            display: flex;
            align-items: center;
            gap: 12px;
            flex: 1;
        }}

        /* 手数选择 */
        .lot-selector {{
            display: flex;
            align-items: center;
            gap: 6px;
            background: #1e293b;
            padding: 4px;
            border-radius: 6px;
        }}
        .lot-btn {{
            padding: 6px 12px;
            border: none;
            background: transparent;
            color: #94a3b8;
            font-size: 13px;
            font-weight: 500;
            cursor: pointer;
            border-radius: 4px;
            transition: all 0.15s;
        }}
        .lot-btn:hover {{ background: #334155; color: #e5e7eb; }}
        .lot-btn.active {{ background: #3b82f6; color: white; }}
        .lot-input {{
            width: 60px;
            padding: 6px 8px;
            border: 1px solid #334155;
            background: #0f172a;
            color: #e5e7eb;
            font-size: 13px;
            text-align: center;
            border-radius: 4px;
        }}

        /* 交易按钮 */
        .action-buttons {{
            display: flex;
            gap: 8px;
        }}
        .action-btn {{
            padding: 8px 16px;
            border: none;
            border-radius: 6px;
            font-size: 13px;
            font-weight: 600;
            cursor: pointer;
            transition: all 0.15s;
            white-space: nowrap;
        }}
        .action-btn:disabled {{ opacity: 0.35; cursor: not-allowed; }}
        .action-btn:not(:disabled):hover {{ filter: brightness(1.1); transform: translateY(-1px); }}
        .btn-long {{ background: #ef4444; color: white; }}
        .btn-short {{ background: #22c55e; color: white; }}
        .btn-close {{ background: #3b82f6; color: white; }}
        .btn-close-all {{ background: #6366f1; color: white; }}

        /* 结束按钮 */
        .end-section {{
            margin-left: auto;
            display: flex;
            align-items: center;
            gap: 12px;
        }}
        .end-btn {{
            padding: 8px 20px;
            background: #475569;
            color: white;
            border: none;
            border-radius: 6px;
            font-weight: 600;
            cursor: pointer;
        }}
        .end-btn:hover {{ background: #64748b; }}

        /* 结算提示层 */
        .settle-overlay {{
            position: fixed;
            inset: 0;
            background: rgba(2, 6, 23, 0.85);
            display: none;
            align-items: center;
            justify-content: center;
            z-index: 999;
        }}
        .settle-card {{
            background: #0f172a;
            border: 1px solid #334155;
            border-radius: 16px;
            padding: 28px 32px;
            min-width: 360px;
            text-align: center;
            box-shadow: 0 20px 60px rgba(0,0,0,0.45);
        }}
        .settle-title {{
            font-size: 20px;
            font-weight: 700;
            margin-bottom: 12px;
            color: #e5e7eb;
        }}
        .settle-symbol {{
            font-size: 26px;
            font-weight: 700;
            color: #e5e7eb;
        }}
        .settle-sub {{
            color: #94a3b8;
            margin-bottom: 16px;
        }}
        .settle-row {{
            display: flex;
            justify-content: space-between;
            gap: 16px;
            margin-top: 12px;
        }}
        .settle-item {{
            flex: 1;
            background: #111827;
            border: 1px solid #1f2937;
            border-radius: 10px;
            padding: 12px;
        }}
        .settle-label {{ color: #94a3b8; font-size: 12px; }}
        .settle-value {{ font-size: 20px; font-weight: 700; }}
        .settle-profit {{ color: #ef4444; }}
        .settle-loss {{ color: #22c55e; }}
    </style>
</head>
<body>
    <!-- 顶部信息栏 -->
    <div class="top-bar">
        <div class="price-section">
            <span class="price-label">当前价格</span>
            <span id="current-price" class="price-value price-up">--</span>
            <span id="price-change" style="font-size:13px;color:#64748b;">--</span>
        </div>
        <div class="progress-section">
            <div class="progress-bar"><div id="progress-fill" class="progress-fill" style="width:0%"></div></div>
            <span id="progress-text" style="color:#94a3b8;font-size:13px;">0/100</span>
        </div>
        <div style="display:flex;gap:12px;">
            <div class="info-badge">杠杆: <span>{config['leverage']}x</span></div>
            <div class="info-badge">每手: <span>1,000元</span></div>
        </div>
    </div>

    <!-- K线图 -->
    <div class="chart-area"><div id="chart"></div></div>

    <!-- 底部交易面板 -->
    <div class="trade-panel">
        <div class="panel-row">
            <!-- 账户信息 -->
            <div class="account-info">
                <div class="account-item">
                    <div class="account-label">可用资金</div>
                    <div id="available-cash" class="account-value">{config['capital']:,}</div>
                </div>
                <div class="account-item">
                    <div class="account-label">持仓市值</div>
                    <div id="position-value" class="account-value">0</div>
                </div>
                <div class="account-item">
                    <div class="account-label">浮动盈亏</div>
                    <div id="floating-pnl" class="account-value">0</div>
                </div>
                <div class="account-item">
                    <div class="account-label">已实现盈亏</div>
                    <div id="realized-pnl" class="account-value">0</div>
                </div>
            </div>

            <!-- 持仓信息 -->
            <div class="position-info">
                <div class="pos-item">
                    <div class="pos-label">方向</div>
                    <div id="pos-direction" class="pos-value" style="color:#94a3b8;">空仓</div>
                </div>
                <div class="pos-item">
                    <div class="pos-label">持仓手数</div>
                    <div id="pos-lots" class="pos-value">0</div>
                </div>
                <div class="pos-item">
                    <div class="pos-label">均价</div>
                    <div id="pos-avg-price" class="pos-value">--</div>
                </div>
            </div>

            <!-- 交易控制 -->
            <div class="trade-controls">
                <!-- 手数选择 -->
                <div class="lot-selector">
                    <button class="lot-btn" onclick="setLots(1)">1手</button>
                    <button class="lot-btn" onclick="setLots(5)">5手</button>
                    <button class="lot-btn" onclick="setLots(10)">10手</button>
                    <button class="lot-btn" onclick="setLots(30)">30手</button>
                    <input type="number" id="lot-input" class="lot-input" value="1" min="1" onchange="updateLotButtons()">
                </div>

                <!-- 交易按钮 -->
                <div class="action-buttons">
                    <button id="btn-long" class="action-btn btn-long" onclick="openPosition('long')">做多</button>
                    <button id="btn-short" class="action-btn btn-short" onclick="openPosition('short')">做空</button>
                    <button id="btn-close" class="action-btn btn-close" onclick="closePosition()" disabled>平仓</button>
                    <button id="btn-close-all" class="action-btn btn-close-all" onclick="closeAll()" disabled>全平</button>
                </div>
            </div>

            <!-- 结束 -->
            <div class="end-section">
                <button class="end-btn" onclick="confirmEnd()">结束游戏</button>
            </div>
        </div>
    </div>

    <!-- 结算遮罩 -->
    <div class="settle-overlay" id="settle-overlay">
        <div class="settle-card">
            <div class="settle-title">结算中</div>
            <div class="settle-symbol" id="settle-symbol-name">--</div>
            <div class="settle-sub" id="settle-symbol-code">--</div>
            <div class="settle-row">
                <div class="settle-item">
                    <div class="settle-label">盈亏</div>
                    <div class="settle-value" id="settle-profit">--</div>
                </div>
                <div class="settle-item">
                    <div class="settle-label">收益率</div>
                    <div class="settle-value" id="settle-rate">--</div>
                </div>
            </div>
        </div>
    </div>

    <script>
        const CONFIG = {{
            capital: {config['capital']},
            leverage: {config['leverage']},
            speed: {config['speed']},
            gameId: {config['gameId']},
            symbol: '{config['symbol']}',
            symbolName: '{config['symbolName']}',
            symbolType: '{config['symbolType']}',
            lotSize: 1000  // 每手1000元
        }};

        const KLINE = {json.dumps(kline_data)};
        const HISTORY = 60, PLAY = 100;

        // 游戏状态
        let state = {{
            bar: HISTORY - 1,
            played: 0,
            running: false,
            ending: false,
            cash: CONFIG.capital,
            position: {{ direction: null, lots: 0, avgPrice: 0, totalCost: 0 }},
            pnl: {{ floating: 0, realized: 0, max: 0, maxDD: 0 }},
            trades: 0,
            prevPrice: KLINE[HISTORY - 1].close
        }};

        let chart, candles;

        // 初始化图表
        function initChart() {{
            const el = document.getElementById('chart');
            chart = LightweightCharts.createChart(el, {{
                width: el.clientWidth, height: el.clientHeight,
                layout: {{ background: {{ type: 'solid', color: '#0f172a' }}, textColor: '#94a3b8' }},
                grid: {{ vertLines: {{ color: '#1e293b' }}, horzLines: {{ color: '#1e293b' }} }},
                crosshair: {{ mode: LightweightCharts.CrosshairMode.Normal }},
                rightPriceScale: {{ borderColor: '#334155' }},
                timeScale: {{ borderColor: '#334155', timeVisible: false }}
            }});

            // 🔧 【修复3】K线颜色：红涨绿跌（中国标准）
            candles = chart.addCandlestickSeries({{
                upColor: '#ef4444', downColor: '#22c55e',
                borderUpColor: '#ef4444', borderDownColor: '#22c55e',
                wickUpColor: '#ef4444', wickDownColor: '#22c55e'
            }});

            candles.setData(KLINE.slice(0, HISTORY).map((d, i) => ({{
                time: i, open: d.open, high: d.high, low: d.low, close: d.close
            }})));
            chart.timeScale().fitContent();

            updateDisplay();
            setTimeout(() => {{ state.running = true; playBar(); }}, 800);
        }}

        // 播放K线
        function playBar() {{
            if (!state.running) return;
            state.bar++;
            state.played++;

            if (state.played > PLAY) {{ endGame(); return; }}

            const bar = KLINE[state.bar];
            candles.update({{ time: state.bar, open: bar.open, high: bar.high, low: bar.low, close: bar.close }});

            calcPnL();
            updateDisplay();
            state.prevPrice = bar.close;

            setTimeout(playBar, CONFIG.speed);
        }}

        // 设置手数
        function setLots(n) {{
            document.getElementById('lot-input').value = n;
            updateLotButtons();
        }}

        function updateLotButtons() {{
            const v = parseInt(document.getElementById('lot-input').value) || 1;
            document.querySelectorAll('.lot-btn').forEach(btn => {{
                btn.classList.toggle('active', parseInt(btn.textContent) === v);
            }});
        }}

        function getLots() {{
            return Math.max(1, parseInt(document.getElementById('lot-input').value) || 1);
        }}

        // 开仓/加仓
        function openPosition(dir) {{
            if (!state.running) return;

            const lots = getLots();
            const price = KLINE[state.bar].close;
            const cost = lots * CONFIG.lotSize;

            // 检查资金
            if (cost > state.cash) {{
                alert('资金不足！需要 ' + cost.toLocaleString() + ' 元，可用 ' + state.cash.toLocaleString() + ' 元');
                return;
            }}

            // 如果已有反向持仓，需要先平仓
            if (state.position.direction && state.position.direction !== dir) {{
                alert('请先平掉现有' + (state.position.direction === 'long' ? '多' : '空') + '仓');
                return;
            }}

            // 开仓或加仓
            if (!state.position.direction) {{
                // 新开仓
                state.position = {{
                    direction: dir,
                    lots: lots,
                    avgPrice: price,
                    totalCost: cost
                }};
            }} else {{
                // 加仓 - 计算新均价
                const oldValue = state.position.lots * state.position.avgPrice;
                const newValue = lots * price;
                const totalLots = state.position.lots + lots;
                state.position.avgPrice = (oldValue + newValue) / totalLots;
                state.position.lots = totalLots;
                state.position.totalCost += cost;
            }}

            state.cash -= cost;
            state.trades++;

            calcPnL();
            updateDisplay();
            updateButtons();
        }}

        // 平仓（按手数）
        function closePosition(force=false) {{
            if ((!state.running && !force) || !state.position.direction) return;

            const lots = Math.min(getLots(), state.position.lots);
            const price = KLINE[state.bar].close;

            // 计算盈亏
            let pnl;
            if (state.position.direction === 'long') {{
                pnl = (price - state.position.avgPrice) * lots * CONFIG.lotSize / state.position.avgPrice * CONFIG.leverage;
            }} else {{
                pnl = (state.position.avgPrice - price) * lots * CONFIG.lotSize / state.position.avgPrice * CONFIG.leverage;
            }}

            // 返还本金
            const returnCost = lots * CONFIG.lotSize;
            state.cash += returnCost + pnl;
            state.pnl.realized += pnl;
            state.trades++;

            // 更新持仓
            state.position.lots -= lots;
            state.position.totalCost -= returnCost;

            if (state.position.lots <= 0) {{
                state.position = {{ direction: null, lots: 0, avgPrice: 0, totalCost: 0 }};
            }}

            calcPnL();
            updateDisplay();
            updateButtons();
        }}

        // 全部平仓
        function closeAll() {{
            if (!state.position.direction) return;
            document.getElementById('lot-input').value = state.position.lots;
            closePosition(true);
        }}

        // 计算浮动盈亏
        function calcPnL() {{
            if (!state.position.direction) {{
                state.pnl.floating = 0;
                return;
            }}

            const price = KLINE[state.bar].close;
            const lots = state.position.lots;

            if (state.position.direction === 'long') {{
                state.pnl.floating = (price - state.position.avgPrice) * lots * CONFIG.lotSize / state.position.avgPrice * CONFIG.leverage;
            }} else {{
                state.pnl.floating = (state.position.avgPrice - price) * lots * CONFIG.lotSize / state.position.avgPrice * CONFIG.leverage;
            }}

            const total = state.pnl.realized + state.pnl.floating;
            if (total > state.pnl.max) state.pnl.max = total;
            const dd = (state.pnl.max - total) / CONFIG.capital;
            if (dd > state.pnl.maxDD) state.pnl.maxDD = dd;
        }}

        // 更新显示
        function updateDisplay() {{
            const price = KLINE[state.bar].close;
            const change = ((price - state.prevPrice) / state.prevPrice * 100);

            // 价格 - 红涨绿跌
            const priceEl = document.getElementById('current-price');
            priceEl.textContent = price.toFixed(2);
            priceEl.className = 'price-value ' + (price >= state.prevPrice ? 'price-up' : 'price-down');
            document.getElementById('price-change').textContent = (change >= 0 ? '+' : '') + change.toFixed(2) + '%';
            document.getElementById('price-change').style.color = change >= 0 ? '#ef4444' : '#22c55e';

            // 进度
            document.getElementById('progress-fill').style.width = (state.played / PLAY * 100) + '%';
            document.getElementById('progress-text').textContent = state.played + '/' + PLAY;

            // 账户
            document.getElementById('available-cash').textContent = Math.round(state.cash).toLocaleString();

            const posValue = state.position.lots * CONFIG.lotSize;
            document.getElementById('position-value').textContent = posValue.toLocaleString();

            // 浮动盈亏 - 红涨绿跌
            const floatEl = document.getElementById('floating-pnl');
            floatEl.textContent = (state.pnl.floating >= 0 ? '+' : '') + Math.round(state.pnl.floating).toLocaleString();
            floatEl.className = 'account-value ' + (state.pnl.floating >= 0 ? 'profit' : 'loss');

            const realEl = document.getElementById('realized-pnl');
            realEl.textContent = (state.pnl.realized >= 0 ? '+' : '') + Math.round(state.pnl.realized).toLocaleString();
            realEl.className = 'account-value ' + (state.pnl.realized >= 0 ? 'profit' : 'loss');

            // 持仓 - 红涨绿跌
            const dirEl = document.getElementById('pos-direction');
            if (state.position.direction === 'long') {{
                dirEl.textContent = '多头';
                dirEl.style.color = '#ef4444';
            }} else if (state.position.direction === 'short') {{
                dirEl.textContent = '空头';
                dirEl.style.color = '#22c55e';
            }} else {{
                dirEl.textContent = '空仓';
                dirEl.style.color = '#94a3b8';
            }}

            document.getElementById('pos-lots').textContent = state.position.lots || 0;
            document.getElementById('pos-avg-price').textContent = state.position.avgPrice ? state.position.avgPrice.toFixed(2) : '--';
        }}

        // 更新按钮状态
        function updateButtons() {{
            const hasPos = state.position.direction !== null;
            document.getElementById('btn-long').disabled = state.position.direction === 'short';
            document.getElementById('btn-short').disabled = state.position.direction === 'long';
            document.getElementById('btn-close').disabled = !hasPos;
            document.getElementById('btn-close-all').disabled = !hasPos;
        }}

        function showSettleOverlay(profit, rate) {{
            const overlay = document.getElementById('settle-overlay');
            document.getElementById('settle-symbol-name').textContent = CONFIG.symbolName;
            document.getElementById('settle-symbol-code').textContent = CONFIG.symbol;
            const profitEl = document.getElementById('settle-profit');
            const rateEl = document.getElementById('settle-rate');
            profitEl.textContent = (profit >= 0 ? '+' : '') + Math.round(profit).toLocaleString();
            rateEl.textContent = (rate >= 0 ? '+' : '') + (rate * 100).toFixed(2) + '%';
            profitEl.className = 'settle-value ' + (profit >= 0 ? 'settle-profit' : 'settle-loss');
            rateEl.className = 'settle-value ' + (rate >= 0 ? 'settle-profit' : 'settle-loss');
            overlay.style.display = 'flex';
        }}

        // 结束游戏
        function endGame() {{
            state.running = false;
            state.ending = true;
            if (state.position.direction) closeAll();

            const profit = state.pnl.realized;
            const rate = profit / CONFIG.capital;

            showSettleOverlay(profit, rate);

            setTimeout(() => {{
                const p = new URLSearchParams({{
                    game_done: '1', profit: Math.round(profit), rate: rate.toFixed(4),
                    trades: state.trades, drawdown: state.pnl.maxDD.toFixed(4),
                    game_id: CONFIG.gameId, symbol: CONFIG.symbol,
                    symbol_name: CONFIG.symbolName, symbol_type: CONFIG.symbolType,
                    capital: CONFIG.capital
                }});
                location.href = '?' + p.toString();
            }}, 1500);
        }}

        function confirmEnd() {{
            if (confirm('确认结束？将自动平仓结算。')) endGame();
        }}

        window.addEventListener('resize', () => {{
            if (chart) {{
                const el = document.getElementById('chart');
                chart.applyOptions({{ width: el.clientWidth, height: el.clientHeight }});
            }}
        }});

        document.addEventListener('DOMContentLoaded', initChart);
        updateLotButtons();
    </script>
</body>
</html>
'''

    components.html(trading_html, height=750, scrolling=False)
