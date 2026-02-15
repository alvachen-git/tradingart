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
from html import escape

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import kline_game as kg
import auth_utils as auth
import extra_streamlit_components as stx

TRADE_API_URL = kg.get_trade_batch_api_url(prefer_same_domain=True, fallback_port=8765)

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
    [data-testid="stAppViewContainer"] { background: #0b1121 !important; }
    [data-testid="stHeader"] {
        background: transparent !important;
        box-shadow: none !important;
        border-bottom: 0 !important;
    }
    .block-container { padding: 0.5rem 1rem !important; max-width: 100% !important; }
    #MainMenu, footer, [data-testid="stDecoration"] { display: none !important; }
    [data-testid="stSidebar"] { background-color: #0f172a !important; }
    [data-testid="stSidebar"] p, [data-testid="stSidebar"] span, [data-testid="stSidebar"] div {
        color: #cbd5e1 !important;
    }
    [data-testid="stSidebarNav"] a {
        border-radius: 8px !important;
        margin: 2px 8px !important;
    }
    [data-testid="stSidebarNav"] a[data-selected="true"] {
        background: #334155 !important;
    }
    /* 左上角侧栏开关：高对比，参考 Home.py */
    button[data-testid="stExpandSidebarButton"],
    [data-testid="stSidebarCollapsedControl"] button,
    [data-testid="stSidebarHeader"] button[data-testid="stBaseButton-headerNoPadding"] {
        width: 36px !important;
        height: 36px !important;
        min-width: 36px !important;
        min-height: 36px !important;
        background: #2563eb !important;
        border: 2px solid rgba(255, 255, 255, 0.92) !important;
        border-radius: 10px !important;
        box-shadow: 0 6px 16px rgba(2, 6, 23, 0.65) !important;
        opacity: 1 !important;
    }
    button[data-testid="stExpandSidebarButton"],
    [data-testid="stSidebarCollapsedControl"] button {
        position: fixed !important;
        top: 14px !important;
        left: 14px !important;
        z-index: 999997 !important;
    }
    button[data-testid="stExpandSidebarButton"] *,
    [data-testid="stSidebarCollapsedControl"] button *,
    [data-testid="stSidebarHeader"] button[data-testid="stBaseButton-headerNoPadding"] * {
        color: #ffffff !important;
        fill: #ffffff !important;
        stroke: #ffffff !important;
        opacity: 1 !important;
        font-weight: 800 !important;
        text-shadow: 0 1px 2px rgba(0, 0, 0, 0.55) !important;
    }
    .stButton > button {
        background: linear-gradient(135deg, #3b82f6, #1d4ed8) !important;
        color: white !important; border: none !important;
        padding: 12px 24px !important; border-radius: 8px !important;
    }
    [data-testid="stSelectbox"] label p {
        color: #cbd5e1 !important;
        font-weight: 700 !important;
        font-size: 14px !important;
    }
    [data-baseweb="select"] > div {
        background: #f8fafc !important;
        border: 1px solid #334155 !important;
    }
    [data-baseweb="select"] div,
    [data-baseweb="select"] span,
    [data-baseweb="select"] input {
        color: #0f172a !important;
        -webkit-text-fill-color: #0f172a !important;
        font-weight: 600 !important;
    }
    [data-baseweb="select"] svg {
        color: #334155 !important;
        fill: #334155 !important;
    }
    .game-setup-card {
        background: linear-gradient(135deg, #1a1f2e, #2a3441);
        border: 2px solid #3b4252; border-radius: 16px;
        padding: 24px; margin: 16px 0;
    }
    .game-guide-card {
        margin-top: 14px;
        padding: 16px;
        border-radius: 12px;
        border: 1px solid #24344d;
        background: linear-gradient(145deg, rgba(15,23,42,.9), rgba(30,41,59,.72));
    }
    .game-guide-title {
        color: #e2e8f0;
        font-size: 18px;
        font-weight: 700;
        margin-bottom: 10px;
    }
    .game-guide-grid {
        display: grid;
        grid-template-columns: repeat(3, minmax(0, 1fr));
        gap: 10px;
    }
    .guide-item {
        border: 1px solid #334155;
        border-radius: 10px;
        padding: 10px 12px;
        background: rgba(2, 6, 23, .55);
    }
    .guide-item b {
        display: block;
        color: #f8fafc;
        margin-bottom: 4px;
    }
    .guide-item span {
        color: #94a3b8;
        font-size: 13px;
        line-height: 1.5;
    }
    .kline-loading-wrap {
        margin: 14px 0 8px;
        padding: 16px;
        border-radius: 12px;
        border: 1px solid #34538a;
        background: linear-gradient(145deg, rgba(15,23,42,.92), rgba(37,99,235,.18));
        text-align: center;
    }
    .kline-loading-spinner {
        width: 34px;
        height: 34px;
        margin: 0 auto 10px;
        border-radius: 50%;
        border: 3px solid rgba(148,163,184,.28);
        border-top-color: #60a5fa;
        animation: kline-spin 0.9s linear infinite;
    }
    .kline-loading-title {
        color: #e2e8f0;
        font-size: 15px;
        font-weight: 700;
    }
    .kline-loading-sub {
        margin-top: 4px;
        color: #93c5fd;
        font-size: 12px;
    }
    .kline-loading-bars {
        margin-top: 10px;
        display: flex;
        justify-content: center;
        gap: 5px;
    }
    .kline-loading-bars span {
        width: 6px;
        height: 16px;
        border-radius: 8px;
        background: #60a5fa;
        animation: kline-bar 0.9s ease-in-out infinite;
    }
    .kline-loading-bars span:nth-child(2) { animation-delay: 0.12s; }
    .kline-loading-bars span:nth-child(3) { animation-delay: 0.24s; }
    .kline-loading-bars span:nth-child(4) { animation-delay: 0.36s; }
    .lb-wrap {
        margin-top: 14px;
        padding: 16px 16px 10px;
        border-radius: 14px;
        border: 1px solid #35537d;
        background:
            radial-gradient(circle at 12% -6%, rgba(250,204,21,.14), rgba(15,23,42,0) 36%),
            radial-gradient(circle at 92% 8%, rgba(59,130,246,.18), rgba(15,23,42,0) 38%),
            linear-gradient(145deg, rgba(12,19,37,.96), rgba(8,14,30,.97));
        box-shadow: inset 0 1px 0 rgba(255,255,255,.06), 0 18px 40px rgba(2,6,23,.35);
    }
    .lb-title {
        color: #f8fafc;
        font-weight: 800;
        font-size: 20px;
        margin-bottom: 2px;
        letter-spacing: .3px;
    }
    .lb-subtitle {
        color: #9fb4d6;
        font-size: 13px;
        margin-bottom: 12px;
    }
    [data-testid="stRadio"] > div {
        background: rgba(8,16,35,.78);
        border: 1px solid #2f456a;
        border-radius: 12px;
        padding: 6px;
        gap: 8px;
    }
    [data-testid="stRadio"] label[data-baseweb="radio"] {
        margin: 0 !important;
        padding: 8px 14px !important;
        min-height: 40px;
        border-radius: 999px;
        border: 1px solid transparent !important;
        background: transparent;
        transition: all .16s ease;
    }
    [data-testid="stRadio"] label[data-baseweb="radio"] > div:first-of-type {
        display: none !important;
    }
    [data-testid="stRadio"] label[data-baseweb="radio"] p {
        color: #c9d6ee !important;
        font-size: 16px !important;
        font-weight: 800 !important;
        letter-spacing: .2px;
        line-height: 1.2;
    }
    [data-testid="stRadio"] label[data-baseweb="radio"]:has(input:checked) {
        background: linear-gradient(135deg, rgba(245,158,11,.25), rgba(59,130,246,.30)) !important;
        border-color: #5b7fb0 !important;
        box-shadow: inset 0 1px 0 rgba(255,255,255,.08), 0 6px 16px rgba(15,23,42,.45);
    }
    [data-testid="stRadio"] label[data-baseweb="radio"]:has(input:checked) p {
        color: #f8fafc !important;
    }
    [data-testid="stRadio"] label[data-baseweb="radio"]:hover p {
        color: #eaf1ff !important;
    }
    .lb-board {
        margin-top: 10px;
        border: 1px solid #385174;
        border-radius: 12px;
        overflow: hidden;
        background:
            radial-gradient(circle at 100% 0%, rgba(56,189,248,.08), rgba(11,17,33,0) 34%),
            linear-gradient(180deg, rgba(9,16,32,.95), rgba(12,20,38,.95));
        box-shadow: inset 0 1px 0 rgba(255,255,255,.05), 0 12px 24px rgba(2,6,23,.24);
    }
    .lb-row {
        display: grid;
        grid-template-columns: 1fr 2fr 2fr;
        align-items: center;
        min-height: 52px;
    }
    .lb-row.lb-head {
        background: linear-gradient(90deg, rgba(245,158,11,.16), rgba(59,130,246,.14));
        border-bottom: 1px solid #365177;
        font-weight: 800;
        color: #e2edff;
        letter-spacing: .2px;
    }
    .lb-row.lb-item {
        border-bottom: 1px solid rgba(71,85,105,.35);
    }
    .lb-row.lb-item:nth-child(even) {
        background: rgba(51, 65, 85, .2);
    }
    .lb-row.lb-item.top1 {
        background: linear-gradient(90deg, rgba(245,158,11,.14), rgba(30,41,59,.35));
    }
    .lb-row.lb-item.top2 {
        background: linear-gradient(90deg, rgba(148,163,184,.12), rgba(30,41,59,.32));
    }
    .lb-row.lb-item.top3 {
        background: linear-gradient(90deg, rgba(251,146,60,.10), rgba(30,41,59,.28));
    }
    .lb-row.lb-item:last-child {
        border-bottom: none;
    }
    .lb-col {
        text-align: center;
        justify-self: center;
        font-size: 14px;
        color: #e8eefc;
        font-weight: 700;
    }
    .lb-rank.top3 {
        color: #fef3c7;
        font-weight: 800;
        text-shadow: 0 0 12px rgba(245,158,11,.25);
    }
    .lb-value.profit-pos { color: #34d399; }
    .lb-value.profit-neg { color: #fb7185; }
    .lb-empty {
        text-align: center;
        color: #94a3b8;
        padding: 24px 8px;
        border: 1px dashed #334155;
        border-radius: 12px;
    }
    @keyframes kline-spin {
        from { transform: rotate(0deg); }
        to { transform: rotate(360deg); }
    }
    @keyframes kline-bar {
        0%, 100% { transform: scaleY(0.7); opacity: 0.65; }
        50% { transform: scaleY(1.35); opacity: 1; }
    }
    @media (max-width: 768px) {
        .game-guide-grid { grid-template-columns: 1fr; }
        [data-testid="stSelectbox"] label p {
            color: #e2e8f0 !important;
            font-size: 17px !important;
            font-weight: 800 !important;
        }
        [data-baseweb="select"] > div {
            min-height: 52px !important;
        }
        [data-baseweb="select"] div,
        [data-baseweb="select"] span,
        [data-baseweb="select"] input {
            font-size: 20px !important;
            color: #0f172a !important;
            -webkit-text-fill-color: #0f172a !important;
        }
        [data-baseweb="popover"] [role="listbox"] * {
            color: #0f172a !important;
            -webkit-text-fill-color: #0f172a !important;
            font-size: 18px !important;
        }
    }
</style>
""", unsafe_allow_html=True)


def render_loading_block():
    return """
    <div class="kline-loading-wrap">
        <div class="kline-loading-spinner"></div>
        <div class="kline-loading-title">正在加载K线，请稍候...</div>
        <div class="kline-loading-sub">系统正在随机抽取标的并准备历史走势</div>
        <div class="kline-loading-bars"><span></span><span></span><span></span><span></span></div>
    </div>
    """


def render_settlement_processing_block():
    return """
    <div class="kline-loading-wrap" style="min-height:45vh;">
        <div class="kline-loading-spinner"></div>
        <div class="kline-loading-title">正在结算本局结果...</div>
        <div class="kline-loading-sub">系统正在保存交易记录并生成结算页</div>
        <div class="kline-loading-bars"><span></span><span></span><span></span><span></span></div>
    </div>
    """


@st.cache_data(ttl=60, show_spinner=False)
def load_entry_leaderboards():
    return kg.get_training_entry_leaderboards(limit=20, min_completed=2)


def _format_lb_value(board_key, value):
    try:
        v = float(value or 0)
    except Exception:
        v = 0.0
    if board_key == "capital":
        return f"{int(round(v)):,.0f}"
    if board_key in ("profit", "max_profit"):
        return f"{'+' if v >= 0 else ''}{int(round(v)):,.0f}"
    return f"{int(round(v))}"


def _render_lb_board(board_key, rows):
    if not rows:
        st.markdown('<div class="lb-empty">暂无符合入榜门槛（完成游戏 > 1）的数据</div>', unsafe_allow_html=True)
        return

    medal = {1: "🥇", 2: "🥈", 3: "🥉"}
    item_html = []
    for i, r in enumerate(rows[:20], start=1):
        user = escape(str(r.get("user_id", "-")))
        raw = float(r.get("value", 0) or 0)
        value = _format_lb_value(board_key, raw)
        rank_text = f"{medal.get(i, '')}{i}" if i <= 3 else str(i)
        rank_cls = "lb-rank top3" if i <= 3 else "lb-rank"
        row_cls = f"top{i}" if i <= 3 else ""
        val_cls = "lb-value"
        if board_key in ("profit", "max_profit"):
            val_cls += " profit-pos" if raw >= 0 else " profit-neg"
        item_html.append(
            f'<div class="lb-row lb-item {row_cls}">'
            f'<div class="lb-col {rank_cls}">{rank_text}</div>'
            f'<div class="lb-col">{user}</div>'
            f'<div class="lb-col {val_cls}">{value}</div>'
            '</div>'
        )

    board_html = (
        '<div class="lb-board">'
        '<div class="lb-row lb-head">'
        '<div class="lb-col">排名</div>'
        '<div class="lb-col">玩家</div>'
        '<div class="lb-col">数据</div>'
        '</div>'
        + ''.join(item_html) +
        '</div>'
    )
    st.markdown(
        board_html,
        unsafe_allow_html=True,
    )


def render_entry_leaderboards():
    data = load_entry_leaderboards() or {}
    board_specs = {
        "总资金排行": "capital",
        "单局最大盈利排行": "max_profit",
        "连胜排行": "streak",
    }
    if st.session_state.get("lb_board_type") not in board_specs:
        st.session_state["lb_board_type"] = "总资金排行"

    st.markdown(
        """
        <div class="lb-wrap">
            <div class="lb-title">🏆 玩家排行榜（Top20）</div>
            <div class="lb-subtitle">仅展示完成局数 &gt; 1 的玩家，数据每 60 秒刷新一次</div>
        </div>
        """,
        unsafe_allow_html=True,
    )
    selected_label = st.radio(
        "排行榜分类",
        list(board_specs.keys()),
        horizontal=True,
        label_visibility="collapsed",
        key="lb_board_type",
    )
    board_key = board_specs.get(selected_label, "capital")
    _render_lb_board(board_key, data.get(board_key, []))


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
if 'kline_cookie_retry_once' not in st.session_state:
    st.session_state['kline_cookie_retry_once'] = False
if 'settlement_pending' not in st.session_state:
    st.session_state['settlement_pending'] = None
if 'settlement_view' not in st.session_state:
    st.session_state['settlement_view'] = None

# 恢复登录
if not st.session_state.get('is_logged_in'):
    try:
        cookies = cookie_manager.get_all() or {}
        restored = auth.restore_login_from_cookies(cookies)
        if not restored and not cookies and not st.session_state.get("kline_cookie_retry_once", False):
            st.session_state["kline_cookie_retry_once"] = True
            time.sleep(0.15)
            st.rerun()
        elif not restored and (cookies.get("username") or cookies.get("token")):
            cookie_manager.delete("username", key="kline_del_user")
            cookie_manager.delete("token", key="kline_del_token")
    except:
        pass

# 侧边栏
with st.sidebar:
    st.markdown("### 🎮 K线训练场")
    if st.session_state.get('is_logged_in'):
        st.success(f"👤 {st.session_state.get('user_id')}")

# 🔧 处理游戏结果：先把 query 参数落到 session，避免被 rerun 清空后页面闪退
game_done = st.query_params.get('game_done', '')
if game_done == '1':
    try:
        st.session_state['settlement_pending'] = {
            'from_iframe': st.query_params.get('from_iframe', '') == '1',
            'profit': float(st.query_params.get('profit', '0') or '0'),
            'profit_rate': float(st.query_params.get('rate', '0') or '0'),
            'trade_count': int(st.query_params.get('trades', '0') or '0'),
            'max_drawdown': float(st.query_params.get('drawdown', '0') or '0'),
            'leverage_used': int(float(st.query_params.get('leverage', '1') or '1')),
            'game_id': int(st.query_params.get('game_id', '0') or '0') or None,
            'symbol': st.query_params.get('symbol', ''),
            'symbol_name': st.query_params.get('symbol_name', '未知'),
            'symbol_type': st.query_params.get('symbol_type', 'stock'),
            'capital_before': int(float(st.query_params.get('capital', '1000000') or '1000000')),
        }
    except Exception:
        st.session_state['settlement_pending'] = None
    st.query_params.clear()
    st.rerun()

# 处理并持久化结算结果（只做一次）
if st.session_state.get('settlement_pending') and not st.session_state.get('settlement_view'):
    st.markdown(render_settlement_processing_block(), unsafe_allow_html=True)
    pending = st.session_state['settlement_pending']
    from_iframe = pending.get('from_iframe', False)
    profit = float(pending.get('profit', 0))
    profit_rate = float(pending.get('profit_rate', 0))
    trade_count = int(pending.get('trade_count', 0))
    max_drawdown = float(pending.get('max_drawdown', 0))
    leverage_used = int(pending.get('leverage_used', 1))
    game_id = pending.get('game_id')
    symbol = pending.get('symbol', '')
    symbol_name = pending.get('symbol_name', '未知')
    symbol_type = pending.get('symbol_type', 'stock')
    capital_before = int(pending.get('capital_before', 1000000))

    st.session_state['game_started'] = False
    if 'game_data' in st.session_state:
        del st.session_state['game_data']
    st.session_state['just_finished_game_id'] = game_id

    new_achievements = []
    try:
        if game_id:
            game_info = kg.get_game_info(game_id)
            if game_info:
                settle_result = kg.end_game(
                    game_id, game_info['user_id'], 'finished', 'completed',
                    profit, profit_rate, capital_before + profit, trade_count, max_drawdown
                )
                if settle_result and settle_result.get("ok"):
                    profit = float(settle_result.get("profit", profit))
                    profit_rate = float(settle_result.get("profit_rate", profit_rate))
                    trade_count = int(settle_result.get("trade_count", trade_count))
                    new_achievements = kg.check_achievements(
                        user_id=game_info['user_id'],
                        game_profit=profit,
                        profit_rate=profit_rate,
                        trade_count=trade_count,
                        max_drawdown=max_drawdown,
                        leverage=leverage_used
                    ) or []
    except Exception as e:
        print(f"结算游戏失败: {e}")

    st.session_state['settlement_view'] = {
        'from_iframe': from_iframe,
        'profit': profit,
        'profit_rate': profit_rate,
        'trade_count': trade_count,
        'symbol': symbol,
        'symbol_name': symbol_name,
        'symbol_type': symbol_type,
        'new_achievements': new_achievements,
    }
    st.session_state['settlement_pending'] = None
    # 避免“处理中动画”和“结算页”出现在同一轮渲染中
    st.rerun()

# 展示结算页（手动确认后才离开）
if st.session_state.get('settlement_view'):
    view = st.session_state['settlement_view']
    from_iframe = view.get('from_iframe', False)
    profit = float(view.get('profit', 0))
    profit_rate = float(view.get('profit_rate', 0))
    trade_count = int(view.get('trade_count', 0))
    symbol = view.get('symbol', '')
    symbol_name = view.get('symbol_name', '未知')
    new_achievements = view.get('new_achievements') or []

    st.markdown("<h1 style='text-align:center;color:#e5e7eb;'>🎯 游戏结束</h1>", unsafe_allow_html=True)
    col1, col2, col3 = st.columns([1, 2, 1])
    with col2:
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
        if new_achievements:
            items = "".join(
                f"<li style='margin:6px 0;color:#e2e8f0;'>🏆 <b>{a['name']}</b>：{a.get('desc','')}（+{int(a.get('exp',0))} EXP）</li>"
                for a in new_achievements
            )
            st.markdown(f"""
            <div class="game-guide-card" style="margin-top:12px;">
                <div class="game-guide-title">🎉 本局解锁成就</div>
                <ul style="margin:0;padding-left:18px;">{items}</ul>
            </div>
            """, unsafe_allow_html=True)
        else:
            st.caption("本局暂无新成就解锁。")

        if from_iframe:
            st.info("本局已完成结算，请点击左侧「K线训练」返回主页面继续。")
        else:
            if st.button("🎮 再来一局", type="primary", use_container_width=True):
                st.session_state['settlement_view'] = None
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
            speed = st.selectbox("播放速度", ["1秒/根", "3秒/根", "5秒/根"], index=0)
            speed_ms = {"1秒/根": 1000, "3秒/根": 3000, "5秒/根": 5000}[speed]
            speed_sec = {"1秒/根": 1, "3秒/根": 3, "5秒/根": 5}[speed]
        with col_b:
            leverage = st.selectbox("杠杆倍数", ["1倍", "10倍"], index=0)
            leverage_val = {"1倍": 1, "10倍": 10}[leverage]

        if st.button("🎮 开始游戏", type="primary", use_container_width=True):
            loading_placeholder = st.empty()
            loading_placeholder.markdown(render_loading_block(), unsafe_allow_html=True)
            with st.spinner("加载K线数据..."):
                symbol, symbol_name, symbol_type, df = kg.get_random_kline_data(bars=100, history_bars=60)
                if df is None or len(df) < 160:
                    loading_placeholder.empty()
                    st.error("数据加载失败")
                    st.stop()

                def _safe_float(v):
                    try:
                        f = float(v)
                    except (TypeError, ValueError):
                        return None
                    return f if pd.notna(f) else None

                # 过滤无效K线，避免前端图表因 NaN/None 直接报错后整页不渲染
                kline_data = []
                for _, r in df.iterrows():
                    o = _safe_float(r.get('open_price'))
                    h = _safe_float(r.get('high_price'))
                    l = _safe_float(r.get('low_price'))
                    c = _safe_float(r.get('close_price'))
                    if None in (o, h, l, c):
                        continue
                    trade_date = r.get('trade_date') if 'trade_date' in r else None
                    if trade_date is None and hasattr(r, "name"):
                        trade_date = r.name
                    if isinstance(trade_date, pd.Timestamp):
                        trade_date = trade_date.strftime("%Y-%m-%d")
                    elif isinstance(trade_date, datetime):
                        trade_date = trade_date.strftime("%Y-%m-%d")
                    elif trade_date is not None:
                        trade_date = str(trade_date)[:10]
                    kline_data.append({'open': o, 'high': h, 'low': l, 'close': c, 'date': trade_date})

                if len(kline_data) < 160:
                    loading_placeholder.empty()
                    st.error("K线数据质量不足，请重试")
                    st.stop()

                game_id = kg.start_game(user_id, symbol, symbol_name, symbol_type, user_capital, leverage_val, speed_sec)
                if not game_id:
                    loading_placeholder.empty()
                    st.error("游戏创建失败")
                    st.stop()

                st.session_state['game_started'] = True
                st.session_state['game_data'] = {
                    'kline_data': kline_data,
                    'config': {'symbol': symbol, 'symbolName': symbol_name, 'symbolType': symbol_type,
                               'capital': user_capital, 'leverage': leverage_val, 'speed': speed_ms,
                               'gameId': game_id, 'userId': user_id, 'tradeApiUrl': TRADE_API_URL or ''}
                }
                loading_placeholder.empty()
                st.rerun()

        st.markdown("""
        <div class="game-guide-card">
            <div class="game-guide-title">🎯 游戏说明</div>
            <div class="game-guide-grid">
                <div class="guide-item"><b>1. 客观数据</b><span>本游戏采用真实市场的历史交易价格，每场游戏是100根日K线。</span></div>
                <div class="guide-item"><b>2. K线为主</b><span>游戏主要训练K线交易，没有任何指标提供。</span></div>
                <div class="guide-item"><b>3. 中离惩罚</b><span>如果没有正常结算游戏，会被处罚扣资金5万。</span></div>
            </div>
        </div>
        """, unsafe_allow_html=True)

        render_entry_leaderboards()

        with st.expander("📋 交易规则（展开查看）"):
            st.markdown("""
            - **每手 = 1,000元**，可自由选择加仓手数
            - 支持**做多**和**做空**
            - 可以多次**加仓**，累计持仓
            - 可以选择**平仓手数**或一键全平
            - 杠杆放大盈亏，注意风险
            - 选择 **10倍杠杆** 时，总开仓保证金不超过当前总资金的 **50%**
            """)
    st.stop()

# ==========================================
# 游戏界面
# ==========================================
if st.session_state.get('game_started') and 'game_data' in st.session_state:
    game_data = st.session_state['game_data']
    kline_data = game_data['kline_data']
    config = game_data['config']

    config_json = json.dumps(config, ensure_ascii=False)
    kline_json = json.dumps(kline_data, ensure_ascii=False)

    # 🔧 【修复3】K线颜色改为中国标准：红涨绿跌
    trading_html = f'''
<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
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
            gap: 12px;
            flex: 1;
            justify-content: flex-start;
        }}
        .action-group {{
            display: flex;
            gap: 12px;
        }}
        .action-btn {{
            min-width: 92px;
            height: 48px;
            padding: 0 18px;
            border: none;
            border-radius: 10px;
            font-size: 16px;
            font-weight: 700;
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

        @media (max-width: 768px) {{
            .trade-panel {{
                padding: 10px 10px 14px;
            }}
            .panel-row {{
                flex-wrap: wrap;
                gap: 12px;
            }}
            .account-info,
            .position-info {{
                width: 100%;
                border-right: none;
                padding: 0;
                justify-content: space-between;
            }}
            .trade-controls {{
                width: 100%;
                flex-direction: column;
                align-items: stretch;
                gap: 10px;
            }}
            .lot-selector {{
                width: 100%;
                justify-content: space-between;
                flex-wrap: wrap;
            }}
            .action-buttons {{
                width: 100%;
                display: grid;
                grid-template-columns: 1fr 1fr;
                gap: 10px;
            }}
            .action-group {{
                display: grid;
                grid-template-rows: 1fr 1fr;
                gap: 10px;
            }}
            .action-btn {{
                width: 100%;
                min-width: 0;
                height: 50px;
                font-size: 17px;
            }}
            .end-section {{
                width: 100%;
                margin-left: 0;
            }}
            .end-btn {{
                width: 100%;
                height: 46px;
                font-size: 16px;
            }}
        }}

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
        .settle-btn {{
            margin-top: 14px;
            width: 100%;
            padding: 10px 12px;
            border: none;
            border-radius: 10px;
            background: linear-gradient(135deg, #3b82f6, #1d4ed8);
            color: #fff;
            font-size: 16px;
            font-weight: 700;
            cursor: pointer;
        }}
        .settle-btn:disabled {{
            opacity: 0.7;
            cursor: not-allowed;
        }}
        .settle-actions {{
            margin-top: 14px;
            display: grid;
            grid-template-columns: 1fr 1fr;
            gap: 10px;
        }}
        .settle-actions .settle-btn {{
            margin-top: 0;
        }}
        .settle-btn.secondary {{
            background: #1e293b;
            border: 1px solid #334155;
            color: #e2e8f0;
        }}
        .settle-hint {{
            margin-top: 10px;
            color: #94a3b8;
            font-size: 12px;
        }}
        .settle-manual {{
            margin-top: 10px;
            width: 100%;
            padding: 9px 12px;
            border-radius: 10px;
            border: 1px dashed #475569;
            background: rgba(30, 41, 59, 0.6);
            color: #cbd5e1;
            font-size: 14px;
            font-weight: 600;
            cursor: pointer;
            display: none;
        }}
        .settle-wait-mask {{
            position: absolute;
            inset: 0;
            background: rgba(2, 6, 23, 0.86);
            border-radius: 18px;
            display: none;
            align-items: center;
            justify-content: center;
            z-index: 10;
        }}
        .settle-wait-box {{
            display: flex;
            flex-direction: column;
            align-items: center;
            gap: 10px;
            color: #e2e8f0;
            font-weight: 600;
        }}
        .settle-wait-spinner {{
            width: 30px;
            height: 30px;
            border-radius: 50%;
            border: 3px solid rgba(96, 165, 250, 0.25);
            border-top-color: #60a5fa;
            animation: settleSpin 0.8s linear infinite;
        }}
        @keyframes settleSpin {{
            to {{ transform: rotate(360deg); }}
        }}
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
                    <div class="action-group action-group-open">
                        <button id="btn-long" class="action-btn btn-long" onclick="openPosition('long')">做多</button>
                        <button id="btn-short" class="action-btn btn-short" onclick="openPosition('short')">做空</button>
                    </div>
                    <div class="action-group action-group-close">
                        <button id="btn-close" class="action-btn btn-close" onclick="closePosition()" disabled>平仓</button>
                        <button id="btn-close-all" class="action-btn btn-close-all" onclick="closeAll()" disabled>全平</button>
                    </div>
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
            <div class="settle-wait-mask" id="settle-wait-mask">
                <div class="settle-wait-box">
                    <div class="settle-wait-spinner"></div>
                    <div id="settle-wait-text">正在提交结算...</div>
                </div>
            </div>
            <div class="settle-title">结算中</div>
            <div class="settle-symbol" id="settle-comment-title">--</div>
            <div class="settle-sub" id="settle-comment-sub">--</div>
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
            <div class="settle-actions">
                <button class="settle-btn secondary" id="settle-review-btn" onclick="reviewAfterSettle()">回顾K线</button>
                <button class="settle-btn" id="settle-confirm-btn" onclick="confirmSettleEnd()">确认结束</button>
            </div>
            <div class="settle-hint">请确认本局结果后，点击按钮结束</div>
            <button class="settle-manual" id="settle-manual-btn" onclick="manualSettleNavigate()">手动返回入口页</button>
        </div>
    </div>

    <script>
        const CONFIG = {config_json};
        CONFIG.lotSize = 1000;

        const KLINE = {kline_json};
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

        let chart, candles, playTimer = null;
        let tradeMarkers = [];
        let settleQuery = '';
        let settleSummary = null;
        let tradeEvents = [];
        let tradePersistPromise = null;
        let tradePersisted = false;

        function clonePosition(pos) {{
            return {{
                direction: pos?.direction || null,
                lots: Number(pos?.lots || 0),
                avgPrice: Number(pos?.avgPrice || 0),
                totalCost: Number(pos?.totalCost || 0),
            }};
        }}

        function resolveTradeApiUrl() {{
            const configured = String(CONFIG.tradeApiUrl || '').trim();
            if (configured) return configured;
            return '/api/kline/trades/batch';
        }}

        function pushTradeEvent(action, lots, price, positionBefore) {{
            if (!lots || lots <= 0) return;
            const bar = getCurrentBar() || {{}};
            const event = {{
                trade_seq: tradeEvents.length + 1,
                action: action,
                trade_time: new Date().toISOString(),
                bar_index: state.bar,
                bar_date: bar.date || null,
                price: Number(price || 0),
                lots: Number(lots || 0),
                amount: Number((lots || 0) * (CONFIG.lotSize || 1000)),
                leverage: Number(CONFIG.leverage || 1),
                symbol: CONFIG.symbol,
                symbol_name: CONFIG.symbolName,
                symbol_type: CONFIG.symbolType,
                position_before: clonePosition(positionBefore),
                position_after: clonePosition(state.position),
                realized_pnl_after: Number(state.pnl.realized || 0),
                floating_pnl_after: Number(state.pnl.floating || 0),
            }};
            tradeEvents.push(event);
        }}

        async function persistTradesOnce() {{
            if (tradePersisted) return {{ ok: true, already: true }};
            if (tradePersistPromise) return tradePersistPromise;

            const apiUrl = resolveTradeApiUrl();
            if (!apiUrl) return {{ ok: false, message: 'trade api unavailable' }};

            const payload = {{
                game_id: Number(CONFIG.gameId || 0),
                user_id: String(CONFIG.userId || ''),
                symbol: CONFIG.symbol,
                symbol_name: CONFIG.symbolName,
                symbol_type: CONFIG.symbolType,
                trades: tradeEvents
            }};

            tradePersistPromise = fetch(apiUrl, {{
                method: 'POST',
                headers: {{ 'Content-Type': 'application/json' }},
                body: JSON.stringify(payload)
            }}).then(async (res) => {{
                let body = {{}};
                try {{
                    body = await res.json();
                }} catch (e) {{
                    body = {{ ok: false, message: 'invalid api response' }};
                }}
                if (res.ok && body.ok) {{
                    tradePersisted = true;
                    return {{ ok: true, body }};
                }}
                return {{ ok: false, status: res.status, body }};
            }}).catch((err) => {{
                return {{ ok: false, message: String(err) }};
            }}).finally(() => {{
                tradePersistPromise = null;
            }});

            return tradePersistPromise;
        }}

        function upsertTradeMarker(type, lots) {{
            if (!candles) return;
            const barIdx = state.bar;
            const markerKey = `${{barIdx}}_${{type}}`;
            const markerMap = {{
                open_long:  {{ position: 'belowBar', color: '#ef4444', shape: 'arrowUp', baseText: '做多' }},
                open_short: {{ position: 'aboveBar', color: '#22c55e', shape: 'arrowDown', baseText: '做空' }},
                close_long: {{ position: 'aboveBar', color: '#60a5fa', shape: 'circle', baseText: '平多' }},
                close_short: {{ position: 'belowBar', color: '#a78bfa', shape: 'circle', baseText: '平空' }}
            }};
            const m = markerMap[type];
            if (!m) return;

            const existing = tradeMarkers.find(x => x.id === markerKey);
            if (existing) {{
                existing.lots += lots;
                existing.text = `${{m.baseText}} ${{existing.lots}}手`;
            }} else {{
                tradeMarkers.push({{
                    id: markerKey,
                    time: barIdx,
                    position: m.position,
                    color: m.color,
                    shape: m.shape,
                    text: `${{m.baseText}} ${{lots}}手`,
                    lots
                }});
            }}

            const sorted = tradeMarkers
                .slice()
                .sort((a, b) => a.time - b.time)
                .map((x) => ({{
                    time: x.time,
                    position: x.position,
                    color: x.color,
                    shape: x.shape,
                    text: x.text
                }}));
            candles.setMarkers(sorted);
        }}

        async function ensureChartLib() {{
            if (window.LightweightCharts) return true;

            const urls = [
                'https://unpkg.com/lightweight-charts@4.0.1/dist/lightweight-charts.standalone.production.js',
                'https://cdn.jsdelivr.net/npm/lightweight-charts@4.0.1/dist/lightweight-charts.standalone.production.js'
            ];

            for (const url of urls) {{
                const loaded = await new Promise((resolve) => {{
                    const script = document.createElement('script');
                    script.src = url;
                    script.async = true;
                    script.onload = () => resolve(true);
                    script.onerror = () => resolve(false);
                    document.head.appendChild(script);
                }});
                if (loaded && window.LightweightCharts) return true;
            }}
            return false;
        }}

        // 初始化图表
        async function initChart() {{
            const libReady = await ensureChartLib();
            if (!libReady) {{
                alert('图表库加载失败，请刷新后重试');
                return;
            }}

            const el = document.getElementById('chart');
            el.innerHTML = '';
            if (chart) chart.remove();
            tradeMarkers = [];
            tradeEvents = [];
            tradePersistPromise = null;
            tradePersisted = false;

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
            if (state.played >= PLAY) {{ endGame(); return; }}
            state.bar++;
            state.played++;

            const bar = KLINE[state.bar];
            if (!bar) {{ endGame(); return; }}
            candles.update({{ time: state.bar, open: bar.open, high: bar.high, low: bar.low, close: bar.close }});

            calcPnL();
            updateDisplay();
            state.prevPrice = bar.close;

            if (playTimer) clearTimeout(playTimer);
            playTimer = setTimeout(playBar, CONFIG.speed);
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

        function getCurrentBar() {{
            const idx = Math.max(0, Math.min(state.bar, KLINE.length - 1));
            return KLINE[idx];
        }}

        function getCurrentPrice() {{
            const bar = getCurrentBar();
            return bar ? bar.close : state.prevPrice;
        }}

        // 开仓/加仓
        function openPosition(dir) {{
            if (!state.running) return;

            const lots = getLots();
            const price = getCurrentPrice();
            const posBefore = clonePosition(state.position);
            const marginCost = lots * CONFIG.lotSize;
            const leverage = CONFIG.leverage || 1;

            // 检查资金
            if (marginCost > state.cash) {{
                alert('资金不足！需要 ' + marginCost.toLocaleString() + ' 元，可用 ' + state.cash.toLocaleString() + ' 元');
                return;
            }}

            // 风控：10倍杠杆时，总开仓保证金 <= 当前总资金的50%
            if (leverage === 10) {{
                const equity = state.cash + state.position.totalCost;
                const nextTotalMargin = state.position.totalCost + marginCost;
                const maxMargin = equity * 0.5;
                if (nextTotalMargin > maxMargin) {{
                    alert(
                        '10倍杠杆风险限制：总开仓保证金不能超过当前总资金50%。\\n' +
                        '当前上限：' + Math.floor(maxMargin).toLocaleString() + ' 元，' +
                        '本次后将达到：' + Math.floor(nextTotalMargin).toLocaleString() + ' 元'
                    );
                    return;
                }}
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
                    totalCost: marginCost
                }};
            }} else {{
                // 加仓 - 计算新均价
                const oldValue = state.position.lots * state.position.avgPrice;
                const newValue = lots * price;
                const totalLots = state.position.lots + lots;
                state.position.avgPrice = (oldValue + newValue) / totalLots;
                state.position.lots = totalLots;
                state.position.totalCost += marginCost;
            }}

            state.cash -= marginCost;
            state.trades++;
            upsertTradeMarker(dir === 'long' ? 'open_long' : 'open_short', lots);

            calcPnL();
            const action = posBefore.direction ? (dir === 'long' ? 'add_long' : 'add_short') : (dir === 'long' ? 'open_long' : 'open_short');
            pushTradeEvent(action, lots, price, posBefore);
            updateDisplay();
            updateButtons();
        }}

        // 平仓（按手数）
        function closePosition(force=false, lotsOverride=null) {{
            if ((!state.running && !force) || !state.position.direction) return;

            const selectedLots = lotsOverride ?? getLots();
            const lots = Math.min(selectedLots, state.position.lots);
            const price = getCurrentPrice();
            const posBefore = clonePosition(state.position);
            const closingDir = state.position.direction;

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
            upsertTradeMarker(closingDir === 'long' ? 'close_long' : 'close_short', lots);

            // 更新持仓
            state.position.lots -= lots;
            state.position.totalCost -= returnCost;

            if (state.position.lots <= 0) {{
                state.position = {{ direction: null, lots: 0, avgPrice: 0, totalCost: 0 }};
            }}

            calcPnL();
            const fullyClosed = lots >= (posBefore.lots || 0);
            const action = closingDir === 'long'
                ? (fullyClosed ? 'close_long_all' : 'close_long_partial')
                : (fullyClosed ? 'close_short_all' : 'close_short_partial');
            pushTradeEvent(action, lots, price, posBefore);
            updateDisplay();
            updateButtons();
        }}

        // 全部平仓
        function closeAll() {{
            if (!state.position.direction) return;
            closePosition(true, state.position.lots);
        }}

        // 计算浮动盈亏
        function calcPnL() {{
            if (!state.position.direction) {{
                state.pnl.floating = 0;
                return;
            }}

            const price = getCurrentPrice();
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
            const price = getCurrentPrice();
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

            const posValue = state.position.lots * CONFIG.lotSize * (CONFIG.leverage || 1);
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

        function buildSettleComment(profit, rate) {{
            const tradeCount = Number(state.trades || 0);
            const drawdown = Number(state.pnl.maxDD || 0);
            const absProfit = Math.abs(profit || 0);

            if (rate >= 0.5 || profit >= 500000) {{
                return {{ title: 'K线之神降临', sub: '你刚刚把市场当成了乐高，拆完还能原样拼回去。' }};
            }}
            if (profit >= 100000) {{
                return {{ title: '印钞机启动', sub: '每根K线都像在给你发工资，老板都想问你缺不缺同事。' }};
            }}
            if (profit > 0 && drawdown <= 0.02) {{
                return {{ title: '稳如老狗（褒义）', sub: '波动在闹，你在笑；回撤想进门都找不到门铃。' }};
            }}
            if (profit > 0) {{
                return {{ title: '顺风小赚王', sub: '今天是“赚一点点但很开心”模式，节奏感在线。' }};
            }}
            if (profit === 0 && tradeCount <= 1) {{
                return {{ title: '忍者潜行局', sub: '几乎没出手，主打一个“市场先动，我再决定”。' }};
            }}
            if (tradeCount >= 20 && profit <= 0) {{
                return {{ title: '手速型选手', sub: '你把交易台当音游在打了，下一局试试少按几个键。' }};
            }}
            if (absProfit >= 500000) {{
                return {{ title: '过山车VIP', sub: '资金曲线体验了失重感，建议先系好风控安全带。' }};
            }}
            if (profit < 0) {{
                return {{ title: '学费已到账', sub: '市场收了点学费，但知识点已经打包进背包了。' }};
            }}
            return {{ title: '剧情平稳收官', sub: '没有大起大落，属于“导演都夸节奏稳”的一局。' }};
        }}

        function showSettleOverlay(profit, rate) {{
            const overlay = document.getElementById('settle-overlay');
            const waitMask = document.getElementById('settle-wait-mask');
            const comment = buildSettleComment(profit, rate);
            document.getElementById('settle-comment-title').textContent = comment.title;
            document.getElementById('settle-comment-sub').textContent = comment.sub;
            const profitEl = document.getElementById('settle-profit');
            const rateEl = document.getElementById('settle-rate');
            const hint = document.querySelector('.settle-hint');
            const manualBtn = document.getElementById('settle-manual-btn');
            profitEl.textContent = (profit >= 0 ? '+' : '') + Math.round(profit).toLocaleString();
            rateEl.textContent = (rate >= 0 ? '+' : '') + (rate * 100).toFixed(2) + '%';
            profitEl.className = 'settle-value ' + (profit >= 0 ? 'settle-profit' : 'settle-loss');
            rateEl.className = 'settle-value ' + (rate >= 0 ? 'settle-profit' : 'settle-loss');
            document.getElementById('settle-confirm-btn').disabled = false;
            document.getElementById('settle-review-btn').disabled = false;
            if (manualBtn) manualBtn.style.display = 'none';
            if (hint) hint.textContent = '可先回顾K线交易，再确认结束';
            if (waitMask) waitMask.style.display = 'none';
            overlay.style.display = 'flex';
        }}

        function reviewAfterSettle() {{
            const overlay = document.getElementById('settle-overlay');
            overlay.style.display = 'none';
        }}

        function manualSettleNavigate() {{
            if (!settleQuery) return;
            try {{
                window.top.location.href = settleQuery;
            }} catch (e) {{
                try {{
                    window.parent.location.href = settleQuery;
                }} catch (e2) {{
                    window.location.href = settleQuery;
                }}
            }}
        }}

        function navigateToResult(url) {{
            // 优先尝试跳出 iframe，避免结束后出现页面嵌套
            try {{
                const pdoc = window.parent && window.parent.document;
                if (pdoc && pdoc.body) {{
                    const a = pdoc.createElement('a');
                    a.href = url;
                    a.target = '_self';
                    a.style.display = 'none';
                    pdoc.body.appendChild(a);
                    a.click();
                    a.remove();
                }}
            }} catch (e) {{}}

            try {{
                window.top.location.assign(url);
            }} catch (e) {{}}

            try {{
                window.parent.location.assign(url);
            }} catch (e) {{}}
        }}

        async function confirmSettleEnd() {{
            if (!settleQuery) return;
            const btn = document.getElementById('settle-confirm-btn');
            const reviewBtn = document.getElementById('settle-review-btn');
            const hint = document.querySelector('.settle-hint');
            const manualBtn = document.getElementById('settle-manual-btn');
            const waitMask = document.getElementById('settle-wait-mask');
            const waitText = document.getElementById('settle-wait-text');
            btn.disabled = true;
            if (reviewBtn) reviewBtn.disabled = true;
            if (manualBtn) manualBtn.style.display = 'none';
            if (hint) hint.textContent = '正在保存交易明细...';
            if (waitText) waitText.textContent = '正在提交结算...';
            if (waitMask) waitMask.style.display = 'flex';

            const persistResult = await persistTradesOnce();
            if (!persistResult || !persistResult.ok) {{
                const msg = persistResult?.body?.message || persistResult?.message || '未知错误';
                if (hint) hint.textContent = '交易明细保存失败，可重试或继续结束';
                const goOn = confirm('交易明细保存失败（' + msg + '）。\\n是否仍结束本局？');
                if (!goOn) {{
                    btn.disabled = false;
                    if (reviewBtn) reviewBtn.disabled = false;
                    if (waitMask) waitMask.style.display = 'none';
                    return;
                }}
            }}
            if (hint) hint.textContent = '结算提交中...';
            if (waitText) waitText.textContent = '正在跳转结算页...';

            navigateToResult(settleQuery);
            // 不再强制 iframe 内跳转，避免移动端 Safari 触发第三方上下文导致“像是登出”
            setTimeout(() => {{
                const stillHere = !window.location.search.includes('game_done=1');
                if (!stillHere) return;
                if (hint) hint.textContent = '若未自动跳转，请点下方按钮手动返回入口页。';
                if (manualBtn) manualBtn.style.display = 'block';
                btn.disabled = false;
                if (reviewBtn) reviewBtn.disabled = false;
                if (waitText) waitText.textContent = '等待你确认手动返回...';
                if (waitMask) waitMask.style.display = 'none';
            }}, 1200);
        }}

        // 结束游戏
        function endGame() {{
            if (state.ending) {{
                if (settleSummary) showSettleOverlay(settleSummary.profit, settleSummary.rate);
                return;
            }}

            state.running = false;
            state.ending = true;
            if (playTimer) {{
                clearTimeout(playTimer);
                playTimer = null;
            }}
            if (state.position.direction) closeAll();

            const profit = state.pnl.realized;
            const rate = profit / CONFIG.capital;
            settleSummary = {{ profit: profit, rate: rate }};

            showSettleOverlay(profit, rate);

            const p = new URLSearchParams({{
                game_done: '1', profit: Math.round(profit), rate: rate.toFixed(4),
                trades: state.trades, drawdown: state.pnl.maxDD.toFixed(4),
                game_id: CONFIG.gameId, symbol: CONFIG.symbol,
                symbol_name: CONFIG.symbolName, symbol_type: CONFIG.symbolType,
                capital: CONFIG.capital, leverage: CONFIG.leverage
            }});
            settleQuery = '?' + p.toString();
            persistTradesOnce();
        }}

        function confirmEnd() {{
            if (state.ending) {{
                endGame();
                return;
            }}
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
