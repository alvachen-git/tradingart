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
from decimal import Decimal
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


def _json_default(obj):
    """JSON 序列化兜底：兼容 Decimal / datetime / pandas.Timestamp 等类型"""
    if isinstance(obj, Decimal):
        return float(obj)
    if hasattr(obj, "isoformat"):
        return obj.isoformat()
    raise TypeError(f"Object of type {type(obj).__name__} is not JSON serializable")

# 页面配置
st.set_page_config(
    page_title="K线交易训练",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="collapsed"
)



# 🔥 添加统一的侧边栏导航
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from sidebar_navigation import show_navigation
with st.sidebar:
    show_navigation()

# 样式
st.markdown("""
<style>
    :root {
        --sp-1: 8px;
        --sp-2: 12px;
        --sp-3: 16px;
        --sp-4: 24px;
    }
    .stApp { background-color: #0b1121 !important; color: white !important; }
    [data-testid="stAppViewContainer"] { background: #0b1121 !important; }
    [data-testid="stHeader"] {
        background: transparent !important;
        box-shadow: none !important;
        border-bottom: 0 !important;
    }
    .block-container { padding: var(--sp-1) var(--sp-3) !important; max-width: 100% !important; }
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
        padding: 12px 24px !important; border-radius: 10px !important;
        transition: transform .16s ease, box-shadow .16s ease, filter .16s ease, background .16s ease !important;
        box-shadow: 0 8px 18px rgba(37, 99, 235, .28);
    }
    .stButton > button:hover {
        transform: translateY(-1px);
        box-shadow: 0 12px 24px rgba(37, 99, 235, .36);
        filter: brightness(1.04);
    }
    .stButton > button:active {
        transform: translateY(1px) scale(.995);
        box-shadow: 0 4px 10px rgba(37, 99, 235, .25);
        filter: brightness(.96);
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
        margin-top: var(--sp-2);
        padding: var(--sp-3);
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
        gap: var(--sp-1);
    }
    .guide-item {
        border: 1px solid #334155;
        border-radius: 10px;
        padding: var(--sp-1) var(--sp-2);
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
    .entry-hero-wrap {
        margin-top: var(--sp-1);
        border: 1px solid #35537d;
        border-radius: 16px;
        padding: var(--sp-2) var(--sp-3);
        background:
            radial-gradient(circle at 8% -8%, rgba(34, 211, 238, .18), rgba(15,23,42,0) 38%),
            radial-gradient(circle at 95% 15%, rgba(59, 130, 246, .25), rgba(15,23,42,0) 42%),
            linear-gradient(135deg, rgba(8,14,29,.96), rgba(10,18,34,.98));
        box-shadow: inset 0 1px 0 rgba(255,255,255,.07), 0 18px 40px rgba(2,6,23,.35);
        position: relative;
        overflow: hidden;
    }
    .entry-hero-wrap::after {
        content: "";
        position: absolute;
        top: -45%;
        left: -30%;
        width: 30%;
        height: 190%;
        background: linear-gradient(100deg, rgba(255,255,255,0), rgba(191,219,254,.35), rgba(255,255,255,0));
        transform: skewX(-18deg);
        animation: heroSweep 4.8s ease-in-out infinite;
        pointer-events: none;
    }
    .entry-title {
        margin-top: 2px;
        font-size: clamp(30px, 4.4vw, 44px);
        font-weight: 900;
        line-height: 1.06;
        letter-spacing: 1px;
        background: linear-gradient(180deg, #f8fbff 0%, #bcd4ff 55%, #7fb3ff 100%);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        text-shadow: 0 0 24px rgba(59,130,246,.18);
        position: relative;
        z-index: 2;
    }
    .entry-sub {
        margin-top: 6px;
        color: #9fb4d6;
        font-size: 12px;
        line-height: 1.6;
    }
    .entry-unified-panel {
        margin-top: var(--sp-2);
    }
    .entry-capital-mini {
        border: 1px solid #35537d;
        border-radius: 12px;
        padding: var(--sp-2);
        background: linear-gradient(145deg, rgba(18,27,48,.94), rgba(12,20,36,.96));
        min-height: 170px;
        display: flex;
        flex-direction: column;
        justify-content: center;
    }
    .entry-capital-row {
        display: grid;
        grid-template-columns: minmax(92px, 1fr) auto;
        align-items: center;
        gap: 12px;
    }
    .entry-capital-label {
        color: #9cb0cf;
        font-size: 16px;
        font-weight: 800;
        letter-spacing: .2px;
        white-space: nowrap;
    }
    .entry-capital-value {
        color: #f8fafc;
        font-size: clamp(30px, 4vw, 42px);
        font-weight: 900;
        line-height: 1;
        white-space: nowrap;
        letter-spacing: .2px;
    }
    .entry-unified-panel [data-testid="stSelectbox"] {
        margin-bottom: 0 !important;
    }
    .entry-unified-panel [data-testid="stSelectbox"] label p {
        font-size: 13px !important;
        color: #dbe8ff !important;
    }
    .entry-unified-panel .stButton > button {
        margin-top: var(--sp-1) !important;
        min-height: 46px !important;
    }
    .entry-gap-hero {
        height: 14px;
    }
    .entry-gap-cta {
        height: 14px;
    }
    .entry-gap-guide {
        height: 16px;
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
        margin-top: var(--sp-2);
        padding: var(--sp-3) var(--sp-3) var(--sp-2);
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
    .lb-podium {
        margin-top: 6px;
        display: grid;
        grid-template-columns: repeat(3, minmax(0, 1fr));
        gap: 10px;
    }
    .lb-podium-card {
        border-radius: 12px;
        padding: 10px 10px 12px;
        border: 1px solid #415c82;
        background: linear-gradient(165deg, rgba(15,23,42,.9), rgba(30,41,59,.72));
        text-align: center;
        box-shadow: inset 0 1px 0 rgba(255,255,255,.06);
    }
    .lb-podium-rank {
        font-weight: 900;
        font-size: 18px;
        letter-spacing: .4px;
        margin-bottom: 2px;
        display: inline-flex;
        align-items: center;
        justify-content: center;
        gap: 6px;
    }
    .lb-medal {
        width: 22px;
        height: 22px;
        border-radius: 999px 999px 999px 999px;
        display: inline-flex;
        align-items: center;
        justify-content: center;
        position: relative;
        box-shadow: inset 0 1px 0 rgba(255,255,255,.45), 0 2px 8px rgba(2,6,23,.35);
    }
    .lb-medal::before {
        content: "✦";
        font-size: 11px;
        line-height: 1;
        font-weight: 900;
        color: rgba(15, 23, 42, .92);
        text-shadow: 0 1px 0 rgba(255,255,255,.28);
    }
    .lb-medal-gold {
        background: linear-gradient(145deg, #fde68a, #f59e0b);
    }
    .lb-medal-silver {
        background: linear-gradient(145deg, #f1f5f9, #94a3b8);
    }
    .lb-medal-bronze {
        background: linear-gradient(145deg, #fdba74, #c2410c);
    }
    .lb-podium-user {
        color: #e2e8f0;
        font-size: 14px;
        font-weight: 800;
        white-space: nowrap;
        overflow: hidden;
        text-overflow: ellipsis;
    }
    .lb-podium-val {
        margin-top: 4px;
        color: #bfdbfe;
        font-size: 15px;
        font-weight: 900;
    }
    .lb-podium-card.rank-1 {
        border-color: #b9892a;
        background: linear-gradient(165deg, rgba(77,51,16,.36), rgba(15,23,42,.94));
        box-shadow: inset 0 1px 0 rgba(255,255,255,.08), 0 10px 24px rgba(146,95,23,.25);
        transform: translateY(-3px);
    }
    .lb-podium-card.rank-2 {
        border-color: #8093ad;
        background: linear-gradient(165deg, rgba(73,85,104,.30), rgba(15,23,42,.94));
    }
    .lb-podium-card.rank-3 {
        border-color: #9a6b3e;
        background: linear-gradient(165deg, rgba(88,56,32,.32), rgba(15,23,42,.94));
    }
    .lb-podium-card.rank-1 .lb-podium-rank { color: #fbbf24; }
    .lb-podium-card.rank-2 .lb-podium-rank { color: #d1d5db; }
    .lb-podium-card.rank-3 .lb-podium-rank { color: #fb923c; }
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
    @keyframes heroSweep {
        0% { transform: translateX(-180%) skewX(-18deg); opacity: 0; }
        20% { opacity: .85; }
        45% { transform: translateX(440%) skewX(-18deg); opacity: 0; }
        100% { transform: translateX(440%) skewX(-18deg); opacity: 0; }
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
        .entry-hero-wrap { padding: 12px 12px; }
        .entry-title { letter-spacing: .6px; }
        .entry-capital-row { grid-template-columns: 1fr; justify-items: start; gap: 6px; }
        .entry-capital-value { font-size: 30px; }
        .entry-unified-panel { padding: 10px; }
        .entry-capital-mini { min-height: 156px; }
        .entry-capital-label {
            font-size: 15px;
        }
        .entry-gap-hero {
            height: 12px;
        }
        .entry-gap-cta {
            height: 12px;
        }
        .entry-gap-guide {
            height: 14px;
        }
        .lb-podium {
            grid-template-columns: 1fr;
            gap: 8px;
        }
        .lb-podium-card.rank-1 { transform: none; }
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
    @media (max-width: 1200px) and (min-width: 769px) {
        .entry-capital-row {
            grid-template-columns: 1fr;
            justify-items: start;
            gap: 8px;
        }
        .entry-capital-value {
            font-size: clamp(30px, 3.2vw, 38px);
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


def open_feedback_dialog(game_id, user_id, symbol, symbol_name, symbol_type):
    @st.dialog("反馈建议")
    def _dialog():
        with st.form(f"feedback_form_dialog_{game_id}", clear_on_submit=True):
            rating = st.slider("本局体验评分", min_value=1, max_value=5, value=4, step=1)
            content = st.text_area("反馈内容", height=140, placeholder="请输入本局游戏体验、问题或优化建议")
            submitted = st.form_submit_button("提交反馈", type="primary", use_container_width=True)

        if submitted:
            if not str(content or "").strip():
                st.warning("请输入反馈内容后再提交。")
                return
            result = kg.save_game_feedback(
                game_id=game_id,
                user_id=user_id,
                content=content,
                rating=rating,
                symbol=symbol,
                symbol_name=symbol_name,
                symbol_type=symbol_type,
            )
            if result.get("ok"):
                saved = list(st.session_state.get('feedback_saved_games') or [])
                gid = str(game_id)
                if gid not in saved:
                    saved.append(gid)
                st.session_state['feedback_saved_games'] = saved
                st.success("反馈已提交，感谢建议。")
                time.sleep(0.4)
                st.rerun()
            else:
                st.error(f"反馈提交失败：{result.get('message', '未知错误')}")

    _dialog()


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

    medal = {1: "TOP 1", 2: "TOP 2", 3: "TOP 3"}
    medal_cls = {1: "lb-medal-gold", 2: "lb-medal-silver", 3: "lb-medal-bronze"}
    top3 = rows[:3]
    if top3:
        podium_html = []
        for i, r in enumerate(top3, start=1):
            user = escape(str(r.get("user_id", "-")))
            raw = float(r.get("value", 0) or 0)
            value = _format_lb_value(board_key, raw)
            val_cls = "lb-podium-val"
            if board_key in ("profit", "max_profit"):
                val_cls += " profit-pos" if raw >= 0 else " profit-neg"
            podium_html.append(
                f'<div class="lb-podium-card rank-{i}">'
                f'<div class="lb-podium-rank"><span class="lb-medal {medal_cls.get(i, "")}"></span>{medal.get(i, f"TOP {i}")}</div>'
                f'<div class="lb-podium-user">{user}</div>'
                f'<div class="{val_cls}">{value}</div>'
                '</div>'
            )
        st.markdown(f'<div class="lb-podium">{"".join(podium_html)}</div>', unsafe_allow_html=True)

    item_html = []
    for i, r in enumerate(rows[:20], start=1):
        if i <= 3:
            continue
        user = escape(str(r.get("user_id", "-")))
        raw = float(r.get("value", 0) or 0)
        value = _format_lb_value(board_key, raw)
        rank_text = str(i)
        rank_cls = "lb-rank"
        row_cls = ""
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
        + (''.join(item_html) if item_html else '<div class="lb-empty" style="border:none;border-radius:0;">已展示前三名，暂无更多玩家</div>') +
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


def _restore_login_with_cookie_state():
    """
    返回:
    - restored: bool
    - state: ok | empty | partial | invalid | error
    - cookies: dict
    """
    try:
        cookies = cookie_manager.get_all() or {}
    except Exception:
        return False, "error", {}

    restored = auth.restore_login_from_cookies(cookies)
    if restored:
        return True, "ok", cookies

    c_user = str(cookies.get("username") or "").strip()
    c_token = str(cookies.get("token") or "").strip()
    if not c_user and not c_token:
        return False, "empty", cookies
    if (c_user and not c_token) or (c_token and not c_user):
        return False, "partial", cookies
    return False, "invalid", cookies

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
if 'feedback_inline_open' not in st.session_state:
    st.session_state['feedback_inline_open'] = False
if 'feedback_saved_games' not in st.session_state:
    st.session_state['feedback_saved_games'] = []
if 'kline_replay_requested' not in st.session_state:
    st.session_state['kline_replay_requested'] = False

# 恢复登录
if not st.session_state.get('is_logged_in'):
    restored, restore_state, _ = _restore_login_with_cookie_state()
    if restored:
        st.session_state["kline_cookie_retry_once"] = False
    elif restore_state in ("empty", "partial", "error") and not st.session_state.get("kline_cookie_retry_once", False):
        # IE/兼容模式下首次读取 cookie 偶尔只返回部分字段，这里重试一次避免误判登出
        st.session_state["kline_cookie_retry_once"] = True
        time.sleep(0.15)
        st.rerun()

# 侧边栏
with st.sidebar:
    st.markdown("### 🎮 K线训练场")
    if st.session_state.get('is_logged_in'):
        st.success(f"👤 {st.session_state.get('user_id')}")

# 🔧 处理游戏结果：先把 query 参数落到 session，避免被 rerun 清空后页面闪退
game_done = st.query_params.get('game_done', '')

# Safari/WebKit 下“再来一局”可能先 rerun 再清掉 game_done，这里给重开路径更高优先级
if st.session_state.get('kline_replay_requested'):
    if game_done == '1':
        try:
            st.query_params.clear()
        except Exception:
            pass
        st.rerun()
    else:
        st.session_state['kline_replay_requested'] = False

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
    pending_gid_for_skip = None
    try:
        pending_gid_for_skip = int((st.session_state.get('settlement_pending') or {}).get('game_id') or 0) or None
    except Exception:
        pending_gid_for_skip = None
    st.query_params.clear()
    if pending_gid_for_skip:
        try:
            st.query_params['skip_unfinished_game_id'] = str(pending_gid_for_skip)
        except Exception:
            pass
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
    review_summary = {}
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
                    try:
                        analysis_payload = kg.get_game_analysis(
                            game_id=game_id,
                            viewer_id=game_info['user_id'],
                            target_user=game_info['user_id'],
                        )
                        if analysis_payload.get("ok"):
                            report = analysis_payload.get("report") or {}
                            review_summary = {
                                "overall_score": float(report.get("overall_score") or 0),
                                "direction_score": float(report.get("direction_score") or 0),
                                "risk_score": float(report.get("risk_score") or 0),
                                "execution_score": float(report.get("execution_score") or 0),
                                "mistakes": list(report.get("mistakes") or []),
                                "ai_status": str(report.get("ai_status") or ""),
                            }
                    except Exception as review_err:
                        print(f"读取复盘摘要失败: {review_err}")
    except Exception as e:
        print(f"结算游戏失败: {e}")

    st.session_state['settlement_view'] = {
        'game_id': game_id,
        'from_iframe': from_iframe,
        'profit': profit,
        'profit_rate': profit_rate,
        'trade_count': trade_count,
        'symbol': symbol,
        'symbol_name': symbol_name,
        'symbol_type': symbol_type,
        'new_achievements': new_achievements,
        'review_summary': review_summary,
    }
    st.session_state['settlement_pending'] = None
    # 避免“处理中动画”和“结算页”出现在同一轮渲染中
    st.rerun()

# 展示结算页（手动确认后才离开）
if st.session_state.get('settlement_view'):
    view = st.session_state['settlement_view']
    game_id = int(view.get('game_id') or 0)
    from_iframe = view.get('from_iframe', False)
    profit = float(view.get('profit', 0))
    profit_rate = float(view.get('profit_rate', 0))
    trade_count = int(view.get('trade_count', 0))
    symbol = view.get('symbol', '')
    symbol_name = view.get('symbol_name', '未知')
    symbol_type = view.get('symbol_type', 'stock')
    new_achievements = view.get('new_achievements') or []
    review_summary = view.get('review_summary') or {}
    uid = st.session_state.get('user_id')
    saved_games = st.session_state.get('feedback_saved_games') or []
    feedback_done = str(game_id) in saved_games

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

        if review_summary:
            score_text = f"{float(review_summary.get('overall_score', 0)):.1f}"
            ds = float(review_summary.get('direction_score', 0))
            rs = float(review_summary.get('risk_score', 0))
            es = float(review_summary.get('execution_score', 0))
            mistakes = review_summary.get('mistakes') or []
            top_mistakes = "、".join([str((m or {}).get('title') or (m or {}).get('tag') or '') for m in mistakes[:2]]) or "暂无"
            ai_status = str(review_summary.get('ai_status') or "")
            st.markdown(
                f"""
                <div class="game-guide-card" style="margin-top:12px;">
                    <div class="game-guide-title">🧠 体系复盘摘要</div>
                    <div style="display:flex;gap:22px;flex-wrap:wrap;">
                        <div><span style="color:#94a3b8;">总分</span><div style="font-size:22px;font-weight:800;color:#e5e7eb;">{score_text}</div></div>
                        <div><span style="color:#94a3b8;">方向分</span><div style="font-size:20px;font-weight:700;color:#e5e7eb;">{ds:.1f}</div></div>
                        <div><span style="color:#94a3b8;">风险分</span><div style="font-size:20px;font-weight:700;color:#e5e7eb;">{rs:.1f}</div></div>
                        <div><span style="color:#94a3b8;">执行分</span><div style="font-size:20px;font-weight:700;color:#e5e7eb;">{es:.1f}</div></div>
                    </div>
                    <div style="margin-top:10px;color:#cbd5e1;">主要问题：{top_mistakes}</div>
                    <div style="margin-top:6px;color:#64748b;font-size:12px;">AI状态：{ai_status or 'rule_only'}（按需生成）</div>
                </div>
                """,
                unsafe_allow_html=True,
            )

        col_feedback, col_review, col_replay = st.columns(3)
        with col_feedback:
            feedback_label = "反馈已提交" if feedback_done else "反馈建议"
            if st.button(feedback_label, use_container_width=True, disabled=feedback_done):
                if hasattr(st, "dialog"):
                    open_feedback_dialog(game_id, uid, symbol, symbol_name, symbol_type)
                else:
                    st.session_state['feedback_inline_open'] = True
                    st.rerun()
        with col_review:
            if st.button("📋 数据复盘", use_container_width=True):
                st.session_state["kline_review_focus_game_id"] = int(game_id or 0)
                st.switch_page("pages/19_K线复盘.py")

        if st.session_state.get('feedback_inline_open') and not feedback_done:
            with st.form(f"feedback_form_inline_{game_id}"):
                rating = st.slider("本局体验评分", min_value=1, max_value=5, value=4, step=1, key=f"feedback_rate_inline_{game_id}")
                content = st.text_area("反馈内容", height=120, placeholder="请输入本局游戏体验、问题或优化建议", key=f"feedback_text_inline_{game_id}")
                submit_feedback = st.form_submit_button("提交反馈", type="primary", use_container_width=True)
                cancel_feedback = st.form_submit_button("取消")
                if cancel_feedback:
                    st.session_state['feedback_inline_open'] = False
                    st.rerun()
                if submit_feedback:
                    if not str(content or "").strip():
                        st.warning("请输入反馈内容后再提交。")
                    else:
                        save_res = kg.save_game_feedback(
                            game_id=game_id,
                            user_id=uid,
                            content=content,
                            rating=rating,
                            symbol=symbol,
                            symbol_name=symbol_name,
                            symbol_type=symbol_type,
                        )
                        if save_res.get("ok"):
                            saved = list(st.session_state.get('feedback_saved_games') or [])
                            gid = str(game_id)
                            if gid not in saved:
                                saved.append(gid)
                            st.session_state['feedback_saved_games'] = saved
                            st.session_state['feedback_inline_open'] = False
                            st.success("反馈已提交，感谢建议。")
                            time.sleep(0.4)
                            st.rerun()
                        else:
                            st.error(f"反馈提交失败：{save_res.get('message', '未知错误')}")

        if from_iframe:
            st.info("本局已完成结算，请点击左侧「K线训练」返回主页面继续。")
        else:
            with col_replay:
                if st.button("🎮 再来一局", type="primary", use_container_width=True):
                    if not st.session_state.get('is_logged_in'):
                        # 点击“再来一局”前再尝试一次恢复，避免结算页可见但入口页被误判未登录
                        st.session_state["kline_cookie_retry_once"] = False
                        restored, restore_state, _ = _restore_login_with_cookie_state()
                        if not restored and restore_state in ("empty", "partial", "error"):
                            time.sleep(0.15)
                            st.rerun()
                        if not restored:
                            st.warning("登录状态已失效，请先回首页登录。")
                            st.page_link("Home.py", label="🏠 返回首页登录", use_container_width=True)
                            st.stop()

                    st.session_state['settlement_view'] = None
                    st.session_state['settlement_pending'] = None
                    st.session_state['feedback_inline_open'] = False
                    st.session_state['kline_replay_requested'] = True
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
        skip_unfinished_qid = None
        try:
            skip_unfinished_qid = int(st.query_params.get('skip_unfinished_game_id', '0') or '0') or None
        except Exception:
            skip_unfinished_qid = None

        last_unfinished = kg.check_unfinished_game(user_id)
        if last_unfinished:
            unfinished_game_id = last_unfinished.get('id')
            game_start_time = last_unfinished.get('game_start_time')

            # 🔧 核心修复：排除刚结束的游戏
            just_finished_id = st.session_state.get('just_finished_game_id')

            if (
                (just_finished_id and unfinished_game_id == just_finished_id) or
                (skip_unfinished_qid and unfinished_game_id == skip_unfinished_qid)
            ):
                # 这是刚结束的游戏，跳过惩罚
                st.session_state['just_finished_game_id'] = None
                try:
                    if skip_unfinished_qid:
                        del st.query_params['skip_unfinished_game_id']
                except Exception:
                    pass
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
                            <p style="color:#fca5a5;">惩罚：-20,000 元</p>
                        </div>""", unsafe_allow_html=True)
                        if st.button("确认并重新开始", type="primary", use_container_width=True):
                            kg.settle_abandoned_game(user_id, last_unfinished['id'])
                            time.sleep(1)
                            st.rerun()
                    st.stop()
        elif skip_unfinished_qid:
            try:
                del st.query_params['skip_unfinished_game_id']
            except Exception:
                pass
    except:
        pass

user_capital = kg.get_user_capital(user_id) or 1000000

# 游戏设置页面
if not st.session_state.get('game_started'):
    col1, col2, col3 = st.columns([1, 2, 1])
    with col2:
        st.markdown("""
        <div class="entry-hero-wrap">
            <div class="entry-title">K线交易训练</div>
            <div class="entry-sub">高压缩信息密度训练场。仅凭K线做决策，在100根日线内完成完整交易闭环。</div>
        </div>
        """, unsafe_allow_html=True)
        st.markdown('<div class="entry-gap-hero"></div>', unsafe_allow_html=True)

        col_a, col_b = st.columns([1, 1], gap="small")
        with col_a:
            st.markdown(f"""
            <div class="entry-capital-mini">
                <div class="entry-capital-row">
                    <div class="entry-capital-label">账户资金</div>
                    <div class="entry-capital-value">{user_capital:,.0f}</div>
                </div>
            </div>
            """, unsafe_allow_html=True)
        with col_b:
            speed = st.selectbox("播放速度", ["1秒/根", "3秒/根", "5秒/根"], index=0)
            speed_ms = {"1秒/根": 1000, "3秒/根": 3000, "5秒/根": 5000}[speed]
            speed_sec = {"1秒/根": 1, "3秒/根": 3, "5秒/根": 5}[speed]
            leverage = st.selectbox("杠杆倍数", ["1倍", "10倍"], index=0)
            leverage_val = {"1倍": 1, "10倍": 10}[leverage]
        st.markdown('<div class="entry-gap-cta"></div>', unsafe_allow_html=True)

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
                    v = _safe_float(r.get('vol'))
                    if None in (o, h, l, c):
                        continue
                    if v is None or v < 0:
                        v = 0.0
                    trade_date = r.get('trade_date') if 'trade_date' in r else None
                    if trade_date is None and hasattr(r, "name"):
                        trade_date = r.name
                    if isinstance(trade_date, pd.Timestamp):
                        trade_date = trade_date.strftime("%Y-%m-%d")
                    elif isinstance(trade_date, datetime):
                        trade_date = trade_date.strftime("%Y-%m-%d")
                    elif trade_date is not None:
                        trade_date = str(trade_date)[:10]
                    kline_data.append({
                        'open': o, 'high': h, 'low': l, 'close': c,
                        'volume': v, 'date': trade_date
                    })

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
        st.markdown('<div class="entry-gap-guide"></div>', unsafe_allow_html=True)

        with st.expander("🎯 游戏说明（展开查看）", expanded=False):
            st.markdown("""
            <div class="game-guide-card" style="margin-top:0;">
                <div class="game-guide-grid">
                    <div class="guide-item"><b>1. 客观数据</b><span>本游戏采用真实市场的历史交易价格，每场游戏是100根日K线。</span></div>
                    <div class="guide-item"><b>2. K线为主</b><span>游戏主要训练K线交易，如果不懂，可咨询客服。</span></div>
                    <div class="guide-item"><b>3. 中离惩罚</b><span>如果没有正常结算游戏，会被处罚扣资金2万。</span></div>
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

    config_json = json.dumps(config, ensure_ascii=False, default=_json_default)
    kline_json = json.dumps(kline_data, ensure_ascii=False, default=_json_default)

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
            gap: 12px;
            flex-wrap: wrap;
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
        .top-right {{
            display: flex;
            align-items: center;
            gap: 10px;
            margin-left: auto;
            flex-wrap: wrap;
            justify-content: flex-end;
        }}
        .badge-row {{
            display: flex;
            gap: 10px;
            flex-wrap: wrap;
        }}
        .indicator-panel {{
            display: flex;
            align-items: center;
            gap: 8px;
            padding: 4px 8px;
            border-radius: 8px;
            border: 1px solid #334155;
            background: rgba(15, 23, 42, 0.8);
        }}
        .indicator-label {{
            color: #94a3b8;
            font-size: 12px;
            font-weight: 700;
            letter-spacing: .2px;
        }}
        .indicator-label .label-short {{ display: none; }}
        .toggle-chip {{
            display: inline-flex;
            align-items: center;
            gap: 6px;
            padding: 4px 8px;
            border: 1px solid #334155;
            border-radius: 999px;
            background: #111827;
            color: #cbd5e1;
            font-size: 12px;
            cursor: pointer;
            user-select: none;
        }}
        .toggle-chip input {{
            accent-color: #3b82f6;
            width: 13px;
            height: 13px;
            cursor: pointer;
        }}
        .segmented {{
            display: inline-flex;
            align-items: center;
            gap: 4px;
            background: #111827;
            border: 1px solid #334155;
            border-radius: 999px;
            padding: 3px;
        }}
        .seg-btn {{
            border: none;
            background: transparent;
            color: #94a3b8;
            font-size: 12px;
            font-weight: 700;
            padding: 5px 9px;
            border-radius: 999px;
            cursor: pointer;
            transition: all .12s ease;
            white-space: nowrap;
        }}
        .seg-btn.active {{
            background: #2563eb;
            color: #fff;
            box-shadow: 0 4px 12px rgba(37, 99, 235, .32);
        }}

        /* K线图区域 */
        .chart-area {{ flex: 1; background: #0f172a; min-height: 0; position: relative; }}
        #chart {{ width: 100%; height: 100%; }}
        .chart-info-panel {{
            position: absolute;
            top: 12px;
            left: 12px;
            min-width: 240px;
            max-width: min(52vw, 520px);
            padding: 6px 10px;
            border-radius: 10px;
            border: 1px solid rgba(51, 65, 85, 0.95);
            background: rgba(15, 23, 42, 0.88);
            color: #dbe6f5;
            font-size: 12px;
            line-height: 1.3;
            white-space: pre-line;
            pointer-events: none;
            z-index: 6;
            box-shadow: 0 8px 20px rgba(2, 6, 23, 0.32);
        }}
        .subpane-label {{
            position: absolute;
            left: 12px;
            bottom: 12px;
            padding: 2px 8px;
            border-radius: 6px;
            border: 1px solid #334155;
            background: rgba(15, 23, 42, 0.78);
            color: #94a3b8;
            font-size: 12px;
            line-height: 1.3;
            pointer-events: none;
            z-index: 5;
        }}

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
            .top-bar {{
                padding: 10px 12px;
                gap: 10px;
            }}
            .price-section {{
                gap: 12px;
                width: 100%;
                justify-content: space-between;
            }}
            .progress-section {{
                width: 100%;
                justify-content: space-between;
            }}
            .progress-bar {{
                width: min(58vw, 220px);
            }}
            .top-right {{
                width: 100%;
                margin-left: 0;
                justify-content: flex-start;
            }}
            .indicator-panel {{
                width: 100%;
                flex-wrap: nowrap;
                gap: 6px;
                overflow-x: auto;
                overflow-y: hidden;
                -webkit-overflow-scrolling: touch;
                scrollbar-width: none;
            }}
            .indicator-panel::-webkit-scrollbar {{
                display: none;
            }}
            .indicator-panel .indicator-label,
            .indicator-panel .toggle-chip,
            .indicator-panel .segmented {{
                flex-shrink: 0;
            }}
            .indicator-panel .indicator-label {{
                font-size: 11px;
            }}
            .indicator-label .label-full {{
                display: none;
            }}
            .indicator-label .label-short {{
                display: inline;
            }}
            .indicator-panel .toggle-chip {{
                padding: 3px 7px;
                font-size: 11px;
                gap: 4px;
            }}
            .indicator-panel .toggle-chip input {{
                width: 12px;
                height: 12px;
            }}
            .indicator-panel .segmented {{
                padding: 2px;
                gap: 2px;
            }}
            .indicator-panel .seg-btn {{
                font-size: 11px;
                padding: 4px 8px;
            }}
            .chart-info-panel {{
                top: 10px;
                left: 10px;
                right: 10px;
                max-width: none;
                min-width: 0;
                font-size: 11px;
                padding: 7px 8px;
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
        <div class="top-right">
            <div class="badge-row">
                <div class="info-badge">杠杆: <span>{config['leverage']}x</span></div>
                <div class="info-badge">每手: <span>1,000元</span></div>
            </div>
            <div class="indicator-panel">
                <span class="indicator-label"><span class="label-full">指标</span><span class="label-short">指</span></span>
                <label class="toggle-chip" for="toggle-ma">
                    <input type="checkbox" id="toggle-ma" onchange="setShowMA(this.checked)">
                    <span>MA(5/20/60)</span>
                </label>
                <span class="indicator-label"><span class="label-full">副图</span><span class="label-short">副</span></span>
                <div class="segmented" id="subpane-switch">
                    <button type="button" class="seg-btn active" data-mode="volume" onclick="setSubpaneMode('volume')">成交量</button>
                    <button type="button" class="seg-btn" data-mode="macd" onclick="setSubpaneMode('macd')">MACD</button>
                    <button type="button" class="seg-btn" data-mode="off" onclick="setSubpaneMode('off')">关闭</button>
                </div>
            </div>
        </div>
    </div>

    <!-- K线图 -->
    <div class="chart-area">
        <div id="chart"></div>
        <div class="chart-info-panel" id="chart-info-panel" style="display:none;"></div>
        <div class="subpane-label" id="subpane-label">成交量</div>
    </div>

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

        const MA_PERIODS = [5, 20, 60];
        const MACD_PARAMS = {{ fast: 12, slow: 26, signal: 9 }};

        let chart, candles, volumeSeries, playTimer = null;
        let ma5Series, ma20Series, ma60Series = null;
        let macdHistSeries, macdDifSeries, macdDeaSeries, macdZeroSeries = null;
        let tradeMarkers = [];
        let settleQuery = '';
        let settleSummary = null;
        let tradeEvents = [];
        let tradePersistPromise = null;
        let tradePersisted = false;
        let indicatorCache = null;
        let hoverState = {{
            active: false,
            barIndex: HISTORY - 1,
        }};
        let uiState = {{
            showMA: false,
            subpaneMode: 'volume', // 'volume' | 'macd' | 'off'
        }};

        function calcSMA(values, period) {{
            const out = new Array(values.length).fill(null);
            if (!Array.isArray(values) || period <= 0) return out;
            let sum = 0;
            for (let i = 0; i < values.length; i++) {{
                const v = Number(values[i]);
                if (!Number.isFinite(v)) continue;
                sum += v;
                if (i >= period) {{
                    const oldV = Number(values[i - period]);
                    if (Number.isFinite(oldV)) sum -= oldV;
                }}
                if (i >= period - 1) {{
                    out[i] = sum / period;
                }}
            }}
            return out;
        }}

        function calcEMA(values, period) {{
            const out = new Array(values.length).fill(null);
            if (!Array.isArray(values) || values.length === 0 || period <= 0) return out;
            const k = 2 / (period + 1);
            let prev = null;
            for (let i = 0; i < values.length; i++) {{
                const v = Number(values[i]);
                if (!Number.isFinite(v)) continue;
                if (prev === null) {{
                    prev = v;
                }} else {{
                    prev = (v * k) + (prev * (1 - k));
                }}
                out[i] = prev;
            }}
            return out;
        }}

        function calcMACD(values, fast=12, slow=26, signal=9) {{
            const emaFast = calcEMA(values, fast);
            const emaSlow = calcEMA(values, slow);
            const dif = new Array(values.length).fill(null);
            for (let i = 0; i < values.length; i++) {{
                if (emaFast[i] == null || emaSlow[i] == null) continue;
                dif[i] = emaFast[i] - emaSlow[i];
            }}

            const difInput = dif.map(v => (v == null ? NaN : v));
            const dea = calcEMA(difInput, signal);
            const hist = new Array(values.length).fill(null);
            for (let i = 0; i < values.length; i++) {{
                if (dif[i] == null || dea[i] == null) continue;
                hist[i] = 2 * (dif[i] - dea[i]);
            }}
            return {{ dif, dea, hist }};
        }}

        function buildIndicatorCache() {{
            const closes = KLINE.map((d) => Number(d.close || 0));
            return {{
                ma5: calcSMA(closes, MA_PERIODS[0]),
                ma20: calcSMA(closes, MA_PERIODS[1]),
                ma60: calcSMA(closes, MA_PERIODS[2]),
                macd: calcMACD(closes, MACD_PARAMS.fast, MACD_PARAMS.slow, MACD_PARAMS.signal),
            }};
        }}

        function indicatorSeriesData(values, endExclusive) {{
            const out = [];
            const max = Math.min(Number(endExclusive || 0), values.length);
            for (let i = 0; i < max; i++) {{
                const v = values[i];
                if (v == null || !Number.isFinite(Number(v))) continue;
                out.push({{ time: i, value: Number(v) }});
            }}
            return out;
        }}

        function macdHistSeriesData(histValues, endExclusive) {{
            const out = [];
            const max = Math.min(Number(endExclusive || 0), histValues.length);
            for (let i = 0; i < max; i++) {{
                const v = histValues[i];
                if (v == null || !Number.isFinite(Number(v))) continue;
                const num = Number(v);
                out.push({{
                    time: i,
                    value: num,
                    color: num >= 0 ? 'rgba(239,68,68,0.55)' : 'rgba(34,197,94,0.55)'
                }});
            }}
            return out;
        }}

        function zeroLineSeriesData(endExclusive) {{
            const out = [];
            const max = Math.max(0, Math.min(Number(endExclusive || 0), KLINE.length));
            for (let i = 0; i < max; i++) {{
                out.push({{ time: i, value: 0 }});
            }}
            return out;
        }}

        function setSeriesVisible(series, visible) {{
            if (!series) return;
            try {{
                series.applyOptions({{ visible: !!visible }});
            }} catch (e) {{
                // 某些版本/系列不支持 visible 时降级为透明
                try {{
                    series.applyOptions({{
                        color: visible ? undefined : 'rgba(0,0,0,0)',
                        lineColor: visible ? undefined : 'rgba(0,0,0,0)',
                    }});
                }} catch (_e) {{}}
            }}
        }}

        function updateSubpaneLabel() {{
            const el = document.getElementById('subpane-label');
            if (!el) return;
            if (uiState.subpaneMode === 'volume') {{
                el.textContent = '成交量';
                el.style.display = 'block';
            }} else if (uiState.subpaneMode === 'macd') {{
                el.textContent = `MACD(${{MACD_PARAMS.fast}},${{MACD_PARAMS.slow}},${{MACD_PARAMS.signal}})`;
                el.style.display = 'block';
            }} else {{
                el.textContent = '副图关闭';
                el.style.display = 'none';
            }}
        }}

        function fmtVal(v, digits=2) {{
            const n = Number(v);
            if (!Number.isFinite(n)) return '--';
            return n.toFixed(digits);
        }}

        function fmtVol(v) {{
            const n = Number(v);
            if (!Number.isFinite(n)) return '--';
            if (Math.abs(n) >= 100000000) return (n / 100000000).toFixed(2) + '亿';
            if (Math.abs(n) >= 10000) return (n / 10000).toFixed(2) + '万';
            return Math.round(n).toLocaleString();
        }}

        function setChartInfoPanelVisible(visible) {{
            const panel = document.getElementById('chart-info-panel');
            if (!panel) return;
            panel.style.display = visible ? 'block' : 'none';
        }}

        function updateChartInfoPanel(barIndex) {{
            const panel = document.getElementById('chart-info-panel');
            if (!panel) return;
            const idx = Math.max(0, Math.min(Number(barIndex || 0), KLINE.length - 1));
            const bar = KLINE[idx];
            if (!bar) {{
                panel.textContent = '无数据';
                return;
            }}

            const lines = [];
            lines.push(
                '开 ' + fmtVal(bar.open) +
                '  高 ' + fmtVal(bar.high) +
                '  低 ' + fmtVal(bar.low) +
                '  收 ' + fmtVal(bar.close) +
                '  量 ' + fmtVol(bar.volume)
            );

            if (indicatorCache) {{
                lines.push(
                    'MA5 ' + fmtVal(indicatorCache.ma5[idx]) +
                    '  MA20 ' + fmtVal(indicatorCache.ma20[idx]) +
                    '  MA60 ' + fmtVal(indicatorCache.ma60[idx]) +
                    '  DIF ' + fmtVal(indicatorCache.macd?.dif?.[idx], 3) +
                    '  DEA ' + fmtVal(indicatorCache.macd?.dea?.[idx], 3) +
                    '  MACD ' + fmtVal(indicatorCache.macd?.hist?.[idx], 3)
                );
            }} else {{
                lines.push('MA5 --  MA20 --  MA60 --  DIF --  DEA --  MACD --');
            }}

            panel.textContent = lines.join('\\n');
        }}

        function bindChartHoverInfo() {{
            if (!chart) return;
            chart.subscribeCrosshairMove((param) => {{
                const point = param && param.point;
                const hasPoint = !!(point && Number.isFinite(point.x) && Number.isFinite(point.y));
                if (!hasPoint) {{
                    hoverState.active = false;
                    hoverState.barIndex = Math.max(0, Math.min(state.bar, KLINE.length - 1));
                    setChartInfoPanelVisible(false);
                    return;
                }}

                const chartEl = document.getElementById('chart');
                if (chartEl) {{
                    if (point.x < 0 || point.y < 0 || point.x > chartEl.clientWidth || point.y > chartEl.clientHeight) {{
                        hoverState.active = false;
                        hoverState.barIndex = Math.max(0, Math.min(state.bar, KLINE.length - 1));
                        setChartInfoPanelVisible(false);
                        return;
                    }}
                }}

                const t = param.time;
                if (typeof t !== 'number' || !Number.isFinite(t)) return;
                const idx = Math.max(0, Math.min(Math.round(t), KLINE.length - 1));
                hoverState.active = true;
                hoverState.barIndex = idx;
                setChartInfoPanelVisible(true);
                updateChartInfoPanel(idx);
            }});
        }}

        function syncIndicatorControls() {{
            const maToggle = document.getElementById('toggle-ma');
            if (maToggle) maToggle.checked = !!uiState.showMA;
            document.querySelectorAll('#subpane-switch .seg-btn').forEach((btn) => {{
                btn.classList.toggle('active', btn.dataset.mode === uiState.subpaneMode);
            }});
            updateSubpaneLabel();
        }}

        function applyIndicatorVisibility() {{
            const showMA = !!uiState.showMA;
            const showVol = uiState.subpaneMode === 'volume';
            const showMacd = uiState.subpaneMode === 'macd';

            setSeriesVisible(ma5Series, showMA);
            setSeriesVisible(ma20Series, showMA);
            setSeriesVisible(ma60Series, showMA);

            setSeriesVisible(volumeSeries, showVol);
            setSeriesVisible(macdHistSeries, showMacd);
            setSeriesVisible(macdDifSeries, showMacd);
            setSeriesVisible(macdDeaSeries, showMacd);
            setSeriesVisible(macdZeroSeries, showMacd);

            updateSubpaneLabel();
        }}

        function setShowMA(enabled) {{
            uiState.showMA = !!enabled;
            syncIndicatorControls();
            applyIndicatorVisibility();
        }}

        function setSubpaneMode(mode) {{
            const nextMode = (mode === 'volume' || mode === 'macd' || mode === 'off') ? mode : 'volume';
            uiState.subpaneMode = nextMode;
            syncIndicatorControls();
            applyIndicatorVisibility();
        }}

        function setInitialIndicatorData(endExclusive) {{
            if (!indicatorCache) return;
            ma5Series?.setData(indicatorSeriesData(indicatorCache.ma5, endExclusive));
            ma20Series?.setData(indicatorSeriesData(indicatorCache.ma20, endExclusive));
            ma60Series?.setData(indicatorSeriesData(indicatorCache.ma60, endExclusive));

            volumeSeries?.setData(KLINE.slice(0, endExclusive).map((d, i) => ({{
                time: i,
                value: Number(d.volume || 0),
                color: d.close >= d.open ? 'rgba(239,68,68,0.45)' : 'rgba(34,197,94,0.45)'
            }})));

            macdHistSeries?.setData(macdHistSeriesData(indicatorCache.macd.hist, endExclusive));
            macdDifSeries?.setData(indicatorSeriesData(indicatorCache.macd.dif, endExclusive));
            macdDeaSeries?.setData(indicatorSeriesData(indicatorCache.macd.dea, endExclusive));
            macdZeroSeries?.setData(zeroLineSeriesData(endExclusive));
        }}

        function updateIndicatorAt(index) {{
            if (!indicatorCache || index < 0) return;
            const bar = KLINE[index];
            if (!bar) return;

            volumeSeries?.update({{
                time: index,
                value: Number(bar.volume || 0),
                color: bar.close >= bar.open ? 'rgba(239,68,68,0.45)' : 'rgba(34,197,94,0.45)'
            }});

            const ma5 = indicatorCache.ma5[index];
            const ma20 = indicatorCache.ma20[index];
            const ma60 = indicatorCache.ma60[index];
            if (ma5 != null && Number.isFinite(Number(ma5))) ma5Series?.update({{ time: index, value: Number(ma5) }});
            if (ma20 != null && Number.isFinite(Number(ma20))) ma20Series?.update({{ time: index, value: Number(ma20) }});
            if (ma60 != null && Number.isFinite(Number(ma60))) ma60Series?.update({{ time: index, value: Number(ma60) }});

            const dif = indicatorCache.macd.dif[index];
            const dea = indicatorCache.macd.dea[index];
            const hist = indicatorCache.macd.hist[index];
            if (dif != null && Number.isFinite(Number(dif))) macdDifSeries?.update({{ time: index, value: Number(dif) }});
            if (dea != null && Number.isFinite(Number(dea))) macdDeaSeries?.update({{ time: index, value: Number(dea) }});
            if (hist != null && Number.isFinite(Number(hist))) {{
                const h = Number(hist);
                macdHistSeries?.update({{
                    time: index,
                    value: h,
                    color: h >= 0 ? 'rgba(239,68,68,0.55)' : 'rgba(34,197,94,0.55)'
                }});
            }}
            macdZeroSeries?.update({{ time: index, value: 0 }});
        }}

        function clonePosition(pos) {{
            return {{
                direction: pos?.direction || null,
                lots: Number(pos?.lots || 0),
                avgPrice: Number(pos?.avgPrice || 0),
                totalCost: Number(pos?.totalCost || 0),
            }};
        }}

        function resolveTradeApiCandidates() {{
            const fallback = '/api/kline/trades/batch';
            const candidates = [];
            const configured = String(CONFIG.tradeApiUrl || '').trim();

            if (configured) {{
                let skipConfigured = false;
                try {{
                    const targetUrl = new URL(configured, window.location.href);
                    const host = String(targetUrl.hostname || '').toLowerCase();
                    const pageHost = String(window.location.hostname || '').toLowerCase();
                    const targetIsLoopback = host === '127.0.0.1' || host === 'localhost' || host === '::1';
                    const pageIsLoopback = pageHost === '127.0.0.1' || pageHost === 'localhost' || pageHost === '::1';
                    if (targetIsLoopback && !pageIsLoopback) {{
                        // Avoid using server-local loopback URL on a public page.
                        skipConfigured = true;
                    }}
                }} catch (e) {{}}
                if (!skipConfigured) {{
                    candidates.push(configured);
                }} else {{
                    console.warn('[TRADE_BATCH] skip loopback api on non-loopback page:', configured);
                }}
            }}

            candidates.push(fallback);
            return Array.from(new Set(candidates.filter(Boolean)));
        }}

        function extractApiErrorMessage(body, status) {{
            if (body && typeof body === 'object') {{
                if (body.message) return String(body.message);
                if (body.detail) return String(body.detail);
                if (body.error) return String(body.error);
            }}
            if (status) return 'HTTP ' + status;
            return 'unknown error';
        }}

        function shouldRetryTradeApi(result) {{
            const code = Number(result?.status || 0);
            if (!code) return true; // network-level failure
            return code === 404 || code === 408 || code === 425 || code === 429 || code >= 500;
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

            const apiCandidates = resolveTradeApiCandidates();
            if (!apiCandidates.length) return {{ ok: false, message: 'trade api unavailable' }};

            const payload = {{
                game_id: Number(CONFIG.gameId || 0),
                user_id: String(CONFIG.userId || ''),
                symbol: CONFIG.symbol,
                symbol_name: CONFIG.symbolName,
                symbol_type: CONFIG.symbolType,
                trades: tradeEvents
            }};

            tradePersistPromise = (async () => {{
                let lastError = null;

                for (const apiUrl of apiCandidates) {{
                    try {{
                        const res = await fetch(apiUrl, {{
                            method: 'POST',
                            headers: {{ 'Content-Type': 'application/json' }},
                            body: JSON.stringify(payload)
                        }});

                        let body = {{}};
                        let rawText = '';
                        try {{
                            rawText = await res.text();
                        }} catch (e) {{
                            rawText = '';
                        }}

                        if (rawText) {{
                            try {{
                                body = JSON.parse(rawText);
                            }} catch (e) {{
                                body = {{ ok: false, message: rawText.slice(0, 300) }};
                            }}
                        }}

                        if (res.ok && body && body.ok) {{
                            tradePersisted = true;
                            return {{ ok: true, body, apiUrl }};
                        }}

                        const failed = {{
                            ok: false,
                            status: res.status,
                            body,
                            message: extractApiErrorMessage(body, res.status),
                            apiUrl
                        }};
                        lastError = failed;

                        if (!shouldRetryTradeApi(failed)) {{
                            return failed;
                        }}
                    }} catch (err) {{
                        lastError = {{ ok: false, message: String(err), apiUrl }};
                    }}
                }}

                return lastError || {{ ok: false, message: 'trade api unavailable' }};
            }})().finally(() => {{
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
            indicatorCache = buildIndicatorCache();
            syncIndicatorControls();

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
                rightPriceScale: {{
                    borderColor: '#334155',
                    scaleMargins: {{ top: 0.08, bottom: 0.28 }}
                }},
                timeScale: {{
                    borderColor: '#334155',
                    timeVisible: false,
                    visible: false
                }}
            }});

            // 🔧 【修复3】K线颜色：红涨绿跌（中国标准）
            candles = chart.addCandlestickSeries({{
                upColor: '#ef4444', downColor: '#22c55e',
                borderUpColor: '#ef4444', borderDownColor: '#22c55e',
                wickUpColor: '#ef4444', wickDownColor: '#22c55e'
            }});

            ma5Series = chart.addLineSeries({{
                color: '#facc15',
                lineWidth: 2,
                lastValueVisible: false,
                priceLineVisible: false,
            }});
            ma20Series = chart.addLineSeries({{
                color: '#38bdf8',
                lineWidth: 2,
                lastValueVisible: false,
                priceLineVisible: false,
            }});
            ma60Series = chart.addLineSeries({{
                color: '#a78bfa',
                lineWidth: 2,
                lastValueVisible: false,
                priceLineVisible: false,
            }});

            // 成交量柱：放在主图底部区域
            volumeSeries = chart.addHistogramSeries({{
                priceFormat: {{ type: 'volume' }},
                priceScaleId: '',
                lastValueVisible: false,
                priceLineVisible: false,
            }});
            macdHistSeries = chart.addHistogramSeries({{
                priceScaleId: '',
                lastValueVisible: false,
                priceLineVisible: false,
            }});
            macdDifSeries = chart.addLineSeries({{
                priceScaleId: '',
                color: '#60a5fa',
                lineWidth: 2,
                lastValueVisible: false,
                priceLineVisible: false,
            }});
            macdDeaSeries = chart.addLineSeries({{
                priceScaleId: '',
                color: '#f59e0b',
                lineWidth: 2,
                lastValueVisible: false,
                priceLineVisible: false,
            }});
            macdZeroSeries = chart.addLineSeries({{
                priceScaleId: '',
                color: 'rgba(148,163,184,0.65)',
                lineWidth: 1,
                lastValueVisible: false,
                priceLineVisible: false,
            }});
            chart.priceScale('').applyOptions({{
                scaleMargins: {{ top: 0.76, bottom: 0 }}
            }});

            candles.setData(KLINE.slice(0, HISTORY).map((d, i) => ({{
                time: i, open: d.open, high: d.high, low: d.low, close: d.close
            }})));
            setInitialIndicatorData(HISTORY);
            applyIndicatorVisibility();
            chart.timeScale().fitContent();
            bindChartHoverInfo();
            hoverState.active = false;
            hoverState.barIndex = HISTORY - 1;
            setChartInfoPanelVisible(false);

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
            updateIndicatorAt(state.bar);

            calcPnL();
            updateDisplay();
            state.prevPrice = bar.close;
            if (!hoverState.active) {{
                setChartInfoPanelVisible(false);
            }}

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
                const msg = persistResult?.message
                    || persistResult?.body?.message
                    || persistResult?.body?.detail
                    || persistResult?.body?.error
                    || (persistResult?.status ? ('HTTP ' + persistResult.status) : 'unknown error');
                if (hint) hint.textContent = '交易明细保存失败，可重试或继续结束';
                const endpointHint = persistResult?.apiUrl ? ('\\n接口：' + persistResult.apiUrl) : '';
                const goOn = confirm('交易明细保存失败（' + msg + '）。\\n是否仍结束本局？' + endpointHint);
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
