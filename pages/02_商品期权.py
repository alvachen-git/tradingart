import streamlit as st
import pandas as pd
from lightweight_charts.widgets import StreamlitChart
import sys
import os
import re
import time
import logging
import data_engine as de
from sqlalchemy import text
import datetime as dt
from ui_components import inject_sidebar_toggle_style
from symbol_match import sql_prefix_condition
# 1. 基础配置
st.set_page_config(
    page_title="爱波塔-商品期权技术分析",
    page_icon="favicon.ico",
    layout="wide",
    initial_sidebar_state="expanded"
)

PAGE_NAME = "商品期权"
_PAGE_T0 = time.perf_counter()
_PERF_LOGGER = logging.getLogger(__name__)
_CACHE_PROBE_SEEN = set()
USE_GLOBAL_MONITOR_SNAPSHOT = False
CONTRACT_LOOKBACK_DAYS = 420
MAX_CONTRACT_POOL_ROWS = 1200
MAX_CONTRACT_OPTIONS = 120
MAX_CHART_ROWS = 520


def _perf_user_id() -> str:
    return str(
        st.session_state.get("username")
        or st.session_state.get("user")
        or st.session_state.get("current_user")
        or "anonymous"
    )


def _probe_cache(tag: str, signature: str) -> int:
    cache_key = f"{PAGE_NAME}::{tag}::{signature}"
    hit = 1 if cache_key in _CACHE_PROBE_SEEN else 0
    _CACHE_PROBE_SEEN.add(cache_key)
    return hit


def _perf_page_log(
    *,
    page: str,
    render_ms: float = 0.0,
    db_ms: float = 0.0,
    api_ms: float = 0.0,
    cache_hit: int = -1,
    stage: str = "main",
) -> None:
    msg = (
        f"PERF_PAGE page={page} stage={stage} "
        f"render_ms={render_ms:.1f} db_ms={db_ms:.1f} api_ms={api_ms:.1f} cache_hit={cache_hit}"
    )
    print(msg)
    _PERF_LOGGER.info(msg)


@st.cache_data(ttl=300, show_spinner=False)
def _cached_recent_contract_pool(
    user_id: str, page: str, symbol: str, date_window: str, cutoff_yyyymmdd: str, variety_code: str
) -> list[str]:
    if de.engine is None:
        return []
    prefix_sql = sql_prefix_condition(variety_code)
    sql = text(
        f"""
        SELECT DISTINCT ts_code
        FROM commodity_iv_history
        WHERE REPLACE(trade_date, '-', '') >= :cutoff
          AND {prefix_sql}
        ORDER BY ts_code DESC
        LIMIT {MAX_CONTRACT_POOL_ROWS}
        """
    )
    with de.engine.connect() as conn:
        rows = conn.execute(sql, {"cutoff": cutoff_yyyymmdd}).fetchall()
    return [str(r[0]) for r in rows if r and r[0]]


@st.cache_data(ttl=300, show_spinner=False)
def _cached_comprehensive_market_data(
    user_id: str, page: str, symbol: str, date_window: str
) -> pd.DataFrame:
    # Reuse the same precomputed dataset as ranking page to keep IV Rank consistent.
    return de.get_comprehensive_market_data()


def _extract_contract_code(contract_label: str) -> str:
    if not isinstance(contract_label, str):
        return ""
    match = re.match(r"([A-Za-z]+\d{3,4})", contract_label.strip())
    return match.group(1).upper() if match else ""


def _pick_market_row_by_product(df_market: pd.DataFrame, product_code: str, used_contract: str = ""):
    if df_market is None or df_market.empty:
        return None

    product_code = str(product_code or "").upper()
    used_contract = str(used_contract or "").upper()
    work = df_market.copy()
    work["__contract_code"] = work["合约"].apply(_extract_contract_code)
    candidates = work[work["__contract_code"].str.startswith(product_code, na=False)]
    if candidates.empty:
        return None

    if used_contract:
        exact = candidates[candidates["__contract_code"] == used_contract]
        if not exact.empty:
            return exact.iloc[0]

    # Default to the first row (same display order as ranking dataset).
    return candidates.iloc[0]



# 🔥 添加统一的侧边栏导航
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from sidebar_navigation import show_navigation
with st.sidebar:
    show_navigation()

with open('style.css', encoding='utf-8') as f:
    st.markdown(f'<style>{f.read()}</style>', unsafe_allow_html=True)

# 🔥【手机端专属补丁】修复文字看不清的问题
st.markdown("""
<style>
    @media (max-width: 768px) {
        /* ===========================
           1. 标题与文字颜色修复 (新增)
           =========================== */
        /* 强制所有标题 (h1-h4) 变成深黑色 */
        h1, h2, h3, h4, h5, h6 {
            color: #1f2937 !important; /* 深炭灰色，对比度极高 */
        }

        /* 修复普通文本 (p) 的颜色，防止正文也看不清 */
        [data-testid="stMarkdownContainer"] p {
            color: #374151 !important;
        }

        /* ===========================
           2. 指标卡片 (st.metric) 修复
           =========================== */
        [data-testid="stMetric"] {
            background-color: #ffffff !important; /* 强制白底 */
            border: 1px solid #e5e7eb !important; /* 浅灰边框 */
            border-radius: 8px !important;
            padding: 12px 16px !important;
            box-shadow: 0 1px 2px rgba(0,0,0,0.05) !important;
            margin-bottom: 8px !important;
        }

        /* 标签 (如 "当前 IV") */
        [data-testid="stMetricLabel"] {
            color: #6b7280 !important; /* 灰色 */
            font-size: 14px !important;
        }

        /* 数值 (如 "15.90%") */
        [data-testid="stMetricValue"] {
            color: #111827 !important; /* 纯黑 */
        }

        /* ===========================
           3. 其他组件适配
           =========================== */
        /* 状态提示框 (st.info/warning) */
        [data-testid="stAlert"] {
            background-color: #f3f4f6 !important;
            color: #1f2937 !important;
            border: 1px solid #e5e7eb !important;
        }
        [data-testid="stAlert"] p {
            color: #1f2937 !important;
        }

        /* 下拉框文字 */
        div[data-baseweb="select"] span {
            color: #1f2937 !important;
        }

        /* 手机端顶部容器文字 */
        .mobile-top-container {
            color: #1f2937 !important;
        }
    }
</style>
""", unsafe_allow_html=True)
st.markdown("<style>.stSelectbox {margin-bottom: 20px;}</style>", unsafe_allow_html=True)

# 🔥【PC端样式修复】解决Edge浏览器白底看不到文字的问题
st.markdown("""
<style>
    /* ===========================
       PC端样式修复 (Edge浏览器兼容)
       =========================== */
    /* PC端全局样式 */
    body, p, span, div {
        color: #1f2937 !important;
    }

    /* 1. 全局文字颜色强制设定 */
    body {
        color: #1f2937 !important;
    }

    p, span, div, label {
        color: #374151 !important;
    }

    /* 2. 所有标题颜色 */
    h1, h2, h3, h4, h5, h6 {
        color: #111827 !important;
        font-weight: 600 !important;
    }

    /* 3. Streamlit Metric 组件修复 */
    [data-testid="stMetric"] {
        background-color: #ffffff !important;
        border: 1px solid #e5e7eb !important;
        border-radius: 8px !important;
        padding: 12px 16px !important;
        box-shadow: 0 1px 3px rgba(0,0,0,0.1) !important;
    }

    [data-testid="stMetricLabel"] {
        color: #6b7280 !important;
        font-size: 14px !important;
        font-weight: 500 !important;
    }

    [data-testid="stMetricValue"] {
        color: #111827 !important;
        font-size: 28px !important;
        font-weight: 600 !important;
    }

    [data-testid="stMetricDelta"] {
        font-size: 14px !important;
        font-weight: 500 !important;
    }

    /* 4. Markdown容器文字 */
    [data-testid="stMarkdownContainer"] {
        color: #1f2937 !important;
    }

    [data-testid="stMarkdownContainer"] p {
        color: #374151 !important;
    }

    [data-testid="stMarkdownContainer"] strong {
        color: #111827 !important;
        font-weight: 600 !important;
    }

    [data-testid="stMarkdownContainer"] li {
        color: #374151 !important;
    }

    /* 5. 下拉框和选择器 */
    div[data-baseweb="select"] {
        background-color: #ffffff !important;
    }

    div[data-baseweb="select"] span {
        color: #1f2937 !important;
    }

    div[data-baseweb="select"] input {
        color: #1f2937 !important;
    }
    

    /* 7. 按钮文字 */
    button {
        color: #ffffff !important;
        font-weight: 500 !important;
    }

    button[kind="secondary"] {
        color: #1f2937 !important;
        background-color: #ffffff !important;
        border: 1px solid #d1d5db !important;
    }

    /* 8. Info/Warning/Success/Error 提示框 */
    [data-testid="stAlert"] {
        background-color: #f3f4f6 !important;
        border-left: 4px solid #3b82f6 !important;
        color: #1f2937 !important;
    }

    [data-testid="stAlert"] p {
        color: #1f2937 !important;
    }

    [data-testid="stAlert"] div {
        color: #1f2937 !important;
    }

    /* 9. 表格样式 */
    [data-testid="stTable"] {
        color: #1f2937 !important;
    }

    [data-testid="stDataFrame"] {
        color: #1f2937 !important;
    }

    table {
        color: #1f2937 !important;
    }

    th {
        background-color: #f3f4f6 !important;
        color: #111827 !important;
        font-weight: 600 !important;
    }

    td {
        color: #374151 !important;
    }

    /* 10. 文本输入框 */
    input, textarea, select {
        color: #1f2937 !important;
        background-color: #ffffff !important;
        border: 1px solid #d1d5db !important;
    }

    input::placeholder {
        color: #9ca3af !important;
    }

    /* 11. 标签和标题 */
    label {
        color: #374151 !important;
        font-weight: 500 !important;
    }

    /* 12. 链接 */
    a {
        color: #2563eb !important;
    }

    a:hover {
        color: #1d4ed8 !important;
    }

    /* 13. 代码块 */
    code {
        color: #111827 !important;
        background-color: #f3f4f6 !important;
        padding: 2px 6px !important;
        border-radius: 4px !important;
    }

    pre {
        background-color: #f3f4f6 !important;
        color: #111827 !important;
    }

    /* 14. Expander组件 */
    [data-testid="stExpander"] {
        background-color: #ffffff !important;
        border: 1px solid #e5e7eb !important;
    }

    [data-testid="stExpander"] summary {
        color: #111827 !important;
        font-weight: 500 !important;
    }

    /* 15. 确保主容器背景色 */
    .main {
        background-color: #ffffff !important;
    }

    [data-testid="stAppViewContainer"] {
        background-color: #ffffff !important;
    }

    /* 16. 修复可能的透明背景问题 */
    .element-container {
        background-color: transparent !important;
    }

    /* 17. 顶部状态栏 */
    header[data-testid="stHeader"] {
        background-color: #ffffff !important;
    }

    /* 18. Tabs组件 */
    [data-testid="stTabs"] button {
        color: #6b7280 !important;
    }

    [data-testid="stTabs"] button[aria-selected="true"] {
        color: #111827 !important;
        font-weight: 600 !important;
    }

    /* 19. 确保所有子元素继承颜色 */
    * {
        color: inherit;
    }
</style>
""", unsafe_allow_html=True)

st.markdown("""
<style>
    /* ========================================
       深色侧边栏样式 (与Home页保持一致)
       ======================================== */
       
    /* 主内容区背景色修复 */
    .stApp {
        background-color: #f8f9fa !important;
    }
    
    [data-testid="stAppViewContainer"] {
        background-color: #f8f9fa !important;
    }
    
    .main {
        background-color: #f8f9fa !important;
    }
    
    [data-testid="stMainBlockContainer"] {
        background-color: #f8f9fa !important;
    }
    
    /* 确保卡片保持白色对比 */
    [data-testid="stMetric"] {
        background-color: #ffffff !important;
    }
    
    [data-testid="stPlotlyChart"] {
        background-color: #ffffff !important;
        padding: 16px !important;
        border-radius: 8px !important;
    }

    /* 1. 侧边栏整体深色背景 */
    [data-testid="stSidebar"] {
        background-color: #0f172a !important; /* 深蓝黑色，与Home页一致 */
    }

    /* 2. 侧边栏内所有文字变亮色 */
    [data-testid="stSidebar"] p,
    [data-testid="stSidebar"] span,
    [data-testid="stSidebar"] div,
    [data-testid="stSidebar"] label {
        color: #cbd5e1 !important; /* 亮灰蓝色文字 */
    }

    /* 3. 侧边栏标题（h1-h6）样式 */
    [data-testid="stSidebar"] h1,
    [data-testid="stSidebar"] h2,
    [data-testid="stSidebar"] h3,
    [data-testid="stSidebar"] h4 {
        color: #f1f5f9 !important; /* 更亮的白色 */
        font-weight: 600 !important;
    }

    /* 4. 下拉选择框样式 */
    [data-testid="stSidebar"] div[data-baseweb="select"] {
        background-color: #1e293b !important; /* 稍亮的深色背景 */
    }

    [data-testid="stSidebar"] div[data-baseweb="select"] > div {
        background-color: #1e293b !important;
        border: 1px solid #334155 !important;
    }

    [data-testid="stSidebar"] div[data-baseweb="select"] [role="combobox"] {
        color: #e2e8f0 !important;
    }

    [data-testid="stSidebar"] div[data-baseweb="select"] input {
        color: #e2e8f0 !important;
        -webkit-text-fill-color: #e2e8f0 !important;
        caret-color: #e2e8f0 !important;
        background-color: transparent !important;
    }

    [data-testid="stSidebar"] div[data-baseweb="select"] input::selection {
        background-color: #334155 !important;
        color: #e2e8f0 !important;
    }

    [data-testid="stSidebar"] div[data-baseweb="select"] span {
        color: #e2e8f0 !important; /* 亮色文字 */
    }

    [data-testid="stSidebar"] div[data-baseweb="select"] svg {
        fill: #cbd5e1 !important; /* 下拉箭头亮色 */
    }

    /* 5. 输入框样式 */
    [data-testid="stSidebar"] input {
        background-color: #1e293b !important;
        color: #e2e8f0 !important;
        border: 1px solid #334155 !important;
    }

    [data-testid="stSidebar"] input::placeholder {
        color: #64748b !important;
    }

    /* 6. 按钮样式 */
    [data-testid="stSidebar"] button {
        background-color: #1e293b !important;
        color: #e2e8f0 !important;
        border: 1px solid #334155 !important;
        transition: all 0.2s ease-in-out !important;
    }

    [data-testid="stSidebar"] button:hover {
        background-color: #334155 !important;
        border-color: #475569 !important;
        color: #ffffff !important;
    }

    /* 7. 单选按钮和复选框 */
    [data-testid="stSidebar"] [data-baseweb="radio"] label,
    [data-testid="stSidebar"] [data-baseweb="checkbox"] label {
        color: #cbd5e1 !important;
    }

    /* 8. 滑块（Slider）样式 */
    [data-testid="stSidebar"] [data-baseweb="slider"] {
        color: #cbd5e1 !important;
    }

    /* 9. Expander（展开器）样式 */
    [data-testid="stSidebar"] [data-testid="stExpander"] {
        background-color: #1e293b !important;
        border: 1px solid #334155 !important;
    }

    [data-testid="stSidebar"] [data-testid="stExpander"] summary {
        color: #e2e8f0 !important;
    }

    /* 10. 分隔线样式 */
    [data-testid="stSidebar"] hr {
        border-color: #334155 !important;
    }

    /* 12. 侧边栏中的Info/Warning/Success框 */
    [data-testid="stSidebar"] [data-testid="stAlert"] {
        background-color: #1e293b !important;
        border-left: 4px solid #3b82f6 !important;
        color: #e2e8f0 !important;
    }

    [data-testid="stSidebar"] [data-testid="stAlert"] p {
        color: #cbd5e1 !important;
    }

    /* 13. 侧边栏中的Markdown容器 */
    [data-testid="stSidebar"] [data-testid="stMarkdownContainer"] {
        color: #cbd5e1 !important;
    }

    [data-testid="stSidebar"] [data-testid="stMarkdownContainer"] strong {
        color: #f1f5f9 !important;
    }

    [data-testid="stSidebar"] [data-testid="stMarkdownContainer"] a {
        color: #60a5fa !important;
    }

    /* 14. 侧边栏中的代码块 */
    [data-testid="stSidebar"] code {
        background-color: #1e3a5f !important;
        color: #ffd700 !important;
        border: 1px solid #4a90e2 !important;
        padding: 2px 6px !important;
        border-radius: 4px !important;
    }

    /* 15. 联系卡片样式（如果有的话）*/
    [data-testid="stSidebar"] .contact-card {
        background-color: #1e293b !important;
        border: 1px solid #334155 !important;
        border-radius: 8px !important;
        padding: 16px !important;
    }

    [data-testid="stSidebar"] .contact-title {
        color: #f1f5f9 !important;
    }

    [data-testid="stSidebar"] .contact-item {
        color: #94a3b8 !important;
    }

    [data-testid="stSidebar"] .wechat-highlight {
        color: #00e676 !important; /* 微信绿 */
    }

    /* 16. 确保侧边栏顶部区域也是深色 */
    [data-testid="stSidebarNav"] {
        background-color: #0f172a !important;
    }

    /* 17. 侧边栏中的选择框下拉菜单 */
    [data-testid="stSidebar"] ul[role="listbox"] {
        background-color: #1e293b !important;
        border: 1px solid #334155 !important;
    }

    [data-testid="stSidebar"] ul[role="listbox"] li {
        color: #e2e8f0 !important;
    }

    [data-testid="stSidebar"] ul[role="listbox"] li:hover {
        background-color: #334155 !important;
    }

    /* 18. 状态指示器 */
    [data-testid="stSidebar"] .stMetricValue {
        color: #f1f5f9 !important;
    }

    [data-testid="stSidebar"] .stMetricLabel {
        color: #94a3b8 !important;
    }
</style>
""", unsafe_allow_html=True)
inject_sidebar_toggle_style(mode="high_contrast")

# 2. 侧边栏逻辑
with st.sidebar:
    st.header("1. 选择品种")
    # 映射表：前端显示中文，后端查询用代码
    COMMODITY_MAP = {
        "IH": "上证50","IF": "沪深300","IM": "中证1000",
        "au": "黄金","ag": "白银","cu": "铜","al": "铝","zn": "锌","ni": "镍","sn": "锡",
        "lc": "碳酸锂", "si": "工业硅", "ps": "多晶硅","pt": "铂金","pd": "钯金",
        "rb": "螺纹钢", "i": "铁矿石", "hc": "热卷","jm": "焦煤","ad": "铝合金","fg": "玻璃","sa": "纯碱","ao": "氧化铝","sh": "烧碱","sp": "纸浆","lg": "原木",
        "M": "豆粕", "a": "豆一", "RM": "菜粕","y": "豆油","oi": "菜油","p": "棕榈油","pk": "花生",
        "sc": "原油","ta": "PTA","px": "对二甲苯","PR": "瓶片",  "ma": "甲醇", "v": "PVC", "eb": "苯乙烯","bz": "纯苯","eg": "乙二醇","pp": "聚丙烯","l": "塑料","bu": "沥青","fu": "燃料油","br": "BR橡胶","ur": "尿素",
        "ru": "橡胶", "c": "玉米", "jd": "鸡蛋", "CF": "棉花", "SR": "白糖", "ap": "苹果", "lh": "生猪"
    }
    variety = st.selectbox("品种", list(COMMODITY_MAP.keys()), format_func=lambda x: f"{x} ({COMMODITY_MAP[x]})")

    st.header("2. 选择合约")


    # 获取合约列表函数 (已修复 % 报错问题)
    @st.cache_data(ttl=300, show_spinner=False)
    def get_contracts(user_id, page, symbol, date_window, v):
        if de.engine is None: return []
        try:
            # 直接按品种前缀在 SQL 过滤，避免全表合约池扫描
            now = dt.datetime.now()
            cutoff_yyyymmdd = (now - dt.timedelta(days=CONTRACT_LOOKBACK_DAYS)).strftime("%Y%m%d")
            raw_codes = _cached_recent_contract_pool(
                user_id, page, v, "contracts_pool_300s", cutoff_yyyymmdd, v
            )
            valid_subs = []
            current_yymm = int(now.strftime('%y%m'))

            for code in raw_codes:
                # 正则提取：字母部分 + 数字部分
                match = re.match(r"([a-zA-Z]+)(\d+)", code)
                if not match: continue

                prefix = match.group(1)
                num_part = match.group(2)

                # SQL端已做严格前缀过滤，这里仅做最终保险
                if prefix.upper() != v.upper():
                    continue

                # --- 修复 2: 过滤过期合约 ---
                # 处理年份：郑商所 3位 (501 -> 2501)，其他 4位 (2501)
                if len(num_part) == 3:
                    # 假设是 2020 年代，补全为 2501 这种格式
                    compare_val = int('2' + num_part)
                elif len(num_part) == 4:
                    compare_val = int(num_part)
                else:
                    continue

                    # 过滤逻辑：只显示 未过期 或 最近1个月内过期 的合约
                # 比如现在是 2512，那么 2511 还会显示，2510 就不显示了
                if compare_val >= (current_yymm - 1):
                    valid_subs.append(code)

            # 去重并限制展示数量，降低 selectbox 与前端渲染负载
            valid_subs = sorted(list(dict.fromkeys(valid_subs)), reverse=True)[:MAX_CONTRACT_OPTIONS]

            # 把 "主力连续" 放在第一个
            options = [f"{v.upper()} (主力连续)"] + valid_subs
            return options

        except Exception as e:
            st.error(f"合约加载失败: {e}")
            return []

    user_id = _perf_user_id()
    contracts_window = "contracts_120s"
    contracts_sig = f"{user_id}|{PAGE_NAME}|{variety}|{contracts_window}"
    contracts_hit = _probe_cache("contracts", contracts_sig)
    _db_t0 = time.perf_counter()
    options = get_contracts(user_id, PAGE_NAME, variety, contracts_window, variety)
    _perf_page_log(
        page=PAGE_NAME,
        db_ms=(time.perf_counter() - _db_t0) * 1000,
        cache_hit=contracts_hit,
        stage="get_contracts",
    )

    if not options:
        st.warning(f"未找到 {variety} 的相关合约数据")
        selected_opt = None
    else:
        selected_opt = st.selectbox("合约代码", options)

    # 客服卡片 CSS 样式
    st.markdown("""
        <style>
            .contact-card {
                background-color: #1E2329;
                border: 1px solid #31333F;
                border-radius: 8px;
                padding: 15px;
                margin-top: 10px;
                text-align: center;
            }
            .contact-title {
                font-size: 14px;
                font-weight: bold;
                color: #e6e6e6;
                margin-bottom: 8px;
            }
            .contact-item {
                font-size: 13px;
                color: #8b949e;
                margin-bottom: 4px;
            }
            .wechat-highlight {
                color: #00e676; /* 微信绿 */
                font-weight: bold;
            }
        </style>

        <div class="contact-card">
            <div class="contact-title">🤝 客服联系</div>
            <div class="contact-item">微信：<span class="wechat-highlight">trader-sec</span></div>
            <div class="contact-item">电话：<span class="wechat-highlight">17521591756</span></div>
            <div class="contact-item" style="font-size: 12px; margin-top: 8px;">
                沪ICP备2021018087号-2
            </div>
        </div>
        """, unsafe_allow_html=True)

if selected_opt and "主力连续" in selected_opt:
    target_contract = variety.upper()
    is_continuous = True
else:
    target_contract = selected_opt
    is_continuous = False

# 3. 数据获取函数
@st.cache_data(ttl=90, show_spinner=False)
def get_chart_data(user_id, page, symbol, date_window, code, is_continuous_flag):
    if not code: return None, None
    try:
        # A. 获取 IV (直接查 commodity_iv_history)
        sql_iv = text(
            f"""
            SELECT trade_date, iv, used_contract
            FROM (
                SELECT trade_date, iv, used_contract
                FROM commodity_iv_history
                WHERE ts_code=:c
                ORDER BY trade_date DESC
                LIMIT {MAX_CHART_ROWS}
            ) t
            ORDER BY trade_date
            """
        )
        df_iv = pd.read_sql(sql_iv, de.engine, params={"c": code})

        # B. 获取 K线 (期货价格)
        sql_k = text(
            f"""
            SELECT trade_date, open_price AS open, high_price AS high, low_price AS low, close_price AS close
            FROM (
                SELECT trade_date, open_price, high_price, low_price, close_price
                FROM futures_price
                WHERE ts_code=:c
                ORDER BY trade_date DESC
                LIMIT {MAX_CHART_ROWS}
            ) t
            ORDER BY trade_date
            """
        )
        df_k = pd.read_sql(sql_k, de.engine, params={"c": code})

        # 容错：如果查 IF (主连) 没查到价格，尝试查 IF0 (常见的连续代码)
        if df_k.empty and is_continuous_flag:
            alternatives = [f"{code}0", f"{code}888", f"{code.lower()}0"]
            for alt in alternatives:
                df_k = pd.read_sql(sql_k, de.engine, params={"c": alt})
                if not df_k.empty: break

        return df_k, df_iv
    except Exception as e:
        return None, None


# 4. 绘图逻辑
if target_contract:
    chart_window = "chart_90s"
    chart_sig = f"{_perf_user_id()}|{PAGE_NAME}|{target_contract}|{chart_window}|{is_continuous}"
    chart_hit = _probe_cache("chart_data", chart_sig)
    _db_t0 = time.perf_counter()
    df_kline, df_iv = get_chart_data(
        _perf_user_id(),
        PAGE_NAME,
        target_contract,
        chart_window,
        target_contract,
        is_continuous,
    )
    _perf_page_log(
        page=PAGE_NAME,
        db_ms=(time.perf_counter() - _db_t0) * 1000,
        cache_hit=chart_hit,
        stage="get_chart_data",
    )

    if df_kline is not None and not df_kline.empty:
        st.subheader(f"{target_contract} ")

        # --- 【新增功能】IV Rank 仪表盘 (仅主力连续显示) ---        if is_continuous and df_iv is not None and not df_iv.empty:
            latest_used_contract = str(df_iv.iloc[-1].get("used_contract") or "").split(".")[0].upper()

            market_row = None
            if USE_GLOBAL_MONITOR_SNAPSHOT:
                monitor_window = "market_monitor_3600s"
                monitor_sig = f"{_perf_user_id()}|{PAGE_NAME}|{variety}|{monitor_window}"
                monitor_hit = _probe_cache("comprehensive_market_data", monitor_sig)
                _db_t1 = time.perf_counter()
                df_market = _cached_comprehensive_market_data(
                    _perf_user_id(), PAGE_NAME, variety, monitor_window
                )
                _perf_page_log(
                    page=PAGE_NAME,
                    db_ms=(time.perf_counter() - _db_t1) * 1000,
                    cache_hit=monitor_hit,
                    stage="get_comprehensive_market_data",
                )
                market_row = _pick_market_row_by_product(df_market, variety, latest_used_contract)
            curr_iv = None
            iv_rank = None

            if market_row is not None:
                curr_iv_val = pd.to_numeric(market_row.get("当前IV"), errors="coerce")
                iv_rank_val = pd.to_numeric(market_row.get("IV Rank"), errors="coerce")
                if pd.notna(curr_iv_val) and pd.notna(iv_rank_val):
                    curr_iv = float(curr_iv_val)
                    iv_rank = float(iv_rank_val)

            # Fallback: keep page available when market snapshot has no usable row.
            if curr_iv is None or iv_rank is None:
                df_rank_base = df_iv.copy()
                df_rank_base["iv"] = pd.to_numeric(df_rank_base["iv"], errors="coerce")
                df_rank_base = df_rank_base[df_rank_base["iv"] > 0.0001].tail(252)
                if not df_rank_base.empty:
                    curr_iv = float(df_rank_base.iloc[-1]["iv"])
                    max_iv = float(df_rank_base["iv"].max())
                    min_iv = float(df_rank_base["iv"].min())
                    iv_rank = (curr_iv - min_iv) / (max_iv - min_iv) * 100 if max_iv > min_iv else 0.0
                else:
                    curr_iv = float(pd.to_numeric(df_iv.iloc[-1]["iv"], errors="coerce") or 0.0)
                    max_iv = curr_iv
                    min_iv = curr_iv
                    iv_rank = 0.0
            else:
                df_rank_base = df_iv.copy()
                df_rank_base["iv"] = pd.to_numeric(df_rank_base["iv"], errors="coerce")
                df_rank_base = df_rank_base[df_rank_base["iv"] > 0.0001].tail(252)
                if not df_rank_base.empty:
                    max_iv = float(df_rank_base["iv"].max())
                    min_iv = float(df_rank_base["iv"].min())
                else:
                    max_iv = curr_iv
                    min_iv = curr_iv

            if iv_rank < 20:
                status = "🟢 偏低 (买方有利)"
            elif iv_rank < 60:
                status = "🔵 正常"
            elif iv_rank < 85:
                status = "🟠 偏高"
            else:
                status = "🔴 极高 (卖方有利)"

            c1, c2, c3, c4 = st.columns(4)
            c1.metric("当前 IV", f"{curr_iv:.2f}%")
            c2.metric("IV Rank (年)", f"{iv_rank:.1f}", help="与排行榜口径一致（同源数据）")
            c3.metric("历史最高 / 最低", f"{max_iv:.1f}% / {min_iv:.1f}%")
            c4.info(f"📳 状态: **{status}**")
            st.divider()


        # --- K线数据处理 ---
        st.subheader(f"历史日线与波动率")
        chart_k = df_kline.rename(columns={'trade_date': 'time'})
        chart_k['time'] = pd.to_datetime(chart_k['time']).dt.strftime('%Y-%m-%d')
        chart_k = chart_k[['time', 'open', 'high', 'low', 'close']]

        # --- IV数据处理 ---
        chart_iv = pd.DataFrame()
        if df_iv is not None and not df_iv.empty:
            df_iv['time'] = pd.to_datetime(df_iv['trade_date']).dt.strftime('%Y-%m-%d')

            # 【修改点 1】定义线条名称变量，保证前后一致
            line_name = '隐含波动率 (IV)'

            # 【修改点 2】将列名重命名为 line_name (而不是 'value')
            chart_iv = df_iv[['time', 'iv']].rename(columns={'iv': line_name})

            # 【修改点 3】过滤时也使用这个变量名
            chart_iv = chart_iv[chart_iv[line_name] > 0]  # 过滤无效IV

        # --- 绘图 ---
        chart = StreamlitChart(height=500)
        chart.legend(visible=True)
        chart.grid(vert_enabled=False, horz_enabled=False)

        # 1. K线 (右轴)
        chart.candle_style(up_color='#ef232a', down_color='#14b143', border_up_color='#ef232a',
                           border_down_color='#14b143', wick_up_color='#ef232a', wick_down_color='#14b143')
        chart.set(chart_k)

        # 2. IV (左轴)
        if not chart_iv.empty:
            # 注意：这里的 name 参数是图例上显示的名称，跟 DataFrame 列名无关
            # DataFrame 列名必须是 'time' 和 'value'
            line = chart.create_line(name=line_name, color='#2962FF', width=2, price_scale_id='left')
            line.set(chart_iv)

        chart.load()

        # 主连特有的提示：告诉用户当前用的是哪个合约
        if is_continuous and not df_iv.empty:
            last_row = df_iv.iloc[-1]
            used = last_row.get('used_contract')
            if used:
                st.info(f"💡 当前主力合约参考: **{used}** (IV 计算基于此合约)")


    else:
        st.warning(f"暂无 {target_contract} 的 K 线数据。")
        if is_continuous:
            st.caption("提示：可能是数据库中 futures_price 表缺少主连代码（如 IF 或 IF0）。")

_perf_page_log(
    page=PAGE_NAME,
    render_ms=(time.perf_counter() - _PAGE_T0) * 1000,
    cache_hit=-1,
    stage="page_done",
)




