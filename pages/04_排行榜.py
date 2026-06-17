import streamlit as st
import pandas as pd
import data_engine as de
import html
from st_aggrid import AgGrid, GridOptionsBuilder, JsCode, ColumnsAutoSizeMode
import time
from market_monitor_grid import (
    AG_GRID_LOCALE_ZH_CN,
    GRID_NUMBER_COMPARATOR,
    GRID_NUMBER_FILTER_PARAMS,
    make_grid_number_filter_value_getter,
)
from ui_components import inject_sidebar_toggle_style

# ============================================================
# 页面配置
# ============================================================

st.set_page_config(
    page_title="爱波塔-全市场监控波动率",
    page_icon="favicon.ico",
    layout="wide",
    initial_sidebar_state="expanded"
)



# 🔥 添加统一的侧边栏导航
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from sidebar_navigation import show_navigation
with st.sidebar:
    show_navigation()

# ============================================================
# 🎯 配置区域 - 在这里自定义品种名称和分类
# ============================================================

# 品种名称映射（合约代码前缀 -> 显示名称）
# 根据你的实际合约代码修改
PRODUCT_NAMES = {
    # 股指
    "IO": "沪深300",
    "MO": "中证1000",
    "HO": "上证50",
    "IF": "沪深300股指",
    "IH": "上证50股指",
    "IM": "中证1000股指",
    "IC": "中证500股指",

    # 农产品
    "C": "玉米",
    "CS": "淀粉",
    "A": "豆一",
    "B": "豆二",
    "M": "豆粕",
    "Y": "豆油",
    "P": "棕榈油",
    "OI": "菜油",
    "RM": "菜粕",
    "SR": "白糖",
    "CF": "棉花",
    "AP": "苹果",
    "CJ": "红枣",
    "PK": "花生",
    "JD": "鸡蛋",
    "LH": "生猪",

    # 工业品
    "RB": "螺纹钢",
    "HC": "热卷",
    "I": "铁矿石",
    "J": "焦炭",
    "JM": "焦煤",
    "SM": "锰硅",
    "SF": "硅铁",
    "SS": "不锈钢",
    "WR": "线材",
    "FG": "玻璃",
    "SA": "纯碱",
    "SP": "纸浆",

    # 化工
    "L": "塑料",
    "PP": "聚丙烯",
    "V": "PVC",
    "EB": "苯乙烯",
    "EG": "乙二醇",
    "PF": "短纤",
    "TA": "PTA",
    "MA": "甲醇",
    "UR": "尿素",
    "RU": "橡胶",
    "NR": "20号胶",
    "BR": "丁苯橡胶",
    "FU": "燃油",
    "LU": "低硫燃油",
    "BU": "沥青",
    "SC": "原油",
    "PG": "LPG",
    "PX": "PX",
    "BZ": "纯苯",
    "PL": "丙烯",
    "PR": "瓶片",

    # 有色金属
    "CU": "铜",
    "AL": "铝",
    "ZN": "锌",
    "PB": "铅",
    "NI": "镍",
    "SN": "锡",
    "BC": "国际铜",
    "AO": "氧化铝",

    # 贵金属
    "AU": "黄金",
    "AG": "白银",
    "PT": "铂金",
    "PD": "钯金",

    # 新能源
    "SI": "工业硅",
    "LC": "碳酸锂",
    "PS": "多晶硅",


    # ETF期权
    "510050": "50ETF",
    "510300": "300ETF沪",
    "510500": "500ETF",
    "159901": "深100ETF",
    "159915": "创业板ETF",
    "159919": "300ETF深",
    "588000": "科创50ETF",
    "588080": "科创板ETF",
}

# 品种分类映射（合约代码前缀 -> 分类）
PRODUCT_CATEGORY = {
    # 股指
    "IO": "股指", "MO": "股指", "HO": "股指",
    "IF": "股指", "IH": "股指", "IM": "股指","IC": "股指",
    "510050": "股指", "510300": "股指", "510500": "股指",
    "159901": "股指", "159915": "股指", "159919": "股指",
    "588000": "股指", "588080": "股指",

    # 农产品
    "C": "农产", "CS": "农产", "A": "农产", "B": "农产",
    "M": "农产", "Y": "农产", "P": "农产", "OI": "农产",
    "RM": "农产", "SR": "农产", "CF": "农产", "AP": "农产",
    "CJ": "农产", "PK": "农产", "JD": "农产", "LH": "农产",

    # 工业品
    "RB": "工业", "HC": "工业", "I": "工业", "J": "工业","RU": "工业",
    "JM": "工业", "SM": "工业", "SF": "工业", "SS": "工业","BR": "工业",
    "WR": "工业", "FG": "工业", "SA": "工业", "SP": "工业","AO": "工业",

    # 化工
    "L": "化工", "PP": "化工", "V": "化工", "EB": "化工",
    "EG": "化工", "PF": "化工", "TA": "化工", "MA": "化工",
    "UR": "化工", "PR": "化工", "FU": "化工","PG": "化工",
    "LU": "化工", "BU": "化工", "SC": "化工","SH": "化工",
    "BR": "化工", "PX": "化工", "BZ": "化工", "PL": "化工",

    # 有色金属
    "CU": "有色", "AL": "有色", "ZN": "有色", "PB": "有色",
    "NI": "有色", "SN": "有色",

    # 贵金属
    "AU": "贵金属", "AG": "贵金属", "PT": "贵金属","PD": "贵金属",

    # 新能源
    "SI": "新能源", "LC": "新能源", "PS": "新能源",
}

# 分类列表（按显示顺序）
CATEGORIES = ["全部", "股指", "农产", "工业", "化工", "有色", "贵金属", "新能源"]


# ============================================================
# 辅助函数
# ============================================================

def extract_product_code(contract_name):
    """从合约名称中提取品种代码"""
    # ETF期权（数字开头）
    if contract_name and contract_name[0].isdigit():
        return contract_name[:6]
    # 商品/股指期权（字母开头）
    code = ""
    for char in contract_name:
        if char.isalpha():
            code += char.upper()
        else:
            break
    return code


def get_product_name(contract_name):
    """获取品种显示名称"""
    code = extract_product_code(contract_name)
    return PRODUCT_NAMES.get(code, code)


def get_product_category(contract_name):
    """获取品种分类"""
    code = extract_product_code(contract_name)
    return PRODUCT_CATEGORY.get(code, "其他")


def to_float(value):
    """把表格中的百分比、千分位数字、N/A 安全转换成浮点数。"""
    if value is None:
        return None
    if isinstance(value, str):
        cleaned = value.replace(",", "").replace("%", "").strip()
        if cleaned in {"", "-", "N/A", "nan", "None", "快到期"}:
            return None
    else:
        cleaned = value
    try:
        number = float(cleaned)
    except (TypeError, ValueError):
        return None
    if pd.isna(number):
        return None
    return number


def count_by_rule(df, column, rule):
    if column not in df.columns:
        return 0
    return int(df[column].apply(lambda value: (to_float(value) is not None) and rule(to_float(value))).sum())


SCAN_FILTERS = [
    {"key": "全部", "label": "全部扫描", "hint": "当前结果"},
    {"key": "高IV", "label": "高IV Rank", "hint": ">= 80"},
    {"key": "低IV", "label": "低IV Rank", "hint": "<= 20"},
    {"key": "IV升幅", "label": "IV日升幅", "hint": ">= 0.30"},
    {"key": "价格异动", "label": "价格异动", "hint": "|日涨跌| >= 1%"},
    {"key": "机构流入", "label": "机构流入", "hint": "日变动 >= 1,000"},
    {"key": "散户流出", "label": "散户流出", "hint": "日变动 <= -1,000"},
    {"key": "缺IV", "label": "缺IV数据", "hint": "无有效IV"},
]


def build_scan_metrics(df):
    missing_iv = 0
    if {"当前IV", "IV Rank"}.issubset(df.columns):
        missing_iv = int(
            df.apply(
                lambda row: to_float(row.get("当前IV")) is None or to_float(row.get("IV Rank")) is None,
                axis=1,
            ).sum()
        )

    return {
        "全部": len(df),
        "高IV": count_by_rule(df, "IV Rank", lambda value: value >= 80),
        "低IV": count_by_rule(df, "IV Rank", lambda value: value <= 20),
        "IV升幅": count_by_rule(df, "IV变动(日)", lambda value: value >= 0.30),
        "价格异动": count_by_rule(df, "涨跌%(日)", lambda value: abs(value) >= 1),
        "机构流入": count_by_rule(df, "机构变动(日)", lambda value: value >= 1000),
        "散户流出": count_by_rule(df, "散户变动(日)", lambda value: value <= -1000),
        "缺IV": missing_iv,
    }


def apply_text_search(df, search_term):
    if not search_term:
        return df
    mask = df['合约'].str.contains(search_term, case=False, na=False) | df['品种'].str.contains(search_term, case=False, na=False)
    return df[mask]


def build_scan_tags(row):
    tags = []
    iv_rank = to_float(row.get("IV Rank"))
    current_iv = to_float(row.get("当前IV"))
    iv_change = to_float(row.get("IV变动(日)"))
    price_change = to_float(row.get("涨跌%(日)"))
    inst_change = to_float(row.get("机构变动(日)"))
    retail_change = to_float(row.get("散户变动(日)"))

    if str(row.get("IV Rank", "")).strip() == "快到期":
        tags.append("快到期")
    elif current_iv is None or iv_rank is None:
        tags.append("缺IV")
    elif iv_rank >= 80:
        tags.append("高波")
    elif iv_rank <= 20:
        tags.append("低波")

    if iv_change is not None and iv_change >= 0.30:
        tags.append("IV升")
    if price_change is not None and abs(price_change) >= 1:
        tags.append("价异动")
    if inst_change is not None and inst_change >= 1000:
        tags.append("机构流入")
    if retail_change is not None and retail_change <= -1000:
        tags.append("散户流出")

    return tags[:3] or ["观察"]


def add_scan_signal_column(df):
    df_with_signal = df.copy()
    if "扫描信号" in df_with_signal.columns:
        return df_with_signal
    insert_at = 1 if "合约" in df_with_signal.columns else 0
    signals = df_with_signal.apply(lambda row: "|".join(build_scan_tags(row)), axis=1)
    df_with_signal.insert(insert_at, "扫描信号", signals)
    return df_with_signal


def format_percent(value):
    number = to_float(value)
    return "-" if number is None else f"{number:.2f}%"


def format_signed_number(value, decimals=2, thousands=False):
    number = to_float(value)
    if number is None:
        return "-"
    sign = "+" if number > 0 else ""
    if thousands:
        return f"{sign}{int(number):,}"
    return f"{sign}{number:.{decimals}f}"


def format_grid_iv_rank(value):
    if str(value).strip() == "快到期":
        return "快到期"
    number = to_float(value)
    if number is None:
        return "N/A"
    return f"{number:.0f}"


def tone_class(value):
    number = to_float(value)
    if number is None or number == 0:
        return "flat"
    return "up" if number > 0 else "down"


def apply_grid_column_groups(grid_options):
    column_defs = grid_options.get("columnDefs", [])
    by_field = {
        column.get("field"): column
        for column in column_defs
        if column.get("field")
    }
    used_fields = set()

    def children(fields):
        grouped_children = []
        for field in fields:
            if field in by_field:
                grouped_children.append(by_field[field])
                used_fields.add(field)
        return grouped_children

    groups = []
    for group_name, fields in [
        ("合约", ["合约", "扫描信号"]),
        ("波动率", ["当前IV", "IV Rank", "IV变动(日)", "IV变动(5日)"]),
        ("价格", ["涨跌%(日)", "涨跌%(5日)"]),
        ("持仓资金", ["散户变动(日)", "散户变动(5日)", "机构变动(日)", "机构变动(5日)"]),
    ]:
        grouped_children = children(fields)
        if grouped_children:
            groups.append({
                "headerName": group_name,
                "marryChildren": True,
                "children": grouped_children,
            })

    leftovers = [column for column in column_defs if column.get("field") not in used_fields]
    grid_options["columnDefs"] = groups + leftovers
    grid_options["groupHeaderHeight"] = 34
    grid_options["headerHeight"] = 36
    return grid_options


def focus_metric_html(label, value, tone="flat"):
    return (
        f'<div class="focus-kpi">'
        f'<div class="focus-kpi-label">{html.escape(label)}</div>'
        f'<div class="focus-kpi-value {tone}">{html.escape(str(value))}</div>'
        f'</div>'
    )


def render_focus_panel(row):
    contract = html.escape(str(row.get("合约", "-")))
    product = html.escape(str(row.get("品种", "-")))
    category = html.escape(str(row.get("分类", "-")))
    tags = "".join(
        f"<span class='focus-tag'>{html.escape(tag)}</span>"
        for tag in build_scan_tags(row)
    )
    iv_rank_value = to_float(row.get("IV Rank"))
    iv_rank_display = "-" if iv_rank_value is None else f"{iv_rank_value:.0f}"

    overview_metrics = [
        focus_metric_html("当前IV", format_percent(row.get("当前IV"))),
        focus_metric_html("IV Rank", iv_rank_display),
        focus_metric_html("IV变动(日)", format_signed_number(row.get("IV变动(日)")), tone_class(row.get("IV变动(日)"))),
        focus_metric_html("IV变动(5日)", format_signed_number(row.get("IV变动(5日)")), tone_class(row.get("IV变动(5日)"))),
    ]
    price_metrics = [
        focus_metric_html("涨跌%(日)", format_signed_number(row.get("涨跌%(日)")), tone_class(row.get("涨跌%(日)"))),
        focus_metric_html("涨跌%(5日)", format_signed_number(row.get("涨跌%(5日)")), tone_class(row.get("涨跌%(5日)"))),
    ]
    holding_metrics = [
        focus_metric_html("散户(日)", format_signed_number(row.get("散户变动(日)"), decimals=0, thousands=True), tone_class(row.get("散户变动(日)"))),
        focus_metric_html("散户(5日)", format_signed_number(row.get("散户变动(5日)"), decimals=0, thousands=True), tone_class(row.get("散户变动(5日)"))),
        focus_metric_html("机构(日)", format_signed_number(row.get("机构变动(日)"), decimals=0, thousands=True), tone_class(row.get("机构变动(日)"))),
        focus_metric_html("机构(5日)", format_signed_number(row.get("机构变动(5日)"), decimals=0, thousands=True), tone_class(row.get("机构变动(5日)"))),
    ]

    panel_html = (
        f'<div class="focus-panel">'
        f'<div class="focus-contract">{contract}</div>'
        f'<div class="focus-meta">{product} · {category}</div>'
        f'<div class="focus-tags">{tags}</div>'
        f'<div class="focus-block">'
        f'<div class="focus-block-title">波动率概览</div>'
        f'<div class="focus-grid">{"".join(overview_metrics)}</div>'
        f'</div>'
        f'<div class="focus-block">'
        f'<div class="focus-block-title">价格表现</div>'
        f'<div class="focus-grid">{"".join(price_metrics)}</div>'
        f'</div>'
        f'<div class="focus-block">'
        f'<div class="focus-block-title">持仓资金</div>'
        f'<div class="focus-grid">{"".join(holding_metrics)}</div>'
        f'</div>'
        f'<div class="focus-block">'
        f'<div class="focus-block-title">扫描备注</div>'
        f'<div class="focus-note">右侧面板用于快速核对单一合约，表格仍保留完整排序和筛选能力。</div>'
        f'</div>'
        f'</div>'
    )
    st.markdown(panel_html, unsafe_allow_html=True)


# ============================================================
# 全局CSS样式
# ============================================================

st.markdown("""
<style>
    /* === 基础重置 === */
    .block-container {
        padding: 1.5rem 2rem !important;
        max-width: 100% !important;
    }
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}

    /* === 深色主题 === */
    .stApp {
        background: linear-gradient(135deg, #0a0f1a 0%, #0d1526 50%, #0a1628 100%);
        color: #e2e8f0;
    }

    /* === 侧边栏 === */
    section[data-testid="stSidebar"] {
        background: linear-gradient(180deg, #0f172a 0%, #1e293b 100%);
        border-right: 1px solid #334155;
    }
    section[data-testid="stSidebar"] p, 
    section[data-testid="stSidebar"] span, 
    section[data-testid="stSidebar"] div {
        color: #94a3b8 !important;
    }

    /* === 标题 === */
    h1, h2, h3 {
        color: #f8fafc !important; 
        font-family: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif;
        font-weight: 600;
    }

    /* === 输入框 === */
    div[data-testid="stTextInput"] input {
        background-color: rgba(30, 41, 59, 0.8) !important;
        border: 1px solid #475569 !important;
        color: #e2e8f0 !important;
        border-radius: 10px;
        padding: 12px 16px;
    }
    div[data-testid="stTextInput"] input:focus {
        border-color: #3b82f6 !important;
        box-shadow: 0 0 0 2px rgba(59, 130, 246, 0.2) !important;
    }

    /* === 按钮 === */
    div.stButton > button {
        background: linear-gradient(135deg, #1e40af, #3b82f6) !important;
        color: #ffffff !important;
        border: none !important;
        border-radius: 10px;
        padding: 10px 24px;
        font-weight: 600;
        box-shadow: 0 4px 15px rgba(59, 130, 246, 0.3);
        transition: all 0.2s ease;
    }
    div.stButton > button:hover {
        box-shadow: 0 6px 20px rgba(59, 130, 246, 0.5);
        transform: translateY(-1px);
    }

    /* === Tab样式 === */
    .stTabs [data-baseweb="tab-list"] {
        gap: 8px;
        background-color: rgba(30, 41, 59, 0.5);
        padding: 6px;
        border-radius: 12px;
        border: 1px solid rgba(71, 85, 105, 0.3);
    }
    .stTabs [data-baseweb="tab"] {
        background-color: transparent;
        border-radius: 8px;
        color: #94a3b8;
        font-weight: 500;
        padding: 10px 20px;
    }
    .stTabs [aria-selected="true"] {
        background: linear-gradient(135deg, #3b82f6, #2563eb) !important;
        color: #ffffff !important;
        box-shadow: 0 4px 12px rgba(59, 130, 246, 0.3);
    }
    .stTabs [data-baseweb="tab-panel"] {
        padding-top: 1rem;
    }

    /* === AgGrid表格 === */
    .stAgGrid {
        background-color: transparent !important;
    }
    .ag-root-wrapper {
        border: 1px solid rgba(71, 85, 105, 0.3) !important;
        border-radius: 16px !important;
        overflow: hidden;
    }

    /* === Expander === */
    .streamlit-expanderHeader {
        background-color: rgba(30, 41, 59, 0.5) !important;
        border-radius: 10px;
    }

    /* === Metric卡片 === */
    [data-testid="stMetric"] {
        background: linear-gradient(135deg, rgba(30, 41, 59, 0.8), rgba(15, 23, 42, 0.9));
        border: 1px solid rgba(71, 85, 105, 0.3);
        border-radius: 16px;
        padding: 20px;
    }
    [data-testid="stMetricLabel"] {
        color: #94a3b8 !important;
    }
    [data-testid="stMetricValue"] {
        color: #f1f5f9 !important;
        font-weight: 700 !important;
    }

    /* === 扫描工作台 === */
    .scan-toolbar {
        margin-top: 18px;
        padding: 14px 16px;
        background: rgba(15, 23, 42, 0.72);
        border: 1px solid rgba(71, 85, 105, 0.36);
        border-radius: 14px;
    }
    .scan-title {
        color: #e2e8f0;
        font-size: 14px;
        font-weight: 700;
        margin-bottom: 4px;
    }
    .scan-subtitle {
        color: #94a3b8;
        font-size: 12px;
        line-height: 1.5;
    }
    .scan-card {
        min-height: 86px;
        padding: 14px 16px;
        background: linear-gradient(180deg, rgba(30, 41, 59, 0.86), rgba(15, 23, 42, 0.92));
        border: 1px solid rgba(71, 85, 105, 0.45);
        border-radius: 12px;
        box-shadow: 0 14px 30px rgba(2, 6, 23, 0.26);
    }
    .scan-card-label {
        color: #cbd5e1;
        font-size: 13px;
        font-weight: 700;
        margin-bottom: 8px;
    }
    .scan-card-rule {
        color: #64748b;
        font-size: 11px;
        margin-top: 5px;
    }
    .scan-card-value {
        color: #f8fafc;
        font-size: 24px;
        line-height: 1;
        font-weight: 800;
        letter-spacing: 0;
        font-family: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif;
    }
    .scan-card-high { border-color: rgba(248, 113, 113, 0.32); }
    .scan-card-low { border-color: rgba(74, 222, 128, 0.32); }
    .scan-card-focus { border-color: rgba(59, 130, 246, 0.36); }
    .st-key-category_filter [data-testid="stWidgetLabel"] {
        display: none;
    }
    .st-key-category_filter [data-baseweb="select"] > div {
        min-height: 86px;
        padding: 14px 16px;
        background: linear-gradient(180deg, rgba(30, 41, 59, 0.9), rgba(15, 23, 42, 0.96)) !important;
        border: 1px solid rgba(71, 85, 105, 0.4) !important;
        border-radius: 12px !important;
        box-shadow: 0 14px 30px rgba(2, 6, 23, 0.26);
        color: #f8fafc !important;
    }
    .st-key-category_filter [data-baseweb="select"] > div div,
    .st-key-category_filter [data-baseweb="select"] > div span {
        color: #f8fafc !important;
        font-size: 15px !important;
        font-weight: 800 !important;
        letter-spacing: 0;
        opacity: 1 !important;
    }
    .st-key-category_filter [data-baseweb="select"] svg {
        color: #94a3b8 !important;
        fill: #94a3b8 !important;
    }
    .scan-table-gap {
        height: 22px;
    }
    .focus-panel {
        background: rgba(15, 23, 42, 0.78);
        border: 1px solid rgba(71, 85, 105, 0.42);
        border-radius: 14px;
        padding: 14px;
        min-height: 600px;
    }
    .focus-contract {
        color: #f8fafc;
        font-size: 18px;
        font-weight: 800;
        margin-bottom: 2px;
    }
    .focus-meta {
        color: #94a3b8;
        font-size: 12px;
        margin-bottom: 12px;
    }
    .focus-tags {
        display: flex;
        flex-wrap: wrap;
        gap: 6px;
        margin-bottom: 14px;
    }
    .focus-tag {
        padding: 4px 8px;
        border-radius: 999px;
        background: rgba(59, 130, 246, 0.16);
        border: 1px solid rgba(96, 165, 250, 0.24);
        color: #bfdbfe;
        font-size: 11px;
        font-weight: 700;
    }
    .focus-block {
        border-top: 1px solid rgba(71, 85, 105, 0.32);
        padding-top: 12px;
        margin-top: 12px;
    }
    .focus-block-title {
        color: #cbd5e1;
        font-size: 12px;
        font-weight: 700;
        margin-bottom: 10px;
    }
    .focus-grid {
        display: grid;
        grid-template-columns: 1fr 1fr;
        gap: 8px;
    }
    .focus-kpi {
        background: rgba(30, 41, 59, 0.58);
        border: 1px solid rgba(71, 85, 105, 0.24);
        border-radius: 10px;
        padding: 9px 10px;
    }
    .focus-kpi-label {
        color: #94a3b8;
        font-size: 11px;
        margin-bottom: 5px;
    }
    .focus-kpi-value {
        color: #e2e8f0;
        font-size: 15px;
        font-weight: 800;
        letter-spacing: 0;
    }
    .focus-kpi-value.up { color: #f87171; }
    .focus-kpi-value.down { color: #4ade80; }
    .focus-kpi-value.flat { color: #94a3b8; }
    .focus-note {
        color: #94a3b8;
        font-size: 12px;
        line-height: 1.6;
        margin-top: 10px;
    }
    .empty-hint {
        padding: 24px;
        border: 1px dashed rgba(148, 163, 184, 0.32);
        border-radius: 12px;
        color: #94a3b8;
        text-align: center;
        background: rgba(15, 23, 42, 0.46);
    }

    /* === 分隔线 === */
    hr {
        border-color: rgba(71, 85, 105, 0.3) !important;
        margin: 1.5rem 0 !important;
    }
</style>
""", unsafe_allow_html=True)
inject_sidebar_toggle_style(mode="high_contrast")

# ============================================================
# 页面标题区域
# ============================================================

col_title, col_info_top, col_refresh = st.columns([4.2, 1.2, 1.2], gap="large")


def _format_latest_date(date_text):
    raw = str(date_text or "").replace("-", "").replace("/", "").strip()
    if len(raw) == 8 and raw.isdigit():
        return f"{raw[:4]}-{raw[4:6]}-{raw[6:]}"
    return raw or "未知"


def get_latest_date_from_market_data(df):
    if df is None or df.empty or '_数据日期' not in df.columns:
        return ""
    latest_values = df['_数据日期'].dropna().astype(str)
    if latest_values.empty:
        return ""
    return latest_values.iloc[0]


with col_title:
    st.markdown("""
    <div style="display: flex; align-items: center; gap: 16px; margin-bottom: 8px;">
        <div style="
            width: 52px; height: 52px;
            background: linear-gradient(135deg, #3b82f6, #8b5cf6);
            border-radius: 14px;
            display: flex; align-items: center; justify-content: center;
            font-size: 28px;
            box-shadow: 0 0 30px rgba(59, 130, 246, 0.4);
        ">📊</div>
        <div>
            <h1 style="
                margin: 0; font-size: 32px; font-weight: 700;
                background: linear-gradient(90deg, #f8fafc, #94a3b8);
                -webkit-background-clip: text;
                -webkit-text-fill-color: transparent;
                letter-spacing: -0.5px;
            ">全商品监控</h1>
            <p style="margin: 4px 0 0; font-size: 14px; color: #64748b;">
                追踪波动率和持仓数据动向
            </p>
        </div>
    </div>
    """, unsafe_allow_html=True)

with col_info_top:
    with st.expander("数据说明", expanded=False):
        st.markdown("""
**指标解释**
- **IV**：Implied volatility（隐含波动率）
- **IV Rank**：当前隐含波动率在最近一年中的百分位排名
- **散户变动**：散户集中期货商净持仓变化，偏反向观察
- **机构变动**：机构集中期货商净持仓变化，偏顺向观察

**扫描建议**
- IV Rank > 80：波动率偏贵，关注卖方机会
- IV Rank < 20：波动率偏便宜，关注买方机会
- 机构流入 + 散户流出：作为资金共振线索继续核对
        """)

refresh_requested = False
with col_refresh:
    refresh_requested = st.button("🔄 刷新数据", key="refresh_btn", use_container_width=True)
    latest_date_placeholder = st.empty()
    latest_date_placeholder.markdown(
        f"""
        <div style="margin-top:6px; text-align:right; font-size:12px; color:#94a3b8;">
            最新数据日期：<span style="color:#e2e8f0; font-weight:600;">读取中</span>
        </div>
        """,
        unsafe_allow_html=True,
    )


# ============================================================
# 数据加载
# ============================================================

@st.cache_data(ttl=1800)  # 缓存30分钟
def load_data():
    """加载并处理数据"""
    df = de.get_comprehensive_market_data()
    if not df.empty:
        # 添加品种名称和分类
        df['品种'] = df['合约'].apply(get_product_name)
        df['分类'] = df['合约'].apply(get_product_category)
    return df


if refresh_requested:
    # 仅清理本页 load_data 缓存，避免全局缓存被清空后引发整站重算。
    load_data.clear()
    if hasattr(de, "clear_comprehensive_market_data_snapshot"):
        de.clear_comprehensive_market_data_snapshot()


# 数据加载：保留缓存，移除固定 sleep，避免命中缓存时仍然“看起来很慢”
start_time = time.time()
if refresh_requested:
    with st.spinner("🔄 正在刷新排行榜数据..."):
        df_monitor = load_data()
else:
    df_monitor = load_data()
load_time = time.time() - start_time
latest_date_display = _format_latest_date(get_latest_date_from_market_data(df_monitor))
latest_date_placeholder.markdown(
    f"""
    <div style="margin-top:6px; text-align:right; font-size:12px; color:#94a3b8;">
        最新数据日期：<span style="color:#e2e8f0; font-weight:600;">{latest_date_display}</span>
    </div>
    """,
    unsafe_allow_html=True,
)

# ============================================================
# 主内容区域
# ============================================================

if not df_monitor.empty:

    st.markdown("<div style='height: 14px'></div>", unsafe_allow_html=True)

    scan_metrics = build_scan_metrics(df_monitor)
    scan_card_styles = {
        "高IV": "scan-card-high",
        "低IV": "scan-card-low",
        "缺IV": "scan-card-focus",
    }
    search_term = ""
    df_searched = apply_text_search(df_monitor, search_term).copy()

    category_counts = {}
    for category_name in CATEGORIES:
        if category_name == "全部":
            category_counts[category_name] = len(df_searched)
        else:
            category_counts[category_name] = len(df_searched[df_searched['分类'] == category_name])
    category_label_lookup = {
        category: (
            f"全部扫描 · {category_counts.get(category, 0):,}"
            if category == "全部"
            else f"{category} · {category_counts.get(category, 0):,}"
        )
        for category in CATEGORIES
    }

    scan_cols = st.columns(len(SCAN_FILTERS), gap="small")
    with scan_cols[0]:
        selected_category = st.selectbox(
            "扫描范围",
            options=CATEGORIES,
            index=0,
            format_func=lambda category: category_label_lookup.get(category, category),
            key="category_filter",
            label_visibility="collapsed",
        )
    for scan_col, item in zip(scan_cols[1:], SCAN_FILTERS[1:]):
        css_class = scan_card_styles.get(item["key"], "scan-card")
        if css_class != "scan-card":
            css_class = f"scan-card {css_class}"
        value = scan_metrics.get(item["key"], 0)
        with scan_col:
            st.markdown(
                f"""
                <div class="{css_class}">
                    <div class="scan-card-label">{html.escape(item["label"])}</div>
                    <div class="scan-card-value">{value:,}</div>
                    <div class="scan-card-rule">{html.escape(item["hint"])}</div>
                </div>
                """,
                unsafe_allow_html=True,
            )

    st.markdown("<div class='scan-table-gap'></div>", unsafe_allow_html=True)

    df_scanned = df_searched.copy()

    # === 分类内容 ===
    category_container = st.container()

    for category in [selected_category]:
        with category_container:
            # 筛选数据
            if category == "全部":
                df_filtered = df_scanned.copy()
            else:
                df_filtered = df_scanned[df_scanned['分类'] == category].copy()

            if df_filtered.empty:
                st.markdown(
                    f"<div class='empty-hint'>暂无符合「{html.escape(category_label_lookup.get(category, category))}」的合约</div>",
                    unsafe_allow_html=True,
                )
                continue

            df_filtered = add_scan_signal_column(df_filtered)

            # 移除品种和分类列（右侧聚焦面板使用，不在表格中重复显示）
            display_columns = [col for col in df_filtered.columns if col not in ['品种', '分类', '_数据日期']]
            if "合约" in display_columns and "扫描信号" in display_columns:
                display_columns = ["合约", "扫描信号"] + [
                    col for col in display_columns if col not in ["合约", "扫描信号"]
                ]
            df_display = df_filtered[display_columns].copy()
            if "IV Rank" in df_display.columns:
                df_display["IV Rank"] = df_display["IV Rank"].apply(format_grid_iv_rank)

            # === AgGrid配置 ===
            gb = GridOptionsBuilder.from_dataframe(df_display)

            # 基础配置
            gb.configure_default_column(
                resizable=True,
                filter=True,
                sortable=True,
                cellStyle={
                    'display': 'flex',
                    'alignItems': 'center',
                    'justifyContent': 'flex-end',
                    'paddingRight': '12px'
                },
                minWidth=90
            )

            # 合约列
            gb.configure_column("合约",
                                pinned='left',
                                width=220,
                                filter=False,
                                cellStyle={
                                    'fontWeight': '600',
                                    'color': '#f1f5f9',
                                    'justifyContent': 'flex-start',
                                    'paddingLeft': '12px'
                                }
                                )

            signal_renderer = JsCode("""
            class SignalRenderer {
                init(params) {
                    const raw = params.value || '观察';
                    const colorMap = {
                        '高波': ['rgba(220, 38, 38, 0.25)', '#fecaca', 'rgba(248, 113, 113, 0.28)'],
                        '低波': ['rgba(22, 163, 74, 0.24)', '#bbf7d0', 'rgba(74, 222, 128, 0.28)'],
                        '缺IV': ['rgba(100, 116, 139, 0.26)', '#cbd5e1', 'rgba(148, 163, 184, 0.24)'],
                        '快到期': ['rgba(217, 119, 6, 0.22)', '#fde68a', 'rgba(251, 191, 36, 0.28)'],
                        'IV升': ['rgba(37, 99, 235, 0.24)', '#bfdbfe', 'rgba(96, 165, 250, 0.28)'],
                        '价异动': ['rgba(126, 34, 206, 0.22)', '#ddd6fe', 'rgba(168, 85, 247, 0.28)'],
                        '机构流入': ['rgba(15, 118, 110, 0.24)', '#99f6e4', 'rgba(45, 212, 191, 0.28)'],
                        '散户流出': ['rgba(30, 64, 175, 0.24)', '#bfdbfe', 'rgba(96, 165, 250, 0.28)'],
                        '观察': ['rgba(51, 65, 85, 0.26)', '#cbd5e1', 'rgba(148, 163, 184, 0.2)'],
                    };
                    const tags = String(raw).split('|').filter(Boolean);
                    this.eGui = document.createElement('div');
                    this.eGui.style.display = 'flex';
                    this.eGui.style.gap = '4px';
                    this.eGui.style.alignItems = 'center';
                    this.eGui.style.overflow = 'hidden';
                    this.eGui.style.width = '100%';
                    this.eGui.innerHTML = tags.map(tag => {
                        const colors = colorMap[tag] || colorMap['观察'];
                        return `<span style="
                            padding: 2px 6px;
                            border-radius: 999px;
                            background: ${colors[0]};
                            border: 1px solid ${colors[2]};
                            color: ${colors[1]};
                            font-size: 11px;
                            font-weight: 700;
                            white-space: nowrap;
                        ">${tag}</span>`;
                    }).join('');
                }
                getGui() { return this.eGui; }
            }
            """)

            gb.configure_column("扫描信号",
                                pinned='left',
                                width=150,
                                filter=False,
                                sortable=False,
                                cellRenderer=signal_renderer,
                                cellStyle={
                                    'justifyContent': 'flex-start',
                                    'paddingLeft': '8px',
                                    'paddingRight': '8px'
                                }
                                )

            # 涨跌配色 - 纯文字颜色，无背景
            change_style_js = JsCode("""
            function(params) {
                let val = parseFloat(params.value);
                let baseStyle = {
                    'backgroundColor': 'transparent',
                    'textAlign': 'right',
                    'paddingRight': '12px'
                };
                if (isNaN(val)) return {...baseStyle, 'color': '#64748b'};
                if (val > 0) return {...baseStyle, 'color': '#f87171', 'fontWeight': '600'};
                if (val < 0) return {...baseStyle, 'color': '#4ade80', 'fontWeight': '600'};
                return {...baseStyle, 'color': '#94a3b8'};
            }
            """)

            # IV Rank 渲染器 - 优化清晰度
            iv_rank_renderer = JsCode("""
            class IVRankRenderer {
                init(params) {
                    const val = params.value;
                    this.eGui = document.createElement('div');
                    this.eGui.style.width = '100%';
                    this.eGui.style.height = '26px';
                    this.eGui.style.position = 'relative';
                    this.eGui.style.display = 'flex';
                    this.eGui.style.alignItems = 'center';
                    this.eGui.style.justifyContent = 'center';

                    if (val === '快到期') {
                        this.eGui.innerHTML = '<span style="color: #fbbf24; font-weight: 700; font-size: 12px;">快到期</span>';
                        return;
                    }

                    if (val === null || val === undefined || val === '' || val === 'N/A') {
                        this.eGui.innerHTML = `
                            <div style="
                                width: 100%; height: 22px;
                                background: rgba(100, 116, 139, 0.24);
                                border: 1px solid rgba(148, 163, 184, 0.22);
                                border-radius: 4px;
                                display: flex;
                                align-items: center;
                                justify-content: center;
                                color: #cbd5e1;
                                font-size: 12px;
                                font-weight: 700;
                            ">N/A</div>
                        `;
                        return;
                    }

                    const numVal = parseFloat(val);
                    if (Number.isNaN(numVal)) {
                        this.eGui.innerHTML = '<span style="color: #94a3b8; font-weight: 700; font-size: 12px;">N/A</span>';
                        return;
                    }
                    let barColor, textColor;

                    if (numVal > 80) {
                        barColor = '#dc2626';
                        textColor = '#fecaca';
                    } else if (numVal < 20) {
                        barColor = '#16a34a';
                        textColor = '#bbf7d0';
                    } else {
                        barColor = '#2563eb';
                        textColor = '#bfdbfe';
                    }

                    this.eGui.innerHTML = `
                        <div style="
                            position: absolute;
                            left: 0; top: 2px;
                            width: 100%; height: 22px;
                            background: #1e293b;
                            border-radius: 4px;
                        "></div>
                        <div style="
                            position: absolute;
                            left: 0; top: 2px;
                            width: ${Math.max(0, Math.min(100, numVal))}%; height: 22px;
                            background: ${barColor};
                            border-radius: 4px;
                            opacity: 0.85;
                        "></div>
                        <span style="
                            position: relative;
                            z-index: 1;
                            font-size: 12px;
                            font-weight: 700;
                            color: ${textColor};
                        ">${numVal.toFixed(0)}</span>
                    `;
                }
                getGui() { return this.eGui; }
            }
            """)

            # IV Rank 排序器
            iv_rank_comparator = JsCode("""
            function(valueA, valueB) {
                const getVal = (v) => {
                    if (v === '快到期') return -100;
                    const parsed = parseFloat(v);
                    return Number.isNaN(parsed) ? -100 : parsed;
                };
                return getVal(valueA) - getVal(valueB);
            }
            """)

            gb.configure_column("IV Rank",
                                width=120,
                                cellRenderer=iv_rank_renderer,
                                comparator=iv_rank_comparator,
                                filter="agNumberColumnFilter",
                                filterParams=GRID_NUMBER_FILTER_PARAMS,
                                filterValueGetter=make_grid_number_filter_value_getter("IV Rank")
                                )

            # 当前IV
            gb.configure_column("当前IV",
                                width=100,
                                valueFormatter=JsCode("""
                                function(params) {
                                    const raw = params.value;
                                    if (raw === null || raw === undefined || raw === '' || raw === '-' || raw === 'N/A') return '-';
                                    const val = Number(String(raw).replace('%', '').replace(',', ''));
                                    if (!Number.isFinite(val) || val <= 0) return '-';
                                    return val.toFixed(2) + '%';
                                }
                                """),
                                comparator=GRID_NUMBER_COMPARATOR,
                                filter="agNumberColumnFilter",
                                filterParams=GRID_NUMBER_FILTER_PARAMS,
                                filterValueGetter=make_grid_number_filter_value_getter("当前IV"),
                                cellStyle={
                                    'color': '#e2e8f0',
                                    'fontWeight': '600',
                                    'textAlign': 'right',
                                    'justifyContent': 'flex-end',
                                    'paddingRight': '12px'
                                }
                                )

            # 数值格式化（保留2位小数）
            number_formatter = JsCode("""
            function(params) {
                if (!params.value && params.value !== 0) return '-';
                let val = parseFloat(params.value);
                if (isNaN(val)) return '-';
                return val.toFixed(2);
            }
            """)

            # 持仓变动格式化
            hold_formatter = JsCode("""
            function(params) {
                if (!params.value && params.value !== 0) return '-';
                let val = parseInt(params.value);
                if (isNaN(val)) return '-';
                return (val > 0 ? '+' : '') + val.toLocaleString();
            }
            """)
            # IV变动、涨跌%等数值列配置
            numeric_columns = ['IV变动(日)', 'IV变动(5日)', '涨跌%(日)', '涨跌%(5日)']
            for col in numeric_columns:
                if col in df_display.columns:
                    gb.configure_column(col,
                                        cellStyle=change_style_js,
                                        valueFormatter=number_formatter,
                                        comparator=GRID_NUMBER_COMPARATOR,
                                        filter="agNumberColumnFilter",
                                        filterParams=GRID_NUMBER_FILTER_PARAMS,
                                        filterValueGetter=make_grid_number_filter_value_getter(col),
                                        width=110
                                        )

            for col in ['散户变动(日)', '散户变动(5日)', '机构变动(日)', '机构变动(5日)']:
                if col in df_display.columns:
                    gb.configure_column(col,
                                        cellStyle=change_style_js,
                                        valueFormatter=hold_formatter,
                                        comparator=GRID_NUMBER_COMPARATOR,
                                        filter="agNumberColumnFilter",
                                        filterParams=GRID_NUMBER_FILTER_PARAMS,
                                        filterValueGetter=make_grid_number_filter_value_getter(col),
                                        width=120
                                        )

            gridOptions = gb.build()
            gridOptions["localeText"] = AG_GRID_LOCALE_ZH_CN
            gridOptions = apply_grid_column_groups(gridOptions)

            grid_col, focus_col = st.columns([4.7, 1.25], gap="small")

            with grid_col:
                # 渲染表格
                AgGrid(
                    df_display,
                    gridOptions=gridOptions,
                    height=600,
                    width='100%',
                    columns_auto_size_mode=ColumnsAutoSizeMode.FIT_ALL_COLUMNS_TO_VIEW,
                    allow_unsafe_jscode=True,
                    enable_enterprise_modules=False,
                    key=f"market_monitor_grid_{category}",
                    update_mode="NO_UPDATE",
                    update_on=[],
                    custom_css={
                    ".ag-root-wrapper": {
                        "background-color": "#0f172a !important",
                        "border": "1px solid rgba(71, 85, 105, 0.3) !important",
                        "border-radius": "12px !important"
                    },
                    ".ag-header": {
                        "background-color": "#1e293b !important",
                        "color": "#94a3b8 !important",
                        "border-bottom": "1px solid rgba(71, 85, 105, 0.4) !important",
                        "font-size": "12px",
                        "font-weight": "600"
                    },
                    ".ag-header-group-cell": {
                        "background-color": "#172235 !important",
                        "border-right": "1px solid rgba(148, 163, 184, 0.24) !important",
                        "color": "#cbd5e1 !important",
                        "font-size": "12px",
                        "font-weight": "800",
                        "padding-left": "12px !important"
                    },
                    ".ag-header-group-cell-label": {
                        "justify-content": "flex-start !important"
                    },
                    ".ag-header-cell": {
                        "border-right": "1px solid rgba(148, 163, 184, 0.22) !important",
                        "box-shadow": "inset -1px 0 0 rgba(15, 23, 42, 0.55)",
                        "padding-left": "12px !important",
                        "padding-right": "8px !important",
                        "text-align": "left !important"
                    },
                    ".ag-header-cell-comp-wrapper": {
                        "justify-content": "flex-start !important",
                        "align-items": "center !important",
                        "width": "100% !important",
                        "min-width": "0"
                    },
                    ".ag-cell-label-container": {
                        "justify-content": "flex-start !important",
                        "align-items": "center !important",
                        "text-align": "left !important",
                        "width": "100% !important"
                    },
                    ".ag-right-aligned-header .ag-cell-label-container": {
                        "flex-direction": "row !important",
                        "justify-content": "flex-start !important",
                        "text-align": "left !important"
                    },
                    ".ag-header-cell-label": {
                        "flex": "1 1 auto !important",
                        "width": "100% !important",
                        "justify-content": "flex-start !important",
                        "text-align": "left !important",
                        "gap": "6px",
                        "min-width": "0"
                    },
                    ".ag-header-cell-text": {
                        "color": "#94a3b8 !important",
                        "text-align": "left !important",
                        "overflow": "hidden",
                        "text-overflow": "ellipsis",
                        "white-space": "nowrap"
                    },
                    ".ag-header-icon": {
                        "margin-left": "2px !important",
                        "opacity": "0.9"
                    },
                    ".ag-header-cell-menu-button, .ag-header-cell-filter-button": {
                        "margin-left": "6px !important",
                        "margin-right": "0 !important"
                    },
                    ".ag-right-aligned-header .ag-header-cell-label": {
                        "flex-direction": "row !important",
                        "justify-content": "flex-start !important"
                    },
                    ".ag-header-cell-filtered": {
                        "background": "rgba(239, 68, 68, 0.12) !important"
                    },
                    ".ag-header-cell-filtered .ag-header-cell-text": {
                        "color": "#f8fafc !important",
                        "font-weight": "700 !important"
                    },
                    ".ag-header-cell-filtered .ag-header-cell-text::after": {
                        "content": "' 筛选中'",
                        "margin-left": "6px",
                        "padding": "1px 5px",
                        "border-radius": "4px",
                        "background": "rgba(239, 68, 68, 0.2)",
                        "color": "#fca5a5",
                        "font-size": "10px",
                        "font-weight": "700"
                    },
                    ".ag-row": {
                        "background-color": "#0f172a !important",
                        "color": "#e2e8f0 !important",
                        "border-bottom": "1px solid rgba(71, 85, 105, 0.2) !important"
                    },
                    ".ag-row-odd": {
                        "background-color": "#0f172a !important"
                    },
                    ".ag-row-even": {
                        "background-color": "#131c2e !important"
                    },
                    ".ag-row-hover": {
                        "background-color": "#1e293b !important"
                    },
                    ".ag-cell": {
                        "background-color": "transparent !important",
                        "border-right": "1px solid rgba(148, 163, 184, 0.08) !important"
                    },
                    ".ag-body-viewport": {
                        "background-color": "#0f172a !important"
                    },
                    "::-webkit-scrollbar": {
                        "width": "8px",
                        "height": "8px",
                        "background": "#0f172a"
                    },
                    "::-webkit-scrollbar-thumb": {
                        "background": "#475569",
                        "border-radius": "4px"
                    },
                    "::-webkit-scrollbar-thumb:hover": {
                        "background": "#64748b"
                    }
                    }
                )

            with focus_col:
                focus_contracts = df_filtered["合约"].astype(str).tolist()
                selected_contract = st.selectbox(
                    "聚焦合约",
                    focus_contracts,
                    key=f"focus_contract_{category}",
                    label_visibility="collapsed",
                )
                focus_row = df_filtered[df_filtered["合约"].astype(str) == selected_contract].iloc[0]
                render_focus_panel(focus_row)


    # 底部图例
    st.markdown("""
    <div style="
        margin-top: 16px;
        padding: 14px 20px;
        background: rgba(30, 41, 59, 0.3);
        border-radius: 12px;
        border: 1px solid rgba(71, 85, 105, 0.2);
        display: flex;
        justify-content: space-between;
        align-items: center;
        font-size: 12px;
        color: #64748b;
    ">
        <div style="display: flex; gap: 24px;">
            <span>
                <span style="display: inline-block; width: 12px; height: 12px; background: #ef4444; border-radius: 2px; margin-right: 6px;"></span>
                IV Rank > 80 (卖方机会)
            </span>
            <span>
                <span style="display: inline-block; width: 12px; height: 12px; background: #22c55e; border-radius: 2px; margin-right: 6px;"></span>
                IV Rank < 20 (买方机会)
            </span>
            <span>
                <span style="display: inline-block; width: 12px; height: 12px; background: #3b82f6; border-radius: 2px; margin-right: 6px;"></span>
                IV Rank 20-80 (中性区间)
            </span>
        </div>
        <div>
            数据来源：Tushare + 各交易所官网 · 每日盘后更新 · 缓存30分钟
        </div>
    </div>
    """, unsafe_allow_html=True)

else:
    # 无数据提示
    st.markdown("""
    <div style="
        text-align: center;
        padding: 60px 20px;
        background: rgba(30, 41, 59, 0.3);
        border-radius: 16px;
        border: 1px solid rgba(71, 85, 105, 0.3);
        margin-top: 40px;
    ">
        <div style="font-size: 64px; margin-bottom: 16px;">📭</div>
        <h3 style="color: #f1f5f9; margin-bottom: 8px;">暂无数据</h3>
        <p style="color: #64748b;">请检查数据库连接或稍后重试</p>
    </div>
    """, unsafe_allow_html=True)

    if st.button("🔍 查看详细错误"):
        st.code("""
可能的原因：
1. 数据库未连接
2. 今日尚无交易数据  
3. 数据表为空

请检查：
- .env 配置是否正确
- 数据库是否运行
- 运行数据更新脚本
        """)
