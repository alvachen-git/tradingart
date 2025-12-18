import streamlit as st
import pandas as pd
import data_engine as de
from st_aggrid import AgGrid, GridOptionsBuilder, JsCode, ColumnsAutoSizeMode
import time

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
    "FU": "燃油",
    "LU": "低硫燃油",
    "BU": "沥青",
    "SC": "原油",
    "PG": "LPG",

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


# ============================================================
# 页面配置
# ============================================================

st.set_page_config(
    page_title="爱波塔-全市场监控波动率",
    page_icon="favicon.ico",
    layout="wide",
    initial_sidebar_state="expanded"
)

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

    /* === 侧边栏折叠按钮 === */
    header[data-testid="stHeader"] {
        background-color: transparent !important;
        visibility: visible !important;
    }
    button[data-testid="collapsedControl"], 
    [data-testid="stSidebarCollapsedControl"] {
        visibility: visible !important;
        display: block !important;
        color: #ffffff !important;
        z-index: 100000 !important;
        position: fixed !important;
        top: 15px !important;
        left: 15px !important;
        background-color: rgba(255, 255, 255, 0.05);
        border-radius: 50%;
        width: 36px;
        height: 36px;
        transition: all 0.3s ease;
    }
    button[data-testid="collapsedControl"]:hover,
    [data-testid="stSidebarCollapsedControl"]:hover {
        background-color: rgba(59, 130, 246, 0.3) !important;
        box-shadow: 0 0 15px rgba(59, 130, 246, 0.5);
    }

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

    /* === 分隔线 === */
    hr {
        border-color: rgba(71, 85, 105, 0.3) !important;
        margin: 1.5rem 0 !important;
    }
</style>
""", unsafe_allow_html=True)

# ============================================================
# 页面标题区域
# ============================================================

col_title, col_refresh = st.columns([5, 1])

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

with col_refresh:
    if st.button("🔄 刷新数据", key="refresh_btn", use_container_width=True):
        st.cache_data.clear()
        st.rerun()


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


# 加载进度
with st.spinner(""):
    progress_bar = st.progress(0, text="🔍 正在连接数据库...")

    progress_bar.progress(25, text="📈 正在查询价格数据...")
    time.sleep(0.05)

    progress_bar.progress(50, text="🌊 正在查询波动率数据...")
    time.sleep(0.05)

    progress_bar.progress(75, text="💼 正在查询持仓数据...")

    start_time = time.time()
    df_monitor = load_data()
    load_time = time.time() - start_time

    progress_bar.progress(100, text=f"✅ 加载完成 ({load_time:.2f}秒)")
    time.sleep(0.3)
    progress_bar.empty()

# ============================================================
# 主内容区域
# ============================================================

if not df_monitor.empty:

    # === 统计卡片 ===
    st.markdown("<div style='height: 16px'></div>", unsafe_allow_html=True)

    # 计算统计数据
    total_count = len(df_monitor)
    high_iv_count = len(df_monitor[df_monitor['IV Rank'].apply(lambda x: isinstance(x, (int, float)) and x > 80)])
    low_iv_count = len(df_monitor[df_monitor['IV Rank'].apply(lambda x: isinstance(x, (int, float)) and x < 20)])

    # 计算机构净流入（需要根据你的实际列名调整）
    inst_col = '机构变动(日)' if '机构变动(日)' in df_monitor.columns else None
    net_inflow = int(df_monitor[inst_col].sum()) if inst_col and inst_col in df_monitor.columns else 0


    # 统计卡片HTML
    def create_stat_card(icon, label, value, suffix, color, glow_color):
        value_display = f"+{value:,}" if isinstance(value, int) and value > 0 else f"{value:,}" if isinstance(value,
                                                                                                              int) else value
        return f"""
        <div style="
            background: linear-gradient(135deg, rgba(30, 41, 59, 0.8), rgba(15, 23, 42, 0.9));
            border-radius: 16px;
            padding: 20px 24px;
            border: 1px solid rgba(71, 85, 105, 0.3);
            box-shadow: 0 8px 32px {glow_color};
            position: relative;
            overflow: hidden;
        ">
            <div style="
                position: absolute;
                top: -20px; right: -20px;
                font-size: 80px;
                opacity: 0.06;
            ">{icon}</div>
            <div style="display: flex; align-items: center; gap: 8px; margin-bottom: 8px;">
                <span style="font-size: 22px;">{icon}</span>
                <span style="font-size: 13px; color: #94a3b8; font-weight: 500;">{label}</span>
            </div>
            <div style="
                font-size: 32px;
                font-weight: 700;
                color: {color};
                text-shadow: 0 0 20px {glow_color};
                font-family: 'JetBrains Mono', monospace;
            ">
                {value_display}
                <span style="font-size: 14px; color: #64748b; margin-left: 4px;">{suffix}</span>
            </div>
        </div>
        """


    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.markdown(create_stat_card("📈", "监控品种", total_count, "个", "#3b82f6", "rgba(59, 130, 246, 0.2)"),
                    unsafe_allow_html=True)
    with col2:
        st.markdown(create_stat_card("🔥", "IV高位 (>80)", high_iv_count, "个", "#ef4444", "rgba(239, 68, 68, 0.2)"),
                    unsafe_allow_html=True)
    with col3:
        st.markdown(create_stat_card("💎", "IV低位 (<20)", low_iv_count, "个", "#22c55e", "rgba(34, 197, 94, 0.2)"),
                    unsafe_allow_html=True)
    with col4:
        inflow_color = "#22c55e" if net_inflow >= 0 else "#ef4444"
        inflow_glow = "rgba(34, 197, 94, 0.2)" if net_inflow >= 0 else "rgba(239, 68, 68, 0.2)"
        st.markdown(create_stat_card("🏦", "机构净流入", net_inflow, "手", inflow_color, inflow_glow),
                    unsafe_allow_html=True)

    st.markdown("<div style='height: 24px'></div>", unsafe_allow_html=True)

    # === 数据说明 + 搜索框 ===
    col_info, col_space, col_search = st.columns([2, 2, 1])

    with col_info:
        with st.expander("📖 数据说明", expanded=False):
            st.markdown("""
    **指标解释：**
    - **IV Rank**：目前隐含波动率在最近一年中的百分位排名
    - **散户变动**：（反向指标）某些散户多的期货商净持仓变化
    - **机构变动**：（正向指标）某些机构强的期货商净持仓变化

    **使用建议：**
    - <span style="color: #ef4444;">IV Rank > 80</span> ：适合卖方策略（波动率偏贵）
    - <span style="color: #22c55e;">IV Rank < 20</span> ：适合买方策略（波动率便宜）
    - 机构流入↑ + 散户流出↓ = 潜在做多信号
    - 机构流出↓ + 散户流入↑ = 潜在做空信号

    **数据更新：** 每天盘后更新，缓存30分钟
            """, unsafe_allow_html=True)

    with col_search:
        search_term = st.text_input(
            "🔍 搜索",
            placeholder="搜索合约...",
            key="search_input",
            label_visibility="collapsed"
        )

    # === 分类Tab ===
    tabs = st.tabs(CATEGORIES)

    for i, category in enumerate(CATEGORIES):
        with tabs[i]:
            # 筛选数据
            if category == "全部":
                df_filtered = df_monitor.copy()
            else:
                df_filtered = df_monitor[df_monitor['分类'] == category].copy()

            if df_filtered.empty:
                st.info(f"📭 暂无 {category} 品种数据")
                continue

            # 应用搜索筛选
            if search_term:
                mask = df_filtered['合约'].str.contains(search_term, case=False, na=False)
                df_filtered = df_filtered[mask]

            # 移除品种和分类列（不在表格中显示）
            display_columns = [col for col in df_filtered.columns if col not in ['品种', '分类']]
            df_display = df_filtered[display_columns].copy()

            # === AgGrid配置 ===
            gb = GridOptionsBuilder.from_dataframe(df_display)

            # 基础配置
            gb.configure_default_column(
                resizable=True,
                filterable=True,
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
                                cellStyle={
                                    'fontWeight': '600',
                                    'color': '#f1f5f9',
                                    'justifyContent': 'flex-start',
                                    'paddingLeft': '12px'
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
                        this.eGui.innerHTML = '<span style="color: #fbbf24; font-weight: 600; font-size: 12px;">⚠️ 快到期</span>';
                        return;
                    }

                    const numVal = parseFloat(val) || 0;
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
                            width: ${numVal}%; height: 22px;
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
                    return parseFloat(v) || -100;
                };
                return getVal(valueA) - getVal(valueB);
            }
            """)

            gb.configure_column("IV Rank",
                                width=120,
                                cellRenderer=iv_rank_renderer,
                                comparator=iv_rank_comparator
                                )

            # 当前IV
            gb.configure_column("当前IV",
                                width=100,
                                valueFormatter="x ? Number(x).toFixed(2) + '%' : '-'",
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
                                        width=110
                                        )

            for col in ['散户变动(日)', '散户变动(5日)', '机构变动(日)', '机构变动(5日)']:
                if col in df_display.columns:
                    gb.configure_column(col,
                                        cellStyle=change_style_js,
                                        valueFormatter=hold_formatter,
                                        width=120
                                        )

            gridOptions = gb.build()

            # 渲染表格
            AgGrid(
                df_display,
                gridOptions=gridOptions,
                height=600,
                width='100%',
                columns_auto_size_mode=ColumnsAutoSizeMode.FIT_ALL_COLUMNS_TO_VIEW,
                allow_unsafe_jscode=True,
                enable_enterprise_modules=False,
                reload_data=False,
                update_mode='MODEL_CHANGED',
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
                    ".ag-header-cell-text": {
                        "color": "#94a3b8 !important"
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
                        "border-right": "none !important"
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