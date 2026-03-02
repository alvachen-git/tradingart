import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import sys
import os
from datetime import datetime, timedelta

# 添加父目录到路径，以便导入主目录的模块
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import market_correlation as mc
import symbol_map as sm

# ==========================================
#  页面配置
# ==========================================
st.set_page_config(
    page_title="爱波塔-相关度分析",
    page_icon="favicon.ico",
    layout="wide",
    initial_sidebar_state="expanded"
)



# 🔥 添加统一的侧边栏导航
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from sidebar_navigation import show_navigation
with st.sidebar:
    show_navigation()

# ==========================================
#  简约深色主题 CSS
# ==========================================
st.markdown("""
<style>
    /* 基础背景 */
    .stApp {
        background-color: #0b1120;
        color: #e5e7eb;
        font-size: 15px;
    }

    /* 隐藏默认元素 */
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
    header[data-testid="stHeader"] {
        background-color: transparent;
        height: 0 !important;
        min-height: 0 !important;
        padding: 0 !important;
    }

    .block-container {
        padding: 1rem 2rem !important;
        padding-top: 1rem !important;
        max-width: 100% !important;
    }

    /* 移除顶部空白 */
    .stApp > header {
        height: 0;
    }
    div[data-testid="stAppViewBlockContainer"] {
        padding-top: 1rem !important;
    }

    /* 全局字体放大 */
    html, body, [class*="css"] {
        font-size: 15px;
    }

    /* 侧边栏 */
    section[data-testid="stSidebar"] {
        background-color: #111827;
        border-right: 1px solid #1f2937;
    }
    section[data-testid="stSidebar"] * {
        color: #9ca3af !important;
    }

    /* 标题 */
    h1, h2, h3 {
        color: #e5e7eb !important;
        font-weight: 600 !important;
    }

    /* 输入框 */
    div[data-testid="stTextInput"] input,
    div[data-testid="stNumberInput"] input {
        background-color: #111827 !important;
        border: 1px solid #1f2937 !important;
        color: #e5e7eb !important;
        border-radius: 4px !important;
    }
    div[data-testid="stTextInput"] input::placeholder {
        color: #d1d5db !important;
        opacity: 1 !important;
    }
    div[data-testid="stTextInput"] input:focus,
    div[data-testid="stNumberInput"] input:focus {
        border-color: #3b82f6 !important;
        box-shadow: none !important;
    }

    /* 下拉框 */
    div[data-testid="stSelectbox"] > div > div {
        background-color: #111827 !important;
        border: 1px solid #1f2937 !important;
        color: #e5e7eb !important;
        border-radius: 4px !important;
    }

    /* 按钮 */
    div.stButton > button {
        background-color: #3b82f6 !important;
        color: #ffffff !important;
        border: none !important;
        border-radius: 4px !important;
        font-weight: 500 !important;
        padding: 0.5rem 1.5rem !important;
    }
    div.stButton > button:hover {
        background-color: #2563eb !important;
    }

    /* Tab 样式 */
    .stTabs [data-baseweb="tab-list"] {
        gap: 0;
        background-color: transparent;
        border-bottom: 1px solid #1f2937;
    }
    .stTabs [data-baseweb="tab"] {
        background-color: transparent;
        border: none;
        border-bottom: 2px solid transparent;
        color: #6b7280;
        padding: 12px 24px;
        font-weight: 400;
    }
    .stTabs [aria-selected="true"] {
        background-color: transparent !important;
        border-bottom: 2px solid #3b82f6 !important;
        color: #e5e7eb !important;
        font-weight: 600 !important;
    }
    .stTabs [data-baseweb="tab-panel"] {
        padding-top: 1.5rem;
    }

    /* 多选框 */
    div[data-testid="stMultiSelect"] > div {
        background-color: #111827 !important;
        border: 1px solid #1f2937 !important;
        border-radius: 4px !important;
    }
    div[data-testid="stMultiSelect"] span[data-baseweb="tag"] {
        background-color: #3b82f6 !important;
        color: #ffffff !important;
    }
    div[data-testid="stMultiSelect"] span[data-baseweb="tag"] span {
        color: #ffffff !important;
    }
    /* 多选框下拉选项 - 深色主题 */
    [data-baseweb="popover"] {
        background-color: #1e293b !important;
    }
    [data-baseweb="popover"] ul {
        background-color: #1e293b !important;
    }
    [data-baseweb="popover"] li {
        color: #e5e7eb !important;
        background-color: #1e293b !important;
    }
    [data-baseweb="popover"] li:hover {
        background-color: #334155 !important;
    }
    [data-baseweb="menu"] {
        background-color: #1e293b !important;
    }
    [data-baseweb="menu"] li {
        color: #e5e7eb !important;
    }
    [role="listbox"] {
        background-color: #1e293b !important;
    }
    [role="option"] {
        color: #e5e7eb !important;
        background-color: #1e293b !important;
    }
    [role="option"]:hover {
        background-color: #334155 !important;
    }

    /* Slider */
    div[data-testid="stSlider"] > div > div > div {
        background-color: #3b82f6 !important;
    }

    /* Expander */
    .streamlit-expanderHeader {
        background-color: #111827 !important;
        border: 1px solid #1f2937 !important;
        border-radius: 4px !important;
        color: #9ca3af !important;
    }

    /* 分隔线 */
    hr {
        border-color: #1f2937 !important;
    }

    /* Metric */
    [data-testid="stMetric"] {
        background-color: #111827;
        padding: 12px 16px;
        border-radius: 4px;
    }
    [data-testid="stMetricLabel"] {
        color: #6b7280 !important;
        font-size: 12px !important;
    }
    [data-testid="stMetricValue"] {
        color: #e5e7eb !important;
        font-size: 18px !important;
    }

    /* Tooltip 样式 */
    .tooltip-container {
        position: relative;
        display: inline-flex;
        align-items: center;
        cursor: help;
    }
    .tooltip-icon {
        display: inline-flex;
        align-items: center;
        justify-content: center;
        width: 18px;
        height: 18px;
        margin-left: 8px;
        background: #1f2937;
        border: 1px solid #374151;
        border-radius: 50%;
        font-size: 12px;
        color: #9ca3af;
        transition: all 0.2s;
    }
    .tooltip-icon:hover {
        background: #374151;
        color: #e5e7eb;
        border-color: #4b5563;
    }
    .tooltip-content {
        visibility: hidden;
        opacity: 0;
        position: absolute;
        left: 50%;
        top: 100%;
        transform: translateX(-50%);
        margin-top: 8px;
        padding: 14px 16px;
        background: #1e293b;
        border: 1px solid #374151;
        border-radius: 6px;
        width: 320px;
        font-size: 13px;
        line-height: 1.7;
        color: #d1d5db;
        box-shadow: 0 10px 25px rgba(0,0,0,0.3);
        z-index: 1000;
        transition: all 0.2s;
    }
    .tooltip-content::before {
        content: '';
        position: absolute;
        bottom: 100%;
        left: 50%;
        transform: translateX(-50%);
        border: 6px solid transparent;
        border-bottom-color: #374151;
    }
    .tooltip-container:hover .tooltip-content {
        visibility: visible;
        opacity: 1;
    }
    .tooltip-title {
        font-weight: 600;
        color: #e5e7eb;
        margin-bottom: 8px;
        font-size: 14px;
    }
    .tooltip-highlight {
        color: #60a5fa;
        font-weight: 500;
    }
</style>
""", unsafe_allow_html=True)

# ==========================================
#  Plotly 深色主题配置
# ==========================================
PLOTLY_LAYOUT = dict(
    paper_bgcolor='rgba(0,0,0,0)',
    plot_bgcolor='#111827',
    font=dict(color='#9ca3af', size=14),
    margin=dict(l=40, r=40, t=50, b=40),
    xaxis=dict(
        gridcolor='#1f2937',
        linecolor='#1f2937',
        tickfont=dict(color='#6b7280', size=12),
    ),
    yaxis=dict(
        gridcolor='#1f2937',
        linecolor='#1f2937',
        tickfont=dict(color='#6b7280', size=12),
    ),
)

# ==========================================
#  期货分组配置
# ==========================================
FUTURES_GROUPS = {
    '金融': ['IH', 'IF', 'IC', 'IM', 'T', 'TL'],
    '贵金属': ['AU','AG','PT','PD'],
    '有色': ['CU', 'AL', 'ZN', 'PB', 'NI', 'SN'],
    '黑色': ['RB', 'HC', 'SS', 'I', 'J', 'JM', 'SM', 'SF'],
    '工业': ['LC', 'SI', 'PS', 'FG', 'SA', 'AO', 'SH', 'AD'],
    '能源': ['SC', 'FU', 'PG', 'LU', 'BU'],
    '化工': ['TA', 'MA', 'EG','EB', 'PP', 'L', 'V', 'BR', 'PX', 'BZ', 'PL', 'PR'],
    '农产品': ['M', 'Y', 'P', 'OI', 'RM','LH','UR','SR', 'CF', 'C', 'CS', 'AP', 'CJ', 'JD', 'RU', 'NR'],
}

# 期货代码中文名称
FUTURES_NAMES = {
    # 金融
    'IH': '上证50', 'IF': '沪深300','IC': '中证500', 'IM': '中证1000','T': '10年国债', 'TL': '30年国债',
    # 贵金属
    'AU': '黄金', 'AG': '白银','PT': '铂金','PD': '钯金',
    # 有色
    'CU': '铜', 'AL': '铝', 'ZN': '锌', 'PB': '铅', 'NI': '镍', 'SN': '锡',
    # 黑色
    'RB': '螺纹钢', 'HC': '热卷', 'SS': '不锈钢', 'I': '铁矿石', 'J': '焦炭', 'JM': '焦煤','SM': '锰硅','SF': '硅铁',
    # 工业
    'LC': '碳酸锂','SI': '工业硅','PS': '多晶硅','FG': '玻璃', 'SA': '纯碱', 'AO': '氧化铝', 'SH': '烧碱','AD': '铝合金', 'RU': '橡胶', 'BR': 'BR橡胶',
    # 能源
    'SC': '原油', 'FU': '燃油', 'PG': '液化气', 'LU': '低硫燃油','BU': '沥青',
    # 化工
    'TA': 'PTA', 'MA': '甲醇', 'EG': '乙二醇', 'EB': '苯乙烯','PP': '聚丙烯', 'L': '塑料', 'V': 'PVC', 'NR': 'NR橡胶',
    'PX': 'PX', 'BZ': '纯苯', 'PL': '丙烯', 'PR': '瓶片',
    # 农产品
    'M': '豆粕', 'Y': '豆油', 'P': '棕榈油', 'OI': '菜油', 'RM': '菜粕','LH': '生猪','UR': '尿素',
    'SR': '白糖', 'CF': '棉花', 'C': '玉米', 'CS': '淀粉','AP': '苹果','CJ': '红枣','JD': '鸡蛋',
}


def format_futures_code(code):
    """格式化期货代码显示"""
    name = FUTURES_NAMES.get(code, code)
    return f"{name} ({code})"


ALL_FUTURES = []
for codes in FUTURES_GROUPS.values():
    ALL_FUTURES.extend(codes)

# ==========================================
#  页面标题
# ==========================================
st.markdown("""
<div style="margin-bottom: 16px;">
    <h1 style="margin: 0; font-size: 26px; font-weight: 600; color: #e5e7eb; display: inline-flex; align-items: center;">
        资产相关性分析
        <span class="tooltip-container">
            <span class="tooltip-icon">?</span>
            <span class="tooltip-content">
                <div class="tooltip-title">计算方法说明</div>
                <div>本系统采用金融界最标准的 <span class="tooltip-highlight">皮尔逊积矩相关系数 (Pearson Correlation Coefficient)</span> 进行计算。</div>
                <div style="margin-top: 8px;">计算基于 <span class="tooltip-highlight">日收益率 (Daily Returns)</span>，而非价格本身，以准确反映资产间的联动关系。</div>
                <div style="margin-top: 8px;">当两个相关度很高的品种，近期价格波动幅度差距过大时，可以做多表现差的，做空表现好的，等待价差回归正常水平时平仓获利。</div>
                <div style="margin-top: 8px; padding-top: 8px; border-top: 1px solid #374151; color: #9ca3af; font-size: 12px;">
                    相关系数范围：-1 (完全负相关) 至 +1 (完全正相关)
                </div>
            </span>
        </span>
    </h1>
    <p style="margin: 4px 0 0; font-size: 15px; color: #6b7280;">
        持仓风险诊断 · 对冲机会发现
    </p>
</div>
""", unsafe_allow_html=True)

st.markdown("<hr style='margin: 16px 0; border-color: #1f2937;'>", unsafe_allow_html=True)

# ==========================================
#  Tab 页面
# ==========================================
tab1, tab2, tab3 = st.tabs(["股票风格诊断", "商品期货矩阵", "滚动相关性"])

# ==========================================
#  Tab 1: 股票风格诊断
# ==========================================
with tab1:
    col_input, col_result = st.columns([1, 3])

    with col_input:
        st.markdown("""
        <div style="font-size: 14px; color: #6b7280; margin-bottom: 8px;">股票名称或代码</div>
        """, unsafe_allow_html=True)
        stock_input = st.text_input(
            "股票名称或代码",
            value="茅台",
            label_visibility="collapsed",
            placeholder="输入名称或代码，如：茅台、宁德时代、600519.SH"
        )

        # 解析输入
        resolved_code, resolved_type = sm.resolve_symbol(stock_input)

        if resolved_code and resolved_type == 'stock':
            st.markdown(f"""
            <div style="
                padding: 8px 12px;
                background: rgba(34, 197, 94, 0.1);
                border: 1px solid rgba(34, 197, 94, 0.3);
                border-radius: 4px;
                font-size: 13px;
                color: #86efac;
                margin-top: 8px;
            ">
                ✓ 识别为：{resolved_code}
            </div>
            """, unsafe_allow_html=True)
            stock_code = resolved_code
        elif stock_input:
            # 尝试直接使用输入（可能是完整代码）
            if '.' in stock_input.upper():
                stock_code = stock_input.upper()
                st.markdown(f"""
                <div style="
                    padding: 8px 12px;
                    background: rgba(59, 130, 246, 0.1);
                    border: 1px solid rgba(59, 130, 246, 0.3);
                    border-radius: 4px;
                    font-size: 13px;
                    color: #93c5fd;
                    margin-top: 8px;
                ">
                    使用代码：{stock_code}
                </div>
                """, unsafe_allow_html=True)
            else:
                stock_code = None
                st.markdown(f"""
                <div style="
                    padding: 8px 12px;
                    background: rgba(239, 68, 68, 0.1);
                    border: 1px solid rgba(239, 68, 68, 0.3);
                    border-radius: 4px;
                    font-size: 13px;
                    color: #fca5a5;
                    margin-top: 8px;
                ">
                    ✗ 未识别，请检查输入
                </div>
                """, unsafe_allow_html=True)
        else:
            stock_code = None

        st.markdown("""
        <div style="font-size: 14px; color: #6b7280; margin-bottom: 8px; margin-top: 16px;">回顾周期</div>
        """, unsafe_allow_html=True)
        lookback_stock = st.selectbox(
            "回顾周期",
            options=[30, 60, 120, 250],
            index=2,
            format_func=lambda x: f"{x} 交易日",
            key="lookback_stock",
            label_visibility="collapsed"
        )

        analyze_btn = st.button("开始分析", type="primary", use_container_width=True, disabled=(stock_code is None))

        st.markdown("""
        <div style="
            margin-top: 16px;
            padding: 14px;
            background: #111827;
            border: 1px solid #1f2937;
            border-radius: 4px;
            font-size: 14px;
            color: #6b7280;
            line-height: 1.7;
        ">
            支持输入：<br>
            • 中文名称：茅台、宁德时代、比亚迪<br>
            • 股票代码：600519.SH、300750.SZ
        </div>
        """, unsafe_allow_html=True)

    with col_result:
        if analyze_btn and stock_code:
            with st.spinner("正在计算相关性..."):
                df_res = mc.analyze_stock_market_correlation(stock_code, lookback_days=lookback_stock)

                if df_res is None or df_res.empty:
                    st.error("无法获取数据，请检查股票代码")
                else:
                    # 柱状图
                    fig_bar = px.bar(
                        df_res,
                        x='相关系数',
                        y='指数名称',
                        orientation='h',
                        color='相关系数',
                        color_continuous_scale=['#3b82f6', '#6b7280', '#ef4444'],
                        range_color=[-1, 1],
                        text='相关系数'
                    )
                    fig_bar.update_layout(
                        **PLOTLY_LAYOUT,
                        height=320,
                        title=dict(
                            text=f"{stock_code} 与宽基指数相关性 (回顾 {lookback_stock} 交易日)",
                            font=dict(size=16, color='#9ca3af'),
                            x=0
                        ),
                        showlegend=False,
                        coloraxis_showscale=False,
                    )
                    fig_bar.update_traces(
                        texttemplate='%{text:.2f}',
                        textposition='outside',
                        textfont=dict(color='#9ca3af', size=13),
                        marker_line_width=0,
                    )
                    st.plotly_chart(fig_bar, use_container_width=True)

                    # ==========================================
                    # 智能分析结论
                    # ==========================================

                    # 按相关系数排序
                    df_sorted = df_res.sort_values('相关系数', ascending=False)

                    # 最高相关
                    max_corr = df_sorted.iloc[0]
                    max_corr_value = max_corr['相关系数']
                    max_corr_name = max_corr['指数名称']

                    # 第二高相关
                    second_corr = df_sorted.iloc[1] if len(df_sorted) > 1 else None

                    # 指数对应的对冲工具映射
                    HEDGE_MAP = {
                        '上证50': {'future': 'IH', 'etf': '50ETF期权 (510050)'},
                        '沪深300': {'future': 'IF', 'etf': '300ETF期权 (510300)'},
                        '中证500': {'future': 'IC', 'etf': '500ETF期权 (510500)'},
                        '中证1000': {'future': 'IM', 'etf': '1000ETF期权'},
                        '创业板': {'future': None, 'etf': '创业板ETF期权 (159915)'},
                        '科创50': {'future': None, 'etf': '科创50ETF期权 (588000)'},
                    }

                    # 确定风格
                    style_map = {
                        '上证50': '大盘权重',
                        '沪深300': '大盘蓝筹',
                        '中证500': '中盘成长',
                        '中证1000': '小盘',
                        '创业板': '创业成长',
                        '科创50': '科技创新',
                    }

                    # 找到匹配的指数
                    matched_index = None
                    for key in HEDGE_MAP.keys():
                        if key in max_corr_name:
                            matched_index = key
                            break

                    style_desc = style_map.get(matched_index, '混合') + '风格'

                    # 生成建议列表
                    conclusions = []

                    # 1. 风格判断
                    conclusions.append(
                        f"该股票与 <span style='color: #e5e7eb;'>{max_corr_name}</span> 相关度最高 ({max_corr_value:.3f})，属于<span style='color: #e5e7eb;'>{style_desc}</span>")

                    # 2. 根据相关系数强度给建议
                    if max_corr_value >= 0.8:
                        corr_comment = "相关性很强，对冲效果优秀"
                        hedge_ratio = "建议对冲比例 80%-100%"
                    elif max_corr_value >= 0.6:
                        corr_comment = "相关性较强，对冲效果良好"
                        hedge_ratio = "建议对冲比例 60%-80%"
                    elif max_corr_value >= 0.4:
                        corr_comment = "相关性中等，对冲效果一般"
                        hedge_ratio = "建议对冲比例 40%-60%，或考虑混合对冲"
                    else:
                        corr_comment = "相关性较弱，单一指数对冲效果有限"
                        hedge_ratio = "建议考虑个股期权或降低仓位"

                    # 3. 对冲工具建议
                    if matched_index and matched_index in HEDGE_MAP:
                        hedge_info = HEDGE_MAP[matched_index]
                        if hedge_info['future']:
                            hedge_tools = f"{hedge_info['future']}股指期货 或 {hedge_info['etf']}"
                        else:
                            hedge_tools = hedge_info['etf']
                    else:
                        hedge_tools = "建议使用个股期权或综合指数ETF"

                    conclusions.append(f"对冲工具：<span style='color: #e5e7eb;'>{hedge_tools}</span>")
                    conclusions.append(f"{corr_comment}，{hedge_ratio}")

                    # 4. 如果第二相关也很高，提示混合对冲
                    if second_corr is not None:
                        second_value = second_corr['相关系数']
                        second_name = second_corr['指数名称']
                        if second_value >= 0.5 and (max_corr_value - second_value) < 0.15:
                            conclusions.append(f"与 {second_name} 也有较高相关 ({second_value:.3f})，可考虑混合对冲")

                    # 5. 特殊风险提示
                    if max_corr_value < 0.4:
                        conclusions.append(
                            "<span style='color: #fbbf24;'>⚠ 该股票与大盘相关性较低，可能有较强的个股Alpha，需注意个股风险</span>")

                    # 渲染结论
                    conclusions_html = "".join([f'<div style="margin-bottom: 8px;">• {c}</div>' for c in conclusions])

                    st.markdown(f"""
                    <div style="
                        background: #111827;
                        border: 1px solid #1f2937;
                        border-radius: 4px;
                        padding: 18px;
                    ">
                        <div style="font-size: 15px; font-weight: 600; color: #e5e7eb; margin-bottom: 14px;">
                            分析结论
                        </div>
                        <div style="font-size: 14px; color: #9ca3af; line-height: 2;">
                            {conclusions_html}
                        </div>
                    </div>
                    """, unsafe_allow_html=True)

# ==========================================
#  Tab 2: 商品期货矩阵
# ==========================================
with tab2:
    # 初始化 session_state (用 multiselect 的 key 名称)
    if 'futures_multiselect' not in st.session_state:
        st.session_state.futures_multiselect = ['AU', 'AG', 'CU', 'RB', 'M']

    # 分组快捷选择
    st.markdown("""
    <div style="font-size: 14px; color: #6b7280; margin-bottom: 10px;">快速选择分组</div>
    """, unsafe_allow_html=True)

    group_cols = st.columns(len(FUTURES_GROUPS))

    for i, (group_name, group_codes) in enumerate(FUTURES_GROUPS.items()):
        with group_cols[i]:
            if st.button(group_name, key=f"grp_{group_name}", use_container_width=True):
                current_selection = list(st.session_state.futures_multiselect)

                # 检查该组是否全部已选
                all_selected = all(code in current_selection for code in group_codes)

                if all_selected:
                    # 全选了就取消该组
                    st.session_state.futures_multiselect = [
                        code for code in current_selection
                        if code not in group_codes
                    ]
                else:
                    # 没全选就添加该组
                    for code in group_codes:
                        if code not in current_selection:
                            current_selection.append(code)
                    st.session_state.futures_multiselect = current_selection
                st.rerun()

    # 品种多选和回顾周期
    col_futures, col_period, col_btn = st.columns([3, 1, 1])

    with col_futures:
        selected_futures = st.multiselect(
            "选择品种",
            options=ALL_FUTURES,
            key="futures_multiselect",
            label_visibility="collapsed",
            format_func=format_futures_code
        )

    with col_period:
        lookback_futures = st.selectbox(
            "回顾周期",
            options=[30, 60, 120, 250],
            index=2,
            format_func=lambda x: f"{x} 交易日",
            key="lookback_futures",
            label_visibility="collapsed"
        )

    with col_btn:
        analyze_futures_btn = st.button("开始分析", type="primary", use_container_width=True, key="analyze_futures")

    if len(selected_futures) < 2:
        st.warning("请至少选择两个品种进行对比")
    elif analyze_futures_btn:
        with st.spinner("构建相关性矩阵..."):
            corr_matrix = mc.analyze_futures_correlation(selected_futures, lookback_days=lookback_futures)

            if not corr_matrix.empty:
                # 将列名和行名转换为中文
                chinese_labels = [f"{FUTURES_NAMES.get(col, col)}" for col in corr_matrix.columns]
                corr_matrix_display = corr_matrix.copy()
                corr_matrix_display.columns = chinese_labels
                corr_matrix_display.index = chinese_labels

                # 热力图
                fig_heat = px.imshow(
                    corr_matrix_display,
                    text_auto='.2f',
                    aspect="auto",
                    color_continuous_scale=['#3b82f6', '#1f2937', '#ef4444'],
                    zmin=-1,
                    zmax=1,
                )
                fig_heat.update_layout(
                    **PLOTLY_LAYOUT,
                    height=500,
                    title=dict(
                        text=f"相关性矩阵 (回顾 {lookback_futures} 交易日)",
                        font=dict(size=16, color='#9ca3af'),
                        x=0
                    ),
                    coloraxis_colorbar=dict(
                        title="",
                        tickvals=[-1, -0.5, 0, 0.5, 1],
                        ticktext=['-1', '-0.5', '0', '0.5', '1'],
                        tickfont=dict(color='#6b7280', size=12),
                    ),
                )
                fig_heat.update_traces(
                    textfont=dict(color='#e5e7eb', size=13),
                )
                st.plotly_chart(fig_heat, use_container_width=True)

                # 分析相关性矩阵，找出高相关和低相关组合
                high_corr_pairs = []
                low_corr_pairs = []

                for i in range(len(corr_matrix.columns)):
                    for j in range(i + 1, len(corr_matrix.columns)):
                        code_i = corr_matrix.columns[i]
                        code_j = corr_matrix.columns[j]
                        name_i = FUTURES_NAMES.get(code_i, code_i)
                        name_j = FUTURES_NAMES.get(code_j, code_j)
                        pair_name = f"{name_i}-{name_j}"
                        corr_val = corr_matrix.iloc[i, j]
                        if corr_val > 0.7:
                            high_corr_pairs.append((pair_name, corr_val))
                        elif corr_val < -0.3:
                            low_corr_pairs.append((pair_name, corr_val))

                # 排序
                high_corr_pairs.sort(key=lambda x: x[1], reverse=True)
                low_corr_pairs.sort(key=lambda x: x[1])

                # 风险提示
                col_warn, col_hedge = st.columns(2)

                with col_warn:
                    if high_corr_pairs:
                        pairs_text = " · ".join([f"{p[0]} ({p[1]:.2f})" for p in high_corr_pairs[:3]])
                    else:
                        pairs_text = "无高相关组合"

                    st.markdown(f"""
                    <div style="
                        background: #111827;
                        border: 1px solid #1f2937;
                        border-left: 3px solid #ef4444;
                        border-radius: 4px;
                        padding: 16px 18px;
                    ">
                        <div style="font-size: 14px; color: #6b7280; margin-bottom: 6px;">高相关预警 (>0.7)</div>
                        <div style="font-size: 15px; color: #e5e7eb;">{pairs_text}</div>
                    </div>
                    """, unsafe_allow_html=True)

                with col_hedge:
                    if low_corr_pairs:
                        pairs_text = " · ".join([f"{p[0]} ({p[1]:.2f})" for p in low_corr_pairs[:3]])
                    else:
                        pairs_text = "无负相关组合"

                    st.markdown(f"""
                    <div style="
                        background: #111827;
                        border: 1px solid #1f2937;
                        border-left: 3px solid #22c55e;
                        border-radius: 4px;
                        padding: 16px 18px;
                    ">
                        <div style="font-size: 14px; color: #6b7280; margin-bottom: 6px;">对冲机会 (<-0.3)</div>
                        <div style="font-size: 15px; color: #e5e7eb;">{pairs_text}</div>
                    </div>
                    """, unsafe_allow_html=True)
            else:
                st.error("数据不足，请确认品种在数据库中有数据")

# ==========================================
#  Tab 3: 滚动相关性
# ==========================================
with tab3:
    st.markdown("""
    <div style="font-size: 14px; color: #6b7280; margin-bottom: 16px;">
        比较两个资产的相关性随时间变化趋势，发现最佳对冲时机
    </div>
    """, unsafe_allow_html=True)

    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.markdown('<div style="font-size: 14px; color: #6b7280; margin-bottom: 8px;">资产 A (名称或代码)</div>',
                    unsafe_allow_html=True)
        asset_a_input = st.text_input("资产A", value="茅台", label_visibility="collapsed", key="roll_a")

        # 解析资产A
        resolved_a, type_a_resolved = sm.resolve_symbol(asset_a_input)
        if resolved_a:
            asset_a = resolved_a
            type_a = type_a_resolved
            st.markdown(f'<div style="font-size: 12px; color: #86efac;">✓ {resolved_a} ({type_a})</div>',
                        unsafe_allow_html=True)
        elif '.' in asset_a_input.upper():
            asset_a = asset_a_input.upper()
            type_a = 'stock'
            st.markdown(f'<div style="font-size: 12px; color: #93c5fd;">{asset_a}</div>', unsafe_allow_html=True)
        else:
            asset_a = asset_a_input.upper()
            type_a = 'future'
            st.markdown(f'<div style="font-size: 12px; color: #93c5fd;">{asset_a} (期货)</div>', unsafe_allow_html=True)

    with col2:
        st.markdown('<div style="font-size: 14px; color: #6b7280; margin-bottom: 8px;">资产 B (名称或代码)</div>',
                    unsafe_allow_html=True)
        asset_b_input = st.text_input("资产B", value="黄金", label_visibility="collapsed", key="roll_b")

        # 解析资产B
        resolved_b, type_b_resolved = sm.resolve_symbol(asset_b_input)
        if resolved_b:
            asset_b = resolved_b
            type_b = type_b_resolved
            st.markdown(f'<div style="font-size: 12px; color: #86efac;">✓ {resolved_b} ({type_b})</div>',
                        unsafe_allow_html=True)
        elif '.' in asset_b_input.upper():
            asset_b = asset_b_input.upper()
            type_b = 'stock'
            st.markdown(f'<div style="font-size: 12px; color: #93c5fd;">{asset_b}</div>', unsafe_allow_html=True)
        else:
            asset_b = asset_b_input.upper()
            type_b = 'future'
            st.markdown(f'<div style="font-size: 12px; color: #93c5fd;">{asset_b} (期货)</div>', unsafe_allow_html=True)

    with col3:
        st.markdown('<div style="font-size: 14px; color: #6b7280; margin-bottom: 8px;">滚动窗口 (天)</div>',
                    unsafe_allow_html=True)
        rolling_win = st.number_input("窗口", value=30, min_value=5, max_value=120, label_visibility="collapsed")

    with col4:
        st.markdown('<div style="font-size: 14px; color: #6b7280; margin-bottom: 8px;">&nbsp;</div>',
                    unsafe_allow_html=True)
        generate_btn = st.button("生成走势图", type="primary", use_container_width=True)

    # 说明信息
    st.markdown("""
    <div style="
        margin-top: 8px;
        padding: 12px 14px;
        background: #111827;
        border: 1px solid #1f2937;
        border-radius: 4px;
        font-size: 13px;
        color: #6b7280;
    ">
        💡 支持输入：中文名称 (茅台、黄金、螺纹) 或代码 (600519.SH、AU、RB)
    </div>
    """, unsafe_allow_html=True)

    if generate_btn:
        start_date_long = (datetime.now() - timedelta(days=730)).strftime('%Y%m%d')

        # 获取显示名称
        display_a = asset_a_input if asset_a_input else asset_a
        display_b = asset_b_input if asset_b_input else asset_b

        with st.spinner("获取数据..."):
            s1 = mc.get_price_series(asset_a, type_a, start_date_long)
            s2 = mc.get_price_series(asset_b, type_b, start_date_long)

            if s1.empty or s2.empty:
                st.error(f"获取数据失败，请检查代码是否正确")
            else:
                df_roll = pd.concat([s1, s2], axis=1, join='inner')
                df_roll.columns = [asset_a, asset_b]

                df_ret = df_roll.pct_change().dropna()
                rolling_corr = df_ret[asset_a].rolling(window=rolling_win).corr(df_ret[asset_b]).dropna()

                # 滚动相关性图
                fig_roll = go.Figure()

                fig_roll.add_trace(go.Scatter(
                    x=rolling_corr.index,
                    y=rolling_corr.values,
                    mode='lines',
                    fill='tozeroy',
                    fillcolor='rgba(59, 130, 246, 0.1)',
                    line=dict(color='#3b82f6', width=1.5),
                    name='相关系数'
                ))

                # 零轴
                fig_roll.add_hline(y=0, line_dash="dot", line_color="#4b5563", line_width=1)

                fig_roll.update_layout(
                    paper_bgcolor='rgba(0,0,0,0)',
                    plot_bgcolor='#111827',
                    font=dict(color='#9ca3af', size=14),
                    margin=dict(l=40, r=40, t=50, b=40),
                    height=350,
                    title=dict(
                        text=f"{display_a} vs {display_b} {rolling_win}日滚动相关性",
                        font=dict(size=16, color='#9ca3af'),
                        x=0
                    ),
                    xaxis=dict(
                        gridcolor='#1f2937',
                        linecolor='#1f2937',
                        tickfont=dict(color='#6b7280', size=12),
                    ),
                    yaxis=dict(
                        gridcolor='#1f2937',
                        linecolor='#1f2937',
                        tickfont=dict(color='#6b7280', size=12),
                        range=[-1, 1],
                        dtick=0.5,
                    ),
                    showlegend=False,
                )

                st.plotly_chart(fig_roll, use_container_width=True)

                # 统计数据
                st.markdown(f"""
                <div style="
                    display: flex;
                    background: #111827;
                    border: 1px solid #1f2937;
                    border-radius: 4px;
                    overflow: hidden;
                ">
                    <div style="flex: 1; padding: 14px 18px; border-right: 1px solid #1f2937;">
                        <div style="font-size: 13px; color: #6b7280;">当前值</div>
                        <div style="font-size: 18px; font-weight: 600; color: #e5e7eb;">{rolling_corr.iloc[-1]:.2f}</div>
                    </div>
                    <div style="flex: 1; padding: 14px 18px; border-right: 1px solid #1f2937;">
                        <div style="font-size: 13px; color: #6b7280;">最大值</div>
                        <div style="font-size: 18px; font-weight: 600; color: #e5e7eb;">{rolling_corr.max():.2f}</div>
                        <div style="font-size: 12px; color: #6b7280;">{rolling_corr.idxmax().strftime('%Y-%m-%d')}</div>
                    </div>
                    <div style="flex: 1; padding: 14px 18px; border-right: 1px solid #1f2937;">
                        <div style="font-size: 13px; color: #6b7280;">最小值</div>
                        <div style="font-size: 18px; font-weight: 600; color: #e5e7eb;">{rolling_corr.min():.2f}</div>
                        <div style="font-size: 12px; color: #6b7280;">{rolling_corr.idxmin().strftime('%Y-%m-%d')}</div>
                    </div>
                    <div style="flex: 1; padding: 14px 18px;">
                        <div style="font-size: 13px; color: #6b7280;">平均值</div>
                        <div style="font-size: 18px; font-weight: 600; color: #e5e7eb;">{rolling_corr.mean():.2f}</div>
                    </div>
                </div>
                """, unsafe_allow_html=True)

# ==========================================
#  底部说明
# ==========================================
st.markdown("""
<div style="
    margin-top: 32px;
    padding-top: 16px;
    border-top: 1px solid #1f2937;
    display: flex;
    justify-content: space-between;
    align-items: center;
    font-size: 13px;
    color: #6b7280;
">
    <div style="display: flex; gap: 20px;">
        <span>
            <span style="display: inline-block; width: 8px; height: 8px; background: #ef4444; border-radius: 1px; margin-right: 6px;"></span>
            正相关 (风险叠加)
        </span>
        <span>
            <span style="display: inline-block; width: 8px; height: 8px; background: #3b82f6; border-radius: 1px; margin-right: 6px;"></span>
            弱相关
        </span>
        <span>
            <span style="display: inline-block; width: 8px; height: 8px; background: #22c55e; border-radius: 1px; margin-right: 6px;"></span>
            负相关 (对冲效应)
        </span>
    </div>
    <div>
        数据来源：Tushare · 每日更新
    </div>
</div>
""", unsafe_allow_html=True)
