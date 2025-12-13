import streamlit as st
import pandas as pd
import data_engine as de
from st_aggrid import AgGrid, GridOptionsBuilder, JsCode, ColumnsAutoSizeMode
import time

# 1. 页面配置
st.set_page_config(
    page_title="爱波塔-全市场监控波动率",
    page_icon="favicon.ico",
    layout="wide",
    initial_sidebar_state="expanded"
)

# 2. 注入全局 CSS
st.markdown("""
<style>
    /* --- 核心修复：强制显示侧边栏折叠箭头 --- */
    header[data-testid="stHeader"] {
        background-color: transparent !important;
        visibility: visible !important;
        z-index: 1;
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
        line-height: 36px;
        transition: all 0.3s ease;
    }

    button[data-testid="collapsedControl"]:hover,
    [data-testid="stSidebarCollapsedControl"]:hover {
        background-color: rgba(255, 255, 255, 0.2) !important;
        color: #00f2ff !important;
        box-shadow: 0 0 10px rgba(0, 242, 255, 0.6);
        transform: scale(1.1);
    }

    .block-container {
        padding: 1rem !important;
        max-width: 100% !important;
    }
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}

    .stApp {
        background-color: #0b1121;
        color: #e2e8f0;
    }

    section[data-testid="stSidebar"] {
        background-color: #0f172a;
        border-right: 1px solid #1e293b;
    }
    section[data-testid="stSidebar"] p, 
    section[data-testid="stSidebar"] span, 
    section[data-testid="stSidebar"] div {
        color: #94a3b8 !important;
    }

    h1, h2, h3 {
        color: #f8fafc !important; 
        font-family: 'Inter', sans-serif;
        font-weight: 600;
        margin-top: 0 !important;
    }
    div[data-testid="stTextInput"] input {
        background-color: #1e293b !important;
        border: 1px solid #334155 !important;
        color: #e2e8f0 !important;
        border-radius: 6px;
    }
    div.stButton > button {
        background-color: #1e293b;
        color: #e2e8f0;
        border: 1px solid #334155;
        border-radius: 6px;
    }

    .stAgGrid {
        background-color: #0b1121 !important;
    }
    .ag-header, .ag-root-wrapper, .ag-root-wrapper-body, .ag-root {
        background-color: #0b1121 !important;
    }
</style>
""", unsafe_allow_html=True)

# === 【新增】页面标题和操作栏 ===
col1, col2, col3 = st.columns([1, 3, 1])

with col1:
    st.subheader("📊 全市场监控")

with col2:
    # === 【新增】底部数据说明 ===
    with st.expander("📖 数据说明", expanded=False):
        st.markdown("""
        **指标解释：**
        - **IV Rank**： 目前隐含波动率在最近一年中的百分位排名，越高表示期权越贵
        - **散户变动**: （反向指标）某些散户多的期货商净持仓变化
        - **机构变动**: （正向指标）某些机构强的期货商净持仓变化

        **使用建议：**
        - IV Rank > 80 适合卖方策略（波动率偏贵）
        - IV Rank < 20 适合买方策略（波动率便宜）
        - 机构持仓正数增加 + 散户持仓负数增加 = 潜在做多信号
        - 机构持仓负数增加 + 散户持仓正数增加 = 潜在做空信号

        **更新频率：** 数据缓存30分钟，点击"刷新"可手动更新
        """)

with col3:
    # 刷新按钮
    if st.button("🔄 刷新", key="refresh_btn", use_container_width=True):
        st.cache_data.clear()
        st.rerun()

st.markdown("---")

# === 【优化】渐进式加载体验 ===
placeholder = st.empty()

with placeholder.container():
    with st.spinner(""):
        # 进度条
        progress_bar = st.progress(0, text="🔍 正在连接数据库...")
        status_text = st.empty()

        # 第1阶段：查询价格数据
        progress_bar.progress(20, text="📈 正在查询价格数据...")
        time.sleep(0.1)

        # 第2阶段：查询IV数据
        progress_bar.progress(40, text="🌊 正在查询波动率数据...")
        time.sleep(0.1)

        # 第3阶段：查询持仓数据
        progress_bar.progress(60, text="💼 正在查询持仓数据...")

        # 开始计时
        start_time = time.time()

        # 加载数据
        df_monitor = de.get_comprehensive_market_data()

        # 计算加载时间
        load_time = time.time() - start_time

        progress_bar.progress(80, text="🔧 正在处理数据...")
        time.sleep(0.1)

        progress_bar.progress(100, text=f"✅ 加载完成 ({load_time:.2f}秒)")
        time.sleep(0.1)

        # 清除加载提示
        progress_bar.empty()
        status_text.empty()

# 记录更新时间
st.session_state['last_update'] = pd.Timestamp.now().strftime('%H:%M:%S')

if not df_monitor.empty:


    # === AgGrid表格配置（保持原有逻辑）===
    gb = GridOptionsBuilder.from_dataframe(df_monitor)

    # 基础配置
    gb.configure_default_column(
        resizable=True, filterable=True, sortable=True,
        cellStyle={'display': 'flex', 'alignItems': 'center'}, minWidth=100
    )

    # 合约列
    gb.configure_column("合约", pinned='left', width=240,
                        cellStyle={'fontWeight': 'bold', 'color': '#f1f5f9', 'backgroundColor': '#0f172a'})

    # 涨跌配色 JS
    soft_color_js = JsCode("""
    function(params) {
        let val = parseFloat(params.value);
        if (!isNaN(val)) {
            if (val > 0) return {'color': '#ff6b6b', 'fontWeight': '500'};
            if (val < 0) return {'color': '#4ecdc4', 'fontWeight': '500'};
        }
        return {'color': '#64748b'}; 
    }
    """)

    for col in df_monitor.columns:
        if col not in ['合约', 'IV Rank']:
            gb.configure_column(col, cellStyle=soft_color_js, valueFormatter="x ? Number(x).toFixed(1) : '0.0'")

    # IV Rank 混合类型排序器
    iv_rank_comparator = JsCode("""
    function(valueA, valueB, nodeA, nodeB, isInverted) {
        const getVal = (v) => {
            if (v === '快到期') return -100; 
            if (typeof v === 'number') return v;
            return parseFloat(v) || -100;
        };
        return getVal(valueA) - getVal(valueB);
    }
    """)

    # IV Rank 样式渲染器
    iv_rank_renderer = JsCode("""
    function(params) {
        let rawVal = params.value;

        if (rawVal === '快到期') {
            return {
                'color': '#fbbf24', 'fontWeight': 'bold', 'textAlign': 'center', 'width': '100%'
            };
        }

        let val = parseFloat(rawVal);
        if (isNaN(val)) val = 0;

        let color_bg, color_border;
        if (val > 80) { color_bg = 'rgba(255, 107, 107, 0.4)'; color_border = '#ff6b6b'; }
        else if (val < 20) { color_bg = 'rgba(78, 205, 196, 0.4)'; color_border = '#4ecdc4'; }
        else { color_bg = 'rgba(56, 189, 248, 0.4)'; color_border = '#38bdf8'; }

        return {
            'backgroundImage': `linear-gradient(to right, ${color_bg} ${val}%, transparent ${val}%)`,
            'borderLeft': `4px solid ${color_border}`,
            'color': '#fff',
            'paddingLeft': '10px',
            'fontWeight': 'bold'
        };
    }
    """)

    gb.configure_column("IV Rank", width=100,
                        cellStyle=iv_rank_renderer,
                        comparator=iv_rank_comparator)

    # 持仓变动格式化
    hold_fmt = JsCode("""
        function(params) {
            if (!params.value) return '0';
            let val = parseInt(params.value);
            return (val > 0 ? '+' : '') + val.toLocaleString();
        }
    """)

    # 当前IV配置
    gb.configure_column("当前IV", width=100, valueFormatter="x ? Number(x).toFixed(2) : '0.00'",
                        cellStyle={'color': '#e2e8f0', 'fontWeight': 'bold'})

    for col in ['散户变动(日)', '散户变动(5日)', '机构变动(日)', '机构变动(5日)']:
        gb.configure_column(col, cellStyle=soft_color_js, valueFormatter=hold_fmt, width=140)

    gridOptions = gb.build()

    # 渲染表格
    AgGrid(
        df_monitor,
        gridOptions=gridOptions,
        height=800,
        width='100%',
        columns_auto_size_mode=ColumnsAutoSizeMode.FIT_ALL_COLUMNS_TO_VIEW,
        allow_unsafe_jscode=True,
        # 【新增】性能优化配置
        enable_enterprise_modules=False,
        reload_data=False,
        update_mode='MODEL_CHANGED',
        custom_css={
            ".ag-root-wrapper": {"background-color": "#0b1121 !important", "border": "none"},
            ".ag-header": {"background-color": "#1e293b !important", "color": "#94a3b8 !important",
                           "border-bottom": "1px solid #334155"},
            ".ag-row": {"background-color": "#0b1121 !important", "color": "#cbd5e1",
                        "border-bottom": "1px solid #1e293b !important"},
            ".ag-row-hover": {"background-color": "#1e293b !important"},
            "::-webkit-scrollbar": {"width": "8px", "height": "8px", "background": "#0b1121"},
            "::-webkit-scrollbar-thumb": {"background": "#334155", "border-radius": "4px"}
        }
    )


else:
    st.warning("⚠️ 暂无数据，请检查数据库连接或稍后重试")

    # 显示调试信息
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