"""
共享的侧边栏导航组件
所有页面都导入这个模块来显示统一的导航菜单
"""
import streamlit as st


def show_navigation():
    """显示分组折叠导航菜单"""
    # 🔥 隐藏 Streamlit 默认的页面导航 + 统一样式
    st.markdown("""
    <style>
        /* 隐藏默认导航 */
        [data-testid="stSidebarNav"] {
            display: none !important;
        }

        /* 🔥 强制统一侧边栏背景色和文字颜色（与情报站一致）*/
        [data-testid="stSidebar"] {
            background-color: #0f172a !important;
            border-right: 1px solid #1e293b !important;
        }

        [data-testid="stSidebar"] * {
            color: #cbd5e1 !important;
        }

        /* 🔥 统一侧边栏样式 - 适配所有页面 */
        /* 折叠框标题样式 */
        [data-testid="stSidebar"] [data-testid="stExpander"] summary,
        [data-testid="stSidebar"] [data-testid="stExpander"] summary p,
        [data-testid="stSidebar"] [data-testid="stExpander"] summary div,
        [data-testid="stSidebar"] [data-testid="stExpander"] summary span {
            color: #cbd5e1 !important;
            font-weight: 600 !important;
            background-color: transparent !important;
        }
        [data-testid="stSidebar"] [data-testid="stExpander"] summary:hover {
            color: #3b82f6 !important;
            background-color: rgba(59, 130, 246, 0.1) !important;
        }
        [data-testid="stSidebar"] [data-testid="stExpander"] summary:hover p,
        [data-testid="stSidebar"] [data-testid="stExpander"] summary:hover div,
        [data-testid="stSidebar"] [data-testid="stExpander"] summary:hover span {
            color: #3b82f6 !important;
        }

        /* 折叠框箭头图标 */
        [data-testid="stSidebar"] [data-testid="stExpander"] svg,
        [data-testid="stSidebar"] [data-testid="stExpander"] svg path {
            fill: #cbd5e1 !important;
        }

        /* 折叠框内部区域 */
        [data-testid="stSidebar"] [data-testid="stExpanderDetails"] {
            background-color: rgba(30, 41, 59, 0.3) !important;
            color: #cbd5e1 !important;
            border: 1px solid rgba(255,255,255,0.1) !important;
            border-radius: 8px !important;
        }

        /* 🔥 页面链接按钮样式 - 所有子元素都强制颜色 */
        [data-testid="stSidebar"] [data-testid="baseButton-secondary"],
        [data-testid="stSidebar"] [data-testid="baseButton-secondary"] *,
        [data-testid="stSidebar"] [data-testid="baseButton-secondary"] p,
        [data-testid="stSidebar"] [data-testid="baseButton-secondary"] div,
        [data-testid="stSidebar"] [data-testid="baseButton-secondary"] span {
            background-color: transparent !important;
            color: #cbd5e1 !important;
            border: 1px solid transparent !important;
            text-align: left !important;
            border-radius: 8px !important;
            transition: all 0.2s ease !important;
        }

        /* 🔥🔥 Hover 效果 - 蓝色高亮 + 橘色左边框（与情报站一致）🔥🔥 */
        [data-testid="stSidebar"] [data-testid="baseButton-secondary"]:hover {
            background-color: rgba(59, 130, 246, 0.15) !important;
            border: 1px solid #3b82f6 !important;
            border-left: 3px solid #ff9800 !important;
            color: #3b82f6 !important;
        }
        [data-testid="stSidebar"] [data-testid="baseButton-secondary"]:hover *,
        [data-testid="stSidebar"] [data-testid="baseButton-secondary"]:hover p,
        [data-testid="stSidebar"] [data-testid="baseButton-secondary"]:hover div,
        [data-testid="stSidebar"] [data-testid="baseButton-secondary"]:hover span {
            color: #3b82f6 !important;
        }

        /* 侧边栏内所有文本元素统一颜色 */
        [data-testid="stSidebar"] p,
        [data-testid="stSidebar"] span,
        [data-testid="stSidebar"] div,
        [data-testid="stSidebar"] label {
            color: #cbd5e1 !important;
        }

    </style>
    """, unsafe_allow_html=True)

    # 首页
    with st.expander("首页", expanded=False):
        st.page_link("Home.py", label="AI对话")

    # 期权数据（去掉icon）
    with st.expander("期权数据", expanded=False):
        st.page_link("pages/01_ETF期权.py", label="ETF期权")
        st.page_link("pages/02_商品期权.py", label="商品期权")
        st.page_link("pages/12_策略回测.py", label="策略回测")
        st.page_link("pages/04_排行榜.py", label="排行榜")

    # 量化分析（去掉icon）
    with st.expander("量化分析", expanded=False):
        st.page_link("pages/03_商品持仓.py", label="商品持仓")
        st.page_link("pages/05_宏观分析.py", label="宏观分析")
        st.page_link("pages/06_相关分析.py", label="相关分析")
        st.page_link("pages/07_对冲分析.py", label="对冲分析")
        st.page_link("pages/08_股票资金.py", label="股票资金")
        st.page_link("pages/18_持仓体检.py", label="持仓体检")
        st.page_link("pages/美股.py", label="美股")

    # K线游戏（去掉icon）
    with st.expander("K线游戏", expanded=False):
        st.page_link("pages/16_K线卡牌MVP.py", label="K线卡牌")
        st.page_link("pages/K线训练.py", label="K线训练")
        st.page_link("pages/期权学习.py", label="期权学习")

    # 个人中心（去掉icon）
    with st.expander("个人中心", expanded=False):
        st.page_link("pages/15_个人资料.py", label="个人资料")
        st.page_link("pages/01_秘书.py", label="秘书")
        st.page_link("pages/11_情报站.py", label="情报站")

    st.markdown("---")
