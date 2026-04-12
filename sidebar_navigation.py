"""共享侧边栏导航组件。"""

from pathlib import Path

import streamlit as st


_APP_ROOT = Path(__file__).resolve().parent


def _safe_page_link(rel_path: str, label: str) -> None:
    """Render page link only when target file exists to avoid runtime page-not-found."""
    if (_APP_ROOT / rel_path).exists():
        st.page_link(rel_path, label=label)


def show_navigation() -> None:
    """显示分组折叠导航菜单。"""
    st.markdown(
        """
        <style>
        [data-testid="stSidebarNav"] { display: none !important; }
        [data-testid="stSidebar"] {
            background-color: #0f172a !important;
            border-right: 1px solid #1e293b !important;
        }
        [data-testid="stSidebar"] * { color: #cbd5e1 !important; }
        [data-testid="stSidebar"] [data-testid="stExpander"] summary,
        [data-testid="stSidebar"] [data-testid="stExpander"] summary * {
            color: #cbd5e1 !important;
            font-weight: 600 !important;
        }
        [data-testid="stSidebar"] [data-testid="stExpander"] summary:hover,
        [data-testid="stSidebar"] [data-testid="stExpander"] summary:hover * {
            color: #60a5fa !important;
            background-color: rgba(59, 130, 246, 0.08) !important;
        }
        [data-testid="stSidebar"] [data-testid="stExpanderDetails"] {
            background-color: rgba(30, 41, 59, 0.26) !important;
            border: 1px solid rgba(255,255,255,0.08) !important;
            border-radius: 8px !important;
        }
        [data-testid="stSidebar"] [data-testid="baseButton-secondary"],
        [data-testid="stSidebar"] [data-testid="baseButton-secondary"] * {
            background-color: transparent !important;
            color: #cbd5e1 !important;
            border: 1px solid transparent !important;
            text-align: left !important;
            border-radius: 8px !important;
            transition: all 0.2s ease !important;
        }
        [data-testid="stSidebar"] [data-testid="baseButton-secondary"]:hover {
            background-color: rgba(59, 130, 246, 0.15) !important;
            border: 1px solid #3b82f6 !important;
            border-left: 3px solid #f59e0b !important;
        }
        [data-testid="stSidebar"] [data-testid="baseButton-secondary"]:hover * {
            color: #60a5fa !important;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )

    with st.expander("首页", expanded=False):
        _safe_page_link("Home.py", label="AI对话")
        _safe_page_link("pages/20_AI模拟投资.py", label="AI炒股")
        _safe_page_link("pages/22_AI炒股2号.py", label="AI炒股2号")

    with st.expander("期权数据", expanded=False):
        _safe_page_link("pages/期权学习.py", label="期权学习")
        _safe_page_link("pages/01_ETF期权.py", label="ETF期权")
        _safe_page_link("pages/02_商品期权.py", label="商品期权")
        _safe_page_link("pages/12_策略回测.py", label="策略回测")
        _safe_page_link("pages/04_排行榜.py", label="排行榜")
        _safe_page_link("pages/10_跨资产IV温度.py", label="跨资产IV温度")

    with st.expander("量化分析", expanded=False):
        _safe_page_link("pages/03_商品持仓.py", label="商品持仓")
        _safe_page_link("pages/09_期限结构.py", label="期限结构")
        _safe_page_link("pages/24_世界混乱指数.py", label="世界混乱指数")
        _safe_page_link("pages/06_相关分析.py", label="相关分析")
        _safe_page_link("pages/07_对冲分析.py", label="对冲分析")
        _safe_page_link("pages/08_股票资金.py", label="股票资金")
        _safe_page_link("pages/21_产业链图谱.py", label="产业链图谱")

    with st.expander("学习交易", expanded=False):
        _safe_page_link("pages/25_期权重盾班.py", label="期权重盾班")
        _safe_page_link("pages/K线训练.py", label="K线训练")
        _safe_page_link("pages/19_K线复盘.py", label="交易复盘")

    with st.expander("个人中心", expanded=False):
        _safe_page_link("pages/15_个人资料.py", label="个人资料")
        _safe_page_link("pages/18_持仓体检.py", label="持仓体检")
        _safe_page_link("pages/11_情报站.py", label="情报站")
        _safe_page_link("pages/17_充值中心.py", label="充值中心")

    st.markdown("---")
