import streamlit as st
import os

# 1. 页面配置
st.set_page_config(
    page_title="Alpha 智能期货终端",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded"
)

# 加载 CSS
with open('style.css', encoding='utf-8') as f:
    st.markdown(f'<style>{f.read()}</style>', unsafe_allow_html=True)




# ----- 隐藏顶部 Header 的 CSS -----
hide_header_style = """
<style>
    /* 隐藏顶部的白色横条 (Deploy, Stop, 汉堡菜单) */
    [data-testid="stHeader"] {
        display: none;
    }

    /* 可选：如果你觉得隐藏后顶部留白太多，可以用下面的代码把内容往上提 */
    .block-container {
        padding-top: 2rem; /* 默认是 6rem 左右，改小一点就上去了 */
    }
</style>
"""
st.markdown(hide_header_style, unsafe_allow_html=True)


# 首页内容
st.title("📱 Alpha 移动交易台")
st.info("👈 请点击左上角 **>** 按钮打开菜单，选择功能模块。")

st.markdown("### 🔥 热门功能")
c1, c2 = st.columns(2)
with c1:
    if st.button("📡 市场雷达 (主力博弈)", use_container_width=True):
        st.switch_page("pages/商品期货.py")
with c2:
    if st.button("💹 ETF 期权分析", use_container_width=True):
        st.switch_page("pages/ETF期权.py")