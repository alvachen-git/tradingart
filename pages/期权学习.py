import streamlit as st
import streamlit.components.v1 as components

# 1. 配置页面基本信息
# 页面配置
st.set_page_config(
    page_title="爱波塔-懂期权的AI-陪你在市场奋斗",
    page_icon="favicon.ico",
    layout="wide",
    initial_sidebar_state="expanded"
)
st.markdown("""
<style>
    /* 1. 强制全局背景为深空蓝黑 */
    .stApp {
        background-color: #0b1121 !important;
        background-image: radial-gradient(circle at 50% 0%, #1e293b 0%, #0b1121 70%);
        color: white !important; /* 强制全局文字变白 */
        font-family: 'JetBrains Mono', 'Courier New', monospace;
    }

 /* --- 修复：输入框样式 --- */
    div[data-testid="stTextInput"] input {
        background-color: #1e293b !important; /* 深色背景 */
        color: #ffffff !important;             /* 白色文字 */
        border: 1px solid #475569 !important;  /* 灰色边框 */
        border-radius: 8px !important;
    }
    /* 输入框的占位符 (Placeholder) 颜色 */
    div[data-testid="stTextInput"] input::placeholder {
        color: #94a3b8 !important;
    }

    /* --- 修复：聊天气泡样式 --- */
    /* 聊天消息容器 */
    div[data-testid="stChatMessage"] {
        background-color: rgba(30, 41, 59, 0.6) !important; /* 半透明深底 */
        border: 1px solid rgba(255, 255, 255, 0.1);
        border-radius: 12px;
        padding: 15px;
        margin-bottom: 10px;
    }

    /* 强制消息内的文字变白 */
    div[data-testid="stChatMessage"] p,
    div[data-testid="stChatMessage"] div,
    div[data-testid="stChatMessage"] span {
        color: #ffffff !important;
    }

    /* 3. 侧边栏文字强制变白 */
    [data-testid="stSidebar"] {
        background-color: #0f172a !important;
    }
    [data-testid="stSidebar"] p, [data-testid="stSidebar"] span, [data-testid="stSidebar"] div {
        color: #cbd5e1 !important;
    }

    /* 4. 修复 Expander 在首页的样式 */
    .streamlit-expanderHeader {
        color: white !important;
        background-color: rgba(255,255,255,0.05) !important;
    }

    /* 5. 隐藏顶部装饰条 */
    header[data-testid="stHeader"] {
        background-color: transparent !important;
    }
</style>
""", unsafe_allow_html=True)

# 加载 CSS
with open('style.css', encoding='utf-8') as f:
    st.markdown(f'<style>{f.read()}</style>', unsafe_allow_html=True)

# 2. 设置你要跳转的目标网址
target_url = "https://tradingart.cn/aritcle"  # <--- 在这里填入你的学习网站地址

# 3. 页面显示内容
st.title("🎓 正在前往期权学习平台...")

st.info("如果不自动跳转，请点击下方按钮。")

# 4. 显示一个大大的跳转按钮 (防止浏览器拦截自动跳转)
st.link_button("👉 点击进入学习平台", target_url, type="primary", use_container_width=True)

# 5. 尝试使用 JavaScript 自动跳转 (在新标签页打开)
# 注意：大部分现代浏览器会拦截非用户触发的弹窗，所以上面的按钮是必须的
js = f"""
<script>
    window.open("{target_url}", "_blank");
</script>
"""
components.html(js, height=0)

# 6. (可选) 如果你想在当前页面直接根据 meta 刷新跳转 (覆盖当前页)
# st.markdown(f'<meta http-equiv="refresh" content="0;url={target_url}">', unsafe_allow_html=True)