import streamlit as st
import pandas as pd
import data_engine as de
import os
import sys
import auth_utils as auth
from datetime import datetime, timedelta
from kline_tools import analyze_kline_pattern
import time
import extra_streamlit_components as stx
from market_tools import get_market_snapshot, get_price_statistics
from data_engine import get_commodity_iv_info, check_option_expiry_status
from captcha_utils import generate_captcha_image
from sqlalchemy import text
from dotenv import load_dotenv
from knowledge_tools import search_investment_knowledge
# --- AI 相关导入 ---
from langchain_community.chat_models import ChatTongyi
from langchain_core.messages import HumanMessage, AIMessage, SystemMessage

try:
    from langgraph.prebuilt import create_react_agent
except ImportError:
    st.error("❌ 请先安装 LangGraph: `pip install langgraph`")
    st.stop()

# 1. 初始化环境
load_dotenv(override=True)

# --- 系统代理清理 ---
for key in ["HTTP_PROXY", "HTTPS_PROXY", "http_proxy", "https_proxy"]:
    if key in os.environ:
        del os.environ[key]

# ==========================================
#  1. 页面配置 (必须在第一行) [修改点：改为 centered 布局]
# ==========================================
st.set_page_config(
    page_title="爱波塔-懂期权的AI-陪你在市场奋斗",
    page_icon="favicon.ico",
    layout="centered",
    initial_sidebar_state="expanded"
)

# ==========================================
#  2. 极简主义 CSS 注入 [修改点：新增卡片样式]
# ==========================================
st.markdown("""
<style>
     /* 1. 强制全局背景为深空蓝黑 */
    .stApp {
        background-color: #0b1121 !important;
        background-image: radial-gradient(circle at 50% 0%, #1e293b 0%, #0b1121 70%);
        color: white !important; /* 强制全局文字变白 */
    }
    /* --- 核心修复：拓宽中间主内容区域 --- */
    /* 默认 centered 大概只有 730px，这里我们强制拓宽到 1000px 或更宽 */
    [data-testid="stMainBlockContainer"] {
        max-width: 65rem !important; /* 约 960px，您可以改成 65rem 或 70rem 甚至更宽 */
        padding-left: 2rem;
        padding-right: 2rem;
    }
/* --- 修复 1：找回侧边栏按钮 --- */
    /* 不要隐藏整个 Header，否则按钮也没了。只把背景变透明 */
    header[data-testid="stHeader"] {
        background-color: transparent !important;
    }
    /* 隐藏顶部的彩虹装饰线条 */
    [data-testid="stDecoration"] {
        display: none;
    }
    
    /* --- 修复 2：强制 AI 回答文字变白 --- */
    /* 针对聊天气泡内的所有文本元素强制设为白色 */
    [data-testid="stChatMessageContent"] p,
    [data-testid="stChatMessageContent"] span,
    [data-testid="stChatMessageContent"] div,
    [data-testid="stChatMessageContent"] li {
        color: #ffffff !important;
        line-height: 1.6;
    }
    /* 稍微调亮 Markdown 中的加粗文字 */
    [data-testid="stChatMessageContent"] strong {
        color: #fcf7f7 !important; /* 金黄色高亮，更易读 */
    }
    
    /* --- [关键修复 1] 代码块样式修复 --- */
    /* 强制代码块背景为深色，文字为亮色 */
    [data-testid="stChatMessageContent"] code {
        background-color: #2b313e !important;
        color: #e6e6e6 !important;
        border: 1px solid #3b4252;
        border-radius: 4px;
        padding: 0.2rem 0.4rem;
    }
    /* 多行代码块容器 */
    [data-testid="stChatMessageContent"] pre {
        background-color: #2b313e !important;
        border: 1px solid #3b4252;
        border-radius: 8px;
    }

/* --- [核心修改] 快捷指令卡片样式：深色背景 + 亮色文字 --- */
    .suggestion-card {
        background-color: #1E2329; /* 改为深色背景 */
        border: 1px solid #2d333b; /* 微亮的边框 */
        border-radius: 10px;
        padding: 15px;
        text-align: center;
        cursor: pointer;
        transition: all 0.2s ease-in-out;
        height: 100%;
        display: flex;
        flex-direction: column;
        justify-content: center;
        align-items: center;
        box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1); /* 加点阴影更有质感 */
    }
  /* --- [关键修复] 按钮样式重写 (解决白字白底问题) --- */
    /* 针对所有的 st.button */
    div.stButton > button {
        background-color: #1E2329 !important; /* 强制深色背景 */
        color: #e6e6e6 !important;            /* 强制亮色文字 */
        border: 1px solid #31333F !important; /* 边框 */
        border-radius: 8px !important;
        padding: 1.2rem !important;           /* 增加内边距 */
        height: auto !important;              /* 高度自适应 */
        white-space: pre-wrap !important;     /* 关键：允许文字换行 (\n) */
        width: 100% !important;               /* 填满列宽 */
        transition: all 0.2s ease-in-out !important;
        font-family: "Source Sans Pro", sans-serif !important;
    }

    /* 按钮 Hover (悬停) 状态 */
    div.stButton > button:hover {
        border-color: #ff4b4b !important;     /* 红色边框 */
        background-color: #262c36 !important; /* 稍微变亮的背景 */
        color: #ffffff !important;            /* 纯白文字 */
        transform: translateY(-2px);          /* 微微上浮效果 */
    }
    
    /* 按钮 Active/Focus (点击) 状态 */
    div.stButton > button:active, div.stButton > button:focus {
        background-color: #262c36 !important;
        color: #ffffff !important;
        border-color: #ff4b4b !important;
        box-shadow: none !important;
    }
    .card-icon { font-size: 24px; margin-bottom: 8px; }
    
    /* 标题文字：亮白色 */
    .card-title { 
        font-weight: bold; 
        font-size: 15px; 
        color: #e6e6e6 !important; /* 强制亮白 */
        margin-bottom: 4px;
    }
    /* 描述文字：稍暗的灰色 */
    .card-desc { 
        font-size: 13px; 
        color: #8b949e !important; /* 强制灰白 */
    }

    /* 调整底部输入框样式 */
    .stChatInput {
        padding-bottom: 20px;
    }
        /* 3. 侧边栏文字强制变白 */
    [data-testid="stSidebar"] {
        background-color: #0f172a !important;
    }
    [data-testid="stSidebar"] p, [data-testid="stSidebar"] span, [data-testid="stSidebar"] div {
        color: #cbd5e1 !important;
    }
</style>
""", unsafe_allow_html=True)


# ==========================================
#  3. Auth & State 初始化 (保持不变)
# ==========================================
def get_manager(): return stx.CookieManager(key="master_cookie_manager")


cookie_manager = get_manager()
cookies = cookie_manager.get_all() or {}

# 尝试从 Cookie 恢复登录
# 【关键修复 1】增加 'just_logged_out' 判断，如果刚点了登出，绝不执行自动登录
should_auto_login = not st.session_state.get('is_logged_in', False) and not st.session_state.get('just_logged_out',
                                                                                                 False)

if should_auto_login and cookies:
    c_user = cookies.get("username")
    c_token = cookies.get("token")

    if c_user and c_token and c_user.strip() != "":
        # 去数据库验证 Token
        if auth.check_token(c_user, c_token):
            st.session_state['is_logged_in'] = True
            st.session_state['user_id'] = c_user
            st.toast(f"欢迎回来，{c_user} (自动登录)")
            time.sleep(0.3)  # 給一點 UI 反應時間
            st.rerun()

# 【关键修复 2】如果已经是登出后的重跑，现在可以重置标记了
# 这样下次用户刷新页面(F5)时，如果 Cookie 还在(虽然应该删了)，还能尝试登录，或者单纯重置状态
if st.session_state.get('just_logged_out', False):
    st.session_state['just_logged_out'] = False

# 只有第一次运行时才初始化，如果已经登录了，不要重置它
if 'is_logged_in' not in st.session_state:
    st.session_state['is_logged_in'] = False
    st.session_state['user_id'] = None
    st.session_state['username'] = None

# --- 1. 初始化验证码 (如果还没生成过) ---
if 'captcha_code' not in st.session_state:
    img, code = generate_captcha_image()
    st.session_state['captcha_img'] = img
    st.session_state['captcha_code'] = code


def refresh_captcha():
    """刷新验证码的回调函数"""
    img, code = generate_captcha_image()
    st.session_state['captcha_img'] = img
    st.session_state['captcha_code'] = code


# 初始化聊天记录
if "messages" not in st.session_state:
    st.session_state.messages = []


# ==========================================
#  4. AI Agent 定义 (完全保留您的核心逻辑)
# ==========================================
def get_agent(current_user="访客"):  # 传入 current_user
    # ... (这里保留您原来的 prompt 和 tools) ...
    tools = [analyze_kline_pattern, search_investment_knowledge, get_market_snapshot, get_commodity_iv_info,
             get_price_statistics, check_option_expiry_status]
    if not os.getenv("DASHSCOPE_API_KEY"):
        st.error("❌ 未配置 API KEY");
        return None

    llm = ChatTongyi(model="qwen-plus", temperature=0.2)

    # 动态日期
    today = datetime.now()
    today_str = today.strftime('%Y%m%d')
    last_week_start = (today - timedelta(days=today.weekday() + 7)).strftime('%Y%m%d')
    last_week_end = (today - timedelta(days=today.weekday() + 1)).strftime('%Y%m%d')

    system_message = f"""
    你是一位专业的K线技术分析师和期权专家，遵守顺势交易的纪律。 
    

    【当前时间基准】：
    - 今天是：{today.strftime('%Y年%m月%d日')} (数据库查询请使用: {today_str})
    - 上周区间参考：{last_week_start} 至 {last_week_end}

    【工具使用指南】：
    1. 被问当前/最新价格数据时 -> 用 `get_market_snapshot`。
    2. 被问 **历史某一天** 或 **指定日期** 的价格-> 可以用 `get_price_statistics`。
       调用此工具时，`start_date` 和 `end_date` 参数必须是 **YYYYMMDD** 格式的字符串（例如 '20231001'）。
    3. 被问股票或期货的技术面、K线形态和趋势时-> 用 `analyze_kline_pattern`
    4. 需要期权知识、期权策略-> 用 `search_investment_knowledge`
    5. 被问期权波动率数据时 -> 用 `get_commodity_iv_info`。
    6. 需要查询期权到期日时 -> 用 `check_option_expiry_status`。

    【你的行为准则】
    1. 避免同时调用超过2个工具，除非用户明确要求全面分析。
    2. 如果用户问题不具体，可以反问客户，多用反问来引导用户做交易决策。
    3. 当用户问期权或K线实战应用问题，优先以知识库工具为信息参考。
    4. 期权策略的建议，需要考虑波动率和距离到期日，使用工具`check_option_expiry_status`和知识库搭配回答
    5. 给出明确的操作建议，根据用户风险偏好（激进/保守）给他喜欢的策略，如果是保守的，就不要给激进建议。

    【回答格式】
   先给结论（看多/看空/震荡），然后解释理由。技术分析只说K线，期权策略的使用要结合技术面和IV。
    """

    try:
        return create_react_agent(llm, tools, state_modifier=system_message)
    except TypeError:
        try:
            return create_react_agent(llm, tools, messages_modifier=system_message)
        except:
            return create_react_agent(llm, tools)


# ==========================================
#  5. 核心逻辑处理函数 [修改点：封装成函数以便复用]
# ==========================================
def process_user_input(prompt_text):
    """处理用户输入（无论是来自输入框还是快捷卡片）"""
    # 1. 显示用户消息
    st.session_state.messages.append({"role": "user", "content": prompt_text})
    with st.chat_message("user"):
        st.markdown(prompt_text)

    # 2. 生成 AI 回复
    current_user = st.session_state.get('user_id', "访客")
    agent = get_agent(current_user)

    if agent:
        with st.chat_message("assistant"):
            with st.spinner("⚡ AI正在思考..."):
                try:
                    # 构建 LangChain 消息历史
                    history = [
                        HumanMessage(content=m["content"]) if m["role"] == "user" else AIMessage(content=m["content"])
                        for m in st.session_state.messages]

                    # 注入用户画像 (保留您原有的逻辑)
                    user_profile = de.get_user_profile(current_user)
                    risk = user_profile.get('risk_preference', '未知')

                    system_instruction = SystemMessage(content=f"""
                                        【当前对话元数据】
                                        - 用户名：{current_user}
                                        - 风险偏好：{risk}
                                        """)
                    # 将 system_instruction 加入 history
                    history.insert(0, system_instruction)

                    response = agent.invoke(
                        {"messages": history},
                        config={"recursion_limit": 100}
                    )
                    ai_response = response["messages"][-1].content

                    # 打字机效果显示
                    placeholder = st.empty()
                    full_response = ""
                    #  for chunk in ai_response.split():  # 简单模拟流式 (由于 invoke 是同步的，这里只是为了视觉效果)
                    #   full_response += chunk + " "
                    #   placeholder.markdown(full_response + "▌")
                    #   time.sleep(0.01)
                    placeholder.markdown(full_response)

                    st.session_state.messages.append({"role": "ai", "content": ai_response})

                    # 更新记忆
                    if hasattr(de, 'update_user_memory_async'):
                        de.update_user_memory_async(current_user, prompt_text)

                except Exception as e:
                    st.error(f"分析中断: {e}")


# ==========================================
#  6. 页面渲染：Welcome Screen (空状态) [修改点：新增]
# ==========================================
def show_welcome_screen():
    st.markdown("<br><br>", unsafe_allow_html=True)
    st.markdown("<h1 style='text-align: center; color: #fff;'>🤓 嗨，我是爱波塔</h1>", unsafe_allow_html=True)
    st.markdown("<p style='text-align: center; color: #8b949e;'>陪你在金融市场奋斗</p>",
                unsafe_allow_html=True)
    st.markdown("<br>", unsafe_allow_html=True)

    # --- 快捷指令卡片 ---
    col1, col2, col3 = st.columns(3)

    # 定义点击回调
    # --- 关键修改：定义回调函数 ---
    # 这个函数会在页面重新加载前优先执行，确保数据这就位
    def set_prompt_callback(text):
        st.session_state.pending_prompt = text

    with col1:
        st.button("📉 波动率分析-50ETF期权现在贵吗？",
                     use_container_width=True,
                     on_click=set_prompt_callback,
                     args=("50ETF期权现在的IV高吗？",)
         )

    with col2:
        st.button("📅 期权学习-什么是飞龙在天？",
                     use_container_width=True,
                     on_click=set_prompt_callback,
                     args=("期权的飞龙在天是什么策略？",)
         )

    with col3:
        st.button("🎯 K线分析-帮我看今天的白银",
                     use_container_width=True,
                     on_click=set_prompt_callback,
                     args=("分析今天白银K线",)
         )

# ==========================================
#  7. 主程序入口
# ==========================================

# A. 侧边栏：登录/设置 (折叠起来保持清爽)
with st.sidebar:
    if not st.session_state['is_logged_in']:
        # --- A. 未登錄狀態 ---
        tab1, tab2 = st.tabs(["登录", "注册"])

        with tab1:
            with st.form("login_form_sidebar"):
                u = st.text_input("账号", key="login_user")
                p = st.text_input("密码", type="password", key="login_pass")
                if st.form_submit_button("登录", type="primary", use_container_width=True):
                    success, msg, token = auth.login_user(u, p)
                    if success:
                        st.session_state['is_logged_in'] = True
                        st.session_state['user_id'] = u

                        # 【關鍵修改】寫入 Cookie (設置 7 天過期)
                        # expires_at 是 datetime 對象
                        expires = datetime.now() + timedelta(days=7)

                        cookie_manager.set("username", u, expires_at=expires, key="set_user_cookie")
                        cookie_manager.set("token", token, expires_at=expires, key="set_token_cookie")

                        st.success("登录成功")
                        time.sleep(0.5)
                        st.rerun()
                    else:
                        st.error(msg)

        with tab2:
            with st.form("reg_form_sidebar"):
                st.write("### 注册新账号")
                new_user = st.text_input("新账号", key="reg_user")
                new_pass = st.text_input("新密码", type="password", key="reg_pass")
                # --- 2. 显示验证码 ---
                col1, col2 = st.columns([1, 1])
                with col1:
                    # 显示图片
                    st.image(st.session_state['captcha_img'], caption="请输入右侧数字", width=120)
                with col2:
                    # 验证码输入框
                    captcha_input = st.text_input("验证码", placeholder="4位数字", label_visibility="collapsed")

                # 刷新按钮 (放在 form 外面或者用特殊处理，最简单是让用户输错自动刷新)
                # 这里我们直接放提交按钮
                submit_btn = st.form_submit_button("注册")
        # --- 3. 处理提交逻辑 ---
        if submit_btn:
            # 🛑 第一关：检查输入是否为空
            if not new_user or not new_pass or not captcha_input:
                st.warning("⚠️ 请填写完整的账号、密码和验证码！")

                # 🛑 第二关：检查验证码
                # 注意：这里判断是否不相等
            elif captcha_input != st.session_state.get('captcha_code'):
                st.error("❌ 验证码错误！已为您更换一张，请重新输入。")
                refresh_captcha()
                # 注意：这里不要加 st.rerun()，否则错误提示会瞬间消失！
                # 用户看到错误后，手动再次点击注册即可。


            # C. 执行注册 (调用之前的 register_user 函数)
            else:
                success, msg = auth.register_user(new_user, new_pass)
                if success:
                    # --- 🎉 注册成功后的自动登录逻辑 ---
                    st.success("注册成功！正在为您自动登录...")
                    # 1. 我们需要拿到刚注册的 user_id (为了后续功能使用)
                    try:
                        login_success, login_msg, token = auth.login_user(new_user, new_pass)

                        # 2. 关键步骤：修改 Session 状态
                        # 这几行代码就是“告诉 Streamlit 我已经登录了”
                        st.session_state['is_logged_in'] = True
                        st.session_state['user_id'] = new_user

                        # 設置 Cookie (保持登錄狀態)
                        expires = datetime.now() + timedelta(days=7)
                        cookie_manager.set("username", new_user, expires_at=expires, key="reg_set_user")
                        cookie_manager.set("token", token, expires_at=expires, key="reg_set_token")

                        time.sleep(0.3)
                        st.rerun()

                        # 3. 清理注册时用的验证码 (防止返回后还在)
                        if 'captcha_code' in st.session_state:
                            del st.session_state['captcha_code']

                        # 4. 强制刷新页面
                        # 刷新后，Streamlit 会重新运行，发现 logged_in=True，就会直接显示主页，而不是登录页
                        st.rerun()
                    except Exception as e:
                        st.error(f"自动登录失败，请尝试手动登录: {e}")
                else:
                    # === ❌ 注册失败 (比如密码太短、用户已存在) ===
                    # 这里直接显示 register_user 函数返回的错误消息
                    st.error(f"❌ {msg}")
                    # 同样，这里也不要 rerun，让用户看到错误并去修改
                    refresh_captcha()  # 为了安全，失败时最好也刷新验证码

        # 加一个小按钮允许用户手动刷新看不清的图片
        if st.button("🔄 看不清？换一张"):
            refresh_captcha()
            st.rerun()

    else:
        # --- B. 已登錄狀態 ---
        user = st.session_state['user_id']
        st.success(f"👤 欢迎回来，{user}")

        # 顯示資產 (模擬)
        try:
            info = pd.read_sql(f"SELECT level, capital FROM users WHERE username='{user}'", de.engine).iloc[0]
            st.metric("爱波币", f"¥{int(info['capital']):,}", f"Lv.{info['level']}")
        except:
            pass

        if st.button("登出", type="primary"):
            st.session_state['is_logged_in'] = False
            st.session_state['user_id'] = None

            # 【關鍵修改 3】設置一個“剛登出”的標記，防止 Rerun 後立馬被自動登錄捕獲
            st.session_state['just_logged_out'] = True

            # 【關鍵修改】刪除 Cookie
            cookie_manager.delete("username", key="del_user_cookie")
            cookie_manager.delete("token", key="del_token_cookie")

            time.sleep(0.3)
            st.rerun()


    if st.button("🗑️ 清空对话历史", use_container_width=True):
        st.session_state.messages = []
        st.rerun()

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

# B. 处理卡片点击产生的 Pending Prompt [修改点：处理快捷指令]
if "pending_prompt" in st.session_state:
    prompt = st.session_state.pending_prompt
    del st.session_state.pending_prompt  # 消费掉，防止循环
    process_user_input(prompt)
    st.rerun()  # 重新加载以显示新消息

# C. 主界面渲染 [修改点：根据是否有消息切换视图]
if not st.session_state.messages:
    # 场景 1：没有消息 -> 显示欢迎页 (Hero Section)
    show_welcome_screen()
else:
    # 场景 2：有消息 -> 显示聊天历史
    for msg in st.session_state.messages:
        with st.chat_message(msg["role"]):
            st.markdown(msg["content"])

# D. 底部输入框 (Sticky Footer) [修改点：使用 st.chat_input]
if prompt := st.chat_input("我受过交易汇训练，欢迎问我任何实战交易问题..."):
    if not st.session_state['is_logged_in']:
        st.warning("🔒 请先在左侧侧边栏登录")
    else:
        process_user_input(prompt)
        st.rerun()  # 确保界面更新