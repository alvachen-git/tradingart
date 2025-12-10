import streamlit as st
import pandas as pd
import data_engine as de
import os
import sys
import plotly.express as px
import auth_utils as auth
from datetime import datetime, timedelta
from kline_tools import analyze_kline_pattern
import time
import extra_streamlit_components as stx
from market_tools import get_market_snapshot, get_price_statistics

# --- 1. 【关键修复】强制清除系统代理 (解决 SSL 报错) ---
# 必须放在其他网络库加载之前
for key in ["HTTP_PROXY", "HTTPS_PROXY", "http_proxy", "https_proxy"]:
    if key in os.environ:
        del os.environ[key]

from captcha_utils import generate_captcha_image
from sqlalchemy import text
from dotenv import load_dotenv
from knowledge_tools import search_investment_knowledge
# --- AI 相关导入 (LangGraph 版) ---
from langchain_community.chat_models import ChatTongyi
from langchain_core.messages import HumanMessage, AIMessage, SystemMessage

# 1. 初始化环境
load_dotenv(override=True)

# 页面配置
st.set_page_config(
    page_title="爱波塔-懂期权的AI-陪你在市场奋斗",
    page_icon="favicon.ico",
    layout="wide",
    initial_sidebar_state="expanded"
)

# B. 【核心修复】强制注入深色主题 CSS (解决手机端白屏问题)
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

# 1. 初始化 Cookie 管理器 (必须在页面内容之前)
# 注意：這個函數本身就有緩存機制，不需要額外加 @st.cache_resource
def get_manager():
    return stx.CookieManager(key="master_cookie_manager")


cookie_manager = get_manager()

# 獲取所有 Cookies (用於讀取)
# 注意：stx 需要一點時間從瀏覽器讀取，首次加載可能為 None
cookies = cookie_manager.get_all()


# 【关键修改】使用 LangGraph 的预构建 Agent
try:
    from langgraph.prebuilt import create_react_agent
except ImportError:
    st.error("❌ 请先安装 LangGraph: `pip install langgraph`")
    st.stop()


current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(current_dir)



# 加载 CSS
with open('style.css', encoding='utf-8') as f:
    st.markdown(f'<style>{f.read()}</style>', unsafe_allow_html=True)

# ==========================================
#  【关键修复】 全局状态初始化 (必须放在最前面！)
# ==========================================


# ==========================================
#  會話狀態初始化
# ==========================================

# 尝试从 Cookie 恢复登录
if not st.session_state.get('is_logged_in', False):
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


# ==========================================
#  側邊欄：統一的登錄/用戶中心
# ==========================================
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
            # 【關鍵修改】刪除 Cookie
            cookie_manager.delete("username", key="del_user_cookie")
            cookie_manager.delete("token", key="del_token_cookie")
            time.sleep(0.3)
            st.rerun()

    st.markdown("---")


# ==========================================
#  AI Agent 初始化 (LangGraph 版)
# ==========================================
def get_agent(user_name="访客"):
    # 1. 定义工具箱
    tools = [analyze_kline_pattern, search_investment_knowledge, get_market_snapshot, get_price_statistics]

    # 2. LLM
    if not os.getenv("DASHSCOPE_API_KEY"):
        st.error("未配置 API KEY")
        return None

    llm = ChatTongyi(model="qwen-plus", temperature=0.2)

    # 3. 系统提示词 (System Prompt)
    system_message = f"""
    你是一位专业的K线技术分析师和期权专家。 【当前时间】：{datetime.now().strftime('%Y年%m月%d日')}
    
    【工具使用指南】：
    1. 被问商品价格数据或期权数据时 -> 用 `get_market_snapshot`。
    2. 分析行情技术面、K线形态和趋势-> 用 `analyze_kline_pattern`。
    3. 查阅期权知识、期权策略、进出场方法-> 用 `search_investment_knowledge`
    4. 被問 **「上个月涨了多少」、「本周黃金和白银誰強」、「历史最高价」** -> 用 `get_price_statistics`。
       - **注意**：調用`get_price_statistics`这工具時，必須根据当前日期（{datetime.now().strftime('%Y%m%d')}），自动计算出准确的 start_date 和 end_date 参数传给工具。

    【你的行为准则】
    1. **情绪感知**：在回答前，先在心里分析用户的情绪（贪婪/恐惧/愤怒/理性）。
    2. **风险评估**：根据用户的问题判断其风险偏好（激进/保守）。
    3. 如果用户问题不具体，可以反问客户，多用反问来引导用户做交易决策。
    4. 当用户询问某个品种（如碳酸锂、中证1000）的“走势”、“技术分析”、“K线形态”时，你要调用工具来回答。
    5. 当用户问期权或实战技术问题，优先以知识库工具为信息参考。
    6. 要结合K线分析和期权知识，给出明确的操作建议，风险偏好高的可以给积极的策略，风险偏好低的就给保守策略。

    【回答格式】
    先说结论（看多/看空/震荡），最后解释理由，技术分析理由只说明K线，不说其他技术指标，期权策略的建议是根据技术分析和IV。

    """

    # 4. 创建 Agent (自动适配参数名)
    try:
        # 尝试使用新版参数 state_modifier
        agent = create_react_agent(llm, tools, state_modifier=system_message)
    except TypeError:
        # 如果报错，尝试使用旧版参数 messages_modifier
        try:
            agent = create_react_agent(llm, tools, messages_modifier=system_message)
        except TypeError:
            # 如果还不行，就不传 modifier，先保证不崩
            agent = create_react_agent(llm, tools)

    return agent


# --- 首页内容 ---

# ==========================================
#  (新) 顶部 AI 操盘手 (普通输入框模式)
# ==========================================

if st.session_state['is_logged_in']:
    # === 已登錄：顯示完整功能 ===
    current_user = st.session_state['user_id']
    st.caption(f"正在為 **{current_user}** 提供個性化服務...")

    # 1. 初始化聊天记录
    if "messages" not in st.session_state:
        st.session_state.messages = []


    # 2. 定义回调函数 (核心魔法)
    def submit_query():
        """
        当用户按回车或点击发送时触发：
        1. 把输入框的内容转存到 'current_query' 变量
        2. 把输入框清空
        """
        if st.session_state.ai_query_input:  # 如果输入框不为空
            st.session_state.current_query = st.session_state.ai_query_input
            st.session_state.ai_query_input = ""  # 清空输入框


    # 初始化中间变量
    if "current_query" not in st.session_state:
        st.session_state.current_query = None

    # 3. 输入区域 (绑定回调)
    col_input, col_btn = st.columns([4, 1])

    with col_input:
        # 注意：这里绑定了 on_change=submit_query，按回车会自动触发
        st.text_input(
            "请输入您的问题...",
            key="ai_query_input",
            label_visibility="collapsed",
            placeholder="我是陈老师分身，你可以问实战问题（例如：50ETF现在能买期权吗）...",
            on_change=submit_query
        )

    with col_btn:
        # 注意：这里绑定了 on_click=submit_query，点击也会触发
        st.button("发送", type="primary", use_container_width=True, on_click=submit_query)

    # 4. 处理逻辑 (只检查 current_query)
    # 只有当回调函数把内容存进 current_query 时，才执行 AI
    if st.session_state.current_query:
        prompt = st.session_state.current_query

        # --- 立即清除 current_query，防止刷新页面后重复执行 ---
        st.session_state.current_query = None

        # --- 下面是正常的 AI 处理逻辑 (和之前一样) ---
        st.chat_message("user").markdown(prompt)
        st.session_state.messages.append({"role": "user", "content": prompt})

        # 获取 Agent
        # 注意：这里要获取当前登录用户
        current_user = st.session_state.get('user_id', "访客")
        agent = get_agent(current_user)

        if agent:
            with st.chat_message("assistant"):
                with st.spinner("AI 正在思考..."):
                    try:
                        # 构建历史
                        history = [HumanMessage(content=m["content"]) if m["role"] == "user" else AIMessage(
                            content=m["content"]) for m in st.session_state.messages[:-1]]
                        history.append(HumanMessage(content=prompt))

                        # 注入用户画像 (可选，之前写的)
                        user_profile = de.get_user_profile(current_user)
                        risk = user_profile.get('risk_preference', '未知')
                        assets = user_profile.get('focus_assets', '暂无')

                        # 2. 构建一条"系统指令"，强行塞给 AI
                        # 这比 system_prompt 更管用，因为它是当前对话的一部分
                        system_instruction = SystemMessage(content=f"""
                                            【当前对话元数据】
                                            - 用户名：{current_user}
                                            - 风险偏好：{risk}
                                            - 关注品种：{assets}

                                            """)

                        # 调用 Agent
                        response = agent.invoke(
                            {"messages": history},
                            config={"recursion_limit": 50}
                        )
                        ai_response = response["messages"][-1].content

                        st.markdown(ai_response)
                        st.session_state.messages.append({"role": "ai", "content": ai_response})

                        # 更新记忆
                        if hasattr(de, 'update_user_memory_async'):
                            de.update_user_memory_async(current_user, prompt)

                    except Exception as e:
                        st.error(f"分析失败: {e}")

    with st.expander("查看历史对话记录"):
        for msg in st.session_state.messages:
            role_label = "👤 用户" if msg["role"] == "user" else "🤖 AI"
            st.markdown(f"**{role_label}:** {msg['content']}")
            st.markdown("---")

else:
    # === 未登錄：顯示鎖定狀態 ===
    st.warning("🔒 此功能僅對會員開放。請在左側登錄後使用。")

st.markdown("---")

# --- 外资动向卡片 ---
st.caption("### 🌍 外资动向 (摩根/瑞银/乾坤)")

# 读库
try:
    # 获取最新日期
    latest_f_date = pd.read_sql("SELECT MAX(trade_date) FROM foreign_capital_analysis", de.engine).iloc[0, 0]

    if latest_f_date:
        df_foreign = pd.read_sql(f"SELECT * FROM foreign_capital_analysis WHERE trade_date='{latest_f_date}'",
                                 de.engine)

        if not df_foreign.empty:
            # 使用列布局展示卡片
            cols = st.columns(4)
            for i, row in df_foreign.iterrows():
                # 循环使用列
                with cols[i % 4]:
                    # --- 【新增】清洗機構名稱 ---
                    # 去除 (代客)、（代客）等後綴
                    cleaned_brokers = row['brokers'].replace('（代客）', '').replace('(代客)', '')

                    color = "#d32f2f" if row['direction'] == "做多" else "#2e7d32"

                    st.markdown(f"""
                                        <div class="metric-card" style="border-top: 3px solid {color};">
                                            <div class="metric-label">{row['symbol'].upper()}</div>
                                            <div class="metric-value" style="color:{color}">{row['direction']}</div>
                                            <div class="metric-delta" style="font-size:0.8rem; color:#888;">
                                               {cleaned_brokers} </div>
                                            <div style="font-size:0.8rem; margin-top:5px; color:#3b3b3b;">
                                               淨量: {int(row['total_net_vol']):,}
                                            </div>
                                        </div>
                                        """, unsafe_allow_html=True)
        else:
            st.info("今日外资无明显共振操作。")
    else:
        st.info("暂无外资分析数据，请运行 calc_foreign_capital.py。")

except Exception as e:
    st.error(f"读取外资数据失败: {e}")

st.markdown("---")

# --- 新增：多空巔峰對決 (Smart vs Dumb) ---
st.caption("### ⚔️ 多空巅峰对决")
st.caption("筛选逻辑：机构与散户差异最大的持仓对比")

# 1. 獲取數據 (直接讀表)
try:
    # 檢查表裡是否有數據
    latest_c_date = pd.read_sql("SELECT MAX(trade_date) FROM market_conflict_daily", de.engine).iloc[0, 0]

    if latest_c_date:
        df_conflict = pd.read_sql(f"SELECT * FROM market_conflict_daily WHERE trade_date='{latest_c_date}'", de.engine)

        if not df_conflict.empty:
            # 創建 4 列佈局
            cols = st.columns(4)
            for i, row in df_conflict.iterrows():
                with cols[i % 4]:  # 防止超過4個報錯
                    # 顏色邏輯
                    direction = row['action']
                    color = "#d32f2f" if direction == "看漲" else "#2e7d32"  # 紅漲綠跌

                    # HTML 結構 (引用上面定義好的 CSS 類名)
                    card_html = f"""
        <div class="conflict-card" style="border-top: 4px solid {color};">
        <div class="conflict-header">
        <div class="conflict-symbol">{row['symbol'].upper()}</div>
        <div class="conflict-direction" style="color: {color};">{direction}</div>
        </div>
        <div class="conflict-body">
        <div class="conflict-item-left">
        <div class="conflict-label">反指(散户)</div>
        <div class="conflict-value" style="color: #333;">{int(row['dumb_net']):,}</div>
        </div>
        <div style="width: 1px; height: 20px; background-color: #ddd;"></div>
        <div class="conflict-item-right">
        <div class="conflict-label">正指(主力)</div>
        <div class="conflict-value" style="color: {color};">{int(row['smart_net']):,}</div>
        </div>
        </div>
        </div>
        """
                    st.markdown(card_html, unsafe_allow_html=True)
        else:
            st.info("今日市場平靜，無明顯正反博弈信號。")
    else:
        st.info("暫無對決數據，請運行後台計算腳本。")

except Exception as e:
    st.error(f"讀取對決數據失敗: {e}")

st.markdown("---")

# 2. 【新增】全市场风云榜
st.caption("### 🏆 全品种盈亏排行榜")
st.caption("统计范围：近200天, (部分期货商亏损是因为做套保)")

# 获取数据
with st.spinner("正在扫描全市场数据..."):
    df_win, df_lose = de.get_cross_market_ranking(days=150, top_n=5)

if not df_win.empty:
    col_win, col_lose = st.columns(2)

    with col_win:

        st.markdown("**👑 盈利王 (Top 5)**")

        # 绘制条形图
        fig_win = px.bar(
            df_win.sort_values('score', ascending=True),  # 升序是为了让最大的在上面
            x='score', y='broker',
            orientation='h',
            text_auto='.0f',
            color='score',
            color_continuous_scale='Reds'
        )
        fig_win.update_layout(
            plot_bgcolor='white',
            margin=dict(l=0, r=0, t=0, b=0),
            height=200,
            xaxis=dict(showgrid=False, title=None),
            yaxis=dict(title=None),
            coloraxis_showscale=False  # 隐藏色条
        )
        st.plotly_chart(fig_win, use_container_width=True)

    with col_lose:

        st.markdown("**💸 亏损王 (Top 5)**")

        # 绘制条形图
        fig_lose = px.bar(
            df_lose.sort_values('score', ascending=False),  # 降序是为了让负分最大的在上面
            x='score', y='broker',
            orientation='h',
            text_auto='.0f',
            color='score',
            color_continuous_scale='Teal_r'  # 绿色系倒序
        )
        fig_lose.update_layout(
            plot_bgcolor='white',
            margin=dict(l=0, r=0, t=0, b=0),
            height=200,
            xaxis=dict(showgrid=False, title=None),
            yaxis=dict(title=None),
            coloraxis_showscale=False
        )
        st.plotly_chart(fig_lose, use_container_width=True)


else:
    st.warning("暂无足够数据进行全市场排名。")

st.markdown("---")
