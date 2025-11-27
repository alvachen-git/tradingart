import streamlit as st
import pandas as pd
import os
import sys
import plotly.express as px
from dotenv import load_dotenv
from fed_data import get_fed_probabilities

# --- AI 相关导入 (LangGraph 版) ---
from langchain_community.chat_models import ChatTongyi
from langchain_core.messages import HumanMessage, AIMessage, SystemMessage
# 【关键修改】使用 LangGraph 的预构建 Agent
try:
    from langgraph.prebuilt import create_react_agent
except ImportError:
    st.error("❌ 请先安装 LangGraph: `pip install langgraph`")
    st.stop()

from kline_tools import analyze_kline_pattern



# 1. 初始化环境
load_dotenv(override=True)

current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(current_dir)
import data_engine as de


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


# ==========================================
#  AI Agent 初始化 (LangGraph 版)
# ==========================================
def get_agent():
    # 1. 定义工具箱
    tools = [analyze_kline_pattern]

    # 2. LLM
    if not os.getenv("DASHSCOPE_API_KEY"):
        st.error("未配置 API KEY")
        return None

    llm = ChatTongyi(model="qwen-plus", temperature=0.1)

    # 3. 系统提示词 (System Prompt)
    system_message = """
    你是一位专业的K线技术分析师。
    你拥有一个强大的工具 `analyze_kline_pattern`，可以计算任何品种的 K 线形态、均线趋势。

    【你的行为准则】
    1. 当用户询问某个品种（如碳酸锂、螺纹钢）的“走势”、“技术面”、“形态”时，**必须**调用工具获取数据。
    2. 拿到工具返回的报告后，请用通俗易懂的语言解读给用户听。
    3. 如果形态是“大阳线”或“金针探底”，提示机会；如果是“大阴线”或“射击之星”，提示风险。
    4. 如果用户没有明确说明什么品种，你就反问客户把问题说详细点
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
st.markdown("### 🤖 陈老师分身")
st.caption("您可以问我：**“碳酸锂技术面怎么样？”** 或 **“螺纹钢现在是多头趋势吗？”**")

# 1. 初始化聊天记录
if "messages" not in st.session_state:
    st.session_state.messages = []

# 2. 输入区域 (放在顶部)
col_input, col_btn = st.columns([4, 1])

with col_input:
    # 使用普通的 text_input，不固定在底部
    user_query = st.text_input("请输入您的问题...", key="ai_query_input", label_visibility="collapsed",
                               placeholder="请输入品种代码或名称（例如：lc, 碳酸锂）...")

with col_btn:
    # 提交按钮
    submit_btn = st.button("发问", type="primary", width='stretch')

# 3. 处理提交逻辑
if submit_btn and user_query:
    # 添加用户消息到历史
    st.session_state.messages.append({"role": "user", "content": user_query})

    # 获取 Agent
    agent = get_agent()
    if agent:
        with st.spinner("正在思考，请稍候..."):
            try:
                # 构建历史记录对象
                history = [
                    HumanMessage(content=m["content"]) if m["role"] == "user" else AIMessage(content=m["content"]) for m
                    in st.session_state.messages[:-1]]
                history.append(HumanMessage(content=user_query))

                # 调用 Agent
                response = agent.invoke({"messages": history})

                # 获取 AI 回复
                ai_response = response["messages"][-1].content

                # 添加 AI 回复到历史
                st.session_state.messages.append({"role": "ai", "content": ai_response})

            except Exception as e:
                st.error(f"分析失败: {e}")

# 4. 显示最新的 AI 回复 (醒目展示)
if st.session_state.messages:
    last_msg = st.session_state.messages[-1]
    if last_msg["role"] == "ai":
        st.info(f"**AI 分析师回复：**\n\n{last_msg['content']}")

# 5. 折叠显示历史记录 (不占用主屏幕)
with st.expander("查看历史对话记录"):
    for msg in st.session_state.messages:
        role_label = "👤 用户" if msg["role"] == "user" else "🤖 AI"
        st.markdown(f"**{role_label}:** {msg['content']}")
        st.markdown("---")

st.markdown("---")

# --- 外资动向卡片 ---
st.markdown("### 🌍 外资动向 (摩根/瑞银/乾坤)")

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
                                            <div style="font-size:0.8rem; margin-top:5px;">
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
st.markdown("### ⚔️ 多空巅峰对决")
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
st.markdown("### 🏆 全品种盈亏排行榜")
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

st.subheader("🏦 美联储降息概率预测 (CME FedWatch)")

# 获取数据
df_fed = get_fed_probabilities()

if df_fed is not None and not df_fed.empty:
    # 获取最近的一次会议日期
    next_meeting = df_fed['会议日期'].iloc[0]

    # 筛选出最近一次会议的数据
    df_next = df_fed[df_fed['会议日期'] == next_meeting]

    st.info(f"📅 下一次议息会议日期：**{next_meeting}**")

    # 画图 (柱状图)
    fig = px.bar(
        df_next,
        x='目标利率',
        y='概率(%)',
        text='概率(%)',
        title=f"{next_meeting} 利率决议概率分布",
        color='概率(%)',
        color_continuous_scale='Blues'
    )
    fig.update_traces(texttemplate='%{text:.1f}%', textposition='outside')
    st.plotly_chart(fig, use_container_width=True)

    # 显示完整表格 (放在折叠栏里)
    with st.expander("查看未来所有会议的详细数据"):
        st.dataframe(df_fed, use_container_width=True)
else:
    st.error("无法获取 CME 数据，请检查服务器网络连接。")
