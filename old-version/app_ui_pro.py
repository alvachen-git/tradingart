import streamlit as st
import pandas as pd
import plotly.express as px
import data_engine as de

# 1. 页面配置
st.set_page_config(
    page_title="Alpha 智能期货终端",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded"
)

# --- 2. CSS 样式表 (详细解构版) ---
st.markdown("""
<style>
    /* === 全局字体 === */
    .stApp {
        font-family: 'Microsoft YaHei', sans-serif;
    }

    /* === 侧边栏 (Sidebar) === */
    /* 1. 侧边栏背景颜色 */
    [data-testid="stSidebar"] {
        background-color: #1E1E1E; /* 深黑色背景 */
    }

    /* 2. 侧边栏所有文字颜色 */
    [data-testid="stSidebar"] * {
        color: #E0E0E0 !important; /* 浅灰色文字 */
    }

    /* 3. 侧边栏大标题 (Alpha 交易台) */
    [data-testid="stSidebar"] h1 {
        color: #FF4B4B !important; /* 红色高亮 */
        font-size: 26px !important;
        font-weight: 800 !important;
        margin-bottom: 20px !important;
    }

    /* 4. 侧边栏分割线 (hr) */
    [data-testid="stSidebar"] hr {
        border-color: #444444 !important; /* 深灰色分割线 */
    }

    /* 5. 侧边栏输入框/下拉菜单 */
    [data-testid="stSidebar"] div[data-baseweb="select"] > div {
        background-color: #2D2D2D !important; /* 输入框深色背景 */
        border-color: #444444 !important;
        color: white !important;
    }

    /* 6. 侧边栏按钮 (红底白字) */
    [data-testid="stSidebar"] button {
        background-color: #D32F2F !important; /* 红色背景 */
        color: white !important;
        border: none;
        font-weight: bold;
    }
    [data-testid="stSidebar"] button:hover {
        background-color: #B71C1C !important; /* 悬停变深红 */
    }

    /* === 客服区域样式 === */
    .service-box {
        background-color: #252526;
        border: 1px solid #333;
        border-radius: 8px;
        padding: 10px;
        text-align: center;
        margin-top: 20px;
    }
    .service-title {
        color: #FF4B4B !important;
        font-size: 14px;
        font-weight: bold;
        margin-bottom: 5px;
    }
    .service-phone {
        color: white !important;
        font-size: 16px;
        font-weight: bold;
        margin-bottom: 5px;
    }
    .qr-img {
        width: 100px;
        border-radius: 4px;
        border: 2px solid #444;
    }
</style>
""", unsafe_allow_html=True)

# --- 3. 侧边栏逻辑 ---
with st.sidebar:
    st.title("交易汇-情报局")

    # --- 新增：市场类型选择 (未来做股票分析用) ---
    st.markdown("**市场类型**")  # 小标题
    market_type = st.selectbox(
        "选择市场",
        ["期货市场 (Futures)", "股票市场 (Stock)"],
        index=0,
        label_visibility="collapsed"  # 隐藏自带的 label，用上面的 markdown 代替
    )

    if market_type == "股票市场 (Stock)":
        st.info("🚧 股票功能开发中...")
        st.stop()  # 暂时停止后续代码执行

    st.markdown("---")

    # --- 期货品种选择 ---
    st.markdown("**选择标的**")
    COMMODITIES = {
        "lc0": "碳酸锂 (LC)",
        "si0": "工业硅 (SI)",
        "IF0": "沪深300 (IF)",
        "IM0": "中证1000 (IM)",
        "IC0": "中证500 (IC)",
        "IH0": "上证50 (IH)",
        "T0": "10年期国债 (T)",
        "rb0": "螺纹钢 (RB)",
        "hc0": "热卷 (HC)",
        "au0": "黄金 (AU)",
        "ag0": "白银 (AG)",
        "cu0": "沪铜 (CU)",
        "m0": "豆粕 (M)",
        "i0": "铁矿石 (I)",
        "p0": "棕榈油 (P)",
        "y0": "豆油 (Y)",
        "fg0": "玻璃 (FG)",
        "sa0": "纯碱 (SA)",
        "ma0": "甲醇 (MA)",
        "ta0": "PTA (TA)"
    }
    option_list = [f"{code} - {name}" for code, name in COMMODITIES.items()]
    selected_option = st.selectbox("选择标的", option_list, index=0, label_visibility="collapsed")

    current_code = selected_option.split(' ')[0]
    current_name = COMMODITIES[current_code].split(' (')[0]

    st.write("")
    if st.button("🔄 刷新实时数据", use_container_width=True):
        st.cache_data.clear()
        st.rerun()

    # --- 紧凑型客服区域 ---
    st.markdown("---")
    # 使用 HTML 渲染自定义客服框
    st.markdown("""
    <div class="service-box">
        <div class="service-title">客服联系方式</div>
        <div class="service-phone">📞17521591756</div>
        <img src="https://img.wanwang.xin/contents/sitefiles2048/10242148/images/51515307.png" class="qr-img">
        <div style="font-size:12px; color:#888; margin-top:5px;">加群可扫描咨询</div>
    </div>
    """, unsafe_allow_html=True)

# --- 4. 主界面逻辑 (保持不变) ---
# (以下代码无需修改，继续使用之前的逻辑)

# 数据加载
df_scores = de.calculate_broker_rankings(symbol=current_code)
if df_scores.empty:
    st.error(f"暂无 {current_name} 数据。")
    st.stop()

df_scores['broker'] = df_scores['broker'].str.replace('（代客）', '', regex=False).str.replace('(代客)', '',
                                                                                             regex=False).str.strip()
latest_date = df_scores['trade_date'].max()
rank_df = df_scores.groupby('broker').agg({'score': 'sum', 'trade_date': 'count', 'net_vol': 'mean'}).reset_index()
rank_df.columns = ['期货商', '总积分', '活跃天数', '平均持仓']
rank_df = rank_df[rank_df['活跃天数'] > 3]

all_dates = df_scores['trade_date'].drop_duplicates().sort_values(ascending=False)
target_dates = all_dates.head(10)
df_recent_10 = df_scores[df_scores['trade_date'].isin(target_dates)]
total_score_10d = df_recent_10['score'].sum()

if total_score_10d > 50:
    welcome_msg = f"今天 {current_name} 行情很好！🚀"
elif total_score_10d < -50:
    welcome_msg = f"{current_name} 情绪低迷。🥶"
else:
    welcome_msg = f"当前 {current_name} 博弈激烈。👀"

# 顶部
st.markdown(
    f"<h2 style='border-left:5px solid #D32F2F; padding-left:10px;'>{current_name} ({current_code}) 主力博弈</h2>",
    unsafe_allow_html=True)
st.caption(f"分析日期: {latest_date} | 监控: {len(rank_df)}家")

c_left, c_right = st.columns([1.2, 2])
with c_left:
    avatar_url = "https://picb8.photophoto.cn/39/857/39857958_1.jpg"
    st.markdown(f"""
    <div style="display:flex; align-items:center; background:#fff; padding:15px; border-radius:10px; box-shadow:0 2px 5px rgba(0,0,0,0.05);">
        <img src="{avatar_url}" style="width:60px; height:60px; border-radius:50%; margin-right:15px;">
        <div style="font-size:16px; color:#333;">{welcome_msg}</div>
    </div>
    """, unsafe_allow_html=True)

with c_right:
    with st.container(border=True):
        c_exp, c_btn = st.columns([2, 1])
        expert_symbol = ''.join([i for i in current_code if not i.isdigit()])
        expert_data = de.get_expert_sentiment(latest_date, symbol=expert_symbol)

        with c_exp:
            st.markdown("**👨‍⚖️ 投研观点**")
            if expert_data:
                s = expert_data['score']
                c = "#D32F2F" if s > 0 else "#2E7D32" if s < 0 else "#666"
                st.markdown(
                    f"<span style='font-size:18px; color:{c}; font-weight:bold'>{s}</span> | {expert_data['reason'][:15]}...",
                    unsafe_allow_html=True)
            else:
                st.caption("今日未录入")
        with c_btn:
            if st.button("✨ 生成报告", type="primary", use_container_width=True):
                if expert_data:
                    with st.spinner("AI 计算中..."):
                        rpt = de.generate_ai_report_agent(rank_df, expert_data, latest_date, current_name)
                        st.session_state['pro_report'] = rpt
                else:
                    st.error("无观点")

if 'pro_report' in st.session_state:
    st.info(st.session_state['pro_report'])

st.write("")

# 核心指标
top_broker = rank_df.sort_values('总积分', ascending=False).iloc[0]
bottom_broker = rank_df.sort_values('总积分', ascending=True).iloc[0]
positive_days = df_recent_10[df_recent_10['score'] > 0].groupby('broker')['trade_date'].count().reset_index()
if not positive_days.empty:
    pot_row = positive_days.sort_values('trade_date', ascending=False).iloc[0]
    pot_name = pot_row['broker']
    pot_days = pot_row['trade_date']
    pot_rate = (pot_days / 10) * 100
else:
    pot_name, pot_days, pot_rate = "无", 0, 0
monitor = ['东方财富', '平安期货']
target_df = df_scores[(df_scores['trade_date'] == latest_date) & (df_scores['broker'].isin(monitor))]
net_v = target_df['net_vol'].sum()

m1, m2, m3, m4 = st.columns(4)
m1.metric("近20日潜力王", pot_name, f"{pot_days}天盈利 ({int(pot_rate)}%)")
m2.metric("常胜军", top_broker['期货商'], f"{top_broker['总积分']:.1f} 分")
m3.metric("常败军", bottom_broker['期货商'], f"{bottom_broker['总积分']:.1f} 分", delta_color="inverse")
m4.metric("反指标风向", f"{int(abs(net_v)):,} 手", "做多" if net_v > 0 else "做空")

st.divider()

# 图表
c1, c2 = st.columns(2)
with c1:
    st.subheader("🔥 盈利排行")
    top10 = rank_df.sort_values('总积分', ascending=False).head(10)
    fig_win = px.bar(top10, x='总积分', y='期货商', orientation='h', text_auto='.1f', color='总积分',
                     color_continuous_scale='Reds')
    fig_win.update_layout(yaxis={'categoryorder': 'total ascending'}, plot_bgcolor='white',
                          margin=dict(l=0, r=0, t=0, b=0), height=350)
    st.plotly_chart(fig_win, use_container_width=True)
with c2:
    st.subheader("💧 亏损排行")
    bot10 = rank_df.sort_values('总积分', ascending=True).head(10)
    fig_lose = px.bar(bot10, x='总积分', y='期货商', orientation='h', text_auto='.1f', color='总积分',
                      color_continuous_scale='Teal_r')
    fig_lose.update_layout(yaxis={'categoryorder': 'total descending'}, plot_bgcolor='white',
                           margin=dict(l=0, r=0, t=0, b=0), height=350)
    st.plotly_chart(fig_lose, use_container_width=True)

st.divider()

# 深度透视
st.header("🔎 机构深度透视")
c_sel, c_info = st.columns([1, 3])
with c_sel:
    broker_list = rank_df.sort_values('总积分', ascending=False)['期货商'].unique()
    selected_broker = st.selectbox("👉 请选择期货商", broker_list)
    b_data = rank_df[rank_df['期货商'] == selected_broker].iloc[0]
    st.metric("累计总积分", f"{b_data['总积分']:.2f}")
    st.metric("平均持仓量", f"{int(b_data['平均持仓']):,}")

with c_info:
    if selected_broker:
        history = df_scores[df_scores['broker'] == selected_broker].sort_values('trade_date')
        history['累计积分'] = history['score'].cumsum()
        fig_line = px.area(history, x='trade_date', y='累计积分', title=f"{selected_broker} 资金曲线", markers=True)
        fig_line.update_traces(line_color='#D32F2F')
        fig_line.update_layout(plot_bgcolor='white', xaxis_gridcolor='#eee', yaxis_gridcolor='#eee', height=300)
        st.plotly_chart(fig_line, use_container_width=True)

        cols = ['trade_date', 'net_vol', 'pct_chg', 'score']
        for c in ['oi', 'weight', 'type']:
            if c in history.columns: cols.append(c)
        display_df = history[cols].sort_values('trade_date', ascending=False).copy()
        if 'pct_chg' in display_df.columns: display_df['pct_chg'] = display_df['pct_chg'] * 100
        st.dataframe(display_df, column_config={
            "trade_date": "日期", "net_vol": st.column_config.NumberColumn("净持仓", format="%d"),
            "pct_chg": st.column_config.NumberColumn("涨跌", format="%.2f%%"),
            "score": st.column_config.ProgressColumn("得分", min_value=-5, max_value=5, format="%.2f")
        }, use_container_width=True, hide_index=True, height=200)