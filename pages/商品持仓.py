import streamlit as st
import pandas as pd
import plotly.express as px
import sys
import os


# 1. 页面配置
st.set_page_config(
    page_title="爱波塔-期货持仓透视",
    page_icon="favicon.ico",
    layout="wide",
    initial_sidebar_state="collapsed"
)
# --- 路径修复: 确保能导入根目录的 data_engine ---
current_dir = os.path.dirname(os.path.abspath(__file__))
root_dir = os.path.dirname(current_dir)
sys.path.append(root_dir)
import data_engine as de

# 加载 CSS (注意路径)
css_path = os.path.join(root_dir, 'style.css')
with open(css_path, encoding='utf-8') as f:
    st.markdown(f'<style>{f.read()}</style>', unsafe_allow_html=True)

# --- 3. 侧边栏逻辑 (Desktop 显示 / Mobile 收起) ---
with st.sidebar:

    COMMODITIES = {
        "ih": "上证50",
        "if": "沪深300",
        "ic": "中证500 ",
        "im": "中证1000",
        "ts": "2年期国债",
        "t": "10年期国债",
        "tl": "30年期国债",
        "lc": "碳酸锂",
        "si": "工业硅",
        "ps": "多晶硅 ",
        "au": "黄金",
        "ag": "白银",
        "cu": "沪铜",
        "al": "沪铝",
        "zn": "沪锌",
        "ni": "沪镍",
        "zn": "沪锡",
        "rb": "螺纹钢",
        "hc": "热卷 ",
        "sp": "纸浆 ",
        "m": "豆粕",
        "a": "豆一",
        "b": "豆二",
        "c": "玉米",
        "lh": "生猪",
        "jd": "鸡蛋",
        "jm": "焦煤",
        "j": "焦炭",
        "i": "铁矿石",
        "p": "棕榈油",
        "y": "豆油",
        "v": "PVC",
        "eb": "苯乙烯",
        "eg": "乙二醇",
        "fg": "玻璃",
        "sa": "纯碱 ",
        "rm": "菜粕 ",
        "oi": "菜油 ",
        "ur": "尿素 ",
        "sr": "白糖",
        "cf": "棉花",
        "ap": "苹果",
        "sh": "烧碱",
        "ma": "甲醇",
        "ta": "PTA"
    }
    option_list = [f"{code} - {name}" for code, name in COMMODITIES.items()]
    # 这里使用 key 保持状态
    selected_option = st.selectbox("选择标的",option_list, index=0, key="sidebar_select")

    current_code = selected_option.split(' ')[0]
    current_name = COMMODITIES[current_code].split(' (')[0]

    st.write("")
    if st.button("刷新數據 :material/refresh:", use_container_width=True):
        st.cache_datast.cache_data.clear()
        st.rerun()

    st.markdown("---")
    # 使用 HTML 渲染自定义客服框
    st.markdown("""
        <div class="service-box">
            <div class="service-title">📞实战课程咨询</div>
            <div class="service-phone">17521591756</div>
            <img src="https://img.wanwang.xin/contents/sitefiles2048/10242148/images/51515307.png" class="qr-img">
            <div class="service-desc">扫码添加客服入群</div>
        </div>
        """, unsafe_allow_html=True)



with st.container():
    # 这里放一个跟侧边栏同步的选择框 (为了手机方便点选)
    # 但 Streamlit 同一个页面不能有两个 key 一样的组件
    # 所以我们只显示当前选中的状态，或者放一些核心指标
    pass

# 辅助函数：生成卡片 HTML
def card(label, value, delta, delta_color="pos"):
    return f"""
    <div class="metric-card">
        <div class="metric-label">{label}</div>
        <div class="metric-value">{value}</div>
        <div class="metric-delta delta-{delta_color}">{delta}</div>
    </div>
    """



# --- 4. 主界面逻辑 (保持不变) ---
# (以下代码无需修改，继续使用之前的逻辑)

# 数据加载
with st.spinner(f"正在扫描持仓数据..."):
    df_scores = de.calculate_broker_rankings(symbol=current_code)

if df_scores.empty:
    st.error(f"暂无 {current_name} 数据。")
    st.stop()

df_scores['broker'] = df_scores['broker'].str.replace('（代客）', '', regex=False).str.replace('(代客)', '',
                                                                                             regex=False).str.strip()
latest_date = df_scores['trade_date'].max()
rank_df = df_scores.groupby('broker').agg({'score': 'sum', 'trade_date': 'count', 'net_vol': 'mean'}).reset_index()
rank_df.columns = ['期货商', '总积分', '活跃天数', '平均持仓']
rank_df = rank_df[rank_df['活跃天数'] > 0]

all_dates = df_scores['trade_date'].drop_duplicates().sort_values(ascending=False)
target_dates = all_dates.head(10)
df_recent_10 = df_scores[df_scores['trade_date'].isin(target_dates)]
total_score_10d = df_recent_10['score'].sum()



# 6.1 顶部标题区
st.markdown(f"""
<div class="desktop-header">
    <div class="main-title">📊{current_name} ({current_code})</div>
    <div style="color:#666;">分析日期: {latest_date} | 监控机构: {len(rank_df)} 家</div>
</div>
""", unsafe_allow_html=True)




# 6.2 小秘书 + AI (在手机上会自动堆叠)
c_sec, c_ai = st.columns([2, 1])

with c_sec:

    st.markdown(f"""
        <div class="metric-card2">
            <img src="https://img.520wangming.com/uploads/allimg/2023052011/rpgg3ucgozi.jpg" class="secretary-img">
            <div class="secretary-text">{current_name} 实时新闻</div>
            
        </div>
        """, unsafe_allow_html=True)




with c_ai:
    with st.container():

        # 使用 columns 在 AI 框内布局，手机上也会自动堆叠
        c_txt, c_btn = st.columns([1, 1])
        expert_symbol = ''.join([i for i in current_code if not i.isdigit()])
        expert_data = de.get_expert_sentiment(latest_date, symbol=expert_symbol)


        with c_btn:
            if st.button("生成報告 :material/analytics:", type="primary", use_container_width=True):

                    with st.spinner("AI 思考中..."):
                        rpt = de.generate_ai_report_agent(rank_df, expert_data, latest_date, current_name)
                        st.session_state['pro_report'] = rpt


        st.markdown('</div>', unsafe_allow_html=True)

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

with m1:
    st.markdown(card(
        "近10日潜力王", pot_name, f"{pot_days}天盈利 ({int(pot_rate)}%)"
    ), unsafe_allow_html=True)

with m2:
    st.markdown(card(
        "常胜军", top_broker['期货商'], f"{top_broker['总积分']:.1f} 分"
    ), unsafe_allow_html=True)

with m3:
    st.markdown(card(
        "常败军", bottom_broker['期货商'], f"{bottom_broker['总积分']:.1f} 分", delta_color="inverse"
    ), unsafe_allow_html=True)

with m4:
    st.markdown(card(
        "反指标风向", f"{int(abs(net_v)):,} 手", "目前做多" if net_v > 0 else "目前做空"
    ), unsafe_allow_html=True)

st.divider()

# 图表
c1, c2 = st.columns(2)
with c1:

    st.subheader("🔥 盈利排行")
    top10 = rank_df.sort_values('总积分', ascending=False).head(10)
    fig_win = px.bar(top10, x='总积分', y='期货商', orientation='h', text_auto='.1f', color='总积分',
                     color_continuous_scale='Reds',template="plotly_dark")
    fig_win.update_layout(yaxis={'categoryorder': 'total ascending'}, plot_bgcolor='white',
                          margin=dict(l=0, r=0, t=0, b=0), height=350)
    st.plotly_chart(fig_win, use_container_width=True)


with c2:
    st.subheader("💧 亏损排行")
    bot10 = rank_df.sort_values('总积分', ascending=True).head(10)
    fig_lose = px.bar(bot10, x='总积分', y='期货商', orientation='h', text_auto='.1f', color='总积分',
                      color_continuous_scale='Teal_r',template="plotly_dark")
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
        # === 美化后的图表绘制 ===
        fig_line = px.area(
            history,
            x='trade_date',
            y='累计积分',
            title=f"{selected_broker} · 得分曲线",
            markers=True,  # 显示数据点
        )

        # 1. 线条和填充样式优化
        fig_line.update_traces(
            line_color='#D32F2F',  # 线条颜色 (金融红)
            line_width=3,  # 线条变粗
            line_shape='spline',  # 【关键】线条变平滑圆润
            marker_size=6,  # 数据点大小
            marker_color='white',  # 数据点填充色
            marker_line_color='#D32F2F',  # 数据点边框色
            marker_line_width=2,
            fill='tozeroy',  # 填充到 X 轴
            # 【关键】填充颜色渐变 (从深红到透明)
            fillcolor='rgba(211, 47, 47, 0.2)'  # 使用半透明红色作为填充基调
        )

        # 2. 布局和坐标轴优化 (极简风格)
        fig_line.update_layout(
            plot_bgcolor='white',  # 绘图区背景白
            paper_bgcolor='white',  # 整个画布背景白
            xaxis=dict(
                showgrid=False,  # 去掉 X 轴网格线
                linecolor='#eee',  # X 轴线颜色
                tickformat='%m-%d',  # 日期格式简化为 月-日
            ),
            yaxis=dict(
                showgrid=True,  # 保留 Y 轴网格线 (辅助看数据)
                gridcolor='#f9f9f9',  # Y 轴网格线颜色变淡
                gridwidth=1,
                zeroline=True,  # 显示 0 刻度线
                zerolinecolor='#eee',
            ),
            hovermode='x unified',  # 鼠标悬停时显示一条垂直线对比
            margin=dict(l=10, r=10, t=40, b=10),  # 调整边距
            height=350
        )
        st.plotly_chart(fig_line, use_container_width=True)

        cols = ['trade_date', 'net_vol', 'pct_chg', 'score']
        for c in ['oi', 'weight', 'type']:
            if c in history.columns: cols.append(c)
        display_df = history[cols].sort_values('trade_date', ascending=False).copy()
        if 'pct_chg' in display_df.columns: display_df['pct_chg'] = display_df['pct_chg']
        st.dataframe(display_df, column_config={
            "trade_date": "日期", "net_vol": st.column_config.NumberColumn("净持仓", format="%d"),
            "pct_chg": st.column_config.NumberColumn("涨跌", format="%.2f%%"),
            "score": st.column_config.ProgressColumn("得分", min_value=-5, max_value=5, format="%.2f")
        }, use_container_width=True, hide_index=True, height=200)




