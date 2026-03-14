import streamlit as st
import pandas as pd
import plotly.express as px
import sys
import os
import time
import logging
from sqlalchemy import text


# 1. 页面配置
st.set_page_config(
    page_title="爱波塔-期货持仓透视",
    page_icon="favicon.ico",
    layout="wide",
    initial_sidebar_state="expanded"
)


# 🔥 添加统一的侧边栏导航
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from sidebar_navigation import show_navigation
with st.sidebar:
    show_navigation()

# --- 路径修复: 确保能导入根目录的 data_engine ---
current_dir = os.path.dirname(os.path.abspath(__file__))
root_dir = os.path.dirname(current_dir)
sys.path.append(root_dir)
import data_engine as de

PAGE_NAME = "商品持仓"
_PAGE_T0 = time.perf_counter()
_PERF_LOGGER = logging.getLogger(__name__)


def _perf_user_id() -> str:
    return str(
        st.session_state.get("username")
        or st.session_state.get("user")
        or st.session_state.get("current_user")
        or "anonymous"
    )


def _probe_cache(tag: str, signature: str) -> int:
    cache_key = f"_perf_cache_probe::{PAGE_NAME}::{tag}::{signature}"
    hit = 1 if st.session_state.get(cache_key) else 0
    st.session_state[cache_key] = True
    return hit


def _perf_page_log(
    *,
    page: str,
    render_ms: float = 0.0,
    db_ms: float = 0.0,
    api_ms: float = 0.0,
    cache_hit: int = -1,
    stage: str = "main",
) -> None:
    msg = (
        f"PERF_PAGE page={page} stage={stage} "
        f"render_ms={render_ms:.1f} db_ms={db_ms:.1f} api_ms={api_ms:.1f} cache_hit={cache_hit}"
    )
    print(msg)
    _PERF_LOGGER.info(msg)


@st.cache_data(ttl=90, show_spinner=False)
def _cached_broker_rankings(user_id: str, page: str, symbol: str, date_window: str) -> pd.DataFrame:
    return de.calculate_broker_rankings(symbol=symbol)


@st.cache_data(ttl=120, show_spinner=False)
def _cached_cross_market_ranking(
    user_id: str, page: str, symbol: str, date_window: str, days: int, top_n: int
):
    return de.get_cross_market_ranking(days=days, top_n=top_n)


@st.cache_data(ttl=120, show_spinner=False)
def _cached_expert_sentiment(
    user_id: str,
    page: str,
    symbol: str,
    date_window: str,
    latest_date,
    expert_symbol: str,
):
    return de.get_expert_sentiment(latest_date, symbol=expert_symbol)


@st.cache_data(ttl=120, show_spinner=False)
def _cached_latest_foreign_capital(user_id: str, page: str, symbol: str, date_window: str) -> pd.DataFrame:
    if de.engine is None:
        return pd.DataFrame()
    latest_df = pd.read_sql(text("SELECT MAX(trade_date) AS trade_date FROM foreign_capital_analysis"), de.engine)
    latest_date = latest_df.iloc[0, 0] if not latest_df.empty else None
    if latest_date is None:
        return pd.DataFrame()
    sql = text(
        """
        SELECT symbol, direction, brokers, total_net_vol
        FROM foreign_capital_analysis
        WHERE trade_date = :trade_date
        """
    )
    return pd.read_sql(sql, de.engine, params={"trade_date": latest_date})


@st.cache_data(ttl=120, show_spinner=False)
def _cached_latest_conflict_data(user_id: str, page: str, symbol: str, date_window: str) -> pd.DataFrame:
    if de.engine is None:
        return pd.DataFrame()
    latest_df = pd.read_sql(text("SELECT MAX(trade_date) AS trade_date FROM market_conflict_daily"), de.engine)
    latest_date = latest_df.iloc[0, 0] if not latest_df.empty else None
    if latest_date is None:
        return pd.DataFrame()
    sql = text(
        """
        SELECT symbol, action, dumb_net, smart_net
        FROM market_conflict_daily
        WHERE trade_date = :trade_date
        """
    )
    return pd.read_sql(sql, de.engine, params={"trade_date": latest_date})

# 加载 CSS (注意路径)
css_path = os.path.join(root_dir, 'style.css')
with open(css_path, encoding='utf-8') as f:
    st.markdown(f'<style>{f.read()}</style>', unsafe_allow_html=True)

# 商品持仓页：主内容区文字对比度修复（兼容手机深色模式）
st.markdown("""
<style>
[data-testid="stAppViewContainer"] .main {
    color: #1f2937 !important;
}

[data-testid="stAppViewContainer"] .main h1,
[data-testid="stAppViewContainer"] .main h2,
[data-testid="stAppViewContainer"] .main h3,
[data-testid="stAppViewContainer"] .main h4,
[data-testid="stAppViewContainer"] .main h5,
[data-testid="stAppViewContainer"] .main h6,
[data-testid="stAppViewContainer"] .main p,
[data-testid="stAppViewContainer"] .main li,
[data-testid="stAppViewContainer"] .main label,
[data-testid="stAppViewContainer"] .main [data-testid="stMarkdownContainer"],
[data-testid="stAppViewContainer"] .main [data-testid="stMetricLabel"],
[data-testid="stAppViewContainer"] .main [data-testid="stMetricValue"],
[data-testid="stAppViewContainer"] .main [data-testid="stMetricDelta"],
[data-testid="stAppViewContainer"] .main [data-testid="stCaptionContainer"] {
    color: #1f2937 !important;
}

[data-testid="stAppViewContainer"] .main [data-testid="stCaptionContainer"] {
    opacity: 0.9;
}

[data-testid="stAppViewContainer"] .main [data-baseweb="select"] label,
[data-testid="stAppViewContainer"] .main .stSelectbox label {
    color: #374151 !important;
}

@media (prefers-color-scheme: dark) {
    [data-testid="stAppViewContainer"] .main {
        background-color: #f5f7f9 !important;
    }
}
</style>
""", unsafe_allow_html=True)

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
        "ps": "多晶硅",
        "pt": "铂金",
        "pd": "钯金",
        "au": "黄金",
        "ag": "白银",
        "cu": "沪铜",
        "al": "沪铝",
        "zn": "沪锌",
        "ni": "沪镍",
        "sn": "沪锡",
        "pb": "沪铅",
        "ru": "橡胶",
        "br": "BR橡胶",
        "i": "铁矿石",
        "jm": "焦煤",
        "j": "焦炭",
        "rb": "螺纹钢",
        "hc": "热卷 ",
        "sp": "纸浆 ",
        "lg": "原木 ",
        "ao": "氧化铝 ",
        "sh": "烧碱",
        "fg": "玻璃",
        "sa": "纯碱 ",
        "m": "豆粕",
        "a": "豆一",
        "b": "豆二",
        "c": "玉米",
        "lh": "生猪",
        "jd": "鸡蛋",
        "cj": "红枣",
        "p": "棕榈油",
        "y": "豆油",
        "oi": "菜油 ",
        "l": "塑料",
        "pk": "花生 ",
        "rm": "菜粕 ",
        "ma": "甲醇",
        "ta": "PTA",
        "PX": "对二甲苯",
        "pr": "瓶片",
        "pp": "聚丙烯",
        "v": "PVC",
        "eb": "苯乙烯",
        "eg": "乙二醇",
        "ss": "不锈钢",
        "ad": "铝合金",
        "bu": "沥青",
        "fu": "燃料油",
        "ec": "集运欧线",
        "ur": "尿素 ",
        "sr": "白糖",
        "cf": "棉花",
        "ap": "苹果"

    }
    option_list = [f"{code} - {name}" for code, name in COMMODITIES.items()]
    # 这里使用 key 保持状态
    selected_option = st.selectbox("选择标的",option_list, index=0, key="sidebar_select")

    current_code = selected_option.split(' ')[0]
    current_name = COMMODITIES[current_code].split(' (')[0]


    st.markdown("---")
    # 使用 HTML 渲染自定义客服框
    st.markdown("""
        <div class="service-box">
            <div class="service-title">📞实战课程咨询</div>
            <div class="service-phone">17521591756</div>
            <img src="https://aiprota-img.oss-cn-beijing.aliyuncs.com/QQ%E6%88%AA%E5%9B%BE20240110194356.png" class="qr-img">
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
page_user_id = _perf_user_id()
rank_window = "rank_90s"
rank_sig = f"{page_user_id}|{PAGE_NAME}|{current_code}|{rank_window}"
rank_hit = _probe_cache("broker_rankings", rank_sig)
_db_t0 = time.perf_counter()
with st.spinner(f"正在扫描持仓数据..."):
    df_scores = _cached_broker_rankings(page_user_id, PAGE_NAME, current_code, rank_window)
_perf_page_log(
    page=PAGE_NAME,
    db_ms=(time.perf_counter() - _db_t0) * 1000,
    cache_hit=rank_hit,
    stage="calculate_broker_rankings",
)

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
        expert_window = "expert_120s"
        expert_sig = f"{page_user_id}|{PAGE_NAME}|{expert_symbol}|{expert_window}"
        expert_hit = _probe_cache("expert_sentiment", expert_sig)
        _api_t0 = time.perf_counter()
        expert_data = _cached_expert_sentiment(
            page_user_id,
            PAGE_NAME,
            current_code,
            expert_window,
            latest_date,
            expert_symbol,
        )
        _perf_page_log(
            page=PAGE_NAME,
            api_ms=(time.perf_counter() - _api_t0) * 1000,
            cache_hit=expert_hit,
            stage="get_expert_sentiment",
        )


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

st.subheader("🏆 全品种盈亏排行榜")
st.caption("统计范围：近200天, (部分期货商亏损是因为做套保)")

# 获取数据
cross_window = "cross_market_120s"
cross_sig = f"{page_user_id}|{PAGE_NAME}|{current_code}|{cross_window}"
cross_hit = _probe_cache("cross_market_ranking", cross_sig)
_db_t0 = time.perf_counter()
with st.spinner("正在扫描全市场数据..."):
    df_win, df_lose = _cached_cross_market_ranking(
        page_user_id, PAGE_NAME, current_code, cross_window, 150, 5
    )
_perf_page_log(
    page=PAGE_NAME,
    db_ms=(time.perf_counter() - _db_t0) * 1000,
    cache_hit=cross_hit,
    stage="get_cross_market_ranking",
)

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
st.divider()
# 深度透视
st.subheader("🔎 机构深度透视")
c_sel, c_info = st.columns([1, 3])
with c_sel:
    broker_list = rank_df.sort_values('总积分', ascending=False)['期货商'].unique()
    selected_broker = st.selectbox("👉 请选择期货商", broker_list)
    b_data = rank_df[rank_df['期货商'] == selected_broker].iloc[0]
    st.metric("累计总积分", f"{b_data['总积分']:.2f}")
    st.metric("平均持仓量", f"{int(b_data['平均持仓']):,}")

with c_info:
    if selected_broker:
        # 1. 先筛选数据并拷贝，防止警告
        history = df_scores[df_scores['broker'] == selected_broker].copy()

        # 🔥【核心修复】强制将 20260101 这种数字/字符串转为真正的日期格式
        # .astype(str) 是为了防止源数据是 int 类型导致转换失败
        history['trade_date'] = pd.to_datetime(history['trade_date'].astype(str), format='%Y%m%d')

        # 2. 转换完日期后再排序，确保时间轴正确
        history = history.sort_values('trade_date')

        # 3. 计算累计分
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

st.markdown("---")

# --- 外资动向卡片 ---
st.caption("### 🌍 外资动向 (摩根/瑞银/乾坤)")

# 读库
try:
    foreign_window = "foreign_120s"
    foreign_sig = f"{page_user_id}|{PAGE_NAME}|{current_code}|{foreign_window}"
    foreign_hit = _probe_cache("foreign_cards", foreign_sig)
    _db_t0 = time.perf_counter()
    df_foreign = _cached_latest_foreign_capital(page_user_id, PAGE_NAME, current_code, foreign_window)
    _perf_page_log(
        page=PAGE_NAME,
        db_ms=(time.perf_counter() - _db_t0) * 1000,
        cache_hit=foreign_hit,
        stage="foreign_capital_cards",
    )

    if not df_foreign.empty:
        cols = st.columns(4)
        for i, row in df_foreign.iterrows():
            with cols[i % 4]:
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

except Exception as e:
    st.error(f"读取外资数据失败: {e}")

st.markdown("---")

# --- 新增：多空巔峰對決 (Smart vs Dumb) ---
st.caption("### ⚔️ 多空巅峰对决")
st.caption("筛选逻辑：机构与散户差异最大的持仓对比")

# 1. 獲取數據 (直接讀表)
try:
    conflict_window = "conflict_120s"
    conflict_sig = f"{page_user_id}|{PAGE_NAME}|{current_code}|{conflict_window}"
    conflict_hit = _probe_cache("conflict_cards", conflict_sig)
    _db_t0 = time.perf_counter()
    df_conflict = _cached_latest_conflict_data(page_user_id, PAGE_NAME, current_code, conflict_window)
    _perf_page_log(
        page=PAGE_NAME,
        db_ms=(time.perf_counter() - _db_t0) * 1000,
        cache_hit=conflict_hit,
        stage="market_conflict_cards",
    )

    if not df_conflict.empty:
        cols = st.columns(4)
        for i, row in df_conflict.iterrows():
            with cols[i % 4]:
                direction = row['action']
                direction_text = str(direction).lower()
                bullish_tokens = ("看涨", "看漲", "做多", "bull", "long", "涨", "漲")
                bearish_tokens = ("看跌", "做空", "bear", "short", "跌")
                is_bullish = any(token in direction_text for token in bullish_tokens)
                is_bearish = any(token in direction_text for token in bearish_tokens)
                if is_bullish and not is_bearish:
                    color = "#d32f2f"  # 红色：看涨/做多
                elif is_bearish and not is_bullish:
                    color = "#2e7d32"  # 绿色：看跌/做空
                else:
                    # Fallback: action 文本异常时，按主力净持仓方向染色
                    color = "#d32f2f" if float(row.get('smart_net', 0)) >= 0 else "#2e7d32"
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

except Exception as e:
    st.error(f"讀取對決數據失敗: {e}")

_perf_page_log(
    page=PAGE_NAME,
    render_ms=(time.perf_counter() - _PAGE_T0) * 1000,
    cache_hit=-1,
    stage="page_done",
)

