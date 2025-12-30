import streamlit as st
import plotly.graph_objects as go
import pandas as pd
from lightweight_charts.widgets import StreamlitChart
from realtime_tools import fetch_minute_trend
from streamlit_autorefresh import st_autorefresh
import sys
import os
import re
import data_engine as de
from sqlalchemy import text
import datetime as dt
# 1. 基础配置
st.set_page_config(
    page_title="爱波塔-商品期权技术分析",
    page_icon="favicon.ico",
    layout="wide",
    initial_sidebar_state="expanded"
)

with open('style.css', encoding='utf-8') as f:
    st.markdown(f'<style>{f.read()}</style>', unsafe_allow_html=True)

# 🔥【手机端专属补丁】修复文字看不清的问题
st.markdown("""
<style>
    @media (max-width: 768px) {
        /* ===========================
           1. 标题与文字颜色修复 (新增)
           =========================== */
        /* 强制所有标题 (h1-h4) 变成深黑色 */
        h1, h2, h3, h4, h5, h6 {
            color: #1f2937 !important; /* 深炭灰色，对比度极高 */
        }

        /* 修复普通文本 (p) 的颜色，防止正文也看不清 */
        [data-testid="stMarkdownContainer"] p {
            color: #374151 !important;
        }

        /* ===========================
           2. 指标卡片 (st.metric) 修复
           =========================== */
        [data-testid="stMetric"] {
            background-color: #ffffff !important; /* 强制白底 */
            border: 1px solid #e5e7eb !important; /* 浅灰边框 */
            border-radius: 8px !important;
            padding: 12px 16px !important;
            box-shadow: 0 1px 2px rgba(0,0,0,0.05) !important;
            margin-bottom: 8px !important;
        }

        /* 标签 (如 "当前 IV") */
        [data-testid="stMetricLabel"] {
            color: #6b7280 !important; /* 灰色 */
            font-size: 14px !important;
        }

        /* 数值 (如 "15.90%") */
        [data-testid="stMetricValue"] {
            color: #111827 !important; /* 纯黑 */
        }

        /* ===========================
           3. 其他组件适配
           =========================== */
        /* 状态提示框 (st.info/warning) */
        [data-testid="stAlert"] {
            background-color: #f3f4f6 !important;
            color: #1f2937 !important;
            border: 1px solid #e5e7eb !important;
        }
        [data-testid="stAlert"] p {
            color: #1f2937 !important;
        }

        /* 下拉框文字 */
        div[data-baseweb="select"] span {
            color: #1f2937 !important;
        }

        /* 手机端顶部容器文字 */
        .mobile-top-container {
            color: #1f2937 !important;
        }
    }
</style>
""", unsafe_allow_html=True)
st.markdown("<style>.stSelectbox {margin-bottom: 20px;}</style>", unsafe_allow_html=True)


# 2. 侧边栏逻辑
with st.sidebar:
    st.header("1. 选择品种")
    # 映射表：前端显示中文，后端查询用代码
    COMMODITY_MAP = {
        "IH": "上证50","IF": "沪深300","IM": "中证1000",
        "au": "黄金","ag": "白银","cu": "铜","al": "铝","zn": "锌","ni": "镍","sn": "锡",
        "lc": "碳酸锂", "si": "工业硅", "ps": "多晶硅","pt": "铂金","pd": "钯金",
        "rb": "螺纹钢", "i": "铁矿石", "hc": "热卷","jm": "焦煤","ad": "铝合金","fg": "玻璃","sa": "纯碱","ao": "氧化铝","sh": "烧碱","sp": "纸浆","lg": "原木",
        "M": "豆粕", "RM": "菜粕","y": "豆油","oi": "菜油","p": "棕榈油","pk": "花生",
        "sc": "原油","ta": "PTA","px": "对二甲苯",  "ma": "甲醇", "v": "PVC", "eb": "苯乙烯","eg": "乙二醇","pp": "聚丙烯","l": "塑料","bu": "沥青","fu": "燃料油","br": "BR橡胶",
        "ru": "橡胶", "c": "玉米", "jd": "鸡蛋", "CF": "棉花", "SR": "白糖", "ap": "苹果", "lh": "生猪"
    }
    variety = st.selectbox("品种", list(COMMODITY_MAP.keys()), format_func=lambda x: f"{x} ({COMMODITY_MAP[x]})")

    st.header("2. 选择合约")


    # 获取合约列表函数 (已修复 % 报错问题)
    @st.cache_data(ttl=3600)
    def get_contracts(v):
        if de.engine is None: return []
        try:
            # 使用参数化查询，彻底解决 % 报错问题
            sql = text("""
                       SELECT DISTINCT ts_code
                       FROM commodity_iv_history
                       WHERE ts_code LIKE :p1
                          OR ts_code LIKE :p2
                          OR ts_code LIKE :p3
                       ORDER BY ts_code DESC
                       """)

            with de.engine.connect() as conn:
                result = conn.execute(sql, {
                    "p1": f"{v}%",
                    "p2": f"{v.upper()}%",
                    "p3": f"{v.lower()}%"
                }).fetchall()

            raw_codes = [row[0] for row in result]
            valid_subs = []

            # 获取当前年月 (YYMM)，用于过滤过期合约
            now = dt.datetime.now()
            current_yymm = int(now.strftime('%y%m'))

            for code in raw_codes:
                # 正则提取：字母部分 + 数字部分
                match = re.match(r"([a-zA-Z]+)(\d+)", code)
                if not match: continue

                prefix = match.group(1)
                num_part = match.group(2)

                # --- 修复 1: 严格品种匹配 ---
                # 如果选的是 C (玉米)，必须严格等于 C，不能匹配到 CF (棉花)
                if prefix.upper() != v.upper():
                    continue

                # --- 修复 2: 过滤过期合约 ---
                # 处理年份：郑商所 3位 (501 -> 2501)，其他 4位 (2501)
                if len(num_part) == 3:
                    # 假设是 2020 年代，补全为 2501 这种格式
                    compare_val = int('2' + num_part)
                elif len(num_part) == 4:
                    compare_val = int(num_part)
                else:
                    continue

                    # 过滤逻辑：只显示 未过期 或 最近1个月内过期 的合约
                # 比如现在是 2512，那么 2511 还会显示，2510 就不显示了
                if compare_val >= (current_yymm - 1):
                    valid_subs.append(code)

            valid_subs.sort(reverse=True)

            # 把 "主力连续" 放在第一个
            options = [f"{v.upper()} (主力连续)"] + valid_subs
            return options

        except Exception as e:
            st.error(f"合约加载失败: {e}")
            return []

    options = get_contracts(variety)

    if not options:
        st.warning(f"未找到 {variety} 的相关合约数据")
        selected_opt = None
    else:
        selected_opt = st.selectbox("合约代码", options)

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

if selected_opt and "主力连续" in selected_opt:
    target_contract = variety.upper()
    is_continuous = True
else:
    target_contract = selected_opt
    is_continuous = False

# 3. 数据获取函数
@st.cache_data(ttl=300)
def get_chart_data(code):
    if not code: return None, None
    try:
        # A. 获取 IV (直接查 commodity_iv_history)
        sql_iv = text(
            "SELECT trade_date, iv, hv, used_contract FROM commodity_iv_history WHERE ts_code=:c ORDER BY trade_date")
        df_iv = pd.read_sql(sql_iv, de.engine, params={"c": code})

        # B. 获取 K线 (期货价格)
        sql_k = text(
            "SELECT trade_date, open_price as open, high_price as high, low_price as low, close_price as close FROM futures_price WHERE ts_code=:c ORDER BY trade_date")
        df_k = pd.read_sql(sql_k, de.engine, params={"c": code})

        # 容错：如果查 IF (主连) 没查到价格，尝试查 IF0 (常见的连续代码)
        if df_k.empty and is_continuous:
            alternatives = [f"{code}0", f"{code}888", f"{code.lower()}0"]
            for alt in alternatives:
                df_k = pd.read_sql(sql_k, de.engine, params={"c": alt})
                if not df_k.empty: break

        return df_k, df_iv
    except Exception as e:
        return None, None


# ==============================================================================
# 🔥【核心修改区】定义局部刷新函数 (放在主逻辑 if target_contract 之前)
# ==============================================================================
@st.fragment(run_every=300)  # 👈 关键：每60秒只重新运行这个函数内部，不刷新整个网页
def render_realtime_chart(symbol):
    """
    这是一个独立的 UI 片段，负责绘制实时分时图。
    它会自动每 60 秒刷新一次，或者点击按钮手动刷新。
    """
    # 布局：标题 + 刷新按钮
    col_title, col_btn = st.columns([8, 2])
    with col_title:
        st.subheader(f"当日分时走势")
    with col_btn:
        if st.button("🔄 刷新", key=f"btn_refresh_{symbol}", use_container_width=True):
            st.rerun()

    # 1. 获取数据 (因为 realtime_tools 里加了 cache，这里很快)
    df_trend = fetch_minute_trend(symbol)

    if not df_trend.empty:
        # --- 计算动态 Y 轴范围 ---
        p_min = df_trend['close'].min()
        p_max = df_trend['close'].max()
        p_range = p_max - p_min
        if p_range == 0: p_range = p_max * 0.01
        y_lower = p_min - (p_range * 0.2)
        y_upper = p_max + (p_range * 0.2)

        # --- 美化 X 轴标签 ---
        df_trend['time_display'] = df_trend['date'].apply(
            lambda x: x.split(' ')[-1][:5] if ' ' in str(x) else str(x))

        # 2. 绘制分时线
        fig_trend = go.Figure()
        fig_trend.add_trace(go.Scatter(
            x=df_trend['date'],
            y=df_trend['close'],
            mode='lines',
            name='最新价',
            line=dict(color='#2962FF', width=2),
            hovertemplate='%{y:.2f}<extra></extra>'
        ))

        # 3. 计算涨跌信息
        last_price = df_trend.iloc[-1]['close']
        last_time = df_trend.iloc[-1]['date'].split(' ')[-1]
        open_price = df_trend.iloc[0]['close']
        chg = last_price - open_price
        chg_pct = (chg / open_price) * 100
        color_code = "#ef232a" if chg >= 0 else "#14b143"
        sign = "+" if chg >= 0 else ""

        # 4. 图表布局
        fig_trend.update_layout(
            title=dict(
                text=f"<b>{last_price}</b> <span style='color:{color_code};'>({sign}{chg:.1f} / {sign}{chg_pct:.2f}%)</span> <span style='font-size:12px;color:#999'>🕒 {last_time}</span>",
                font=dict(size=20),
                x=0, y=1
            ),
            height=320,
            margin=dict(l=10, r=10, t=40, b=10),
            xaxis=dict(
                type='category',
                tickmode='auto',
                nticks=6,
                tickangle=0,
                showgrid=False,
                linecolor='#333',
                ticktext=df_trend['time_display'].iloc[::len(df_trend) // 6].tolist(),
                tickfont=dict(size=10, color="#666")
            ),
            yaxis=dict(
                range=[y_lower, y_upper],
                showgrid=True,
                gridcolor='rgba(128,128,128,0.1)',
                side='right',
                tickfont=dict(size=10, color="#666"),
                zeroline=False
            ),
            template="plotly_white",
            paper_bgcolor='rgba(0,0,0,0)',
            plot_bgcolor='rgba(0,0,0,0)',
            hovermode="x unified"
        )
        st.plotly_chart(fig_trend, use_container_width=True)
    else:
        st.info(f"💤 暂无 {symbol} 的实时分时数据")

    st.divider()

# 4. 绘图逻辑
if target_contract:
    df_kline, df_iv = get_chart_data(target_contract)

    if df_kline is not None and not df_kline.empty:
        st.subheader(f"{target_contract} ")

        # --- 【新增功能】IV Rank 仪表盘 (仅主力连续显示) ---
        if is_continuous and df_iv is not None and not df_iv.empty:
            # 取最新数据
            curr_iv = df_iv.iloc[-1]['iv']

            # 取过去一年数据计算 Rank
            df_year = df_iv.tail(252)
            max_iv = df_year['iv'].max()
            min_iv = df_year['iv'].min()

            if max_iv > min_iv:
                iv_rank = (curr_iv - min_iv) / (max_iv - min_iv) * 100
            else:
                iv_rank = 0

            if iv_rank < 20:
                status = "🟢 偏低 (买方有利)"
            elif iv_rank < 60:
                status = "🔵 正常"
            elif iv_rank < 85:
                status = "🟠 偏高"
            else:
                status = "🔴 极高 (卖方有利)"

            c1, c2, c3, c4 = st.columns(4)
            c1.metric("当前 IV", f"{curr_iv:.2f}%")
            c2.metric("IV Rank (年)", f"{iv_rank:.1f}", help="当前IV在过去一年中的百分位水平")
            c3.metric("历史最高 / 最低", f"{max_iv:.1f}% / {min_iv:.1f}%")
            c4.info(f"📊 状态: **{status}**")
            st.divider()  # 分割线，下面接着显示历史 K 线

            # 🔥【核心修改区】在这里调用刚才定义的局部刷新函数
            # 注意：不再需要 st_autorefresh 插件，@st.fragment 会自动处理
            render_realtime_chart(target_contract)


        # --- K线数据处理 ---
        st.subheader(f"历史日线与波动率")
        chart_k = df_kline.rename(columns={'trade_date': 'time'})
        chart_k['time'] = pd.to_datetime(chart_k['time']).dt.strftime('%Y-%m-%d')
        chart_k = chart_k[['time', 'open', 'high', 'low', 'close']]

        # --- IV数据处理 ---
        chart_iv = pd.DataFrame()
        if df_iv is not None and not df_iv.empty:
            df_iv['time'] = pd.to_datetime(df_iv['trade_date']).dt.strftime('%Y-%m-%d')

            # 【修改点 1】定义线条名称变量，保证前后一致
            line_name = '隐含波动率 (IV)'

            # 【修改点 2】将列名重命名为 line_name (而不是 'value')
            chart_iv = df_iv[['time', 'iv']].rename(columns={'iv': line_name})

            # 【修改点 3】过滤时也使用这个变量名
            chart_iv = chart_iv[chart_iv[line_name] > 0]  # 过滤无效IV

        # --- 绘图 ---
        chart = StreamlitChart(height=500)
        chart.legend(visible=True)
        chart.grid(vert_enabled=False, horz_enabled=False)

        # 1. K线 (右轴)
        chart.candle_style(up_color='#ef232a', down_color='#14b143', border_up_color='#ef232a',
                           border_down_color='#14b143', wick_up_color='#ef232a', wick_down_color='#14b143')
        chart.set(chart_k)

        # 2. IV (左轴)
        if not chart_iv.empty:
            # 注意：这里的 name 参数是图例上显示的名称，跟 DataFrame 列名无关
            # DataFrame 列名必须是 'time' 和 'value'
            line = chart.create_line(name=line_name, color='#2962FF', width=2, price_scale_id='left')
            line.set(chart_iv)

        chart.load()

        # 主连特有的提示：告诉用户当前用的是哪个合约
        if is_continuous and not df_iv.empty:
            last_row = df_iv.iloc[-1]
            used = last_row.get('used_contract')
            if used:
                st.info(f"💡 当前主力合约参考: **{used}** (IV 计算基于此合约)")


    else:
        st.warning(f"暂无 {target_contract} 的 K 线数据。")
        if is_continuous:
            st.caption("提示：可能是数据库中 futures_price 表缺少主连代码（如 IF 或 IF0）。")






