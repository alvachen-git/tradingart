import streamlit as st
import plotly.graph_objects as go
import pandas as pd
from lightweight_charts.widgets import StreamlitChart
from realtime_tools import fetch_sina_kline_data
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
st.markdown("<style>.stSelectbox {margin-bottom: 20px;}</style>", unsafe_allow_html=True)


# 2. 侧边栏逻辑
with st.sidebar:
    st.header("1. 选择品种")
    # 映射表：前端显示中文，后端查询用代码
    COMMODITY_MAP = {
        "IH": "上证50","IF": "沪深300","IM": "中证1000",
        "au": "黄金","ag": "白银","cu": "铜","al": "铝","zn": "锌","ni": "镍",
        "lc": "碳酸锂", "si": "工业硅", "ps": "多晶硅",
        "rb": "螺纹钢", "i": "铁矿石", "fg": "玻璃","sa": "纯碱","ao": "氧化铝","sh": "烧碱",
        "M": "豆粕", "RM": "菜粕","y": "豆油","oi": "菜油","p": "棕榈油",
        "sc": "原油","ta": "PTA", "ma": "甲醇", "v": "PVC", "eb": "苯乙烯",
        "ru": "橡胶", "c": "玉米", "CF": "棉花", "SR": "白糖"
    }
    variety = st.selectbox("品种", list(COMMODITY_MAP.keys()), format_func=lambda x: f"{x} ({COMMODITY_MAP[x]})")

    st.header("2. 选择合约")


    # 获取合约列表函数 (已修复 % 报错问题)
    @st.cache_data(ttl=60)
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


# 4. 绘图逻辑
if target_contract:
    df_kline, df_iv = get_chart_data(target_contract)

    if df_kline is not None and not df_kline.empty:
        st.subheader(f"{target_contract} 价格与波动率图")

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

            if iv_rank < 15:
                status = "🟢 极低 (买方有利)"
            elif iv_rank < 40:
                status = "🔵 偏低"
            elif iv_rank < 70:
                status = "🟠 偏高"
            else:
                status = "🔴 极高 (卖方有利)"

            c1, c2, c3, c4 = st.columns(4)
            c1.metric("当前 IV", f"{curr_iv:.2f}%")
            c2.metric("IV Rank (年)", f"{iv_rank:.1f}%", help="当前IV在过去一年中的百分位水平")
            c3.metric("历史最高 / 最低", f"{max_iv:.1f}% / {min_iv:.1f}%")
            c4.info(f"📊 状态: **{status}**")
            st.divider()

        # --- K线数据处理 ---
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

