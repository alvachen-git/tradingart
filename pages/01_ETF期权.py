import streamlit as st
import pandas as pd
import plotly.express as px
import sys
import os
from lightweight_charts.widgets import StreamlitChart
# 【关键修改】导入新的独立工具模块，不再依赖 data_engine
import etf_option_tool as de


# 路径修复
current_dir = os.path.dirname(os.path.abspath(__file__))
root_dir = os.path.dirname(current_dir)
sys.path.append(root_dir)

# 1. 页面配置
st.set_page_config(
    page_title="爱波塔-ETF期权分析",
    page_icon="favicon.ico",
    layout="wide",
    initial_sidebar_state="expanded"
)


# 加载 CSS

css_path = os.path.join(root_dir, 'style.css')
with open(css_path, encoding='utf-8') as f:
    st.markdown(f'<style>{f.read()}</style>', unsafe_allow_html=True)


# --- 页面逻辑 ---
st.markdown('<div class="mobile-top-container">', unsafe_allow_html=True)
c1, c2 = st.columns([1, 2])
with c1:
    st.markdown("### 💹 **ETF 期权**")
with c2:
    target = st.selectbox("选择标的", ["510300 (300ETF)", "510050 (50ETF)","510500 (500ETF)", "588000 (科创50ETF)",
                                       "159915 (创业板ETF)"], label_visibility="collapsed")
st.markdown('</div>', unsafe_allow_html=True)

# 1. 获取基础代码
etf_code = target.split(' ')[0] # 得到 "510050"

# --- 【关键修复】补全后缀 (匹配数据库格式) ---
if "." not in etf_code:
    if etf_code.startswith("15") or etf_code.startswith("16"):
        etf_code = etf_code + ".SZ" # 深市
    else:
        etf_code = etf_code + ".SH" # 沪市 (50/300/500/科创)

# 数据获取
with st.spinner(f"正在扫描 {etf_code} 全市场持仓数据..."):
    df = de.get_etf_option_analysis(etf_code, days=20)

if df is None or df.empty:
    st.error("暂无数据，可能是非交易时间或 Tushare 接口受限。")
    st.stop()

# --- 【关键修复】健壮的日期处理 ---
try:
    # 1. 先统一转为字符串，去除可能的空格
    df['date'] = df['date'].astype(str).str.strip()

    # 2. 智能解析日期 (兼容 '20251121', '2025-11-21' 等多种格式)
    df['date_obj'] = pd.to_datetime(df['date'], errors='coerce')

    # 3. 再次格式化为我们想要的字符串 (用于表格显示)
    df['date_str'] = df['date_obj'].dt.strftime('%Y-%m-%d')

    # 4. 按时间排序
    df = df.sort_values('date_obj')

    # 5. 去除解析失败的坏数据
    df = df.dropna(subset=['date_obj'])

except Exception as e:
    st.error(f"日期处理出错: {e}")
    st.stop()

# --- 1. 核心洞察卡片 ---
latest_date = df['date_str'].max()  # 使用格式化后的字符串取最大值
latest_data = df[df['date_str'] == latest_date]

# --- 核心指标卡片 ---
# (此处调用 tool.get_iv_rank_data)
iv_stats = de.get_iv_rank_data(etf_code, window=252)

try:
    call_row = latest_data[latest_data['type'].str.contains('认购')].iloc[0]
    put_row = latest_data[latest_data['type'].str.contains('认沽')].iloc[0]

    st.info(f"📅 分析日期：**{latest_date}**")
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.markdown(
            f"""<div class="metric-card"><div class="metric-label">上方压力</div><div class="metric-value" style="color:#d32f2f">{call_row['strike']:.3f}</div><div class="metric-delta">持仓 {int(call_row['oi']):,}</div></div>""",
            unsafe_allow_html=True)
    with col2:
        st.markdown(
            f"""<div class="metric-card"><div class="metric-label">下方支撑</div><div class="metric-value" style="color:#2e7d32">{put_row['strike']:.3f}</div><div class="metric-delta">持仓 {int(put_row['oi']):,}</div></div>""",
            unsafe_allow_html=True)
    with col3:
        spread = f"{put_row['strike']:.3f} ~ {call_row['strike']:.3f}"
        st.markdown(
            f"""<div class="metric-card"><div class="metric-label">博弈区间</div><div class="metric-value" style="font-size:1.2rem">{spread}</div><div class="metric-delta delta-neu">多空分界</div></div>""",
            unsafe_allow_html=True)
    with col4:
        if iv_stats:
            iv_color = "#d32f2f" if iv_stats['iv_percentile'] > 80 else "#2e7d32" if iv_stats[
                                                                                         'iv_percentile'] < 20 else "#555"
            st.markdown(
                f"""<div class="metric-card" style="border-top: 3px solid {iv_color};"><div class="metric-label">IV 等级</div><div class="metric-value" style="color:{iv_color}">{iv_stats['iv_percentile']:.0f}</div><div class="metric-delta">（1~100）</div></div>""",
                unsafe_allow_html=True)
except:
    pass
st.markdown("---")

# --- 3. 价格与波动率 (K线 + IV) ---
# 【核心调用】从新文件获取数据
df_kline, df_iv = de.get_kline_and_iv_data(etf_code, limit=500)



if not df_kline.empty and not df_iv.empty:
    st.subheader("📊 价格与波动率")

    # --- 1. 数据清洗 ---
    # K线
    k_df = df_kline[['trade_date', 'open', 'high', 'low', 'close']].copy()
    k_df.columns = ['date', 'open', 'high', 'low', 'close']
    k_df['date'] = pd.to_datetime(k_df['date']).dt.strftime('%Y-%m-%d')

    # IV
    iv_df = df_iv[['trade_date', 'iv']].copy()
    iv_df.columns = ['date', '隐含波动率']
    iv_df = iv_df[iv_df['隐含波动率'] > 0]
    iv_df['date'] = pd.to_datetime(iv_df['date']).dt.strftime('%Y-%m-%d')


    # --- 2. 创建图表 ---
    chart = StreamlitChart(height=500, width=None)

    # --- 【新增】设置背景水印 (显示中文名) ---
    if 'name' in df_kline.columns and not df_kline['name'].empty:
        # 获取第一行的名字 (例如 "华夏上证50ETF")
        etf_name_cn = df_kline['name'].iloc[0]
        # 设置水印文字
        #chart.watermark(
            #f"{etf_name_cn} ({etf_code})",
        # color='rgba(0, 0, 0, 0.1)',  # 淡淡的灰色
            #font_size=24,  # 大字體
        #)
    else:
        # 兜底：如果数据库没名字，就用 target 变量
        chart.watermark(target, color='rgba(0, 0, 0, 0.1)', font_size=48)

    # 【关键修复 1】去除背景网格线
    chart.grid(vert_enabled=False, horz_enabled=False)

    # 【修正 1】使用 layout 方法设置背景和文字颜色
    chart.layout(background_color='white', text_color='#333333')

    # 【修正 2】设置图例
    chart.legend(visible=True, font_size=14, color='#333333')

    # --- 3. 设置 K 线 (默认在右轴) ---
    # 注意：candle_style 只负责颜色，不要传 price_scale_id
    chart.candle_style(
        up_color='#ef232a', down_color='#14b143',
        border_up_color='#ef232a', border_down_color='#14b143',
        wick_up_color='#ef232a', wick_down_color='#14b143'
    )
    chart.set(k_df)

    # --- 4. 设置 IV 曲线 (绑定到左轴) ---
    # 【关键】在这里指定 price_scale_id='left'，这会自动开启左轴
    line = chart.create_line(
        name='隐含波动率',
        color='#2962FF',
        width=2,
        price_scale_id='left'
    )
    line.set(iv_df)

    # --- 5. 渲染 ---
    chart.load()

else:
    st.info("暂无波动率数据。")




st.markdown("---")

# --- 2. 趋势图表：防线移动 ---
st.subheader("📊 主力持仓防线移动 (近20日)")
st.caption(
    "观察期权最大持仓合约的移动。若红色线上移，代表压力上移（卖方看多）；若绿色线下移，代表支撑下移（卖方看空）。")

# 【关键修改】使用 date_obj (时间对象) 作为 X 轴，而不是字符串
# 这样 Plotly 会自动处理时间刻度，不会出现 2011 这种解析错误
fig = px.line(
    df,
    x='date_obj',
    y='strike',
    color='type',
    markers=True,
    symbol='type',
    title=f"{target} 压力与支撑位变动",
    color_discrete_map={"认购 (压力)": "#d32f2f", "认沽 (支撑)": "#2e7d32"}
)

# 优化图表布局
fig.update_layout(
    plot_bgcolor='white',
    xaxis_title="日期",
    yaxis_title="行权价",
    hovermode="x unified",
    height=400,
    xaxis=dict(
        showgrid=False,
        tickformat="%Y-%m-%d"  # 强制显示为 YYYY-MM-DD 格式
    ),
    yaxis=dict(showgrid=True, gridcolor='#eee')
)
st.plotly_chart(fig, use_container_width=True)

# --- 3. 详细数据 ---
with st.expander("查看详细数据表"):
    # 增加一列：持仓金额估算 (价格 * 持仓 * 10000)
    df['amt_est'] = df['price'] * df['oi'] * 10000 / 100000000  # 亿

    st.dataframe(
        df[['date_str', 'type', 'strike', 'oi', 'price', 'code']],
        column_config={
            "date_str": "日期",
            "type": "类型",
            "strike": st.column_config.NumberColumn("行权价", format="%.3f"),
            "oi": st.column_config.NumberColumn("持仓量(张)", format="%d"),
            "price": st.column_config.NumberColumn("期权价", format="%.4f"),
            "code": "合约代码"
        },
        use_container_width=True
    )

