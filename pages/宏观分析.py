import streamlit as st
import plotly.graph_objects as go
from macro_data import get_china_us_spread, get_gold_silver_ratio, get_cpi_ppi_data
import pandas as pd
from fed_data import get_fed_probabilities
import plotly.express as px
from macro_data import get_dashboard_metrics
st.set_page_config(page_title="宏观全景", layout="wide")

st.title("🌏 宏观与大类资产全景")

# --- 第一部分：关键指标仪表盘 ---
# 获取真实数据
metrics = get_dashboard_metrics()

# 定义默认值 (防止数据库为空时报错)
default_val = {'value': '-', 'delta': '0'}

# 布局
col1, col2, col3, col4 = st.columns(4)

# 1. 利差
m_spread = metrics.get('spread', default_val)
col1.metric("中美国债利差", m_spread['value'], m_spread['delta'], delta_color="inverse")
# 注：利差通常是负数，inverse 表示"跌得越多越红(危险)"，或者你可以按自己喜好改

# 2. 金银比
m_gs = metrics.get('gs_ratio', default_val)
col2.metric("金银比", m_gs['value'], m_gs['delta'], delta_color="inverse")
# 金银比上升通常代表避险/衰退，所以用 inverse (红色示警)

# 3. PPI
m_ppi = metrics.get('ppi', default_val)
col3.metric("PPI 同比", m_ppi['value'], m_ppi['delta'])

# 4. 美元指数 (DXY)
# 获取数据 (如果没有抓到，给个默认空值)
m_dxy = metrics.get('dxy', {'value': '-', 'delta': '0'})

col4.metric(
    "美元指数",
    m_dxy['value'],
    #m_dxy['delta'],
    #delta_color="inverse"
)

st.markdown("---")

# --- 第二部分：风险偏好 (金银比) ---
df_gs = get_gold_silver_ratio()


if not df_gs.empty and 'date' in df_gs.columns:
    df_gs['date'] = pd.to_datetime(df_gs['date'].astype(str))
    fig = go.Figure()
    # 金银比曲线
    fig.add_trace(go.Scatter(x=df_gs['date'], y=df_gs['ratio'], name='金银比', line=dict(color='#FFD700', width=3)))
    # 可以在这里叠加标普500或者沪深300走势做对比
    # 优化一下布局，让日期显示更自然
    fig.update_layout(
        title="【金银比】数值越高，代表黄金较强，资金避险属性高，数值越低，代表白银较强，资金投机属性高",
        hovermode="x unified",
        height=400,
        xaxis_tickformat='%Y-%m-%d'  # 强制显示为 年-月-日
    )
    st.plotly_chart(fig, use_container_width=True)

# --- 第二部分：流动性 (中美利差) ---
col_a, col_b = st.columns([3, 1])

with col_a:
    df_spread = get_china_us_spread()
    if not df_spread.empty:
        fig2 = go.Figure()

        # 1. 画中国国债 (红色)
        fig2.add_trace(go.Scatter(
            x=df_spread['date'], y=df_spread['cn_10y'],
            name='中国10Y', line=dict(color='#d32f2f', width=2)
        ))

        # 2. 画美国国债 (蓝色)
        fig2.add_trace(go.Scatter(
            x=df_spread['date'], y=df_spread['us_10y'],
            name='美国10Y', line=dict(color='#1976d2', width=2)
        ))

        # 3. 画利差 (阴影区域，使用右轴)
        fig2.add_trace(go.Scatter(
            x=df_spread['date'], y=df_spread['spread'],
            name='利差 (BP)',
            yaxis='y2',
            fill='tozeroy',  # 填充阴影
            line=dict(color='gray', width=0),
            opacity=0.2  # 透明度
        ))

        # 双轴布局设置
        fig2.update_layout(
            title="中美10年期国债收益率 & 利差",
            yaxis=dict(title="收益率 (%)"),
            yaxis2=dict(
                title="利差 (BP)",
                overlaying='y',
                side='right',
                showgrid=False  # 右轴不显示网格，防止太乱
            ),
            hovermode="x unified",
            height=450,
            legend=dict(orientation="h", y=1.1)  # 图例放上面
        )
        st.plotly_chart(fig2, use_container_width=True)

    else:
        st.warning("暂无数据，请运行 update_macro_data.py 更新数据库。")

with col_b:
    # 显示最新数值
    if not df_spread.empty:
        last = df_spread.iloc[-1]
        st.metric("最新利差", f"{last['spread']:.0f} BP",
                  f"{last['spread'] - df_spread.iloc[-2]['spread']:.0f} BP")
        st.metric("中国 10Y", f"{last['cn_10y']:.2f}%")
        st.metric("美国 10Y", f"{last['us_10y']:.2f}%")

# --- 第四部分：通胀 (CPI/PPI) ---

df_cpi = get_cpi_ppi_data()

if not df_cpi.empty:
    # 2. 布局：左图右文
    col_chart, col_info = st.columns([3, 1])

    with col_chart:
        fig3 = go.Figure()

        # 1. 剪刀差 (面积图 - 放在最底层作为背景)
        fig3.add_trace(go.Scatter(
            x=df_cpi['date'], y=df_cpi['scissor'],
            name='剪刀差 (PPI-CPI)',
            fill='tozeroy',  # 填充颜色
            line=dict(color='gray', width=0),  # 不显示边框线
            opacity=0.2  # 透明度低一点，不要抢眼
        ))

        # 2. PPI (蓝色实线)
        fig3.add_trace(go.Scatter(
            x=df_cpi['date'], y=df_cpi['ppi_yoy'],
            name='PPI (工业)',
            line=dict(color='#1976d2', width=2)
        ))

        # 3. CPI (红色实线)
        fig3.add_trace(go.Scatter(
            x=df_cpi['date'], y=df_cpi['cpi_yoy'],
            name='CPI (消费)',
            line=dict(color='#d32f2f', width=2)
        ))

        # 4. 零轴参考线
        fig3.add_hline(y=0, line_dash="dash", line_color="black", opacity=0.5)

        # ➖ 添加一条 0 轴参考线 (判断通缩)
        fig3.add_hline(y=0, line_dash="dash", line_color="gray", opacity=0.5)

        fig3.update_layout(
            title="中国 CPI & PPI 同比走势",
            yaxis=dict(title="同比 (%)"),
            hovermode="x unified",
            height=400,
            legend=dict(orientation="h", y=1.1),  # 图例横排放在顶部
            margin=dict(l=20, r=20, t=80, b=20)
        )
        st.plotly_chart(fig3, use_container_width=True)

    with col_info:
        # 获取最新一期数据
        last = df_cpi.iloc[-1]
        prev = df_cpi.iloc[-2]
        date_str = last['date'].strftime('%Y-%m')

        st.write(f"#### 📅 最新数据 ({date_str})")

        # 使用 metric 显示涨跌
        st.metric("CPI (消费)", f"{last['cpi_yoy']}%", f"{last['cpi_yoy'] - prev['cpi_yoy']:.1f}%")
        st.metric("PPI (工业)", f"{last['ppi_yoy']}%", f"{last['ppi_yoy'] - prev['ppi_yoy']:.1f}%")

        # 剪刀差分析
        scissor = last['scissor']
        st.metric("剪刀差 (PPI-CPI)", f"{scissor:.1f}%", delta_color="off")

        if scissor > 0:
            st.warning("🏭 **PPI强于CPI**\n,上游企业占定价优势，工业品原料偏多。")
        else:
            st.success("🛒 **CPI强于PPI**\n,下游企业占定价优势，工业品原料偏空。")

else:
    st.warning("暂无数据，请运行 update_cpi_ppi.py")

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