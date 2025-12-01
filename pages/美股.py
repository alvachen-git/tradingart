import streamlit as st
import pandas as pd
from sqlalchemy import create_engine
import plotly.graph_objects as go
import sys
import os

# --- 【修改点 1】设置页面为宽屏模式 ---
# 注意：这行代码必须放在所有 st 命令之前！
st.set_page_config(layout="wide", page_title="美股K线")

current_dir = os.path.dirname(os.path.abspath(__file__))
root_dir = os.path.dirname(current_dir)
sys.path.append(root_dir)

# 加载 CSS (注意路径)
css_path = os.path.join(root_dir, 'style.css')
with open(css_path, encoding='utf-8') as f:
    st.markdown(f'<style>{f.read()}</style>', unsafe_allow_html=True)



# 数据库连接
DB_USER = 'root'
DB_PASSWORD = 'alva13557941'
DB_HOST = '39.102.215.198'
DB_PORT = '3306'
DB_NAME = 'finance_data'

db_url = f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(db_url)

def get_data_from_db(symbol):
    """直接从本地数据库读取，按日期升序排列方便画图"""
    # 注意：这里改成 ASC (升序)，因为画 K 线图通常是从左到右时间递增
    query = f"SELECT * FROM stock_prices WHERE symbol = '{symbol}' ORDER BY date ASC"
    df = pd.read_sql(query, engine)
    # 确保 date 列是时间格式
    df['date'] = pd.to_datetime(df['date'])
    return df


# --- 页面内容 ---
st.title("美股K线图")

# 这里可以换成你数据库里实际有的股票
available_symbols = ['TSLA','NVDA', 'GOOG','AAPL', 'MSFT', 'AVGO', 'AMD', 'META', 'AMZN', 'TSM', 'INTC']
symbol = st.selectbox("请选择股票", available_symbols)

# 1. 读取数据
df = get_data_from_db(symbol)

if not df.empty:

    # --- 1. 計算默認顯示的時間範圍 (最近半年) ---
    # 獲取數據中最新的一天
    latest_date = df['date'].max()
    # 往前推 6 個月 (DateOffset 是 pandas 處理日期的神器)
    start_date = latest_date - pd.DateOffset(months=6)
    # --- 📊 开始画专业的 K 线图 ---

    # 创建一个 Plotly 图形对象
    fig = go.Figure()

    # --- 【修改点 2】自定义颜色 (红涨绿跌) ---
    # increasing: 上涨的设置
    # decreasing: 下跌的设置
    fig.add_trace(go.Candlestick(
        x=df['date'],
        open=df['open'],
        high=df['high'],
        low=df['low'],
        close=df['close'],
        name='K线',

        # 🔴 上涨：设为红色 (#d32f2f 是比较好看的深红)
        increasing_line_color='#d32f2f',
        increasing_fillcolor='#d32f2f',

        # 🟢 下跌：设为绿色 (#2e7d32 是比较好看的深绿)
        decreasing_line_color='#2e7d32',
        decreasing_fillcolor='#2e7d32'
    ))

    # Config 设置
    my_config = {
        'scrollZoom': True,
        'displayModeBar': True,

        # 🟢 关键设置 1：开启全局编辑模式
        # 这会让 Plotly 监听键盘事件，选中图形后按 Delete 键就能删除了！
        'editable': True,

        # 🟢 确保 eraseshape 在列表里
        'modeBarButtonsToAdd': [
            'drawline',
            'drawrect',
            'drawcircle',
            'eraseshape'  # <--- 必须有这个
        ]
    }

    # --- 图表布局美化 ---
    fig.update_layout(
        title=f'📈 {symbol} - 日 K 线图',
        yaxis_title='价格 (USD)',
        xaxis_title='日期',
        xaxis_rangeslider_visible=False,  # 隐藏底部滑块
        hovermode='x unified',
        height=650,  # 高度可以稍微设大一点，宽屏下更好看
        dragmode='pan',
        # 稍微调整一下边距，让图表撑满
        margin=dict(l=20, r=20, t=60, b=20),
        # 2. 設置畫出來的線條樣式
        newshape=dict(
            line_color='blue',  # 線條顏色
            line_width=2,  # 線寬
            opacity=0.7  # 透明度
        ),
        # 🟢 【關鍵修改】設置 X 軸的初始顯示範圍
        # 這樣打開時只顯示半年，K線會變得很清楚，但用戶依然可以往左拖動看歷史
        xaxis_range=[start_date, latest_date]

    )

    # 解決空白間隙問題 (可選优化)
    # 如果你的圖表在週末會有斷裂的空白，加上這行可以隱藏非交易日
    fig.update_xaxes(
        rangebreaks=[
            dict(bounds=["sat", "mon"]),  # 隱藏週六到週一之間的空白
        ]
    )
    # 在 Streamlit 中展示交互式图表
    # use_container_width=True 让图表自动充满宽度
    st.plotly_chart(fig, width='stretch', config=my_config)

    # --- 展示原始数据表格 (可选，放在折叠框里不占地) ---
    with st.expander("查看详细历史数据表格"):
        # 把日期设为索引，显示更整齐，按时间降序看最近的数据
        st.dataframe(df.sort_values(by='date', ascending=False).set_index('date'))

else:
    st.warning(f"⚠️ 数据库里还没有 【{symbol}】 的数据。")
    st.info("💡 请先运行数据更新脚本 `update_stock_tiingo.py` 来下载数据。")

# 假设 symbol 是用户当前在 selectbox 里选中的股票，例如 'AAPL'
# symbol = st.selectbox("请选择股票", ['AAPL', 'TSLA', 'NVDA', 'AMD', 'MSFT'])

st.divider()  # 画一条分割线

# --- 🎯 核心修改：建立 代码 -> 中文搜索词 的映射字典 ---
# 必须和你 update_news_google.py 里用的搜索词一致
symbol_map = {
    'AAPL': '苹果公司',
    'TSLA': '特斯拉',
    'GOOG': '谷歌',
    'NVDA': '英伟达',
    'AVGO': '博通',
    'AMD': 'AMD',  # 如果当时搜的是 AMD
    'MSFT': '微软',
    'AMZN': '亚马逊',
    'META': 'META'
}

# 获取对应的中文搜索词
target_keyword = symbol_map.get(symbol)

st.subheader(f"📰 {symbol} 相关新闻")

if target_keyword:
    try:
        # --- 🔍 核心修改：SQL 语句增加 WHERE 筛选 ---
        # 只从数据库里拿 tickers 字段等于 target_keyword 的新闻
        query = f"SELECT * FROM stock_news WHERE tickers = '{target_keyword}' ORDER BY publishedDate DESC LIMIT 10"
        df_news = pd.read_sql(query, engine)

        if not df_news.empty:
            for index, row in df_news.iterrows():
                # 显示新闻
                st.markdown(f"### [{row['title']}]({row['url']})")

                # 格式化一下时间，把秒去掉，看起来更干净
                pub_time = pd.to_datetime(row['publishedDate']).strftime('%Y-%m-%d %H:%M')
                st.caption(f"🗓️ {pub_time} | 📢 {row['source']}")

                if row['description']:
                    st.write(row['description'])

                st.markdown("---")  # 细分割线
        else:
            st.info(f"暂无关于 {symbol} ({target_keyword}) 的最新新闻。")

    except Exception as e:
        st.error(f"读取新闻出错: {e}")
else:
    st.warning(f"未配置 {symbol} 的新闻映射，请检查代码。")

