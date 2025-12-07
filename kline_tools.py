import pandas as pd
import os
import streamlit as st
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from langchain_core.tools import tool
import symbol_map

# --- 1. 独立初始化 ---
load_dotenv(override=True)

DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")

@st.cache_resource
def get_db_engine():
    if not all([DB_USER, DB_PASSWORD, DB_HOST, DB_NAME]):
        return None
    db_url = f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    return create_engine(db_url, pool_recycle=3600, pool_pre_ping=True)


engine = get_db_engine()




# --- 2. 核心工具定义 ---

@tool
def analyze_kline_pattern(query: str, ppprev_open=None):
    """
    【K线形态计算器】
    根据用户输入的商品，分析其最近的 K 线形态，判断多空方向。
    包含：单根K线形状（大阳/大阴/影线）、吞噬形态、趋势强弱、是否有转折K线。
    当用户询问“技术面怎么看”、“走势如何”、“K线形态”时，**必须**调用此工具。
    """
    if engine is None: return "数据库连接失败"
    if not query: return "请输入有效的品种名称或代码。"  # 【新增】空值拦截

    # 1. 智能解析名称
    # 【核心修复】安全拆包
    result = symbol_map.resolve_symbol(query)
    if not result or result[0] is None:
        return f"未找到与'{query}'相关的品种，请尝试输入全名。"

    symbol, asset_type = result


    # 1. 容错处理
    clean_symbol = ''.join([i for i in symbol if not i.isdigit()])
    target_code_1 = f"{clean_symbol}0"
    target_code_2 = clean_symbol

    try:
        # 2. 获取数据
        if asset_type == 'stock':
            sql = f"""
                SELECT trade_date, open_price, high_price, low_price, close_price 
                FROM stock_price
                WHERE ts_code='{symbol}' 
                ORDER BY trade_date DESC LIMIT 60
            """
            df = pd.read_sql(sql, engine)

        elif asset_type == 'future':
            sql = f"""
                SELECT trade_date, open_price, high_price, low_price, close_price 
                FROM futures_price
                WHERE ts_code='{target_code_1}' OR ts_code='{target_code_2}'
                ORDER BY trade_date DESC LIMIT 60
            """
            df = pd.read_sql(sql, engine)

        if df.empty:
            return f"未找到品种 {symbol} 的历史价格数据，无法进行技术分析。"

        # 3. 数据预处理
        df = df.sort_values('trade_date').reset_index(drop=True)
        for col in ['open_price', 'high_price', 'low_price', 'close_price']:
            df[col] = pd.to_numeric(df[col])

        # 4. 计算均线
        df['MA5'] = df['close_price'].rolling(window=5).mean()
        df['MA10'] = df['close_price'].rolling(window=10).mean()
        df['MA20'] = df['close_price'].rolling(window=20).mean()
        df['MA60'] = df['close_price'].rolling(window=60).mean()

        if len(df) < 2: return "数据不足，无法分析趋势。"

        # 提取今日和昨日数据
        curr = df.iloc[-1]
        prev = df.iloc[-2]
        pprev = df.iloc[-3]
        tprev = df.iloc[-4]
        pppprev = df.iloc[-5]
        fprev = df.iloc[-6]

        date = curr['trade_date']
        close = curr['close_price']
        open_p = curr['open_price']
        high = curr['high_price']
        low = curr['low_price']

        # 前几日数据
        prev_close = prev['close_price']
        pprev_close = pprev['close_price']
        prev_open = prev['open_price']
        pprev_open = pprev['open_price']
        tprev_close = tprev['close_price']
        pppprev_close = pppprev['close_price']
        fprev_close = fprev['close_price']
        tprev_open = tprev['open_price']
        pppprev_open = pppprev['open_price']
        fprev_open = fprev['open_price']
        prev_high = prev['high_price']
        pprev_high = pprev['high_price']
        tprev_high = tprev['high_price']
        pppprev_high = pppprev['high_price']
        fprev_high = fprev['high_price']
        prev_low = prev['low_price']
        pprev_low = pprev['low_price']
        tprev_low = tprev['low_price']
        pppprev_low = pppprev['low_price']
        fprev_low = fprev['low_price']

        # --- 5. 形态识别逻辑 (核心算法) ---

        # A. 基础计算
        body_size = abs(close - open_p)
        upper_shadow = high - max(close, open_p)
        lower_shadow = min(close, open_p) - low
        total_range = high - low if high != low else 0.01

        body_pct = body_size / total_range
        upper_pct = upper_shadow / body_size
        lower_pct = lower_shadow / body_size

        # 今日涨跌幅
        chg_pct = (close - prev_close) / prev_close

        # 昨日实体涨跌幅 (用于判断大阴/大阳)
        prev_chg_pct = (prev_close - prev_open) / prev_open
        pprev_chg_pct = (pprev_close - pprev_open) / pprev_open
        tprev_chg_pct = (tprev_close - tprev_open) / tprev_open
        pppprev_chg_pct = (pppprev_close - pppprev_open) / pppprev_open
        fprev_chg_pct = (fprev_close - fprev_open) / fprev_open

        # 计算一段时间的最高价最低价
        prev_2_days_high = df['high_price'].iloc[-3:-1].max()
        prev_2_days_low = df['low_price'].iloc[-3:-1].min()
        prev_3_days_high = df['high_price'].iloc[-4:-1].max()
        prev_3_days_low = df['low_price'].iloc[-4:-1].min()
        prev_4_days_high = df['high_price'].iloc[-5:-1].max()
        prev_4_days_low = df['low_price'].iloc[-5:-1].min()
        prev_5_days_high = df['high_price'].iloc[-6:-1].max()
        prev_5_days_low = df['low_price'].iloc[-6:-1].min()
        prev_6_days_high = df['high_price'].iloc[-7:-1].max()
        prev_6_days_low = df['low_price'].iloc[-7:-1].min()

        # B. K线形态判断列表
        patterns = []

        # --- 【新增】吞噬形态 (Engulfing Pattern) ---

        # 1. 多头吞噬 (Bullish Engulfing)
        # 条件：空头趋势(5<20) + 昨日大跌(>2%) + 今日大涨 + 包住昨日
        if (curr['MA5'] < curr['MA20']) and \
                (prev_chg_pct < -0.02) and \
                (chg_pct > 0) and \
                (open_p < prev_close) and \
                (close > prev_open):
            patterns.append("【多头吞噬】(空头趋势末端，一阳吞一阴，强力反转信号！)")

        # 2. 空头吞噬 (Bearish Engulfing)
        # 条件：多头趋势(5>20) + 昨日大涨(>2%) + 今日大跌 + 包住昨日
        if (curr['MA5'] > curr['MA20']) and \
                (prev_chg_pct > 0.02) and \
                (chg_pct < 0) and \
                (open_p > prev_close) and \
                (close < prev_open):
            patterns.append("【空头吞噬】(多头趋势末端，一阴吞一阳，强力见顶信号！)")

        # 3. 上升三法
        if (curr['MA5'] > curr['MA10']) and chg_pct > 0.01:
            if pprev_chg_pct > 0.015 and prev_close < pprev_high and close > prev_2_days_high:
                patterns.append("【上升三法】(中继再涨，多头持续上攻！)")
            elif tprev_chg_pct > 0.015 and pprev_close < tprev_high and prev_close < tprev_high and close > prev_3_days_high:
                patterns.append("【上升三法】(中继再涨，多头持续上攻！！)")
            elif pppprev_chg_pct > 0.015 and tprev_close < pppprev_high and pprev_close < pppprev_high and prev_close < pppprev_high and close > prev_4_days_high:
                patterns.append("【上升三法】(中继再涨，多头持续上攻！！)")
            elif fprev_chg_pct > 0.015 and pppprev_close < fprev_high and tprev_close < fprev_high and pprev_close < fprev_high and prev_close < fprev_high and close > prev_5_days_high:
                patterns.append("【上升三法】(中继再涨，多头持续上攻！！)")
        # 4. 下降三法
        if (curr['MA5'] < curr['MA10']) and chg_pct < -0.01:
            if pprev_chg_pct < -0.015 and prev_close > pprev_low and close < prev_2_days_low:
                patterns.append("【下降三法】(中继再跌，空头持续发力！)")
            elif tprev_chg_pct < -0.015 and pprev_close > tprev_low and prev_close > tprev_low and close < prev_3_days_low:
                patterns.append("【下降三法】(中继再跌，空头持续发力！)")
            elif pppprev_chg_pct < -0.015 and tprev_close > pppprev_low and pprev_close > pppprev_low and prev_close > pppprev_low and close < prev_4_days_low:
                patterns.append("【下降三法】(中继再跌，空头持续发力！)")
            elif fprev_chg_pct < -0.015 and pppprev_close > fprev_low and tprev_close > fprev_low and pprev_close > fprev_low and prev_close > fprev_low and close < prev_5_days_low:
                patterns.append("【下降三法】(中继再跌，空头持续发力！)")

        # --- 基础形态 ---

        # 3. 大阳/大阴
        if body_pct > 0.8 and abs(chg_pct) > 0.03:
            if close > open_p:
                patterns.append("【大阳线】(多头气势强)")
            else:
                patterns.append("【大阴线】(空头气势强)")

        # 4. 长下影
        if lower_pct > 2 and body_pct < 0.3 and body_pct > 0.05 and curr['MA5'] < curr['MA20']and close < prev_close:
            patterns.append("【锤子】(多头抵抗)")

        # 5. 长上影
        if upper_pct > 2 and body_pct < 0.3 and body_pct > 0.05 and curr['MA5'] > curr['MA20'] and close > prev_close:
            patterns.append("【倒状锤子】(卖压沉重)")

        # 6. 十字星
        if body_pct < 0.1:
            patterns.append("【十字星】(变盘前兆)")

        # C. 趋势位置判断
        trends = []
        if close > curr['MA5']:
            trends.append("站上5日线(短强)")
        else:
            trends.append("跌破5日线(短弱)")

        if close > curr['MA20']:
            if curr['MA20'] > prev['MA20']:
                trends.append("站稳20日线且向上(中多)")
            else:
                trends.append("20日线上方震荡")
        else:
            trends.append("跌破20日线(中空)")

        if curr['MA5'] > curr['MA20'] > curr['MA60']:
            trends.append("均线多头排列")
        elif curr['MA5'] < curr['MA20'] < curr['MA60']:
            trends.append("均线空头排列")

        if curr['MA20'] > curr['MA5'] > curr['MA10']:
            trends.append("震荡横盘格局")
        elif curr['MA5'] > curr['MA20'] > curr['MA10']:
            trends.append("震荡偏多格局")
        elif curr['MA20'] < curr['MA5'] < curr['MA10']:
            trends.append("震荡横盘格局")
        elif curr['MA5'] < curr['MA20'] < curr['MA10']:
            trends.append("震荡偏空格局")

        # --- 6. 输出报告 ---
        report = f"""
        📊 **{symbol} K线技术面诊断** ({date})

        1. **形态信号**：{' 🔥 '.join(patterns) if patterns else '普通震荡K线。'}
        2. **趋势结构**：{'，'.join(trends)}。
        3. **关键数据**：
           - 收盘：{close} (涨跌 {chg_pct * 100:.2f}%)
        """

        return report

    except Exception as e:
        return f"K线分析出错: {e}"


if __name__ == "__main__":
    # 工具必须用 .invoke 并在字典里传参
    print(analyze_kline_pattern.invoke({"symbol": "im"}))