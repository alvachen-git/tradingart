import pandas as pd
import os
import re
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
    return create_engine(db_url, pool_recycle=7200, pool_pre_ping=True)


engine = get_db_engine()


# --- 2. 核心工具定义 ---

@tool
def analyze_kline_pattern(query: str, trade_date: str = None):
    """
    【K线形态计算器】
    分析指定商品的K线形态、趋势结构及多空信号。
    适用于回答技术面、走势、形态等问题。

    参数:
    - query: 品种名称，如 "白银", "50ETF"
    - trade_date: (可选) 指定分析哪一天的K线，格式 YYYYMMDD (如 '20251210')。如果不填则默认分析最新一天。
    """
    if engine is None: return "数据库连接失败"
    # 🔥 [新增] 强力清洗：去除空格、单引号、双引号，防止 AI 传参格式错误
    if query:
        query = str(query).strip().replace("'", "").replace('"', "")
    if not query: return "请输入有效的品种名称或代码。"  # 【新增】空值拦截

    # 1. 智能解析名称
    # 【核心修复】安全拆包
    result = symbol_map.resolve_symbol(query)
    if not result or result[0] is None:
        return f"未找到与'{query}'相关的品种，请尝试输入全名。"

    symbol, asset_type = result

    # 【修改点 2】构建日期过滤条件
    # 逻辑：如果指定了 12月10日，我们要查 <= 12月10日 的最近60条记录
    # 这样第1条就是12月10日，后面是9日、8日... 用于计算均线
    date_condition = ""
    if trade_date:
        # 清洗日期格式 2025-12-10 -> 20251210
        clean_date = trade_date.replace("-", "").replace("/", "")
        date_condition = f"AND trade_date <= '{clean_date}'"

    try:
        # 2. 获取数据
        if asset_type == 'stock':
            sql = f"""
                SELECT trade_date, open_price, high_price, low_price, close_price 
                FROM stock_price
                WHERE ts_code='{symbol}' 
                {date_condition} 
                ORDER BY trade_date DESC LIMIT 60
            """
            df = pd.read_sql(sql, engine)

        elif asset_type == 'future':
            # 🔥【核心修复】判断是否指定了具体合约月份
            has_month = bool(re.search(r'\d{2,4}', symbol))  # 检查是否有3-4位数字

            if has_month:
                # 用户指定了具体合约（如 AG2602）
                sql = f"""
                    SELECT trade_date, open_price, high_price, low_price, close_price 
                    FROM futures_price
                    WHERE ts_code LIKE '{symbol}%%' AND ts_code NOT LIKE '%%TAS%%'
                    {date_condition}
                    ORDER BY trade_date DESC LIMIT 60
                """
            else:
                # 用户只输入品种（如 白银），查询主力合约
                clean_symbol = ''.join([i for i in symbol if not i.isdigit()])

                # 步骤1：找出主力合约（持仓量最大的）
                sql_main = f"""
                        SELECT ts_code FROM futures_price 
                        WHERE ts_code LIKE '{clean_symbol}%%' 
                          AND ts_code NOT LIKE '%%TAS%%'
                          AND ts_code REGEXP '[0-9]{{4}}$'
                        ORDER BY trade_date DESC, oi DESC 
                        LIMIT 1
                    """
                df_main = pd.read_sql(sql_main, engine)

                if df_main.empty:
                    return f"未找到 {query} 的主力合约"

                main_contract = df_main.iloc[0]['ts_code']

                # 步骤2：只查主力合约的K线数据
                sql = f"""
                        SELECT trade_date, open_price, high_price, low_price, close_price 
                        FROM futures_price
                        WHERE ts_code = '{main_contract}'
                        {date_condition}
                        ORDER BY trade_date DESC LIMIT 60
                    """
            df = pd.read_sql(sql, engine)

        # 🔥【新增】指数查询逻辑
        elif asset_type == 'index':
            sql = f"""
                SELECT trade_date, open_price, high_price, low_price, close_price 
                FROM index_price
                WHERE ts_code='{symbol}' 
                {date_condition} 
                ORDER BY trade_date DESC LIMIT 60
            """
            df = pd.read_sql(sql, engine)

        if df.empty:
            target_date_msg = f" ({trade_date})" if trade_date else ""
            return f"未找到品种 {symbol}{target_date_msg} 的历史价格数据，无法进行技术分析。"

        # 3. 数据预处理
        df = df.sort_values('trade_date').reset_index(drop=True)
        for col in ['open_price', 'high_price', 'low_price', 'close_price']:
            df[col] = pd.to_numeric(df[col])

        # 4. 计算均线
        df['MA5'] = df['close_price'].rolling(window=5).mean()
        df['MA10'] = df['close_price'].rolling(window=10).mean()
        df['MA20'] = df['close_price'].rolling(window=20).mean()
        df['MA30'] = df['close_price'].rolling(window=30).mean()
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
        uplo_pct = upper_shadow / lower_shadow

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

        # 昨日K线实体大小
        prebody_pct = abs(prev_close - prev_open) / (prev_high - prev_low)

        # 前日K线实体大小
        pprebody_pct = abs(pprev_close - pprev_open) / (pprev_high - pprev_low)

        # B. K线形态判断列表
        patterns = []

        # --- 【新增】吞噬形态 (Engulfing Pattern) ---

        # 1. 多头吞噬 (Bullish Engulfing)
        # 条件：空头趋势(5<20) + 昨日大跌(>2%) + 今日大涨 + 包住昨日
        if (curr['MA5'] < curr['MA20']) and (prev_chg_pct < -0.005) and (chg_pct > 0) and (open_p < prev_close) and (
                close > prev_open):
            patterns.append("【多头吞噬】(空头趋势阶段见底，转折信号！)")

        # 2. 空头吞噬 (Bearish Engulfing)
        # 条件：多头趋势(5>20) + 昨日大涨(>2%) + 今日大跌 + 包住昨日
        if (curr['MA5'] > curr['MA20']) and \
                (prev_chg_pct > 0) and \
                (chg_pct < 0) and \
                (open_p > prev_close) and \
                (close < prev_open):
            patterns.append("【空头吞噬】(多头趋势阶段见顶，反转信号！)")

        # 3. 上升三法
        if (curr['MA5'] > curr['MA10']) and chg_pct > 0.01:
            if pprev_chg_pct > 0.015 and prev_close < pprev_close and prev_open < pprev_close and close > prev_2_days_high:
                patterns.append("【上升三法】(中继再涨，多头持续上攻！)")
            elif tprev_chg_pct > 0.015 and pprev_close < tprev_high and prev_close < tprev_high and close > prev_3_days_high:
                patterns.append("【上升三法】(中继再涨，多头持续上攻！！)")
            elif pppprev_chg_pct > 0.015 and tprev_close < pppprev_high and pprev_close < pppprev_high and prev_close < pppprev_high and close > prev_4_days_high:
                patterns.append("【上升三法】(中继再涨，多头持续上攻！！)")
            elif fprev_chg_pct > 0.015 and pppprev_close < fprev_high and tprev_close < fprev_high and pprev_close < fprev_high and prev_close < fprev_high and close > prev_5_days_high:
                patterns.append("【上升三法】(中继再涨，多头持续上攻！！)")
        # 4. 下降三法
        if (curr['MA5'] < curr['MA10']) and chg_pct < -0.01:
            if pprev_chg_pct < -0.015 and prev_close > pprev_close and prev_open > pprev_close and close < prev_2_days_low:
                patterns.append("【下降三法】(中继再跌，空头持续发力！)")
            elif tprev_chg_pct < -0.015 and pprev_close > tprev_low and prev_close > tprev_low and close < prev_3_days_low:
                patterns.append("【下降三法】(中继再跌，空头持续发力！)")
            elif pppprev_chg_pct < -0.015 and tprev_close > pppprev_low and pprev_close > pppprev_low and prev_close > pppprev_low and close < prev_4_days_low:
                patterns.append("【下降三法】(中继再跌，空头持续发力！)")
            elif fprev_chg_pct < -0.015 and pppprev_close > fprev_low and tprev_close > fprev_low and pprev_close > fprev_low and prev_close > fprev_low and close < prev_5_days_low:
                patterns.append("【下降三法】(中继再跌，空头持续发力！)")

        # 6. 晨星
        if (curr['MA5'] < curr[
            'MA10']) and pprebody_pct > -0.01 and pprebody_pct > 0.8 and close > prev_high and open_p > prev_close:
            if prebody_pct < 0.3 and prev_close < pprev_close:
                patterns.append("【晨星】(反转迹象-从空转多)")

        # 6.夜星
        if (curr['MA5'] > curr[
            'MA10']) and pprev_chg_pct > 0.01 and pprebody_pct > 0.8 and close < prev_low and open_p < prev_close:
            if prebody_pct < 0.3 and prev_close > pprev_close:
                patterns.append("【夜星】(反转迹象-从多转空)")

        # 6.步步为营
        if (curr['MA5'] > curr['MA10']) and pprev_chg_pct > 0.015:
            if pprev_chg_pct > prev_chg_pct > chg_pct > 0:
                patterns.append("【步步为营】(多头力量减弱，小心回调)")

        if (curr['MA5'] < curr['MA10']) and pprev_chg_pct > -0.015:
            if pprev_chg_pct < prev_chg_pct < chg_pct < 0:
                patterns.append("【步步为营】(空头力量减弱，小心反弹)")

        # 1. 计算 ATR (波动率尺子) 用于动态衡量箱体
        # TR = Max(High-Low, Abs(High-PrevClose), Abs(Low-PrevClose))
        df['h-l'] = df['high_price'] - df['low_price']
        df['h-pc'] = abs(df['high_price'] - df['close_price'].shift(1))
        df['l-pc'] = abs(df['low_price'] - df['close_price'].shift(1))
        df['tr'] = df[['h-l', 'h-pc', 'l-pc']].max(axis=1)
        # 通常用 14 天 ATR
        df['atr'] = df['tr'].rolling(window=14).mean()

        # --- 【ATR 动态自适应横盘策略】 ---

        # 定义扫描周期
        scan_periods = [5, 10, 20, 30, 60]
        breakout_found = False

        # 获取昨日的 ATR (作为基准，不用今天的防止未来函数)
        ref_atr = df['atr'].iloc[-2]

        if not pd.isna(ref_atr) and ref_atr > 0:

            for period in scan_periods:
                if len(df) <= period + 1: continue

                # 1. 截取箱体 (不含今天)
                recent_box = df.iloc[-(period + 1):-1]
                box_high = recent_box['high_price'].max()
                box_low = recent_box['low_price'].min()
                box_height = box_high - box_low

                # 2. 【核心】计算 ATR 倍数 (ATR Ratio)
                # 含义：箱体高度 相当于 几天的平均波动？
                # 倍数越小，说明压缩越极致
                atr_ratio = box_height / ref_atr

                # 3. 动态阈值设置 (这里是精华)
                # 5天横盘：箱体高度不应超过 2.5 倍 ATR (非常极致)
                # 10天横盘：箱体高度不应超过 4.0 倍 ATR
                # 20天横盘：箱体高度不应超过 6.0 倍 ATR
                # 60天横盘：箱体高度不应超过 10.0 倍 ATR
                if period <= 5:
                    max_atr_multiple = 2.5
                    p_name = "极致压缩"
                elif period <= 10:
                    max_atr_multiple = 4.0
                    p_name = "短线旗形"
                elif period <= 20:
                    max_atr_multiple = 6.0
                    p_name = "标准箱体"
                else:
                    max_atr_multiple = 10.0
                    p_name = "长线平台"

                # 4. 判断逻辑
                if atr_ratio <= max_atr_multiple:

                    # A. 向上突破 (需配合中阳线/大阳线)
                    if close > box_high and body_pct > 0.6:
                        msg = f"【{p_name}突破】，压缩比{atr_ratio:.1f}ATR)"
                        patterns.append(msg)
                        breakout_found = True
                        break  # 找到最有爆发力的就不找了

                    # B. 向下破位
                    if close < box_low and body_pct > 0.6:
                        msg = f"【{p_name}破位】，压缩比{atr_ratio:.1f}ATR)"
                        patterns.append(msg)
                        breakout_found = True
                        break

                # =================================================================
                # 【新增】ATR 假突破/假跌破识别 (False Breakout/Breakdown)
                # 逻辑：昨天突破了箱体，今天又跌回箱体以内
                # =================================================================

                # 1. 获取前天 ATR (用于判断昨天突破前的盘整状态，因为要还原昨天的场景)
                ref_atr_prev = df['atr'].iloc[-3] if len(df) > 3 else 0

                # 2. 只有当前天有有效的 ATR 时才计算
                if not pd.isna(ref_atr_prev) and ref_atr_prev > 0:
                    for period in scan_periods:
                        # 至少需要 period + 2 天的数据 (今天+昨天+周期)
                        if len(df) <= period + 2: continue

                        # 3. 定义 "昨天突破前" 的箱体 (不含昨天和今天)
                        # 索引: -1是今天, -2是昨天. 切片 [-(period+2) : -2]
                        box_prev_days = df.iloc[-(period + 2):-2]

                        # 计算那个时候的箱体上下沿
                        box_high_prev = box_prev_days['high_price'].max()
                        box_low_prev = box_prev_days['low_price'].min()
                        box_height_prev = box_high_prev - box_low_prev

                        # 计算压缩比
                        atr_ratio_prev = box_height_prev / ref_atr_prev

                        # 动态阈值 (复用之前的逻辑)
                        if period <= 5:
                            max_atr_multiple = 2.5;
                            p_name_prev = "极致压缩"
                        elif period <= 10:
                            max_atr_multiple = 4.0;
                            p_name_prev = "短线旗形"
                        elif period <= 20:
                            max_atr_multiple = 6.0;
                            p_name_prev = "标准箱体"
                        else:
                            max_atr_multiple = 10.0;
                            p_name_prev = "长线平台"

                        # 4. 判断逻辑：如果昨天那个时候是压缩状态
                        if atr_ratio_prev <= max_atr_multiple:

                            # A. 假突破 (Bull Trap)
                            # 条件: 昨天收盘 > 上沿 (真突破), 今天收盘 < 上沿 (跌回)
                            if prev_close > box_high_prev and close < box_high_prev:
                                msg = f"【假突破(多头陷阱)】(昨天突破{period}日{p_name_prev}，今天跌回，警惕诱多！)"
                                patterns.append(msg)
                                break

                                # B. 假跌破 (Bear Trap)
                            # 条件: 昨天收盘 < 下沿 (真跌破), 今天收盘 > 下沿 (收回)
                            if prev_close < box_low_prev and close > box_low_prev:
                                msg = f"【假跌破(空头陷阱)】(昨天跌破{period}日{p_name_prev}，今天收回，警惕诱空！)"
                                patterns.append(msg)
                                break

        # --- 基础形态 ---

        # 3. 大阳/大阴
        if body_pct > 0.8 and abs(chg_pct) > 0.03:
            if close > open_p:
                patterns.append("【大阳线】(多头气势强)")
            else:
                patterns.append("【大阴线】(空头气势强)")

        # 3. 波动转折突破
        if curr['MA30'] > curr['MA20']:
            if abs(chg_pct) > 0.01 and close > prev_5_days_high and body_pct > 0.6:
                patterns.append("【多头反击】(波段转折)")
        if curr['MA30'] < curr['MA20']:
            if chg_pct < -0.01 and close < prev_5_days_low and body_pct > 0.6:
                patterns.append("【空头反击】(波段转折)")

        # 4. 长下影
        if lower_pct > 2.0 and upper_pct < 1.0 and body_pct < 0.3 and body_pct > 0.01:
            if curr['MA5'] < curr['MA20']:
                patterns.append("【锤子】(多头抵抗)")
            elif curr['MA5'] > curr['MA20']:
                patterns.append("【吊人线】(高位抛压预警)")

        # 5. 长上影
        if upper_pct > 2.0 and lower_pct < 1.0 and body_pct < 0.3 and body_pct > 0.01 and curr['MA5'] > curr['MA20']:
            patterns.append("【倒状锤子】(卖压沉重)")

        # 6. 十字星
        if body_pct < 0.1 and 0.5 < uplo_pct < 1.5:
            patterns.append("【十字星】(多空对峙)")

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

        # ============================================================
        #  【新增】近几日K线形态概览 & 多日组合分析
        # ============================================================

        # A. 辅助函数：判断单根K线基本形态
        def classify_single_kline(o, h, l, c, prev_c):
            """判断单根K线形态，返回简短描述"""
            body = abs(c - o)
            total = h - l if h != l else 0.01
            upper = h - max(c, o)
            lower = min(c, o) - l
            body_pct = body / total
            chg = (c - prev_c) / prev_c if prev_c else 0

            # 涨跌方向
            direction = "阳" if c > o else "阴" if c < o else "平"

            # 实体大小
            if body_pct > 0.8 and abs(chg) > 0.025:
                size = "大"
            elif body_pct > 0.5:
                size = "中"
            elif body_pct < 0.15:
                # 十字星或小实体
                if body_pct < 0.08:
                    if lower > body * 2 and upper < body:
                        return f"锤子线({chg * 100:+.1f}%)"
                    elif upper > body * 2 and lower < body:
                        return f"倒锤子({chg * 100:+.1f}%)"
                    else:
                        return f"十字星({chg * 100:+.1f}%)"
                size = "小"
            else:
                size = "小"

            return f"{size}{direction}({chg * 100:+.1f}%)"

        # B. 生成近5日K线概览
        recent_klines = []
        for i in range(-5, 0):
            if len(df) > abs(i):
                row = df.iloc[i]
                prev_row = df.iloc[i - 1] if len(df) > abs(i - 1) else row
                kline_desc = classify_single_kline(
                    row['open_price'], row['high_price'],
                    row['low_price'], row['close_price'],
                    prev_row['close_price']
                )
                day_label = ["T-4", "T-3", "T-2", "T-1", "今日"][i + 5]
                recent_klines.append(f"{day_label}: {kline_desc}")

        # C. 多日组合形态识别
        combo_patterns = []

        # 获取近几日涨跌幅
        recent_changes = []
        for i in range(-5, 0):
            if len(df) > abs(i) and len(df) > abs(i - 1):
                row = df.iloc[i]
                prev_row = df.iloc[i - 1]
                chg = (row['close_price'] - prev_row['close_price']) / prev_row['close_price']
                recent_changes.append(chg)

        # 连续阳线/阴线判断
        consecutive_up = sum(1 for c in recent_changes[-3:] if c > 0.005)
        consecutive_down = sum(1 for c in recent_changes[-3:] if c < -0.005)

        if consecutive_up >= 3:
            # 红三兵判断：连续3阳且都是实体阳线
            if all(c > 0.01 for c in recent_changes[-3:]):
                combo_patterns.append("【红三兵】(强势上攻，多头气势如虹)")
            else:
                combo_patterns.append(f"【连续{consecutive_up}阳】(多头占优)")

        if consecutive_down >= 3:
            # 三只乌鸦判断：连续3阴且都是实体阴线
            if all(c < -0.01 for c in recent_changes[-3:]):
                combo_patterns.append("【三只乌鸦】(空头肆虐，注意风险)")
            else:
                combo_patterns.append(f"【连续{consecutive_down}阴】(空头占优)")

        # 先跌后涨（V型反转雏形）
        if len(recent_changes) >= 4:
            if recent_changes[-4] < -0.01 and recent_changes[-3] < -0.01 and recent_changes[-2] > 0 and recent_changes[
                -1] > 0.01:
                combo_patterns.append("【V型反转雏形】(连跌后连涨，关注反转确认)")

        # 先涨后跌（倒V雏形）
        if len(recent_changes) >= 4:
            if recent_changes[-4] > 0.01 and recent_changes[-3] > 0.01 and recent_changes[-2] < 0 and recent_changes[
                -1] < -0.01:
                combo_patterns.append("【倒V见顶雏形】(连涨后连跌，注意回调风险)")

        # 缩量整理（波动收窄）
        recent_ranges = []
        for i in range(-5, 0):
            if len(df) > abs(i):
                row = df.iloc[i]
                recent_ranges.append(row['high_price'] - row['low_price'])
        if len(recent_ranges) >= 3:
            if recent_ranges[-1] < recent_ranges[-2] < recent_ranges[-3]:
                combo_patterns.append("【波动收窄】(整理蓄势，关注方向选择)")

        # 放量突破
        if len(recent_ranges) >= 3:
            avg_range = sum(recent_ranges[:-1]) / len(recent_ranges[:-1])
            if recent_ranges[-1] > avg_range * 1.5 and chg_pct > 0.015:
                combo_patterns.append("【放量突破】(波动放大配合上涨，多头发力)")
            elif recent_ranges[-1] > avg_range * 1.5 and chg_pct < -0.015:
                combo_patterns.append("【放量下跌】(波动放大配合下跌，空头发力)")

        # D. 整合多日趋势判断
        multi_day_trend = ""
        if len(recent_changes) >= 5:
            total_chg = sum(recent_changes)
            up_days = sum(1 for c in recent_changes if c > 0)
            down_days = sum(1 for c in recent_changes if c < 0)

            if total_chg > 0.05 and up_days >= 4:
                multi_day_trend = "📈 近5日强势上涨，多头主导"
            elif total_chg < -0.05 and down_days >= 4:
                multi_day_trend = "📉 近5日持续下跌，空头主导"
            elif abs(total_chg) < 0.02:
                multi_day_trend = "↔️ 近5日横盘震荡，方向待选"
            elif total_chg > 0:
                multi_day_trend = "📊 近5日小幅上涨，震荡偏多"
            else:
                multi_day_trend = "📊 近5日小幅下跌，震荡偏空"

        # --- 6. 输出报告 (增强版) ---
        report = f"""
📊 **{symbol} K线技术面诊断** ({date})

**一、今日形态信号**
{' 🔥 '.join(patterns) if patterns else '普通震荡K线，无明显形态。'}

**二、近5日K线概览**
{' → '.join(recent_klines)}

**三、多日组合形态**
{' | '.join(combo_patterns) if combo_patterns else '暂无明显组合形态。'}

**四、趋势研判**
- 均线位置：{'，'.join(trends)}
- 多日趋势：{multi_day_trend}

**五、价格数据**
- 今日收盘：{close} (涨跌 {chg_pct * 100:+.2f}%)
- MA5: {curr['MA5']:.2f} | MA10: {curr['MA10']:.2f} | MA20: {curr['MA20']:.2f}
        """

        return report

    except Exception as e:
        return f"K线分析出错: {e}"


if __name__ == "__main__":
    # 工具必须用 .invoke 并在字典里传参
    print(analyze_kline_pattern.invoke({"symbol": "茅台"}))