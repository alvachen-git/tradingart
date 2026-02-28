# kline_algo.py
import pandas as pd
import numpy as np


def calculate_kline_signals(df: pd.DataFrame):
    """
    核心K线算法库 (严格复刻版)
    逻辑与 kline_tools.py 100% 保持一致
    """
    if df.empty or len(df) < 30:
        return {'patterns': [], 'trends': [], 'score': 50}

    # 1. 确保数值类型正确
    cols = ['open_price', 'high_price', 'low_price', 'close_price']
    for col in cols:
        df[col] = pd.to_numeric(df[col], errors='coerce')

    # 2. 计算均线 (保持一致)
    df['MA5'] = df['close_price'].rolling(window=5).mean()
    df['MA10'] = df['close_price'].rolling(window=10).mean()
    df['MA20'] = df['close_price'].rolling(window=20).mean()
    df['MA30'] = df['close_price'].rolling(window=30).mean()
    df['MA60'] = df['close_price'].rolling(window=60).mean()

    # 3. 计算 ATR (保持一致)
    df['h-l'] = df['high_price'] - df['low_price']
    df['h-pc'] = abs(df['high_price'] - df['close_price'].shift(1))
    df['l-pc'] = abs(df['low_price'] - df['close_price'].shift(1))
    df['tr'] = df[['h-l', 'h-pc', 'l-pc']].max(axis=1)
    df['atr'] = df['tr'].rolling(window=14).mean()

    # 4. 提取关键数据
    curr = df.iloc[-1]
    prev = df.iloc[-2]
    pprev = df.iloc[-3]
    tprev = df.iloc[-4]
    pppprev = df.iloc[-5]
    fprev = df.iloc[-6]

    # 基础变量
    close = curr['close_price']
    open_p = curr['open_price']
    high = curr['high_price']
    low = curr['low_price']

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

    # 涨跌幅
    chg_pct = (close - prev_close) / prev_close if prev_close != 0 else 0
    prev_chg_pct = (prev_close - prev_open) / prev_open if prev_open != 0 else 0
    pprev_chg_pct = (pprev_close - pprev_open) / pprev_open
    tprev_chg_pct = (tprev_close - tprev_open) / tprev_open
    pppprev_chg_pct = (pppprev_close - pppprev_open) / pppprev_open
    fprev_chg_pct = (fprev_close - fprev_open) / fprev_open

    # 形态比例计算 (复刻原逻辑)
    body_size = abs(close - open_p)
    upper_shadow = high - max(close, open_p)
    lower_shadow = min(close, open_p) - low
    total_range = high - low if high != low else 0.01

    body_pct = body_size / total_range
    upper_pct = upper_shadow / body_size if body_size != 0 else 0
    lower_pct = lower_shadow / body_size if body_size != 0 else 0
    uplo_pct = upper_shadow / lower_shadow if lower_shadow != 0 else 0

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

    # 实体大小
    prebody_pct = abs(prev_close - prev_open) / (prev_high - prev_low) if (prev_high - prev_low) > 0 else 0
    pprebody_pct = abs(pprev['close_price'] - pprev['open_price']) / (pprev['high_price'] - pprev['low_price']) if (
                                                                                                                               pprev[
                                                                                                                                   'high_price'] -
                                                                                                                               pprev[
                                                                                                                                   'low_price']) > 0 else 0

    patterns = []
    trends = []
    score_change = 0

    # ==========================
    #  A. 形态识别 (严格复刻)
    # ==========================

    # 1. 吞噬形态 (Engulfing)
    if (curr['MA5'] < curr['MA20']) and (prev_chg_pct < -0.015) and (chg_pct > 0) and (open_p < prev_close) and (close > prev_high):
        if chg_pct > 0.03:
            patterns.append("多头吞噬")
            score_change += 30
        else:
            patterns.append("多头吞噬")
            score_change += 10

    if (curr['MA5'] > curr['MA20']) and (prev_chg_pct > 0.015) and (chg_pct < 0) and (open_p > prev_close) and (close < prev_open):
        if chg_pct < -0.03:
            patterns.append("空头吞噬")
            score_change -= 40
        else:
            patterns.append("空头吞噬")
            score_change -= 20

    # 3. 上升三法
    if (curr['MA5'] > curr['MA10']) and chg_pct > 0.02:
        if pprev_chg_pct > 0.02 and prev_close < pprev_close and prev_open < pprev_close and close > prev_2_days_high:
            patterns.append("【上升三法】(中继再涨，多头持续上攻！)")
            score_change += 15
        elif tprev_chg_pct > 0.02 and pprev_close < tprev_high and prev_close < tprev_high and close > prev_3_days_high:
            patterns.append("【上升三法】(中继再涨，多头持续上攻)")
            score_change += 20
        elif pppprev_chg_pct > 0.02 and tprev_close < pppprev_high and pprev_close < pppprev_high and prev_close < pppprev_high and close > prev_4_days_high:
            patterns.append("【上升三法】(中继再涨，多头持续上攻)")
            score_change += 20
        elif fprev_chg_pct > 0.02 and pppprev_close < fprev_high and tprev_close < fprev_high and pprev_close < fprev_high and prev_close < fprev_high and close > prev_5_days_high:
            patterns.append("【上升三法】(中继再涨，多头持续上攻)")
            score_change += 25
    # 4. 下降三法
    if (curr['MA5'] < curr['MA10']) and chg_pct < -0.02:
        if pprev_chg_pct < -0.02 and prev_close > pprev_close and prev_open > pprev_close and close < prev_2_days_low:
            patterns.append("【下降三法】(中继再跌，多头持续溃逃)")
            score_change -= 15
        elif tprev_chg_pct < -0.02 and pprev_close > tprev_low and prev_close > tprev_low and close < prev_3_days_low:
            patterns.append("【下降三法】(中继再跌，多头持续溃逃)")
            score_change -= 20
        elif pppprev_chg_pct < -0.02 and tprev_close > pppprev_low and pprev_close > pppprev_low and prev_close > pppprev_low and close < prev_4_days_low:
            patterns.append("【下降三法】(中继再跌，多头持续溃逃)")
            score_change -= 20
        elif fprev_chg_pct < -0.02 and pppprev_close > fprev_low and tprev_close > fprev_low and pprev_close > fprev_low and prev_close > fprev_low and close < prev_5_days_low:
            patterns.append("【下降三法】(中继再跌，多头持续溃逃)")
            score_change -= 25

    # 6. 晨星
    if (curr['MA5'] < curr['MA10']) and pprev_chg_pct < -0.02 and pprebody_pct  > 0.8 and close > prev_high and open_p > prev_close:
        if prebody_pct < 0.3 and prev_close < pprev_close:
            patterns.append("【晨星】(反转迹象-从空转多)")
            score_change += 15

    # 6.夜星
    if (curr['MA5'] > curr['MA10']) and pprev_chg_pct > 0.02 and pprebody_pct  > 0.8 and close < prev_low and open_p < prev_close:
        if prebody_pct < 0.3 and prev_close > pprev_close:
            patterns.append("【夜星】(反转迹象-从多转空)")
            score_change -= 15

    # 6.步步为营
    if (curr['MA5'] > curr['MA10']) and pprev_chg_pct > 0.015:
        if pprev_chg_pct > prev_chg_pct > chg_pct > 0:
            patterns.append("【步步为营】(多头力量减弱，小心回调)")
            score_change -= 5

    if (curr['MA5'] < curr['MA10']) and pprev_chg_pct > -0.015:
        if pprev_chg_pct < prev_chg_pct < chg_pct < 0:
            patterns.append("【步步为营】(空头力量减弱，小心反弹)")
            score_change += 10

    # 5. ATR 动态箱体突破 & 假突破 (复刻您的核心逻辑)
    scan_periods = [5, 10, 20, 30, 60]
    ref_atr = df['atr'].iloc[-2]

    if not pd.isna(ref_atr) and ref_atr > 0:
        for period in scan_periods:
            if len(df) <= period + 1: continue

            # --- 真突破检测 ---
            recent_box = df.iloc[-(period + 1):-1]
            box_high = recent_box['high_price'].max()
            box_low = recent_box['low_price'].min()
            box_height = box_high - box_low
            atr_ratio = box_height / ref_atr

            # 阈值
            if period <= 5:
                max_mul = 2.5
            elif period <= 10:
                max_mul = 4.0
            elif period <= 20:
                max_mul = 6.0
            else:
                max_mul = 10.0

            if atr_ratio <= max_mul:
                if close > box_high and body_pct > 0.6:
                    patterns.append(f"{period}日平台突破")
                    score_change += 30
                    break
                # B. 向下破位
                if close < box_low and body_pct > 0.6:
                    patterns.append(f"{period}日平台跌破")
                    score_change -= 30
                    break

                    # --- 假突破/假跌破检测 (复刻原代码逻辑) ---
            # 还原昨天的场景
            ref_atr_prev = df['atr'].iloc[-3] if len(df) > 3 else 0

            if not pd.isna(ref_atr_prev) and ref_atr_prev > 0:
                if len(df) <= period + 2: continue

                # 定义"昨天突破前"的箱体
                box_prev_days = df.iloc[-(period + 2):-2]
                box_high_prev = box_prev_days['high_price'].max()
                box_low_prev = box_prev_days['low_price'].min()
                box_height_prev = box_high_prev - box_low_prev
                atr_ratio_prev = box_height_prev / ref_atr_prev

                if atr_ratio_prev <= max_mul:
                    # A. 假突破 (Bull Trap)
                    # 昨天收盘 > 上沿，今天收盘 < 上沿
                    if prev_close > box_high_prev and close < box_high_prev:
                        patterns.append("假突破(诱多)")
                        score_change -= 25
                        break

                    # B. 假跌破 (Bear Trap)
                    # 昨天收盘 < 下沿，今天收盘 > 下沿
                    if prev_close < box_low_prev and close > box_low_prev:
                        patterns.append("假跌破(诱空)")
                        score_change += 25
                        break

    # 6. 大阳/大阴
    if body_pct > 0.8 and abs(chg_pct) > 0.03:
        if close > open_p:
            patterns.append("大阳线")
            score_change += 5
        else:
            patterns.append("大阴线")
            score_change -= 5

    # 7. 波动转折 (复刻原代码)
    # 多头反击
    if curr['MA30'] > curr['MA20']:
        if abs(chg_pct) > 0.01 and close > prev_5_days_high and body_pct > 0.6:
            patterns.append("波动转折(多头)")
            score_change += 20
    # 空头反击
    if curr['MA30'] < curr['MA20']:
        if chg_pct < -0.01 and close < prev_5_days_low and body_pct > 0.6:
            patterns.append("波动转折(空头)")
            score_change -= 15

    # 区间突破
    if close > prev_5_days_high and  upper_pct < 0.5 :
        if 0.05 > chg_pct > 0.01 :
            patterns.append("小区间突破")
            score_change += 20
        elif  chg_pct > 0.05:
            patterns.append("小区间强势突破")
            score_change += 10


    # 8. 长下影 (复刻原代码)
    # 条件：下影 > 2倍实体，实体 < 0.3，MA5 < MA20，收盘 < 昨日收盘
    if lower_pct > 2 and upper_pct < 1.0 and body_pct < 0.3 and body_pct > 0.01 and curr['MA5'] < curr['MA20'] :
        patterns.append("锤子线(长下影)")
        score_change += 10

    # 9. 长上影 (复刻原代码)
    # 条件：上影 > 2倍实体，MA5 > MA20，收盘 > 昨日收盘
    if upper_pct > 2 and lower_pct < 1.0 and body_pct < 0.3 and body_pct > 0.01 and curr['MA5'] > curr['MA20'] :
        patterns.append("倒锤子(长上影)")
        score_change -= 10

    # 10. 十字星
    if body_pct < 0.1 and 0.5 < uplo_pct < 1.5:
        patterns.append("十字星")

        # ==========================
        # [新增] 11. 创新高策略 (Breakout Strategy)
        # ==========================

     # 策略 A: 创90日新高 (中期突破，海龟战法核心)
    # 逻辑：今天的收盘价 > 过去 59 个交易日(不含今天)的最高价
    if len(df) >= 90:
        # iloc[-61:-1] 取的是从倒数第61天到昨天的数据
        prev_60_days_high = df['high_price'].iloc[-91:-1].max()

        # 必须是收盘价站上去才算有效突破，光是盘中摸一下不算
        if close > prev_60_days_high:
            patterns.append("创新高")
            score_change += 10  # 这是一个非常强的多头信号，加分权重高一些

    # ==========================
    #  B. 趋势判定 (复刻逻辑)
    # ==========================
    if close > curr['MA5']:
        trends.append("站上5日线(短强)")
        score_change += 5
    else:
        trends.append("跌破5日线(短弱)")

    if close > curr['MA20']:
        if curr['MA20'] > prev['MA20']:
            trends.append("站稳20日线且向上(中多)")
            score_change += 10
        else:
            trends.append("20日线上方震荡")
    else:
        trends.append("跌破20日线(中空)")
        score_change -= 10

    if curr['MA5'] > curr['MA20'] > curr['MA60']:
        trends.append("均线多头排列")
        score_change += 20
    elif curr['MA5'] < curr['MA20'] < curr['MA60']:
        trends.append("均线空头排列")
        score_change -= 20

    if curr['MA20'] > curr['MA5'] > curr['MA10']:
        trends.append("震荡横盘格局")
    elif curr['MA5'] > curr['MA20'] > curr['MA10']:
        trends.append("震荡偏多格局")
    elif curr['MA20'] < curr['MA5'] < curr['MA10']:
        trends.append("震荡横盘格局")
    elif curr['MA5'] < curr['MA20'] < curr['MA10']:
        trends.append("震荡偏空格局")

    return {
        'patterns': patterns,
        'trends': trends,
        'score': 50 + score_change
    }
