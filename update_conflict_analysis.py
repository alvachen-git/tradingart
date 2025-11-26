import pandas as pd
import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from datetime import datetime

# 1. 初始化環境
load_dotenv(override=True)

DB_USER = os.getenv("DB_USER", "root")
DB_PASSWORD = os.getenv("DB_PASSWORD", "alva13557941")
DB_HOST = os.getenv("DB_HOST", "39.102.215.198")
DB_PORT = os.getenv("DB_PORT", "3306")
DB_NAME = os.getenv("DB_NAME", "finance_data")

db_url = f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(db_url)


def calculate_and_save_conflict():
    print("=== 开始计算多空巅峰对决信号 (含清洗) ===")

    # 1. 获取数据库中最新的持仓日期
    with engine.connect() as conn:
        latest_date = conn.execute(text("SELECT MAX(trade_date) FROM futures_holding")).scalar()

    if not latest_date:
        print("[-] 数据库无持仓数据")
        return

    print(f"[*] 分析日期: {latest_date}")

    # 2. 定义阵营
    smart_keywords = ['海通', '东证', '国泰君安']
    dumb_keywords = ['中信建投', '东方财富', '方正中期']
    all_keywords = "|".join(smart_keywords + dumb_keywords)

    # 3. 查库
    sql = f"""
        SELECT ts_code, broker, net_vol 
        FROM futures_holding 
        WHERE trade_date='{latest_date}' 
          AND broker REGEXP '{all_keywords}'
    """
    df = pd.read_sql(sql, engine)

    if df.empty:
        print("[-] 未找到相关机构持仓")
        return

    # --- 【关键修改】数据清洗 ---
    # 目标：把 '玻璃FG', 'fg0', 'FG' 统一清洗为 'fg'
    def clean_symbol(s):
        # 1. 转小写
        s = s.lower()
        # 2. 去掉中文 (只保留字母和数字)
        import re
        # 匹配字母和数字
        match = re.search(r'[a-z]+', s)
        if match:
            return match.group(0)  # 返回第一个匹配到的字母串 (比如 'fg')
        return s

    df['ts_code'] = df['ts_code'].apply(clean_symbol)

    # 4. 分组计算 (现在按清洗后的代码分组，'玻璃fg' 和 'fg' 会合并在一起)
    results = []

    for symbol, group in df.groupby('ts_code'):
        # 过滤掉非主流品种 (比如长度超过 3 的大概率是脏数据，或者保留主力品种)
        if len(symbol) > 3: continue

        # 计算正方合计
        smart_vol = 0
        for kw in smart_keywords:
            smart_vol += group[group['broker'].str.contains(kw)]['net_vol'].sum()

        # 计算反方合计
        dumb_vol = 0
        for kw in dumb_keywords:
            dumb_vol += group[group['broker'].str.contains(kw)]['net_vol'].sum()

        # 核心逻辑：判断是否对立 (异号，且两边都有持仓)
        # 增加一个阈值：持仓量太小的冲突没有意义 (比如几百手)
        if abs(smart_vol) > 1000 and abs(dumb_vol) > 1000 and (smart_vol * dumb_vol < 0):
            action = "看涨" if smart_vol > 0 else "看跌"

            results.append({
                'trade_date': latest_date,
                'symbol': symbol,
                'smart_net': int(smart_vol),
                'dumb_net': int(dumb_vol),
                'diff_abs': abs(smart_vol - dumb_vol),  # 排序依据：分歧最大 (绝对差值)
                'action': action
            })

    # 5. 排序与入库
    if results:
        df_res = pd.DataFrame(results)

        # 按照【分歧程度】排序 (主力 - 散户 的差值的绝对值)
        # 这样能找出多空打架最激烈的品种
        df_top4 = df_res.sort_values('diff_abs', ascending=False).head(4)

        # 准备入库字段
        df_save = df_top4[['trade_date', 'symbol', 'smart_net', 'dumb_net', 'action']]

        # 写入数据库
        with engine.connect() as conn:
            conn.execute(text(f"DELETE FROM market_conflict_daily WHERE trade_date='{latest_date}'"))
            conn.commit()

        df_save.to_sql('market_conflict_daily', engine, if_exists='append', index=False)

        print(f"[√] 计算完成！Top 4 冲突品种: {df_save['symbol'].tolist()}")
    else:
        print("[-] 今日无明显多空对决信号")


if __name__ == "__main__":
    calculate_and_save_conflict()

if __name__ == "__main__":
    calculate_and_save_conflict()