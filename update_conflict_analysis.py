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
    print("=== 開始計算多空巔峰對決信號 ===")

    # 1. 獲取數據庫中最新的持倉日期
    with engine.connect() as conn:
        latest_date = conn.execute(text("SELECT MAX(trade_date) FROM futures_holding")).scalar()

    if not latest_date:
        print("[-] 數據庫無持倉數據")
        return

    print(f"[*] 分析日期: {latest_date}")

    # 2. 定義陣營
    # 正向指標 (聰明錢)
    smart_keywords = ['海通', '东证', '国泰君安']  # 簡體匹配
    # 反向指標 (散戶大本營)
    dumb_keywords = ['中信建投', '东方财富', '方正中期']

    # 3. 獲取當日所有相關機構的持倉
    # 使用正則表達式一次性查出
    all_keywords = "|".join(smart_keywords + dumb_keywords)

    sql = f"""
        SELECT ts_code, broker, net_vol 
        FROM futures_holding 
        WHERE trade_date='{latest_date}' 
          AND broker REGEXP '{all_keywords}'
    """
    df = pd.read_sql(sql, engine)

    if df.empty:
        print("[-] 未找到相關機構持倉")
        return

    # 4. 分組計算
    results = []

    for symbol, group in df.groupby('ts_code'):
        # 計算正方合計
        smart_vol = 0
        for kw in smart_keywords:
            # 模糊匹配累加
            smart_vol += group[group['broker'].str.contains(kw)]['net_vol'].sum()

        # 計算反方合計
        dumb_vol = 0
        for kw in dumb_keywords:
            dumb_vol += group[group['broker'].str.contains(kw)]['net_vol'].sum()

        # 核心邏輯：判斷是否對立 (異號，且兩邊都有持倉，且不為0)
        if smart_vol != 0 and dumb_vol != 0 and (smart_vol * dumb_vol < 0):
            # 以“聰明錢”的方向為準
            action = "看涨" if smart_vol > 0 else "看跌"

            # 記錄結果
            results.append({
                'trade_date': latest_date,
                'symbol': symbol,
                'smart_net': int(smart_vol),
                'dumb_net': int(dumb_vol),
                'dumb_abs': abs(dumb_vol),  # 用於排序 (反指熱度)
                'action': action
            })

    # 5. 排序與入庫
    if results:
        df_res = pd.DataFrame(results)
        # 按照反指標持倉量大小排序，取前 4
        df_top4 = df_res.sort_values('dumb_abs', ascending=False).head(4)

        # 準備入庫字段
        df_save = df_top4[['trade_date', 'symbol', 'smart_net', 'dumb_net', 'action']]

        # 寫入數據庫 (先刪後寫，防止重複)
        with engine.connect() as conn:
            conn.execute(text(f"DELETE FROM market_conflict_daily WHERE trade_date='{latest_date}'"))
            conn.commit()

        df_save.to_sql('market_conflict_daily', engine, if_exists='append', index=False)

        print(f"[√] 計算完成！已存入 {len(df_save)} 條衝突信號")
        print(df_save)
    else:
        print("[-] 今日無明顯多空對決信號")


if __name__ == "__main__":
    calculate_and_save_conflict()