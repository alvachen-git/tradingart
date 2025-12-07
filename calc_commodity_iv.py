import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
import os
from dotenv import load_dotenv
from py_vollib_vectorized import vectorized_implied_volatility
from datetime import datetime
import tushare as ts
import warnings

# 1. 初始化
load_dotenv(override=True)

DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")

db_url = f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(db_url)

ts_token = os.getenv("TUSHARE_TOKEN")
ts.set_token(ts_token)
pro = ts.pro_api()


def calc_iv_from_db(symbol="rb", days=365):
    print(f"\n[*] 正在計算 {symbol} (修復版)...")

    # 1. 獲取標的期貨價格
    target_code_list = [f"{symbol.lower()}0", f"{symbol.upper()}0"]
    target_str = "', '".join(target_code_list)

    sql_future = f"SELECT trade_date, close_price as close FROM futures_price WHERE ts_code IN ('{target_str}') ORDER BY trade_date"
    df_future = pd.read_sql(sql_future, engine)

    if df_future.empty:
        print(f" [-] 未找到標的 {symbol}0 的價格")
        return

    # 計算 HV
    df_future['log_ret'] = np.log(df_future['close'] / df_future['close'].shift(1))
    df_future['hv'] = df_future['log_ret'].rolling(20).std() * np.sqrt(252) * 100

    process_dates = df_future['trade_date'].sort_values(ascending=False).head(days).tolist()

    count_saved = 0
    for date in process_dates:
        try:
            curr_row = df_future[df_future['trade_date'] == date].iloc[0]
            S = curr_row['close']
            HV = curr_row['hv']
            if pd.isna(HV): HV = 0

            # 查詢期權
            sql = text("""
                       SELECT a.ts_code,
                              a.close          as price,
                              a.oi,
                              b.exercise_price as k,
                              b.maturity_date  as expiry,
                              b.call_put
                       FROM commodity_opt_daily a
                                JOIN commodity_option_basic b ON a.ts_code = b.ts_code
                       WHERE a.trade_date = :date
                         AND (b.underlying = :symbol OR b.underlying = upper(:symbol))
                         AND a.oi > 0
                       ORDER BY a.oi DESC LIMIT 10
                       """)

            with engine.connect() as conn:
                opts = pd.read_sql(sql, conn, params={"date": date, "symbol": symbol})

            if opts.empty: continue

            # 計算 IV
            valid_ivs = []
            for _, row in opts.iterrows():
                try:
                    price = row['price']
                    K = row['k']
                    expiry = str(int(row['expiry']))
                    cp = row['call_put'].lower()

                    if not expiry: continue
                    days_left = (pd.to_datetime(expiry) - pd.to_datetime(date)).days
                    T = days_left / 365.0

                    if T <= 0.005: continue

                    # 計算
                    iv = vectorized_implied_volatility(price, S, K, T, 0.015, cp, return_as='numpy')

                    # --- 【關鍵修復】提取數值 ---
                    if isinstance(iv, np.ndarray):
                        iv = iv.item()
                    # --------------------------

                    if not np.isnan(iv) and iv > 0 and iv < 5:
                        valid_ivs.append(iv)
                except:
                    continue

            if valid_ivs:
                avg_iv = np.mean(valid_ivs) * 100

                with engine.connect() as conn:
                    conn.execute(
                        text(f"DELETE FROM commodity_iv_history WHERE trade_date='{date}' AND ts_code='{symbol}'"))
                    ins = text("INSERT INTO commodity_iv_history (trade_date, ts_code, iv, hv) VALUES (:d, :c, :i, :h)")
                    conn.execute(ins, {"d": date, "c": symbol, "i": avg_iv, "h": HV})
                    conn.commit()
                count_saved += 1

        except Exception as e:
            print(f"日期 {date} 出錯: {e}")
            continue

    print(f" [√] 計算完成，共更新 {count_saved} 天數據")


if __name__ == "__main__":
    # 定义要计算的品种列表
    # 注意：必须确保这些品种在 commodity_option_basic 和 commodity_opt_daily 表里有数据
    target_list = ['rb','i','sm','sf','fg', 'sa', 'bu', 'pg','sc', 'm','c','rm','y','oi','p', 'ag', 'au','cu','al','zn','ru','sn','ni','pb','ao','sh', 'lc', 'si','ps', 'sr','cf','ta','ma','eb', 'eg','v', 'if', 'ih', 'im', 'ap', 'cj']

    print(f"=== 开始批量计算 IV，共 {len(target_list)} 个品种 ===")

    for symbol in target_list:
        try:
            # 计算最近 1 年的数据
            calc_iv_from_db(symbol, days=200)
        except Exception as e:
            print(f" [!] {symbol} 计算出错: {e}")

    print("=== 所有品种计算结束 ===")