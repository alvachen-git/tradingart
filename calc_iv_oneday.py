import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
import os
from dotenv import load_dotenv
from py_vollib_vectorized import vectorized_implied_volatility
from datetime import datetime
import time  # 引入 time
import gc  # 引入垃圾回收

# 1. 初始化
load_dotenv(override=True)
DB_USER = os.getenv("DB_USER") or 'root'  # 防错
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT") or '3306'
DB_NAME = os.getenv("DB_NAME") or 'finance_data'

# 增加 pool_recycle 防止连接丢失
db_url = f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(db_url, pool_recycle=3600)


def calculate_and_save_iv(etf_code="510050.SH"):
    print(f"[*] 正在分析 {etf_code} ...", end="")

    # A. 获取标的 ETF 价格 (S) - 这一步很快，内存占用极小
    # 我们取最近 60 天是为了算 HV，这部分逻辑保留
    sql_stock = f"""
        SELECT trade_date, close_price 
        FROM stock_price 
        WHERE ts_code='{etf_code}' 
        ORDER BY trade_date DESC LIMIT 60
    """
    try:
        df_stock = pd.read_sql(sql_stock, engine).sort_values('trade_date')
    except Exception as e:
        print(f" [!] 数据库读取失败: {e}")
        return

    if df_stock.empty:
        print(f" [-] 无价格数据")
        return

    # 计算 HV (20日历史波动率)
    df_stock['log_ret'] = np.log(df_stock['close_price'] / df_stock['close_price'].shift(1))
    df_stock['hv'] = df_stock['log_ret'].rolling(window=20).std() * np.sqrt(252) * 100

    # --- 优化点 1: 确定只计算哪一天 ---
    # 原代码是循环 days=3，然后扔掉前两天。
    # 现在我们直接定位到“数据库里最新的那个交易日”进行计算
    target_date = df_stock['trade_date'].max()
    target_row = df_stock[df_stock['trade_date'] == target_date].iloc[0]

    print(f" 目标日期: {target_date} ...", end="")

    # B. 计算 IV (隐含波动率)
    # 我们不再循环 df_stock，只针对 target_row 计算

    iv_results = []

    try:
        date = target_row['trade_date']
        S = target_row['close_price']
        hv_val = target_row['hv']

        # 1. 找当天的期权数据
        # 优化 SQL: 只查需要的字段，减少内存传输
        sql_opt = f"""
            SELECT d.close as price, b.exercise_price as strike, b.call_put, b.delist_date
            FROM option_daily d
            JOIN option_basic b ON d.ts_code = b.ts_code
            WHERE b.underlying = '{etf_code}' 
              AND d.trade_date = '{date}'
              AND d.vol > 10
        """
        df_opt = pd.read_sql(sql_opt, engine)

        if not df_opt.empty:
            # 2. 筛选平值合约 (ATM)
            df_opt['diff'] = abs(df_opt['strike'] - S)
            min_diff = df_opt['diff'].min()
            atm_opts = df_opt[df_opt['diff'] <= min_diff * 1.2].copy()

            if not atm_opts.empty:
                # 3. 准备 BS 参数
                atm_opts['T'] = (pd.to_datetime(atm_opts['delist_date']) - pd.to_datetime(date)).dt.days / 365.0
                atm_opts = atm_opts[atm_opts['T'] > 0.01]

                if not atm_opts.empty:
                    # 4. 计算 IV (这是最耗 CPU 的一步)
                    r = 0.015
                    ivs = vectorized_implied_volatility(
                        atm_opts['price'].values,
                        S,
                        atm_opts['strike'].values,
                        atm_opts['T'].values,
                        r,
                        atm_opts['call_put'].str.lower().values,
                        return_as='numpy'
                    )

                    valid_ivs = ivs[~np.isnan(ivs) & (ivs > 0)]

                    if len(valid_ivs) > 0:
                        avg_iv = np.mean(valid_ivs) * 100
                        iv_results.append({
                            'trade_date': date,
                            'etf_code': etf_code,
                            'iv': avg_iv,
                            'hv': hv_val
                        })

            # --- 优化点 2: 显式内存回收 ---
            del df_opt
            del atm_opts
            gc.collect()

    except Exception as e:
        print(f" [!] 计算出错: {e}")

    # C. 入库
    if iv_results:
        df_res = pd.DataFrame(iv_results)

        try:
            with engine.connect() as conn:
                del_sql = text("""
                               DELETE
                               FROM etf_iv_history
                               WHERE etf_code = :etf_code
                                 AND trade_date = :trade_date
                               """)
                conn.execute(del_sql, {
                    "etf_code": etf_code,
                    "trade_date": target_date
                })
                conn.commit()

            df_res.to_sql('etf_iv_history', engine, if_exists='append', index=False)
            print(f" [√] 入库成功 (IV: {df_res['iv'].iloc[0]:.2f})")

        except Exception as e:
            print(f" [X] 数据库写入失败: {e}")
    else:
        print(f" [-] 无有效 IV 数据")

    # --- 优化点 3: 防止连续高频计算导致 CPU 持续 100% ---
    time.sleep(1)


if __name__ == "__main__":
    # 依次执行，每两个之间会有 time.sleep(1) 的休息时间
    etf_list = [
        "510050.SH",
        "510300.SH",
        "510500.SH",
        "159915.SZ",
        "588000.SH"
    ]

    for etf in etf_list:
        calculate_and_save_iv(etf)