import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
import os
from dotenv import load_dotenv
from py_vollib_vectorized import vectorized_implied_volatility
from datetime import datetime

# 1. 初始化
load_dotenv(override=True)
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")

db_url = f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(db_url)


def calculate_and_save_iv(etf_code="510050.SH", days=300):
    print(f"[*] 正在计算 {etf_code} 的波动率数据...")

    # A. 获取标的 ETF 价格 (S)
    # 计算 HV (历史波动率) 需要用到
    sql_stock = f"""
        SELECT trade_date, close_price 
        FROM stock_price 
        WHERE ts_code='{etf_code}' 
        ORDER BY trade_date DESC LIMIT {days + 60}
    """
    df_stock = pd.read_sql(sql_stock, engine).sort_values('trade_date')

    if df_stock.empty:
        print(f" [-] 未找到标的 {etf_code} 的价格数据")
        return

    # 计算 HV (20日历史波动率)
    # Log Returns
    df_stock['log_ret'] = np.log(df_stock['close_price'] / df_stock['close_price'].shift(1))
    # Rolling StdDev * sqrt(252)
    df_stock['hv'] = df_stock['log_ret'].rolling(window=20).std() * np.sqrt(252) * 100

    # 截取需要的日期范围
    cutoff_date = (datetime.now() - pd.Timedelta(days=days)).strftime('%Y%m%d')
    df_stock = df_stock[df_stock['trade_date'] >= cutoff_date].copy()

    # B. 计算 IV (隐含波动率)
    # 逻辑：每天找一个"平值期权"(ATM)来代表当天的 IV

    iv_results = []

    for idx, row in df_stock.iterrows():
        date = row['trade_date']
        S = row['close_price']  # 标的价格

        # 1. 找当天的期权数据
        # 关联 option_daily 和 option_basic
        # 筛选条件：属于该 ETF，且是当月或次月合约（流动性好），且行权价最接近 S

        # 先找出离 S 最近的行权价
        # 注意：为了速度，这里简化逻辑，直接查该日所有合约，然后在内存筛选
        sql_opt = f"""
            SELECT d.close as price, b.exercise_price as strike, b.call_put, b.delist_date
            FROM option_daily d
            JOIN option_basic b ON d.ts_code = b.ts_code
            WHERE b.underlying = '{etf_code}' 
              AND d.trade_date = '{date}'
              AND d.vol > 10  -- 只看有成交的
        """
        df_opt = pd.read_sql(sql_opt, engine)

        if df_opt.empty: continue

        # 2. 筛选平值合约 (ATM)
        # 计算每个合约行权价与 S 的差距
        df_opt['diff'] = abs(df_opt['strike'] - S)
        # 选出差距最小的（平值）
        min_diff = df_opt['diff'].min()
        # 稍微放宽一点范围，防止只有一个合约
        atm_opts = df_opt[df_opt['diff'] <= min_diff * 1.2].copy()

        if atm_opts.empty: continue

        # 3. 准备 Black-Scholes 参数
        # T: 剩余年数
        # 简单的 T 计算：(到期日 - 当前日期) / 365
        try:
            atm_opts['T'] = (pd.to_datetime(atm_opts['delist_date']) - pd.to_datetime(date)).dt.days / 365.0
        except:
            continue

        # 过滤快到期的（小于5天），因为 IV 会失真
        atm_opts = atm_opts[atm_opts['T'] > 0.01]
        if atm_opts.empty: continue

        # 4. 计算 IV
        # 利率 r 假定 2.5%
        r = 0.015

        try:
            # 向量化计算
            ivs = vectorized_implied_volatility(
                atm_opts['price'].values,
                S,
                atm_opts['strike'].values,
                atm_opts['T'].values,
                r,
                atm_opts['call_put'].str.lower().values,  # 'c' or 'p'
                return_as='numpy'
            )
            # 过滤无效值 (NaN 或 0)
            valid_ivs = ivs[~np.isnan(ivs) & (ivs > 0)]

            if len(valid_ivs) > 0:
                # 取平均值作为当天的 IV 指数
                avg_iv = np.mean(valid_ivs) * 100  # 转为百分比
                iv_results.append({
                    'trade_date': date,
                    'etf_code': etf_code,
                    'iv': avg_iv,
                    'hv': row['hv']
                })
        except Exception as e:
            print(f"计算出错 {date}: {e}")
            continue

    # C. 入库
    if iv_results:
        df_res = pd.DataFrame(iv_results)
        # 存入
        with engine.connect() as conn:
            del_sql = text(f"DELETE FROM etf_iv_history WHERE etf_code='{etf_code}'")
            conn.execute(del_sql)
            conn.commit()

        df_res.to_sql('etf_iv_history', engine, if_exists='append', index=False)
        print(f"[√] {etf_code} 波动率计算完成，存入 {len(df_res)} 条")


if __name__ == "__main__":
    # 计算 50ETF 和 300ETF
    calculate_and_save_iv("510050.SH")
    calculate_and_save_iv("510300.SH")
    calculate_and_save_iv("510500.SH")
    calculate_and_save_iv("159915.SZ")
    calculate_and_save_iv("588000.SH")