import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
import os
from dotenv import load_dotenv
from py_vollib_vectorized import vectorized_implied_volatility
import warnings
import re
import datetime
import time
import sys

warnings.simplefilter("ignore")

# 1. 初始化配置
load_dotenv(override=True)

db_url = f"mysql+pymysql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
engine = create_engine(db_url, pool_recycle=3600)

# 映射配置
SPECIAL_MAPPING = {'IO': 'IF', 'MO': 'IM', 'HO': 'IH'}
FUT_TO_OPT = {v: k for k, v in SPECIAL_MAPPING.items()}

# 目标品种
TARGETS = ['jm']


def get_opt_prefix(fut_symbol):
    sym_upper = fut_symbol.upper()
    if sym_upper in FUT_TO_OPT:
        return FUT_TO_OPT[sym_upper]
    return sym_upper


def get_realtime_hv(ts_code, current_date, window=20):
    try:
        ts_code = ts_code.upper()
        lookback_date = (pd.to_datetime(current_date) - datetime.timedelta(days=60)).strftime('%Y%m%d')

        sql = text(f"""
            SELECT close_price 
            FROM futures_price 
            WHERE ts_code = :code AND trade_date <= :date AND trade_date >= :lookback
            ORDER BY trade_date ASC
        """)

        df = pd.read_sql(sql, engine, params={"code": ts_code, "date": current_date, "lookback": lookback_date})

        if len(df) < window + 1: return 0
        p = df['close_price'].replace(0, np.nan).dropna()
        if len(p) < window + 1: return 0

        log_ret = np.log(p / p.shift(1))
        hv = log_ret.tail(window).std() * np.sqrt(252) * 100
        return hv if not pd.isna(hv) else 0
    except:
        return 0


def calc_iv_core(date, S, HV, fut_code, opt_prefix):
    """
    核心 IV 计算函数 (已优化：增强对低流动性品种 LC/AG 的支持)
    """
    try:
        m = re.search(r"(\d+)", fut_code)
        if not m: return None
        num_part = m.group(1)

        zce_products = ('CF', 'SR', 'TA', 'PX', 'PR', 'MA', 'RM', 'OI', 'SH', 'FG', 'SA', 'PF', 'PK', 'SM', 'SF', 'UR',
                        'AP', 'CJ')
        if len(num_part) == 4 and fut_code.upper().startswith(zce_products):
            opt_num = num_part[1:]
        else:
            opt_num = num_part

        prefix_upper = opt_prefix.upper()

        # 【优化1】增加查询 settle (结算价)，并放宽 oi 限制
        sql_opt = text(f"""
            SELECT ts_code, close, vol, settle,
                   (SELECT exercise_price FROM commodity_option_basic WHERE ts_code = a.ts_code) as k,
                   (SELECT maturity_date FROM commodity_option_basic WHERE ts_code = a.ts_code) as expiry,
                   (SELECT call_put FROM commodity_option_basic WHERE ts_code = a.ts_code) as call_put
            FROM commodity_opt_daily a
            WHERE trade_date = '{date}'
              AND ts_code LIKE '{prefix_upper}{opt_num}%%'
              AND oi > 0 
        """)

        with engine.connect() as conn:
            opts = pd.read_sql(sql_opt, conn)

        if opts.empty: return None

        valid_ivs = []
        for _, row in opts.iterrows():
            try:
                # 【优化2】优先使用结算价，若无结算价则用收盘价
                # 并且去掉了 row['vol'] <= 0 的限制，解决 LC/AG 无成交算不出 IV 的问题
                price = row['settle'] if (row.get('settle') and row['settle'] > 0) else row['close']

                if price <= 0: continue

                # 【优化3】增加空值检查，防止 Basic 表缺失导致崩溃
                if pd.isna(row['k']) or pd.isna(row['expiry']):
                    # print(f"Warning: {row['ts_code']} 缺少行权价或到期日")
                    continue

                K = float(row['k'])
                expiry = str(int(row['expiry']))
                cp = row['call_put'].lower()

                days_left = (pd.to_datetime(expiry) - pd.to_datetime(date)).days
                T = days_left / 365.0

                if days_left <= 2: continue

                # 【优化4】放宽虚值实值判断范围
                # 原来 0.05 (5%) 对碳酸锂这种波动大的品种太严苛了，容易过滤掉所有期权
                if days_left < 10:
                    threshold = 0.02  # 临近到期 5%
                else:
                    threshold = 0.05  # 平时放宽到 15%

                if not (S * (1 - threshold) < K < S * (1 + threshold)): continue

                iv = vectorized_implied_volatility(price, S, K, T, 0.02, cp, return_as='numpy')
                if isinstance(iv, np.ndarray): iv = iv.item()

                if not np.isnan(iv) and 0.01 < iv < 2.0:  # 放宽上限到 200% (针对 LC)
                    valid_ivs.append(iv)
            except Exception as e:
                continue

        if valid_ivs:
            return np.median(valid_ivs) * 100
    except Exception as e:
        # print(f"Error in core: {e}")
        pass
    return None


def process_daily_commodity(symbol, target_date):
    # 1. 强制转大写
    symbol = symbol.upper()
    print(f"[*] 处理 {symbol} ({target_date})...")

    # 2. 【数据清洗】幂等性删除
    with engine.connect() as conn:
        sql_del = text(
            f"DELETE FROM commodity_iv_history WHERE trade_date='{target_date}' AND ts_code REGEXP '^{symbol}([0-9]|$)'")
        conn.execute(sql_del)
        conn.commit()

    # 3. 获取期货行情
    sql_fut = f"""
        SELECT ts_code, close_price, oi
        FROM futures_price
        WHERE trade_date = '{target_date}'
          AND (ts_code LIKE '{symbol}%%') 
          AND LENGTH(ts_code) > {len(symbol)} 
    """
    try:
        df_fut = pd.read_sql(sql_fut, engine)
    except:
        return

    if df_fut.empty: return

    df_fut['prefix'] = df_fut['ts_code'].str.extract(r'^([a-zA-Z]+)')
    df_fut = df_fut[df_fut['prefix'].str.upper() == symbol]
    if df_fut.empty: return

    opt_prefix = get_opt_prefix(symbol)
    data_to_insert = []

    # =======================================================
    #  A. 分合约计算
    # =======================================================
    for _, row in df_fut.iterrows():
        fut_code = row['ts_code'].upper()
        S_close = row['close_price']

        if S_close <= 0 or pd.isna(S_close): continue

        hv_val = get_realtime_hv(fut_code, target_date)
        iv = calc_iv_core(target_date, S_close, 0, fut_code, opt_prefix)

        if iv:
            data_to_insert.append({
                'trade_date': target_date,
                'ts_code': fut_code,
                'iv': iv,
                'hv': hv_val
            })

    # =======================================================
    #  B. 主力/主连计算
    # =======================================================
    if not df_fut.empty:
        df_fut_sorted = df_fut.sort_values('oi', ascending=False)

        # 尝试前 3 个持仓最大的合约
        for i in range(min(3, len(df_fut_sorted))):
            dom_row = df_fut_sorted.iloc[i]
            real_contract = dom_row['ts_code'].upper()
            S_dom = dom_row['close_price']

            if S_dom > 0:
                iv_dom = calc_iv_core(target_date, S_dom, 0, real_contract, opt_prefix)

                if iv_dom:
                    data_to_insert.append({
                        'trade_date': target_date,
                        'ts_code': symbol,
                        'iv': iv_dom,
                        'hv': 0,
                        'used_contract': real_contract
                    })
                    print(f"   [主力修正] {symbol} 使用 {real_contract} (排名第{i + 1}) IV={iv_dom:.2f}")
                    break

                    # --- C. 入库 ---
    if data_to_insert:
        df_save = pd.DataFrame(data_to_insert)
        df_save.drop_duplicates(subset=['ts_code'], keep='last', inplace=True)
        if 'used_contract' not in df_save.columns:
            df_save['used_contract'] = None

        try:
            df_save.to_sql('commodity_iv_history', engine, if_exists='append', index=False)
            print(f"   -> 入库 {len(df_save)} 条")
        except Exception as e:
            print(f"   [!] 入库失败: {e}")
    else:
        print(f"   -> 无有效数据")


# ==========================================
#  主程序入口：可指定日期范围
# ==========================================
if __name__ == "__main__":

    # 🔴 在这里配置你想计算的日期区间 (格式 YYYYMMDD)
    START_DATE = "20251201"
    END_DATE = "20260301"

    # 自动生成日期序列
    date_range = pd.date_range(start=START_DATE, end=END_DATE)

    print(f"=== 🔄 开始更新区间 IV 数据: {START_DATE} 至 {END_DATE} ===")

    for date_obj in date_range:
        target_date = date_obj.strftime('%Y%m%d')

        # 跳过周末
        if date_obj.weekday() >= 5:
            print(f"[-] {target_date} 是周末，跳过。")
            continue

        print(f"\n>>> 📅 正在处理日期: {target_date}")

        for t in TARGETS:
            try:
                process_daily_commodity(t, target_date)
            except Exception as e:
                print(f"   [!] {t} 异常: {e}")

    print("\n=== ✅ 区间更新任务全部完成 ===")
