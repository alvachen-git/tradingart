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
TARGETS = ['LC', 'SI', 'PS', 'pd', 'pt', 'IH', 'IF', 'IM', 'au', 'ag', 'cu', 'al', 'zn', 'ni', 'sn',
           'rb', 'i', 'sm', 'sf', 'fg', 'sa', 'm', 'a', 'b', 'rm', 'y', 'oi', 'p', 'ta', 'pr', 'ma', 'v', 'eb', 'eg', 'l', 'pp',
           'ru','br','lg','lh','px',
           'c', 'cf', 'ap', 'cj', 'pk', 'jd', 'sr', 'ao', 'sh', 'ur', 'sp', 'fu', 'bu', 'sc']


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
    try:
        m = re.search(r"(\d+)", fut_code)
        if not m: return None
        num_part = m.group(1)

        zce_products = ('CF', 'SR', 'TA','PX','PR','MA', 'RM', 'OI', 'SH', 'FG', 'SA', 'PF', 'PK', 'SM', 'SF', 'UR', 'AP', 'CJ')
        if len(num_part) == 4 and fut_code.upper().startswith(zce_products):
            opt_num = num_part[1:]
        else:
            opt_num = num_part

        prefix_upper = opt_prefix.upper()

        sql_opt = text(f"""
            SELECT ts_code, close, vol,
                   (SELECT exercise_price FROM commodity_option_basic WHERE ts_code = a.ts_code) as k,
                   (SELECT maturity_date FROM commodity_option_basic WHERE ts_code = a.ts_code) as expiry,
                   (SELECT call_put FROM commodity_option_basic WHERE ts_code = a.ts_code) as call_put
            FROM commodity_opt_daily a
            WHERE trade_date = '{date}'
              AND ts_code LIKE '{prefix_upper}{opt_num}%%'
              AND oi > 50 
        """)

        with engine.connect() as conn:
            opts = pd.read_sql(sql_opt, conn)

        if opts.empty: return None

        valid_ivs = []
        for _, row in opts.iterrows():
            try:
                if row['close'] <= 0 or row['vol'] <= 0: continue
                price = row['close']
                K = row['k']
                expiry = str(int(row['expiry']))
                cp = row['call_put'].lower()

                days_left = (pd.to_datetime(expiry) - pd.to_datetime(date)).days
                T = days_left / 365.0

                if days_left <= 2: continue

                if days_left < 10:
                    threshold = 0.04
                else:
                    threshold = 0.08

                if not (S * (1 - threshold) < K < S * (1 + threshold)): continue

                iv = vectorized_implied_volatility(price, S, K, T, 0.02, cp, return_as='numpy')
                if isinstance(iv, np.ndarray): iv = iv.item()

                if not np.isnan(iv) and 0.01 < iv < 1.5:
                    valid_ivs.append(iv)
            except:
                continue

        if valid_ivs:
            return np.median(valid_ivs) * 100
    except:
        pass
    return None


def process_daily_commodity(symbol, target_date):
    # 1. 强制转大写，防止 'cu' vs 'CU' 问题
    symbol = symbol.upper()
    print(f"[*] 处理 {symbol} ({target_date})...")

    # 2. 【数据清洗】幂等性删除
    # 说明：在计算前，先把数据库里这一天、这个品种的所有数据（包括分合约和主连）删掉。
    # 正则解释：^{symbol}([0-9]|$) 匹配 "RU2405" 或 "RU"，但不匹配 "RUBY"
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
    #  A. 分合约计算 (遍历每个具体的期货合约，如 RU2505, RU2509)
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

    # 🛑 修正点：注意这里！缩进退回最左边，不要放在上面的 for 循环里！

    # =======================================================
    #  B. 主力/主连计算 (针对整个品种只算一次，如 RU)
    # =======================================================
    if not df_fut.empty:
        # 按持仓量降序排列
        df_fut_sorted = df_fut.sort_values('oi', ascending=False)

        # 尝试前 3 个持仓最大的合约，直到找到一个能算出 IV 的
        # 解决问题：当月合约因临近交割被过滤(days_left<=2)，导致主力IV缺失
        for i in range(min(3, len(df_fut_sorted))):
            dom_row = df_fut_sorted.iloc[i]
            real_contract = dom_row['ts_code'].upper()
            S_dom = dom_row['close_price']

            if S_dom > 0:
                # 尝试计算
                iv_dom = calc_iv_core(target_date, S_dom, 0, real_contract, opt_prefix)

                # 如果算出来了，就认定它是当前可参考的主力 IV，并退出循环
                if iv_dom:
                    data_to_insert.append({
                        'trade_date': target_date,
                        'ts_code': symbol,  # 存为通用代码 (如 IF, RU)
                        'iv': iv_dom,
                        'hv': 0,
                        'used_contract': real_contract  # 记录实际用的是哪个合约
                    })
                    print(f"   [主力修正] {symbol} 使用 {real_contract} (排名第{i + 1})")
                    break  # 找到一个就够了，跳出循环

    # --- C. 入库 ---
    if data_to_insert:
        # 双重保险：在 Python 层面去重，防止 list 里有重复的主键
        # 以 ts_code 为基准去重，保留最后一个（或第一个）
        df_save = pd.DataFrame(data_to_insert)
        df_save.drop_duplicates(subset=['ts_code'], keep='last', inplace=True)

        if 'used_contract' not in df_save.columns:
            df_save['used_contract'] = None

        try:
            df_save.to_sql('commodity_iv_history', engine, if_exists='append', index=False)
            print(f"   -> 入库 {len(df_save)} 条")
        except Exception as e:
            # 捕获入库时的唯一键冲突，打印更详细的错误
            print(f"   [!] 入库失败 (可能是脏数据未删净): {e}")
    else:
        print(f"   -> 无有效数据")


if __name__ == "__main__":
    now = datetime.datetime.now()

    # 循环回溯3天
    print(f"=== 🔄 开始更新最近3天 IV 数据 ===")
    for i in range(2):
        calc_date = now - datetime.timedelta(days=i)
        target_date = calc_date.strftime('%Y%m%d')

        if calc_date.weekday() >= 5:
            print(f"[-] {target_date} 是周末，跳过。")
            continue

        print(f"\n>>> 📅 正在处理日期: {target_date}")
        for t in TARGETS:
            try:
                process_daily_commodity(t, target_date)
            except Exception as e:
                print(f"   [!] {t} 异常: {e}")

    print("\n=== ✅ 更新任务全部完成 ===")