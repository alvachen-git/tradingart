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

# 1. 初始化配置 (适配服务器路径)
load_dotenv(override=True)

# 使用 pool_recycle 防止 MySQL 连接超时断开
db_url = f"mysql+pymysql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
engine = create_engine(db_url, pool_recycle=3600)

# 映射配置
SPECIAL_MAPPING = {'IO': 'IF', 'MO': 'IM', 'HO': 'IH'}
FUT_TO_OPT = {v: k for k, v in SPECIAL_MAPPING.items()}

# 目标品种 (与重算脚本保持一致)
TARGETS = ['LC', 'SI', 'PS','pd', 'pt','IH', 'IF', 'IM', 'au', 'ag', 'cu', 'al', 'zn', 'ni','sn',
            'rb', 'i','sm','fg', 'sa', 'm', 'a','b','rm', 'y', 'oi', 'p', 'ta', 'ma', 'v', 'eb', 'eg','l','pp','ru',
            'c', 'cf', 'ap','cj','pk','jd','sr', 'ao', 'sh','ur', 'sp','fu', 'bu', 'sc']


def get_opt_prefix(fut_symbol):
    if fut_symbol.upper() in FUT_TO_OPT:
        return FUT_TO_OPT[fut_symbol.upper()]
    return fut_symbol


def get_realtime_hv(ts_code, current_date, window=20):
    """
    【内存优化】只从数据库拉取该合约最近 30 条记录来计算当天的 HV
    """
    try:
        # 往前多取一点，确保由足够的交易日
        lookback_date = (pd.to_datetime(current_date) - datetime.timedelta(days=60)).strftime('%Y%m%d')

        sql = text(f"""
            SELECT close_price 
            FROM futures_price 
            WHERE ts_code = :code AND trade_date <= :date AND trade_date >= :lookback
            ORDER BY trade_date ASC
        """)

        df = pd.read_sql(sql, engine, params={"code": ts_code, "date": current_date, "lookback": lookback_date})

        if len(df) < window + 1: return 0

        # 计算对数收益率
        p = df['close_price'].replace(0, np.nan).dropna()
        if len(p) < window + 1: return 0

        log_ret = np.log(p / p.shift(1))

        # 取最后 window 天的标准差
        hv = log_ret.tail(window).std() * np.sqrt(252) * 100
        return hv if not pd.isna(hv) else 0
    except:
        return 0


def calc_iv_core(date, S, HV, fut_code, opt_prefix):
    """
    核心 IV 计算 (保持与重算脚本一致的最新逻辑)
    """
    try:
        m = re.search(r"(\d+)", fut_code)
        if not m: return None
        num_part = m.group(1)

        zce_products = ('CF', 'SR', 'TA', 'MA', 'RM', 'OI', 'SH', 'FG', 'SA', 'PF', 'PK', 'SM', 'SF', 'UR', 'AP', 'CJ')
        if len(num_part) == 4 and fut_code.upper().startswith(zce_products):
            opt_num = num_part[1:]
        else:
            opt_num = num_part

        # 查期权
        sql_opt = text(f"""
            SELECT ts_code, close, vol,
                   (SELECT exercise_price FROM commodity_option_basic WHERE ts_code = a.ts_code) as k,
                   (SELECT maturity_date FROM commodity_option_basic WHERE ts_code = a.ts_code) as expiry,
                   (SELECT call_put FROM commodity_option_basic WHERE ts_code = a.ts_code) as call_put
            FROM commodity_opt_daily a
            WHERE trade_date = '{date}'
              AND ts_code LIKE '{opt_prefix}{opt_num}%%'
              AND oi > 50 
        """)

        with engine.connect() as conn:
            opts = pd.read_sql(sql_opt, conn)

        if opts.empty: return None

        valid_ivs = []
        for _, row in opts.iterrows():
            try:
                # 1. 严格使用收盘价
                if row['close'] <= 0 or row['vol'] <= 0: continue
                price = row['close']

                K = row['k']
                expiry = str(int(row['expiry']))
                cp = row['call_put'].lower()

                days_left = (pd.to_datetime(expiry) - pd.to_datetime(date)).days
                T = days_left / 365.0

                if days_left <= 2: continue  # 极度临近不予计算

                # 2. 动态阈值筛选
                if days_left < 10:
                    threshold = 0.03  # 快到期：±2%
                else:
                    threshold = 0.06  # 远月：±5%

                if not (S * (1 - threshold) < K < S * (1 + threshold)): continue

                # 3. Black-76 (r=0)
                iv = vectorized_implied_volatility(price, S, K, T, 0.0, cp, return_as='numpy')
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
    """
    处理单个品种的单日数据 (分合约 + 主连)
    """
    print(f"[*] 处理 {symbol} ({target_date})...")

    # 1. 幂等性：先删除该品种今日已有的 IV 数据
    with engine.connect() as conn:
        conn.execute(
            text(f"DELETE FROM commodity_iv_history WHERE trade_date='{target_date}' AND ts_code LIKE '{symbol}%'"))
        conn.commit()

    # 2. 获取该品种今日期货行情
    sql_fut = f"""
        SELECT ts_code, close_price, oi
        FROM futures_price
        WHERE trade_date = '{target_date}'
          AND (ts_code LIKE '{symbol}%%' OR ts_code LIKE '{symbol.lower()}%%')
          AND LENGTH(ts_code) > {len(symbol)} 
    """
    try:
        df_fut = pd.read_sql(sql_fut, engine)
    except:
        return

    if df_fut.empty: return

    # 严格过滤
    df_fut['prefix'] = df_fut['ts_code'].str.extract(r'^([a-zA-Z]+)')
    df_fut = df_fut[df_fut['prefix'].str.upper() == symbol.upper()]
    if df_fut.empty: return

    opt_prefix = get_opt_prefix(symbol)
    data_to_insert = []

    # --- A. 计算所有分合约 ---
    for _, row in df_fut.iterrows():
        fut_code = row['ts_code']
        S_close = row['close_price']

        # 严格校验：无收盘价不计算
        if S_close <= 0 or pd.isna(S_close): continue

        # 回溯计算 HV
        hv_val = get_realtime_hv(fut_code, target_date)

        # 计算 IV
        iv = calc_iv_core(target_date, S_close, 0, fut_code, opt_prefix)

        if iv:
            data_to_insert.append({
                'trade_date': target_date,
                'ts_code': fut_code.upper(),
                'iv': iv,
                'hv': hv_val
            })

    # --- B. 计算主力连续 ---
    # 找今日 OI 最大的合约
    if not df_fut.empty:
        dom_row = df_fut.sort_values('oi', ascending=False).iloc[0]
        real_contract = dom_row['ts_code']
        S_dom = dom_row['close_price']

        if S_dom > 0:
            iv_dom = calc_iv_core(target_date, S_dom, 0, real_contract, opt_prefix)
            if iv_dom:
                data_to_insert.append({
                    'trade_date': target_date,
                    'ts_code': symbol.upper(),
                    'iv': iv_dom,
                    'hv': 0,  # 主连 HV 暂记为 0
                    'used_contract': real_contract
                })

    # --- C. 批量入库 ---
    if data_to_insert:
        df_save = pd.DataFrame(data_to_insert)
        df_save.to_sql('commodity_iv_history', engine, if_exists='append', index=False)
        print(f"   -> 入库 {len(df_save)} 条")
    else:
        print(f"   -> 无有效数据")


if __name__ == "__main__":
    # 获取今天日期
    now = datetime.datetime.now()

    # 1. 周末检查
    if now.weekday() >= 5:
        print(f"[-] 今天是周末 ({now.strftime('%Y%m%d')})，跳过。")
        sys.exit(0)

    # 2. 确定计算日期 (通常是当天)
    # 如果您是在凌晨跑昨天的，可以用 now - timedelta(days=1)
    target_date = now.strftime('%Y%m%d')

    # 3. 循环处理
    print(f">>> 启动每日 IV 更新: {target_date}")
    for t in TARGETS:
        try:
            process_daily_commodity(t, target_date)
        except Exception as e:
            print(f"   [!] {t} 异常: {e}")

    print("=== ✅ 更新完成 ===")