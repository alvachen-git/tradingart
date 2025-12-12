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

warnings.simplefilter("ignore")
load_dotenv(override=True)

# 1. 数据库连接
if not os.getenv("DB_USER"):
    raise ValueError("❌ 未找到数据库配置，请检查 .env 文件")

db_url = f"mysql+pymysql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
engine = create_engine(db_url)

# 2. 映射配置
SPECIAL_MAPPING = {'IO': 'IF', 'MO': 'IM', 'HO': 'IH'}
FUT_TO_OPT = {v: k for k, v in SPECIAL_MAPPING.items()}


def reset_iv_table():
    print("!!! 正在重置 IV 数据表 (TRUNCATE) ...")
    time.sleep(2)
    with engine.connect() as conn:
        try:
            conn.execute(text("TRUNCATE TABLE commodity_iv_history"))
            print(" [√] 表已清空。")
        except:
            conn.execute(text("DELETE FROM commodity_iv_history"))
            print(" [√] 表已清空 (DELETE)。")


def get_opt_prefix(fut_symbol):
    if fut_symbol.upper() in FUT_TO_OPT:
        return FUT_TO_OPT[fut_symbol.upper()]
    return fut_symbol


def calc_iv_core(date, S, HV, fut_code, opt_prefix):
    """
    核心计算函数：只使用收盘价，取中位数
    """
    try:
        m = re.search(r"(\d+)", fut_code)
        if not m: return None
        num_part = m.group(1)

        # 郑商所处理
        zce_products = ('CF', 'SR', 'TA', 'MA', 'RM', 'OI', 'SH', 'FG', 'SA', 'PF', 'PK', 'SM', 'SF', 'UR', 'AP', 'CJ')
        if len(num_part) == 4 and fut_code.upper().startswith(zce_products):
            opt_num = num_part[1:]
        else:
            opt_num = num_part

        # 查期权数据 (只取 close 和 vol)
        # 过滤 oi > 50 保证基本活跃度
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
                # --- [修改 1] 强制使用收盘价 ---
                # 只有当收盘价 > 0 且 有成交量 (vol > 0) 时才计算
                # 既然您确认收盘价准确，这里我们严格依赖收盘价
                if row['close'] <= 0 or row['vol'] <= 0:
                    continue

                price = row['close']

                K = row['k']
                expiry = str(int(row['expiry']))
                cp = row['call_put'].lower()

                days_left = (pd.to_datetime(expiry) - pd.to_datetime(date)).days
                T = days_left / 365.0

                # [安全过滤] 极度临近到期 (<=2天) 不计算，模型失效风险极高
                if days_left <= 2: continue

                # --- [核心修改] 动态行权价筛选逻辑 ---
                if days_left < 10:
                    # 快到期 (3-9天)：极度收敛，只看 ±2%
                    threshold = 0.02
                else:
                    # 远月 (>=10天)：适度收敛，看 ±5%
                    threshold = 0.05

                # 执行筛选：只保留 S*(1-threshold) < K < S*(1+threshold)
                if not (S * (1 - threshold) < K < S * (1 + threshold)):
                    continue

                # --- 模型参数 ---
                # 商品期货期权近似 Black-76：r (无风险利率) 设为 0
                iv = vectorized_implied_volatility(price, S, K, T, 0.0, cp, return_as='numpy')
                if isinstance(iv, np.ndarray): iv = iv.item()

                if not np.isnan(iv) and 0.01 < iv < 1.0:
                    valid_ivs.append(iv)
            except:
                continue

        # --- [修改 2] 确认使用中位数 (Median) ---
        if valid_ivs:
            return np.median(valid_ivs) * 100

    except:
        pass
    return None


def process_contracts(symbol, df_source, opt_prefix, is_continuous=False):
    """
    通用处理逻辑
    """
    data_to_insert = []

    # 确保有 close_price 列
    if 'close_price' not in df_source.columns:
        df_source['close_price'] = df_source.get('close', 0)

    for _, row in df_source.iterrows():
        date = row['trade_date']

        if is_continuous:
            real_contract = row.get('real_contract')
            if not real_contract: continue
            save_code = symbol.upper()
        else:
            real_contract = row['ts_code']
            save_code = real_contract.upper()

        if not re.search(r'\d', real_contract): continue

        # --- [修改 1] 只使用期货收盘价 ---
        S_close = row['close_price']

        # 如果期货收盘价无效，直接跳过，绝不使用结算价
        if S_close <= 0 or pd.isna(S_close):
            continue

        # 计算 IV (传入期货收盘价)
        iv = calc_iv_core(date, S_close, 0, real_contract, opt_prefix)

        if iv:
            # 计算 HV (基于收盘价)
            hv_val = row.get('hv', 0)
            if pd.isna(hv_val): hv_val = 0

            entry = {
                'trade_date': date,
                'ts_code': save_code,
                'iv': iv,
                'hv': hv_val
            }
            if is_continuous:
                entry['used_contract'] = real_contract

            data_to_insert.append(entry)

    return data_to_insert


def calc_all_contracts(symbol, days=365):
    print(f"[*] 计算分合约: {symbol}...")

    # 查期货数据
    sql_contracts = f"""
        SELECT DISTINCT ts_code FROM futures_price 
        WHERE (ts_code LIKE '{symbol}%%' OR ts_code LIKE '{symbol.lower()}%%')
          AND trade_date >= DATE_FORMAT(DATE_SUB(NOW(), INTERVAL {days} DAY), '%%Y%%m%%d')
          AND LENGTH(ts_code) > {len(symbol)} 
    """
    try:
        contracts = pd.read_sql(sql_contracts, engine)['ts_code'].tolist()
    except:
        return

    opt_prefix = get_opt_prefix(symbol)

    real_contracts = []
    for c in contracts:
        match = re.match(r"^([a-zA-Z]+)(\d+)$", c)
        if match and match.group(1).upper() == symbol.upper():
            real_contracts.append(c)

    if not real_contracts: return

    for fut_code in real_contracts:
        df_fut = pd.read_sql(f"SELECT * FROM futures_price WHERE ts_code='{fut_code}' ORDER BY trade_date", engine)
        if df_fut.empty: continue

        # 计算 HV (只用 Close Price)
        p = df_fut['close_price'].replace(0, np.nan)
        df_fut['hv'] = np.log(p / p.shift(1)).rolling(20).std() * np.sqrt(252) * 100

        # 只取最近 days 天
        df_target = df_fut.tail(days).copy()

        data = process_contracts(symbol, df_target, opt_prefix, is_continuous=False)

        if data:
            df_save = pd.DataFrame(data)
            df_save.to_sql('commodity_iv_history', engine, if_exists='append', index=False)

        print(f"   -> {fut_code}: {len(data)} 条")


def calc_continuous_iv(symbol, days=365):
    """
    计算主力连续 IV (智能换月版：如果第一主力快到期，自动用第二主力)
    """
    print(f"[*] 计算主连: {symbol} (智能换月)...")
    start_date = (datetime.datetime.now() - datetime.timedelta(days=days)).strftime('%Y%m%d')

    # 1. 查出该品种所有分合约 (不只查主力，而是查所有，以便备选)
    sql_main = f"""
        SELECT trade_date, ts_code, close_price, oi
        FROM futures_price
        WHERE (ts_code LIKE '{symbol}%%' OR ts_code LIKE '{symbol.lower()}%%')
          AND trade_date >= '{start_date}'
          AND LENGTH(ts_code) > {len(symbol)} 
    """
    try:
        df_all = pd.read_sql(sql_main, engine)
    except:
        return
    if df_all.empty: return

    # 2. 严格过滤品种
    df_all['prefix'] = df_all['ts_code'].str.extract(r'^([a-zA-Z]+)')
    df_all = df_all[df_all['prefix'].str.upper() == symbol.upper()]
    if df_all.empty: return

    opt_prefix = get_opt_prefix(symbol)

    # 确保 close_price 存在
    if 'close_price' not in df_all.columns:
        df_all['close_price'] = df_all.get('close', 0)

    data_to_insert = []

    # 3. 按日期分组处理
    # 每天可能有多个合约，我们按持仓量(OI)降序排列，依次尝试
    for date, group in df_all.groupby('trade_date'):
        # 按 OI 降序排列 (Candidate 1, Candidate 2...)
        candidates = group.sort_values('oi', ascending=False)

        found_valid = False

        for _, row in candidates.iterrows():
            real_contract = row['ts_code']
            S = row['close_price']

            if S <= 0 or pd.isna(S): continue

            # 尝试计算 IV
            # 注意：calc_iv_core 内部有 <10天 的过滤逻辑
            # 如果该合约快到期，calc_iv_core 会返回 None
            iv = calc_iv_core(date, S, 0, real_contract, opt_prefix)

            if iv:
                # 成功算出来了！说明这个合约既是主力(或次主力)，又没有快到期
                data_to_insert.append({
                    'trade_date': date,
                    'ts_code': symbol.upper(),  # 存为主连代码
                    'iv': iv,
                    'hv': 0,
                    'used_contract': real_contract
                })
                found_valid = True
                break  # 只要找到一个能用的，今天就结束，跳出循环

        # if not found_valid:
        #     print(f"警告: {date} {symbol} 所有合约都无法计算 (可能都快到期了)")

    # 4. 批量入库
    if data_to_insert:
        df_save = pd.DataFrame(data_to_insert)
        df_save.to_sql('commodity_iv_history', engine, if_exists='append', index=False)

    print(f"   -> {symbol}(主连): {len(data_to_insert)} 条 (智能修正)")


if __name__ == "__main__":
    reset_iv_table()

    # 您关注的品种，可按需增减
    targets = ['LC', 'SI', 'PS','pd', 'pt','IH', 'IF', 'IM', 'au', 'ag', 'cu', 'al', 'zn', 'ni','sn',
            'rb', 'i','sm','fg', 'sa', 'm', 'a','b','rm', 'y', 'oi', 'p', 'ta', 'ma', 'v', 'eb', 'eg','l','pp','ru',
            'c', 'cf', 'ap','cj','pk','jd','sr', 'ao', 'sh','ur','fu', 'bu', 'sc']

    print(f">>> 启动 IV 深度清洗与重算 ({len(targets)} 品种)...")

    for t in targets:
        calc_all_contracts(t, days=300)
        calc_continuous_iv(t, days=300)

    print("\n[√] 任务完成！")