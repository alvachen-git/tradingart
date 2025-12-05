import pandas as pd
import tushare as ts
from sqlalchemy import create_engine, text, types
import os
import re
from dotenv import load_dotenv
import time
from datetime import datetime, timedelta

# --- 1. 初始化配置 ---
load_dotenv(override=True)

# 数据库配置
DB_USER = 'root'
DB_PASSWORD = 'alva13557941'
DB_HOST = '39.102.215.198'
DB_PORT = '3306'
DB_NAME = 'finance_data'

db_url = f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(db_url)

# Tushare 配置
token = os.getenv("TUSHARE_TOKEN")
if not token:
    print("❌ 错误：未找到 TUSHARE_TOKEN，请在 .env 文件中配置。")
    exit()

ts.set_token(token)
pro = ts.pro_api()


# --- 2. 核心逻辑：获取、清洗、筛选字段 ---
def fetch_and_save_tushare(date_str, exchange):
    """
    exchange: GFE(广期), DCE(大商), CZCE(郑商), SHFE(上期), CFFEX(中金)
    """
    print(f"[*] 正在请求 Tushare [{exchange}] {date_str} ...", end="")

    try:
        # 1. 调用接口
        df = pro.fut_holding(trade_date=date_str, exchange=exchange)

        if df.empty:
            print(" [-] 无数据")
            return

        # 2. 数据预处理
        # 提取品种代码 (如 RB2501 -> rb)
        df['ts_code'] = df['symbol'].apply(lambda x: re.sub(r'\d+', '', x).lower().strip())

        # 确保数值列是数字
        num_cols = ['long_hld', 'long_chg', 'short_hld', 'short_chg']
        for c in num_cols:
            df[c] = pd.to_numeric(df[c], errors='coerce').fillna(0)

        # 3. 聚合 (Group By)
        # 将同一品种下所有合约的数据加总
        # 注意：我们只聚合需要的列，故意忽略 vol (成交量)
        df_agg = df.groupby(['trade_date', 'ts_code', 'broker'])[num_cols].sum().reset_index()

        # 4. 重命名 (匹配数据库字段名)
        # Tushare名 -> 数据库名
        df_agg = df_agg.rename(columns={
            'long_hld': 'long_vol',
            'long_chg': 'long_chg',
            'short_hld': 'short_vol',
            'short_chg': 'short_chg'
        })

        # 5. 计算净持仓
        df_agg['net_vol'] = df_agg['long_vol'] - df_agg['short_vol']

        # --- 【核心修正】字段白名单过滤 ---
        # 您的数据库只接受这 8 个字段，多余的字段(如 vol)会导致报错
        # 我们在这里强制只取这 8 列
        db_columns = [
            'trade_date',
            'ts_code',
            'broker',
            'long_vol',
            'long_chg',
            'short_vol',
            'short_chg',
            'net_vol'
        ]

        # 筛选数据
        df_final = df_agg[db_columns].copy()

        # 6. 入库
        save_to_db(df_final, date_str)

    except Exception as e:
        print(f" [!] 异常: {e}")


def save_to_db(df, date_str):
    if df.empty: return
    try:
        # 获取本次涉及的品种列表
        symbols = df['ts_code'].unique().tolist()
        symbols_str = "', '".join(symbols)

        with engine.connect() as conn:
            # 覆盖逻辑：先删除当天、这些品种的旧数据
            sql = f"DELETE FROM futures_holding WHERE trade_date='{date_str}' AND ts_code IN ('{symbols_str}')"
            conn.execute(text(sql))
            conn.commit()

        # 写入
        df.to_sql('futures_holding', engine, if_exists='append', index=False)
        print(f" [√] 入库成功 ({len(df)}条)")

    except Exception as e:
        print(f" [X] 数据库写入失败: {e}")


# --- 3. 批量运行 ---
def run_job(start_date, end_date):
    dates = pd.date_range(start=start_date, end=end_date)

    # Tushare 官方标准交易所代码
    EXCHANGES = [
        'GFEX',  # 广期所

    ]

    for single_date in dates:
        date_str = single_date.strftime('%Y%m%d')
        if single_date.weekday() >= 5: continue  # 跳过周末

        print(f"\n--- 处理日期: {date_str} ---")
        for ex in EXCHANGES:
            fetch_and_save_tushare(date_str, ex)
            # Tushare 限制每分钟访问次数，稍微停顿
            time.sleep(0.4)


if __name__ == "__main__":
    # 自动补全最近 5 天的数据
    today = datetime.now().strftime('%Y%m%d')
    start = (datetime.now() - timedelta(days=200)).strftime('%Y%m%d')

    # 或者您可以手动指定日期测试
    # start = '20251114'
    # today = '20251114'

    run_job(start, today)