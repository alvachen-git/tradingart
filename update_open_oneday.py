import pandas as pd
import tushare as ts
from sqlalchemy import create_engine, text, types
import os
import re
from dotenv import load_dotenv
import time
from datetime import datetime, timedelta
import gc  # <--- 1. 新增：引入垃圾回收模块

# --- 1. 初始化配置 ---
load_dotenv(override=True)

# 数据库配置
DB_USER = 'root'
DB_PASSWORD = 'alva13557941'  # 建议也放入 .env 文件
DB_HOST = '39.102.215.198'
DB_PORT = '3306'
DB_NAME = 'finance_data'

db_url = f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
# 增加 pool_recycle 防止数据库连接超时断开
engine = create_engine(db_url, pool_recycle=3600)

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
        df['ts_code'] = df['symbol'].apply(lambda x: re.sub(r'\d+', '', x).lower().strip())

        num_cols = ['long_hld', 'long_chg', 'short_hld', 'short_chg']
        for c in num_cols:
            df[c] = pd.to_numeric(df[c], errors='coerce').fillna(0)

        # 3. 聚合
        df_agg = df.groupby(['trade_date', 'ts_code', 'broker'])[num_cols].sum().reset_index()

        # --- 2. 优化：原始 df 已经没用了，立即删除并回收内存 ---
        del df
        gc.collect()

        # 4. 重命名
        df_agg = df_agg.rename(columns={
            'long_hld': 'long_vol',
            'long_chg': 'long_chg',
            'short_hld': 'short_vol',
            'short_chg': 'short_chg'
        })

        # 5. 计算净持仓
        df_agg['net_vol'] = df_agg['long_vol'] - df_agg['short_vol']

        db_columns = [
            'trade_date', 'ts_code', 'broker',
            'long_vol', 'long_chg', 'short_vol', 'short_chg', 'net_vol'
        ]

        df_final = df_agg[db_columns].copy()

        # --- 3. 优化：df_agg 也没用了，再次释放 ---
        del df_agg
        gc.collect()

        # 6. 入库
        save_to_db(df_final, date_str)

        # 最后再清理一次 df_final
        del df_final
        gc.collect()

    except Exception as e:
        print(f" [!] 异常: {e}")


def save_to_db(df, date_str):
    if df.empty: return
    try:
        symbols = df['ts_code'].unique().tolist()
        symbols_str = "', '".join(symbols)

        with engine.connect() as conn:
            # 先删除旧数据
            sql = f"DELETE FROM futures_holding WHERE trade_date='{date_str}' AND ts_code IN ('{symbols_str}')"
            conn.execute(text(sql))
            conn.commit()

        # --- 4. 核心优化：手动分批写入 + 强制休眠 ---
        # 你的服务器只有2G内存，这里必须切得很细，给Web服务留喘息时间

        batch_size = 1000  # 每次只写入 1000 条
        total_len = len(df)
        print(f" [Saving {total_len} rows] ", end="")

        for i in range(0, total_len, batch_size):
            # 切片
            chunk = df.iloc[i: i + batch_size]

            # 写入数据库
            chunk.to_sql('futures_holding', engine, if_exists='append', index=False)

            # 打印进度点
            print(".", end="", flush=True)

            # 关键：每写 1000 条，强制睡 0.5 秒
            # 这就是防止网站 502 的关键，把 CPU 让给 Nginx
            time.sleep(0.5)

            # 清理这一小块的内存
            del chunk
            gc.collect()

        print(f" [√] 完成")

    except Exception as e:
        print(f" [X] 数据库写入失败: {e}")


# --- 3. 批量运行 ---
def run_job(start_date, end_date):
    dates = pd.date_range(start=start_date, end=end_date)

    EXCHANGES = ['GFEX', 'SHFE', 'DCE', 'CZCE', 'CFFEX']

    for single_date in dates:
        date_str = single_date.strftime('%Y%m%d')
        if single_date.weekday() >= 5: continue

        print(f"\n--- 处理日期: {date_str} ---")
        for ex in EXCHANGES:
            fetch_and_save_tushare(date_str, ex)

            # 处理完一个交易所后，再休息一下
            time.sleep(1)


if __name__ == "__main__":
    today = datetime.now().strftime('%Y%m%d')
    start = (datetime.now() - timedelta(days=1)).strftime('%Y%m%d')

    # start = '20251114' # 调试用

    print(f"开始任务: {start} -> {today}")
    run_job(start, today)