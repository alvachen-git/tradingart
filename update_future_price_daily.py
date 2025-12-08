import tushare as ts
import pandas as pd
from sqlalchemy import create_engine, text
import os
from dotenv import load_dotenv
import time
from datetime import datetime
import gc  # 引入垃圾回收模块
import warnings

# 忽略 SQLAlchemy 的一些警告
warnings.filterwarnings('ignore')

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

# 交易所列表
EXCHANGES = ['SHFE', 'DCE', 'CZCE', 'CFFEX', 'GFEX', 'INE']


def is_trading_day(date_str):
    """检查是否是交易日，避免非交易日空跑浪费资源"""
    try:
        # 查上期所的日历即可代表全市场
        df = pro.trade_cal(exchange='SHFE', start_date=date_str, end_date=date_str)
        if not df.empty and df.iloc[0]['is_open'] == 1:
            return True
        return False
    except:
        # 如果接口报错，为了保险起见，假设它是交易日，让程序尝试去抓数据
        return True


def save_daily_all_contracts(trade_date):
    """抓取某一天全市场所有合约的日线 (低内存版)"""
    print(f"[*] [Daily Job] 正在启动 {trade_date} 全市场期货更新...")

    # 1. 交易日检查 (省流第一步)
    if not is_trading_day(trade_date):
        print(f" [-] {trade_date} 是非交易日，任务跳过。")
        return

    start_time = time.time()
    total_count = 0

    for ex in EXCHANGES:
        try:
            # 获取数据
            df = pro.fut_daily(trade_date=trade_date, exchange=ex)

            if df.empty:
                time.sleep(0.5)
                continue

            # --- 数据清洗 ---
            # 去除后缀 (rb2505.SHF -> rb2505)
            df['ts_code'] = df['ts_code'].apply(lambda x: x.split('.')[0] if '.' in x else x)

            # 重命名
            df = df.rename(columns={
                'open': 'open_price', 'high': 'high_price',
                'low': 'low_price', 'close': 'close_price',
                'settle': 'settle_price', 'change1': 'change'
            })

            # 计算涨跌幅
            if 'pre_close' in df.columns and 'close_price' in df.columns:
                df['pct_chg'] = (df['close_price'] - df['pre_close']) / df['pre_close'] * 100
            else:
                df['pct_chg'] = 0.0

            # 筛选字段
            cols = ['trade_date', 'ts_code', 'open_price', 'high_price', 'low_price', 'close_price', 'settle_price',
                    'vol', 'oi', 'pct_chg']
            df_save = df[cols].copy()
            df_save.fillna(0, inplace=True)

            # --- 关键：先删后写 (防止重复) ---
            # 使用 SQL 删除当天、该交易所的数据
            # 这样即使脚本重复运行，也不会导致数据重复
            codes = tuple(df_save['ts_code'].tolist())
            if not codes: continue

            with engine.connect() as conn:
                # 删除旧数据 (幂等性保证)
                # 注意：这里我们简单粗暴地删除当天该交易所的所有数据，然后重新插入
                # 这种方式比 delete where ts_code in (...) 更快且不容易出错
                # 假设 futures_price 里 ts_code 是纯代码 (rb2505)，无法直接通过后缀判断交易所
                # 所以我们还是得用 ts_code 列表来删除

                # 优化 SQL：如果列表太长，SQL 可能会报错，分批删除或直接删全天数据(如果有 ex 字段)
                # 这里为了稳妥，我们直接删除当天这些 ts_code 的数据

                # 构造删除语句 (处理 tuple 只有一个元素时的逗号问题)
                codes_str = str(codes) if len(codes) > 1 else f"('{codes[0]}')"
                del_sql = text(f"DELETE FROM futures_price WHERE trade_date='{trade_date}' AND ts_code IN {codes_str}")
                conn.execute(del_sql)

                # 写入
                df_save.to_sql('futures_price', conn, if_exists='append', index=False, chunksize=2000)
                conn.commit()

            count = len(df_save)
            total_count += count
            print(f"   -> {ex}: 更新 {count} 条")

            # --- 关键：内存释放 ---
            del df
            del df_save
            gc.collect()  # 强制回收内存

        except Exception as e:
            print(f"   [!] {ex} 异常: {e}")

        # 避免 API 频率限制
        time.sleep(1)

    duration = time.time() - start_time
    print(f" [√] 完成，共更新 {total_count} 条，耗时 {duration:.2f}s\n")


if __name__ == "__main__":
    # 每天只跑今天的数据
    today = datetime.now().strftime('%Y%m%d')
    save_daily_all_contracts(today)