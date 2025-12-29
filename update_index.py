import tushare as ts
import pandas as pd
from sqlalchemy import create_engine, text
import os
from dotenv import load_dotenv
import time
from datetime import datetime, timedelta

# 1. 初始化
load_dotenv(override=True)
db_url = f"mysql+pymysql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
engine = create_engine(db_url)
ts.set_token(os.getenv("TUSHARE_TOKEN"))
pro = ts.pro_api()


def init_index_table():
    """初始化指数价格表"""
    with engine.connect() as conn:
        sql = """
              CREATE TABLE IF NOT EXISTS index_price \
              ( \
                  trade_date \
                  VARCHAR \
              ( \
                  20 \
              ),
                  ts_code VARCHAR \
              ( \
                  20 \
              ),
                  open_price FLOAT,
                  high_price FLOAT,
                  low_price FLOAT,
                  close_price FLOAT,
                  pct_chg FLOAT,
                  vol FLOAT,
                  amount FLOAT,
                  PRIMARY KEY \
              ( \
                  trade_date, \
                  ts_code \
              )
                  ) DEFAULT CHARSET=utf8mb4;
              """
        conn.execute(text(sql))


def fetch_and_save_indices(start_date, end_date):
    # 指数列表
    indices = {
        '000001.SH': '上证指数',
        '399001.SZ': '深证成指',
        '000300.SH': '沪深300',
        '000905.SH': '中证500',
        '000852.SH': '中证1000',
        '000688.SH': '科创50',
        '399006.SZ': '创业板指',
        '000016.SH': '上证50',
        '399005.SZ': '中小100',
        '932000.CSI': '中证2000',
    }

    print(f"🚀 开始拉取指数数据: {start_date} 至 {end_date} ...")

    for code, name in indices.items():
        try:
            # 1. 清理旧数据 (幂等性)
            with engine.connect() as conn:
                del_sql = text(
                    f"DELETE FROM index_price WHERE ts_code='{code}' AND trade_date >= '{start_date}' AND trade_date <= '{end_date}'")
                conn.execute(del_sql)
                conn.commit()

            # 2. 调用接口
            df = pro.index_daily(ts_code=code, start_date=start_date, end_date=end_date)

            if not df.empty:
                rename_map = {
                    'close': 'close_price',
                    'open': 'open_price',
                    'high': 'high_price',
                    'low': 'low_price',
                }
                df = df.rename(columns=rename_map)

                cols_to_save = ['trade_date', 'ts_code', 'open_price', 'high_price',
                                'low_price', 'close_price', 'pct_chg', 'vol', 'amount']
                final_cols = [c for c in cols_to_save if c in df.columns]

                # 3. 入库
                df[final_cols].to_sql('index_price', engine, if_exists='append', index=False)
                print(f"   [√] {name} 更新成功")
            else:
                print(f"   [-] {name} 无数据 (可能是非交易日或收盘前)")

            time.sleep(0.3)

        except Exception as e:
            print(f"   [x] {name} 异常: {e}")


# ==========================================
#  🔴 修改重点：自动获取“今天”
# ==========================================
if __name__ == "__main__":
    init_index_table()

    # 1. 自动获取今天的日期 (格式 YYYYMMDD)
    today = datetime.now().strftime('%Y%m%d')

    # 2. 如果你想跑昨天的，可以用这行 (把上面那行注释掉):
    # today = (datetime.now() - timedelta(days=1)).strftime('%Y%m%d')

    print(f"⚡️ 正在执行单日更新模式: {today}")

    # 3. 开始与结束都设为今天，就只跑今天
    fetch_and_save_indices(today, today)

    print("\n=== ✅ 今日数据任务结束 ===")