import tushare as ts
import pandas as pd
import akshare as ak
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


def fetch_and_save_hk_indices(start_date, end_date):
    """
    抓取港股核心指数 (使用 AkShare)
    并自动计算涨跌幅
    """
    # 强制转为字符串，防止报错
    start_date = str(start_date)
    end_date = str(end_date)

    # 映射表: AkShare代码 -> 中文名
    # HSI: 恒生指数, HSTECH: 恒生科技指数
    hk_indices = {
        'HSI': '恒生指数',
        'HSTECH': '恒生科技指数'
    }

    print(f"🚀 [港股] 开始拉取指数数据: {start_date} 至 {end_date} ...")

    for symbol, name in hk_indices.items():
        try:
            # 1. 清理旧数据
            with engine.connect() as conn:
                del_sql = text(
                    f"DELETE FROM index_price WHERE ts_code='{symbol}' AND trade_date >= '{start_date}' AND trade_date <= '{end_date}'")
                conn.execute(del_sql)
                conn.commit()

            # 2. 调用 AkShare 接口 (获取全量历史)
            df = ak.stock_hk_index_daily_sina(symbol=symbol)

            if df.empty:
                print(f"   [-] {name} 接口未返回数据")
                continue

            # 3. 数据清洗与计算
            if 'date' not in df.columns:
                print(f"   [!] {name} 缺少 date 列")
                continue

            # 日期格式化
            df['trade_date'] = pd.to_datetime(df['date']).dt.strftime('%Y%m%d')

            # 🔥【关键新增】计算涨跌幅 (pct_chg)
            # 逻辑：(今收 - 昨收) / 昨收 * 100
            # 必须在过滤日期之前计算，否则第一天的数据会因为没有前一天而变成 NaN
            df['close'] = pd.to_numeric(df['close'], errors='coerce')
            df['pct_chg'] = df['close'].pct_change() * 100
            df['pct_chg'] = df['pct_chg'].fillna(0).round(4)  # 保留4位小数

            # 4. 过滤日期区间
            mask = (df['trade_date'] >= start_date) & (df['trade_date'] <= end_date)
            df_filtered = df.loc[mask].copy()

            if df_filtered.empty:
                print(f"   [-] {name} 区间内无数据")
                continue

            # 字段重命名
            rename_map = {
                'open': 'open_price',
                'high': 'high_price',
                'low': 'low_price',
                'close': 'close_price',
                'volume': 'vol'
            }
            df_filtered = df_filtered.rename(columns=rename_map)

            # 补充字段
            df_filtered['ts_code'] = symbol
            df_filtered['amount'] = 0

            # 5. 入库
            cols_to_save = ['trade_date', 'ts_code', 'open_price', 'high_price',
                            'low_price', 'close_price', 'pct_chg', 'vol', 'amount']

            for c in cols_to_save:
                if c not in df_filtered.columns:
                    df_filtered[c] = 0

            df_filtered[cols_to_save].to_sql('index_price', engine, if_exists='append', index=False)
            print(f"   [√] {name} 更新成功 ({len(df_filtered)} 条)")

            time.sleep(1)

        except Exception as e:
            print(f"   [x] {name} 异常: {e}")


if __name__ == "__main__":
    init_index_table()

    # 自动获取今天
    today = datetime.now().strftime('%Y%m%d')

    # 如果想补全历史数据，可以把下面这行解开注释并修改日期：
    # fetch_and_save_hk_indices("20240101", today)

    print(f"⚡️ 正在执行每日更新模式: {today}")

    # 更新 A 股
    fetch_and_save_indices(today, today)

    # 更新 港股
    fetch_and_save_hk_indices(today, today)

    print("\n=== ✅ 所有指数数据更新结束 ===")