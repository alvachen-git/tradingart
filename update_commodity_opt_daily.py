import tushare as ts
import pandas as pd
from sqlalchemy import create_engine, text
import os
from dotenv import load_dotenv
import time
from datetime import datetime, timedelta

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

# 交易所列表 (上期所、大商所、郑商所、广期所、上能源)
EXCHANGES = ['SHFE', 'DCE', 'CZCE', 'GFEX', 'INE']

SUFFIX_MAP = {
    'SHFE': '.SHF',  # 上期所是 .SHF
    'DCE':  '.DCE',  # 大商所是 .DCE
    'CZCE': '.ZCE',  # 鄭商所是 .ZCE
    'GFEX': '.GFE',  # 廣期所是 .GFE
    'INE':  '.INE'   # 上能源是 .INE
}

def get_trade_cal(start_date, end_date):
    """获取交易日历，判断某天是否开盘"""
    df = pro.trade_cal(exchange='SHFE', start_date=start_date, end_date=end_date, is_open='1')
    return df['cal_date'].tolist() if not df.empty else []


def save_daily_option_price(trade_date):
    """
    抓取指定日期的所有商品期权价格 -> 存入数据库
    """
    print(f"[*] 正在抓取 {trade_date} 的期权行情...")

    start_time = time.time()
    total_records = 0

    for ex in EXCHANGES:
        try:
            # 1. 调用 Tushare 接口 (每次只取一个交易所，减少内存压力)
            # fields: 代码, 日期, 收盘价, 开高低, 持仓量, 成交量
            df = pro.opt_daily(trade_date=trade_date, exchange=ex,
                               fields='ts_code,trade_date,close,open,high,low,vol,oi')

            if df.empty:
                continue

            # 2. 简单清洗
            df['ts_code'] = df['ts_code'].astype(str)
            df.fillna(0, inplace=True)

            # 【修復 1】強制去重，防止數據源自帶重複
            df.drop_duplicates(subset=['ts_code', 'trade_date'], inplace=True)

            row_count = len(df)
            total_records += row_count

            # 3. 入库 (幂等操作)
            with engine.connect() as conn:
                # 【修復 2】使用正確的後綴進行刪除
                suffix = SUFFIX_MAP.get(ex, f".{ex}")
                del_sql = text(
                    f"DELETE FROM commodity_opt_daily WHERE trade_date='{trade_date}' AND ts_code LIKE '%{suffix}'")
                conn.execute(del_sql)

                # 4. 流式写入
                # chunksize=2000 确保即使这一个交易所数据很多，也不会卡死数据库
                df.to_sql('commodity_opt_daily', conn, if_exists='append', index=False, chunksize=2000)
                conn.commit()

            print(f"   -> {ex}: {row_count} 条入库")

            # 5. 【关键】主动释放内存
            del df

        except Exception as e:
            if "Duplicate entry" in str(e):
                print(f"   [!] {ex} 重复数据，已跳过")
            else:
                print(f"   [!] {ex} 抓取失敗: {e}")

        # 避免触发 API 频率限制
        time.sleep(0.3)

    duration = time.time() - start_time
    print(f" [√] {trade_date} 更新完毕，共 {total_records} 条，耗时 {duration:.2f}秒\n")


def run_update_task(mode='daily', days_back=0):
    """
    mode='daily': 只跑今天 (适合 crontab)
    mode='history': 补跑最近 N 天
    """
    today = datetime.now().strftime('%Y%m%d')

    if mode == 'daily':
        # 检查今天是否是交易日 (如果是晚上跑，就跑今天)
        # 建议设置定时任务在 18:00 以后
        dates = get_trade_cal(today, today)
        if not dates:
            print(f" [x] 今天 ({today}) 是非交易日，无需更新。")
            return
        target_dates = [today]

    else:
        # 补跑历史
        start = (datetime.now() - timedelta(days=days_back)).strftime('%Y%m%d')
        dates = get_trade_cal(start, today)
        target_dates = dates

        # 循环处理每一天
    for d in target_dates:
        save_daily_option_price(d)


if __name__ == "__main__":
    # 用法 A: 服务器每日定时任务 (默认)
    #run_update_task(mode='daily')

    # 用法 B: 手动补数据 (例如补最近 5 天)
    run_update_task(mode='history', days_back=200)