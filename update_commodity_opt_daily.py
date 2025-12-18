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

# 交易所全列表
# 包括: 上期所, 大商所, 郑商所, 广期所, 上能源, 中金所
EXCHANGES = ['SHFE', 'DCE', 'CZCE', 'GFEX', 'INE', 'CFFEX']


def get_trade_cal(start_date, end_date):
    """获取交易日历"""
    try:
        df = pro.trade_cal(exchange='SHFE', start_date=start_date, end_date=end_date, is_open='1')
        return df['cal_date'].tolist() if not df.empty else []
    except:
        # 如果获取日历失败，默认返回结束日期作为尝试
        return [end_date]


def save_daily_option_price(trade_date):
    """
    抓取指定日期的所有商品期权价格 -> 使用 INSERT IGNORE 存入数据库
    """
    print(f"[*] 正在启动期权更新: {trade_date} ...")
    start_time = time.time()
    total_records = 0

    for ex in EXCHANGES:
        try:
            # 1. 调用 Tushare 接口
            # 获取字段: 代码, 日期, 收盘, 开, 高, 低, 量, 持仓
            df = pro.opt_daily(trade_date=trade_date, exchange=ex,
                               fields='ts_code,trade_date,close,open,high,low,vol,oi,settle')

            if df.empty:
                continue

            # 2. 数据清洗
            # 填充空值为 0
            df = df.fillna(0)

            # 确保列名与数据库一致 (数据库通常是 close, open...)
            # Tushare 返回的就是这些，无需重命名

            # 3. 构造批量插入 SQL (INSERT IGNORE)
            # 这种方式比 to_sql 更快且不会因为主键重复而报错
            values_list = []
            for _, row in df.iterrows():
                vals = (
                    row['trade_date'],
                    row['ts_code'],
                    row.get('open', 0),
                    row.get('high', 0),
                    row.get('low', 0),
                    row.get('close', 0),
                    row.get('vol', 0),
                    row.get('oi', 0),
                    row.get('settle', 0)  # 新增字段
                )
                values_list.append(str(vals))

            if values_list:
                # 分批写入，防止 SQL 语句过长
                batch_size = 2000
                for i in range(0, len(values_list), batch_size):
                    batch = values_list[i: i + batch_size]
                    sql_vals = ",".join(batch)

                    sql = f"""
                        INSERT IGNORE INTO commodity_opt_daily 
                        (trade_date, ts_code, open, high, low, close, vol, oi, settle)
                        VALUES {sql_vals}
                    """

                    with engine.connect() as conn:
                        conn.execute(text(sql))
                        conn.commit()

                count = len(df)
                total_records += count
                print(f"   -> {ex}: 成功入库 {count} 条")

        except Exception as e:
            print(f"   [!] {ex} 更新异常: {e}")

        # 避免触发 API 频率限制 (尤其是包含 CFFEX 时)
        time.sleep(0.3)

    duration = time.time() - start_time
    if total_records > 0:
        print(f" [√] {trade_date} 全部完成，共更新 {total_records} 条数据，耗时 {duration:.2f}s\n")
    else:
        print(f" [-] {trade_date} 无数据或非交易日\n")


def run_update_task(mode='daily', days_back=5):
    """
    主运行函数
    :param mode: 'daily' (只跑今天), 'history' (补跑历史)
    :param days_back: 补跑天数
    """
    today = datetime.now().strftime('%Y%m%d')

    if mode == 'daily':
        # 即使是非交易日，尝试跑一下也没坏处，Tushare 会返回空
        target_dates = [today]
    else:
        # 补跑模式
        start_date = (datetime.now() - timedelta(days=days_back)).strftime('%Y%m%d')
        # 获取期间的所有交易日
        target_dates = get_trade_cal(start_date, today)
        print(f"=== 准备补全历史数据: {len(target_dates)} 个交易日 ===")

    for d in target_dates:
        save_daily_option_price(d)


if __name__ == "__main__":
    # --- 配置区域 ---

    # 场景 1: 日常更新 (每天运行一次)
    run_update_task(mode='daily')

    # 场景 2: 首次修复/补全数据 (建议先运行这个！)
    # 补全最近 30 天，确保 M, IO 等数据都齐了
    #print(">>> 开始执行全市场期权数据补全...")
    #run_update_task(mode='history', days_back=5)