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
    """
    获取交易日历 (只返回开盘的日期)
    使用 SHFE 作为基准，通常商品期货的交易日是一致的
    """
    try:
        print(f"[*] 正在获取 {start_date} 到 {end_date} 的交易日历...")
        df = pro.trade_cal(exchange='SHFE', start_date=start_date, end_date=end_date, is_open='1')
        if df.empty:
            return []
        # 按日期升序排列
        return sorted(df['cal_date'].tolist())
    except Exception as e:
        print(f"[!] 获取日历失败: {e}")
        # 如果获取日历失败，为了保险起见，返回 start_date 到 end_date 的每一天
        # 但这样可能会遇到周末报错，不过也比不跑好
        date_list = []
        current = datetime.strptime(start_date, '%Y%m%d')
        end = datetime.strptime(end_date, '%Y%m%d')
        while current <= end:
            date_list.append(current.strftime('%Y%m%d'))
            current += timedelta(days=1)
        return date_list


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
            # 获取字段: 代码, 日期, 收盘, 开, 高, 低, 量, 持仓, 结算价
            df = pro.opt_daily(trade_date=trade_date, exchange=ex,
                               fields='ts_code,trade_date,close,open,high,low,vol,oi,settle')

            if df.empty:
                continue

            # 2. 数据清洗
            # 填充空值为 0
            df = df.fillna(0)

            # 3. 构造批量插入 SQL (INSERT IGNORE)
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
                    row.get('settle', 0)
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

        # 避免触发 API 频率限制
        time.sleep(0.3)

    duration = time.time() - start_time
    if total_records > 0:
        print(f" [√] {trade_date} 全部完成，共更新 {total_records} 条数据，耗时 {duration:.2f}s\n")
    else:
        print(f" [-] {trade_date} 无数据或非交易日\n")


# ==========================================
#  主程序入口：可指定日期范围
# ==========================================
if __name__ == "__main__":

    # 🔴 在这里配置你想更新的日期区间 (格式 YYYYMMDD)
    # 提示：如果是想跑“昨天”的数据，START_DATE 和 END_DATE 填一样即可
    START_DATE = "20241231"
    END_DATE   = "20250611"

    print(f"=== 🚀 开始更新区间期权价格: {START_DATE} 至 {END_DATE} ===")

    # 1. 获取该区间内的有效交易日 (自动跳过周末/节假日)
    trade_dates = get_trade_cal(START_DATE, END_DATE)

    print(f"=== 📅 共有 {len(trade_dates)} 个交易日待处理 ===")

    # 2. 循环处理
    for date_str in trade_dates:
        try:
            save_daily_option_price(date_str)
        except Exception as e:
            print(f"❌ {date_str} 严重错误，跳过: {e}")

    print("\n=== ✅ 区间更新任务全部完成 ===")