import pandas as pd
import tushare as ts
from sqlalchemy import create_engine, text, types
from datetime import datetime, timedelta
import os
from dotenv import load_dotenv
import time

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


# --- 2. 初始化表结构 (运行一次即可) ---
def init_tables():
    with engine.connect() as conn:
        print("[*] 正在检查/创建期权数据表...")

        # 表1: 期权基础信息 (合约身份证)
        # 关键字段: ts_code(主键), exercise_price(行权价), call_put(类型)
        sql_basic = """
                    CREATE TABLE IF NOT EXISTS option_basic \
                    ( \
                        ts_code \
                        VARCHAR \
                    ( \
                        20 \
                    ) NOT NULL,
                        name VARCHAR \
                    ( \
                        50 \
                    ),
                        underlying VARCHAR \
                    ( \
                        20 \
                    ) COMMENT '标的ETF',
                        call_put VARCHAR \
                    ( \
                        10 \
                    ) COMMENT '认购C/认沽P',
                        exercise_price FLOAT COMMENT '行权价',
                        list_date VARCHAR \
                    ( \
                        8 \
                    ),
                        delist_date VARCHAR \
                    ( \
                        8 \
                    ),
                        PRIMARY KEY \
                    ( \
                        ts_code \
                    ),
                        INDEX idx_underlying \
                    ( \
                        underlying \
                    )
                        ) \
                    """
        conn.execute(text(sql_basic))

        # 表2: 期权日线行情 (每天的量价)
        # 关键字段: oi(持仓量), close(收盘价)
        sql_daily = """
                    CREATE TABLE IF NOT EXISTS option_daily \
                    ( \
                        trade_date \
                        VARCHAR \
                    ( \
                        8 \
                    ) NOT NULL,
                        ts_code VARCHAR \
                    ( \
                        20 \
                    ) NOT NULL,
                        close FLOAT,
                        oi FLOAT COMMENT '持仓量(张)',
                        vol FLOAT COMMENT '成交量(张)',
                        PRIMARY KEY \
                    ( \
                        trade_date, \
                        ts_code \
                    ),
                        INDEX idx_date \
                    ( \
                        trade_date \
                    )
                        ) \
                    """
        conn.execute(text(sql_daily))
        print("[√] 表结构就绪 (option_basic, option_daily)")


# --- 3. 抓取并入库 ---
def fetch_and_save_options(etf_code="510050.SH", start_date=None, end_date=None):
    # 1. 确定交易所和名称匹配规则
    exchange = 'SSE'  # 默认上交所
    target_name_keyword = ""

    if "510050" in etf_code:
        target_name_keyword = "50ETF"
    elif "510300" in etf_code:
        target_name_keyword = "300ETF"
    elif "510500" in etf_code:
        target_name_keyword = "500ETF"
    elif "588000" in etf_code:
        target_name_keyword = "科创"  # 修正后的关键词
    elif "159915" in etf_code:
        target_name_keyword = "创业板"
        exchange = 'SZSE'
    else:
        print(f" [!] 未知品种: {etf_code}")
        return

    print(f"\n>>> 开始处理 {target_name_keyword} ({etf_code}) - 交易所: {exchange} ...")

    try:
        # === A. 更新合约列表 (option_basic) ===
        print(f"  [*] 正在获取合约列表 (opt_basic)...")

        # 获取该交易所所有上市合约
        df_basic = pro.opt_basic(exchange=exchange, list_status='L',
                                 fields='ts_code,name,call_put,exercise_price,list_date,delist_date')

        if df_basic.empty:
            print("  [-] Tushare 未返回任何合约数据")
            return

        # 关键词过滤
        df_basic = df_basic[df_basic['name'].str.contains(target_name_keyword)]

        if df_basic.empty:
            print(f"  [-] 未找到名称包含 '{target_name_keyword}' 的合约")
            return

        df_basic['underlying'] = etf_code

        # --- 【关键修正】精准去重 ---
        # 不按 underlying 删，而是按 ts_code (合约代码) 删
        # 这样无论旧数据被标记成了什么，都会被清除，腾出位置
        codes_to_insert = df_basic['ts_code'].tolist()
        if codes_to_insert:
            codes_str = "', '".join(codes_to_insert)
            with engine.connect() as conn:
                # 删除这些即将要插入的代码
                del_sql = f"DELETE FROM option_basic WHERE ts_code IN ('{codes_str}')"
                conn.execute(text(del_sql))
                conn.commit()

        # 入库
        df_basic.to_sql('option_basic', engine, if_exists='append', index=False, dtype={
            'exercise_price': types.Float()
        })
        print(f"  [√] 更新了 {len(df_basic)} 个合约信息")

        # === B. 更新日线行情 (option_daily) ===
        print(f"  [*] 正在获取日线行情 ({start_date} - {end_date})...")

        dates_df = pro.trade_cal(exchange=exchange, start_date=start_date, end_date=end_date, is_open='1')
        dates_list = dates_df['cal_date'].tolist()

        target_contracts = df_basic['ts_code'].tolist()

        for date in dates_list:
            try:
                df_daily = pro.opt_daily(trade_date=date, exchange=exchange, fields='ts_code,trade_date,close,oi,vol')
            except:
                continue

            if df_daily.empty: continue

            df_save = df_daily[df_daily['ts_code'].isin(target_contracts)]

            if not df_save.empty:
                codes_str = "', '".join(df_save['ts_code'].unique())
                with engine.connect() as conn:
                    del_sql = f"DELETE FROM option_daily WHERE trade_date='{date}' AND ts_code IN ('{codes_str}')"
                    conn.execute(text(del_sql))
                    conn.commit()

                df_save.to_sql('option_daily', engine, if_exists='append', index=False, dtype={
                    'close': types.Float(), 'oi': types.Float(), 'vol': types.Float()
                })
                print(f"    - {date}: 存入 {len(df_save)} 条记录")

            time.sleep(0.2)

    except Exception as e:
        print(f"  [!] 发生错误: {e}")


if __name__ == "__main__":
    # 1. 初始化表
    init_tables()

    # 2. 设置抓取范围 (建议抓最近 20 天，构建完整趋势图)
    today = datetime.now().strftime('%Y%m%d')
    start = (datetime.now() - timedelta(days=120)).strftime('%Y%m%d')

    # 3. 抓取 50ETF 和 300ETF
    fetch_and_save_options(etf_code="510050.SH", start_date=start, end_date=today)
    fetch_and_save_options(etf_code="510300.SH", start_date=start, end_date=today)
    fetch_and_save_options(etf_code="510500.SH", start_date=start, end_date=today)
    fetch_and_save_options(etf_code="588000.SH", start_date=start, end_date=today)
    fetch_and_save_options(etf_code="159915.SZ", start_date=start, end_date=today)

    print("\n=== 全部更新完成！前端可以直接读库了 ===")