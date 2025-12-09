import tushare as ts
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
import os
from dotenv import load_dotenv
import time
from datetime import datetime
import sys

# --- 1. 初始化配置 ---
load_dotenv(override=True)

if not os.getenv("DB_USER"):
    print("❌ [Error] 环境变量未加载，请检查 .env 文件路径")
    sys.exit(1)

# 数据库连接
db_url = f"mysql+pymysql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
engine = create_engine(db_url, pool_recycle=3600)

# Tushare 初始化
ts.set_token(os.getenv("TUSHARE_TOKEN"))
pro = ts.pro_api()

# 交易所后缀映射表
EXCHANGE_CONFIG = {
    'SHFE': '.SHF',
    'DCE': '.DCE',
    'CZCE': '.ZCE',
    'CFFEX': '.CFX',
    'INE': '.INE',
    'GFEX': '.GFE'
}


def get_trade_cal(date_str):
    """判断今天是否是交易日"""
    try:
        df = pro.trade_cal(exchange='SHFE', start_date=date_str, end_date=date_str)
        if not df.empty:
            return df.iloc[0]['is_open'] == 1
    except:
        return True
    return False


def update_daily_data(trade_date):
    """
    执行单日数据更新：抓取 -> 过滤 -> 清洗 -> 防撞检查 -> 入库
    """
    print(f"[*] 启动每日更新任务: {trade_date}")
    start_t = time.time()

    # 1. 全局清理：先删除当天的旧数据
    # 注意：如果这脚本今天跑了一半挂了，重跑时会把之前跑成功的也删掉，重新来过，这是安全的。
    with engine.connect() as conn:
        conn.execute(text(f"DELETE FROM futures_price WHERE trade_date = '{trade_date}'"))
        conn.commit()

    print(" [√] 已清理当日旧数据，准备重新入库...")

    total_records = 0

    # 2. 逐个交易所处理
    for ex, suffix in EXCHANGE_CONFIG.items():
        try:
            # A. 抓取
            df = pro.fut_daily(trade_date=trade_date, exchange=ex)
            if df.empty: continue

            # 过滤后缀 (虽然可能拦不住所有脏数据，但还是加上)
            df = df[df['ts_code'].str.endswith(suffix)].copy()
            if df.empty: continue

            # B. 基础清洗
            df['ts_code'] = df['ts_code'].apply(lambda x: x.split('.')[0] if '.' in x else x)

            # 重命名
            df = df.rename(columns={
                'open': 'open_price', 'high': 'high_price',
                'low': 'low_price', 'close': 'close_price',
                'settle': 'settle_price'
            })

            # 填充空值
            cols = ['open_price', 'high_price', 'low_price', 'close_price', 'settle_price', 'vol', 'oi']
            for c in cols:
                if c not in df.columns: df[c] = 0
            df[cols] = df[cols].fillna(0)

            # C. 价格修复
            mask_fix = (df['close_price'] <= 0.001) & (df['settle_price'] > 0)
            df.loc[mask_fix, 'close_price'] = df.loc[mask_fix, 'settle_price']
            for c in ['open_price', 'high_price', 'low_price']:
                df.loc[df[c] <= 0.001, c] = df['close_price']

            # 计算涨跌幅
            if 'pct_chg' not in df.columns:
                if 'pre_close' in df.columns:
                    df['pre_close'] = df['pre_close'].fillna(0)
                    df['pct_chg'] = np.where(df['pre_close'] > 0,
                                             (df['close_price'] - df['pre_close']) / df['pre_close'] * 100,
                                             0)
                else:
                    df['pct_chg'] = 0.0

            # D. 生成主力合约
            df['symbol'] = df['ts_code'].str.extract(r'^([a-zA-Z]+)')
            idx_max = df.dropna(subset=['symbol']).groupby('symbol')['oi'].idxmax()
            df_dom = df.loc[idx_max].copy()
            df_dom['ts_code'] = df_dom['symbol']

            # E. 合并
            df_final = pd.concat([df, df_dom], ignore_index=True)
            df_final = df_final.drop_duplicates(subset=['ts_code'], keep='last')

            # 筛选字段
            final_cols = ['trade_date', 'ts_code', 'open_price', 'high_price',
                          'low_price', 'close_price', 'settle_price',
                          'vol', 'oi', 'pct_chg']
            df_save = df_final[final_cols]

            # 🔥🔥🔥【核心大招：入库前防撞检查】🔥🔥🔥
            # 1. 先查一下数据库里今天已经有了哪些代码 (比如 SHFE 已经存了 AL)
            existing_codes_df = pd.read_sql(
                f"SELECT ts_code FROM futures_price WHERE trade_date='{trade_date}'",
                engine
            )
            existing_set = set(existing_codes_df['ts_code'].tolist())

            # 2. 只有数据库里没有的，我才插入
            # 这样如果 Tushare 发疯把 AL 塞进 DCE，这里会发现数据库已有 AL，直接过滤掉
            initial_save_count = len(df_save)
            df_save = df_save[~df_save['ts_code'].isin(existing_set)]

            if len(df_save) < initial_save_count:
                print(
                    f"   [🛡️] 触发防撞机制：自动剔除了 {initial_save_count - len(df_save)} 条重复/脏数据 (如 {list(set(df_final['ts_code']) - set(df_save['ts_code']))[:3]}...)")

            if df_save.empty:
                print(f"   [i] {ex} 所有数据均已存在，跳过入库")
                continue

            # 写入数据库
            df_save.to_sql('futures_price', engine, if_exists='append', index=False, chunksize=2000)

            count = len(df_save)
            total_records += count
            print(f"   -> {ex}: 成功入库 {count} 条")

            del df, df_dom, df_final, df_save

        except Exception as e:
            print(f"   [!] {ex} 更新异常: {e}")
            continue

    duration = time.time() - start_t
    print(f" [√] 更新完成。日期: {trade_date}, 总条数: {total_records}, 耗时: {duration:.2f}s\n")


if __name__ == "__main__":
    now = datetime.now()
    today_str = now.strftime('%Y%m%d')

    if now.weekday() >= 5:
        print(f" [-] 今天是周末 ({today_str})，跳过更新。")
        sys.exit(0)

    if not get_trade_cal(today_str):
        print(f" [-] 今天 ({today_str}) 是非交易日，跳过更新。")
        sys.exit(0)

    try:
        update_daily_data(today_str)
    except Exception as e:
        print(f" [!!!] 脚本执行发生致命错误: {e}")
        sys.exit(1)