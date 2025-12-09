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
# 获取脚本所在目录的绝对路径，确保在服务器 crontab 运行时能找到 .env
current_dir = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(current_dir, '.env'), override=True)

# 检查配置
if not os.getenv("DB_USER"):
    print("❌ [Error] 环境变量未加载，请检查 .env 文件路径")
    sys.exit(1)

# 数据库连接
db_url = f"mysql+pymysql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
engine = create_engine(db_url, pool_recycle=3600)  # pool_recycle 防止数据库连接超时

# Tushare 初始化
ts.set_token(os.getenv("TUSHARE_TOKEN"))
pro = ts.pro_api()

# 交易所列表
EXCHANGES = ['SHFE', 'DCE', 'CZCE', 'CFFEX', 'GFEX', 'INE']


def get_trade_cal(date_str):
    """判断今天是否是交易日"""
    try:
        df = pro.trade_cal(exchange='SHFE', start_date=date_str, end_date=date_str)
        if not df.empty:
            return df.iloc[0]['is_open'] == 1
    except:
        return True  # 如果接口挂了，默认尝试跑一下
    return False


def update_daily_data(trade_date):
    """
    执行单日数据更新：抓取 -> 清洗 -> 修复 -> 生成主力 -> 入库
    """
    print(f"[*] 启动每日更新任务: {trade_date}")
    start_t = time.time()

    # 1. 安全清理：先删除当天的旧数据 (防止重跑时主键冲突)
    #    这样做是幂等的，一天跑多次也没关系
    with engine.connect() as conn:
        conn.execute(text(f"DELETE FROM futures_price WHERE trade_date = '{trade_date}'"))
        conn.commit()

    total_records = 0

    # 2. 逐个交易所处理 (内存优化关键：处理完一个就释放)
    for ex in EXCHANGES:
        try:
            # A. 抓取
            df = pro.fut_daily(trade_date=trade_date, exchange=ex)
            if df.empty: continue

            # B. 基础清洗
            # 去除后缀 (rb2505.SHF -> rb2505)
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

            # C. 价格修复 (解决大商所 Close=0 问题)
            # 逻辑：如果 Close <= 0 且 Settle > 0，强制用 Settle
            mask_fix = (df['close_price'] <= 0.001) & (df['settle_price'] > 0)
            df.loc[mask_fix, 'close_price'] = df.loc[mask_fix, 'settle_price']

            # 辅助修复 OHLC
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

            # D. 生成【主力连续】合约 (内存优化版)
            # 直接在当前交易所的数据里找主力，不需要全市场合并
            # 提取品种代码 (正则匹配开头字母)
            df['symbol'] = df['ts_code'].str.extract(r'^([a-zA-Z]+)')

            # 找到 OI 最大的行
            # dropna 防止提取失败报错
            idx_max = df.dropna(subset=['symbol']).groupby('symbol')['oi'].idxmax()

            # 复制出主力数据
            df_dom = df.loc[idx_max].copy()
            df_dom['ts_code'] = df_dom['symbol']  # 改名为 rb, M 等

            # E. 合并与入库
            # 将原始分合约 + 主力连续合约 合并
            df_final = pd.concat([df, df_dom], ignore_index=True)

            # 去重 (防止万一 Tushare 本身就有 'M' 这种代码)
            df_final = df_final.drop_duplicates(subset=['ts_code'], keep='last')

            # 筛选字段
            final_cols = ['trade_date', 'ts_code', 'open_price', 'high_price',
                          'low_price', 'close_price', 'settle_price',
                          'vol', 'oi', 'pct_chg']

            df_save = df_final[final_cols]

            # 写入数据库
            df_save.to_sql('futures_price', engine, if_exists='append', index=False, chunksize=2000)

            count = len(df_save)
            total_records += count
            print(f"   -> {ex}: 入库 {count} 条")

            # 主动释放内存 (虽然 Python 有 GC，但显式删除在大循环里是个好习惯)
            del df, df_dom, df_final, df_save

        except Exception as e:
            print(f"   [!] {ex} 更新异常: {e}")
            # 服务器脚本遇到单个异常不应退出，继续跑下一个交易所
            continue

    duration = time.time() - start_t
    print(f" [√] 更新完成。日期: {trade_date}, 总条数: {total_records}, 耗时: {duration:.2f}s\n")


if __name__ == "__main__":
    # 获取今天日期
    now = datetime.now()
    today_str = now.strftime('%Y%m%d')

    # 1. 简单的时间检查：如果是周末，直接不跑 (节省服务器资源)
    # Tushare 免费用户有时周末调取会扣积分，没必要
    if now.weekday() >= 5:  # 5=周六, 6=周日
        print(f" [-] 今天是周末 ({today_str})，跳过更新。")
        sys.exit(0)

    # 2. 交易日历检查 (更严谨)
    # 建议在每天下午 16:00 或 18:00 后运行
    if not get_trade_cal(today_str):
        print(f" [-] 今天 ({today_str}) 是非交易日，跳过更新。")
        sys.exit(0)

    # 3. 执行更新
    try:
        update_daily_data(today_str)
    except Exception as e:
        print(f" [!!!] 脚本执行发生致命错误: {e}")
        sys.exit(1)