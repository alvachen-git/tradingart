import tushare as ts
import pandas as pd
from sqlalchemy import create_engine, text
import os
import time
from datetime import datetime, timedelta
from dotenv import load_dotenv

load_dotenv(override=True)

# 初始化
db_url = f"mysql+pymysql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
engine = create_engine(db_url)
ts.set_token(os.getenv("TUSHARE_TOKEN"))
pro = ts.pro_api()

# 定义关注的指数
CORE_INDICES = {
    '000001.SH': '上证指数',
    '000300.SH': '沪深300',
    '000905.SH': '中证500',
    '000852.SH': '中证1000',
    '000688.SH': '科创50',
    '000016.SH': '上证50',
    '399001.SZ': '深证成指',
    '399006.SZ': '创业板指',
    # '932000.CSI': '中证2000',
}


def update_index_valuation(is_backfill=False, years=3):
    """
    更新指数估值
    :param is_backfill: True=补历史数据, False=只更今天
    """
    mode_str = '历史补全' if is_backfill else '每日更新'
    print(f"🚀 开始更新指数估值 (模式: {mode_str})...")

    # 1. 获取目标日期
    # 生产环境恢复为当前系统时间
    current_dt = datetime.now()

    if is_backfill:
        # 历史模式：抓过去 N 年
        start_date = current_dt - timedelta(days=365 * years)
        cal = pro.trade_cal(exchange='SSE', start_date=start_date.strftime('%Y%m%d'),
                            end_date=current_dt.strftime('%Y%m%d'))
        if cal.empty:
            print("❌ 获取日历失败")
            return
        trade_dates = cal[cal['is_open'] == 1]['cal_date'].tolist()
        trade_dates.reverse()
    else:
        # 每日模式：只抓今天
        # Tushare 估值数据通常在下午 17:00 后更新，如果在盘中运行可能会取不到
        trade_dates = [current_dt.strftime('%Y%m%d')]

    # 2. 循环处理
    for i, d in enumerate(trade_dates):
        try:
            # 只传日期，获取全市场指数数据
            df = pro.index_dailybasic(trade_date=d, fields='ts_code,trade_date,pe,pe_ttm,pb,total_mv,turnover_rate')

            if df.empty:
                if not is_backfill:
                    print(f"⚠️ {d} 暂无数据 (可能是周末、节假日或数据尚未更新)")
                continue

            # 本地筛选需要的指数
            target_codes = list(CORE_INDICES.keys())
            df_filtered = df[df['ts_code'].isin(target_codes)].copy()

            if df_filtered.empty:
                continue

            df_filtered = df_filtered.fillna(0)

            # 入库
            with engine.connect() as conn:
                trans = conn.begin()
                try:
                    conn.execute(text(f"DELETE FROM index_valuation WHERE trade_date = '{d}'"))
                    df_filtered.to_sql('index_valuation', conn, if_exists='append', index=False)
                    trans.commit()
                except Exception as db_e:
                    trans.rollback()
                    print(f"❌ 入库失败: {db_e}")

            if not is_backfill:
                print(f"✅ {d} 指数估值更新成功！({len(df_filtered)}条)")
            elif i % 10 == 0:
                print(f"✅ [{i + 1}/{len(trade_dates)}] {d} 补全成功")

            if is_backfill: time.sleep(0.12)

        except Exception as e:
            print(f"❌ {d} 异常: {e}")


if __name__ == "__main__":
    # 🔥 生产环境：每日更新模式
    update_index_valuation(is_backfill=False)