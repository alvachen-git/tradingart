import tushare as ts
import pandas as pd
from sqlalchemy import create_engine, text
import os
import time
from datetime import datetime
from dotenv import load_dotenv

# 1. 初始化
load_dotenv(override=True)
db_url = f"mysql+pymysql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
engine = create_engine(db_url)
ts.set_token(os.getenv("TUSHARE_TOKEN"))
pro = ts.pro_api()


def update_daily_valuation():
    """
    更新每日估值指标 (PE, PB, 市值等)
    接口: daily_basic
    """
    today = datetime.now().strftime('%Y%m%d')
    # 如果是补历史数据，可以手动修改这里，例如: today = '20250108'

    print(f"🚀 开始获取 {today} 的全市场估值数据...")

    try:
        # Tushare daily_basic 接口一次能取全市场
        df = pro.daily_basic(trade_date=today,
                             fields='ts_code,trade_date,pe_ttm,pb,ps_ttm,dv_ratio,total_mv,circ_mv,turnover_rate')

        if df.empty:
            print(f"⚠️ {today} 暂无估值数据 (可能是周末或晚上数据未更新)")
            return

        # 简单的清洗
        df = df.fillna(0)

        # 入库 (先删后插，防止重复)
        print(f"💾 正在写入 {len(df)} 条数据...")
        with engine.connect() as conn:
            # 删除旧数据
            conn.execute(text(f"DELETE FROM stock_valuation WHERE trade_date = '{today}'"))
            conn.commit()

            # 批量写入
            df.to_sql('stock_valuation', conn, if_exists='append', index=False, chunksize=2000)

        print("✅ 估值数据更新成功！")

    except Exception as e:
        print(f"❌ 更新失败: {e}")


if __name__ == "__main__":
    update_daily_valuation()