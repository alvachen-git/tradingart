import tushare as ts
import pandas as pd
import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import time
from datetime import datetime, timedelta

# 1. 初始化
load_dotenv(override=True)
ts.set_token(os.getenv("TUSHARE_TOKEN"))
pro = ts.pro_api()

DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")

db_url = f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(db_url)


def fetch_sector_moneyflow(trade_date):
    print(f"🚀 正在抓取 {trade_date} 的板块资金流 (东方财富源)...")

    try:
        # 接口: 东方财富板块资金流
        # 字段: trade_date, ts_code, name, pct_change, net_amount(净流入), net_amount_rate(净流入率), close
        df = pro.moneyflow_ind_dc(trade_date=trade_date)

        if df.empty:
            print(f"   ⚠️ {trade_date} 无数据 (可能是非交易日)")
            return

        # --- 数据清洗 ---

        # 1. 筛选：只保留“概念”板块 (该接口混合了行业和概念)
        # 东方财富的概念板块通常更活跃，适合做热点追踪
        if 'content_type' in df.columns:
            df = df[df['content_type'] == '概念'].copy()

        # 2. 计算【主力净流入】 (单位转换：元 -> 万元)
        # net_amount 就是主力净流入
        df['main_net_inflow'] = df['net_amount'] / 10000.0

        # 3. 核心计算：反推【总成交额】
        # 公式：总成交额 = 净流入额 / (净流入率 / 100)
        # 这一步完美解决了之前“缺 amount” 的问题
        def calc_turnover(row):
            try:
                rate = row['net_amount_rate']
                net = row['net_amount']
                if rate == 0:
                    return 0
                # 结果转为万元
                return (net / (rate / 100.0)) / 10000.0
            except:
                return 0

        df['total_turnover'] = df.apply(calc_turnover, axis=1)

        # 4. 字段重命名 (适配数据库)
        df.rename(columns={
            'name': 'industry',
            'net_amount_rate': 'net_rate'
        }, inplace=True)

        # 5. 准备入库
        # 确保有涨跌幅字段
        if 'pct_change' not in df.columns:
            df['pct_change'] = 0

        data_to_save = df[[
            'trade_date', 'industry', 'main_net_inflow',
            'total_turnover', 'pct_change', 'net_rate'
        ]].copy()

        # 填充可能出现的空值
        data_to_save.fillna(0, inplace=True)

        # 简单去重 (防止东财数据偶发的重复)
        data_to_save = data_to_save.drop_duplicates(subset=['trade_date', 'industry'])

        # --- 入库 ---
        with engine.begin() as conn:
            # 幂等性删除：防止重复插入
            conn.execute(text(f"DELETE FROM sector_moneyflow WHERE trade_date='{trade_date}'"))
            data_to_save.to_sql('sector_moneyflow', conn, if_exists='append', index=False)

        print(f"   ✅ 成功入库 {len(data_to_save)} 条数据 (已自动计算成交额)")

    except Exception as e:
        print(f"   ❌ 抓取失败: {e}")
        # 权限提示
        if "permission" in str(e).lower():
            print("   💡 提示：请确认 Tushare 积分 >= 5000 (moneyflow_ind_dc 要求)")


if __name__ == "__main__":
    # 补抓最近 5 个交易日
    end_date = datetime.now()
    start_date = end_date - timedelta(days=1)

    current = start_date
    while current <= end_date:
        d_str = current.strftime('%Y%m%d')
        # 简单跳过周末
        if current.weekday() < 5:
            fetch_sector_moneyflow(d_str)
            time.sleep(1)  # 增加延时
        current += timedelta(days=1)