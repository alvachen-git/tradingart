import pandas as pd
import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from datetime import datetime, timedelta

# 1. 初始化
load_dotenv(override=True)
DB_USER = os.getenv("DB_USER", "root")
DB_PASSWORD = os.getenv("DB_PASSWORD", "alva13557941")
DB_HOST = os.getenv("DB_HOST", "39.102.215.198")
DB_PORT = os.getenv("DB_PORT", "3306")
DB_NAME = os.getenv("DB_NAME", "finance_data")

db_url = f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(db_url)

# --- 配置区域 ---
# 目标外资
TARGET_BROKERS = ['摩根大通', '乾坤期货', '瑞银期货']

# 【关键修改】只关注这 4 个品种
# au=黄金, ag=白银, sc=原油, m=豆粕
WATCH_LIST = ['au', 'ag', 'i', 'm']


def run_foreign_analysis():
    print("=== 开始计算外资动向 (限定品种) ===")

    # 1. 获取最新日期
    with engine.connect() as conn:
        latest_date = conn.execute(text("SELECT MAX(trade_date) FROM futures_holding")).scalar()

    if not latest_date:
        print("[-] 数据库无持仓数据")
        return

    print(f"[*] 分析日期: {latest_date}")

    # 2. 构建 SQL 查询
    # 只查指定品种 + 指定机构
    symbols_str = "', '".join(WATCH_LIST)

    sql = f"""
    SELECT ts_code, broker, net_vol, long_vol, short_vol 
    FROM futures_holding 
    WHERE trade_date = '{latest_date}' 
      AND (broker LIKE '%%摩根%%' OR broker LIKE '%%乾坤%%' OR broker LIKE '%%瑞银%%')
      AND ts_code IN ('{symbols_str}')
    """

    df = pd.read_sql(sql, engine)

    if df.empty:
        print(f"[-] 今日 ({latest_date}) 未找到外资在 {WATCH_LIST} 上的持仓记录")
        return

    # 3. 分析逻辑：计算“外资合力”
    # 对于每个品种，把所有外资的净持仓加起来
    # 如果总和 > 0 -> 外资整体做多
    # 如果总和 < 0 -> 外资整体做空

    results = []

    for symbol, group in df.groupby('ts_code'):
        # 计算合计净持仓
        total_net_vol = group['net_vol'].sum()

        # 判断方向
        if total_net_vol > 0:
            direction_str = "做多"
        elif total_net_vol < 0:
            direction_str = "做空"
        else:
            continue  # 净持仓为0，跳过

        # 记录参与的机构
        brokers_list = group['broker'].unique().tolist()
        brokers_str = ",".join(brokers_list)

        results.append({
            'trade_date': latest_date,
            'symbol': symbol,
            'direction': direction_str,
            'brokers': brokers_str,
            'avg_score': 0,  # 暂不计算分
            'total_net_vol': total_net_vol
        })

    # 4. 入库
    if results:
        df_res = pd.DataFrame(results)
        print(f"[√] 分析完成，共 {len(df_res)} 个品种有外资动向")
        print(df_res[['symbol', 'direction', 'total_net_vol', 'brokers']])

        with engine.connect() as conn:
            # 覆盖当日旧数据
            conn.execute(text(f"DELETE FROM foreign_capital_analysis WHERE trade_date='{latest_date}'"))
            conn.commit()

        df_res.to_sql('foreign_capital_analysis', engine, if_exists='append', index=False)
        print("[√] 入库成功")
    else:
        print("[-] 虽有持仓，但合计净量为0")


if __name__ == "__main__":
    run_foreign_analysis()