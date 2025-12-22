import tushare as ts
import pandas as pd
from sqlalchemy import create_engine, text
import os
import time
from datetime import datetime
from dotenv import load_dotenv

# 1. 引入刚才新建的算法库
import kline_algo

load_dotenv(override=True)

# 2. 初始化数据库和 Tushare
# 务必确保 .env 文件里配置了 DB 信息和 TUSHARE_TOKEN
db_url = f"mysql+pymysql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
engine = create_engine(db_url)
ts.set_token(os.getenv("TUSHARE_TOKEN"))
pro = ts.pro_api()


def run_daily_scan():
    print("🚀 [盘后作业] 开始全市场 K 线形态扫描...")

    # 1. 获取全市场股票列表
    # 为了测试，这里限制跑前 50 只。生产环境请把 .head(50) 去掉。
    # exchange='' 代表所有交易所 (SSE, SZSE, BSE)
    print("📋 正在获取全市场股票列表...")
    stock_list = pro.stock_basic(exchange='', list_status='L', fields='ts_code,name,industry')
    #stock_list = stock_list.head(50) # <--- 测试时打开此注释

    # 设定分析日期
    # 如果是今天盘后跑，就用今天；如果是周末跑，需指定上一个交易日
    today = datetime.now().strftime('%Y%m%d')
    #today = '20251219' # 手动指定测试日期

    print(f"🎯 目标日期: {today} | 目标数量: {len(stock_list)} 只")

    data_buffer = []
    error_count = 0

    # 2. 循环处理每一只股票
    for index, row in stock_list.iterrows():
        ts_code = row['ts_code']
        name = row['name']

        try:
            # 拉取日线数据 (至少需要 60-80 天才能算准 MA60)
            # Tushare 接口频次有限制，注意流控
            df = pro.daily(ts_code=ts_code, end_date=today, limit=100)

            # 数据太少无法分析
            if df.empty or len(df) < 30:
                continue

            # 【关键适配】列名转换
            # Tushare 返回: trade_date, open, high, low, close, vol...
            # kline_algo 需要: open_price, high_price...
            df = df.rename(columns={
                'close': 'close_price',
                'open': 'open_price',
                'high': 'high_price',
                'low': 'low_price',
                'vol': 'volume'
            })

            # Tushare 默认是倒序 (最新在前)，必须转为正序 (时间在前) 给算法计算
            df = df.sort_values('trade_date').reset_index(drop=True)

            # 【核心步骤】调用独立算法库
            result = kline_algo.calculate_kline_signals(df)

            # 只有当最新数据的日期 == 目标日期，才入库
            # (防止停牌股票用旧数据充数)
            latest = df.iloc[-1]
            if latest['trade_date'] != today:
                continue

            # 准备入库数据
            pattern_str = ",".join(result['patterns'])
            trend_str = ",".join(result['trends'])

            # 只有当有形态或趋势时才记录 (节省空间)，或者全部记录
            record = {
                'trade_date': latest['trade_date'],
                'ts_code': ts_code,
                'name': name,
                'industry': row['industry'],
                'close': latest['close_price'],
                'pct_chg': latest['pct_chg'],  # Tushare 这一列叫 pct_chg
                'ma_trend': trend_str,
                'pattern': pattern_str,
                'score': result['score'],
                # 生成一句给 AI 读的摘要
                'ai_summary': f"趋势:{trend_str}。形态:{pattern_str}。"
            }
            data_buffer.append(record)

            # 进度条 & 简单流控 (每 100 只打印一次)
            if index % 100 == 0:
                print(f"✅ [{index}/{len(stock_list)}] {ts_code} {name}: {pattern_str} (Score: {result['score']})")
                time.sleep(0.1)  # 防止 Tushare 封 IP

        except Exception as e:
            error_count += 1
            # print(f"❌ {ts_code} 出错: {e}")
            continue

    # 3. 批量写入数据库
    if data_buffer:
        print(f"💾 正在将 {len(data_buffer)} 条结果写入数据库...")

        df_result = pd.DataFrame(data_buffer)

        with engine.connect() as conn:
            # 幂等性设计：先删除今日已跑过的数据，防止重复插入报错
            del_sql = text(f"DELETE FROM daily_stock_screener WHERE trade_date = '{today}'")
            conn.execute(del_sql)
            conn.commit()
            print("🗑️ 旧数据清理完成")

        # 写入新数据
        # if_exists='append' 表示追加
        df_result.to_sql('daily_stock_screener', engine, if_exists='append', index=False)
        print(f"🎉 作业完成！成功入库: {len(data_buffer)} 条 | 失败/跳过: {error_count} 条")

        # 简单统计
        top_picks = df_result.sort_values('score', ascending=False).head(5)
        print("\n🏆 今日最高分前 5 名:")
        print(top_picks[['ts_code', 'name', 'score', 'pattern']])

    else:
        print("⚠️ 今日无有效数据生成 (可能是非交易日或接口限制)")


if __name__ == "__main__":
    run_daily_scan()