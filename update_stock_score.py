import tushare as ts
import pandas as pd
from sqlalchemy import create_engine, text
import os
import time
from datetime import datetime
from dotenv import load_dotenv
from typing import Optional

# 1. 引入刚才新建的算法库
import kline_algo

load_dotenv(override=True)

# 2. 初始化数据库和 Tushare
# 务必确保 .env 文件里配置了 DB 信息和 TUSHARE_TOKEN
db_url = f"mysql+pymysql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
engine = create_engine(db_url)
ts.set_token(os.getenv("TUSHARE_TOKEN"))
pro = ts.pro_api()


def _fetch_daily_with_retry(ts_code: str, end_date: str, limit: int = 100, retries: int = 2) -> Optional[pd.DataFrame]:
    last_err = None
    for i in range(retries + 1):
        try:
            return pro.daily(ts_code=ts_code, end_date=end_date, limit=limit)
        except Exception as e:
            last_err = e
            time.sleep(0.2 * (i + 1))
    raise last_err


def run_daily_scan():
    print("🚀 [盘后作业] 开始全市场 K 线形态扫描...")

    # 1. 获取全市场股票列表
    # 为了测试，这里限制跑前 50 只。生产环境请把 .head(50) 去掉。
    # exchange='' 代表所有交易所 (SSE, SZSE, BSE)
    print("📋 正在获取全市场股票列表...")
    stock_list = pro.stock_basic(exchange='', list_status='L', fields='ts_code,name,industry')
    #stock_list = stock_list.head(50) # <--- 测试时打开此注释

    # 设定分析目标日期：用“可获取到K线数据的最新交易日”，避免脚本在盘中/周末运行时整批跳过。
    today = datetime.now().strftime('%Y%m%d')
    target_date = today
    try:
        probe = pro.daily(ts_code='000001.SH', end_date=today, limit=1)
        if probe is not None and (not probe.empty):
            target_date = str(probe.iloc[0]['trade_date'])
    except Exception as e:
        print(f"⚠️ 探测最新交易日失败，回退使用系统日期 {today}: {e}")

    print(f"🎯 目标日期: {target_date} | 目标数量: {len(stock_list)} 只")

    data_buffer = []
    error_count = 0
    error_samples = []

    # 2. 循环处理每一只股票
    for index, row in stock_list.iterrows():
        ts_code = row['ts_code']
        name = row['name']

        try:
            # 拉取日线数据 (至少需要 60-80 天才能算准 MA60)
            # Tushare 接口频次有限制，注意流控
            df = _fetch_daily_with_retry(ts_code=ts_code, end_date=target_date, limit=100, retries=2)
            time.sleep(0.015)

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
            if latest['trade_date'] != target_date:
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
            if len(error_samples) < 12:
                error_samples.append(f"{ts_code}: {e}")
            continue

    print(f"📌 扫描完成：成功 {len(data_buffer)} / {len(stock_list)}，失败 {error_count}")
    if error_samples:
        print("⚠️ 失败样例（最多12条）：")
        for msg in error_samples:
            print(f"   - {msg}")
    if len(stock_list) > 0:
        coverage = len(data_buffer) / len(stock_list)
        if coverage < 0.8:
            print(f"⚠️ 覆盖率偏低：{coverage:.1%}，可能存在 API 限流或网络问题。")

    # 3. 批量写入数据库 (升级版：带容错救援机制)
    if data_buffer:
        print(f"💾 准备写入 {len(data_buffer)} 条结果...")
        df_result = pd.DataFrame(data_buffer)

        # 1. 清理旧数据
        try:
            with engine.connect() as conn:
                conn.execute(text(f"DELETE FROM daily_stock_screener WHERE trade_date = '{target_date}'"))
                conn.commit()
                print("🗑️ 旧数据清理完成")
        except Exception as e:
            print(f"⚠️ 清理旧数据失败 (可能是表不存在，跳过): {e}")

        # 2. 尝试批量写入 (快)
        try:
            # chunksize=100 表示分批次写入
            df_result.to_sql('daily_stock_screener', engine, if_exists='append', index=False, chunksize=100)
            print(f"🎉 完美成功！今日 {len(df_result)} 条选股数据已全部入库。")

        except Exception as e:
            print(f"❌ 批量写入遭遇错误: {e}")
            print("🔄 正在启动【逐条救援模式】，尝试抢救有效数据...")

            success_count = 0
            fail_count = 0

            # 3. 逐条写入 (慢，但是稳)
            for i, row in df_result.iterrows():
                try:
                    # 将单行转为 DataFrame 写入
                    pd.DataFrame([row]).to_sql('daily_stock_screener', engine, if_exists='append', index=False)
                    success_count += 1
                except Exception as inner_e:
                    fail_count += 1
                    # 🌟 打印具体是哪只股票出了问题，方便您排查
                    print(f"   ⚠️ 写入失败 [{row['ts_code']} {row['name']}]: {inner_e}")

            print(f"🏁 救援完成: 成功入库 {success_count} 条 | 丢弃坏数据 {fail_count} 条")

        # 4. 显示前三名
        if not df_result.empty:
            print("\n🏆 今日【形态评分】前 5 名:")
            # 简单的按分数排序显示
            try:
                top = df_result.sort_values('score', ascending=False).head(5)
                for _, row in top.iterrows():
                    print(f"   - {row['name']} ({row['ts_code']}): {row['score']}分 | {row['pattern']}")
            except:
                pass

    else:
        print("⚠️ 今日无有效数据生成 (可能是周末或接口没数据)")


if __name__ == "__main__":
    run_daily_scan()
