import akshare as ak
import pandas as pd
from sqlalchemy import create_engine, text
import os
import time
from datetime import datetime, timedelta
from dotenv import load_dotenv
# 如果服务器没有安装 tqdm，可以注释掉下面这行，并删除循环里的 tqdm
from tqdm import tqdm

# 1. 引入核心算法库 (请确保 kline_algo.py 在同级目录)
import kline_algo

# 2. 初始化环境
load_dotenv(override=True)

# 数据库配置
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")

# 建立连接池 (pool_recycle 防止连接超时断开)
db_url = f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(db_url, pool_recycle=3600)


def get_hk_stock_list():
    """高效获取港股名单"""
    print("📋 [1/4] 正在拉取港股实时目录...")
    try:
        df = ak.stock_hk_spot()
        # 简单清洗，确保只有主板或有成交量的股票
        # 这里只做基础筛选，保留代码和名称
        if '代码' in df.columns and '中文名称' in df.columns:
            # 过滤掉代码不是5位的奇怪品种
            df = df[df['代码'].astype(str).str.len() == 5]
            return df[['代码', '中文名称']].values.tolist()  # 返回列表 [[code, name], ...]
        else:
            print(f"❌ 接口列名变更: {df.columns}")
            return []
    except Exception as e:
        print(f"❌ 获取列表失败: {e}")
        return []


def run_hk_score_update():
    # === A. 设定时间窗口 ===
    # 目标日期：默认为今天
    #target_date = datetime.now().strftime('%Y%m%d')
    target_date="20260102"

    # 起始日期：往前推 180 天 (足够计算 MA60, MA120 等长周期均线)
    # 优化点：不要拉取几年的数据，浪费带宽和内存
    start_dt = datetime.now() - timedelta(days=100)
    start_date = start_dt.strftime('%Y%m%d')

    print(f"🚀 [港股] 启动形态扫描 | 目标: {target_date} | 回溯至: {start_date}")

    # === B. 获取名单 ===
    stock_list = get_hk_stock_list()
    if not stock_list:
        print("⚠️ 未获取到股票列表，任务终止")
        return

    print(f"📊 [2/4] 待分析股票: {len(stock_list)} 只")

    results_buffer = []

    # === C. 循环处理 (核心) ===
    # 使用 tqdm 显示进度条，如果是在无界面的服务器跑，建议保留打印即可
    for code, name in tqdm(stock_list, desc="计算进度"):
        ts_code = f"{code}.HK"

        try:
            # 1. 拉取历史 K 线 (只拉取 180 天)
            # adjust="" 不复权，通常形态分析用不复权数据
            df = ak.stock_hk_hist(
                symbol=code,
                period="daily",
                start_date=start_date,
                end_date=target_date,
                adjust=""
            )

            # 数据校验：如果为空或数据太少(无法算均线)，跳过
            if df.empty or len(df) < 30:
                continue

            # 2. 列名对齐 (AkShare -> kline_algo 标准)
            rename_map = {
                '日期': 'trade_date',
                '开盘': 'open_price', '收盘': 'close_price',
                '最高': 'high_price', '最低': 'low_price',
                '成交量': 'volume', '涨跌幅': 'pct_chg'
            }
            df = df.rename(columns=rename_map)

            # 格式清洗
            df['trade_date'] = pd.to_datetime(df['trade_date']).dt.strftime('%Y%m%d')
            numeric_cols = ['open_price', 'high_price', 'low_price', 'close_price', 'volume']
            for c in numeric_cols:
                df[c] = pd.to_numeric(df[c], errors='coerce')

            # 确保时间正序 (旧->新)
            df = df.sort_values('trade_date').reset_index(drop=True)

            # 3. 校验最后一天是否为目标日期
            # 防止停牌股混入 (比如今天跑脚本，但这只股最后交易日是上周)
            latest_row = df.iloc[-1]
            if latest_row['trade_date'] != target_date:
                continue

            # 4. 🔥 调用形态算法 (CPU计算密集型)
            algo_res = kline_algo.calculate_kline_signals(df)

            # 5. 存入缓冲区 (内存操作，极快)
            # 只有分数 > 0 或者有形态的才存，节省数据库空间 (可选项)
            # if algo_res['score'] > 0:
            results_buffer.append({
                'trade_date': target_date,
                'ts_code': ts_code,
                'name': name,
                'industry': '港股',
                'close': float(latest_row['close_price']),
                'pct_chg': float(latest_row['pct_chg']),
                'ma_trend': ",".join(algo_res['trends']),
                'pattern': ",".join(algo_res['patterns']),
                'score': int(algo_res['score']),
                'ai_summary': f"趋势:{','.join(algo_res['trends'])}。形态:{','.join(algo_res['patterns'])}。"
            })

            # 6. [效能优化] 礼貌休眠
            # 虽然会增加总耗时，但能显著降低被封 IP 的风险，也能降低 CPU 瞬时占用
            time.sleep(0.05)

        except Exception as e:
            # 捕获单只股票的错误，不中断循环
            # print(f"⚠️ {ts_code} 处理异常: {e}")
            continue

    # === D. 批量入库 (I/O 密集型) ===
    if results_buffer:
        print(f"💾 [3/4] 计算完成，准备写入 {len(results_buffer)} 条数据...")

        df_save = pd.DataFrame(results_buffer)

        with engine.connect() as conn:
            trans = conn.begin()
            try:
                # 1. 清理旧数据 (仅删除【当天】且【港股】的数据)
                # 这样即使你一天跑好几次，或者和 A 股脚本混跑，也不会互相覆盖
                print("🧹 清理旧数据...")
                del_sql = text("DELETE FROM daily_stock_screener WHERE trade_date = :d AND ts_code LIKE '%.HK'")
                conn.execute(del_sql, {"d": target_date})

                # 2. 批量写入
                # chunksize=500: 每 500 条生成一个 Insert 语句，平衡内存和速度
                print("📥 正在入库...")
                df_save.to_sql('daily_stock_screener', conn, if_exists='append', index=False, chunksize=500)

                trans.commit()
                print(f"✅ [4/4] 成功！港股形态评分已更新 ({len(df_save)} 条)")

                # 打印前三名看看效果
                print("\n🏆 今日港股前三名:")
                print(df_save.sort_values('score', ascending=False).head(3)[['ts_code', 'name', 'score', 'pattern']])

            except Exception as e:
                trans.rollback()
                print(f"❌ 数据库写入失败: {e}")
    else:
        print("⚠️ 今日无有效数据 (可能是休市或网络问题)")


if __name__ == "__main__":
    start_time = time.time()
    run_hk_score_update()
    print(f"⏱️ 总耗时: {time.time() - start_time:.2f} 秒")