import tushare as ts
import pandas as pd
import akshare as ak
from sqlalchemy import create_engine, text
import os
from dotenv import load_dotenv
import time
from datetime import datetime, timedelta

# 1. 初始化
load_dotenv(override=True)
db_url = f"mysql+pymysql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
engine = create_engine(db_url)
TUSHARE_TOKEN = os.getenv("TUSHARE_TOKEN")
pro = ts.pro_api(TUSHARE_TOKEN) if TUSHARE_TOKEN else None


def init_index_table():
    """初始化指数价格表"""
    with engine.connect() as conn:
        sql = """
              CREATE TABLE IF NOT EXISTS index_price \
              ( \
                  trade_date \
                  VARCHAR \
              ( \
                  20 \
              ),
                  ts_code VARCHAR \
              ( \
                  20 \
              ),
                  open_price FLOAT,
                  high_price FLOAT,
                  low_price FLOAT,
                  close_price FLOAT,
                  pct_chg FLOAT,
                  vol FLOAT,
                  amount FLOAT,
                  PRIMARY KEY \
              ( \
                  trade_date, \
                  ts_code \
              )
                  ) DEFAULT CHARSET=utf8mb4;
              """
        conn.execute(text(sql))


def fetch_and_save_indices(start_date, end_date):
    # 指数列表
    indices = {
        '000001.SH': '上证指数',
        '399001.SZ': '深证成指',
        '000300.SH': '沪深300',
        '000905.SH': '中证500',
        '000852.SH': '中证1000',
        '000688.SH': '科创50',
        '399006.SZ': '创业板指',
        '000016.SH': '上证50',
        '399005.SZ': '中小100',
        '932000.CSI': '中证2000',
    }

    print(f"🚀 开始拉取指数数据: {start_date} 至 {end_date} ...")

    for code, name in indices.items():
        try:
            # 1. 清理旧数据 (幂等性)
            with engine.connect() as conn:
                del_sql = text(
                    f"DELETE FROM index_price WHERE ts_code='{code}' AND trade_date >= '{start_date}' AND trade_date <= '{end_date}'")
                conn.execute(del_sql)
                conn.commit()

            # 2. 调用接口
            df = pro.index_daily(ts_code=code, start_date=start_date, end_date=end_date)

            if not df.empty:
                rename_map = {
                    'close': 'close_price',
                    'open': 'open_price',
                    'high': 'high_price',
                    'low': 'low_price',
                }
                df = df.rename(columns=rename_map)

                cols_to_save = ['trade_date', 'ts_code', 'open_price', 'high_price',
                                'low_price', 'close_price', 'pct_chg', 'vol', 'amount']
                final_cols = [c for c in cols_to_save if c in df.columns]

                # 3. 入库
                df[final_cols].to_sql('index_price', engine, if_exists='append', index=False)
                print(f"   [√] {name} 更新成功")
            else:
                print(f"   [-] {name} 无数据 (可能是非交易日或收盘前)")

            time.sleep(0.3)

        except Exception as e:
            print(f"   [x] {name} 异常: {e}")


def fetch_and_save_hk_indices(start_date, end_date):
    """
    抓取港股核心指数 (使用 AkShare)
    并自动计算涨跌幅
    """
    # 强制转为字符串，防止报错
    start_date = str(start_date)
    end_date = str(end_date)

    # 映射表: AkShare代码 -> 中文名
    # HSI: 恒生指数, HSTECH: 恒生科技指数
    hk_indices = {
        'HSI': '恒生指数',
        'HSTECH': '恒生科技指数'
    }

    print(f"🚀 [港股] 开始拉取指数数据: {start_date} 至 {end_date} ...")

    for symbol, name in hk_indices.items():
        try:
            # 1. 拉取数据（多源容错）
            source_used = None
            df = pd.DataFrame()
            latest_from_source = None
            source_results = []

            def fetch_tushare_hk_index(ts_code):
                if pro is None:
                    raise ValueError("TUSHARE_TOKEN 缺失，无法使用 tushare 兜底")
                # 取较长历史区间，便于后续统一计算 pct_chg
                df_ts = pro.query("index_global", ts_code=ts_code, start_date="20000101", end_date=end_date)
                if df_ts is None or df_ts.empty:
                    return pd.DataFrame()
                rename_map_ts = {
                    "trade_date": "date",
                    "vol": "volume",
                }
                df_ts = df_ts.rename(columns=rename_map_ts)
                return df_ts

            def fetch_hk_spot_today(ts_code):
                """当日实时兜底：仅在 end_date=今天且收盘后尝试抓取快照。"""
                today_str = datetime.now().strftime('%Y%m%d')
                if end_date != today_str:
                    return pd.DataFrame()
                # 港股 16:00 收盘，保守留到 16:20 后再使用快照写入日线
                if datetime.now().hour < 16:
                    return pd.DataFrame()
                df_spot = ak.stock_hk_index_spot_sina()
                if df_spot is None or df_spot.empty:
                    return pd.DataFrame()
                code_map = {'HSI': 'hkHSI', 'HSTECH': 'hkHSTECH'}
                target_code = code_map.get(ts_code, f'hk{ts_code}')
                one = df_spot[df_spot['代码'].astype(str).str.upper() == str(target_code).upper()]
                if one.empty:
                    return pd.DataFrame()
                row = one.iloc[0]
                return pd.DataFrame([{
                    'date': today_str,
                    'open': row.get('今开', 0),
                    'high': row.get('最高', 0),
                    'low': row.get('最低', 0),
                    'close': row.get('最新价', 0),
                    'pct_chg': row.get('涨跌幅', 0),
                    'volume': 0,
                }])

            source_candidates = [
                ("tushare", lambda: fetch_tushare_hk_index(symbol)),
                ("sina", lambda: ak.stock_hk_index_daily_sina(symbol=symbol)),
                ("em", lambda: ak.stock_hk_index_daily_em(symbol=symbol)),
                ("spot_today", lambda: fetch_hk_spot_today(symbol)),
            ]
            for source_name, fetcher in source_candidates:
                try:
                    retries_map = {"sina": 2, "em": 4, "tushare": 3, "spot_today": 2}
                    max_retries = retries_map.get(source_name, 2)
                    raw_df = None
                    for attempt in range(1, max_retries + 1):
                        try:
                            raw_df = fetcher()
                            if attempt > 1:
                                print(f"   [i] {name} {source_name} 第 {attempt} 次重试成功")
                            break
                        except Exception as retry_err:
                            if attempt < max_retries:
                                wait_s = min(8, attempt * 1.5)
                                print(
                                    f"   [!] {name} {source_name} 第 {attempt}/{max_retries} 次失败: {retry_err}，{wait_s:.1f}s 后重试"
                                )
                                time.sleep(wait_s)
                            else:
                                print(
                                    f"   [x] {name} {source_name} 第 {attempt}/{max_retries} 次失败: {retry_err}"
                                )

                    if raw_df is None:
                        continue

                    if raw_df is None or raw_df.empty:
                        print(f"   [-] {name} {source_name} 返回空数据")
                        continue

                    one_df = raw_df.copy()
                    # em 接口字段是 latest，这里统一成 close
                    if source_name == "em" and "latest" in one_df.columns and "close" not in one_df.columns:
                        one_df["close"] = one_df["latest"]

                    if 'date' not in one_df.columns or 'close' not in one_df.columns:
                        print(f"   [!] {name} {source_name} 缺少关键列: {one_df.columns.tolist()}")
                        continue

                    # 统一日期格式
                    one_df['trade_date'] = pd.to_datetime(one_df['date'], errors='coerce').dt.strftime('%Y%m%d')
                    one_df = one_df.dropna(subset=['trade_date']).copy()
                    if one_df.empty:
                        print(f"   [-] {name} {source_name} 日期解析后为空")
                        continue

                    latest_from_source = one_df['trade_date'].max()
                    print(f"   [i] {name} {source_name} 最新日期: {latest_from_source}")
                    source_results.append((latest_from_source, source_name, one_df))
                except Exception as source_err:
                    print(f"   [x] {name} {source_name} 异常: {source_err}")
                    continue

            if not source_results:
                print(f"   [-] {name} 所有数据源均不可用")
                continue

            # 选择“最新日期”更大的来源，避免主源停更但仍命中 start_date 的情况
            source_results.sort(key=lambda x: x[0], reverse=True)
            latest_from_source, source_used, df = source_results[0]
            if latest_from_source < end_date:
                print(
                    f"   [!] {name} 所有源最新仅到 {latest_from_source}，目标结束日期为 {end_date}"
                )

            # 2. 数据清洗与计算
            for col in ['open', 'high', 'low', 'close', 'volume', 'pct_chg']:
                if col not in df.columns:
                    df[col] = 0

            # 统一数值类型
            for col in ['open', 'high', 'low', 'close', 'volume', 'pct_chg']:
                df[col] = pd.to_numeric(df[col], errors='coerce')

            # 计算涨跌幅（优先基于 close 连续计算；若首行无前值则保留源端 pct_chg）
            # 逻辑：(今收 - 昨收) / 昨收 * 100
            df = df.sort_values('trade_date').reset_index(drop=True)
            source_pct = df['pct_chg'].copy()
            df['pct_chg'] = df['close'].pct_change() * 100
            df['pct_chg'] = df['pct_chg'].where(df['pct_chg'].notna(), source_pct)
            df['pct_chg'] = df['pct_chg'].fillna(0).round(4)  # 保留4位小数

            # 3. 过滤日期区间
            mask = (df['trade_date'] >= start_date) & (df['trade_date'] <= end_date)
            df_filtered = df.loc[mask].copy()

            if df_filtered.empty:
                print(
                    f"   [-] {name} 区间内无数据 (source={source_used}, latest={latest_from_source}, target={start_date}~{end_date})"
                )
                continue

            # 字段重命名
            rename_map = {
                'open': 'open_price',
                'high': 'high_price',
                'low': 'low_price',
                'close': 'close_price',
                'volume': 'vol'
            }
            df_filtered = df_filtered.rename(columns=rename_map)

            # 补充字段
            df_filtered['ts_code'] = symbol
            df_filtered['amount'] = 0

            # 4. 入库（仅替换“本次已获取到的日期”，避免删空区间）
            cols_to_save = ['trade_date', 'ts_code', 'open_price', 'high_price',
                            'low_price', 'close_price', 'pct_chg', 'vol', 'amount']

            for c in cols_to_save:
                if c not in df_filtered.columns:
                    df_filtered[c] = 0

            replace_dates = sorted(df_filtered['trade_date'].dropna().astype(str).unique().tolist())
            with engine.connect() as conn:
                trans = conn.begin()
                try:
                    del_sql = text("DELETE FROM index_price WHERE ts_code=:ts_code AND trade_date=:trade_date")
                    for one_day in replace_dates:
                        conn.execute(del_sql, {"ts_code": symbol, "trade_date": one_day})
                    df_filtered[cols_to_save].to_sql('index_price', conn, if_exists='append', index=False)
                    trans.commit()
                except Exception:
                    trans.rollback()
                    raise
            print(f"   [√] {name} 更新成功 ({len(df_filtered)} 条，source={source_used})")

            time.sleep(1)

        except Exception as e:
            print(f"   [x] {name} 异常: {e}")


if __name__ == "__main__":
    init_index_table()

    # 自动获取今天
    today = datetime.now().strftime('%Y%m%d')

    # 如果想补全历史数据，可以把下面这行解开注释并修改日期：
    # fetch_and_save_hk_indices("20240101", today)

    print(f"⚡️ 正在执行每日更新模式: {today}")

    # 更新 A 股
    fetch_and_save_indices(today, today)

    # 更新 港股
    fetch_and_save_hk_indices(today, today)

    print("\n=== ✅ 所有指数数据更新结束 ===")
