import pandas as pd
import tushare as ts
from sqlalchemy import create_engine, text, types
import os
import re
from dotenv import load_dotenv
import time
from datetime import datetime, timedelta
import gc
import akshare as ak  # 确保已安装: pip install akshare --upgrade

# --- 1. 初始化配置 ---
load_dotenv(override=True)

# 数据库配置
DB_USER = 'root'
# 建议检查 .env 是否配置了密码，这里保留你原文件的硬编码作为备选
DB_PASSWORD = os.getenv("DB_PASSWORD", 'alva13557941')
DB_HOST = '39.102.215.198'
DB_PORT = '3306'
DB_NAME = 'finance_data'

db_url = f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
# 增加 pool_recycle 防止数据库连接超时断开
engine = create_engine(db_url, pool_recycle=3600)

# Tushare 配置
token = os.getenv("TUSHARE_TOKEN")
if not token:
    print("❌ 错误：未找到 TUSHARE_TOKEN，请在 .env 文件中配置。")
    exit()

ts.set_token(token)
pro = ts.pro_api()


# ==========================================
#  新增：AkShare 广期所(GFEX) 全品种专用补丁
#  覆盖品种：si(工业硅), lc(碳酸锂), ps(多晶硅), pt(铂金), pd(钯金)
# ==========================================
def get_gfex_function():
    """自动查找正确的广期所函数名"""
    if hasattr(ak, 'futures_gfex_position_rank'):
        return ak.futures_gfex_position_rank
    candidates = ['futures_hold_rank_gfex', 'get_gfex_rank_table', 'futures_gfex_holding_rank']
    for c in candidates:
        if hasattr(ak, c): return getattr(ak, c)
    return None


def fetch_gfex_patch(date_str):
    """
    广期所全品种补录函数
    逻辑：AkShare抓取 -> 筛选目标品种 -> 清洗宽表 -> 转长表 -> 聚合 -> 复用 save_to_db 入库
    """
    # 定义我们要补录的广期所品种列表
    TARGET_VARIETIES = ['si', 'lc', 'ps', 'pt', 'pd']
    target_str = "|".join(TARGET_VARIETIES)  # 用于正则匹配，如 "si|lc|ps|pt|pd"

    print(f" [*] [补丁] 正在通过 AkShare 修补广期所数据 ({target_str}) {date_str} ...", end="")
    try:
        func = get_gfex_function()
        if not func: return

        # 1. 调用接口 (AkShare 会一次性返回该交易所当天的所有数据)
        raw_data = func(date=date_str)

        # 2. 处理字典/DataFrame 兼容性
        df = pd.DataFrame()
        if isinstance(raw_data, dict):
            dfs = []
            for key, val in raw_data.items():
                if isinstance(val, pd.DataFrame): dfs.append(val)
            if dfs: df = pd.concat(dfs, ignore_index=True)
        elif isinstance(raw_data, pd.DataFrame):
            df = raw_data

        if df.empty:
            print(" [-] AkShare 返回空")
            return

        # 3. 超级映射表 (兼容各种列名)
        rename_dict = {
            'symbol': 'ts_code', '合约代码': 'ts_code', 'variety': 'variety',
            # 三榜合一的关键列名
            'vol_party_name': 'vol_party_name', '成交量会员简称': 'vol_party_name',
            'long_party_name': 'long_party_name', '持买单会员简称': 'long_party_name',
            'short_party_name': 'short_party_name', '持卖单会员简称': 'short_party_name',
            # 数值列
            'vol': 'vol', '成交量': 'vol', 'vol_chg': 'vol_chg', '成交量增减': 'vol_chg',
            'long_open_interest': 'long_vol', '持买单量': 'long_vol', '买持仓': 'long_vol',
            'long_open_interest_chg': 'long_chg', '持买单量增减': 'long_chg', '买持仓增减': 'long_chg',
            'short_open_interest': 'short_vol', '持卖单量': 'short_vol', '卖持仓': 'short_vol',
            'short_open_interest_chg': 'short_chg', '持卖单量增减': 'short_chg', '卖持仓增减': 'short_chg'
        }
        df = df.rename(columns=rename_dict)

        # 4. 【核心修改】筛选目标品种 (使用正则匹配 si, lc, ps 等)
        if 'ts_code' not in df.columns: return

        # 这里的正则意思是：只要 ts_code 包含 si 或 lc 或 ps... (忽略大小写)
        df = df[df['ts_code'].str.contains(target_str, case=False, na=False)]

        if df.empty:
            print(f" [-] 无相关品种数据")
            return

        # 5. 宽表转长表 (拆解三榜合一)
        expected_cols = ['vol_party_name', 'vol', 'vol_chg',
                         'long_party_name', 'long_vol', 'long_chg',
                         'short_party_name', 'short_vol', 'short_chg']
        for c in expected_cols:
            if c not in df.columns:
                df[c] = None if 'name' in c else 0

        # A. 拆解成交量
        df_vol = df[['ts_code', 'vol_party_name', 'vol', 'vol_chg']].rename(columns={'vol_party_name': 'broker'})
        df_vol['long_vol'] = 0;
        df_vol['long_chg'] = 0;
        df_vol['short_vol'] = 0;
        df_vol['short_chg'] = 0

        # B. 拆解买单
        df_long = df[['ts_code', 'long_party_name', 'long_vol', 'long_chg']].rename(
            columns={'long_party_name': 'broker'})
        df_long['vol'] = 0;
        df_long['vol_chg'] = 0;
        df_long['short_vol'] = 0;
        df_long['short_chg'] = 0

        # C. 拆解卖单
        df_short = df[['ts_code', 'short_party_name', 'short_vol', 'short_chg']].rename(
            columns={'short_party_name': 'broker'})
        df_short['vol'] = 0;
        df_short['vol_chg'] = 0;
        df_short['long_vol'] = 0;
        df_long['long_chg'] = 0

        # D. 合并
        df_combined = pd.concat([df_vol, df_long, df_short], ignore_index=True)

        # 6. 清洗
        df_combined = df_combined.dropna(subset=['broker'])
        df_combined = df_combined[df_combined['broker'].astype(str).str.len() > 1]
        df_combined = df_combined[~df_combined['broker'].isin(['-', 'None', 'nan'])]

        num_cols = ['vol', 'vol_chg', 'long_vol', 'long_chg', 'short_vol', 'short_chg']
        for c in num_cols:
            df_combined[c] = df_combined[c].astype(str).str.replace(',', '', regex=False)
            df_combined[c] = pd.to_numeric(df_combined[c], errors='coerce').fillna(0)

        # 提取纯代码 (si2501 -> si, lc2501 -> lc)
        df_combined['ts_code'] = df_combined['ts_code'].apply(lambda x: re.sub(r'\d+', '', str(x)).lower().strip())

        # 7. 聚合与计算
        df_final = df_combined.groupby(['ts_code', 'broker'])[num_cols].sum().reset_index()
        df_final['trade_date'] = date_str
        df_final['net_vol'] = df_final['long_vol'] - df_final['short_vol']

        # 8. 准备入库
        db_cols = ['trade_date', 'ts_code', 'broker', 'long_vol', 'long_chg', 'short_vol', 'short_chg', 'net_vol']
        for c in db_cols:
            if c not in df_final.columns: df_final[c] = 0

        save_data = df_final[db_cols].copy()

        # 9. 复用原本的 save_to_db (享受内存优化)
        # 注意：save_to_db 内部会根据传入的品种(ts_code)自动删除旧数据
        # 所以如果 Tushare 抓了一部分 lc，这里传入新的 lc 会覆盖掉 Tushare 的，保证数据是 AkShare 的完整版
        save_to_db(save_data, date_str)

        # 清理内存
        del df, df_vol, df_long, df_short, df_combined, df_final, save_data
        gc.collect()

    except Exception as e:
        print(f" [!] AkShare 补丁异常: {e}")


# --- 2. 核心逻辑：获取、清洗、筛选字段 ---
def fetch_and_save_tushare(date_str, exchange):
    """
    exchange: GFEX(广期), DCE(大商), CZCE(郑商), SHFE(上期), CFFEX(中金)
    """
    print(f"[*] 正在请求 Tushare [{exchange}] {date_str} ...", end="")

    try:
        # 1. 调用接口
        df = pro.fut_holding(trade_date=date_str, exchange=exchange)

        if not df.empty:
            # 2. 数据预处理
            df['ts_code'] = df['symbol'].apply(lambda x: re.sub(r'\d+', '', x).lower().strip())

            num_cols = ['long_hld', 'long_chg', 'short_hld', 'short_chg']
            for c in num_cols:
                df[c] = pd.to_numeric(df[c], errors='coerce').fillna(0)

            # 3. 聚合
            df_agg = df.groupby(['trade_date', 'ts_code', 'broker'])[num_cols].sum().reset_index()

            del df
            gc.collect()

            # 4. 重命名
            df_agg = df_agg.rename(columns={
                'long_hld': 'long_vol',
                'long_chg': 'long_chg',
                'short_hld': 'short_vol',
                'short_chg': 'short_chg'
            })

            # 5. 计算净持仓
            df_agg['net_vol'] = df_agg['long_vol'] - df_agg['short_vol']

            db_columns = [
                'trade_date', 'ts_code', 'broker',
                'long_vol', 'long_chg', 'short_vol', 'short_chg', 'net_vol'
            ]

            df_final = df_agg[db_columns].copy()

            del df_agg
            gc.collect()

            # 6. 入库
            save_to_db(df_final, date_str)

            del df_final
            gc.collect()
        else:
            print(" [-] Tushare 无数据", end="")

        # ==========================================
        #  修改处：在 Tushare 逻辑执行完后，启动广期所补丁
        # ==========================================
        if exchange == 'GFEX':
            # 这里的补丁现在会覆盖 Tushare 可能不完整的数据 (si, lc, ps, pt, pd)
            # 以 AkShare 官网数据为准
            print("")  # 换行
            fetch_gfex_patch(date_str)

    except Exception as e:
        print(f" [!] 异常: {e}")


def save_to_db(df, date_str):
    if df.empty: return
    try:
        symbols = df['ts_code'].unique().tolist()
        symbols_str = "', '".join(symbols)

        with engine.connect() as conn:
            # 先删除旧数据 (防止重复)
            # 如果是 AkShare 补录，会删除同名品种，覆盖旧数据
            sql = f"DELETE FROM futures_holding WHERE trade_date='{date_str}' AND ts_code IN ('{symbols_str}')"
            conn.execute(text(sql))
            conn.commit()

        # --- 4. 核心优化：手动分批写入 + 强制休眠 ---
        # 你的服务器只有2G内存，这里必须切得很细，给Web服务留喘息时间

        batch_size = 1000  # 每次只写入 1000 条
        total_len = len(df)
        print(f" [Saving {total_len} rows] ", end="")

        for i in range(0, total_len, batch_size):
            # 切片
            chunk = df.iloc[i: i + batch_size]

            # 写入数据库
            chunk.to_sql('futures_holding', engine, if_exists='append', index=False)

            # 打印进度点
            print(".", end="", flush=True)

            # 关键：每写 1000 条，强制睡 0.5 秒
            # 这就是防止网站 502 的关键，把 CPU 让给 Nginx
            time.sleep(0.5)

            # 清理这一小块的内存
            del chunk
            gc.collect()

        print(f" [√] 完成")

    except Exception as e:
        print(f" [X] 数据库写入失败: {e}")


# --- 3. 批量运行 ---
def run_job(start_date, end_date):
    dates = pd.date_range(start=start_date, end=end_date)

    EXCHANGES = ['GFEX', 'SHFE', 'DCE', 'CZCE', 'CFFEX']

    for single_date in dates:
        date_str = single_date.strftime('%Y%m%d')
        if single_date.weekday() >= 5: continue

        print(f"\n--- 处理日期: {date_str} ---")
        for ex in EXCHANGES:
            fetch_and_save_tushare(date_str, ex)

            # 处理完一个交易所后，再休息一下
            time.sleep(1)


if __name__ == "__main__":
    today = datetime.now().strftime('%Y%m%d')
    start = (datetime.now() - timedelta(days=2)).strftime('%Y%m%d')

    # 若要补录旧数据，可手动修改 start
    # start = '20251126'

    print(f"开始任务: {start} -> {today}")
    run_job(start, today)