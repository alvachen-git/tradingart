import tushare as ts
import pandas as pd
from sqlalchemy import create_engine, text, types
import os
import argparse
from dotenv import load_dotenv
from datetime import datetime, timedelta
import time

from astock_target_supplements import a_share_supplement_targets

# 1. 初始化
load_dotenv(override=True)

engine = None
pro = None
NAME_MAP = None

DEFAULT_NEW_STOCK_TARGETS = ["301525.SZ", "301528.SZ", "301529.SZ"]


def get_engine():
    global engine
    if engine is None:
        db_user = os.getenv("DB_USER")
        db_password = os.getenv("DB_PASSWORD")
        db_host = os.getenv("DB_HOST")
        db_port = os.getenv("DB_PORT")
        db_name = os.getenv("DB_NAME")
        db_url = f"mysql+pymysql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
        engine = create_engine(db_url)
    return engine


def get_pro():
    global pro
    if pro is None:
        ts.set_token(os.getenv("TUSHARE_TOKEN"))
        pro = ts.pro_api()
    return pro


# --- 獲取全市場名稱字典 (用於填補 name) ---
def get_name_map():
    global NAME_MAP
    if NAME_MAP is not None:
        return NAME_MAP

    print("[*] 正在加載名稱字典...")
    try:
        # 股票
        ts_pro = get_pro()
        df_s = ts_pro.stock_basic(exchange='', list_status='L', fields='ts_code,name')
        # 基金/ETF
        df_f = ts_pro.fund_basic(market='E', status='L', fields='ts_code,name')
        df_all = pd.concat([df_s, df_f])
        NAME_MAP = dict(zip(df_all['ts_code'], df_all['name']))
    except:
        NAME_MAP = {}
    return NAME_MAP


# --- 3. 核心抓取邏輯 ---
def fetch_and_save_data(ts_code, start_date, end_date, asset_type='E'):
    """
    asset_type: 'E' = ETF, 'S' = Stock
    """
    # 嘗試從字典獲取名稱，如果沒有則用代碼
    code_name = get_name_map().get(ts_code, ts_code)
    print(f"[*] 正在獲取 {code_name} ({ts_code})...", end="")

    try:
        ts_pro = get_pro()
        if asset_type == 'S':
            df = ts_pro.daily(ts_code=ts_code, start_date=start_date, end_date=end_date)
        else:
            df = ts_pro.fund_daily(ts_code=ts_code, start_date=start_date, end_date=end_date)

        if df.empty:
            print(" [-] 無數據")
            return

        # --- 【關鍵修改】統一列名 ---
        # 把 Tushare 的 open/high/low/close 改成 open_price/high_price...
        df = df.rename(columns={
                'open': 'open_price',
                'high': 'high_price',
                'low': 'low_price',
                'close': 'close_price'
            })


        # --- 【核心修復 1】填入名稱 ---
        df['name'] = code_name

        # --- 【核心修復 2】嚴格過濾字段 (防止 Unknown column 報錯) ---
        # 只保留資料庫中肯定存在的字段
        target_cols = [
            'trade_date', 'ts_code', 'name',
            'open_price', 'high_price', 'low_price', 'close_price',  # 新列名
            'vol', 'amount', 'pct_chg'
        ]

        # 確保 DataFrame 裡有這些列，沒有的填 0
        for c in target_cols:
            if c not in df.columns:
                df[c] = 0

        # 只取這些列，剔除 pre_close 等多餘列
        df_save = df[target_cols].copy()

        # 入庫 (先刪後寫)
        db_engine = get_engine()
        with db_engine.connect() as conn:
            del_sql = f"DELETE FROM stock_price WHERE ts_code='{ts_code}' AND trade_date >= '{start_date}' AND trade_date <= '{end_date}'"
            conn.execute(text(del_sql))
            conn.commit()

        df_save.to_sql('stock_price', db_engine, if_exists='append', index=False, dtype={
            'trade_date': types.VARCHAR(8),
            'ts_code': types.VARCHAR(10),
            'name': types.VARCHAR(50),
            'open_price': types.Float(), 'high_price': types.Float(), 'low_price': types.Float(), 'close_price': types.Float(),
            'vol': types.Float(), 'amount': types.Float(), 'pct_chg': types.Float()
        })
        print(f" [√] 成功入庫 {len(df)} 條")

    except Exception as e:
        print(f" [!] 錯誤: {e}")


# --- 4. 批量運行 ---
def parse_args():
    parser = argparse.ArgumentParser(description="回补 A 股/ETF 历史日线到 stock_price")
    parser.add_argument(
        "--target-set",
        choices=["default", "supplement"],
        default="default",
        help="default=原有 ETF+新股名单；supplement=中证2000缺口+强制补充名单",
    )
    parser.add_argument("--days", type=int, default=1000, help="从今天往前回补的自然日天数")
    parser.add_argument("--dry-run", action="store_true", help="只打印目标数量和日期范围，不写数据库")
    return parser.parse_args()


def resolve_stock_targets(target_set):
    if target_set == "supplement":
        return a_share_supplement_targets()
    return list(DEFAULT_NEW_STOCK_TARGETS)


def build_date_range(days, now=None):
    current = now or datetime.now()
    today = current.strftime('%Y%m%d')
    start = (current - timedelta(days=days)).strftime('%Y%m%d')
    return start, today


if __name__ == "__main__":
    args = parse_args()
    start, today = build_date_range(args.days)

    print(f"=== 開始抓取 ({start} - {today}) ===")

    # 1. ETF
    # Keep this selected ETF pool in sync with update_astock_daily.py.
    ETF_TARGETS = ["510050.SH", "159915.SZ", "588000.SH", "510300.SH", "510500.SH",
                   "159901.SZ",
                   # === Broad-market supplements: A500 / CSI 1000 / A50 ===
                   "159338.SZ", "512050.SH", "563800.SH", "159361.SZ", "560510.SH",
                   "560610.SH", "563880.SH", "159362.SZ", "159359.SZ", "159357.SZ",
                   "512100.SH", "560010.SH", "159845.SZ", "159633.SZ", "159629.SZ",
                   "561750.SH", "512250.SH", "159593.SZ", "159136.SZ",

                   # === ChiNext / STAR Market / chip supplements ===
                   "159949.SZ", "159967.SZ", "159368.SZ", "159369.SZ", "159383.SZ",
                   "588200.SH", "588040.SH", "588720.SH", "588870.SH", "588940.SH",

                   # === Hong Kong and overseas supplements ===
                   "513130.SH", "513010.SH", "513060.SH", "513090.SH", "513730.SH",
                   "159941.SZ", "159740.SZ", "513520.SH", "159605.SZ", "159607.SZ",

                   # === Sector, commodity and thematic supplements ===
                   "159611.SZ", "159828.SZ", "516010.SH", "515700.SH", "512980.SH",
                   "159870.SZ", "159930.SZ", "159937.SZ", "159980.SZ", "159981.SZ",
                   "518800.SH", "159934.SZ",
                   "510880.SH",  # 绾㈠埄ETF (楂樿偂鎭槻瀹?
                   "588080.SH",  # 绉戝垱100 (绉戝垱涓皬鐩?
                   # === B. 绉戞妧鎴愰暱 (鏈€娲昏穬) ===
                   "512480.SH",  # 鍗婂浣?(鍥借仈瀹? - 琛屼笟瑙勬ā鏈€澶?
                   "512760.SH",  # 鑺墖ETF (鍥芥嘲) - 鍙︿竴鍙法澶?
                   "515050.SH",  # 5G閫氫俊ETF
                   "512720.SH",  # 璁＄畻鏈篍TF
                   "515250.SH",  # 鏅鸿兘娑堣垂 (AI/浜哄伐鏅鸿兘姒傚康)
                   "159819.SZ",  # 浜哄伐鏅鸿兘ETF
                   "515030.SH",  # 鏂拌兘婧愯溅ETF (榫欏ご)
                   "515790.SH",  # 鍏変紡ETF
                   "159755.SZ",  # 鐢垫睜ETF
                   "512660.SH",  # 鍐涘伐ETF
                   "516110.SH",  # 姹借溅ETF
                   "515980.SH",  # 浜哄伐鏅鸿兘ETF (鍗庡瘜)
                   "516160.SH",  # 鏂拌兘婧怑TF (榫欏ご)
                   "513120.SH","159992.SZ",

                   # === C. 澶ф秷璐逛笌鍖昏嵂 (闀跨墰鏉垮潡) ===
                   "512690.SH",  # 閰扙TF (楣忓崕) - 鐧介厭淇′话
                   "515170.SH",  # 椋熷搧楗枡ETF
                   "159928.SZ",  # 娑堣垂ETF (姹囨坊瀵?
                   "512010.SH",  # 鍖昏嵂ETF (鏄撴柟杈?
                   "512170.SH",  # 鍖荤枟ETF (鍗庡疂) - CXO/鍖荤枟鍣ㄦ
                   "512290.SH",  # 鐢熺墿鍖昏嵂
                   "513360.SH",  # 鏁欒偛ETF (鍗氭椂)
                   "515000.SH",  # 绉戞妧榫欏ご (鍚秷璐圭數瀛?
                   "516220.SH",  # 鍖栧伐ETF (涔熷彲浠ョ畻鍛ㄦ湡)
                   "159996.SZ",  # 瀹剁數ETF

                   # === D. 閲戣瀺涓庡湴浜?(鐗涘競鏃楁墜) ===
                   "512880.SH",  # 璇佸埜ETF (鍥芥嘲) - 瑙勬ā鏈€澶?
                   "512070.SH",  # 闈為摱ETF (鍚繚闄?
                   "512800.SH",  # 閾惰ETF
                   "512200.SH",  # 鍦颁骇ETF
                   "515080.SH",  # 涓瘉绾㈠埄

                   # === E. 鍛ㄦ湡涓庤祫婧?(閫氳儉/閬块櫓) ===
                   "515220.SH",  # 鐓ょ偔ETF (楂樿偂鎭?
                   "512400.SH",  # 鏈夎壊閲戝睘ETF
                   "515210.SH",  # 閽㈤搧ETF
                   "159985.SZ",  # 璞嗙矔ETF (鍟嗗搧)
                   "518880.SH",  # 榛勯噾ETF (鍟嗗搧)
                   "511260.SH",  # 鍗佸勾鍥藉€篍TF
                   "511010.SH",  # 鍥藉€篍TF
                   "513100.SH",  # 绾虫寚ETF (璺ㄥ)
                   "513500.SH",  # 鏍囨櫘500 (璺ㄥ)
                   "513050.SH",  # 鎭掔敓浜掕仈缃?(璺ㄥ)
                   "513330.SH",  # 鎭掔敓浜掕仈 (璺ㄥ)
                   "159920.SZ",  # 鎭掔敓ETF
                   "513180.SH",  # 鎭掔敓绉戞妧
                   "515400.SH",  # 澶ф暟鎹?
                   "516510.SH",  # 浜戣绠?
                   "159938.SZ",  # 鍖昏嵂鍗敓
                   "159995.SZ",  # 鑺墖
                   "159869.SZ",  # 娓告垙ETF
                   "516780.SH",  # 绋€鍦烢TF
                   "516150.SH",  # 绋€鏈夐噾灞?
                   "159865.SZ" # 鍏绘畺ETF (鐚倝)

                   ]
    # 2. 個股 (茅台在這裡！)
    STOCK_TARGETS = resolve_stock_targets(args.target_set)
    if args.dry_run:
        print(f"[DRY-RUN] target_set={args.target_set}")
        print(f"[DRY-RUN] days={args.days} range={start}~{today}")
        print(f"[DRY-RUN] ETF targets={len(ETF_TARGETS) if args.target_set == 'default' else 0}")
        print(f"[DRY-RUN] stock targets={len(STOCK_TARGETS)}")
        print(f"[DRY-RUN] includes_600522={'600522.SH' in STOCK_TARGETS}")
        print(f"[DRY-RUN] first 10 stocks={STOCK_TARGETS[:10]}")
        raise SystemExit(0)

    if args.target_set == "default":
        for code in ETF_TARGETS:
            fetch_and_save_data(code, start, today, asset_type='E')
            time.sleep(0.3)

    for code in STOCK_TARGETS:
        fetch_and_save_data(code, start, today, asset_type='S')
        time.sleep(0.5)

    print("=== 全部完成 ===")
