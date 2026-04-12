import akshare as ak
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text
import requests
import time
import os
from dotenv import load_dotenv

# ==========================================
# 配置区
# ==========================================
load_dotenv(override=True)

DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT", "3306")
DB_NAME = os.getenv("DB_NAME")
FRED_API_KEY = os.getenv("FRED_API_KEY", "")

if not all([DB_USER, DB_PASSWORD, DB_HOST, DB_NAME]):
    raise ValueError("❌ 数据库配置缺失！请检查 .env 文件")

DB_URL = f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(DB_URL, pool_recycle=3600, pool_pre_ping=True)


# ==========================================
# 数据库初始化
# ==========================================
def init_database():
    """创建宏观数据表"""
    create_sql = """
                 CREATE TABLE IF NOT EXISTS macro_daily \
                 ( \
                     id \
                     INT \
                     AUTO_INCREMENT \
                     PRIMARY \
                     KEY, \
                     trade_date \
                     DATE \
                     NOT \
                     NULL, \
                     indicator_code \
                     VARCHAR \
                 ( \
                     20 \
                 ) NOT NULL,
                     indicator_name VARCHAR \
                 ( \
                     50 \
                 ),
                     category VARCHAR \
                 ( \
                     20 \
                 ),
                     close_value DECIMAL \
                 ( \
                     12, \
                     4 \
                 ),
                     change_value DECIMAL \
                 ( \
                     12, \
                     4 \
                 ),
                     change_pct DECIMAL \
                 ( \
                     8, \
                     4 \
                 ),
                     update_time DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP, \
                     UNIQUE KEY uk_date_code \
                 ( \
                     trade_date, \
                     indicator_code \
                 ),
                     INDEX idx_code \
                 ( \
                     indicator_code \
                 ),
                     INDEX idx_date \
                 ( \
                     trade_date \
                 ),
                     INDEX idx_category \
                 ( \
                     category \
                 )
                     ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='宏观指标日频数据'; \
                 """
    with engine.connect() as conn:
        conn.execute(text(create_sql))
        conn.commit()
    print("✅ 数据库表 macro_daily 已就绪")


# ==========================================
# 数据抓取函数
# ==========================================

def fetch_bond_rates() -> dict:
    """
    抓取中美国债利率 (使用 bond_zh_us_rate)

    可获取的列：
    - 中国国债收益率2年/5年/10年/30年
    - 美国国债收益率2年/5年/10年/30年
    - 中国GDP年增率, 美国GDP年增率
    - 利差 10年-2年
    """
    results = {}

    try:
        print("  📡 调用 ak.bond_zh_us_rate()...")
        df = ak.bond_zh_us_rate()

        if df.empty:
            print("  ⚠️ 返回空数据")
            return results

        print(f"  ✓ 获取 {len(df)} 行数据")

        # 列名映射
        col_mapping = {
            '中国国债收益率2年': ('CN2Y', '中国2年期国债', 'bond'),
            '中国国债收益率5年': ('CN5Y', '中国5年期国债', 'bond'),
            '中国国债收益率10年': ('CN10Y', '中国10年期国债', 'bond'),
            '中国国债收益率30年': ('CN30Y', '中国30年期国债', 'bond'),
            '美国国债收益率2年': ('US2Y', '美国2年期国债', 'bond'),
            '美国国债收益率5年': ('US5Y', '美国5年期国债', 'bond'),
            '美国国债收益率10年': ('US10Y', '美国10年期国债', 'bond'),
            '美国国债收益率30年': ('US30Y', '美国30年期国债', 'bond'),
            '中国国债收益率10年-2年': ('CN10Y2Y', '中国10Y-2Y利差', 'bond'),
            '美国国债收益率10年-2年': ('US10Y2Y', '美国10Y-2Y利差', 'bond'),
        }

        # 找到日期列
        date_col = '日期' if '日期' in df.columns else df.columns[0]

        for col_name, (code, name, category) in col_mapping.items():
            if col_name in df.columns:
                temp_df = df[[date_col, col_name]].copy()
                temp_df.columns = ['trade_date', 'close_value']
                temp_df['trade_date'] = pd.to_datetime(temp_df['trade_date'])
                temp_df['close_value'] = pd.to_numeric(temp_df['close_value'], errors='coerce')
                temp_df = temp_df.dropna()

                if not temp_df.empty:
                    results[code] = {'df': temp_df, 'name': name, 'category': category}
                    print(f"    ✓ {code} ({name}): {len(temp_df)} 条")

        return results

    except Exception as e:
        print(f"  ❌ 失败: {e}")
        return results


def fetch_bdi() -> dict:
    """抓取波罗的海干散货指数 (使用 macro_shipping_bdi)"""
    results = {}

    try:
        print("  📡 调用 ak.macro_shipping_bdi()...")
        df = ak.macro_shipping_bdi()

        if df.empty:
            print("  ⚠️ 返回空数据")
            return results

        # 处理列名
        if len(df.columns) >= 2:
            df = df.iloc[:, :2].copy()
            df.columns = ['trade_date', 'close_value']
            df['trade_date'] = pd.to_datetime(df['trade_date'])
            df['close_value'] = pd.to_numeric(df['close_value'], errors='coerce')
            df = df.dropna()

            results['BDI'] = {'df': df, 'name': '波罗的海干散货指数', 'category': 'shipping'}
            print(f"    ✓ BDI: {len(df)} 条")

        return results

    except Exception as e:
        print(f"  ❌ 失败: {e}")
        return results


def fetch_currency_rates() -> dict:
    """
    抓取汇率数据 (修复版：解决单位放大100倍问题)
    """
    results = {}
    raw_rates = {}

    currencies = [
        ('美元', 'USDCNY', '美元兑人民币'),
        ('欧元', 'EURCNY', '欧元兑人民币'),
        ('日元', 'JPYCNY', '100日元兑人民币'),  # 注意：这里名字可以改一下，实际存的是1日元
        ('港币', 'HKDCNY', '港币兑人民币'),
        ('英镑', 'GBPCNY', '英镑兑人民币'),
    ]

    start_date_str = "20100101"
    end_date_str = datetime.now().strftime("%Y%m%d")

    try:
        for currency_name, code, name in currencies:
            print(f"  📡 抓取 {currency_name} 汇率 ({start_date_str} - {end_date_str})...")

            try:
                df = ak.currency_boc_sina(symbol=currency_name, start_date=start_date_str, end_date=end_date_str)

                if df.empty:
                    print(f"    ⚠️ {currency_name} 无数据")
                    continue

                value_col = None
                for col in ['央行中间价', '中行折算价', '中行汇买价']:
                    if col in df.columns:
                        value_col = col
                        break

                if value_col is None:
                    value_col = df.columns[1]

                temp_df = df[['日期', value_col]].copy()
                temp_df.columns = ['trade_date', 'close_value']
                temp_df['trade_date'] = pd.to_datetime(temp_df['trade_date'])
                temp_df['close_value'] = pd.to_numeric(temp_df['close_value'], errors='coerce')

                # === 核心修复：所有中行数据都是基于100单位的，必须除以100 ===
                temp_df['close_value'] = temp_df['close_value'] / 100.0

                temp_df = temp_df.dropna()
                # 重新去重逻辑：保留最新日期的
                temp_df = temp_df.drop_duplicates(subset=['trade_date'], keep='last')

                if not temp_df.empty:
                    # 如果是日元，现在的逻辑是：1日元 = 0.044人民币
                    # 建议将名称改为 '日元兑人民币' 以免误解
                    final_name = '日元兑人民币' if code == 'JPYCNY' else name

                    results[code] = {'df': temp_df.copy(), 'name': final_name, 'category': 'fx'}
                    raw_rates[code] = temp_df.set_index('trade_date')['close_value']
                    print(f"    ✓ {code}: {len(temp_df)} 条 (已修正单位)")

                time.sleep(0.5)

            except Exception as e:
                print(f"    ❌ {currency_name} 失败: {e}")

        # ==========================================
        # 计算交叉汇率 (修正版)
        # ==========================================
        print("\n  📊 计算交叉汇率...")

        # 此时 raw_rates 里的数据已经是：
        # USDCNY = 7.2 (1美元)
        # JPYCNY = 0.048 (1日元)

        if 'USDCNY' in raw_rates:
            usd_cny = raw_rates['USDCNY']

            # 1. USDJPY = 美元单价 / 日元单价 = 7.2 / 0.048 = 150
            if 'JPYCNY' in raw_rates:
                jpy_cny = raw_rates['JPYCNY']
                aligned = pd.DataFrame({'usd': usd_cny, 'jpy': jpy_cny}).dropna()
                if not aligned.empty:
                    # 修复：不需要再乘以100了，直接相除
                    usdjpy = aligned['usd'] / aligned['jpy']
                    usdjpy_df = usdjpy.reset_index()
                    usdjpy_df.columns = ['trade_date', 'close_value']
                    results['USDJPY'] = {'df': usdjpy_df, 'name': '美元兑日元', 'category': 'fx'}
                    print(f"    ✓ USDJPY (计算): {len(usdjpy_df)} 条")

            # 2. USDEUR = 美元单价 / 欧元单价 = 7.2 / 8.0 = 0.9
            if 'EURCNY' in raw_rates:
                eur_cny = raw_rates['EURCNY']
                aligned = pd.DataFrame({'usd': usd_cny, 'eur': eur_cny}).dropna()
                if not aligned.empty:
                    usdeur = aligned['usd'] / aligned['eur']
                    usdeur_df = usdeur.reset_index()
                    usdeur_df.columns = ['trade_date', 'close_value']
                    results['USDEUR'] = {'df': usdeur_df, 'name': '美元兑欧元', 'category': 'fx'}
                    print(f"    ✓ USDEUR (计算): {len(usdeur_df)} 条")

            # 3. USDHKD
            if 'HKDCNY' in raw_rates:
                hkd_cny = raw_rates['HKDCNY']
                aligned = pd.DataFrame({'usd': usd_cny, 'hkd': hkd_cny}).dropna()
                if not aligned.empty:
                    usdhkd = aligned['usd'] / aligned['hkd']
                    usdhkd_df = usdhkd.reset_index()
                    usdhkd_df.columns = ['trade_date', 'close_value']
                    results['USDHKD'] = {'df': usdhkd_df, 'name': '美元兑港币', 'category': 'fx'}
                    print(f"    ✓ USDHKD (计算): {len(usdhkd_df)} 条")

            # 4. USDGBP
            if 'GBPCNY' in raw_rates:
                gbp_cny = raw_rates['GBPCNY']
                aligned = pd.DataFrame({'usd': usd_cny, 'gbp': gbp_cny}).dropna()
                if not aligned.empty:
                    usdgbp = aligned['usd'] / aligned['gbp']
                    usdgbp_df = usdgbp.reset_index()
                    usdgbp_df.columns = ['trade_date', 'close_value']
                    results['USDGBP'] = {'df': usdgbp_df, 'name': '美元兑英镑', 'category': 'fx'}
                    print(f"    ✓ USDGBP (计算): {len(usdgbp_df)} 条")

        return results

    except Exception as e:
        print(f"  ❌ 汇率抓取失败: {e}")
        return results


def fetch_usd_index_from_fred() -> dict:
    """从 FRED 抓取美元指数 (DXY)"""
    results = {}

    if not FRED_API_KEY:
        print("  ⚠️ 未配置 FRED_API_KEY，跳过美元指数")
        return results

    try:
        print("  📡 从 FRED 抓取美元指数...")

        # FRED 美元指数代码: DTWEXBGS (Broad) 或 DTWEXM (Major Currencies)
        url = "https://api.stlouisfed.org/fred/series/observations"
        params = {
            "series_id": "DTWEXBGS",  # Trade Weighted U.S. Dollar Index: Broad
            "api_key": FRED_API_KEY,
            "file_type": "json",
            "observation_start": "2015-01-01",
            "observation_end": datetime.now().strftime("%Y-%m-%d"),
        }

        resp = requests.get(url, params=params, timeout=30)
        data = resp.json()

        if "observations" not in data:
            print(f"  ⚠️ FRED 返回异常")
            return results

        records = []
        for obs in data["observations"]:
            if obs["value"] != ".":
                records.append({
                    "trade_date": pd.to_datetime(obs["date"]),
                    "close_value": float(obs["value"])
                })

        if records:
            df = pd.DataFrame(records)
            results['DXY'] = {'df': df, 'name': '美元指数(FRED)', 'category': 'fx'}
            print(f"    ✓ DXY: {len(df)} 条")

        return results

    except Exception as e:
        print(f"  ❌ FRED 美元指数失败: {e}")
        return results


def fetch_offshore_cny_from_fred() -> dict:
    """从 FRED 抓取离岸人民币汇率 (USDCNH)"""
    results = {}

    if not FRED_API_KEY:
        return results

    try:
        print("  📡 从 FRED 抓取离岸人民币...")

        url = "https://api.stlouisfed.org/fred/series/observations"
        params = {
            "series_id": "DEXCHUS",  # China / U.S. Foreign Exchange Rate
            "api_key": FRED_API_KEY,
            "file_type": "json",
            "observation_start": "2015-01-01",
            "observation_end": datetime.now().strftime("%Y-%m-%d"),
        }

        resp = requests.get(url, params=params, timeout=30)
        data = resp.json()

        if "observations" in data:
            records = []
            for obs in data["observations"]:
                if obs["value"] != ".":
                    records.append({
                        "trade_date": pd.to_datetime(obs["date"]),
                        "close_value": float(obs["value"])
                    })

            if records:
                df = pd.DataFrame(records)
                results['USDCNH'] = {'df': df, 'name': '美元兑离岸人民币', 'category': 'fx'}
                print(f"    ✓ USDCNH: {len(df)} 条")

        return results

    except Exception as e:
        print(f"  ❌ FRED 离岸人民币失败: {e}")
        return results


def fetch_japan_bond_from_fred() -> dict:
    """从 FRED 抓取日本国债利率"""
    results = {}

    if not FRED_API_KEY:
        print("  ⚠️ 未配置 FRED_API_KEY，跳过日本国债")
        return results

    try:
        print("  📡 从 FRED 抓取日本国债...")

        # FRED 日本国债代码
        jp_bonds = {
            'JP10Y': ('IRLTLT01JPM156N', '日本10年期国债'),
        }

        for code, (fred_code, name) in jp_bonds.items():
            url = "https://api.stlouisfed.org/fred/series/observations"
            params = {
                "series_id": fred_code,
                "api_key": FRED_API_KEY,
                "file_type": "json",
                "observation_start": "2015-01-01",
                "observation_end": datetime.now().strftime("%Y-%m-%d"),
            }

            resp = requests.get(url, params=params, timeout=30)
            data = resp.json()

            if "observations" in data:
                records = []
                for obs in data["observations"]:
                    if obs["value"] != ".":
                        records.append({
                            "trade_date": pd.to_datetime(obs["date"]),
                            "close_value": float(obs["value"])
                        })

                if records:
                    df = pd.DataFrame(records)
                    results[code] = {'df': df, 'name': name, 'category': 'bond'}
                    print(f"    ✓ {code}: {len(df)} 条")

        return results

    except Exception as e:
        print(f"  ❌ FRED 日本国债失败: {e}")
        return results


def fetch_ushy_from_fred() -> dict:
    """从 FRED 抓取美国高收益债利差"""
    results = {}

    if not FRED_API_KEY:
        print("  ⚠️ 未配置 FRED_API_KEY，跳过高收益债")
        return results

    try:
        print("  📡 从 FRED 抓取高收益债利差...")

        url = "https://api.stlouisfed.org/fred/series/observations"
        params = {
            "series_id": "BAMLH0A0HYM2",  # ICE BofA US High Yield Index Option-Adjusted Spread
            "api_key": FRED_API_KEY,
            "file_type": "json",
            "observation_start": "2015-01-01",
            "observation_end": datetime.now().strftime("%Y-%m-%d"),
        }

        resp = requests.get(url, params=params, timeout=30)
        data = resp.json()

        if "observations" in data:
            records = []
            for obs in data["observations"]:
                if obs["value"] != ".":
                    records.append({
                        "trade_date": pd.to_datetime(obs["date"]),
                        "close_value": float(obs["value"])
                    })

            if records:
                df = pd.DataFrame(records)
                results['USHY'] = {'df': df, 'name': '美国高收益债利差', 'category': 'bond'}
                print(f"    ✓ USHY: {len(df)} 条")

        return results

    except Exception as e:
        print(f"  ❌ FRED 高收益债失败: {e}")
        return results


def fetch_sp500_pe_from_multpl() -> dict:
    """
    抓取标普500 PE(TTM) 月度数据（用于美股股债比）。
    数据源: https://www.multpl.com/s-p-500-pe-ratio/table/by-month
    """
    results = {}
    try:
        print("  📡 抓取标普500 PE (multpl)...")
        tables = pd.read_html("https://www.multpl.com/s-p-500-pe-ratio/table/by-month")
        if not tables:
            print("  ⚠️ multpl 返回空表")
            return results

        df = tables[0].copy()
        if df.shape[1] < 2:
            print("  ⚠️ multpl 表结构异常")
            return results

        d_col, v_col = df.columns[0], df.columns[1]
        df = df[[d_col, v_col]].copy()
        df.columns = ["trade_date", "close_value"]
        df["trade_date"] = pd.to_datetime(df["trade_date"], errors="coerce")
        df["close_value"] = pd.to_numeric(df["close_value"].astype(str).str.replace(r"[^0-9.\-]", "", regex=True), errors="coerce")
        df = df.dropna(subset=["trade_date", "close_value"])
        if df.empty:
            print("  ⚠️ multpl 无有效数值")
            return results

        # 仅保留近15年，足够覆盖分位统计且减小重复写入压力。
        cutoff = datetime.now() - timedelta(days=365 * 15)
        df = df[df["trade_date"] >= cutoff].copy()
        df = df.sort_values("trade_date").drop_duplicates(subset=["trade_date"], keep="last")
        if df.empty:
            return results

        results["SP500_PE"] = {
            "df": df,
            "name": "标普500市盈率(PE-TTM)",
            "category": "valuation",
        }
        print(f"    ✅ SP500_PE: {len(df)} 条")
        return results
    except Exception as e:
        print(f"  ❌ multpl 标普PE失败: {e}")
        return results


# ==========================================
# 数据处理和保存
# ==========================================

def calculate_changes(df: pd.DataFrame) -> pd.DataFrame:
    """计算涨跌幅 (增加清洗 Inf 逻辑)"""
    if df.empty or len(df) < 2:
        df["change_value"] = None
        df["change_pct"] = None
        return df

    df = df.sort_values("trade_date").reset_index(drop=True)
    df["change_value"] = df["close_value"].diff()
    df["change_pct"] = df["close_value"].pct_change() * 100

    # === 新增：将 Inf (无限大) 和 -Inf 替换为 NaN (空值) ===
    df.replace([np.inf, -np.inf], np.nan, inplace=True)

    #以此防止 MySQL 写入报错
    return df


def save_to_db(df: pd.DataFrame, indicator_code: str, indicator_name: str, category: str):
    """保存到数据库 - 极速批量版"""
    if df.empty:
        return 0

    # 1. 数据清洗
    df = df.copy()
    df.replace([np.inf, -np.inf], np.nan, inplace=True)

    # 2. 将 DataFrame 转换为字典列表 (List of Dicts)
    # 相比逐行 execute，这是性能提升的关键
    data_list = []
    current_time_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    for _, row in df.iterrows():
        # 安全处理空值 (NaN -> None)
        val_close = float(row['close_value']) if pd.notna(row['close_value']) else None
        val_change = float(row['change_value']) if pd.notna(row.get('change_value')) else None
        val_pct = float(row['change_pct']) if pd.notna(row.get('change_pct')) else None

        # 处理日期
        trade_date = row['trade_date']
        if hasattr(trade_date, 'strftime'):
            trade_date_str = trade_date.strftime('%Y-%m-%d')
        else:
            trade_date_str = str(trade_date)[:10]

        data_list.append({
            "trade_date": trade_date_str,
            "indicator_code": indicator_code,
            "indicator_name": indicator_name,
            "category": category,
            "close_value": val_close,
            "change_value": val_change,
            "change_pct": val_pct,
            "update_time": current_time_str
        })

    # 3. 准备 SQL 语句
    # 使用 :key 的形式，SQLAlchemy 会自动匹配字典中的 key
    sql = text("""
               REPLACE
               INTO macro_daily 
        (trade_date, indicator_code, indicator_name, category, close_value, change_value, change_pct, update_time)
        VALUES (:trade_date, :indicator_code, :indicator_name, :category, :close_value, :change_value, :change_pct, :update_time)
               """)

    # 4. 批量执行 (分批次，防止包过大)
    batch_size = 1000  # 每次写入 1000 条
    total_inserted = 0

    try:
        # 使用 engine.begin() 自动开启/提交事务
        with engine.begin() as conn:
            for i in range(0, len(data_list), batch_size):
                batch = data_list[i: i + batch_size]
                if not batch: continue

                # 这一步会执行 executemany，速度极快
                conn.execute(sql, batch)
                total_inserted += len(batch)

                # 打印进度条 (覆盖同一行，让你知道它在动)
                print(f"    ... 正在写入({min(i + batch_size, len(data_list))}/{len(data_list)})", end='\r')

        print(f"                                          ", end='\r')  # 清除进度文字
    except Exception as e:
        print(f"    ❌ 批量写入失败: {e}")
        return 0

    return total_inserted


# ==========================================
# 主运行逻辑
# ==========================================

def run_fetch():
    """执行数据抓取"""
    print(f"\n{'=' * 60}")
    print(f"📊 宏观数据抓取 - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'=' * 60}")

    all_results = {}

    # 1. 中美国债利率 (AKShare)
    print("\n🔄 [1/7] 抓取中美国债利率 (AKShare)...")
    results = fetch_bond_rates()
    all_results.update(results)
    time.sleep(1)

    # 2. 波罗的海指数 (AKShare)
    print("\n🔄 [2/7] 抓取波罗的海指数 (AKShare)...")
    results = fetch_bdi()
    all_results.update(results)
    time.sleep(1)

    # 3. 汇率 (AKShare) + 交叉汇率计算
    print("\n🔄 [3/7] 抓取汇率 & 计算交叉汇率 (AKShare)...")
    results = fetch_currency_rates()
    all_results.update(results)
    time.sleep(1)

    # 4. 美元指数 + 离岸人民币 (FRED)
    print("\n🔄 [4/7] 抓取美元指数 & 离岸人民币 (FRED)...")
    results = fetch_usd_index_from_fred()
    all_results.update(results)
    results = fetch_offshore_cny_from_fred()
    all_results.update(results)

    # 5. 日本国债 (FRED)
    print("\n🔄 [5/7] 抓取日本国债 (FRED)...")
    results = fetch_japan_bond_from_fred()
    all_results.update(results)

    # 6. 美国高收益债 (FRED)
    print("\n🔄 [6/7] 抓取美国高收益债 (FRED)...")
    results = fetch_ushy_from_fred()
    all_results.update(results)

    # 7. 标普500估值 (multpl)
    print("\n🔄 [7/7] 抓取标普500估值 (multpl)...")
    results = fetch_sp500_pe_from_multpl()
    all_results.update(results)

    # 保存到数据库
    print(f"\n{'=' * 60}")
    print("💾 保存数据到数据库...")
    print(f"{'=' * 60}")

    total_saved = 0
    for code, data in all_results.items():
        # === 新增：增加 try/except 容错 ===
        try:
            df = data['df']
            df = calculate_changes(df)
            count = save_to_db(df, code, data['name'], data['category'])
            print(f"  ✅ {code} ({data['name']}): {count} 条")
            total_saved += count
        except Exception as e:
            print(f"  ❌ 保存 {code} 时发生严重错误，已跳过。错误信息: {e}")
            # 继续执行下一个循环，不中断脚本
            continue

    # 汇总
    print(f"\n{'=' * 60}")
    print(f"✅ 完成! 共保存 {total_saved} 条记录")
    print(f"{'=' * 60}")

    # 按类别显示
    print("\n📋 已获取的指标:")

    # 分类
    bonds = [k for k in all_results if all_results[k]['category'] == 'bond']
    fx = [k for k in all_results if all_results[k]['category'] == 'fx']
    shipping = [k for k in all_results if all_results[k]['category'] == 'shipping']
    valuation = [k for k in all_results if all_results[k]['category'] == 'valuation']

    if bonds:
        print("\n  🏦 国债利率:")
        for code in sorted(bonds):
            print(f"    • {code}: {all_results[code]['name']}")

    if fx:
        print("\n  💱 汇率:")
        for code in sorted(fx):
            print(f"    • {code}: {all_results[code]['name']}")

    if shipping:
        print("\n  🚢 航运:")
        for code in sorted(shipping):
            print(f"    • {code}: {all_results[code]['name']}")

    if valuation:
        print("\n  📊 估值:")
        for code in sorted(valuation):
            print(f"    • {code}: {all_results[code]['name']}")

    # 提示缺失的指标
    if not FRED_API_KEY:
        print("\n⚠️ 提示: 配置 FRED_API_KEY 可获取更多数据:")
        print("  • DXY (美元指数)")
        print("  • JP10Y (日本10年期国债)")
        print("  • USHY (美国高收益债利差)")
        print("  • USDCNH (美元兑离岸人民币)")
        print("  申请地址: https://fred.stlouisfed.org/docs/api/api_key.html")


# ==========================================
# 入口
# ==========================================
if __name__ == "__main__":
    import sys

    init_database()

    if len(sys.argv) > 1 and sys.argv[1] == "check":
        print(f"AKShare 版本: {ak.__version__}")
        print(f"FRED API Key: {'已配置' if FRED_API_KEY else '未配置'}")
    else:
        run_fetch()
