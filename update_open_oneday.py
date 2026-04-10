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
import requests
import zipfile
from io import BytesIO
from functools import lru_cache
import warnings

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

FORCE_GFEX_AK_PATCH = str(os.getenv("FORCE_GFEX_AK_PATCH", "false")).strip().lower() in {"1", "true", "yes", "on"}
FORCE_SHFE_AK_PATCH = str(os.getenv("FORCE_SHFE_AK_PATCH", "false")).strip().lower() in {"1", "true", "yes", "on"}
# Default disabled to avoid long hangs in DCE LG patch path.
ENABLE_DCE_LG_PATCH = str(os.getenv("ENABLE_DCE_LG_PATCH", "false")).strip().lower() in {"1", "true", "yes", "on"}
_DCE_LG_UPSTREAM_BLOCKED = False


@lru_cache(maxsize=512)
def is_trading_day(date_str: str) -> bool:
    """
    使用 Tushare 交易日历判断是否为交易日；失败时退化为工作日规则。
    """
    try:
        cal = pro.trade_cal(exchange="", start_date=date_str, end_date=date_str)
        if not cal.empty and "is_open" in cal.columns:
            return str(cal.iloc[0]["is_open"]) == "1"
    except Exception:
        pass

    try:
        return datetime.strptime(date_str, "%Y%m%d").weekday() < 5
    except Exception:
        return False


def ensure_akshare_calendar(date_str: str) -> None:
    """
    AkShare 本地 calendar 在 2026 年后已过期，交易日场景下手动补齐，避免误判“非交易日”。
    """
    try:
        from akshare.futures import cot as ak_cot

        calendar = getattr(ak_cot, "calendar", None)
        if isinstance(calendar, list) and date_str not in calendar:
            calendar.append(date_str)
            calendar.sort()
    except Exception:
        return


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

        ensure_akshare_calendar(date_str)

        # 1. 调用接口 (AkShare 会一次性返回该交易所当天的所有数据)
        with warnings.catch_warnings():
            warnings.filterwarnings("ignore", message=rf"{date_str}非交易日")
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


def get_shfe_function():
    """自动查找正确的上期所函数名"""
    if hasattr(ak, 'futures_shfe_position_rank'):
        return ak.futures_shfe_position_rank
    candidates = ['get_shfe_rank_table', 'futures_hold_rank_shfe', 'futures_shfe_holding_rank']
    for c in candidates:
        if hasattr(ak, c):
            return getattr(ak, c)
    return None


def fetch_shfe_patch(date_str):
    """
    上期所全品种补录函数
    逻辑：AkShare抓取 -> 清洗宽表 -> 转长表 -> 聚合 -> 复用 save_to_db 入库
    """
    print(f" [*] [补丁] 正在通过 AkShare 修补上期所数据 (SHFE) {date_str} ...", end="")
    try:
        func = get_shfe_function()
        if not func:
            print(" [-] 未找到 AkShare SHFE 接口")
            return

        ensure_akshare_calendar(date_str)

        # 1. 调用接口 (不同 AkShare 版本参数名可能不同，逐个尝试)
        raw_data = None
        call_errors = []
        for kwargs in ({'date': date_str}, {'trade_date': date_str}):
            try:
                with warnings.catch_warnings():
                    warnings.filterwarnings("ignore", message=rf"{date_str}非交易日")
                    raw_data = func(**kwargs)
                break
            except TypeError as e:
                call_errors.append(str(e))
                continue
        if raw_data is None:
            try:
                with warnings.catch_warnings():
                    warnings.filterwarnings("ignore", message=rf"{date_str}非交易日")
                    raw_data = func(date_str)
            except Exception as e:
                call_errors.append(str(e))
                raise RuntimeError(" / ".join(call_errors + [str(e)]))

        # 2. 处理字典/DataFrame 兼容性
        df = pd.DataFrame()
        if isinstance(raw_data, dict):
            dfs = []
            for _, val in raw_data.items():
                if isinstance(val, pd.DataFrame):
                    dfs.append(val)
            if dfs:
                df = pd.concat(dfs, ignore_index=True)
        elif isinstance(raw_data, pd.DataFrame):
            df = raw_data

        if df.empty:
            print(" [-] AkShare 返回空")
            return

        # 3. 超级映射表 (兼容各种列名)
        rename_dict = {
            'symbol': 'ts_code', '合约代码': 'ts_code', 'variety': 'variety',
            'vol_party_name': 'vol_party_name', '成交量会员简称': 'vol_party_name',
            'long_party_name': 'long_party_name', '持买单会员简称': 'long_party_name',
            'short_party_name': 'short_party_name', '持卖单会员简称': 'short_party_name',
            'vol': 'vol', '成交量': 'vol', 'vol_chg': 'vol_chg', '成交量增减': 'vol_chg',
            'long_open_interest': 'long_vol', '持买单量': 'long_vol', '买持仓': 'long_vol',
            'long_open_interest_chg': 'long_chg', '持买单量增减': 'long_chg', '买持仓增减': 'long_chg',
            'short_open_interest': 'short_vol', '持卖单量': 'short_vol', '卖持仓': 'short_vol',
            'short_open_interest_chg': 'short_chg', '持卖单量增减': 'short_chg', '卖持仓增减': 'short_chg'
        }
        df = df.rename(columns=rename_dict)

        if 'ts_code' not in df.columns:
            print(" [-] 缺少 ts_code 列")
            return

        # 4. 宽表转长表 (拆解三榜合一)
        expected_cols = ['vol_party_name', 'vol', 'vol_chg',
                         'long_party_name', 'long_vol', 'long_chg',
                         'short_party_name', 'short_vol', 'short_chg']
        for c in expected_cols:
            if c not in df.columns:
                df[c] = None if 'name' in c else 0

        # A. 拆解成交量
        df_vol = df[['ts_code', 'vol_party_name', 'vol', 'vol_chg']].rename(columns={'vol_party_name': 'broker'})
        df_vol['long_vol'] = 0
        df_vol['long_chg'] = 0
        df_vol['short_vol'] = 0
        df_vol['short_chg'] = 0

        # B. 拆解买单
        df_long = df[['ts_code', 'long_party_name', 'long_vol', 'long_chg']].rename(columns={'long_party_name': 'broker'})
        df_long['vol'] = 0
        df_long['vol_chg'] = 0
        df_long['short_vol'] = 0
        df_long['short_chg'] = 0

        # C. 拆解卖单
        df_short = df[['ts_code', 'short_party_name', 'short_vol', 'short_chg']].rename(columns={'short_party_name': 'broker'})
        df_short['vol'] = 0
        df_short['vol_chg'] = 0
        df_short['long_vol'] = 0
        df_short['long_chg'] = 0

        # D. 合并
        df_combined = pd.concat([df_vol, df_long, df_short], ignore_index=True)

        # 5. 清洗
        df_combined = df_combined.dropna(subset=['broker'])
        df_combined = df_combined[df_combined['broker'].astype(str).str.len() > 1]
        df_combined = df_combined[~df_combined['broker'].isin(['-', 'None', 'nan'])]

        num_cols = ['vol', 'vol_chg', 'long_vol', 'long_chg', 'short_vol', 'short_chg']
        for c in num_cols:
            df_combined[c] = df_combined[c].astype(str).str.replace(',', '', regex=False)
            df_combined[c] = pd.to_numeric(df_combined[c], errors='coerce').fillna(0)

        # 提取纯代码 (rb2501 -> rb)
        df_combined['ts_code'] = df_combined['ts_code'].apply(lambda x: re.sub(r'\d+', '', str(x)).lower().strip())
        df_combined = df_combined[df_combined['ts_code'].str.fullmatch(r'[a-z]+', na=False)]

        if df_combined.empty:
            print(" [-] 清洗后无有效 SHFE 数据")
            return

        # 6. 聚合与计算
        df_final = df_combined.groupby(['ts_code', 'broker'])[num_cols].sum().reset_index()
        df_final['trade_date'] = date_str
        df_final['net_vol'] = df_final['long_vol'] - df_final['short_vol']

        db_cols = ['trade_date', 'ts_code', 'broker', 'long_vol', 'long_chg', 'short_vol', 'short_chg', 'net_vol']
        for c in db_cols:
            if c not in df_final.columns:
                df_final[c] = 0

        save_data = df_final[db_cols].copy()

        # 7. 复用原本的 save_to_db，覆盖 Tushare 可能缺失/不完整的上期所品种
        save_to_db(save_data, date_str)

        del df, df_vol, df_long, df_short, df_combined, df_final, save_data
        gc.collect()

    except Exception as e:
        print(f" [!] AkShare 补丁异常: {e}")


def _has_symbol_holding(date_str: str, symbol: str) -> bool:
    """
    检查指定交易日是否已有该品种持仓数据。
    """
    try:
        sql = text(
            "SELECT COUNT(*) FROM futures_holding "
            "WHERE trade_date=:d AND ts_code=:s"
        )
        with engine.connect() as conn:
            cnt = conn.execute(sql, {"d": date_str, "s": symbol.lower()}).scalar() or 0
        return int(cnt) > 0
    except Exception:
        return False


def _parse_dce_rank_text(raw_bytes: bytes, file_name: str) -> pd.DataFrame:
    """
    解析大商所 batchDownload 压缩包中的单个 TXT 文件。
    返回与 AkShare futures_dce_position_rank 一致的字段集。
    """
    try:
        data = pd.read_table(BytesIO(raw_bytes), header=None, sep="\t")
        if data.empty:
            return pd.DataFrame()
        if (data.iloc[:, 0].astype(str).str.find("会员类别") == 0).sum() > 0:
            data = data.iloc[:-6]
        if len(data) < 12:
            return pd.DataFrame()

        head_idx = data[data.iloc[:, 0].astype(str).str.find("名次") == 0].index.tolist()
        if len(head_idx) < 3:
            return pd.DataFrame()
        if head_idx[1] - head_idx[0] < 5:
            return pd.DataFrame()

        data = data.iloc[
            head_idx[0]:,
            data.columns[data.iloc[head_idx[0], :].notnull()],
        ]
        data.reset_index(inplace=True, drop=True)
        head_idx = data[data.iloc[:, 0].astype(str).str.find("名次") == 0].index.tolist()
        tail_idx = data[data.iloc[:, 0].astype(str).str.contains(r"(?:总计|合计)", na=False)].index.tolist()
        if len(head_idx) < 3 or len(tail_idx) < 3:
            return pd.DataFrame()

        part_one = data[head_idx[0]: tail_idx[0]].iloc[1:, :]
        part_two = data[head_idx[1]: tail_idx[1]].iloc[1:, :]
        part_three = data[head_idx[2]: tail_idx[2]].iloc[1:, :]
        temp_df = pd.concat(
            objs=[
                part_one.reset_index(drop=True),
                part_two.reset_index(drop=True),
                part_three.reset_index(drop=True),
            ],
            axis=1,
            ignore_index=True,
        )
        if temp_df.empty or temp_df.shape[1] < 12:
            return pd.DataFrame()

        temp_df = temp_df.iloc[:, :12]
        temp_df.columns = [
            "名次",
            "会员简称",
            "成交量",
            "增减",
            "名次",
            "会员简称",
            "持买单量",
            "增减",
            "名次",
            "会员简称",
            "持卖单量",
            "增减",
        ]
        temp_df["rank"] = range(1, len(temp_df) + 1)
        del temp_df["名次"]
        temp_df.columns = [
            "vol_party_name",
            "vol",
            "vol_chg",
            "long_party_name",
            "long_open_interest",
            "long_open_interest_chg",
            "short_party_name",
            "short_open_interest",
            "short_open_interest_chg",
            "rank",
        ]

        contract = file_name.split("_")[1].upper()
        temp_df["symbol"] = contract
        temp_df["variety"] = re.sub(r"\d", "", contract).upper()
        temp_df = temp_df[
            [
                "long_open_interest",
                "long_open_interest_chg",
                "long_party_name",
                "rank",
                "short_open_interest",
                "short_open_interest_chg",
                "short_party_name",
                "vol",
                "vol_chg",
                "vol_party_name",
                "symbol",
                "variety",
            ]
        ]
        temp_df = temp_df.map(lambda x: str(x).replace(",", ""))
        num_cols = [
            "long_open_interest",
            "long_open_interest_chg",
            "short_open_interest",
            "short_open_interest_chg",
            "vol",
            "vol_chg",
            "rank",
        ]
        for col in num_cols:
            temp_df[col] = pd.to_numeric(temp_df[col], errors="coerce")
        return temp_df
    except Exception:
        return pd.DataFrame()


def _fetch_dce_rank_via_batch_download(date_str: str, variety: str = "lg") -> dict:
    """
    直接请求大商所 batchDownload 接口，规避 AkShare 日历过期导致的非交易日判断。
    """
    url = "http://www.dce.com.cn/dcereport/publicweb/dailystat/memberDealPosi/batchDownload"
    referer = "http://www.dce.com.cn/dalianshangpin/xqsj/tjsj26/rtj/rcjccpm/index.html"
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
        ),
        "Accept": "*/*",
        "Content-Type": "application/json;charset=UTF-8",
        "Origin": "http://www.dce.com.cn",
        "Referer": referer,
        "X-Requested-With": "XMLHttpRequest",
    }
    # 这里沿用 AkShare 官方实现里的通用占位参数。
    # batchDownload 返回的是交易日整包 ZIP，再按文件名过滤目标品种；
    # 不要把具体品种/合约硬编码进请求，否则更容易命中上游校验。
    payload = {
        "tradeDate": date_str,
        "varietyId": "a",
        "contractId": "a2601",
        "tradeType": "1",
        "lang": "zh",
    }

    session = requests.Session()
    session.headers.update(headers)
    # 先访问详情页拿 cookie，部分节点会校验来源
    session.get(referer, timeout=20)
    resp = session.post(url, json=payload, timeout=20)
    if resp.status_code != 200:
        if resp.status_code == 412:
            raise RuntimeError("DCE batchDownload HTTP 412 (upstream anti-bot or signature challenge)")
        raise RuntimeError(f"DCE batchDownload HTTP {resp.status_code}")

    content_type = str(resp.headers.get("Content-Type", "")).lower()
    if "zip" not in content_type and not resp.content.startswith(b"PK"):
        raise RuntimeError(f"DCE batchDownload 非ZIP响应: {content_type or 'unknown'}")

    out = {}
    with zipfile.ZipFile(BytesIO(resp.content), mode="r") as zf:
        for name in zf.namelist():
            if not str(name).startswith(date_str):
                continue
            parts = str(name).split("_")
            if len(parts) < 2:
                continue
            contract = str(parts[1]).lower()
            if not contract.startswith(variety.lower()):
                continue
            parsed = _parse_dce_rank_text(zf.read(name), name)
            if parsed is not None and not parsed.empty:
                out[parts[1]] = parsed
    return out


def _rank_dict_to_holding_df(raw_data: dict, date_str: str, target_symbol: str) -> pd.DataFrame:
    """
    将 DCE 排名 dict 统一转为 futures_holding 标准列。
    """
    if not isinstance(raw_data, dict) or not raw_data:
        return pd.DataFrame()

    long_frames = []
    short_frames = []
    target_symbol = target_symbol.lower().strip()
    for key, frame in raw_data.items():
        if frame is None or frame.empty:
            continue
        contract = str(key).lower()
        if not contract.startswith(target_symbol):
            continue

        if not {
            "long_party_name",
            "long_open_interest",
            "long_open_interest_chg",
            "short_party_name",
            "short_open_interest",
            "short_open_interest_chg",
        }.issubset(set(frame.columns)):
            continue

        long_df = frame[["long_party_name", "long_open_interest", "long_open_interest_chg"]].copy()
        long_df.columns = ["broker", "long_vol", "long_chg"]
        short_df = frame[["short_party_name", "short_open_interest", "short_open_interest_chg"]].copy()
        short_df.columns = ["broker", "short_vol", "short_chg"]
        long_frames.append(long_df)
        short_frames.append(short_df)

    if not long_frames:
        return pd.DataFrame()

    df_long = pd.concat(long_frames, ignore_index=True)
    df_short = pd.concat(short_frames, ignore_index=True)
    filter_pat = "合计|共计|总计"
    df_long = df_long[df_long["broker"].notna() & (~df_long["broker"].astype(str).str.contains(filter_pat))]
    df_short = df_short[df_short["broker"].notna() & (~df_short["broker"].astype(str).str.contains(filter_pat))]

    for col in ["long_vol", "long_chg"]:
        df_long[col] = pd.to_numeric(df_long[col], errors="coerce").fillna(0)
    for col in ["short_vol", "short_chg"]:
        df_short[col] = pd.to_numeric(df_short[col], errors="coerce").fillna(0)

    df_final = pd.merge(df_long, df_short, on="broker", how="outer").fillna(0)
    df_final = df_final.groupby("broker")[["long_vol", "long_chg", "short_vol", "short_chg"]].sum().reset_index()
    df_final["trade_date"] = date_str
    df_final["ts_code"] = target_symbol
    df_final["net_vol"] = df_final["long_vol"] - df_final["short_vol"]
    return df_final[
        ["trade_date", "ts_code", "broker", "long_vol", "long_chg", "short_vol", "short_chg", "net_vol"]
    ]


def fetch_dce_lg_patch(date_str: str):
    """
    DCE 原木(LG)补丁：
    当 Tushare 不返回 LG 时，尝试 AkShare 和 DCE 官网直连兜底。
    """
    global _DCE_LG_UPSTREAM_BLOCKED
    target_symbol = "lg"
    if not ENABLE_DCE_LG_PATCH:
        print(f" [i] DCE原木补丁关闭：ENABLE_DCE_LG_PATCH=0")
        return

    if _DCE_LG_UPSTREAM_BLOCKED:
        print(f" [i] DCE原木补丁跳过：已检测到上游接口阻断")
        return

    if _has_symbol_holding(date_str, target_symbol):
        print(f" [i] DCE原木补丁跳过：{date_str} 已有 LG 持仓")
        return

    print(f" [*] [补丁] 尝试修补 DCE 原木持仓 (LG) {date_str} ...", end="")
    attempts = []

    # 1) AkShare 官方函数（先绕过日历判断）
    try:
        ensure_akshare_calendar(date_str)
        with warnings.catch_warnings():
            warnings.filterwarnings("ignore", message=rf"{date_str}非交易日")
            raw = ak.futures_dce_position_rank(date=date_str, vars_list=["LG"])
        df = _rank_dict_to_holding_df(raw, date_str, target_symbol)
        if not df.empty:
            save_to_db(df, date_str)
            print(f" [√] AkShare futures_dce_position_rank 成功 ({len(df)}条)")
            return
        attempts.append("ak.futures_dce_position_rank empty")
    except Exception as e:
        attempts.append(f"ak.futures_dce_position_rank err={e}")

    # 2) AkShare 备用接口
    try:
        ensure_akshare_calendar(date_str)
        with warnings.catch_warnings():
            warnings.filterwarnings("ignore", message=rf"{date_str}非交易日")
            raw = ak.get_dce_rank_table(date=date_str, vars_list=["LG"])
        df = _rank_dict_to_holding_df(raw, date_str, target_symbol)
        if not df.empty:
            save_to_db(df, date_str)
            print(f" [√] AkShare get_dce_rank_table 成功 ({len(df)}条)")
            return
        attempts.append("ak.get_dce_rank_table empty")
    except Exception as e:
        attempts.append(f"ak.get_dce_rank_table err={e}")

    # 3) AkShare 旧 HTML 表格接口
    try:
        ensure_akshare_calendar(date_str)
        with warnings.catch_warnings():
            warnings.filterwarnings("ignore", message=rf"{date_str}非交易日")
            raw = ak.futures_dce_position_rank_other(date=date_str)
        df = _rank_dict_to_holding_df(raw, date_str, target_symbol)
        if not df.empty:
            save_to_db(df, date_str)
            print(f" [√] AkShare futures_dce_position_rank_other 成功 ({len(df)}条)")
            return
        attempts.append("ak.futures_dce_position_rank_other empty")
    except Exception as e:
        attempts.append(f"ak.futures_dce_position_rank_other err={e}")

    # 4) 交易所直连
    try:
        raw = _fetch_dce_rank_via_batch_download(date_str, variety=target_symbol)
        df = _rank_dict_to_holding_df(raw, date_str, target_symbol)
        if not df.empty:
            save_to_db(df, date_str)
            print(f" [√] DCE batchDownload 成功 ({len(df)}条)")
            return
        attempts.append("dce.batchDownload empty")
    except Exception as e:
        attempts.append(f"dce.batchDownload err={e}")

    attempts_text = " | ".join(attempts)
    if "HTTP 412" in attempts_text or "list index out of range" in attempts_text:
        _DCE_LG_UPSTREAM_BLOCKED = True
    print(f" [-] 原木补丁失败: {' | '.join(attempts)}")


# --- 2. 核心逻辑：获取、清洗、筛选字段 ---
def fetch_and_save_tushare(date_str, exchange):
    """
    exchange: GFEX(广期), DCE(大商), CZCE(郑商), SHFE(上期), CFFEX(中金)
    """
    print(f"[*] 正在请求 Tushare [{exchange}] {date_str} ...", end="")
    has_tushare_data = False

    try:
        # 1. 调用接口
        df = pro.fut_holding(trade_date=date_str, exchange=exchange)

        if not df.empty:
            has_tushare_data = True
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
            # 默认只在 Tushare 无数据时触发；如需强制覆盖，可设置 FORCE_GFEX_AK_PATCH=1
            if FORCE_GFEX_AK_PATCH or (not has_tushare_data):
                print("")  # 换行
                fetch_gfex_patch(date_str)
        elif exchange == 'SHFE':
            # 默认只在 Tushare 无数据时触发；如需强制覆盖，可设置 FORCE_SHFE_AK_PATCH=1
            if FORCE_SHFE_AK_PATCH or (not has_tushare_data):
                print("")  # 换行
                fetch_shfe_patch(date_str)
        elif exchange == 'DCE':
            # 大商所补丁：Tushare 近阶段不稳定返回 LG（原木），增加兜底抓取
            if ENABLE_DCE_LG_PATCH:
                print("")  # 换行
                fetch_dce_lg_patch(date_str)

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
        if not is_trading_day(date_str):
            print(f"\n--- 跳过非交易日: {date_str} ---")
            continue

        print(f"\n--- 处理日期: {date_str} ---")
        for ex in EXCHANGES:
            fetch_and_save_tushare(date_str, ex)

            # 处理完一个交易所后，再休息一下
            time.sleep(1)


def run_lg_backfill(start_date: str, end_date: str):
    """
    仅回补 DCE 原木(LG)持仓，用于历史断档修复。
    """
    dates = pd.date_range(start=start_date, end=end_date)
    print(f"\n=== LG 持仓回补: {start_date} -> {end_date} ===")
    for single_date in dates:
        date_str = single_date.strftime('%Y%m%d')
        if not is_trading_day(date_str):
            continue
        fetch_dce_lg_patch(date_str)
        time.sleep(0.8)
    print("=== LG 持仓回补结束 ===")


if __name__ == "__main__":
    today = datetime.now().strftime('%Y%m%d')
    start = (datetime.now() - timedelta(days=2)).strftime('%Y%m%d')

    # 若要补录旧数据，可手动修改 start
    # start = '20251126'

    print(f"开始任务: {start} -> {today}")
    run_job(start, today)

    # 可选：设置环境变量 LG_BACKFILL_START=YYYYMMDD 后，自动触发原木持仓历史回补
    lg_backfill_start = os.getenv("LG_BACKFILL_START", "").strip()
    if lg_backfill_start:
        run_lg_backfill(lg_backfill_start, today)
