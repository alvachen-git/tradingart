import io
import os
import random
import time
import traceback
import warnings
from contextlib import redirect_stderr, redirect_stdout
from datetime import datetime, timedelta
from typing import Any

import akshare as ak
import pandas as pd
import requests
import yfinance as yf
from dotenv import load_dotenv
from sqlalchemy import create_engine, text


load_dotenv(override=True)

DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT", "3306")
DB_NAME = os.getenv("DB_NAME")

if not all([DB_USER, DB_PASSWORD, DB_HOST, DB_NAME]):
    raise ValueError("数据库配置缺失，请检查 .env")

DB_URL = f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(DB_URL, pool_recycle=3600, pool_pre_ping=True)

LOOKBACK_DAYS = int(os.getenv("MACRO_LOOKBACK_DAYS", "5"))
VERBOSE_TRACEBACK = os.getenv("MACRO_VERBOSE_TRACEBACK", "0") == "1"
FRED_API_KEY = os.getenv("FRED_API_KEY", "")
DXY_FRED_SERIES_ID = os.getenv("DXY_FRED_SERIES_ID", "DTWEXM").strip() or "DTWEXM"
DXY_BACKFILL_DAYS = int(os.getenv("DXY_BACKFILL_DAYS", "20"))
DXY_REQUIRED = os.getenv("DXY_REQUIRED", "1").strip().lower() in {"1", "true", "yes", "on"}
DXY_MAX_STALE_DAYS = int(os.getenv("DXY_MAX_STALE_DAYS", "3"))
DXY_FETCH_ROUNDS = max(1, int(os.getenv("DXY_FETCH_ROUNDS", "3")))
DXY_RETRY_SLEEP_SECONDS = max(1, int(os.getenv("DXY_RETRY_SLEEP_SECONDS", "45")))
DXY_FRED_FETCH_DAYS = max(30, int(os.getenv("DXY_FRED_FETCH_DAYS", "180")))
DXY_FRED_SERIES_CANDIDATES = [
    s.strip()
    for s in os.getenv(
        "DXY_FRED_SERIES_CANDIDATES",
        f"{DXY_FRED_SERIES_ID},DTWEXBGS,DTWEXAFEGS,DTWEXEMEGS",
    ).split(",")
    if s.strip()
]

# Third-party warning noise should not pollute daily update logs.
warnings.filterwarnings("ignore", category=FutureWarning, module="akshare")

FRESHNESS_THRESHOLD_BY_FREQ = {
    "D": 7,
    "W": 21,
    "M": 45,
    "Q": 120,
}

FRED_BACKFILL_DAYS_BY_FREQ = {
    "D": 180,
    "W": 365,
    "M": 1460,
    "Q": 3650,
}

FRED_CORE_SERIES: dict[str, dict[str, str]] = {
    "FEDFUNDS": {
        "series_id": "FEDFUNDS",
        "name": "联邦基金利率",
        "category": "bond",
        "frequency": "M",
        "unit": "%",
    },
    "SOFR": {
        "series_id": "SOFR",
        "name": "SOFR隔夜融资利率",
        "category": "bond",
        "frequency": "D",
        "unit": "%",
    },
    "DGS2": {
        "series_id": "DGS2",
        "name": "美国2年期国债收益率(FRED)",
        "category": "bond",
        "frequency": "D",
        "unit": "%",
    },
    "DGS10": {
        "series_id": "DGS10",
        "name": "美国10年期国债收益率(FRED)",
        "category": "bond",
        "frequency": "D",
        "unit": "%",
    },
    "T10Y3M": {
        "series_id": "T10Y3M",
        "name": "美国10Y-3M期限利差",
        "category": "bond",
        "frequency": "D",
        "unit": "%",
    },
    "CPIAUCSL": {
        "series_id": "CPIAUCSL",
        "name": "美国CPI(季调)",
        "category": "inflation",
        "frequency": "M",
        "unit": "index",
    },
    "PCEPILFE": {
        "series_id": "PCEPILFE",
        "name": "美国核心PCE价格指数",
        "category": "inflation",
        "frequency": "M",
        "unit": "index",
    },
    "DFII10": {
        "series_id": "DFII10",
        "name": "美国10Y实际利率(TIPS)",
        "category": "bond",
        "frequency": "D",
        "unit": "%",
    },
    "UNRATE": {
        "series_id": "UNRATE",
        "name": "美国失业率",
        "category": "growth",
        "frequency": "M",
        "unit": "%",
    },
    "PAYEMS": {
        "series_id": "PAYEMS",
        "name": "美国非农就业总人数",
        "category": "growth",
        "frequency": "M",
        "unit": "thousand_persons",
    },
    "BAMLH0A0HYM2": {
        "series_id": "BAMLH0A0HYM2",
        "name": "美高收益债OAS利差",
        "category": "credit",
        "frequency": "D",
        "unit": "%",
    },
    "WALCL": {
        "series_id": "WALCL",
        "name": "美联储总资产",
        "category": "liquidity",
        "frequency": "W",
        "unit": "million_usd",
    },
    "GFDEBTN": {
        "series_id": "GFDEBTN",
        "name": "美国联邦政府总债务",
        "category": "debt",
        "frequency": "Q",
        "unit": "million_usd",
    },
    "GDP": {
        "series_id": "GDP",
        "name": "美国名义GDP",
        "category": "growth",
        "frequency": "Q",
        "unit": "billion_usd",
    },
    "GFDEGDQ188S": {
        "series_id": "GFDEGDQ188S",
        "name": "美国联邦债务占GDP比",
        "category": "debt",
        "frequency": "Q",
        "unit": "%",
    },
}


def get_date_range() -> tuple[str, str]:
    end_date = datetime.now()
    start_date = end_date - timedelta(days=LOOKBACK_DAYS)
    return start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d")


def _find_first_col(df: pd.DataFrame, candidates: list[str]) -> str | None:
    for col in candidates:
        if col in df.columns:
            return col
    return None


def _log_error(prefix: str, err: Exception) -> None:
    print(f"{prefix}: {err}")
    if VERBOSE_TRACEBACK:
        traceback.print_exc()


def _retry_call(name: str, fn, attempts: int = 3, base_sleep: float = 1.5):
    last_err = None
    for i in range(1, attempts + 1):
        try:
            return fn()
        except Exception as e:
            last_err = e
            if i == attempts:
                break
            sleep_sec = base_sleep * i + random.uniform(0, 0.8)
            print(f"    ⚠️ {name} 第{i}次失败，{sleep_sec:.1f}s 后重试: {e}")
            time.sleep(sleep_sec)
    if last_err is not None:
        raise last_err


def _build_series(
    df: pd.DataFrame,
    date_col: str,
    value_col: str,
    start_date: datetime | pd.Timestamp | None = None,
) -> pd.DataFrame:
    out = df[[date_col, value_col]].copy()
    out = out.rename(columns={date_col: "trade_date", value_col: "close_value"})
    out["trade_date"] = pd.to_datetime(out["trade_date"], errors="coerce")
    out["close_value"] = pd.to_numeric(out["close_value"], errors="coerce")
    out = out.dropna(subset=["trade_date", "close_value"])
    if start_date is not None:
        out = out[out["trade_date"] >= pd.to_datetime(start_date)]
    out = out.sort_values("trade_date")
    out["change"] = out["close_value"].diff()
    out["pct_chg"] = out["close_value"].pct_change() * 100
    return out


def _normalize_series(df: pd.DataFrame, date_col: str, value_col: str) -> pd.DataFrame:
    start_str, _ = get_date_range()
    return _build_series(df, date_col, value_col, start_date=pd.to_datetime(start_str))


def _call_quiet(fn):
    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", category=FutureWarning)
        with redirect_stdout(io.StringIO()), redirect_stderr(io.StringIO()):
            return fn()


def _fetch_fred_series(
    series_id: str,
    observation_start: str | None = None,
    observation_end: str | None = None,
) -> pd.DataFrame:
    if not FRED_API_KEY:
        return pd.DataFrame()

    end_str = observation_end or datetime.now().strftime("%Y-%m-%d")
    start_str = observation_start or (datetime.now() - timedelta(days=120)).strftime("%Y-%m-%d")

    url = "https://api.stlouisfed.org/fred/series/observations"
    params = {
        "series_id": series_id,
        "api_key": FRED_API_KEY,
        "file_type": "json",
        "observation_start": start_str,
        "observation_end": end_str,
    }
    resp = requests.get(url, params=params, timeout=20)
    resp.raise_for_status()
    data = resp.json()

    records = []
    for obs in data.get("observations", []):
        value = obs.get("value")
        if value in (None, "."):
            continue
        records.append({"trade_date": pd.to_datetime(obs["date"]), "close_value": float(value)})

    if not records:
        return pd.DataFrame()
    return pd.DataFrame(records)


def _threshold_days_for_freq(freq: str) -> int:
    return FRESHNESS_THRESHOLD_BY_FREQ.get((freq or "D").upper(), 45)


def _backfill_days_for_freq(freq: str) -> int:
    return FRED_BACKFILL_DAYS_BY_FREQ.get((freq or "D").upper(), 365)


def _get_stale_flag(trade_date: datetime | pd.Timestamp | None, freq: str) -> tuple[str, int]:
    if trade_date is None:
        return "UNKNOWN", -1
    as_of = pd.to_datetime(trade_date, errors="coerce")
    if pd.isna(as_of):
        return "UNKNOWN", -1
    stale_days = (datetime.now().date() - as_of.date()).days
    threshold = _threshold_days_for_freq(freq)
    return ("Y" if stale_days > threshold else "N"), stale_days


def _upsert_indicator_meta(meta_map: dict[str, dict[str, str]]) -> None:
    if not meta_map:
        return

    create_sql = text(
        """
        CREATE TABLE IF NOT EXISTS macro_indicator_meta (
            indicator_code VARCHAR(64) PRIMARY KEY,
            indicator_name VARCHAR(128) NOT NULL,
            category VARCHAR(32) NOT NULL,
            source VARCHAR(32) NOT NULL,
            frequency VARCHAR(8) NOT NULL,
            unit VARCHAR(64) NOT NULL,
            update_time DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
        )
        """
    )
    upsert_sql = text(
        """
        REPLACE INTO macro_indicator_meta
        (indicator_code, indicator_name, category, source, frequency, unit)
        VALUES (:indicator_code, :indicator_name, :category, :source, :frequency, :unit)
        """
    )

    with engine.begin() as conn:
        conn.execute(create_sql)
        for code, meta in meta_map.items():
            conn.execute(
                upsert_sql,
                {
                    "indicator_code": code,
                    "indicator_name": meta["name"],
                    "category": meta["category"],
                    "source": "fred",
                    "frequency": meta["frequency"],
                    "unit": meta["unit"],
                },
            )


def save_to_db(result_dict: dict) -> int:
    if not result_dict:
        return 0

    saved_count = 0
    with engine.begin() as conn:
        for code, data in result_dict.items():
            df = data.get("df")
            if df is None or df.empty:
                continue

            for _, row in df.iterrows():
                trade_date = row.get("trade_date")
                close_val = pd.to_numeric(row.get("close_value"), errors="coerce")
                if pd.isna(trade_date) or pd.isna(close_val):
                    continue

                change_val = pd.to_numeric(row.get("change"), errors="coerce")
                pct_val = pd.to_numeric(row.get("pct_chg"), errors="coerce")
                if pd.isna(change_val):
                    change_val = 0.0
                if pd.isna(pct_val):
                    pct_val = 0.0

                sql = text(
                    """
                    REPLACE INTO macro_daily
                    (trade_date, indicator_code, indicator_name, category, close_value, change_value, change_pct)
                    VALUES (:date, :code, :name, :cat, :val, :chg, :pct)
                    """
                )
                conn.execute(
                    sql,
                    {
                        "date": pd.to_datetime(trade_date).date(),
                        "code": code,
                        "name": data["name"],
                        "cat": data["category"],
                        "val": float(close_val),
                        "chg": float(change_val),
                        "pct": float(pct_val),
                    },
                )
                saved_count += 1

    return saved_count


def fetch_bond_yields() -> dict:
    results = {}
    print(f"  更新中美国债数据(近{LOOKBACK_DAYS}天)...")

    try:
        start_date, _ = get_date_range()

        def _fetch_bond_df():
            try:
                return ak.bond_zh_us_rate(start_date=start_date.replace("-", ""))
            except TypeError:
                return ak.bond_zh_us_rate()

        df = _retry_call("bond_zh_us_rate", _fetch_bond_df, attempts=3, base_sleep=2.0)

        if df.empty:
            print("    ⚠️ 中美国债数据为空")
            return results

        date_col = _find_first_col(df, ["日期", "date", "trade_date"]) or df.columns[0]
        df[date_col] = pd.to_datetime(df[date_col], errors="coerce")

        mapping = {
            "US10Y": {"name": "美国10年期国债收益率", "cols": ["美国国债收益率10年"]},
            "US2Y": {"name": "美国2年期国债收益率", "cols": ["美国国债收益率2年"]},
            "CN10Y": {"name": "中国10年期国债收益率", "cols": ["中国国债收益率10年"]},
            "CN2Y": {"name": "中国2年期国债收益率", "cols": ["中国国债收益率2年"]},
            "US10Y2Y": {"name": "美国10Y-2Y利差", "cols": ["美国国债收益率10年2年", "美国国债收益率10年-2年"]},
        }

        for code, meta in mapping.items():
            value_col = _find_first_col(df, meta["cols"])
            if not value_col:
                continue
            series_df = _normalize_series(df, date_col, value_col)
            if series_df.empty:
                continue
            results[code] = {"df": series_df, "name": meta["name"], "category": "bond"}
            print(f"    ✓ {code}: 获取 {len(series_df)} 条")

    except Exception as e:
        _log_error("    ❌ 中美国债数据获取失败", e)

    return results


def _is_rate_limit_err(err: Exception) -> bool:
    msg = str(err).lower()
    cls_name = err.__class__.__name__.lower()
    return "too many requests" in msg or "ratelimit" in msg or "ratelimit" in cls_name


def fetch_dxy_index() -> tuple[dict, str]:
    """统一美元指数口径: Yahoo主源, AkShare回退, FRED多序列兜底。"""
    results = {}
    source = "none"
    print(f"  更新美元指数(近{LOOKBACK_DAYS}天)...")

    # 1) 主源: Yahoo Finance (双ticker容错)
    for ticker in ("DX-Y.NYB", "DX=F"):
        try:
            def _fetch_dxy_yf():
                t = yf.Ticker(ticker)
                return t.history(period="2mo", interval="1d")

            df = _retry_call(f"yfinance:{ticker}", _fetch_dxy_yf, attempts=2, base_sleep=3.0)
            if not df.empty:
                df = df.reset_index()
                if "Date" in df.columns and pd.api.types.is_datetime64tz_dtype(df["Date"]):
                    df["Date"] = df["Date"].dt.tz_localize(None)
                series_df = _normalize_series(df, "Date", "Close")
                if not series_df.empty:
                    results["DXY"] = {"df": series_df, "name": "美元指数", "category": "fx"}
                    print(f"    ✓ DXY: 获取 {len(series_df)} 条 (source=yahoo:{ticker})")
                    return results, f"yahoo:{ticker}"
            print(f"    ⚠️ Yahoo({ticker}) DXY 数据为空，尝试下一源")
        except Exception as e:
            if _is_rate_limit_err(e):
                print(f"    ⚠️ Yahoo({ticker}) 被限流，尝试下一源")
            else:
                print(f"    ⚠️ Yahoo({ticker}) 异常，尝试下一源: {e}")

    # 2) 回退: AkShare 东方财富
    try:
        df = _retry_call(
            "akshare:index_global_hist_em(美元指数)",
            lambda: ak.index_global_hist_em(symbol="美元指数"),
            attempts=3,
            base_sleep=2.0,
        )
        if not df.empty:
            date_col = _find_first_col(df, ["日期", "date"])
            value_col = _find_first_col(df, ["最新价", "收盘", "close"])
            if date_col and value_col:
                series_df = _normalize_series(df, date_col, value_col)
                if not series_df.empty:
                    results["DXY"] = {"df": series_df, "name": "美元指数", "category": "fx"}
                    source = "akshare"
                    print(f"    ✓ DXY: 获取 {len(series_df)} 条 (source=akshare:index_global_hist_em)")
                    return results, source
            print("    ⚠️ AkShare DXY 字段缺失或无有效数据，切换 FRED 兜底源")
        else:
            print("    ⚠️ AkShare DXY 数据为空，切换 FRED 兜底源")
    except Exception as e:
        print(f"    ⚠️ AkShare DXY 异常，切换 FRED 兜底源: {e}")

    # 3) 最终兜底: FRED 多候选序列
    if not FRED_API_KEY:
        print("    ⚠️ FRED 未配置，DXY 无可用兜底源")
        return results, source

    fred_start = (datetime.now() - timedelta(days=DXY_FRED_FETCH_DAYS)).strftime("%Y-%m-%d")
    fred_end = datetime.now().strftime("%Y-%m-%d")
    for series_id in DXY_FRED_SERIES_CANDIDATES:
        try:
            df_fred = _retry_call(
                f"fred:{series_id}",
                lambda sid=series_id: _fetch_fred_series(
                    sid,
                    observation_start=fred_start,
                    observation_end=fred_end,
                ),
                attempts=2,
                base_sleep=2.0,
            )
            if df_fred.empty:
                print(f"    ⚠️ FRED({series_id}) 返回空数据，尝试下一序列")
                continue

            # FRED 序列发布节奏可能慢于 LOOKBACK_DAYS，不用全局窗口截断。
            series_df = _build_series(
                df_fred,
                "trade_date",
                "close_value",
                start_date=datetime.now() - timedelta(days=DXY_FRED_FETCH_DAYS),
            )
            if series_df.empty:
                print(f"    ⚠️ FRED({series_id}) 归一化后为空，尝试下一序列")
                continue

            results["DXY"] = {"df": series_df, "name": "美元指数", "category": "fx"}
            source = f"fred:{series_id}"
            print(f"    ✓ DXY: 获取 {len(series_df)} 条 (source={source})")
            return results, source
        except Exception as e:
            print(f"    ⚠️ FRED({series_id}) 异常，尝试下一序列: {e}")

    print("    ❌ DXY 全部数据源失败")

    return results, source


def _dxy_latest_age_days() -> tuple[str, int]:
    latest = get_latest_indicator_date("DXY")
    if latest == "NONE":
        return latest, -1
    latest_ts = pd.to_datetime(latest, errors="coerce")
    if pd.isna(latest_ts):
        return latest, -1
    return latest, (datetime.now().date() - latest_ts.date()).days


def ensure_dxy_daily_update() -> tuple[str, int, int, int]:
    """
    DXY 守护流程:
    1) 多轮抓取写库
    2) 每轮后做 FRED 缺口回补
    3) 每轮检查最新日期是否满足新鲜度
    """
    total_backfilled = 0
    total_saved = 0
    last_source = "none"

    for round_idx in range(1, DXY_FETCH_ROUNDS + 1):
        print(f"  DXY 守护轮次: {round_idx}/{DXY_FETCH_ROUNDS}")
        dxy_data, last_source = fetch_dxy_index()
        total_saved += save_to_db(dxy_data)

        backfilled = backfill_dxy_from_fred(DXY_BACKFILL_DAYS)
        total_backfilled += backfilled

        latest, age_days = _dxy_latest_age_days()
        print(
            f"DXY_GUARD_ROUND={round_idx}|SOURCE={last_source}|LATEST={latest}|"
            f"AGE_DAYS={age_days}|BACKFILLED_TOTAL={total_backfilled}"
        )

        if age_days >= 0 and age_days <= DXY_MAX_STALE_DAYS:
            return last_source, total_backfilled, age_days, total_saved

        if round_idx < DXY_FETCH_ROUNDS:
            sleep_sec = DXY_RETRY_SLEEP_SECONDS * round_idx
            print(
                f"  ⚠️ DXY 仍不新鲜(age_days={age_days}, threshold={DXY_MAX_STALE_DAYS})，"
                f"{sleep_sec}s 后重试"
            )
            time.sleep(sleep_sec)

    latest, age_days = _dxy_latest_age_days()
    return last_source, total_backfilled, age_days, total_saved


def _get_indicator_dates(indicator_code: str, start_dt: datetime, end_dt: datetime) -> set:
    sql = text(
        """
        SELECT trade_date
        FROM macro_daily
        WHERE indicator_code = :code
          AND trade_date BETWEEN :start_date AND :end_date
        """
    )
    with engine.connect() as conn:
        rows = conn.execute(
            sql,
            {
                "code": indicator_code,
                "start_date": start_dt.date(),
                "end_date": end_dt.date(),
            },
        ).fetchall()
    return {pd.to_datetime(row[0]).date() for row in rows if row[0] is not None}


def backfill_dxy_from_fred(days: int) -> int:
    days = max(days, 0)
    if days == 0:
        print("  跳过 DXY 缺口回补: DXY_BACKFILL_DAYS=0")
        return 0
    if not FRED_API_KEY:
        print("  ⚠️ 跳过 DXY 缺口回补: 未配置 FRED_API_KEY")
        return 0

    end_dt = datetime.now()
    start_dt = end_dt - timedelta(days=days)
    print(f"  检查 DXY 缺口并回补(近{days}天, source=fred:{DXY_FRED_SERIES_ID})...")

    try:
        df_fred = _retry_call(
            f"fred:{DXY_FRED_SERIES_ID}:backfill",
            lambda: _fetch_fred_series(
                DXY_FRED_SERIES_ID,
                observation_start=start_dt.strftime("%Y-%m-%d"),
                observation_end=end_dt.strftime("%Y-%m-%d"),
            ),
            attempts=2,
            base_sleep=2.0,
        )
    except Exception as e:
        _log_error("    ⚠️ DXY 缺口回补抓取失败", e)
        return 0

    if df_fred.empty:
        print("    ⚠️ DXY 缺口回补未抓到 FRED 数据")
        return 0

    series_df = _build_series(df_fred, "trade_date", "close_value", start_date=start_dt)
    if series_df.empty:
        print("    ⚠️ DXY 缺口回补无有效序列")
        return 0

    existing_dates = _get_indicator_dates("DXY", start_dt, end_dt)
    fred_dates = set(series_df["trade_date"].dt.date.tolist())
    missing_dates = sorted(d for d in fred_dates if d not in existing_dates)
    if not missing_dates:
        print("    ✓ DXY 近窗无缺口，无需回补")
        return 0

    missing_df = series_df[series_df["trade_date"].dt.date.isin(missing_dates)].copy()
    missing_df = missing_df.sort_values("trade_date")
    saved = save_to_db({"DXY": {"df": missing_df, "name": "美元指数", "category": "fx"}})
    print(
        f"    ✓ DXY 回补 {len(missing_dates)} 个缺失交易日 "
        f"(source=fred:{DXY_FRED_SERIES_ID}, db_rows={saved})"
    )
    return saved


def get_latest_indicator_date(indicator_code: str) -> str:
    sql = text("SELECT MAX(trade_date) FROM macro_daily WHERE indicator_code = :code")
    with engine.connect() as conn:
        latest = conn.execute(sql, {"code": indicator_code}).scalar()
    if latest is None:
        return "NONE"
    return pd.to_datetime(latest).strftime("%Y-%m-%d")


def fetch_offshore_cny_yahoo() -> dict:
    results = {}
    print("  更新离岸人民币(Yahoo)...")

    # 1) 主源: Yahoo CNH=F
    try:
        def _fetch_cnh():
            ticker = yf.Ticker("CNH=F")
            return ticker.history(period="1mo", interval="1d")

        df = _retry_call("yfinance:CNH=F", _fetch_cnh, attempts=2, base_sleep=2.5)
        if not df.empty:
            df = df.reset_index()
            if "Date" in df.columns and pd.api.types.is_datetime64tz_dtype(df["Date"]):
                df["Date"] = df["Date"].dt.tz_localize(None)
            series_df = _normalize_series(df, "Date", "Close")
            if not series_df.empty:
                results["USDCNH"] = {"df": series_df, "name": "离岸人民币", "category": "fx"}
                print(f"    ✓ USDCNH: 获取 {len(series_df)} 条 (source=yahoo:CNH=F)")
                return results
        print("    ⚠️ Yahoo USDCNH 数据为空，尝试 FRED 回退源")
    except Exception as e:
        if _is_rate_limit_err(e):
            print("    ⚠️ Yahoo USDCNH 被限流，尝试 FRED 回退源")
        else:
            print(f"    ⚠️ Yahoo USDCNH 异常，尝试 FRED 回退源: {e}")

    # 2) 回退: FRED DEXCHUS（需要 FRED_API_KEY）
    try:
        df_fred = _retry_call("fred:DEXCHUS", lambda: _fetch_fred_series("DEXCHUS"), attempts=2, base_sleep=2.0)
        if not df_fred.empty:
            series_df = _normalize_series(df_fred, "trade_date", "close_value")
            if not series_df.empty:
                results["USDCNH"] = {"df": series_df, "name": "离岸人民币", "category": "fx"}
                print(f"    ✓ USDCNH: 获取 {len(series_df)} 条 (source=fred:DEXCHUS)")
                return results

        if not FRED_API_KEY:
            print("    ⚠️ USDCNH 未更新：Yahoo限流且未配置 FRED_API_KEY")
        else:
            print("    ⚠️ USDCNH 未更新：FRED 返回空数据")
    except Exception as e:
        print(f"    ⚠️ USDCNH 未更新：FRED 回退失败: {e}")

    return results


def fetch_bdi_index() -> dict:
    results = {}
    print("  更新波罗的海指数(BDI)...")

    try:
        df = _retry_call(
            "akshare:macro_shipping_bdi",
            lambda: _call_quiet(lambda: ak.macro_shipping_bdi()),
            attempts=3,
            base_sleep=2.0,
        )
        if df.empty:
            print("    ⚠️ BDI 数据为空")
            return results

        date_col = _find_first_col(df, ["日期", "date"])
        value_col = _find_first_col(df, ["最新值", "值", "收盘", "close"])
        pct_col = _find_first_col(df, ["涨跌幅", "涨跌幅(%)", "pct_chg"])
        if not date_col or not value_col:
            print("    ⚠️ BDI 字段缺失")
            return results

        series_df = _normalize_series(df, date_col, value_col)
        if pct_col:
            pct = pd.to_numeric(df[pct_col], errors="coerce")
            series_df = series_df.merge(
                pd.DataFrame({"trade_date": pd.to_datetime(df[date_col], errors="coerce"), "pct_chg_override": pct}),
                on="trade_date",
                how="left",
            )
            series_df["pct_chg"] = series_df["pct_chg_override"].combine_first(series_df["pct_chg"])
            series_df = series_df.drop(columns=["pct_chg_override"])

        if series_df.empty:
            print("    ⚠️ BDI 无有效数据")
            return results

        results["BDI"] = {"df": series_df, "name": "波罗的海干散货指数", "category": "shipping"}
        print(f"    ✓ BDI: 获取 {len(series_df)} 条")
    except Exception as e:
        _log_error("    ❌ BDI 失败", e)

    return results


def fetch_fred_core_macro() -> tuple[dict[str, dict[str, Any]], list[str], list[str]]:
    results: dict[str, dict[str, Any]] = {}
    ok_codes: list[str] = []
    fail_codes: list[str] = []

    if not FRED_API_KEY:
        print("  ⚠️ FRED_CORE_SKIP=missing_api_key")
        return results, ok_codes, list(FRED_CORE_SERIES.keys())

    now_dt = datetime.now()
    print("  更新 FRED 核心宏观指标(12条)...")
    _upsert_indicator_meta(FRED_CORE_SERIES)

    for code, meta in FRED_CORE_SERIES.items():
        series_id = meta["series_id"]
        freq = meta["frequency"]
        start_dt = now_dt - timedelta(days=_backfill_days_for_freq(freq))

        try:
            df_fred = _retry_call(
                f"fred:{series_id}",
                lambda s=series_id, sdt=start_dt: _fetch_fred_series(
                    s,
                    observation_start=sdt.strftime("%Y-%m-%d"),
                    observation_end=now_dt.strftime("%Y-%m-%d"),
                ),
                attempts=2,
                base_sleep=2.0,
            )
            if df_fred.empty:
                fail_codes.append(code)
                print(
                    f"FRED_FETCH_FAIL={code}|SERIES={series_id}|LATEST_DATE=NONE|"
                    f"STALE_FLAG=UNKNOWN|REASON=empty_series"
                )
                continue

            series_df = _build_series(df_fred, "trade_date", "close_value", start_date=start_dt)
            if series_df.empty:
                fail_codes.append(code)
                print(
                    f"FRED_FETCH_FAIL={code}|SERIES={series_id}|LATEST_DATE=NONE|"
                    f"STALE_FLAG=UNKNOWN|REASON=empty_normalized"
                )
                continue

            latest_trade_date = series_df["trade_date"].max()
            stale_flag, stale_days = _get_stale_flag(latest_trade_date, freq)
            results[code] = {
                "df": series_df,
                "name": meta["name"],
                "category": meta["category"],
            }
            ok_codes.append(code)
            print(
                f"FRED_FETCH_OK={code}|SERIES={series_id}|LATEST_DATE={latest_trade_date.date()}|"
                f"STALE_FLAG={stale_flag}|STALE_DAYS={stale_days}|FREQ={freq}|UNIT={meta['unit']}"
            )
        except Exception as e:
            fail_codes.append(code)
            print(
                f"FRED_FETCH_FAIL={code}|SERIES={series_id}|LATEST_DATE=NONE|"
                f"STALE_FLAG=UNKNOWN|REASON={type(e).__name__}"
            )
            _log_error(f"    ❌ FRED 指标抓取失败 {code}", e)

    return results, ok_codes, fail_codes


def run_daily_update() -> None:
    print("=" * 50)
    print(f"宏观数据每日更新任务 ({datetime.now().strftime('%Y-%m-%d %H:%M:%S')})")
    print(f"更新窗口: {get_date_range()[0]} ~ {get_date_range()[1]}")
    print("=" * 50)

    total = 0
    total += save_to_db(fetch_bond_yields())
    dxy_source, dxy_backfilled, dxy_age_days, dxy_saved_rows = ensure_dxy_daily_update()
    total += dxy_saved_rows
    total += dxy_backfilled
    total += save_to_db(fetch_offshore_cny_yahoo())
    total += save_to_db(fetch_bdi_index())

    fred_core_data, fred_ok_codes, fred_fail_codes = fetch_fred_core_macro()
    total += save_to_db(fred_core_data)

    dxy_latest_date = get_latest_indicator_date("DXY")

    print(f"\n任务完成! 共更新 {total} 条数据。")
    print(f"DXY_SOURCE={dxy_source}")
    print(f"DXY_SAVED_ROWS={dxy_saved_rows}")
    print(f"DXY_BACKFILLED_DATES={dxy_backfilled}")
    print(f"DXY_LATEST_DATE={dxy_latest_date}")
    print(f"DXY_AGE_DAYS={dxy_age_days}")
    print(f"DXY_REQUIRED={1 if DXY_REQUIRED else 0}")
    print(f"DXY_MAX_STALE_DAYS={DXY_MAX_STALE_DAYS}")
    print(f"FRED_FETCH_OK={','.join(sorted(fred_ok_codes)) if fred_ok_codes else 'NONE'}")
    print(f"FRED_FETCH_FAIL={','.join(sorted(fred_fail_codes)) if fred_fail_codes else 'NONE'}")

    if DXY_REQUIRED and (dxy_age_days < 0 or dxy_age_days > DXY_MAX_STALE_DAYS):
        raise RuntimeError(
            f"DXY freshness check failed: latest={dxy_latest_date}, "
            f"age_days={dxy_age_days}, threshold={DXY_MAX_STALE_DAYS}"
        )


if __name__ == "__main__":
    try:
        run_daily_update()
    except Exception as e:
        _log_error("\n致命错误", e)
