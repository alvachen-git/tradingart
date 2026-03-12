import io
import os
import time
import traceback
import warnings
from contextlib import redirect_stderr, redirect_stdout
from datetime import datetime, timedelta

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

# Third-party warning noise should not pollute daily update logs.
warnings.filterwarnings("ignore", category=FutureWarning, module="akshare")


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
            sleep_sec = base_sleep * i
            print(f"    ⚠️ {name} 第{i}次失败，{sleep_sec:.1f}s 后重试: {e}")
            time.sleep(sleep_sec)
    if last_err is not None:
        raise last_err


def _normalize_series(df: pd.DataFrame, date_col: str, value_col: str) -> pd.DataFrame:
    start_str, _ = get_date_range()
    start_dt = pd.to_datetime(start_str)

    out = df[[date_col, value_col]].copy()
    out = out.rename(columns={date_col: "trade_date", value_col: "close_value"})
    out["trade_date"] = pd.to_datetime(out["trade_date"], errors="coerce")
    out["close_value"] = pd.to_numeric(out["close_value"], errors="coerce")
    out = out.dropna(subset=["trade_date", "close_value"])
    out = out[out["trade_date"] >= start_dt].sort_values("trade_date")
    out["change"] = out["close_value"].diff()
    out["pct_chg"] = out["close_value"].pct_change() * 100
    return out


def _call_quiet(fn):
    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", category=FutureWarning)
        with redirect_stdout(io.StringIO()), redirect_stderr(io.StringIO()):
            return fn()


def _fetch_fred_series(series_id: str) -> pd.DataFrame:
    if not FRED_API_KEY:
        return pd.DataFrame()

    url = "https://api.stlouisfed.org/fred/series/observations"
    params = {
        "series_id": series_id,
        "api_key": FRED_API_KEY,
        "file_type": "json",
        "observation_start": (datetime.now() - timedelta(days=120)).strftime("%Y-%m-%d"),
        "observation_end": datetime.now().strftime("%Y-%m-%d"),
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


def fetch_dxy_index() -> dict:
    """统一美元指数口径: Yahoo(DX-Y.NYB)主源, AkShare回退。"""
    results = {}
    print(f"  更新美元指数(近{LOOKBACK_DAYS}天)...")

    # 1) 主源: Yahoo Finance
    try:
        def _fetch_dxy_yf():
            ticker = yf.Ticker("DX-Y.NYB")
            return ticker.history(period="1mo", interval="1d")

        df = _retry_call("yfinance:DX-Y.NYB", _fetch_dxy_yf, attempts=2, base_sleep=3.0)
        if not df.empty:
            df = df.reset_index()
            if "Date" in df.columns and pd.api.types.is_datetime64tz_dtype(df["Date"]):
                df["Date"] = df["Date"].dt.tz_localize(None)
            series_df = _normalize_series(df, "Date", "Close")
            if not series_df.empty:
                results["DXY"] = {"df": series_df, "name": "美元指数", "category": "fx"}
                print(f"    ✓ DXY: 获取 {len(series_df)} 条 (source=yahoo:DX-Y.NYB)")
                return results
        print("    ⚠️ Yahoo DXY 数据为空，切换 AkShare 回退源")
    except Exception as e:
        if _is_rate_limit_err(e):
            print("    ⚠️ Yahoo DXY 被限流，切换 AkShare 回退源")
        else:
            print(f"    ⚠️ Yahoo DXY 异常，切换 AkShare 回退源: {e}")

    # 2) 回退: AkShare 东方财富
    try:
        df = _retry_call(
            "akshare:index_global_hist_em(美元指数)",
            lambda: ak.index_global_hist_em(symbol="美元指数"),
            attempts=3,
            base_sleep=2.0,
        )
        if df.empty:
            print("    ⚠️ AkShare DXY 数据为空")
            return results

        date_col = _find_first_col(df, ["日期", "date"])
        value_col = _find_first_col(df, ["最新价", "收盘", "close"])
        if not date_col or not value_col:
            print("    ⚠️ AkShare DXY 字段缺失")
            return results

        series_df = _normalize_series(df, date_col, value_col)
        if series_df.empty:
            print("    ⚠️ AkShare DXY 无有效数据")
            return results

        results["DXY"] = {"df": series_df, "name": "美元指数", "category": "fx"}
        print(f"    ✓ DXY: 获取 {len(series_df)} 条 (source=akshare:index_global_hist_em)")
    except Exception as e:
        _log_error("    ❌ DXY 全部数据源失败", e)

    return results


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


def run_daily_update() -> None:
    print("=" * 50)
    print(f"宏观数据每日更新任务 ({datetime.now().strftime('%Y-%m-%d %H:%M:%S')})")
    print(f"更新窗口: {get_date_range()[0]} ~ {get_date_range()[1]}")
    print("=" * 50)

    total = 0
    total += save_to_db(fetch_bond_yields())
    total += save_to_db(fetch_dxy_index())
    total += save_to_db(fetch_offshore_cny_yahoo())
    total += save_to_db(fetch_bdi_index())

    print(f"\n任务完成! 共更新 {total} 条数据。")


if __name__ == "__main__":
    try:
        run_daily_update()
    except Exception as e:
        _log_error("\n致命错误", e)
