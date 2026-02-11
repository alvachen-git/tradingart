import os
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

from volume_oi_tools import COMMODITY_MAP, ETF_MAP


load_dotenv(override=True)

DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")


def get_db_engine():
    if not all([DB_USER, DB_PASSWORD, DB_HOST, DB_NAME]):
        return None
    db_url = f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    return create_engine(
        db_url,
        pool_pre_ping=True,
        pool_recycle=3600,
        pool_size=5,
        max_overflow=10,
    )


engine = get_db_engine()


OPTION_MULTIPLIER = {
    "510050": 10000,
    "510300": 10000,
    "510500": 10000,
    "159901": 10000,
    "159915": 10000,
    "159919": 10000,
    "159922": 10000,
    "588000": 10000,
    "588080": 10000,
    "AU": 1000,
    "AG": 15,
    "CU": 5,
    "AL": 5,
    "ZN": 5,
    "PB": 5,
    "SN": 1,
    "NI": 1,
    "I": 100,
    "RB": 10,
    "HC": 10,
    "J": 100,
    "JM": 60,
    "SM": 5,
    "SF": 5,
    "PS": 3,
    "LC": 1,
    "SI": 5,
    "PT": 1000,
    "PD": 1000,
    "SH": 30,
    "AO": 20,
    "SC": 1000,
    "FU": 10,
    "LU": 10,
    "PG": 20,
    "MA": 10,
    "TA": 5,
    "PP": 5,
    "L": 5,
    "V": 5,
    "EB": 5,
    "EG": 10,
    "RU": 10,
    "NR": 10,
    "BR": 5,
    "BU": 10,
    "SA": 20,
    "FG": 20,
    "UR": 20,
    "M": 10,
    "Y": 10,
    "P": 10,
    "OI": 10,
    "RM": 10,
    "C": 10,
    "A": 10,
    "CF": 5,
    "SR": 10,
    "AP": 10,
    "PK": 5,
    "CJ": 5,
    "LH": 16,
}


def _normalize_etf_code(code: str) -> str:
    raw = code.replace(".SH", "").replace(".SZ", "")
    if raw.isdigit() and len(raw) == 6:
        if raw.startswith("15") or raw.startswith("16"):
            return f"{raw}.SZ"
        return f"{raw}.SH"
    return code


def _resolve_symbol(query: str):
    q = query.strip()
    if not q:
        return None, None, None

    # ETF (code or alias)
    code_match = None
    if "." in q and len(q.split(".")) == 2:
        code_match = q
    elif q.isdigit() and len(q) == 6:
        code_match = q

    if code_match:
        underlying = _normalize_etf_code(code_match)
        display = underlying.replace(".SH", "").replace(".SZ", "")
        return "etf", underlying, display

    for name, (code, display) in ETF_MAP.items():
        if name in q.upper():
            return "etf", code, display

    # Commodity (Chinese alias or code)
    for name, prefix in COMMODITY_MAP.items():
        if name in q:
            return "commodity", prefix.lower(), name

    if q.isalpha():
        return "commodity", q.lower(), q.upper()

    return None, None, None


def _get_multiplier(asset_type: str, underlying: str) -> int:
    if asset_type == "etf":
        code = underlying.replace(".SH", "").replace(".SZ", "")
        return OPTION_MULTIPLIER.get(code, 10000)
    code = underlying.upper()
    return OPTION_MULTIPLIER.get(code, 1)


def _fetch_option_data(asset_type: str, underlying: str, start_date: str, end_date: str) -> pd.DataFrame:
    if engine is None:
        return pd.DataFrame()

    if asset_type == "etf":
        sql = text(
            """
            SELECT d.trade_date, d.ts_code, d.close, d.oi,
                   b.call_put, b.underlying, b.exercise_price, b.delist_date
            FROM option_daily d
            INNER JOIN option_basic b ON d.ts_code = b.ts_code
            WHERE b.underlying = :underlying
              AND d.trade_date BETWEEN :start_date AND :end_date
            """
        )
        params = {"underlying": underlying, "start_date": start_date, "end_date": end_date}
        return pd.read_sql(sql, engine, params=params)

    sql = text(
        """
        SELECT d.trade_date, d.ts_code, d.close, d.oi,
               b.call_put, b.underlying, b.exercise_price, b.delist_date
        FROM commodity_opt_daily d
        INNER JOIN commodity_option_basic b ON d.ts_code = b.ts_code
        WHERE b.underlying = :underlying
          AND d.trade_date BETWEEN :start_date AND :end_date
        """
    )
    params = {"underlying": underlying, "start_date": start_date, "end_date": end_date}
    return pd.read_sql(sql, engine, params=params)

def _fetch_underlying_prices(underlying: str, start_date: str, end_date: str) -> dict:
    if engine is None:
        return {}
    sql = text(
        """
        SELECT trade_date, close_price
        FROM stock_price
        WHERE ts_code = :ts_code
          AND trade_date BETWEEN :start_date AND :end_date
        """
    )
    params = {"ts_code": underlying, "start_date": start_date, "end_date": end_date}
    df = pd.read_sql(sql, engine, params=params)
    if df.empty:
        raw = underlying.replace(".SH", "").replace(".SZ", "")
        params = {"ts_code": raw, "start_date": start_date, "end_date": end_date}
        df = pd.read_sql(sql, engine, params=params)
    if df.empty:
        return {}
    return {str(r["trade_date"]): float(r["close_price"]) for _, r in df.iterrows()}


def _calc_short_margin(
    S: float,
    K: float,
    cp: str,
    multiplier: float,
    margin_rate: float,
    premium_total: float,
    lots: int,
) -> float:
    if S is None or K is None:
        return 0.0
    if cp == "C":
        otm = max(K - S, 0) * multiplier
        base = max(S * multiplier * margin_rate - otm, 0)
    else:
        otm = max(S - K, 0) * multiplier
        base = max(S * multiplier * margin_rate - otm, K * multiplier * margin_rate)
    margin = base * lots + premium_total
    # Floor: at least 8% of notional
    floor = S * multiplier * 0.08 * lots
    return max(margin, floor)

def get_etf_underlyings():
    if engine is None:
        return []
    sql = text("SELECT DISTINCT underlying FROM option_basic WHERE underlying IS NOT NULL ORDER BY underlying")
    with engine.connect() as conn:
        rows = conn.execute(sql).fetchall()
    return [r[0] for r in rows if r and r[0]]


def get_etf_expiries(underlying: str, trade_date: str):
    if engine is None:
        return []
    sql = text(
        """
        SELECT DISTINCT b.delist_date
        FROM option_daily d
        INNER JOIN option_basic b ON d.ts_code = b.ts_code
        WHERE b.underlying = :underlying
          AND d.trade_date = :trade_date
          AND b.delist_date IS NOT NULL
        ORDER BY b.delist_date
        """
    )
    with engine.connect() as conn:
        rows = conn.execute(sql, {"underlying": underlying, "trade_date": trade_date}).fetchall()
    return [str(r[0]) for r in rows if r and r[0]]


def get_etf_strikes_for_expiry(underlying: str, trade_date: str, expiry: str):
    if engine is None:
        return {"C": [], "P": []}
    sql = text(
        """
        SELECT DISTINCT b.exercise_price, b.call_put
        FROM option_daily d
        INNER JOIN option_basic b ON d.ts_code = b.ts_code
        WHERE b.underlying = :underlying
          AND d.trade_date = :trade_date
          AND b.delist_date = :expiry
        """
    )
    df = pd.read_sql(sql, engine, params={"underlying": underlying, "trade_date": trade_date, "expiry": expiry})
    if df.empty:
        return {"C": [], "P": []}
    df["call_put"] = df["call_put"].apply(_normalize_call_put)
    df["exercise_price"] = pd.to_numeric(df["exercise_price"], errors="coerce")
    df = df[df["exercise_price"].apply(_is_standard_strike)]
    strikes = {"C": [], "P": []}
    for cp in ["C", "P"]:
        strikes[cp] = sorted(df[df["call_put"] == cp]["exercise_price"].dropna().unique().tolist())
    return strikes


def get_etf_strikes_for_range(underlying: str, start_date: str, end_date: str):
    if engine is None:
        return {"C": [], "P": []}
    sql = text(
        """
        SELECT DISTINCT b.exercise_price, b.call_put
        FROM option_daily d
        INNER JOIN option_basic b ON d.ts_code = b.ts_code
        WHERE b.underlying = :underlying
          AND d.trade_date BETWEEN :start_date AND :end_date
        """
    )
    df = pd.read_sql(
        sql,
        engine,
        params={"underlying": underlying, "start_date": start_date, "end_date": end_date},
    )
    if df.empty:
        return {"C": [], "P": []}
    df["call_put"] = df["call_put"].apply(_normalize_call_put)
    df["exercise_price"] = pd.to_numeric(df["exercise_price"], errors="coerce")
    df = df.dropna(subset=["call_put", "exercise_price"])
    df = df[df["exercise_price"].apply(_is_standard_strike)]
    strikes = {"C": [], "P": []}
    for cp in ["C", "P"]:
        strikes[cp] = sorted(df[df["call_put"] == cp]["exercise_price"].dropna().unique().tolist())
    return strikes


def get_etf_first_trade_date(underlying: str, start_date: str, end_date: str):
    if engine is None:
        return None
    sql = text(
        """
        SELECT MIN(d.trade_date) AS td
        FROM option_daily d
        INNER JOIN option_basic b ON d.ts_code = b.ts_code
        WHERE b.underlying = :underlying
          AND d.trade_date BETWEEN :start_date AND :end_date
        """
    )
    with engine.connect() as conn:
        row = conn.execute(sql, {"underlying": underlying, "start_date": start_date, "end_date": end_date}).fetchone()
    if row and row[0]:
        return str(row[0])
    return None


def _calc_max_drawdown(nav: pd.Series) -> float:
    if nav.empty:
        return 0.0
    peak = nav.cummax()
    drawdown = nav / peak - 1.0
    return float(drawdown.min())


def run_max_oi_backtest(
    symbol: str,
    option_type: str,
    start_date: str = None,
    end_date: str = None,
    fee_rate: float = 0.0003,
):
    if engine is None:
        return {"error": "❌ 数据库未连接"}

    asset_type, underlying, display = _resolve_symbol(symbol)
    if not asset_type:
        return {"error": f"⚠️ 无法识别标的: {symbol}"}

    if end_date is None:
        end_date = datetime.now().strftime("%Y%m%d")
    if start_date is None:
        start_date = (datetime.strptime(end_date, "%Y%m%d") - timedelta(days=180)).strftime("%Y%m%d")

    df = _fetch_option_data(asset_type, underlying, start_date, end_date)
    if df.empty:
        return {"error": f"⚠️ {display} 在区间 {start_date}-{end_date} 无数据"}

    df = df.dropna(subset=["close", "oi"])
    df["call_put"] = df["call_put"].astype(str).str.upper()
    df = df[df["call_put"] == option_type.upper()]
    if df.empty:
        return {"error": f"⚠️ {display} 没有 {option_type} 合约数据"}

    df = df.sort_values(["ts_code", "trade_date"])
    df["next_close"] = df.groupby("ts_code")["close"].shift(-1)
    df["next_date"] = df.groupby("ts_code")["trade_date"].shift(-1)

    df_day = df.sort_values(["trade_date", "oi"], ascending=[True, False]).groupby("trade_date").head(1)
    df_day = df_day.dropna(subset=["next_close"])

    if df_day.empty:
        return {"error": f"⚠️ {display} 没有可用的连续价格用于回测"}

    df_day["ret"] = (df_day["next_close"] - df_day["close"]) / df_day["close"] - fee_rate * 2
    df_day = df_day.rename(columns={"trade_date": "entry_date"})
    df_day["exit_date"] = df_day["next_date"]
    df_day = df_day[["entry_date", "exit_date", "ts_code", "close", "next_close", "ret"]]
    df_day = df_day.sort_values("entry_date")

    nav = (1 + df_day["ret"]).cumprod()
    total_return = float(nav.iloc[-1] - 1.0)
    n_days = len(nav)
    ann_return = float((nav.iloc[-1] ** (252 / n_days) - 1.0) if n_days > 0 else 0.0)
    max_dd = _calc_max_drawdown(nav)
    win_rate = float((df_day["ret"] > 0).mean())
    avg_ret = float(df_day["ret"].mean())

    multiplier = _get_multiplier(asset_type, underlying)

    summary = {
        "symbol": display,
        "asset_type": asset_type,
        "strategy": f"max_oi_{'call' if option_type.upper() == 'C' else 'put'}",
        "start_date": df_day["entry_date"].iloc[0],
        "end_date": df_day["exit_date"].iloc[-1],
        "trades": len(df_day),
        "total_return": total_return,
        "annualized_return": ann_return,
        "max_drawdown": max_dd,
        "win_rate": win_rate,
        "avg_return": avg_ret,
        "fee_rate": fee_rate,
        "multiplier": multiplier,
    }

    equity = pd.DataFrame(
        {
            "date": df_day["exit_date"].values,
            "nav": nav.values,
        }
    )

    return {
        "summary": summary,
        "trades": df_day.reset_index(drop=True),
        "equity": equity,
    }


def _normalize_call_put(val: str) -> str:
    v = str(val).strip().upper()
    if "认购" in v:
        return "C"
    if "认沽" in v:
        return "P"
    return v


def _is_standard_strike(val: float) -> bool:
    if val is None or (isinstance(val, float) and np.isnan(val)):
        return False
    try:
        x = float(val)
    except Exception:
        return False
    r = round(x, 2)
    if abs(r - x) > 1e-6:
        return False
    return int(round(r * 100)) % 5 == 0


def _is_effective_standard(val: float, target_strike: float | None) -> bool:
    if target_strike is None:
        return _is_standard_strike(val)
    try:
        v = float(val)
        t = float(target_strike)
    except Exception:
        return False
    if abs(v - t) < 1e-6:
        return True
    return _is_standard_strike(v)


def run_etf_roll_backtest(
    underlying: str,
    strategy: str,
    start_date: str = None,
    end_date: str = None,
    fee_per_lot: float = 2.0,
    margin_rate: float = 0.15,
    strike_mode: str = "ATM",
    manual_params: dict | None = None,
    lots: int = 1,
    calendar_type: str = "C",
):
    """
    ETF 近月滚动策略回测：
    - double_sell: 双卖（同月卖出OI最大认购+认沽）
    - deep_otm_put: 买深虚值看跌（同月最小行权价认沽）
    """
    if engine is None:
        return {"error": "❌ 数据库未连接"}

    underlying = _normalize_etf_code(underlying)
    if end_date is None:
        end_date = datetime.now().strftime("%Y%m%d")
    if start_date is None:
        start_date = (datetime.strptime(end_date, "%Y%m%d") - timedelta(days=365)).strftime("%Y%m%d")

    if strategy == "hold_underlying":
        underlying_prices = _fetch_underlying_prices(underlying, start_date, end_date)
        if not underlying_prices:
            return {"error": f"⚠️ {underlying} 在区间 {start_date}-{end_date} 无标的价格数据"}
        dates = sorted(underlying_prices.keys())
        if not dates:
            return {"error": f"⚠️ {underlying} 在区间 {start_date}-{end_date} 无标的价格数据"}

        multiplier = 10000
        pnl_series = []
        cum_pnl = 0.0
        prev_date = None
        for date_str in dates:
            if prev_date:
                p0 = underlying_prices.get(prev_date)
                p1 = underlying_prices.get(date_str)
                if p0 is None or p1 is None:
                    pnl_series.append((date_str, None))
                    prev_date = date_str
                    continue
                day_pnl = (p1 - p0) * multiplier * lots
                cum_pnl += day_pnl
            pnl_series.append((date_str, cum_pnl))
            prev_date = date_str

        equity = pd.DataFrame(pnl_series, columns=["date", "pnl"])
        pnl_values = equity["pnl"]
        total_return = float(pnl_values.iloc[-1]) if not pnl_values.empty else 0.0
        try:
            cal_days = (datetime.strptime(end_date, "%Y%m%d") - datetime.strptime(start_date, "%Y%m%d")).days + 1
        except Exception:
            cal_days = len(pnl_values)
        ann_return = float(total_return / cal_days * 365) if cal_days > 0 else 0.0
        max_dd = 0.0
        if not pnl_values.empty:
            peak = pnl_values.cummax()
            dd = pnl_values - peak
            max_dd = float(dd.min())
        avg_return = 0.0
        entry_price = underlying_prices.get(dates[0])
        invested = entry_price * 10000 * lots if entry_price is not None else 0.0
        ann_return_pct = None
        if invested > 0 and cal_days > 0:
            ann_return_pct = float((total_return / invested) * 365 / cal_days)
        summary = {
            "symbol": underlying.replace(".SH", "").replace(".SZ", ""),
            "strategy": strategy,
            "start_date": equity["date"].iloc[0],
            "end_date": equity["date"].iloc[-1],
            "trades": 1,
            "total_pnl": total_return,
            "annualized_pnl": ann_return,
            "max_drawdown": max_dd,
            "win_rate": 0.0,
            "avg_return": avg_return,
            "fee_per_lot": 0.0,
            "avg_margin": 0.0,
            "annualized_return_pct": ann_return_pct,
            "premium_paid_total": 0.0,
            "fee_total": 0.0,
            "realized_pnl": total_return,
            "unrealized_pnl": 0.0,
        }
        return {
            "summary": summary,
            "trades": pd.DataFrame(),
            "equity": equity,
            "open_positions": pd.DataFrame(
                [
                    {
                        "ts_code": underlying,
                        "name": "持有标的ETF",
                        "entry_date": dates[0],
                        "entry_price": underlying_prices.get(dates[0]),
                        "last_price": underlying_prices.get(dates[-1]),
                        "unrealized_pnl": total_return,
                        "margin": 0.0,
                        "underlying_price": underlying_prices.get(dates[-1]),
                    }
                ]
            ),
            "missing_dates": [],
            "no_contract_dates": [],
        }

    df = _fetch_option_data("etf", underlying, start_date, end_date)
    if df.empty:
        return {"error": f"⚠️ {underlying} 在区间 {start_date}-{end_date} 无数据"}

    df = df.dropna(subset=["close"])
    df["call_put"] = df["call_put"].apply(_normalize_call_put)
    df = df.dropna(subset=["call_put"])
    df["exercise_price"] = pd.to_numeric(df["exercise_price"], errors="coerce")

    df = df.sort_values(["trade_date", "ts_code"])
    dates = sorted(df["trade_date"].unique().tolist())
    if len(dates) < 2:
        return {"error": f"⚠️ {underlying} 数据天数不足"}

    # 价格索引
    price_map = {(r["ts_code"], r["trade_date"]): r["close"] for _, r in df.iterrows()}
    strike_map = {r["ts_code"]: r["exercise_price"] for _, r in df.dropna(subset=["exercise_price"]).iterrows()}
    call_put_map = {r["ts_code"]: r["call_put"] for _, r in df.iterrows()}
    underlying_prices = _fetch_underlying_prices(underlying, start_date, end_date)

    def _select_contract(
        df_exp,
        call_put,
        target_strike=None,
        standard_only=True,
        direction_filter=None,
        tolerance_steps: int | None = None,
    ):
        df_cp = df_exp[df_exp["call_put"] == call_put].copy()
        if df_cp.empty:
            return None
        if target_strike is None:
            if standard_only:
                df_cp = df_cp[df_cp["exercise_price"].apply(_is_standard_strike)]
            if df_cp.empty:
                return None
            return df_cp.sort_values("oi", ascending=False).iloc[0]
        if standard_only:
            df_cp = df_cp[df_cp["exercise_price"].apply(lambda x: _is_effective_standard(x, target_strike))]
        if df_cp.empty:
            return None
        if direction_filter == "gte":
            df_cp = df_cp[df_cp["exercise_price"] >= target_strike]
            if df_cp.empty:
                return None
        elif direction_filter == "lte":
            df_cp = df_cp[df_cp["exercise_price"] <= target_strike]
            if df_cp.empty:
                return None
        df_cp["diff"] = (df_cp["exercise_price"] - target_strike).abs()
        if tolerance_steps is not None and tolerance_steps >= 0:
            uniq = sorted(df_cp["exercise_price"].dropna().unique().tolist())
            tick = None
            for i in range(1, len(uniq)):
                d = uniq[i] - uniq[i - 1]
                if d > 1e-6:
                    tick = d
                    break
            if tick is not None:
                max_diff = tick * tolerance_steps
                df_tol = df_cp[df_cp["diff"] <= max_diff]
                if not df_tol.empty:
                    df_cp = df_tol
        df_cp = df_cp.sort_values(["diff", "oi"], ascending=[True, False])
        return df_cp.iloc[0]

    def _select_by_otm(df_exp, call_put, S, target_otm, prefer_standard=True):
        if df_exp is None or df_exp.empty or S is None:
            return None
        df_cp = df_exp[df_exp["call_put"] == call_put].copy()
        if df_cp.empty:
            return None
        df_cp["exercise_price"] = pd.to_numeric(df_cp["exercise_price"], errors="coerce")
        df_cp = df_cp.dropna(subset=["exercise_price"])
        if df_cp.empty:
            return None
        # Direction preference: call K >= S, put K <= S
        if call_put == "C":
            df_dir = df_cp[df_cp["exercise_price"] >= S]
        else:
            df_dir = df_cp[df_cp["exercise_price"] <= S]
        if df_dir.empty:
            df_dir = df_cp
        # Prefer standard strikes if available
        if prefer_standard:
            df_std = df_dir[df_dir["exercise_price"].apply(_is_standard_strike)]
            if not df_std.empty:
                df_dir = df_std
        # Compute OTM percentage
        if call_put == "C":
            df_dir["otm_pct"] = df_dir["exercise_price"] / S - 1.0
        else:
            df_dir["otm_pct"] = 1.0 - df_dir["exercise_price"] / S
        df_dir["otm_diff"] = (df_dir["otm_pct"] - target_otm).abs()
        df_dir = df_dir.sort_values(["otm_diff", "oi"], ascending=[True, False])
        return df_dir.iloc[0]

    def _next_strike(df_exp, call_put, base_strike, direction="up", prefer_standard=True):
        if df_exp is None or df_exp.empty or base_strike is None:
            return None
        df_cp = df_exp[df_exp["call_put"] == call_put].copy()
        if df_cp.empty:
            return None
        df_cp["exercise_price"] = pd.to_numeric(df_cp["exercise_price"], errors="coerce")
        df_cp = df_cp.dropna(subset=["exercise_price"])
        if df_cp.empty:
            return None
        if prefer_standard:
            df_std = df_cp[df_cp["exercise_price"].apply(_is_standard_strike)]
            if not df_std.empty:
                df_cp = df_std
        uniq = sorted(df_cp["exercise_price"].unique().tolist())
        if direction == "up":
            cands = [x for x in uniq if x > base_strike + 1e-9]
            if not cands:
                return None
            target = cands[0]
        else:
            cands = [x for x in uniq if x < base_strike - 1e-9]
            if not cands:
                return None
            target = cands[-1]
        df_pick = df_cp[df_cp["exercise_price"] == target]
        if df_pick.empty:
            return None
        return df_pick.sort_values("oi", ascending=False).iloc[0]

    def _select_contract_debug(
        df_exp,
        call_put,
        target_strike,
        standard_only=True,
        direction_filter=None,
        tolerance_steps: int | None = None,
    ):
        info = {
            "target": target_strike,
            "cnt_all": 0,
            "cnt_std": 0,
            "cnt_dir": 0,
            "min_all": None,
            "max_all": None,
            "min_std": None,
            "max_std": None,
            "min_dir": None,
            "max_dir": None,
            "selected_strike": None,
            "selected_ts": None,
        }
        if df_exp is None or df_exp.empty:
            return None, info
        df_cp = df_exp[df_exp["call_put"] == call_put].copy()
        if df_cp.empty:
            return None, info
        df_cp["exercise_price"] = pd.to_numeric(df_cp["exercise_price"], errors="coerce")
        df_cp = df_cp.dropna(subset=["exercise_price"])
        if df_cp.empty:
            return None, info
        info["cnt_all"] = int(df_cp.shape[0])
        info["min_all"] = float(df_cp["exercise_price"].min())
        info["max_all"] = float(df_cp["exercise_price"].max())
        df_std = df_cp
        if standard_only:
            df_std = df_cp[df_cp["exercise_price"].apply(lambda x: _is_effective_standard(x, target_strike))]
        if df_std.empty:
            info["cnt_std"] = 0
            return None, info
        info["cnt_std"] = int(df_std.shape[0])
        info["min_std"] = float(df_std["exercise_price"].min())
        info["max_std"] = float(df_std["exercise_price"].max())
        df_dir = df_std
        if direction_filter == "gte":
            df_dir = df_std[df_std["exercise_price"] >= target_strike]
            if df_dir.empty:
                info["cnt_dir"] = 0
                return None, info
        elif direction_filter == "lte":
            df_dir = df_std[df_std["exercise_price"] <= target_strike]
            if df_dir.empty:
                info["cnt_dir"] = 0
                return None, info
        info["cnt_dir"] = int(df_dir.shape[0])
        info["min_dir"] = float(df_dir["exercise_price"].min())
        info["max_dir"] = float(df_dir["exercise_price"].max())
        df_dir = df_dir.copy()
        df_dir["diff"] = (df_dir["exercise_price"] - target_strike).abs()
        if tolerance_steps is not None and tolerance_steps >= 0:
            uniq = sorted(df_dir["exercise_price"].dropna().unique().tolist())
            tick = None
            for i in range(1, len(uniq)):
                d = uniq[i] - uniq[i - 1]
                if d > 1e-6:
                    tick = d
                    break
            if tick is not None:
                max_diff = tick * tolerance_steps
                df_tol = df_dir[df_dir["diff"] <= max_diff]
                if not df_tol.empty:
                    df_dir = df_tol
        df_dir = df_dir.sort_values(["diff", "oi"], ascending=[True, False])
        pick = df_dir.iloc[0]
        info["selected_strike"] = float(pick["exercise_price"])
        info["selected_ts"] = pick["ts_code"]
        return pick, info

    def _select_contract_exact(df_exp, call_put, target_strike, standard_only=True):
        df_cp = df_exp[df_exp["call_put"] == call_put].copy()
        if df_cp.empty:
            return None
        if standard_only:
            df_cp = df_cp[df_cp["exercise_price"].apply(lambda x: _is_effective_standard(x, target_strike))]
        df_cp = df_cp[df_cp["exercise_price"] == target_strike]
        if df_cp.empty:
            return None
        return df_cp.sort_values("oi", ascending=False).iloc[0]

    def _pick_strike(S, call_put, mode, manual_key=None):
        if mode == "ATM":
            return S
        if mode == "OTM5":
            return S * 1.05 if call_put == "C" else S * 0.95
        if mode == "OTM10":
            return S * 1.10 if call_put == "C" else S * 0.90
        if mode == "MANUAL" and manual_params:
            return manual_params.get(manual_key)
        return S

    calendar_diag_rows = []
    pick_diag_rows = []

    def _strike_stats(df_exp: pd.DataFrame) -> dict:
        stats = {
            "call_min": None,
            "call_max": None,
            "put_min": None,
            "put_max": None,
            "call_cnt": 0,
            "put_cnt": 0,
        }
        if df_exp is None or df_exp.empty:
            return stats
        df_c = df_exp[df_exp["call_put"] == "C"]
        df_p = df_exp[df_exp["call_put"] == "P"]
        if not df_c.empty:
            stats["call_cnt"] = int(df_c.shape[0])
            stats["call_min"] = float(df_c["exercise_price"].min())
            stats["call_max"] = float(df_c["exercise_price"].max())
        if not df_p.empty:
            stats["put_cnt"] = int(df_p.shape[0])
            stats["put_min"] = float(df_p["exercise_price"].min())
            stats["put_max"] = float(df_p["exercise_price"].max())
        return stats

    def pick_contracts(date_str: str, min_expiry: str = None, standard_only: bool = True, reason: str = "entry"):
        df_today = df[df["trade_date"] == date_str]
        if df_today.empty:
            return None, None
        df_valid = df_today[df_today["delist_date"] >= date_str]
        if min_expiry is not None:
            df_valid = df_valid[df_valid["delist_date"] > min_expiry]
        if df_valid.empty:
            if strategy == "calendar_spread":
                calendar_diag_rows.append(
                    {
                        "date": date_str,
                        "near_expiry": None,
                        "far_expiry": None,
                        "near_cnt": 0,
                        "far_cnt": 0,
                        "cp": None,
                    }
                )
            return None, None
        expiries = sorted(df_valid["delist_date"].unique().tolist())
        expiry = expiries[0]
        df_exp = df_valid[df_valid["delist_date"] == expiry]

        # 获取标的价格（若当日缺失，用上一交易日价格兜底）
        S = underlying_prices.get(date_str)
        if S is None:
            S = last_S
        if S is None:
            return None, None

        target_otm = 0.0
        if strike_mode == "OTM5":
            target_otm = 0.05
        elif strike_mode == "OTM10":
            target_otm = 0.10

        if strategy in {"double_sell", "double_buy"}:
            call_strike = _pick_strike(S, "C", strike_mode, "call_strike")
            put_strike = _pick_strike(S, "P", strike_mode, "put_strike")
            call = None
            put = None
            expiry_used = None
            df_exp_used = None
            call_dbg = {}
            put_dbg = {}
            if strike_mode == "MANUAL":
                call = _select_contract_exact(df_exp, "C", call_strike, standard_only=standard_only)
                put = _select_contract_exact(df_exp, "P", put_strike, standard_only=standard_only)
                if call is None:
                    call, call_dbg = _select_contract_debug(
                        df_exp,
                        "C",
                        call_strike,
                        standard_only=standard_only,
                        direction_filter="gte",
                        tolerance_steps=tolerance_steps if standard_only else None,
                    )
                else:
                    call_dbg = {"selected_strike": strike_map.get(call["ts_code"]), "selected_ts": call["ts_code"]}
                if put is None:
                    put, put_dbg = _select_contract_debug(
                        df_exp,
                        "P",
                        put_strike,
                        standard_only=standard_only,
                        direction_filter="lte",
                        tolerance_steps=tolerance_steps if standard_only else None,
                    )
                else:
                    put_dbg = {"selected_strike": strike_map.get(put["ts_code"]), "selected_ts": put["ts_code"]}
                expiry_used = expiry
                df_exp_used = df_exp
            else:
                df_exp = df_valid[df_valid["delist_date"] == expiry]
                call = _select_by_otm(df_exp, "C", S, target_otm, prefer_standard=True)
                put = _select_by_otm(df_exp, "P", S, target_otm, prefer_standard=True)
                if call is None or put is None:
                    call = _select_by_otm(df_exp, "C", S, target_otm, prefer_standard=False)
                    put = _select_by_otm(df_exp, "P", S, target_otm, prefer_standard=False)
                if call is not None and put is not None:
                    expiry_used = expiry
                    df_exp_used = df_exp
                    call_dbg = {"selected_strike": strike_map.get(call["ts_code"]), "selected_ts": call["ts_code"]}
                    put_dbg = {"selected_strike": strike_map.get(put["ts_code"]), "selected_ts": put["ts_code"]}
            if call is None or put is None or expiry_used is None:
                return None, None
            stats = _strike_stats(df_exp_used)
            direction = "short" if strategy == "double_sell" else "long"
            contracts = [
                {"ts_code": call["ts_code"], "direction": direction, "lots": lots},
                {"ts_code": put["ts_code"], "direction": direction, "lots": lots},
            ]
            call_strike_pick = strike_map.get(call["ts_code"])
            put_strike_pick = strike_map.get(put["ts_code"])
            pick_diag_rows.append(
                {
                    "date": date_str,
                    "reason": reason,
                    "strategy": strategy,
                    "S": S,
                    "target_call": call_strike,
                    "target_put": put_strike,
                    "picked_call": call_strike_pick,
                    "picked_put": put_strike_pick,
                    "call_ts": call["ts_code"],
                    "put_ts": put["ts_code"],
                    "expiry": expiry_used,
                    "expiries": ",".join([str(x) for x in expiries[:6]]),
                    "standard_only": standard_only,
                    "call_min": stats["call_min"],
                    "call_max": stats["call_max"],
                    "put_min": stats["put_min"],
                    "put_max": stats["put_max"],
                    "call_cnt": stats["call_cnt"],
                    "put_cnt": stats["put_cnt"],
                    "call_cnt_all": call_dbg.get("cnt_all") if isinstance(call_dbg, dict) else None,
                    "call_cnt_std": call_dbg.get("cnt_std") if isinstance(call_dbg, dict) else None,
                    "call_cnt_dir": call_dbg.get("cnt_dir") if isinstance(call_dbg, dict) else None,
                    "call_min_std": call_dbg.get("min_std") if isinstance(call_dbg, dict) else None,
                    "call_max_std": call_dbg.get("max_std") if isinstance(call_dbg, dict) else None,
                    "call_min_dir": call_dbg.get("min_dir") if isinstance(call_dbg, dict) else None,
                    "call_max_dir": call_dbg.get("max_dir") if isinstance(call_dbg, dict) else None,
                    "put_cnt_all": put_dbg.get("cnt_all") if isinstance(put_dbg, dict) else None,
                    "put_cnt_std": put_dbg.get("cnt_std") if isinstance(put_dbg, dict) else None,
                    "put_cnt_dir": put_dbg.get("cnt_dir") if isinstance(put_dbg, dict) else None,
                    "put_min_std": put_dbg.get("min_std") if isinstance(put_dbg, dict) else None,
                    "put_max_std": put_dbg.get("max_std") if isinstance(put_dbg, dict) else None,
                    "put_min_dir": put_dbg.get("min_dir") if isinstance(put_dbg, dict) else None,
                    "put_max_dir": put_dbg.get("max_dir") if isinstance(put_dbg, dict) else None,
                }
            )
            return expiry_used, contracts

        if strategy in {"single_call", "single_put", "single_sell_call", "single_sell_put"}:
            if strategy in {"single_call", "single_sell_call"}:
                cp = "C"
            else:
                cp = "P"
            mode = strike_mode
            strike = _pick_strike(S, cp, mode, "single_strike")
            if strike_mode == "MANUAL":
                pick = _select_contract_exact(df_exp, cp, strike, standard_only=standard_only)
                if pick is None:
                    pick = _select_by_otm(df_exp, cp, S, target_otm, prefer_standard=True)
            else:
                pick = _select_by_otm(df_exp, cp, S, target_otm, prefer_standard=True)
                if pick is None:
                    pick = _select_by_otm(df_exp, cp, S, target_otm, prefer_standard=False)
            if pick is None:
                return None, None
            direction = "short" if strategy in {"single_sell_call", "single_sell_put"} else "long"
            contracts = [{"ts_code": pick["ts_code"], "direction": direction, "lots": lots}]
            pick_diag_rows.append(
                {
                    "date": date_str,
                    "reason": reason,
                    "strategy": strategy,
                    "S": S,
                    "target_call": strike if cp == "C" else None,
                    "target_put": strike if cp == "P" else None,
                    "picked_call": strike_map.get(pick["ts_code"]) if cp == "C" else None,
                    "picked_put": strike_map.get(pick["ts_code"]) if cp == "P" else None,
                    "call_ts": pick["ts_code"] if cp == "C" else None,
                    "put_ts": pick["ts_code"] if cp == "P" else None,
                    "expiry": expiry,
                    "standard_only": standard_only,
                }
            )
            return expiry, contracts

        if strategy in {"bull_spread", "bear_spread"}:
            if strategy == "bull_spread":
                if strike_mode == "MANUAL":
                    low = _pick_strike(S, "C", strike_mode, "low_strike")
                    high = _pick_strike(S, "C", strike_mode, "high_strike")
                    buy_leg = _select_contract_exact(df_exp, "C", low, standard_only=standard_only)
                    sell_leg = _select_contract_exact(df_exp, "C", high, standard_only=standard_only)
                    if buy_leg is None:
                        buy_leg = _select_by_otm(df_exp, "C", S, target_otm, prefer_standard=True)
                    if sell_leg is None:
                        base_strike = buy_leg["exercise_price"] if buy_leg is not None else None
                        sell_leg = _next_strike(df_exp, "C", base_strike, direction="up", prefer_standard=True)
                else:
                    buy_leg = _select_by_otm(df_exp, "C", S, target_otm, prefer_standard=True)
                    base_strike = buy_leg["exercise_price"] if buy_leg is not None else None
                    sell_leg = _next_strike(df_exp, "C", base_strike, direction="up", prefer_standard=True)
                    if buy_leg is None or sell_leg is None:
                        buy_leg = _select_by_otm(df_exp, "C", S, target_otm, prefer_standard=False)
                        base_strike = buy_leg["exercise_price"] if buy_leg is not None else None
                        sell_leg = _next_strike(df_exp, "C", base_strike, direction="up", prefer_standard=False)
                if buy_leg is None or sell_leg is None:
                    return None, None
                contracts = [
                    {"ts_code": buy_leg["ts_code"], "direction": "long", "lots": lots},
                    {"ts_code": sell_leg["ts_code"], "direction": "short", "lots": lots},
                ]
                pick_diag_rows.append(
                    {
                        "date": date_str,
                        "reason": reason,
                        "strategy": strategy,
                        "S": S,
                    "target_call": S * (1.0 + target_otm) if strike_mode in {"OTM5", "OTM10"} else S,
                    "target_put": None,
                    "picked_call": strike_map.get(buy_leg["ts_code"]),
                    "picked_put": None,
                    "call_ts": buy_leg["ts_code"],
                    "put_ts": None,
                    "expiry": expiry,
                    "standard_only": standard_only,
                }
            )
                return expiry, contracts
            else:
                if strike_mode == "MANUAL":
                    high = _pick_strike(S, "P", strike_mode, "high_strike")
                    low = _pick_strike(S, "P", strike_mode, "low_strike")
                    buy_leg = _select_contract_exact(df_exp, "P", high, standard_only=standard_only)
                    sell_leg = _select_contract_exact(df_exp, "P", low, standard_only=standard_only)
                    if buy_leg is None:
                        buy_leg = _select_by_otm(df_exp, "P", S, target_otm, prefer_standard=True)
                    if sell_leg is None:
                        base_strike = buy_leg["exercise_price"] if buy_leg is not None else None
                        sell_leg = _next_strike(df_exp, "P", base_strike, direction="down", prefer_standard=True)
                else:
                    buy_leg = _select_by_otm(df_exp, "P", S, target_otm, prefer_standard=True)
                    base_strike = buy_leg["exercise_price"] if buy_leg is not None else None
                    sell_leg = _next_strike(df_exp, "P", base_strike, direction="down", prefer_standard=True)
                    if buy_leg is None or sell_leg is None:
                        buy_leg = _select_by_otm(df_exp, "P", S, target_otm, prefer_standard=False)
                        base_strike = buy_leg["exercise_price"] if buy_leg is not None else None
                        sell_leg = _next_strike(df_exp, "P", base_strike, direction="down", prefer_standard=False)
                if buy_leg is None or sell_leg is None:
                    return None, None
                contracts = [
                    {"ts_code": buy_leg["ts_code"], "direction": "long", "lots": lots},
                    {"ts_code": sell_leg["ts_code"], "direction": "short", "lots": lots},
                ]
                pick_diag_rows.append(
                    {
                        "date": date_str,
                        "reason": reason,
                        "strategy": strategy,
                        "S": S,
                    "target_call": None,
                    "target_put": S * (1.0 - target_otm) if strike_mode in {"OTM5", "OTM10"} else S,
                    "picked_call": None,
                    "picked_put": strike_map.get(buy_leg["ts_code"]),
                    "call_ts": None,
                    "put_ts": buy_leg["ts_code"],
                    "expiry": expiry,
                    "standard_only": standard_only,
                }
            )
                return expiry, contracts

        if strategy == "calendar_spread":
            if len(expiries) < 2:
                calendar_diag_rows.append(
                    {
                        "date": date_str,
                        "near_expiry": expiry,
                        "far_expiry": None,
                        "near_cnt": int(df_exp.shape[0]),
                        "far_cnt": 0,
                        "cp": "C" if "认购" in str(calendar_type) else "P",
                    }
                )
                return None, None
            expiry_far = expiries[1]
            df_far = df_valid[df_valid["delist_date"] == expiry_far]
            cal_label = str(calendar_type)
            cp = "C" if "认购" in cal_label else "P"
            calendar_diag_rows.append(
                {
                    "date": date_str,
                    "near_expiry": expiry,
                    "far_expiry": expiry_far,
                    "near_cnt": int(df_exp[df_exp["call_put"] == cp].shape[0]),
                    "far_cnt": int(df_far[df_far["call_put"] == cp].shape[0]),
                    "cp": cp,
                }
            )
            strike = _pick_strike(S, cp, strike_mode, "calendar_strike")
            if strike_mode == "MANUAL":
                near_leg = _select_contract_exact(df_exp, cp, strike, standard_only=standard_only)
                far_leg = _select_contract_exact(df_far, cp, strike, standard_only=standard_only)
                if near_leg is None:
                    near_leg = _select_by_otm(df_exp, cp, S, target_otm, prefer_standard=True)
                if far_leg is None:
                    far_leg = _select_by_otm(df_far, cp, S, target_otm, prefer_standard=True)
            else:
                near_leg = _select_by_otm(df_exp, cp, S, target_otm, prefer_standard=True)
                far_leg = _select_by_otm(df_far, cp, S, target_otm, prefer_standard=True)
                if near_leg is None:
                    near_leg = _select_by_otm(df_exp, cp, S, target_otm, prefer_standard=False)
                if far_leg is None:
                    far_leg = _select_by_otm(df_far, cp, S, target_otm, prefer_standard=False)
            if near_leg is None or far_leg is None:
                return None, None
            if "卖近买远" in cal_label:
                near_dir = "short"
                far_dir = "long"
            else:
                near_dir = "long"
                far_dir = "short"
            contracts = [
                {"ts_code": near_leg["ts_code"], "direction": near_dir, "lots": lots},
                {"ts_code": far_leg["ts_code"], "direction": far_dir, "lots": lots},
            ]
            pick_diag_rows.append(
                {
                    "date": date_str,
                    "reason": reason,
                    "strategy": strategy,
                    "S": S,
                    "target_call": strike if cp == "C" else None,
                    "target_put": strike if cp == "P" else None,
                    "picked_call": strike_map.get(near_leg["ts_code"]) if cp == "C" else None,
                    "picked_put": strike_map.get(near_leg["ts_code"]) if cp == "P" else None,
                    "call_ts": near_leg["ts_code"] if cp == "C" else None,
                    "put_ts": near_leg["ts_code"] if cp == "P" else None,
                    "expiry": expiry,
                    "standard_only": standard_only,
                }
            )
            return expiry, contracts

        return None, None

    cum_pnl = 0.0
    pnl_series = []
    margin_series = []
    missing_dates = []
    missing_detail = []
    no_contract_dates = []
    trades = []
    realized_pnl = 0.0

    current_expiry = None
    current_contracts = None
    entry_date = None
    entry_prices = {}
    locked_strikes = None
    leg_cum_pnl = {}
    fee_open_total = 0.0
    fee_total = 0.0
    premium_paid_total = 0.0
    spread_margin_entry = None

    prev_date = None
    last_S = None
    multiplier = _get_multiplier("etf", underlying)

    for date_str in dates:
        # 首次建仓
        if current_contracts is None:
            current_expiry, current_contracts = pick_contracts(date_str, standard_only=True, reason="entry")
            if current_contracts is None:
                current_expiry, current_contracts = pick_contracts(date_str, standard_only=False, reason="entry")
            if current_contracts is None:
                pnl_series.append((date_str, None))
                no_contract_dates.append(date_str)
                prev_date = date_str
                continue
            entry_date = date_str
            entry_prices = {c["ts_code"]: price_map.get((c["ts_code"], date_str)) for c in current_contracts}
            if any(v is None for v in entry_prices.values()):
                current_contracts = None
                pnl_series.append((date_str, None))
                no_contract_dates.append(date_str)
                prev_date = date_str
                continue
            # Spread margin (debit spread: max loss = net debit)
            spread_margin_entry = None
            if strategy in {"bull_spread", "bear_spread"}:
                net_debit = 0.0
                for c in current_contracts:
                    ep = entry_prices.get(c["ts_code"])
                    if ep is None:
                        net_debit = None
                        break
                    if c["direction"] == "long":
                        net_debit += ep * multiplier * c["lots"]
                    else:
                        net_debit -= ep * multiplier * c["lots"]
                if net_debit is not None:
                    spread_margin_entry = max(net_debit, 0.0)
            if strike_mode == "MANUAL":
                locked_strikes = {c["ts_code"]: strike_map.get(c["ts_code"]) for c in current_contracts}
            leg_cum_pnl = {c["ts_code"]: 0.0 for c in current_contracts}
            fee_open_total = 0.0
            for c in current_contracts:
                fee_open_total += fee_per_lot * c["lots"]
            cum_pnl -= fee_open_total
            fee_total += fee_open_total
            for c in current_contracts:
                if c["direction"] == "long":
                    ep = entry_prices.get(c["ts_code"])
                    if ep is not None:
                        premium_paid_total += ep * multiplier * c["lots"]
            if strategy in {"bull_spread", "bear_spread"} and spread_margin_entry is not None:
                S_entry = underlying_prices.get(entry_date)
                if S_entry is None:
                    S_entry = last_S
                floor = S_entry * multiplier * 0.08 * lots if S_entry is not None else 0.0
                spread_margin_entry = max(spread_margin_entry, floor)

        # 若当日无持仓（严格锁定导致空仓）
        if current_contracts is None:
            pnl_series.append((date_str, None))
            no_contract_dates.append(date_str)
            prev_date = date_str
            continue

        # 日收益
        if prev_date and current_contracts:
            day_pnl = 0.0
            missing_price = False
            for c in current_contracts:
                ts = c["ts_code"]
                lots = c["lots"]
                p0 = price_map.get((ts, prev_date))
                p1 = price_map.get((ts, date_str))
                ep = entry_prices.get(ts)
                if p0 is None or p1 is None or ep is None:
                    missing_price = True
                    missing_detail.append(
                        {
                            "date": date_str,
                            "ts_code": ts,
                            "p0": p0,
                            "p1": p1,
                            "entry_price": ep,
                        }
                    )
                    break
                # 当日标的盈亏（按权利金变化）
                if c["direction"] == "short":
                    raw = (p0 - p1) * multiplier * lots
                    day_pnl += raw
                else:
                    raw = (p1 - p0) * multiplier * lots
                    day_pnl += raw
            if missing_price:
                pnl_series.append((date_str, None))
                missing_dates.append(date_str)
                prev_date = date_str
                continue
            cum_pnl += day_pnl

        # 日保证金（仅卖方）
        if date_str in underlying_prices:
            last_S = underlying_prices.get(date_str)
        S = last_S
        if S is not None and current_contracts:
            margin_day = 0.0
            if strategy in {"bull_spread", "bear_spread"} and spread_margin_entry is not None:
                margin_day = spread_margin_entry
            else:
                for c in current_contracts:
                    if c["direction"] != "short":
                        continue
                    ts = c["ts_code"]
                    lots = c["lots"]
                    K = strike_map.get(ts)
                    ep = entry_prices.get(ts)
                    if K is None or ep is None:
                        continue
                    cp = call_put_map.get(ts)
                    premium = ep * multiplier * lots
                    margin_day += _calc_short_margin(S, K, cp, multiplier, margin_rate, premium, lots)
            if margin_day > 0:
                margin_series.append(margin_day)

        # 到期滚动：在到期日当日换月
        if current_expiry and date_str >= current_expiry:
            exit_prices = {c["ts_code"]: price_map.get((c["ts_code"], date_str)) for c in current_contracts}
            leg_returns = []
            for c in current_contracts:
                ts = c["ts_code"]
                lots = c["lots"]
                ep = entry_prices.get(ts)
                xp = exit_prices.get(ts)
                if ep is None or xp is None:
                    continue
                if c["direction"] == "short":
                    leg_returns.append((ep - xp) * multiplier * lots)
                else:
                    leg_returns.append((xp - ep) * multiplier * lots)

            fee_close_total = 0.0
            for c in current_contracts:
                ts = c["ts_code"]
                lots = c["lots"]
                xp = exit_prices.get(ts)
                # 卖方：平仓必收手续费；买方：仅在有可卖出价格时收
                if c["direction"] == "short":
                    fee_close_total += fee_per_lot * lots
                else:
                    if xp is not None and xp > 0:
                        fee_close_total += fee_per_lot * lots
            cum_pnl -= fee_close_total
            fee_total += fee_close_total
            trade_gross = sum(leg_returns)
            realized_pnl += trade_gross
            trade_ret = trade_gross - fee_open_total - fee_close_total
            delist_map = {r["ts_code"]: r["delist_date"] for _, r in df.iterrows()}

            def _fmt_contract(ts_code: str, direction: str) -> str:
                strike = strike_map.get(ts_code)
                cp = call_put_map.get(ts_code)
                expiry = delist_map.get(ts_code)
                if strike is None or cp is None:
                    prefix = "买" if direction == "long" else "卖"
                    return f"{prefix}{ts_code}"
                label = "认购" if cp == "C" else "认沽"
                prefix = "买" if direction == "long" else "卖"
                if expiry:
                    try:
                        exp_str = str(expiry)
                        year = exp_str[0:4]
                        month = exp_str[4:6]
                        return f"{prefix}{year}年{int(month)}月{strike:.2f}{label}"
                    except Exception:
                        return f"{prefix}{expiry}-{strike:.2f}{label}"
                return f"{prefix}{strike:.2f}{label}"

            # 记录卖方保证金（按入场日估算）
            margin_at_entry = 0.0
            S_entry = underlying_prices.get(entry_date)
            if strategy in {"bull_spread", "bear_spread"} and spread_margin_entry is not None:
                margin_at_entry = spread_margin_entry
            else:
                if S_entry is not None:
                    for c in current_contracts:
                        if c["direction"] != "short":
                            continue
                        K = strike_map.get(c["ts_code"])
                        ep = entry_prices.get(c["ts_code"])
                        if K is None or ep is None:
                            continue
                        cp = call_put_map.get(c["ts_code"])
                        premium = ep * multiplier * c["lots"]
                        margin_at_entry += _calc_short_margin(S_entry, K, cp, multiplier, margin_rate, premium, c["lots"])

            trades.append(
                {
                    "entry_date": entry_date,
                    "exit_date": date_str,
                    "contracts": ",".join([_fmt_contract(c["ts_code"], c["direction"]) for c in current_contracts]),
                    "gross_pnl": trade_gross,
                    "fees": fee_open_total + fee_close_total,
                    "net_pnl": trade_ret,
                    "margin": margin_at_entry,
                    "underlying_price": S_entry,
                }
            )
            if strategy in {"bull_spread", "bear_spread"}:
                long_leg = next((c for c in current_contracts if c["direction"] == "long"), None)
                short_leg = next((c for c in current_contracts if c["direction"] == "short"), None)
                if long_leg and short_leg:
                    long_ts = long_leg["ts_code"]
                    short_ts = short_leg["ts_code"]
                    long_ep = entry_prices.get(long_ts)
                    short_ep = entry_prices.get(short_ts)
                    k_long = strike_map.get(long_ts)
                    k_short = strike_map.get(short_ts)
                    spread_width = None
                    net_debit = None
                    max_loss = None
                    if k_long is not None and k_short is not None:
                        spread_width = abs(k_short - k_long) * multiplier * long_leg["lots"]
                    if long_ep is not None and short_ep is not None:
                        net_debit = (long_ep - short_ep) * multiplier * long_leg["lots"]
                        if spread_width is not None:
                            if net_debit >= 0:
                                max_loss = net_debit
                            else:
                                max_loss = spread_width + net_debit
                    trades[-1].update(
                        {
                            "long_entry": long_ep,
                            "short_entry": short_ep,
                            "net_debit": net_debit,
                            "spread_width": spread_width,
                            "max_loss": max_loss,
                        }
                    )

            # 当日换月开新仓（如果当日有可用新合约）
            current_expiry, current_contracts = pick_contracts(
                date_str, min_expiry=current_expiry, standard_only=True, reason="roll"
            )
            if current_contracts is None:
                current_expiry, current_contracts = pick_contracts(
                    date_str, min_expiry=current_expiry, standard_only=False, reason="roll"
                )
            if current_contracts is None:
                pnl_series.append((date_str, None))
                no_contract_dates.append(date_str)
                prev_date = date_str
                continue
            # 保护：避免同日换月又选回同一合约
            old_codes = set(exit_prices.keys())
            new_codes = {c["ts_code"] for c in current_contracts}
            if old_codes & new_codes:
                current_contracts = None
                pnl_series.append((date_str, cum_pnl))
                prev_date = date_str
                continue
            entry_date = date_str
            entry_prices = {c["ts_code"]: price_map.get((c["ts_code"], date_str)) for c in current_contracts}
            if any(v is None for v in entry_prices.values()):
                current_contracts = None
                pnl_series.append((date_str, None))
                no_contract_dates.append(date_str)
                prev_date = date_str
                continue
            spread_margin_entry = None
            if strategy in {"bull_spread", "bear_spread"}:
                net_debit = 0.0
                for c in current_contracts:
                    ep = entry_prices.get(c["ts_code"])
                    if ep is None:
                        net_debit = None
                        break
                    if c["direction"] == "long":
                        net_debit += ep * multiplier * c["lots"]
                    else:
                        net_debit -= ep * multiplier * c["lots"]
                if net_debit is not None:
                    spread_margin_entry = max(net_debit, 0.0)
                    S_entry = underlying_prices.get(entry_date)
                    if S_entry is None:
                        S_entry = last_S
                    floor = S_entry * multiplier * 0.08 * lots if S_entry is not None else 0.0
                    spread_margin_entry = max(spread_margin_entry, floor)
            if strike_mode == "MANUAL":
                locked_strikes = {c["ts_code"]: strike_map.get(c["ts_code"]) for c in current_contracts}
            leg_cum_pnl = {c["ts_code"]: 0.0 for c in current_contracts}
            fee_open_total = 0.0
            for c in current_contracts:
                fee_open_total += fee_per_lot * c["lots"]
            cum_pnl -= fee_open_total
            fee_total += fee_open_total
            for c in current_contracts:
                if c["direction"] == "long":
                    ep = entry_prices.get(c["ts_code"])
                    if ep is not None:
                        premium_paid_total += ep * multiplier * c["lots"]

        pnl_series.append((date_str, cum_pnl))
        prev_date = date_str

    equity = pd.DataFrame(pnl_series, columns=["date", "pnl"])
    pnl_values = equity["pnl"].dropna()
    total_return = float(pnl_values.iloc[-1]) if not pnl_values.empty else 0.0
    n_days = len(pnl_values)
    cal_days = 0
    try:
        cal_days = (datetime.strptime(end_date, "%Y%m%d") - datetime.strptime(start_date, "%Y%m%d")).days + 1
    except Exception:
        cal_days = n_days
    ann_return = float(total_return / cal_days * 365) if cal_days > 0 else 0.0
    max_dd = 0.0
    max_dd_pct = None
    if not pnl_values.empty:
        peak = pnl_values.cummax()
        dd = pnl_values - peak
        max_dd = float(dd.min())
        with np.errstate(divide="ignore", invalid="ignore"):
            dd_pct = dd / peak
        if not dd_pct.empty and np.isfinite(dd_pct).any():
            max_dd_pct = float(dd_pct.min())
    win_rate = float((pd.Series([t.get("net_pnl", 0.0) for t in trades]) > 0).mean()) if trades else 0.0
    avg_ret = float(pd.Series([t.get("net_pnl", 0.0) for t in trades]).mean()) if trades else 0.0
    avg_margin = float(np.mean(margin_series)) if margin_series else 0.0
    # 未平仓浮盈亏（按最后一个交易日估值）
    unrealized_pnl = 0.0
    open_positions = []
    if current_contracts and prev_date:
        for c in current_contracts:
            ts = c["ts_code"]
            lots = c["lots"]
            ep = entry_prices.get(ts)
            last_price = price_map.get((ts, prev_date))
            if ep is None:
                continue
            if last_price is None:
                last_price = 0.0
            if c["direction"] == "short":
                upnl = (ep - last_price) * multiplier * lots
            else:
                upnl = (last_price - ep) * multiplier * lots
            unrealized_pnl += upnl

            strike = strike_map.get(ts)
            cp = call_put_map.get(ts)
            label = ts
            if strike is not None and cp is not None:
                label = f"{'买' if c['direction']=='long' else '卖'}{strike:.2f}{'认购' if cp == 'C' else '认沽'}"
            # 持仓保证金（卖方按最新标的估算，买方为0）
            pos_margin = 0.0
            S_pos = underlying_prices.get(prev_date)
            if strategy in {"bull_spread", "bear_spread"} and spread_margin_entry is not None:
                pos_margin = spread_margin_entry
            else:
                if c["direction"] == "short":
                    if S_pos is not None:
                        K = strike_map.get(ts)
                        if K is not None:
                            premium = ep * multiplier * lots
                            pos_margin = _calc_short_margin(S_pos, K, cp, multiplier, margin_rate, premium, lots)

            open_positions.append(
                {
                    "ts_code": ts,
                    "name": label,
                    "entry_date": entry_date,
                    "entry_price": ep,
                    "last_price": last_price,
                    "unrealized_pnl": upnl,
                    "margin": pos_margin,
                    "underlying_price": S_pos,
                }
            )
    ann_return_pct = None
    if avg_margin > 0 and cal_days > 0:
        ann_return_pct = float((total_return / avg_margin) * 365 / cal_days)
    else:
        total_cost = premium_paid_total + fee_total
        if total_cost > 0 and cal_days > 0:
            ann_return_pct = float((total_return / total_cost) * 365 / cal_days)

    summary = {
        "symbol": underlying.replace(".SH", "").replace(".SZ", ""),
        "strategy": strategy,
        "start_date": equity["date"].iloc[0],
        "end_date": equity["date"].iloc[-1],
        "trades": len(trades),
        "total_pnl": realized_pnl + unrealized_pnl - fee_total,
        "annualized_pnl": ann_return,
        "max_drawdown": max_dd,
        "max_drawdown_pct": max_dd_pct,
        "win_rate": win_rate,
        "avg_return": avg_ret,
        "fee_per_lot": fee_per_lot,
        "avg_margin": avg_margin,
        "annualized_return_pct": ann_return_pct,
        "premium_paid_total": premium_paid_total,
        "fee_total": fee_total,
        "realized_pnl": realized_pnl,
        "unrealized_pnl": unrealized_pnl,
    }

    return {
        "summary": summary,
        "trades": pd.DataFrame(trades),
        "equity": equity,
        "open_positions": pd.DataFrame(open_positions),
        "missing_dates": missing_dates,
        "no_contract_dates": no_contract_dates,
        "calendar_diag": pd.DataFrame(calendar_diag_rows) if calendar_diag_rows else pd.DataFrame(),
        "pick_diag": pd.DataFrame(pick_diag_rows) if pick_diag_rows else pd.DataFrame(),
        "missing_detail": pd.DataFrame(missing_detail) if missing_detail else pd.DataFrame(),
    }
