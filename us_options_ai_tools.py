from __future__ import annotations

import math
import re
from typing import Any

import pandas as pd
from langchain_core.tools import tool
from pydantic import BaseModel, Field

from us_market_dashboard_data import (
    UNDERLYING_DISPLAY_NAMES,
    calculate_atm_iv_pct,
    calculate_overview_metrics_from_market_history,
    calculate_volatility_positioning_metrics,
    dashboard_engine,
    load_available_option_trade_dates,
    load_iv_history,
    load_market_metrics_history,
    load_oi_defense_history,
    load_option_chain_daily,
    load_option_chain_summary,
    load_stock_daily,
    normalize_trade_date,
    selected_underlying_price,
)


US_OPTION_PROFILE_MIN_HISTORY_DAYS = 60

_US_OPTION_ALIASES = {
    "标普500ETF": "SPY",
    "标普ETF": "SPY",
    "标普": "SPY",
    "纳指100ETF": "QQQ",
    "纳指ETF": "QQQ",
    "纳指": "QQQ",
    "罗素2000ETF": "IWM",
    "道指ETF": "DIA",
    "黄金ETF": "GLD",
    "白银ETF": "SLV",
    "美债ETF": "TLT",
    "20年美债ETF": "TLT",
    "金融板块ETF": "XLF",
    "能源板块ETF": "XLE",
    "高收益债ETF": "HYG",
    "特斯拉": "TSLA",
    "英伟达": "NVDA",
    "辉达": "NVDA",
    "超威半导体": "AMD",
    "苹果": "AAPL",
    "亚马逊": "AMZN",
}
_US_OPTION_ALIASES.update({name: symbol for symbol, name in UNDERLYING_DISPLAY_NAMES.items()})


class USOptionMarketProfileInput(BaseModel):
    underlying: str = Field(description="美股或美股ETF代码/名称，例如 SPY、QQQ、NVDA、TSLA、英伟达")
    trade_date: str = Field(default="", description="可选交易日，格式 YYYYMMDD 或 YYYY-MM-DD；留空时使用本地最新美股期权数据日")
    window: int = Field(default=252, description="历史观察窗口，默认252个交易日")


class USOptionStrategyCandidatesInput(BaseModel):
    underlying: str = Field(description="美股或美股ETF代码/名称，例如 AAPL、SPY、NVDA、TSLA")
    strategy: str = Field(default="", description="策略名称/别名，例如 卖put、卖call、双卖、铁鹰、bull_put_spread")
    trade_date: str = Field(default="", description="可选交易日，格式 YYYYMMDD 或 YYYY-MM-DD；留空使用本地最新美股期权数据日")
    dte_min: int = Field(default=30, description="候选合约最小DTE，默认30")
    dte_max: int = Field(default=45, description="候选合约最大DTE，默认45")
    max_candidates: int = Field(default=5, description="最多返回候选数量，默认5")
    risk_budget_pct: float = Field(default=5.0, description="单笔风险预算占总资金比例，仅用于展示风险口径")
    risk_preference: str = Field(default="", description="可选风险偏好，例如 稳健、激进")


_STRATEGY_ALIASES = {
    "": "seller_income",
    "卖方": "seller_income",
    "收租": "seller_income",
    "卖期权": "seller_income",
    "seller": "seller_income",
    "income": "seller_income",
    "cashsecuredput": "cash_secured_put",
    "cashsecuredputs": "cash_secured_put",
    "cash_secured_put": "cash_secured_put",
    "csp": "cash_secured_put",
    "担保卖沽": "cash_secured_put",
    "现金担保卖put": "cash_secured_put",
    "现金担保卖沽": "cash_secured_put",
    "coveredcall": "covered_call",
    "covered_call": "covered_call",
    "备兑": "covered_call",
    "备兑开仓": "covered_call",
    "shortput": "short_put",
    "short_put": "short_put",
    "sellput": "short_put",
    "卖put": "short_put",
    "卖沽": "short_put",
    "shortcall": "short_call",
    "short_call": "short_call",
    "sellcall": "short_call",
    "卖call": "short_call",
    "卖购": "short_call",
    "bullputspread": "bull_put_spread",
    "bull_put_spread": "bull_put_spread",
    "宽行权价牛市认沽价差": "bull_put_spread",
    "牛市认沽价差": "bull_put_spread",
    "认沽价差": "bull_put_spread",
    "bearcallspread": "bear_call_spread",
    "bear_call_spread": "bear_call_spread",
    "熊市认购价差": "bear_call_spread",
    "认购价差": "bear_call_spread",
    "shortstrangle": "short_strangle",
    "short_strangle": "short_strangle",
    "卖宽跨": "short_strangle",
    "双卖宽跨": "short_strangle",
    "shortstraddle": "short_straddle",
    "short_straddle": "short_straddle",
    "卖跨": "short_straddle",
    "双卖跨式": "short_straddle",
    "双卖": "short_strangle",
    "ironcondor": "iron_condor",
    "iron_condor": "iron_condor",
    "铁鹰": "iron_condor",
    "铁秃鹰": "iron_condor",
}

_STRATEGY_LABELS = {
    "seller_income": "卖方收租候选组合",
    "cash_secured_put": "现金担保卖认沽（Cash-Secured Put）",
    "covered_call": "备兑卖认购（Covered Call）",
    "short_put": "卖认沽（Short Put）",
    "short_call": "卖认购（Short Call）",
    "bull_put_spread": "牛市认沽信用价差（Bull Put Spread）",
    "bear_call_spread": "熊市认购信用价差（Bear Call Spread）",
    "short_strangle": "卖宽跨（Short Strangle）",
    "short_straddle": "卖跨式（Short Straddle）",
    "iron_condor": "铁鹰（Iron Condor）",
}


def _clean_float(value: Any) -> float | None:
    try:
        out = float(value)
    except Exception:
        return None
    if math.isnan(out) or math.isinf(out):
        return None
    return out


def _fmt_date(value: Any) -> str:
    text = normalize_trade_date(value)
    if len(text) != 8:
        return "-"
    return f"{text[:4]}-{text[4:6]}-{text[6:8]}"


def _fmt_number(value: Any, digits: int = 2, suffix: str = "") -> str:
    val = _clean_float(value)
    if val is None:
        return "暂无"
    return f"{val:,.{digits}f}{suffix}"


def _fmt_pct(value: Any, digits: int = 1) -> str:
    return _fmt_number(value, digits=digits, suffix="%")


def _fmt_signed_pct(value: Any, digits: int = 1) -> str:
    val = _clean_float(value)
    if val is None:
        return "暂无"
    sign = "+" if val > 0 else ""
    return f"{sign}{val:.{digits}f}%"


def normalize_us_option_strategy(strategy: str) -> tuple[str, str]:
    raw = str(strategy or "").strip()
    key = re.sub(r"[\s\-_]+", "", raw.lower())
    if raw in _STRATEGY_ALIASES:
        canonical = _STRATEGY_ALIASES[raw]
    else:
        canonical = _STRATEGY_ALIASES.get(key, "")
    if not canonical:
        if "铁鹰" in raw or "铁秃鹰" in raw:
            canonical = "iron_condor"
        elif "宽跨" in raw:
            canonical = "short_strangle"
        elif "跨" in raw and "卖" in raw:
            canonical = "short_straddle"
        elif "卖" in raw and ("put" in raw.lower() or "沽" in raw or "认沽" in raw):
            canonical = "short_put"
        elif "卖" in raw and ("call" in raw.lower() or "购" in raw or "认购" in raw):
            canonical = "short_call"
        else:
            canonical = "seller_income"
    return canonical, _STRATEGY_LABELS.get(canonical, canonical)


def _normal_cdf(value: float) -> float:
    return 0.5 * (1.0 + math.erf(float(value) / math.sqrt(2.0)))


def _estimate_option_delta(
    *,
    underlying_price: Any,
    strike: Any,
    dte: Any,
    iv: Any,
    call_put: Any,
    risk_free_rate: float = 0.045,
) -> float | None:
    spot = _clean_float(underlying_price)
    strike_val = _clean_float(strike)
    dte_val = _clean_float(dte)
    iv_val = _clean_float(iv)
    if spot is None or strike_val is None or dte_val is None or iv_val is None:
        return None
    if spot <= 0 or strike_val <= 0 or dte_val <= 0 or iv_val <= 0:
        return None
    try:
        t = float(dte_val) / 365.0
        d1 = (math.log(float(spot) / float(strike_val)) + (risk_free_rate + 0.5 * iv_val * iv_val) * t) / (
            iv_val * math.sqrt(t)
        )
    except Exception:
        return None
    side = str(call_put or "").strip().upper()
    if side == "P":
        return _normal_cdf(d1) - 1.0
    if side == "C":
        return _normal_cdf(d1)
    return None


def _row_iv_decimal(row: pd.Series) -> float | None:
    for key in ("iv", "provider_iv", "computed_iv"):
        value = _clean_float(row.get(key))
        if value is not None and value > 0:
            return value / 100.0 if value > 3 else value
    iv_pct = _clean_float(row.get("iv_pct"))
    if iv_pct is not None and iv_pct > 0:
        return iv_pct / 100.0
    return None


def _prepare_candidate_chain(chain_df: pd.DataFrame, underlying_price: float | None, dte_min: int, dte_max: int) -> pd.DataFrame:
    if chain_df is None or chain_df.empty:
        return pd.DataFrame()
    df = chain_df.copy()
    for col in ("strike", "close", "volume", "open_interest", "iv_pct", "iv", "dte", "moneyness_pct", "underlying_price"):
        if col not in df.columns:
            df[col] = None
        df[col] = pd.to_numeric(df[col], errors="coerce")
    for col in ("call_put", "expiration_date", "option_ticker", "expiration_type"):
        if col not in df.columns:
            df[col] = ""

    price = _clean_float(underlying_price)
    if price is None:
        valid_prices = pd.to_numeric(df["underlying_price"], errors="coerce").dropna()
        if not valid_prices.empty:
            price = float(valid_prices.iloc[-1])
    if price is not None:
        df["underlying_price"] = df["underlying_price"].fillna(float(price))
        missing_moneyness = df["moneyness_pct"].isna()
        if missing_moneyness.any():
            df.loc[missing_moneyness, "moneyness_pct"] = (
                (df.loc[missing_moneyness, "strike"] - float(price)) / float(price) * 100.0
            )

    df["call_put"] = df["call_put"].astype(str).str.upper()
    df["iv_decimal"] = df.apply(_row_iv_decimal, axis=1)
    df["delta_est"] = df.apply(
        lambda row: _estimate_option_delta(
            underlying_price=row.get("underlying_price"),
            strike=row.get("strike"),
            dte=row.get("dte"),
            iv=row.get("iv_decimal"),
            call_put=row.get("call_put"),
        ),
        axis=1,
    )
    df["abs_delta"] = pd.to_numeric(df["delta_est"], errors="coerce").abs()
    df["liquidity_score"] = df["volume"].fillna(0).clip(lower=0).apply(math.log1p) + df["open_interest"].fillna(0).clip(lower=0).apply(math.log1p)
    df = df[
        (df["dte"].between(int(dte_min), int(dte_max)))
        & (df["strike"] > 0)
        & (df["close"] > 0)
        & df["call_put"].isin(["C", "P"])
    ].copy()
    return df.sort_values(["expiration_date", "strike", "call_put", "option_ticker"]).reset_index(drop=True)


def _leg_from_row(row: pd.Series, action: str) -> dict[str, Any]:
    delta_val = _clean_float(row.get("delta_est"))
    iv_pct = _clean_float(row.get("iv_pct"))
    if iv_pct is None:
        iv_dec = _clean_float(row.get("iv_decimal"))
        iv_pct = iv_dec * 100.0 if iv_dec is not None else None
    return {
        "action": action,
        "option_ticker": str(row.get("option_ticker") or ""),
        "call_put": str(row.get("call_put") or ""),
        "strike": _clean_float(row.get("strike")),
        "expiration_date": str(row.get("expiration_date") or ""),
        "dte": int(_clean_float(row.get("dte")) or 0),
        "eod_close": _clean_float(row.get("close")),
        "volume": int(_clean_float(row.get("volume")) or 0),
        "open_interest": int(_clean_float(row.get("open_interest")) or 0),
        "iv_pct": iv_pct,
        "delta_est": delta_val,
        "moneyness_pct": _clean_float(row.get("moneyness_pct")),
    }


def _candidate_liquidity(legs: list[dict[str, Any]]) -> dict[str, int]:
    volumes = [int(leg.get("volume") or 0) for leg in legs]
    oi_values = [int(leg.get("open_interest") or 0) for leg in legs]
    return {
        "min_volume": min(volumes) if volumes else 0,
        "min_open_interest": min(oi_values) if oi_values else 0,
        "total_volume": sum(volumes),
        "total_open_interest": sum(oi_values),
    }


def _option_side_rows(df: pd.DataFrame, side: str) -> pd.DataFrame:
    return df[df["call_put"] == side].copy()


def _sort_single_rows(rows: pd.DataFrame, target_abs_delta: float) -> pd.DataFrame:
    scoped = rows.copy()
    if scoped.empty:
        return scoped
    scoped["delta_distance"] = (scoped["abs_delta"] - float(target_abs_delta)).abs()
    scoped["delta_distance"] = scoped["delta_distance"].fillna((scoped["moneyness_pct"].abs() - 5.0).abs() / 100.0)
    return scoped.sort_values(["delta_distance", "dte", "liquidity_score"], ascending=[True, True, False])


def _single_leg_candidates(
    df: pd.DataFrame,
    strategy: str,
    underlying_price: float | None,
    max_candidates: int,
) -> list[dict[str, Any]]:
    side = "P" if strategy in {"cash_secured_put", "short_put"} else "C"
    rows = _option_side_rows(df, side)
    if rows.empty:
        return []
    if side == "P":
        preferred = rows[rows["abs_delta"].between(0.20, 0.35)]
        if preferred.empty:
            preferred = rows[(rows["moneyness_pct"] <= -1.0) & (rows["moneyness_pct"] >= -20.0)]
        target = 0.30
        action = "SELL_PUT"
    else:
        preferred = rows[rows["abs_delta"].between(0.20, 0.35)]
        if preferred.empty:
            preferred = rows[(rows["moneyness_pct"] >= 1.0) & (rows["moneyness_pct"] <= 20.0)]
        target = 0.30
        action = "SELL_CALL"
    if preferred.empty:
        preferred = rows
    selected = _sort_single_rows(preferred, target).head(max_candidates)
    out: list[dict[str, Any]] = []
    for _, row in selected.iterrows():
        leg = _leg_from_row(row, action)
        strike = float(leg.get("strike") or 0)
        premium = float(leg.get("eod_close") or 0)
        metrics: dict[str, Any] = {
            "net_credit": premium,
            "premium_cash": premium * 100.0,
            "liquidity": _candidate_liquidity([leg]),
        }
        if side == "P":
            metrics["breakeven"] = strike - premium
            metrics["cash_secured_notional"] = strike * 100.0
            metrics["max_loss_cash_secured"] = max((strike - premium) * 100.0, 0.0)
        else:
            metrics["breakeven"] = strike + premium
            metrics["covered_stock_notional"] = (float(underlying_price) * 100.0) if underlying_price else None
            metrics["max_loss_note"] = "裸卖认购理论风险无上限；若持有正股则按备兑逻辑管理。"
        out.append({"strategy": strategy, "legs": [leg], "metrics": metrics})
    return out


def _credit_spread_candidates(df: pd.DataFrame, strategy: str, max_candidates: int) -> list[dict[str, Any]]:
    side = "P" if strategy == "bull_put_spread" else "C"
    rows = _option_side_rows(df, side)
    if rows.empty:
        return []
    out: list[dict[str, Any]] = []
    for expiration, group in rows.groupby("expiration_date", dropna=False):
        group = group.sort_values("strike")
        for _, short_row in group.iterrows():
            short_delta = _clean_float(short_row.get("abs_delta"))
            if short_delta is not None and not (0.20 <= short_delta <= 0.45):
                continue
            if side == "P":
                long_rows = group[group["strike"] < float(short_row["strike"])]
                action_short, action_long = "SELL_PUT", "BUY_PUT"
            else:
                long_rows = group[group["strike"] > float(short_row["strike"])]
                action_short, action_long = "SELL_CALL", "BUY_CALL"
            for _, long_row in long_rows.iterrows():
                width = abs(float(short_row["strike"]) - float(long_row["strike"]))
                if width <= 0 or width < 2.5 or width > 25:
                    continue
                credit = float(short_row["close"]) - float(long_row["close"])
                if credit <= 0:
                    continue
                short_leg = _leg_from_row(short_row, action_short)
                long_leg = _leg_from_row(long_row, action_long)
                breakeven = float(short_row["strike"]) - credit if side == "P" else float(short_row["strike"]) + credit
                max_loss = max(width - credit, 0.0) * 100.0
                out.append(
                    {
                        "strategy": strategy,
                        "legs": [short_leg, long_leg],
                        "metrics": {
                            "expiration_date": str(expiration or ""),
                            "width": width,
                            "net_credit": credit,
                            "premium_cash": credit * 100.0,
                            "max_loss": max_loss,
                            "breakeven": breakeven,
                            "reward_risk": (credit / max(width - credit, 0.01)),
                            "liquidity": _candidate_liquidity([short_leg, long_leg]),
                        },
                    }
                )
    return _sort_candidates(out)[:max_candidates]


def _short_strangle_candidates(df: pd.DataFrame, max_candidates: int) -> list[dict[str, Any]]:
    puts = _option_side_rows(df, "P")
    calls = _option_side_rows(df, "C")
    out: list[dict[str, Any]] = []
    for expiration in sorted(set(puts["expiration_date"]).intersection(set(calls["expiration_date"]))):
        put_group = puts[puts["expiration_date"] == expiration]
        call_group = calls[calls["expiration_date"] == expiration]
        put_candidates = put_group[put_group["abs_delta"].between(0.08, 0.25)]
        call_candidates = call_group[call_group["abs_delta"].between(0.08, 0.25)]
        if put_candidates.empty:
            put_candidates = put_group[put_group["moneyness_pct"].between(-20.0, -3.0)]
        if call_candidates.empty:
            call_candidates = call_group[call_group["moneyness_pct"].between(3.0, 20.0)]
        put_candidates = _sort_single_rows(put_candidates if not put_candidates.empty else put_group, 0.16).head(4)
        call_candidates = _sort_single_rows(call_candidates if not call_candidates.empty else call_group, 0.16).head(4)
        for _, put_row in put_candidates.iterrows():
            for _, call_row in call_candidates.iterrows():
                if float(put_row["strike"]) >= float(call_row["strike"]):
                    continue
                put_leg = _leg_from_row(put_row, "SELL_PUT")
                call_leg = _leg_from_row(call_row, "SELL_CALL")
                credit = float(put_row["close"]) + float(call_row["close"])
                out.append(
                    {
                        "strategy": "short_strangle",
                        "legs": [put_leg, call_leg],
                        "metrics": {
                            "expiration_date": str(expiration or ""),
                            "net_credit": credit,
                            "premium_cash": credit * 100.0,
                            "lower_breakeven": float(put_row["strike"]) - credit,
                            "upper_breakeven": float(call_row["strike"]) + credit,
                            "max_loss_note": "上方理论风险无上限；下方风险接近标的跌至零。",
                            "liquidity": _candidate_liquidity([put_leg, call_leg]),
                        },
                    }
                )
    return _sort_candidates(out)[:max_candidates]


def _short_straddle_candidates(df: pd.DataFrame, underlying_price: float | None, max_candidates: int) -> list[dict[str, Any]]:
    if underlying_price is None:
        return []
    out: list[dict[str, Any]] = []
    for expiration, group in df.groupby("expiration_date", dropna=False):
        calls = group[group["call_put"] == "C"]
        puts = group[group["call_put"] == "P"]
        common = sorted(set(calls["strike"]).intersection(set(puts["strike"])))
        for strike in common:
            call_row = calls[calls["strike"] == strike].iloc[0]
            put_row = puts[puts["strike"] == strike].iloc[0]
            call_leg = _leg_from_row(call_row, "SELL_CALL")
            put_leg = _leg_from_row(put_row, "SELL_PUT")
            credit = float(call_row["close"]) + float(put_row["close"])
            out.append(
                {
                    "strategy": "short_straddle",
                    "legs": [put_leg, call_leg],
                    "metrics": {
                        "expiration_date": str(expiration or ""),
                        "net_credit": credit,
                        "premium_cash": credit * 100.0,
                        "lower_breakeven": float(strike) - credit,
                        "upper_breakeven": float(strike) + credit,
                        "distance_to_atm": abs(float(strike) - float(underlying_price)),
                        "max_loss_note": "上方理论风险无上限；下方风险接近标的跌至零。",
                        "liquidity": _candidate_liquidity([put_leg, call_leg]),
                    },
                }
            )
    out.sort(key=lambda item: (float(item["metrics"].get("distance_to_atm") or 999999), -_liquidity_rank(item)))
    return out[:max_candidates]


def _iron_condor_candidates(df: pd.DataFrame, max_candidates: int) -> list[dict[str, Any]]:
    puts = _option_side_rows(df, "P")
    calls = _option_side_rows(df, "C")
    out: list[dict[str, Any]] = []
    expirations = sorted(set(puts["expiration_date"]).intersection(set(calls["expiration_date"])))
    for expiration in expirations:
        put_group = puts[puts["expiration_date"] == expiration].sort_values("strike")
        call_group = calls[calls["expiration_date"] == expiration].sort_values("strike")
        short_puts = _sort_single_rows(put_group[put_group["abs_delta"].between(0.12, 0.30)] if not put_group.empty else put_group, 0.20).head(4)
        short_calls = _sort_single_rows(call_group[call_group["abs_delta"].between(0.12, 0.30)] if not call_group.empty else call_group, 0.20).head(4)
        if short_puts.empty:
            short_puts = _sort_single_rows(put_group, 0.20).head(4)
        if short_calls.empty:
            short_calls = _sort_single_rows(call_group, 0.20).head(4)
        for _, sp in short_puts.iterrows():
            lp_rows = put_group[put_group["strike"] < float(sp["strike"])]
            for _, sc in short_calls.iterrows():
                lc_rows = call_group[call_group["strike"] > float(sc["strike"])]
                for _, lp in lp_rows.tail(3).iterrows():
                    put_width = float(sp["strike"]) - float(lp["strike"])
                    if put_width < 2.5 or put_width > 25:
                        continue
                    for _, lc in lc_rows.head(3).iterrows():
                        call_width = float(lc["strike"]) - float(sc["strike"])
                        if call_width < 2.5 or call_width > 25:
                            continue
                        credit = float(sp["close"]) - float(lp["close"]) + float(sc["close"]) - float(lc["close"])
                        if credit <= 0:
                            continue
                        legs = [
                            _leg_from_row(sp, "SELL_PUT"),
                            _leg_from_row(lp, "BUY_PUT"),
                            _leg_from_row(sc, "SELL_CALL"),
                            _leg_from_row(lc, "BUY_CALL"),
                        ]
                        max_width = max(put_width, call_width)
                        out.append(
                            {
                                "strategy": "iron_condor",
                                "legs": legs,
                                "metrics": {
                                    "expiration_date": str(expiration or ""),
                                    "net_credit": credit,
                                    "premium_cash": credit * 100.0,
                                    "put_width": put_width,
                                    "call_width": call_width,
                                    "max_loss": max(max_width - credit, 0.0) * 100.0,
                                    "lower_breakeven": float(sp["strike"]) - credit,
                                    "upper_breakeven": float(sc["strike"]) + credit,
                                    "liquidity": _candidate_liquidity(legs),
                                },
                            }
                        )
    return _sort_candidates(out)[:max_candidates]


def _liquidity_rank(candidate: dict[str, Any]) -> float:
    liq = (candidate.get("metrics") or {}).get("liquidity") or {}
    return float(liq.get("min_volume") or 0) + float(liq.get("min_open_interest") or 0) / 10.0


def _sort_candidates(candidates: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return sorted(
        candidates,
        key=lambda item: (
            -_liquidity_rank(item),
            -float((item.get("metrics") or {}).get("net_credit") or 0),
            float((item.get("metrics") or {}).get("max_loss") or 0),
        ),
    )


def normalize_us_option_underlying(underlying: str) -> tuple[str, str]:
    raw = str(underlying or "").strip()
    if not raw:
        return "", "请提供美股或美股ETF代码，例如 SPY、QQQ、NVDA、TSLA。"

    if raw in _US_OPTION_ALIASES:
        return _US_OPTION_ALIASES[raw], ""

    upper = raw.upper().strip()
    upper = upper.replace("NASDAQ:", "").replace("NYSE:", "").replace("AMEX:", "")
    upper = upper.split(".", 1)[0] if upper.endswith((".US", ".NASDAQ", ".NYSE")) else upper
    upper = re.sub(r"[^A-Z0-9]", "", upper)

    if re.fullmatch(r"\d{6}", upper) or raw.endswith((".SH", ".SZ", ".SS")):
        return "", "本工具仅支持美股/美股ETF期权，不用于A股ETF、商品或股指期权。"
    if re.search(r"[\u4e00-\u9fff]", raw):
        return "", "未识别为受支持的美股/美股ETF标的；中文名请使用如“英伟达”“特斯拉”“标普500ETF”。"
    if not re.fullmatch(r"[A-Z]{1,5}", upper):
        return "", "未识别为有效美股代码。"
    return upper, ""


def _resolve_trade_date(
    underlying: str,
    requested_trade_date: str,
    *,
    use_test_tables: bool,
    engine,
) -> tuple[str, str, list[str]]:
    available_dates = load_available_option_trade_dates(
        underlying,
        use_test_tables=use_test_tables,
        limit=520,
        engine=engine,
    )
    available_dates = sorted({normalize_trade_date(value) for value in available_dates if normalize_trade_date(value)})
    requested = normalize_trade_date(requested_trade_date)
    if not available_dates:
        return "", "", []
    if not requested:
        return available_dates[-1], "", available_dates
    if requested in available_dates:
        return requested, "", available_dates
    earlier = [value for value in available_dates if value <= requested]
    if earlier:
        selected = earlier[-1]
        return selected, f"请求日期 {_fmt_date(requested)} 无期权数据，已回退到 {_fmt_date(selected)}。", available_dates
    selected = available_dates[-1]
    return selected, f"请求日期 {_fmt_date(requested)} 早于本地样本，已使用最新数据日 {_fmt_date(selected)}。", available_dates


def _latest_oi_defense_row(defense_history: pd.DataFrame) -> dict[str, Any]:
    if defense_history is None or defense_history.empty:
        return {}
    return defense_history.sort_values("trade_date").iloc[-1].to_dict()


def _build_profile_summary(metrics: dict[str, Any]) -> str:
    pieces: list[str] = []
    iv_rank = _clean_float(metrics.get("iv_rank"))
    if iv_rank is None:
        pieces.append("IV分位数据不足")
    elif iv_rank >= 70:
        pieces.append("IV处在偏高区")
    elif iv_rank <= 20:
        pieces.append("IV处在偏低区")
    else:
        pieces.append("IV处在中性区")

    term_state = str(metrics.get("term_state") or "").strip()
    if term_state == "Backwardation":
        pieces.append("期限结构偏倒挂，近端事件/避险压力更明显")
    elif term_state == "Contango":
        pieces.append("期限结构偏Contango，短端压力相对温和")
    elif term_state:
        pieces.append(f"期限结构{term_state}")

    put_call_oi = _clean_float(metrics.get("put_call_oi"))
    if put_call_oi is not None:
        if put_call_oi >= 1.2:
            pieces.append("Put OI占优，保护需求较强")
        elif put_call_oi <= 0.8:
            pieces.append("Call OI相对占优，上方博弈更活跃")

    zero_dte_share = _clean_float(metrics.get("zero_dte_volume_share_pct"))
    if zero_dte_share is not None and zero_dte_share >= 30:
        pieces.append("0DTE成交占比较高")

    return "；".join(pieces[:4]) if pieces else "数据不足，暂不做方向解读"


def _coverage_line(summary: dict[str, Any], metrics: dict[str, Any], source: str) -> str:
    return (
        f"链：{int(summary.get('rows') or 0)}行，"
        f"月度{int(summary.get('monthly') or 0)} / 短周期{int(summary.get('short_cycle') or 0)}，"
        f"IV行 provider {int(summary.get('provider_iv_rows') or 0)} + computed {int(summary.get('computed_iv_rows') or 0)}，"
        f"OI行{int(summary.get('open_interest_rows') or 0)}，"
        f"历史IV样本{int(metrics.get('iv_history_days') or 0)}天，"
        f"来源：{source}"
    )


def _render_profile_report(data: dict[str, Any]) -> str:
    metrics = data.get("metrics") or {}
    summary = data.get("chain_summary") or {}
    oi = data.get("oi_defense") or {}
    gaps = [str(item) for item in data.get("gaps") or [] if str(item).strip()]

    lines = [
        f"【美股期权体检】{data.get('underlying')} | 数据日：{_fmt_date(data.get('trade_date'))}（美股日线EOD）",
        f"结论：{_build_profile_summary(metrics)}。",
        (
            f"- 标的收盘价：{_fmt_number(data.get('underlying_price'), 2)}；"
            f"ATM IV：{_fmt_pct(metrics.get('atm_iv_pct'), 1)}；"
            f"IV Rank：{_fmt_pct(metrics.get('iv_rank'), 1)}；"
            f"IV Percentile：{_fmt_pct(metrics.get('iv_percentile'), 1)}"
        ),
        (
            f"- RV/溢价：RV20 {_fmt_pct(metrics.get('rv20_pct'), 1)}，"
            f"RV60 {_fmt_pct(metrics.get('rv60_pct'), 1)}，"
            f"IV-RV20 spread {_fmt_signed_pct(metrics.get('iv_rv20_spread'), 1)}，"
            f"1日IV变化 {_fmt_signed_pct(metrics.get('iv_change_1d'), 1)}"
        ),
        (
            f"- 期限结构：30D IV {_fmt_pct(metrics.get('iv_30d'), 1)}，"
            f"60D IV {_fmt_pct(metrics.get('iv_60d'), 1)}，"
            f"30/60D斜率 {_fmt_signed_pct(metrics.get('term_slope_30_60'), 1)}，"
            f"状态 {metrics.get('term_state') or '暂无'}"
        ),
        (
            f"- Skew/情绪：Put Skew {_fmt_signed_pct(metrics.get('put_skew_5pct'), 1)}，"
            f"Call Skew {_fmt_signed_pct(metrics.get('call_skew_5pct'), 1)}，"
            f"Put/Call OI {_fmt_number(metrics.get('put_call_oi'), 2)}，"
            f"Put/Call Volume {_fmt_number(metrics.get('put_call_volume'), 2)}，"
            f"0DTE成交占比 {_fmt_pct(metrics.get('zero_dte_volume_share_pct'), 1)}"
        ),
        (
            f"- OI防线：Call墙 {_fmt_number(oi.get('call_strike'), 2)}"
            f"（OI {_fmt_number(oi.get('call_oi'), 0)}，距现价 {_fmt_signed_pct(oi.get('call_distance_pct'), 1)}）；"
            f"Put墙 {_fmt_number(oi.get('put_strike'), 2)}"
            f"（OI {_fmt_number(oi.get('put_oi'), 0)}，距现价 {_fmt_signed_pct(oi.get('put_distance_pct'), 1)}）"
        ),
        f"- 数据覆盖：{_coverage_line(summary, metrics, data.get('source') or 'unknown')}",
    ]
    if data.get("date_note"):
        lines.append(f"- 日期说明：{data['date_note']}")
    lines.append(f"- 数据缺口：{'；'.join(gaps) if gaps else '未发现关键缺口'}")
    lines.append("提示：本工具仅基于本地美股期权EOD数据做体检，不提供实时成交价或直接买卖指令。")
    return "\n".join(lines)


def build_us_option_market_profile(
    underlying: str,
    trade_date: str = "",
    window: int = 252,
    *,
    use_test_tables: bool = False,
    engine=None,
    use_dashboard_engine: bool = True,
) -> dict[str, Any]:
    symbol, reason = normalize_us_option_underlying(underlying)
    if not symbol:
        return {
            "status": "unsupported",
            "underlying": "",
            "trade_date": "",
            "gaps": [reason],
            "report": f"【美股期权体检】数据不足\n- 原因：{reason}",
        }

    engine = engine if engine is not None else (dashboard_engine() if use_dashboard_engine else None)
    if engine is None:
        reason = "数据库未连接，无法读取本地美股期权数据。"
        return {
            "status": "no_data",
            "underlying": symbol,
            "trade_date": "",
            "gaps": [reason],
            "report": f"【美股期权体检】{symbol}\n结论：数据不足\n- 原因：{reason}",
        }

    try:
        window = min(max(int(window or 252), 20), 1500)
    except Exception:
        window = 252

    selected_date, date_note, available_dates = _resolve_trade_date(
        symbol,
        trade_date,
        use_test_tables=use_test_tables,
        engine=engine,
    )
    if not selected_date:
        reason = "未找到该标的的本地美股期权交易日数据。"
        return {
            "status": "no_data",
            "underlying": symbol,
            "trade_date": "",
            "available_dates": available_dates,
            "gaps": [reason],
            "report": f"【美股期权体检】{symbol}\n结论：数据不足\n- 原因：{reason}",
        }

    stock_df = load_stock_daily(symbol, limit=max(window + 80, 420), engine=engine)
    underlying_price = selected_underlying_price(stock_df, selected_date)
    chain_summary = load_option_chain_summary(
        symbol,
        selected_date,
        include_short_cycle=True,
        use_test_tables=use_test_tables,
        engine=engine,
    )

    source = "us_option_market_metrics_daily"
    market_metrics_history = load_market_metrics_history(
        symbol,
        window=window,
        use_test_tables=use_test_tables,
        engine=engine,
    )
    if market_metrics_history is not None and not market_metrics_history.empty:
        metrics = calculate_overview_metrics_from_market_history(
            stock_df=stock_df,
            market_metrics_history=market_metrics_history,
            trade_date=selected_date,
        )
    else:
        source = "derived_option_chain"
        chain_df = load_option_chain_daily(
            symbol,
            selected_date,
            include_short_cycle=True,
            use_test_tables=use_test_tables,
            underlying_price=underlying_price,
            engine=engine,
        )
        iv_history = load_iv_history(symbol, window=window, use_test_tables=use_test_tables, engine=engine)
        current_iv_pct = calculate_atm_iv_pct(
            chain_df,
            underlying_price=underlying_price,
        )
        metrics = calculate_volatility_positioning_metrics(
            stock_df=stock_df,
            chain_df=chain_df,
            iv_history=iv_history,
            trade_date=selected_date,
            current_iv_pct=current_iv_pct,
            iv_rank=None,
            market_metrics_history=None,
        )

    defense_history = load_oi_defense_history(
        symbol,
        selected_date,
        window=20,
        use_test_tables=use_test_tables,
        engine=engine,
    )
    oi_defense = _latest_oi_defense_row(defense_history)

    gaps: list[str] = []
    if date_note:
        gaps.append(date_note)
    if stock_df is None or stock_df.empty or underlying_price is None:
        gaps.append("标的收盘价缺失，RV和距离现价指标可能不可用")
    if int(chain_summary.get("rows") or 0) <= 0:
        gaps.append("期权链日线缺失")
    if int(chain_summary.get("provider_iv_rows") or 0) + int(chain_summary.get("computed_iv_rows") or 0) <= 0:
        gaps.append("IV数据不足")
    if int(chain_summary.get("open_interest_rows") or 0) <= 0:
        gaps.append("OI数据不足")
    history_days = int(metrics.get("iv_history_days") or 0)
    if history_days < US_OPTION_PROFILE_MIN_HISTORY_DAYS:
        gaps.append(f"历史IV样本不足（{history_days}/{US_OPTION_PROFILE_MIN_HISTORY_DAYS}）")
    if not oi_defense:
        gaps.append("OI防线历史不足")

    data = {
        "status": "ok" if int(chain_summary.get("rows") or 0) > 0 else "partial",
        "underlying": symbol,
        "trade_date": selected_date,
        "date_note": date_note,
        "available_dates": available_dates,
        "underlying_price": underlying_price,
        "metrics": metrics,
        "chain_summary": chain_summary,
        "oi_defense": oi_defense,
        "source": source,
        "gaps": gaps,
    }
    data["report"] = _render_profile_report(data)
    return data


def _strategy_candidates_for_chain(
    chain: pd.DataFrame,
    strategy: str,
    underlying_price: float | None,
    max_candidates: int,
) -> list[dict[str, Any]]:
    if strategy in {"cash_secured_put", "short_put", "covered_call", "short_call"}:
        return _single_leg_candidates(chain, strategy, underlying_price, max_candidates)
    if strategy in {"bull_put_spread", "bear_call_spread"}:
        return _credit_spread_candidates(chain, strategy, max_candidates)
    if strategy == "short_strangle":
        return _short_strangle_candidates(chain, max_candidates)
    if strategy == "short_straddle":
        return _short_straddle_candidates(chain, underlying_price, max_candidates)
    if strategy == "iron_condor":
        return _iron_condor_candidates(chain, max_candidates)
    if strategy == "seller_income":
        out: list[dict[str, Any]] = []
        for child in ("cash_secured_put", "bull_put_spread", "iron_condor", "short_strangle"):
            out.extend(_strategy_candidates_for_chain(chain, child, underlying_price, max(1, min(max_candidates, 2))))
        return _sort_candidates(out)[:max_candidates]
    return []


def _candidate_strategy_summary(strategy: str, metrics: dict[str, Any], risk_preference: str) -> str:
    iv_rank = _clean_float(metrics.get("iv_rank"))
    iv_percentile = _clean_float(metrics.get("iv_percentile"))
    risk_text = str(risk_preference or "").strip()
    high_risk = any(word in risk_text for word in ("激进", "高", "进取", "aggressive"))
    low_risk = any(word in risk_text for word in ("保守", "稳健", "低", "conservative"))

    pieces: list[str] = []
    if iv_rank is not None:
        if iv_rank >= 60:
            pieces.append(f"IV Rank {_fmt_pct(iv_rank, 1)}，权利金相对更厚，卖方策略可作为正式候选")
        elif iv_rank <= 30:
            pieces.append(f"IV Rank {_fmt_pct(iv_rank, 1)} 偏低，卖方收租性价比一般，候选需降级看待")
        else:
            pieces.append(f"IV Rank {_fmt_pct(iv_rank, 1)} 中性，卖方策略更依赖方向和风控")
    elif iv_percentile is not None:
        pieces.append(f"IV Percentile {_fmt_pct(iv_percentile, 1)}，缺少IV Rank时以分位辅助判断")
    else:
        pieces.append("IV Rank缺失，卖方策略仅按合约链候选展示")

    if strategy in {"short_call", "short_strangle", "short_straddle"}:
        if high_risk:
            pieces.append("用户风险偏好若为激进，可讨论裸卖/双卖，但必须设置止损、移仓和保证金预案")
        else:
            pieces.append("裸卖/双卖风险较高，默认同时参考有限风险替代结构")
    elif low_risk:
        pieces.append("稳健偏好下优先看现金担保、备兑或有限风险信用价差")
    return "；".join(pieces)


def _fmt_leg(leg: dict[str, Any]) -> str:
    side = "认购" if leg.get("call_put") == "C" else "认沽"
    return (
        f"{leg.get('action')} {side} { _fmt_number(leg.get('strike'), 2) } "
        f"{leg.get('expiration_date')} DTE {leg.get('dte')} | "
        f"EOD权利金 {_fmt_number(leg.get('eod_close'), 2)} | "
        f"Delta {_fmt_number(leg.get('delta_est'), 2)} | "
        f"IV {_fmt_pct(leg.get('iv_pct'), 1)} | "
        f"Vol {leg.get('volume', 0)} / OI {leg.get('open_interest', 0)}"
    )


def _render_candidate_metrics(candidate: dict[str, Any]) -> str:
    metrics = candidate.get("metrics") or {}
    pieces = []
    if metrics.get("net_credit") is not None:
        pieces.append(f"净收权利金 {_fmt_number(metrics.get('net_credit'), 2)}")
    if metrics.get("width") is not None:
        pieces.append(f"价差宽度 {_fmt_number(metrics.get('width'), 2)}")
    if metrics.get("max_loss") is not None:
        pieces.append(f"最大亏损/组 {_fmt_number(metrics.get('max_loss'), 0)} USD")
    if metrics.get("breakeven") is not None:
        pieces.append(f"盈亏平衡 {_fmt_number(metrics.get('breakeven'), 2)}")
    if metrics.get("lower_breakeven") is not None and metrics.get("upper_breakeven") is not None:
        pieces.append(
            f"盈亏平衡区间 {_fmt_number(metrics.get('lower_breakeven'), 2)} ~ {_fmt_number(metrics.get('upper_breakeven'), 2)}"
        )
    if metrics.get("cash_secured_notional") is not None:
        pieces.append(f"现金担保名义资金 {_fmt_number(metrics.get('cash_secured_notional'), 0)} USD/张")
    if metrics.get("covered_stock_notional") is not None:
        pieces.append(f"备兑股票名义资金 {_fmt_number(metrics.get('covered_stock_notional'), 0)} USD/100股")
    if metrics.get("reward_risk") is not None:
        pieces.append(f"收益/风险比 {_fmt_number(metrics.get('reward_risk'), 2)}")
    if metrics.get("max_loss_note"):
        pieces.append(str(metrics.get("max_loss_note")))
    return "；".join(pieces) if pieces else "暂无风险指标"


def _render_strategy_candidates_report(data: dict[str, Any]) -> str:
    candidates = data.get("candidates") or []
    gaps = [str(item) for item in data.get("gaps") or [] if str(item).strip()]
    lines = [
        (
            f"【美股期权策略候选】{data.get('underlying')} | "
            f"数据日：{_fmt_date(data.get('trade_date'))}（EOD） | "
            f"策略：{data.get('strategy_label')}"
        ),
        f"策略适配：{data.get('suitability') or '暂无'}。",
        f"- 标的收盘价：{_fmt_number(data.get('underlying_price'), 2)}；DTE范围：{data.get('dte_min')}~{data.get('dte_max')}；返回候选：{len(candidates)}组",
    ]
    if data.get("date_note"):
        lines.append(f"- 日期说明：{data['date_note']}")
    lines.append(f"- 数据缺口：{'；'.join(gaps) if gaps else '未发现关键缺口'}")

    for idx, candidate in enumerate(candidates, start=1):
        label = _STRATEGY_LABELS.get(candidate.get("strategy"), str(candidate.get("strategy") or ""))
        lines.append(f"\n候选 {idx}｜{label}")
        for leg in candidate.get("legs") or []:
            lines.append(f"- {_fmt_leg(leg)}")
        lines.append(f"- 风险/收益：{_render_candidate_metrics(candidate)}")

    lines.append("\n提示：以上为本地美股期权EOD候选筛选，不是实时盘口或下单指令；盘中需复核 bid/ask、保证金、滑点和成交量。")
    return "\n".join(lines)


def build_us_option_strategy_candidates(
    underlying: str,
    strategy: str = "",
    trade_date: str = "",
    dte_min: int = 30,
    dte_max: int = 45,
    max_candidates: int = 5,
    risk_budget_pct: float = 5.0,
    risk_preference: str = "",
    *,
    use_test_tables: bool = False,
    engine=None,
    use_dashboard_engine: bool = True,
) -> dict[str, Any]:
    symbol, reason = normalize_us_option_underlying(underlying)
    canonical_strategy, strategy_label = normalize_us_option_strategy(strategy)
    if not symbol:
        return {
            "status": "unsupported",
            "underlying": "",
            "strategy": canonical_strategy,
            "gaps": [reason],
            "candidates": [],
            "report": f"【美股期权策略候选】数据不足\n- 原因：{reason}",
        }

    engine = engine if engine is not None else (dashboard_engine() if use_dashboard_engine else None)
    if engine is None:
        reason = "数据库未连接，无法读取本地美股期权链。"
        return {
            "status": "no_data",
            "underlying": symbol,
            "strategy": canonical_strategy,
            "gaps": [reason],
            "candidates": [],
            "report": f"【美股期权策略候选】{symbol}\n结论：数据不足\n- 原因：{reason}",
        }

    try:
        dte_min = max(int(dte_min), 1)
        dte_max = max(int(dte_max), dte_min)
    except Exception:
        dte_min, dte_max = 30, 45
    try:
        max_candidates = min(max(int(max_candidates or 5), 1), 12)
    except Exception:
        max_candidates = 5
    try:
        risk_budget_pct = min(max(float(risk_budget_pct or 5.0), 0.1), 100.0)
    except Exception:
        risk_budget_pct = 5.0

    selected_date, date_note, available_dates = _resolve_trade_date(
        symbol,
        trade_date,
        use_test_tables=use_test_tables,
        engine=engine,
    )
    if not selected_date:
        reason = "未找到该标的的本地美股期权交易日数据。"
        return {
            "status": "no_data",
            "underlying": symbol,
            "trade_date": "",
            "available_dates": available_dates,
            "strategy": canonical_strategy,
            "gaps": [reason],
            "candidates": [],
            "report": f"【美股期权策略候选】{symbol}\n结论：数据不足\n- 原因：{reason}",
        }

    stock_df = load_stock_daily(symbol, limit=420, engine=engine)
    underlying_price = selected_underlying_price(stock_df, selected_date)
    chain_df = load_option_chain_daily(
        symbol,
        selected_date,
        include_short_cycle=True,
        use_test_tables=use_test_tables,
        underlying_price=underlying_price,
        engine=engine,
    )
    if underlying_price is None and chain_df is not None and not chain_df.empty:
        price_values = pd.to_numeric(chain_df.get("underlying_price"), errors="coerce").dropna()
        if not price_values.empty:
            underlying_price = float(price_values.iloc[-1])

    prepared = _prepare_candidate_chain(chain_df, underlying_price, dte_min, dte_max)
    candidates = _strategy_candidates_for_chain(prepared, canonical_strategy, underlying_price, max_candidates)

    profile = build_us_option_market_profile(
        symbol,
        trade_date=selected_date,
        window=252,
        use_test_tables=use_test_tables,
        engine=engine,
        use_dashboard_engine=False,
    )
    metrics = profile.get("metrics") or {}

    gaps: list[str] = []
    if date_note:
        gaps.append(date_note)
    if stock_df is None or stock_df.empty or underlying_price is None:
        gaps.append("标的收盘价缺失，无法完整计算moneyness和风险指标")
    if chain_df is None or chain_df.empty:
        gaps.append("期权链日线缺失")
    if prepared.empty:
        gaps.append(f"未找到DTE {dte_min}-{dte_max} 且具备EOD价格的候选合约")
    has_iv_rows = (not prepared.empty) and int(prepared["iv_decimal"].notna().sum()) > 0
    if not has_iv_rows:
        gaps.append("IV缺失，Delta估算不可用或候选质量下降")
    if not candidates:
        gaps.append("未筛出符合当前策略条件的候选组合")
    # 逐腿判断，避免把无量合约包装成强推荐。
    if candidates:
        weak = []
        for idx, candidate in enumerate(candidates, start=1):
            liq = (candidate.get("metrics") or {}).get("liquidity") or {}
            if int(liq.get("min_volume") or 0) <= 0 or int(liq.get("min_open_interest") or 0) <= 0:
                weak.append(str(idx))
        if weak:
            gaps.append(f"候选{','.join(weak)}存在低流动性腿，需盘中复核")

    data = {
        "status": "ok" if candidates else "partial",
        "underlying": symbol,
        "trade_date": selected_date,
        "date_note": date_note,
        "available_dates": available_dates,
        "strategy": canonical_strategy,
        "strategy_label": strategy_label,
        "dte_min": dte_min,
        "dte_max": dte_max,
        "risk_budget_pct": risk_budget_pct,
        "risk_preference": risk_preference,
        "underlying_price": underlying_price,
        "metrics": metrics,
        "candidates": candidates,
        "gaps": gaps,
        "suitability": _candidate_strategy_summary(canonical_strategy, metrics, risk_preference),
    }
    data["report"] = _render_strategy_candidates_report(data)
    return data


@tool(args_schema=USOptionMarketProfileInput)
def get_us_option_market_profile(underlying: str, trade_date: str = "", window: int = 252) -> str:
    """
    查询美股/美股ETF期权单标的波动率体检。

    仅用于美股期权问题，例如 SPY、QQQ、NVDA、TSLA 的 IV Rank、期限结构、
    skew、Put/Call、0DTE成交占比、OI防线。不要用于A股ETF期权、商品期权或股指期权。
    返回本地EOD数据摘要；缺数据时会明确说明缺口。
    """
    return build_us_option_market_profile(
        underlying=underlying,
        trade_date=trade_date,
        window=window,
        use_test_tables=False,
    ).get("report", "")


@tool(args_schema=USOptionStrategyCandidatesInput)
def get_us_option_strategy_candidates(
    underlying: str,
    strategy: str = "",
    trade_date: str = "",
    dte_min: int = 30,
    dte_max: int = 45,
    max_candidates: int = 5,
    risk_budget_pct: float = 5.0,
    risk_preference: str = "",
) -> str:
    """
    查询美股/美股ETF期权策略候选合约。

    用于美股期权策略问法，例如 AAPL 卖put、SPY 铁鹰、NVDA bull put spread。
    返回本地EOD行权价、权利金、估算Delta、成交量/OI和风险指标；不是实时盘口或下单指令。
    """
    return build_us_option_strategy_candidates(
        underlying=underlying,
        strategy=strategy,
        trade_date=trade_date,
        dte_min=dte_min,
        dte_max=dte_max,
        max_candidates=max_candidates,
        risk_budget_pct=risk_budget_pct,
        risk_preference=risk_preference,
        use_test_tables=False,
    ).get("report", "")
