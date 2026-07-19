from __future__ import annotations

import json
import math
from typing import Any

import pandas as pd
from langchain_core.tools import tool
from pydantic import BaseModel, Field
from sqlalchemy import text

from cn_market_climate_data import (
    MARGIN_LEVERAGE,
    MARGIN_MOMENTUM_5D,
    MARKET_AMOUNT,
    empirical_percentile,
)


MIN_PERCENTILE_SAMPLES = 252
HIGH_LEVERAGE_PERCENTILE = 90.0
LOW_LEVERAGE_PERCENTILE = 20.0
HOT_MOMENTUM_PERCENTILE = 80.0
COOL_MOMENTUM_PERCENTILE = 20.0
MIN_DIRECTION_STREAK = 3
MAX_FRESH_LAG_SESSIONS = 2

CSI300 = "000300.SH"
CSI1000 = "000852.SH"
REQUIRED_INDEX_CODES = (CSI300, CSI1000)
REQUIRED_CLIMATE_CODES = (MARGIN_LEVERAGE, MARGIN_MOMENTUM_5D, MARKET_AMOUNT)


class CNMarginMarketSignalInput(BaseModel):
    as_of_date: str = Field(
        default="",
        description="可选数据截止日，格式 YYYYMMDD 或 YYYY-MM-DD；留空使用本地最新共同数据日",
    )


def _number(value: Any) -> float | None:
    if value is None:
        return None
    try:
        out = float(value)
    except (TypeError, ValueError):
        return None
    return out if math.isfinite(out) else None


def _compact_date(value: Any) -> str:
    raw = str(value or "").strip().replace("-", "").replace("/", "")
    if len(raw) != 8 or not raw.isdigit():
        return ""
    try:
        pd.to_datetime(raw, format="%Y%m%d", errors="raise")
    except (TypeError, ValueError):
        return ""
    return raw


def _date_label(value: Any) -> str:
    day = _compact_date(value)
    return f"{day[:4]}-{day[4:6]}-{day[6:]}" if day else "--"


def _json_object(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    try:
        parsed = json.loads(str(value or "{}"))
    except (TypeError, ValueError, json.JSONDecodeError):
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _fmt_number(value: Any, digits: int = 2) -> str:
    number = _number(value)
    return "--" if number is None else f"{number:,.{digits}f}"


def _fmt_signed_pct(value: Any, digits: int = 2) -> str:
    number = _number(value)
    return "--" if number is None else f"{number:+.{digits}f}%"


def _fmt_percentile(value: Any) -> str:
    number = _number(value)
    return "--" if number is None else f"{number:.0f}/100"


def _scaled(value: Any, divisor: float) -> float | None:
    number = _number(value)
    if number is None:
        return None
    return number / divisor


def _default_engine() -> Any:
    # Lazy import avoids creating another connection pool during ordinary module import.
    from data_engine import engine

    return engine


def _empty_result(status: str, reason: str, *, requested_date: str = "") -> dict[str, Any]:
    report = "【A股融资市场信号】\n结论：数据不足"
    if reason:
        report += f"\n- 原因：{reason}"
    return {
        "status": status,
        "requested_date": requested_date,
        "data_date": "",
        "signal_code": "DATA_UNAVAILABLE",
        "signal_label": "数据不足",
        "risk_level": "unknown",
        "market_bias": "unknown",
        "metrics": {},
        "confirmation": {},
        "source_dates": {},
        "sources": {},
        "gaps": [reason] if reason else [],
        "report": report,
    }


def _read_frames(engine: Any, as_of_date: str = "") -> dict[str, pd.DataFrame]:
    end_date = as_of_date or "99991231"
    params = {"end_date": end_date}
    margin = pd.read_sql(
        text(
            """
            SELECT trade_date, exchange_id, financing_balance_yuan, financing_buy_yuan,
                   source_name, quality_status
            FROM cn_margin_daily
            WHERE trade_date <= :end_date
            ORDER BY trade_date, exchange_id
            """
        ),
        engine,
        params=params,
    )
    climate = pd.read_sql(
        text(
            """
            SELECT trade_date, metric_code, metric_value, percentile, secondary_value,
                   sample_count, payload_json, source_dates_json, quality_status
            FROM cn_market_climate_daily
            WHERE trade_date <= :end_date
              AND metric_code IN ('MARGIN_LEVERAGE', 'MARGIN_MOMENTUM_5D', 'MARKET_AMOUNT')
            ORDER BY trade_date, metric_code
            """
        ),
        engine,
        params=params,
    )
    index_prices = pd.read_sql(
        text(
            """
            SELECT REPLACE(REPLACE(trade_date, '-', ''), '/', '') AS trade_date,
                   ts_code, close_price
            FROM index_price
            WHERE ts_code IN ('000300.SH', '000852.SH')
              AND REPLACE(REPLACE(trade_date, '-', ''), '/', '') <= :end_date
            ORDER BY trade_date, ts_code
            """
        ),
        engine,
        params=params,
    )
    return {"margin": margin, "climate": climate, "index": index_prices}


def _prepare_margin_history(frame: pd.DataFrame) -> pd.DataFrame:
    required = {"trade_date", "exchange_id", "financing_balance_yuan"}
    if frame is None or frame.empty or not required.issubset(frame.columns):
        return pd.DataFrame()
    data = frame.copy()
    data["trade_date"] = data["trade_date"].map(_compact_date)
    data["exchange_id"] = data["exchange_id"].astype(str).str.upper().str.strip()
    data["financing_balance_yuan"] = pd.to_numeric(
        data["financing_balance_yuan"], errors="coerce"
    )
    if "financing_buy_yuan" not in data.columns:
        data["financing_buy_yuan"] = float("nan")
    data["financing_buy_yuan"] = pd.to_numeric(data["financing_buy_yuan"], errors="coerce")
    for column in ("source_name", "quality_status"):
        if column not in data.columns:
            data[column] = ""
    data = data[
        data["trade_date"].str.len().eq(8)
        & data["exchange_id"].isin({"SSE", "SZSE"})
        & data["financing_balance_yuan"].gt(0)
    ].sort_values(["trade_date", "exchange_id"])
    data = data.drop_duplicates(["trade_date", "exchange_id"], keep="last")
    if data.empty:
        return pd.DataFrame()

    balance = data.pivot(index="trade_date", columns="exchange_id", values="financing_balance_yuan")
    if not {"SSE", "SZSE"}.issubset(balance.columns):
        return pd.DataFrame()
    history = balance[["SSE", "SZSE"]].dropna().rename(
        columns={"SSE": "sse_balance_yuan", "SZSE": "szse_balance_yuan"}
    )
    history["balance_yuan"] = history["sse_balance_yuan"] + history["szse_balance_yuan"]

    buys = data.pivot(index="trade_date", columns="exchange_id", values="financing_buy_yuan")
    if {"SSE", "SZSE"}.issubset(buys.columns):
        history["buy_yuan"] = buys[["SSE", "SZSE"]].sum(axis=1, min_count=2)
    else:
        history["buy_yuan"] = float("nan")

    metadata = data.set_index(["trade_date", "exchange_id"])
    for exchange_id, prefix in (("SSE", "sse"), ("SZSE", "szse")):
        exchange_rows = metadata.xs(exchange_id, level="exchange_id", drop_level=True)
        history[f"{prefix}_source"] = exchange_rows["source_name"]
        history[f"{prefix}_quality"] = exchange_rows["quality_status"]
    return history.reset_index().sort_values("trade_date").reset_index(drop=True)


def _prepare_climate_history(frame: pd.DataFrame) -> pd.DataFrame:
    required = {"trade_date", "metric_code", "metric_value"}
    if frame is None or frame.empty or not required.issubset(frame.columns):
        return pd.DataFrame()
    data = frame.copy()
    data["trade_date"] = data["trade_date"].map(_compact_date)
    data["metric_code"] = data["metric_code"].astype(str).str.strip()
    for column in ("metric_value", "percentile", "secondary_value", "sample_count"):
        if column not in data.columns:
            data[column] = None
        data[column] = pd.to_numeric(data[column], errors="coerce")
    for column, default in (
        ("payload_json", "{}"),
        ("source_dates_json", "{}"),
        ("quality_status", ""),
    ):
        if column not in data.columns:
            data[column] = default
    data = data[
        data["trade_date"].str.len().eq(8)
        & data["metric_code"].isin(REQUIRED_CLIMATE_CODES)
    ]
    data = data[
        data.apply(
            lambda row: bool(_json_object(row.get("source_dates_json")))
            and all(
                _compact_date(source_day) == row["trade_date"]
                for source_day in _json_object(row.get("source_dates_json")).values()
            ),
            axis=1,
        )
    ]
    return data.sort_values(["trade_date", "metric_code"]).drop_duplicates(
        ["trade_date", "metric_code"], keep="last"
    )


def _prepare_index_history(frame: pd.DataFrame) -> pd.DataFrame:
    required = {"trade_date", "ts_code", "close_price"}
    if frame is None or frame.empty or not required.issubset(frame.columns):
        return pd.DataFrame()
    data = frame.copy()
    data["trade_date"] = data["trade_date"].map(_compact_date)
    data["ts_code"] = data["ts_code"].astype(str).str.upper().str.strip()
    data["close_price"] = pd.to_numeric(data["close_price"], errors="coerce")
    return data[
        data["trade_date"].str.len().eq(8)
        & data["ts_code"].isin(REQUIRED_INDEX_CODES)
        & data["close_price"].gt(0)
    ].sort_values(["trade_date", "ts_code"]).drop_duplicates(
        ["trade_date", "ts_code"], keep="last"
    )


def _aligned_dates(
    margin: pd.DataFrame,
    climate: pd.DataFrame,
    index_prices: pd.DataFrame,
) -> list[str]:
    if margin.empty or climate.empty or index_prices.empty:
        return []
    climate_counts = climate.groupby("trade_date")["metric_code"].nunique()
    climate_dates = set(climate_counts[climate_counts >= len(REQUIRED_CLIMATE_CODES)].index)
    index_counts = index_prices.groupby("trade_date")["ts_code"].nunique()
    index_dates = set(index_counts[index_counts >= len(REQUIRED_INDEX_CODES)].index)
    return sorted(set(margin["trade_date"]) & climate_dates & index_dates)


def _change_pct(values: pd.Series, sessions: int) -> float | None:
    series = pd.to_numeric(values, errors="coerce").dropna()
    if len(series) <= sessions:
        return None
    previous = _number(series.iloc[-sessions - 1])
    current = _number(series.iloc[-1])
    if previous is None or previous <= 0 or current is None:
        return None
    return (current / previous - 1.0) * 100.0


def _direction_streak(values: pd.Series) -> tuple[int, int]:
    diffs = pd.to_numeric(values, errors="coerce").diff().dropna()
    if diffs.empty:
        return 0, 0
    latest_sign = 1 if diffs.iloc[-1] > 0 else (-1 if diffs.iloc[-1] < 0 else 0)
    if latest_sign == 0:
        return 0, 0
    count = 0
    for value in reversed(diffs.tolist()):
        sign = 1 if value > 0 else (-1 if value < 0 else 0)
        if sign != latest_sign:
            break
        count += 1
    return (count, 0) if latest_sign > 0 else (0, count)


def _is_new_high(values: pd.Series, sessions: int) -> bool | None:
    series = pd.to_numeric(values, errors="coerce").dropna()
    if len(series) <= sessions:
        return None
    current = _number(series.iloc[-1])
    previous_max = _number(series.iloc[-sessions - 1 : -1].max())
    if current is None or previous_max is None:
        return None
    return current > previous_max


def classify_cn_margin_signal(
    *,
    leverage_percentile: Any,
    momentum_5d_pct: Any,
    momentum_percentile: Any,
    up_streak: int,
    down_streak: int,
    new_high_252d: bool | None = None,
    new_high_756d: bool | None = None,
    stale: bool = False,
    sufficient: bool = True,
) -> dict[str, str]:
    leverage_pct = _number(leverage_percentile)
    momentum = _number(momentum_5d_pct)
    momentum_pct = _number(momentum_percentile)
    if stale:
        return {
            "signal_code": "DATA_STALE",
            "signal_label": "数据陈旧",
            "risk_level": "unknown",
            "market_bias": "unknown",
            "summary": "融资数据已超过有效时限，本次不触发市场预警。",
        }
    if not sufficient or leverage_pct is None or momentum is None or momentum_pct is None:
        return {
            "signal_code": "INSUFFICIENT_HISTORY",
            "signal_label": "样本不足",
            "risk_level": "unknown",
            "market_bias": "unknown",
            "summary": "融资杠杆或动能历史不足，暂不判断市场状态。",
        }

    warming = (
        int(up_streak) >= MIN_DIRECTION_STREAK
        and momentum > 0
        and momentum_pct >= HOT_MOMENTUM_PERCENTILE
    )
    cooling = (
        int(down_streak) >= MIN_DIRECTION_STREAK
        and momentum < 0
        and momentum_pct <= COOL_MOMENTUM_PERCENTILE
    )
    high_leverage = leverage_pct >= HIGH_LEVERAGE_PERCENTILE
    low_leverage = leverage_pct <= LOW_LEVERAGE_PERCENTILE

    if high_leverage and warming:
        record_high = bool(new_high_252d) or bool(new_high_756d)
        return {
            "signal_code": "OVERHEATED_RISING",
            "signal_label": "高杠杆继续升温",
            "risk_level": "high" if record_high else "medium",
            "market_bias": "cautious",
            "summary": (
                "杠杆已处高位且融资继续快速增加，出现杠杆拥挤和市场过热风险。"
                if record_high
                else "杠杆已处高位且融资继续升温，需要防范拥挤交易。"
            ),
        }
    if high_leverage and cooling:
        return {
            "signal_code": "HIGH_LEVERAGE_DELEVERAGING",
            "signal_label": "高位去杠杆",
            "risk_level": "medium",
            "market_bias": "cautious",
            "summary": "杠杆仍在高位，但融资资金正快速撤退，短期波动可能被放大。",
        }
    if low_leverage and warming:
        return {
            "signal_code": "RISK_APPETITE_RECOVERY",
            "signal_label": "风险偏好修复",
            "risk_level": "low",
            "market_bias": "supportive",
            "summary": "杠杆处于低位而融资开始回流，风险偏好有所修复。",
        }
    if cooling:
        return {
            "signal_code": "SPECULATIVE_RETREAT",
            "signal_label": "投机资金撤退",
            "risk_level": "medium",
            "market_bias": "cautious",
            "summary": "融资连续下降且动能偏弱，短线投机资金和市场承接正在降温。",
        }
    return {
        "signal_code": "NEUTRAL_WATCH",
        "signal_label": "中性观察",
        "risk_level": "low",
        "market_bias": "neutral",
        "summary": "融资变化尚未形成明确的升温或撤退信号。",
    }


def _index_return(index_prices: pd.DataFrame, code: str, day: str, sessions: int) -> float | None:
    series = index_prices[
        (index_prices["ts_code"] == code) & (index_prices["trade_date"] <= day)
    ].sort_values("trade_date")
    return _change_pct(series["close_price"], sessions)


def _build_market_confirmation(
    *,
    momentum_5d_pct: Any,
    csi300_5d_pct: Any,
    csi1000_5d_pct: Any,
    turnover_ma20_ratio: Any,
) -> dict[str, Any]:
    momentum = _number(momentum_5d_pct)
    csi300 = _number(csi300_5d_pct)
    csi1000 = _number(csi1000_5d_pct)
    ma20_ratio = _number(turnover_ma20_ratio)
    both_up = csi300 is not None and csi1000 is not None and csi300 > 0 and csi1000 > 0
    both_down = csi300 is not None and csi1000 is not None and csi300 < 0 and csi1000 < 0
    weak_turnover = ma20_ratio is not None and ma20_ratio < 0.9

    if momentum is not None and momentum > 0 and both_up:
        code = "RISK_APPETITE_CONFIRMED"
        summary = "指数与融资同步上升，风险偏好得到市场表现确认。"
    elif momentum is not None and momentum > 0 and both_down:
        code = "LEVERAGED_DIP_BUYING"
        summary = "指数下跌但融资增加，可能存在杠杆抄底或被套资金累积。"
    elif momentum is not None and momentum < 0 and both_down and weak_turnover:
        code = "RETREAT_CONFIRMED"
        summary = "指数、融资和成交活跃度同步走弱，资金撤退信号得到加强。"
    elif momentum is not None and momentum < 0 and both_up:
        code = "UNLEVERAGED_RALLY"
        summary = "指数上涨但融资下降，本轮上涨并非由杠杆资金推动，不直接视为利空。"
    else:
        code = "MIXED"
        summary = "指数、融资与成交额尚未形成一致方向。"
    return {"confirmation_code": code, "summary": summary}


def _activity_metrics(
    margin: pd.DataFrame,
    climate: pd.DataFrame,
    selected_date: str,
) -> dict[str, Any]:
    amount = climate[climate["metric_code"] == MARKET_AMOUNT][
        ["trade_date", "metric_value"]
    ].rename(columns={"metric_value": "amount_yuan"})
    joined = margin[["trade_date", "buy_yuan"]].merge(amount, on="trade_date", how="inner")
    joined["buy_yuan"] = pd.to_numeric(joined["buy_yuan"], errors="coerce")
    joined["amount_yuan"] = pd.to_numeric(joined["amount_yuan"], errors="coerce")
    joined = joined[
        (joined["trade_date"] <= selected_date)
        & joined["buy_yuan"].gt(0)
        & joined["amount_yuan"].gt(0)
    ].copy()
    if joined.empty:
        return {"buy_yuan": None, "buy_turnover_ratio_pct": None, "percentile": None, "sample_count": 0}
    joined["buy_turnover_ratio_pct"] = joined["buy_yuan"] / joined["amount_yuan"] * 100.0
    latest = joined[joined["trade_date"] == selected_date]
    if latest.empty:
        return {"buy_yuan": None, "buy_turnover_ratio_pct": None, "percentile": None, "sample_count": 0}
    current = _number(latest.iloc[-1]["buy_turnover_ratio_pct"])
    start = (pd.to_datetime(selected_date, format="%Y%m%d") - pd.DateOffset(years=3)).strftime("%Y%m%d")
    window = joined[joined["trade_date"] >= start]
    percentile, sample_count = empirical_percentile(
        window["buy_turnover_ratio_pct"], current, min_samples=MIN_PERCENTILE_SAMPLES
    )
    return {
        "buy_yuan": _number(latest.iloc[-1]["buy_yuan"]),
        "buy_turnover_ratio_pct": current,
        "percentile": percentile,
        "sample_count": sample_count,
    }


def _render_report(result: dict[str, Any]) -> str:
    metrics = result.get("metrics") or {}
    confirmation = result.get("confirmation") or {}
    changes = metrics.get("balance_changes_pct") or {}
    streak_text = "暂无连续方向"
    if int(metrics.get("up_streak") or 0) > 0:
        streak_text = f"连续增加{int(metrics['up_streak'])}日"
    elif int(metrics.get("down_streak") or 0) > 0:
        streak_text = f"连续下降{int(metrics['down_streak'])}日"
    high_flags: list[str] = []
    if metrics.get("new_high_252d"):
        high_flags.append("创1年新高")
    if metrics.get("new_high_756d"):
        high_flags.append("创3年新高")
    high_text = "、".join(high_flags) if high_flags else "未创新高"

    activity = metrics.get("activity") or {}
    amount = metrics.get("market_amount") or {}
    lines = [
        f"【A股融资市场信号】数据日：{_date_label(result.get('data_date'))}（EOD/T+1）",
        f"结论：{result.get('signal_label')}。{result.get('summary')}",
        (
            f"- 融资余额：{_fmt_number(_scaled(metrics.get('balance_yuan'), 1_000_000_000_000), 2)}万亿元；"
            f"1日 { _fmt_signed_pct(changes.get('1d')) }，5日 { _fmt_signed_pct(changes.get('5d')) }，"
            f"10日 { _fmt_signed_pct(changes.get('10d')) }，20日 { _fmt_signed_pct(changes.get('20d')) }；"
            f"{streak_text}，{high_text}。"
        ),
        (
            f"- 杠杆位置：融资占流通市值 {_fmt_number(metrics.get('leverage_ratio_pct'), 2)}%，"
            f"3年分位 {_fmt_percentile(metrics.get('leverage_percentile'))}；"
            f"5日动能 {_fmt_signed_pct(metrics.get('momentum_5d_pct'))}，"
            f"分位 {_fmt_percentile(metrics.get('momentum_percentile'))}。"
        ),
        (
            f"- 融资活跃度：当日融资买入 {_fmt_number(_scaled(activity.get('buy_yuan'), 100_000_000), 0)}亿元，"
            f"占沪深成交额 {_fmt_number(activity.get('buy_turnover_ratio_pct'), 2)}%，"
            f"分位 {_fmt_percentile(activity.get('percentile'))}。"
        ),
        (
            f"- 市场确认：沪深300近5日 {_fmt_signed_pct(confirmation.get('csi300_5d_pct'))}、"
            f"中证1000近5日 {_fmt_signed_pct(confirmation.get('csi1000_5d_pct'))}；"
            f"成交额 {_fmt_number(_scaled(amount.get('amount_yuan'), 1_000_000_000_000), 2)}万亿元，"
            f"相当于20日均值 {_fmt_number(_scaled(amount.get('ma20_ratio'), 0.01), 0)}%。"
        ),
        f"- 辅助判断：{confirmation.get('summary') or '市场确认数据不足。'}",
    ]
    sources = result.get("sources") or {}
    source_text = "；".join(
        f"{label}{sources.get(key) or '未知'}"
        for key, label in (("SSE", "上交所："), ("SZSE", "深交所："))
    )
    lines.append(f"- 数据来源：{source_text}。")
    if result.get("date_note"):
        lines.append(f"- 日期说明：{result['date_note']}")
    gaps = [str(item) for item in result.get("gaps") or [] if str(item).strip()]
    if gaps:
        lines.append(f"- 数据缺口：{'；'.join(gaps)}")
    lines.append("提示：融资反映杠杆资金状态，是行情辅助信号，不等同于指数必涨或必跌，也不单独构成买卖建议。")
    return "\n".join(lines)


def build_cn_margin_market_signal_from_frames(
    margin_df: pd.DataFrame,
    climate_df: pd.DataFrame,
    index_df: pd.DataFrame,
    *,
    as_of_date: str = "",
) -> dict[str, Any]:
    requested = _compact_date(as_of_date)
    if as_of_date and not requested:
        return _empty_result("invalid_request", "日期格式无效，请使用 YYYYMMDD 或 YYYY-MM-DD。")

    margin = _prepare_margin_history(margin_df)
    climate = _prepare_climate_history(climate_df)
    index_prices = _prepare_index_history(index_df)
    aligned = _aligned_dates(margin, climate, index_prices)
    if requested:
        aligned = [day for day in aligned if day <= requested]
    if not aligned:
        return _empty_result("no_data", "没有找到沪深融资、流通市值、成交额和指数同日对齐的数据。", requested_date=requested)
    selected_date = aligned[-1]

    trading_dates = sorted(
        {
            day
            for day in index_prices[index_prices["ts_code"] == CSI1000]["trade_date"].tolist()
            if not requested or day <= requested
        }
    )
    reference_date = trading_dates[-1] if trading_dates else selected_date
    stale_lag = sum(selected_date < day <= reference_date for day in trading_dates)
    stale = stale_lag > MAX_FRESH_LAG_SESSIONS

    margin_history = margin[margin["trade_date"] <= selected_date].copy()
    latest_margin = margin_history.iloc[-1]
    climate_day = climate[climate["trade_date"] == selected_date].set_index("metric_code")
    leverage_row = climate_day.loc[MARGIN_LEVERAGE]
    momentum_row = climate_day.loc[MARGIN_MOMENTUM_5D]
    amount_row = climate_day.loc[MARKET_AMOUNT]
    leverage_samples = int(_number(leverage_row.get("sample_count")) or 0)
    momentum_samples = int(_number(momentum_row.get("sample_count")) or 0)
    sufficient = leverage_samples >= MIN_PERCENTILE_SAMPLES and momentum_samples >= MIN_PERCENTILE_SAMPLES
    leverage_percentile = _number(leverage_row.get("percentile")) if sufficient else None
    momentum_percentile = _number(momentum_row.get("percentile")) if sufficient else None
    up_streak, down_streak = _direction_streak(margin_history["balance_yuan"])
    new_high_252d = _is_new_high(margin_history["balance_yuan"], 252)
    new_high_756d = _is_new_high(margin_history["balance_yuan"], 756)
    momentum_5d_pct = _number(momentum_row.get("metric_value"))

    signal = classify_cn_margin_signal(
        leverage_percentile=leverage_percentile,
        momentum_5d_pct=momentum_5d_pct,
        momentum_percentile=momentum_percentile,
        up_streak=up_streak,
        down_streak=down_streak,
        new_high_252d=new_high_252d,
        new_high_756d=new_high_756d,
        stale=stale,
        sufficient=sufficient,
    )
    amount_payload = _json_object(amount_row.get("payload_json"))
    csi300_5d = _index_return(index_prices, CSI300, selected_date, 5)
    csi1000_5d = _index_return(index_prices, CSI1000, selected_date, 5)
    confirmation = _build_market_confirmation(
        momentum_5d_pct=momentum_5d_pct,
        csi300_5d_pct=csi300_5d,
        csi1000_5d_pct=csi1000_5d,
        turnover_ma20_ratio=amount_payload.get("ma20_ratio") or amount_row.get("secondary_value"),
    )
    confirmation.update(
        csi300_5d_pct=csi300_5d,
        csi300_20d_pct=_index_return(index_prices, CSI300, selected_date, 20),
        csi1000_5d_pct=csi1000_5d,
        csi1000_20d_pct=_index_return(index_prices, CSI1000, selected_date, 20),
    )
    if signal["signal_code"] == "HIGH_LEVERAGE_DELEVERAGING" and confirmation["confirmation_code"] == "RETREAT_CONFIRMED":
        signal["risk_level"] = "high"
        signal["summary"] = "杠杆仍在高位，融资、指数和成交活跃度同步下降，去杠杆可能放大短期波动。"

    activity = _activity_metrics(margin, climate, selected_date)
    amount_value = _number(amount_row.get("metric_value"))
    amount_ma20_ratio = _number(amount_payload.get("ma20_ratio"))
    if amount_ma20_ratio is None:
        amount_ma20_ratio = _number(amount_row.get("secondary_value"))

    date_note = ""
    if requested and selected_date < requested:
        date_note = f"请求 {_date_label(requested)}，已回退到最近完整共同数据日 {_date_label(selected_date)}。"
    if stale:
        stale_note = f"共同数据日落后市场参考日{stale_lag}个交易日，本次不触发当前预警。"
        date_note = f"{date_note} {stale_note}".strip()

    source_qualities = {
        str(latest_margin.get("sse_quality") or "").strip(),
        str(latest_margin.get("szse_quality") or "").strip(),
    }
    source_qualities.discard("")
    status = "stale" if stale else ("ok" if sufficient else "insufficient_history")
    if not stale and any("fallback" in item or "mirror" in item for item in source_qualities):
        status = "fallback_validated" if sufficient else "insufficient_history"

    gaps: list[str] = []
    if not sufficient:
        gaps.append("融资杠杆或5日动能的有效历史不足252个交易日。")
    if activity.get("percentile") is None:
        gaps.append("融资买入占成交额的历史分位暂不可用。")
    result = {
        "status": status,
        "requested_date": requested,
        "data_date": selected_date,
        **signal,
        "summary": signal["summary"],
        "metrics": {
            "balance_yuan": _number(latest_margin.get("balance_yuan")),
            "balance_changes_pct": {
                "1d": _change_pct(margin_history["balance_yuan"], 1),
                "5d": _change_pct(margin_history["balance_yuan"], 5),
                "10d": _change_pct(margin_history["balance_yuan"], 10),
                "20d": _change_pct(margin_history["balance_yuan"], 20),
            },
            "leverage_ratio_pct": _number(leverage_row.get("metric_value")),
            "leverage_percentile": leverage_percentile,
            "leverage_sample_count": leverage_samples,
            "momentum_5d_pct": momentum_5d_pct,
            "momentum_percentile": momentum_percentile,
            "momentum_sample_count": momentum_samples,
            "up_streak": up_streak,
            "down_streak": down_streak,
            "new_high_252d": new_high_252d,
            "new_high_756d": new_high_756d,
            "activity": activity,
            "market_amount": {
                "amount_yuan": amount_value,
                "percentile": _number(amount_row.get("percentile")),
                "ma20_ratio": amount_ma20_ratio,
            },
        },
        "confirmation": confirmation,
        "source_dates": {
            "margin": selected_date,
            "leverage": selected_date,
            "market_amount": selected_date,
            "CSI300": selected_date,
            "CSI1000": selected_date,
        },
        "sources": {
            "SSE": str(latest_margin.get("sse_source") or ""),
            "SZSE": str(latest_margin.get("szse_source") or ""),
        },
        "source_quality": sorted(source_qualities),
        "stale_trading_days": stale_lag,
        "date_note": date_note,
        "gaps": gaps,
    }
    result["report"] = _render_report(result)
    return result


def build_cn_margin_market_signal(
    as_of_date: str = "",
    *,
    engine: Any = None,
) -> dict[str, Any]:
    requested = _compact_date(as_of_date)
    if as_of_date and not requested:
        return _empty_result("invalid_request", "日期格式无效，请使用 YYYYMMDD 或 YYYY-MM-DD。")
    try:
        active_engine = engine if engine is not None else _default_engine()
        frames = _read_frames(active_engine, requested)
    except Exception as exc:
        return _empty_result("error", f"本地融资数据读取失败：{exc}", requested_date=requested)
    return build_cn_margin_market_signal_from_frames(
        frames["margin"],
        frames["climate"],
        frames["index"],
        as_of_date=requested,
    )


@tool(args_schema=CNMarginMarketSignalInput)
def get_cn_margin_market_signal(as_of_date: str = "") -> str:
    """
    查询A股沪深融资市场信号，用于大盘、指数、A股ETF及ETF期权的市场环境判断。

    返回融资余额、杠杆分位、5日动能、连续升降、融资活跃度，以及沪深300、
    中证1000和成交额的同日确认。数据来自本地EOD/T+1缓存，不访问实时交易所，
    不用于公司再融资、期货保证金，也不单独构成买卖建议。
    """
    return build_cn_margin_market_signal(as_of_date=as_of_date).get("report", "")
