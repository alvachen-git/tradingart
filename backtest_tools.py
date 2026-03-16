from __future__ import annotations

import json
import re
from datetime import datetime

import pandas as pd
from langchain_core.tools import tool
from pydantic import BaseModel, Field
from sqlalchemy import text

from backtest_engine import engine, run_etf_roll_backtest, run_max_oi_backtest
from backtest_time_parser import resolve_window


ETF_STRATEGIES = {
    "hold_underlying",
    "single_call",
    "single_put",
    "single_sell_call",
    "single_sell_put",
    "bull_spread",
    "bear_spread",
    "double_buy",
    "double_sell",
    "calendar_spread",
}

STRATEGY_ALIASES = {
    "hold_underlying": "hold_underlying",
    "持有标的etf": "hold_underlying",
    "持有etf": "hold_underlying",
    "单买认购": "single_call",
    "single_call": "single_call",
    "buy_call": "single_call",
    "单买认沽": "single_put",
    "single_put": "single_put",
    "buy_put": "single_put",
    "单卖认购": "single_sell_call",
    "single_sell_call": "single_sell_call",
    "sell_call": "single_sell_call",
    "单卖认沽": "single_sell_put",
    "single_sell_put": "single_sell_put",
    "sell_put": "single_sell_put",
    "牛市价差": "bull_spread",
    "bull_spread": "bull_spread",
    "bullcallspread": "bull_spread",
    "熊市价差": "bear_spread",
    "bear_spread": "bear_spread",
    "bearputspread": "bear_spread",
    "双买": "double_buy",
    "double_buy": "double_buy",
    "双卖": "double_sell",
    "double_sell": "double_sell",
    "日历价差": "calendar_spread",
    "calendar_spread": "calendar_spread",
    "max_oi_call": "max_oi_call",
    "max_oi_put": "max_oi_put",
    "deep_otm_put": "single_put",
}

STRIKE_MODE_ALIASES = {
    "atm": "ATM",
    "平值atm": "ATM",
    "平值": "ATM",
    "otm5": "OTM5",
    "虚值5%": "OTM5",
    "虚值5": "OTM5",
    "otm10": "OTM10",
    "虚值10%": "OTM10",
    "虚值10": "OTM10",
    "manual": "MANUAL",
    "手动": "MANUAL",
    "手动选择": "MANUAL",
}


class OptionStrategyBacktestRequest(BaseModel):
    symbol: str = Field(description="ETF标的，如 510500 / 510500.SH / 中证500ETF")
    strategy: str = Field(description="策略，如 双卖 / 牛市价差 / hold_underlying / single_call")
    start_date: str | None = Field(default=None, description="开始日期 YYYYMMDD；优先级最高")
    end_date: str | None = Field(default=None, description="结束日期 YYYYMMDD；优先级最高")
    time_expr: str | None = Field(default=None, description="时间表达式，如 近6个月/去年全年/2024Q3")
    lookback_days: int | None = Field(default=None, ge=1, le=3650, description="兜底回溯天数")
    strike_mode: str = Field(default="ATM", description="ATM|OTM5|OTM10|MANUAL")
    manual_params: dict | None = Field(default=None, description="手动行权价参数字典")
    lots: int = Field(default=1, ge=1, le=200, description="手数")
    extra_margin_rate: float = Field(default=0.0, ge=0.0, le=0.5, description="额外保证金比例")
    fee_per_lot: float = Field(default=2.0, ge=0.0, description="每手手续费")
    calendar_type: str = Field(default="卖近买远(认购)", description="日历价差方向")


class BacktestRequest(BaseModel):
    symbol: str = Field(description="标的，如 510050/510300/白银/黄金/au")
    strategy: str = Field(description="策略：max_oi_call / max_oi_put / double_sell / deep_otm_put")
    start_date: str = Field(description="起始日 YYYYMMDD", default=None)
    end_date: str = Field(description="结束日 YYYYMMDD", default=None)
    fee_rate: float = Field(description="手续费率，如 0.0003", default=0.0003)
    fee_per_lot: float = Field(description="每手固定手续费(元)", default=2.0)


def _norm_key(value: str | None) -> str:
    if not value:
        return ""
    return re.sub(r"[\s_\-()（）]", "", str(value).strip().lower())


def _normalize_strategy(strategy: str | None) -> str | None:
    raw = str(strategy or "").strip()
    if not raw:
        return None
    if raw in STRATEGY_ALIASES:
        return STRATEGY_ALIASES[raw]
    k = _norm_key(raw)
    return STRATEGY_ALIASES.get(k)


def _normalize_strike_mode(mode: str | None, strategy: str | None = None) -> str:
    if strategy == "hold_underlying":
        return "ATM"
    raw = str(mode or "ATM").strip()
    if raw in STRIKE_MODE_ALIASES:
        return STRIKE_MODE_ALIASES[raw]
    return STRIKE_MODE_ALIASES.get(_norm_key(raw), "ATM")


def _normalize_manual_params(manual_params: dict | str | None) -> dict:
    if manual_params is None:
        return {}
    if isinstance(manual_params, dict):
        return manual_params
    if isinstance(manual_params, str):
        txt = manual_params.strip()
        if not txt:
            return {}
        try:
            obj = json.loads(txt)
            return obj if isinstance(obj, dict) else {}
        except Exception:
            return {}
    return {}


def _normalize_calendar_type(calendar_type: str | None) -> str:
    txt = str(calendar_type or "").strip()
    if not txt:
        return "卖近买远(认购)"
    if txt in {"C", "认购"}:
        return "卖近买远(认购)"
    if txt in {"P", "认沽"}:
        return "卖近买远(认沽)"
    return txt


def _normalize_etf_underlying(symbol: str | None) -> str:
    s = str(symbol or "").strip().upper()
    if not s:
        return s
    if "." in s and len(s.split(".")) == 2:
        return s
    digits = re.sub(r"\D", "", s)
    if len(digits) == 6:
        if digits.startswith(("15", "16")):
            return f"{digits}.SZ"
        return f"{digits}.SH"
    return s


def _latest_trade_day_for_underlying(underlying: str | None) -> str:
    today = datetime.now().strftime("%Y%m%d")
    if engine is None:
        return today
    try:
        with engine.connect() as conn:
            if underlying and "." in str(underlying):
                sql = text(
                    """
                    SELECT MAX(d.trade_date) AS md
                    FROM option_daily d
                    INNER JOIN option_basic b ON d.ts_code = b.ts_code
                    WHERE b.underlying = :underlying
                    """
                )
                row = conn.execute(sql, {"underlying": underlying}).fetchone()
                if row and row[0]:
                    return str(row[0])
            row = conn.execute(text("SELECT MAX(trade_date) AS md FROM option_daily")).fetchone()
            if row and row[0]:
                return str(row[0])
    except Exception:
        return today
    return today


def align_to_prev_trade_day(date_yyyymmdd: str, underlying: str | None = None) -> str:
    if engine is None:
        return date_yyyymmdd
    d = re.sub(r"[^0-9]", "", str(date_yyyymmdd or "").strip())
    if len(d) != 8:
        return date_yyyymmdd
    try:
        with engine.connect() as conn:
            if underlying and "." in str(underlying):
                sql = text(
                    """
                    SELECT MAX(d.trade_date) AS md
                    FROM option_daily d
                    INNER JOIN option_basic b ON d.ts_code = b.ts_code
                    WHERE b.underlying = :underlying AND d.trade_date <= :date
                    """
                )
                row = conn.execute(sql, {"underlying": underlying, "date": d}).fetchone()
                if row and row[0]:
                    return str(row[0])

            row = conn.execute(
                text("SELECT MAX(trade_date) AS md FROM option_daily WHERE trade_date <= :date"),
                {"date": d},
            ).fetchone()
            if row and row[0]:
                return str(row[0])
    except Exception:
        return date_yyyymmdd
    return date_yyyymmdd


def _calc_profit_loss_ratio(trades_df: pd.DataFrame) -> tuple[float | None, str]:
    if trades_df is None or trades_df.empty:
        return None, "无成交记录"

    pnl_col = None
    if "net_pnl" in trades_df.columns:
        pnl_col = "net_pnl"
    elif "ret" in trades_df.columns:
        pnl_col = "ret"
    if pnl_col is None:
        return None, "成交记录缺少盈亏字段"

    series = pd.to_numeric(trades_df[pnl_col], errors="coerce").dropna()
    if series.empty:
        return None, "成交记录无有效盈亏数据"

    pos = series[series > 0]
    neg = series[series < 0]
    if pos.empty and neg.empty:
        return None, "全为0收益"
    if neg.empty:
        return float("inf"), "无亏损样本"
    if pos.empty:
        return 0.0, "无盈利样本"

    ratio = float(pos.mean() / abs(neg.mean()))
    return ratio, ""


def _format_result(
    result: dict,
    strategy: str,
    effective_start_date: str,
    effective_end_date: str,
    requested_time_expr: str | None,
    time_resolution_note: str,
) -> str:
    if "error" in result:
        return result["error"]

    summary = result.get("summary", {}) or {}
    trades = result.get("trades", pd.DataFrame())

    total_pnl = summary.get("total_pnl", summary.get("total_return", 0.0))
    annual_pnl = summary.get("annualized_pnl", summary.get("annualized_return", 0.0))
    max_dd = summary.get("max_drawdown", 0.0)
    max_dd_pct = summary.get("max_drawdown_pct", None)
    win_rate = summary.get("win_rate", 0.0)
    avg_trade = summary.get("avg_return", 0.0)
    ann_pct = summary.get("annualized_return_pct", None)

    pl_ratio, pl_note = _calc_profit_loss_ratio(trades if isinstance(trades, pd.DataFrame) else pd.DataFrame())
    if pl_ratio is None:
        pl_ratio_line = f"- 盈亏比(单笔均值比): N/A ({pl_note})"
    elif pl_ratio == float("inf"):
        pl_ratio_line = f"- 盈亏比(单笔均值比): INF ({pl_note})"
    else:
        pl_ratio_line = f"- 盈亏比(单笔均值比): {pl_ratio:.2f}"

    ann_pct_line = f"- 年化收益率: {ann_pct:.2%}" if ann_pct is not None else "- 年化收益率: N/A"
    max_dd_pct_line = f"{max_dd_pct:.2%}" if max_dd_pct is not None else "N/A"

    lines = [
        "**期权策略回测结果**",
        f"- 标的: {summary.get('symbol', '')}",
        f"- 策略: {strategy}",
        f"- 生效区间: {effective_start_date} ~ {effective_end_date}",
        f"- 实际回测区间: {summary.get('start_date', '')} ~ {summary.get('end_date', '')}",
        f"- requested_time_expr: {requested_time_expr or 'N/A'}",
        f"- time_resolution_note: {time_resolution_note or '无'}",
        f"- 交易次数: {summary.get('trades', 0)}",
        f"- 总盈亏: {total_pnl:.2f}",
        f"- 年化盈亏: {annual_pnl:.2f}",
        ann_pct_line,
        f"- 最大回撤(元): {max_dd:.2f}",
        f"- 最大回撤(%): {max_dd_pct_line}",
        f"- 胜率: {win_rate:.2%}",
        pl_ratio_line,
        f"- 平均单笔: {avg_trade:.2f}",
        f"- 平均保证金: {summary.get('avg_margin', 0.0):.2f}",
    ]
    return "\n".join(lines)


@tool(args_schema=OptionStrategyBacktestRequest)
def run_option_strategy_backtest(
    symbol: str,
    strategy: str,
    start_date: str | None = None,
    end_date: str | None = None,
    time_expr: str | None = None,
    lookback_days: int | None = None,
    strike_mode: str = "ATM",
    manual_params: dict | None = None,
    lots: int = 1,
    extra_margin_rate: float = 0.0,
    fee_per_lot: float = 2.0,
    calendar_type: str = "卖近买远(认购)",
):
    """
    ETF期权策略回测（任意时间段）：
    - 时间优先级：start/end > time_expr > lookback_days > 默认365天。
    - 支持常见时间表达：近N天/周/月/年、今年、去年、YTD、YYYYQx、YYYY-MM、YYYY年MM月。
    """
    strat = _normalize_strategy(strategy)
    if not strat:
        return "⚠️ 无法识别 strategy，请使用：双卖/双买/牛市价差/熊市价差/单买认购/单卖认沽等。"

    underlying = _normalize_etf_underlying(symbol)
    anchor_date = _latest_trade_day_for_underlying(underlying)

    try:
        window = resolve_window(
            start_date=start_date,
            end_date=end_date,
            time_expr=time_expr,
            lookback_days=lookback_days,
            anchor_date=anchor_date,
        )
    except Exception as exc:
        return f"⚠️ 时间参数错误: {exc}"

    effective_start_date = align_to_prev_trade_day(window.start_date, underlying=underlying)
    effective_end_date = align_to_prev_trade_day(window.end_date, underlying=underlying)

    notes = []
    if window.note:
        notes.append(window.note)
    if effective_start_date != window.start_date:
        notes.append(f"开始日为非交易日，已前对齐至 {effective_start_date}")
    if effective_end_date != window.end_date:
        notes.append(f"结束日为非交易日，已前对齐至 {effective_end_date}")
    time_resolution_note = "；".join(notes)

    if effective_start_date > effective_end_date:
        return "⚠️ 生效日期区间无效（开始日晚于结束日），请检查时间参数。"

    if strat in {"max_oi_call", "max_oi_put"}:
        result = run_max_oi_backtest(
            symbol=symbol,
            option_type="C" if strat == "max_oi_call" else "P",
            start_date=effective_start_date,
            end_date=effective_end_date,
            fee_rate=0.0003,
        )
    elif strat in ETF_STRATEGIES:
        # deep_otm_put 兼容映射为 single_put + OTM10
        final_strike_mode = _normalize_strike_mode(strike_mode, strategy=strat)
        if _norm_key(strategy) in {"deepotmput", "deep_otm_put"}:
            final_strike_mode = "OTM10"
        result = run_etf_roll_backtest(
            underlying=symbol,
            strategy=strat,
            start_date=effective_start_date,
            end_date=effective_end_date,
            fee_per_lot=fee_per_lot,
            margin_rate=0.15 + float(extra_margin_rate or 0.0),
            strike_mode=final_strike_mode,
            manual_params=_normalize_manual_params(manual_params),
            lots=int(lots),
            calendar_type=_normalize_calendar_type(calendar_type),
        )
    else:
        return "⚠️ 当前工具首版仅支持 ETF 全策略与 max_oi_call/max_oi_put。"

    return _format_result(
        result=result,
        strategy=strat,
        effective_start_date=effective_start_date,
        effective_end_date=effective_end_date,
        requested_time_expr=window.requested_time_expr,
        time_resolution_note=time_resolution_note,
    )


@tool(args_schema=BacktestRequest)
def run_option_backtest(
    symbol: str,
    strategy: str,
    start_date: str = None,
    end_date: str = None,
    fee_rate: float = 0.0003,
    fee_per_lot: float = 2.0,
):
    """
    兼容旧接口：内部转调 run_option_strategy_backtest。
    """
    strat = _normalize_strategy(strategy)
    if not strat:
        return "⚠️ strategy 无法识别"

    # 旧参数中 fee_rate 仅用于 max_oi_*，保持兼容
    if strat in {"max_oi_call", "max_oi_put"}:
        result = run_max_oi_backtest(
            symbol=symbol,
            option_type="C" if strat == "max_oi_call" else "P",
            start_date=start_date,
            end_date=end_date,
            fee_rate=fee_rate,
        )
        return _format_result(
            result=result,
            strategy=strat,
            effective_start_date=start_date or "",
            effective_end_date=end_date or "",
            requested_time_expr=None,
            time_resolution_note="兼容旧接口",
        )

    # deep_otm_put 旧语义：自动映射为 single_put + OTM10
    mapped_strategy = "single_put" if _norm_key(strategy) in {"deepotmput", "deep_otm_put"} else strat
    mapped_strike = "OTM10" if mapped_strategy == "single_put" and _norm_key(strategy) in {"deepotmput", "deep_otm_put"} else "ATM"
    return run_option_strategy_backtest.invoke(
        {
            "symbol": symbol,
            "strategy": mapped_strategy,
            "start_date": start_date,
            "end_date": end_date,
            "strike_mode": mapped_strike,
            "fee_per_lot": fee_per_lot,
        }
    )
