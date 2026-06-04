import argparse
import json
import math
import re
from pathlib import Path
from typing import Any, Dict, Optional

import pandas as pd
from sqlalchemy import text

import ai_simulation_service as sim


def _clean_date(value: str) -> str:
    cleaned = re.sub(r"[^0-9]", "", str(value or ""))[:8]
    if len(cleaned) != 8:
        raise argparse.ArgumentTypeError("date must look like YYYYMMDD")
    return cleaned


def _portfolio_id(run_id: str) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9_-]+", "_", str(run_id or "").strip()).strip("_")
    if not cleaned:
        raise argparse.ArgumentTypeError("run-id cannot be empty")
    if cleaned.startswith(sim.BACKTEST_V3_PREFIX):
        return cleaned
    return f"{sim.BACKTEST_V3_PREFIX}{cleaned}"


def _config_overrides(args: argparse.Namespace) -> Dict[str, Any]:
    mapping = {
        "max_positions": args.max_positions,
        "max_daily_trades": args.max_daily_trades,
        "max_single_weight_soft": args.single_soft_cap,
        "max_single_weight_hard": args.single_hard_cap,
        "v3_strong_budget": args.strong_budget,
        "v3_range_budget": args.range_budget,
        "v3_bear_budget": args.bear_budget,
    }
    return {k: v for k, v in mapping.items() if v is not None}


def _load_nav(portfolio_id: str, start_date: str, end_date: str) -> pd.DataFrame:
    sql = text(
        """
        SELECT trade_date, cash, position_value, nav, daily_return, cum_return,
               max_drawdown, turnover, bench_hs300, alpha_vs_hs300
        FROM ai_sim_nav_daily
        WHERE portfolio_id = :pid
          AND trade_date >= :start_date
          AND trade_date <= :end_date
        ORDER BY trade_date
        """
    )
    with sim.engine.connect() as conn:
        return pd.read_sql(sql, conn, params={"pid": portfolio_id, "start_date": start_date, "end_date": end_date})


def _benchmark_return(ts_code: str, start_date: str, end_date: str) -> float:
    sql = text(
        """
        SELECT trade_date, close_price
        FROM index_price
        WHERE ts_code = :ts_code
          AND trade_date >= :start_date
          AND trade_date <= :end_date
          AND close_price IS NOT NULL
          AND close_price > 0
        ORDER BY trade_date
        """
    )
    with sim.engine.connect() as conn:
        df = pd.read_sql(sql, conn, params={"ts_code": ts_code, "start_date": start_date, "end_date": end_date})
    if df.empty or len(df) < 2:
        return 0.0
    start = sim._to_float(df.iloc[0].get("close_price"), 0.0)
    end = sim._to_float(df.iloc[-1].get("close_price"), 0.0)
    return end / start - 1.0 if start > 0 and end > 0 else 0.0


def _trade_stats(portfolio_id: str, start_date: str, end_date: str) -> Dict[str, Any]:
    sql = text(
        """
        SELECT *
        FROM ai_sim_trades
        WHERE portfolio_id = :pid
          AND trade_date >= :start_date
          AND trade_date <= :end_date
        ORDER BY trade_date, created_at, id
        """
    )
    with sim.engine.connect() as conn:
        trades = pd.read_sql(sql, conn, params={"pid": portfolio_id, "start_date": start_date, "end_date": end_date})
    if trades.empty:
        return {"trade_count": 0, "sell_count": 0, "win_rate": None}

    trades = sim._recompute_realized_pnl_for_trades(trades)
    sells = trades[trades["side"].astype(str).str.lower() == "sell"].copy()
    if sells.empty:
        win_rate = None
    else:
        pnl = pd.to_numeric(sells["realized_pnl"], errors="coerce").fillna(0.0)
        win_rate = float((pnl > 0).mean())
    return {"trade_count": int(len(trades)), "sell_count": int(len(sells)), "win_rate": win_rate}


def _position_count(portfolio_id: str, end_date: str) -> int:
    df = sim.get_positions(portfolio_id, as_of_date=end_date, strict_as_of=False)
    return int(len(df)) if df is not None else 0


def _summarize(portfolio_id: str, start_date: str, end_date: str, days_run: int) -> Dict[str, Any]:
    nav = _load_nav(portfolio_id, start_date, end_date)
    if nav.empty:
        return {"portfolio_id": portfolio_id, "status": "empty"}

    nav["nav"] = pd.to_numeric(nav["nav"], errors="coerce")
    nav["turnover"] = pd.to_numeric(nav["turnover"], errors="coerce").fillna(0.0)
    first_nav = sim.INITIAL_CAPITAL
    last_nav = sim._to_float(nav.iloc[-1].get("nav"), first_nav)
    cum_return = last_nav / first_nav - 1.0 if first_nav > 0 else 0.0
    n = max(1, len(nav))
    annual_return = math.pow(max(last_nav / first_nav, 1e-9), 252.0 / n) - 1.0
    max_drawdown = float(pd.to_numeric(nav["max_drawdown"], errors="coerce").fillna(0.0).min())
    avg_turnover = float(nav["turnover"].mean())

    hs300_ret = _benchmark_return("000300.SH", start_date, end_date)
    star50_ret = _benchmark_return("000688.SH", start_date, end_date)
    equal_bench = (hs300_ret + star50_ret) / 2.0
    trade_stats = _trade_stats(portfolio_id, start_date, end_date)

    return {
        "portfolio_id": portfolio_id,
        "status": "success",
        "start_date": str(nav.iloc[0].get("trade_date")),
        "end_date": str(nav.iloc[-1].get("trade_date")),
        "days_requested": int(days_run),
        "days_settled": int(len(nav)),
        "final_nav": round(last_nav, 2),
        "cum_return": cum_return,
        "annual_return": annual_return,
        "max_drawdown": max_drawdown,
        "avg_turnover": avg_turnover,
        "trade_count": trade_stats["trade_count"],
        "sell_count": trade_stats["sell_count"],
        "win_rate": trade_stats["win_rate"],
        "position_count": _position_count(portfolio_id, str(nav.iloc[-1].get("trade_date"))),
        "hs300_return": hs300_ret,
        "star50_return": star50_ret,
        "equal_benchmark_return": equal_bench,
        "excess_vs_hs300": cum_return - hs300_ret,
        "excess_vs_star50": cum_return - star50_ret,
        "excess_vs_equal_benchmark": cum_return - equal_bench,
        "beat_equal_benchmark": bool(cum_return > equal_bench),
    }


def _fmt_pct(value: Optional[float]) -> str:
    if value is None:
        return "-"
    return f"{value:+.2%}"


def _print_summary(summary: Dict[str, Any]) -> None:
    print("\n=== 选股3号回测摘要 ===")
    for key in ["portfolio_id", "start_date", "end_date", "days_settled", "final_nav"]:
        print(f"{key}: {summary.get(key)}")
    print(f"累计收益: {_fmt_pct(summary.get('cum_return'))}")
    print(f"年化收益: {_fmt_pct(summary.get('annual_return'))}")
    print(f"最大回撤: {_fmt_pct(summary.get('max_drawdown'))}")
    print(f"平均换手: {_fmt_pct(summary.get('avg_turnover'))}")
    print(f"交易次数: {summary.get('trade_count')} | 卖出次数: {summary.get('sell_count')} | 胜率: {_fmt_pct(summary.get('win_rate'))}")
    print(f"期末持仓数: {summary.get('position_count')}")
    print(f"沪深300同期: {_fmt_pct(summary.get('hs300_return'))}")
    print(f"科创50同期: {_fmt_pct(summary.get('star50_return'))}")
    print(f"沪深300+科创50等权: {_fmt_pct(summary.get('equal_benchmark_return'))}")
    print(f"相对等权基准超额: {_fmt_pct(summary.get('excess_vs_equal_benchmark'))}")
    print(f"是否跑赢等权基准: {'是' if summary.get('beat_equal_benchmark') else '否'}")


def _export_csv(path: str, portfolio_id: str, start_date: str, end_date: str, summary: Dict[str, Any]) -> None:
    export_path = Path(path).expanduser()
    export_path.parent.mkdir(parents=True, exist_ok=True)
    nav = _load_nav(portfolio_id, start_date, end_date)
    nav.to_csv(export_path, index=False)
    summary_path = export_path.with_name(f"{export_path.stem}.summary.csv")
    pd.DataFrame([summary]).to_csv(summary_path, index=False)
    print(f"已导出每日净值: {export_path}")
    print(f"已导出摘要: {summary_path}")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run AI stock v3 experimental backtest.")
    parser.add_argument("--start-date", required=True, type=_clean_date)
    parser.add_argument("--end-date", required=True, type=_clean_date)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--decision-mode", choices=["rule", "llm", "llm_fallback"], default="rule")
    parser.add_argument("--force", action="store_true", help="Delete existing rows for this run_id from start-date before running.")
    parser.add_argument("--review", dest="generate_review", action="store_true", default=False)
    parser.add_argument("--no-review", dest="generate_review", action="store_false")
    parser.add_argument("--save-watchlist", dest="save_watchlist", action="store_true", default=True)
    parser.add_argument("--no-watchlist", dest="save_watchlist", action="store_false")
    parser.add_argument("--max-positions", type=int)
    parser.add_argument("--max-daily-trades", type=int)
    parser.add_argument("--single-soft-cap", type=float)
    parser.add_argument("--single-hard-cap", type=float)
    parser.add_argument("--strong-budget", type=float)
    parser.add_argument("--range-budget", type=float)
    parser.add_argument("--bear-budget", type=float)
    parser.add_argument("--export-csv")
    return parser


def main() -> int:
    args = build_parser().parse_args()
    portfolio_id = _portfolio_id(args.run_id)
    overrides = _config_overrides(args)

    if args.force:
        sim._delete_from_day(portfolio_id, args.start_date)

    days = sim._get_trade_dates_between(args.start_date, args.end_date)
    if not days:
        print("没有可回测交易日。")
        return 2

    print(f"准备运行选股3号回测: {portfolio_id}")
    print(f"区间: {days[0]} -> {days[-1]} | 交易日: {len(days)} | decision_mode={args.decision_mode} | review={args.generate_review}")
    if overrides:
        print("参数覆盖:", json.dumps(overrides, ensure_ascii=False, sort_keys=True))

    for i, td in enumerate(days, 1):
        result = sim.run_daily_simulation(
            trade_date=td,
            portfolio_id=portfolio_id,
            force=args.force,
            allow_rewind=args.force,
            generate_review=bool(args.generate_review),
            save_watchlist=bool(args.save_watchlist),
            decision_mode=args.decision_mode,
            config_overrides=overrides,
        )
        print(
            f"[{i}/{len(days)}] {td} status={result.get('status')} nav={result.get('nav')} "
            f"trades={result.get('trade_count')} warning={result.get('ai_warning', '')} "
            f"reason={result.get('reason', '')} error={result.get('error', '')}",
            flush=True,
        )

    summary = _summarize(portfolio_id, days[0], days[-1], len(days))
    _print_summary(summary)
    if args.export_csv:
        _export_csv(args.export_csv, portfolio_id, days[0], days[-1], summary)
    return 0 if summary.get("status") == "success" else 1


if __name__ == "__main__":
    raise SystemExit(main())
