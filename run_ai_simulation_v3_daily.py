from __future__ import annotations

import argparse
from datetime import datetime, timedelta
from typing import Any, Dict

import ai_simulation_service as sim
import update_stock_price_qfq as qfq


def _clean_date(value: str) -> str:
    return qfq.clean_date(value, "trade-date")


def _lookback_start(trade_date: str, calendar_days: int) -> str:
    end = datetime.strptime(_clean_date(trade_date), "%Y%m%d").date()
    return (end - timedelta(days=max(1, int(calendar_days)))).strftime("%Y%m%d")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Prepare qfq prices and run official AI stock v3 daily settlement.")
    parser.add_argument("--trade-date", default="", help="YYYYMMDD；为空时使用 stock_price 最新交易日")
    parser.add_argument("--decision-mode", choices=["llm", "llm_fallback", "rule"], default="llm_fallback")
    parser.add_argument("--force", action="store_true", help="覆盖已存在的当日3号结果")
    parser.add_argument("--allow-rewind", action="store_true", help="允许重跑旧日期时删除该日及之后3号结果")
    parser.add_argument("--no-review", dest="generate_review", action="store_false", default=True)
    parser.add_argument("--prepare-only", action="store_true", help="只更新 stock_price_qfq，不执行3号日结")
    parser.add_argument("--run-only", action="store_true", help="只执行3号日结，不更新 stock_price_qfq")
    parser.add_argument("--dry-run", action="store_true", help="只演练 qfq 更新；不会写 qfq，也不会执行日结")
    parser.add_argument("--strict-qfq", action="store_true", help="任意 qfq 预备代码失败都中断日结")
    parser.add_argument("--qfq-lookback-days", type=int, default=420, help="前复权重算自然日窗口，默认约覆盖一年多")
    parser.add_argument("--candidate-limit", type=int, default=800, help="3号当日预备候选代码数量")
    parser.add_argument("--sleep-sec", type=float, default=0.05, help="逐代码补 qfq 的间隔秒数")
    return parser


def _prepare_qfq(args: argparse.Namespace, trade_date: str) -> Dict[str, Any]:
    start_date = _lookback_start(trade_date, int(args.qfq_lookback_days))
    results = qfq.run_update(
        start_date=start_date,
        end_date=trade_date,
        portfolio_id=sim.OFFICIAL_PORTFOLIO_3_ID,
        v3_daily_candidates=True,
        candidate_date=trade_date,
        candidate_limit=int(args.candidate_limit),
        dry_run=bool(args.dry_run),
        sleep_sec=float(args.sleep_sec),
    )
    failed = [r for r in results if not r.ok]
    return {
        "start_date": start_date,
        "end_date": trade_date,
        "total": len(results),
        "ok": len(results) - len(failed),
        "failed": failed,
    }


def main() -> int:
    args = build_parser().parse_args()
    if args.prepare_only and args.run_only:
        raise SystemExit("--prepare-only 和 --run-only 不能同时使用")

    trade_date = _clean_date(args.trade_date) if args.trade_date else sim._normalize_trade_date(None)
    print(f"选股3号每日任务 | trade_date={trade_date} | decision_mode={args.decision_mode}")

    if not args.run_only:
        prep = _prepare_qfq(args, trade_date)
        print(f"前复权准备: {prep['ok']}/{prep['total']} OK | 区间 {prep['start_date']} -> {prep['end_date']}")
        if prep["failed"]:
            for item in prep["failed"][:20]:
                print(f"  qfq失败: {item.ts_code} error={item.error}")
            if args.strict_qfq or prep["ok"] <= 0:
                return 2
            print("  qfq存在非关键缺口：继续执行，缺价股票会在3号日结中被确定性风控跳过。")
        if args.prepare_only or args.dry_run:
            return 0

    result = sim.run_daily_simulation(
        trade_date=trade_date,
        portfolio_id=sim.OFFICIAL_PORTFOLIO_3_ID,
        force=bool(args.force),
        allow_rewind=bool(args.allow_rewind),
        generate_review=bool(args.generate_review),
        save_watchlist=True,
        decision_mode=str(args.decision_mode),
    )
    print(
        "3号日结结果: "
        f"status={result.get('status')} nav={result.get('nav')} "
        f"trades={result.get('trade_count')} warning={result.get('ai_warning', '')} "
        f"reason={result.get('reason', '')} error={result.get('error', '')}"
    )
    return 0 if result.get("status") in {"success", "skipped"} else 1


if __name__ == "__main__":
    raise SystemExit(main())
