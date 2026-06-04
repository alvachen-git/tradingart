from __future__ import annotations

import argparse
import os
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

import pandas as pd
import tushare as ts
from dotenv import load_dotenv
from sqlalchemy import create_engine, text


DEFAULT_SECTOR_TYPE = "行业"
DEFAULT_SECTOR_LOOKBACK_DAYS = 180
DEFAULT_STEPS = ("sector_flow", "sector_ohlc", "stock_moneyflow", "stock_score")


@dataclass(frozen=True)
class CoverageRule:
    table_name: str
    date_column: str
    min_rows: int
    where_sql: str = ""


@dataclass
class CoverageReport:
    name: str
    total_trade_dates: int
    covered_dates: int
    missing_dates: List[str]
    min_date: Optional[str]
    max_date: Optional[str]
    min_rows: int

    @property
    def complete(self) -> bool:
        return not self.missing_dates


def normalize_date(value: str, label: str = "date") -> str:
    cleaned = str(value or "").strip().replace("-", "")
    if len(cleaned) != 8 or not cleaned.isdigit():
        raise ValueError(f"{label} 格式错误: {value}, 期望 YYYYMMDD")
    datetime.strptime(cleaned, "%Y%m%d")
    return cleaned


def infer_sector_ohlc_start_date(start_date: str, lookback_days: int = DEFAULT_SECTOR_LOOKBACK_DAYS) -> str:
    start = datetime.strptime(normalize_date(start_date, "start_date"), "%Y%m%d").date()
    return (start - timedelta(days=max(0, int(lookback_days)))).strftime("%Y%m%d")


def get_db_engine():
    load_dotenv(override=True)
    user = os.getenv("DB_USER")
    pwd = os.getenv("DB_PASSWORD")
    host = os.getenv("DB_HOST")
    port = os.getenv("DB_PORT", "3306")
    name = os.getenv("DB_NAME")
    if not all([user, pwd, host, name]):
        raise RuntimeError("数据库配置缺失，请先 source .env 或检查 DB_USER/DB_PASSWORD/DB_HOST/DB_NAME")
    return create_engine(f"mysql+pymysql://{user}:{pwd}@{host}:{port}/{name}", pool_pre_ping=True, pool_recycle=3600)


def get_tushare_pro():
    token = os.getenv("TUSHARE_TOKEN")
    if not token:
        return None
    ts.set_token(token)
    return ts.pro_api()


def _date_range_weekdays(start_date: str, end_date: str) -> List[str]:
    start = datetime.strptime(start_date, "%Y%m%d").date()
    end = datetime.strptime(end_date, "%Y%m%d").date()
    dates: List[str] = []
    current = start
    while current <= end:
        if current.weekday() < 5:
            dates.append(current.strftime("%Y%m%d"))
        current += timedelta(days=1)
    return dates


def get_trade_dates(engine, start_date: str, end_date: str) -> List[str]:
    start_date = normalize_date(start_date, "start_date")
    end_date = normalize_date(end_date, "end_date")
    if start_date > end_date:
        raise ValueError("start_date 不能晚于 end_date")

    pro = get_tushare_pro()
    if pro is not None:
        try:
            cal = pro.trade_cal(exchange="SSE", start_date=start_date, end_date=end_date)
            if cal is not None and not cal.empty:
                cal["is_open"] = pd.to_numeric(cal["is_open"], errors="coerce").fillna(0).astype(int)
                dates = cal[cal["is_open"] == 1]["cal_date"].astype(str).sort_values().tolist()
                if dates:
                    return dates
        except Exception as exc:
            print(f"⚠️ Tushare 交易日历获取失败，尝试使用本地 index_price/stock_price 回退: {exc}")

    for table_name in ("index_price", "stock_price"):
        try:
            with engine.connect() as conn:
                rows = conn.execute(
                    text(
                        f"""
                        SELECT DISTINCT trade_date
                        FROM {table_name}
                        WHERE trade_date >= :start_date AND trade_date <= :end_date
                        ORDER BY trade_date
                        """
                    ),
                    {"start_date": start_date, "end_date": end_date},
                ).fetchall()
            dates = [str(r[0]).replace("-", "")[:8] for r in rows if r and r[0]]
            if dates:
                return sorted(set(dates))
        except Exception:
            continue

    return _date_range_weekdays(start_date, end_date)


def _fetch_date_counts(engine, rule: CoverageRule, start_date: str, end_date: str) -> Tuple[Dict[str, int], Optional[str], Optional[str]]:
    where_extra = f" AND {rule.where_sql}" if rule.where_sql else ""
    sql = text(
        f"""
        SELECT {rule.date_column} AS trade_date, COUNT(*) AS row_count
        FROM {rule.table_name}
        WHERE {rule.date_column} >= :start_date
          AND {rule.date_column} <= :end_date
          {where_extra}
        GROUP BY {rule.date_column}
        ORDER BY {rule.date_column}
        """
    )
    with engine.connect() as conn:
        rows = conn.execute(sql, {"start_date": start_date, "end_date": end_date}).fetchall()
    counts: Dict[str, int] = {}
    for row in rows:
        trade_date = str(row[0]).replace("-", "")[:8]
        counts[trade_date] = int(row[1] or 0)
    if not counts:
        return counts, None, None
    return counts, min(counts), max(counts)


def find_undercovered_dates(trade_dates: Sequence[str], counts: Dict[str, int], min_rows: int, mode: str) -> List[str]:
    if mode == "all":
        return list(trade_dates)
    return [d for d in trade_dates if int(counts.get(d, 0)) < int(min_rows)]


def build_coverage_report(
    engine,
    name: str,
    rule: CoverageRule,
    trade_dates: Sequence[str],
    start_date: str,
    end_date: str,
    mode: str,
) -> CoverageReport:
    counts, min_date, max_date = _fetch_date_counts(engine, rule, start_date, end_date)
    missing = find_undercovered_dates(trade_dates, counts, rule.min_rows, mode=mode)
    return CoverageReport(
        name=name,
        total_trade_dates=len(trade_dates),
        covered_dates=len(trade_dates) - len(missing),
        missing_dates=missing,
        min_date=min_date,
        max_date=max_date,
        min_rows=rule.min_rows,
    )


def print_coverage_report(report: CoverageReport, sample_size: int = 8) -> None:
    status = "OK" if report.complete else "MISSING"
    sample = ", ".join(report.missing_dates[:sample_size])
    if len(report.missing_dates) > sample_size:
        sample += ", ..."
    print(
        f"   {status:<7} {report.name:<22} 覆盖 {report.covered_dates}/{report.total_trade_dates} "
        f"| 库内范围 {report.min_date or '-'} -> {report.max_date or '-'} | 最小行数 {report.min_rows}"
    )
    if report.missing_dates:
        print(f"           缺口样例: {sample}")


def _run_sector_flow(dates: Iterable[str], sleep_sec: float, dry_run: bool) -> None:
    import update_sector_flow as sector_flow

    dates = list(dates)
    if dry_run:
        print(f"   dry-run: 将补 sector_moneyflow {len(dates)} 个交易日")
        return
    sector_flow.ensure_sector_moneyflow_schema()
    for idx, trade_date in enumerate(dates, start=1):
        print(f"   [{idx}/{len(dates)}] sector_moneyflow {trade_date}")
        sector_flow.fetch_sector_moneyflow(trade_date)
        if sleep_sec > 0:
            time.sleep(sleep_sec)


def _run_sector_ohlc(start_date: str, end_date: str, sector_type: str, sleep_sec: float, dry_run: bool) -> None:
    import update_sector_index_price as sector_price

    print(f"   sector_index_price {start_date} -> {end_date} | sector_type={sector_type}")
    result = sector_price.run_update(
        start_date=start_date,
        end_date=end_date,
        sector_type=sector_type,
        dry_run=dry_run,
        trade_date_for_sectors=end_date,
        daily_interval_sec=max(0.0, float(sleep_sec)),
    )
    print(f"   板块OHLC结果: {result}")
    if result.get("price_errors"):
        raise RuntimeError(f"sector_index_price 存在拉取错误: {result.get('price_errors')[:5]}")


def _run_stock_moneyflow(dates: Iterable[str], sleep_sec: float, dry_run: bool) -> None:
    import update_stock_moneyflow_daily as stock_mf

    dates = list(dates)
    if dry_run:
        print(f"   dry-run: 将补 stock_moneyflow_daily {len(dates)} 个交易日")
        return
    for idx, trade_date in enumerate(dates, start=1):
        print(f"   [{idx}/{len(dates)}] stock_moneyflow_daily {trade_date}")
        stock_mf.run_update(trade_date=trade_date, dry_run=False)
        if sleep_sec > 0:
            time.sleep(sleep_sec)


def _run_stock_score(
    start_date: str,
    end_date: str,
    mode: str,
    max_stocks: int,
    retries: int,
    sleep_sec: float,
    dry_run: bool,
) -> None:
    import backfill_stock_score as stock_score

    print(f"   daily_stock_screener {start_date} -> {end_date} | mode={mode}")
    stock_score.run_backfill(
        start_date=start_date,
        end_date=end_date,
        mode=mode,
        max_stocks=max(0, int(max_stocks)),
        retries=max(0, int(retries)),
        sleep_sec=max(0.0, float(sleep_sec)),
        dry_run=dry_run,
    )


def parse_steps(raw_steps: str) -> List[str]:
    if not raw_steps or raw_steps.strip().lower() == "all":
        return list(DEFAULT_STEPS)
    steps = [x.strip() for x in raw_steps.split(",") if x.strip()]
    unknown = [x for x in steps if x not in DEFAULT_STEPS]
    if unknown:
        raise ValueError(f"未知步骤: {unknown}, 可选: {', '.join(DEFAULT_STEPS)}")
    return steps


def describe_dry_run_step(step: str, missing_dates: Sequence[str], sector_ohlc_start: str, end_date: str) -> str:
    if step == "sector_ohlc":
        return f"dry-run: 将补 sector_index_price 区间 {sector_ohlc_start} -> {end_date}"
    return f"dry-run: 将补 {step} {len(missing_dates)} 个交易日"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="选股3号回测依赖数据统一检查与回填")
    parser.add_argument("--start-date", required=True, help="回测开始日期 YYYYMMDD")
    parser.add_argument("--end-date", required=True, help="回测结束日期 YYYYMMDD")
    parser.add_argument(
        "--sector-ohlc-start-date",
        default="",
        help="板块OHLC起始日期；为空时自动用 start_date 往前推 180 个自然日",
    )
    parser.add_argument("--sector-type", default=DEFAULT_SECTOR_TYPE, help="板块类型，默认 行业")
    parser.add_argument("--mode", choices=["missing", "all"], default="missing", help="missing=只补缺口, all=全部重算")
    parser.add_argument("--steps", default="all", help="逗号分隔步骤，默认 all；可选 sector_flow,sector_ohlc,stock_moneyflow,stock_score")
    parser.add_argument("--dry-run", action="store_true", help="只检查覆盖率和打印计划，不写库、不调用外部采集")
    parser.add_argument("--date-sleep", type=float, default=0.5, help="按交易日补数据时的间隔秒数")
    parser.add_argument("--sector-ohlc-sleep", type=float, default=0.36, help="板块OHLC单板块请求间隔秒数")
    parser.add_argument("--stock-score-sleep", type=float, default=0.015, help="股票评分单股票请求间隔秒数")
    parser.add_argument("--stock-score-retries", type=int, default=2, help="股票评分单股重试次数")
    parser.add_argument("--max-stock-score-stocks", type=int, default=0, help="测试用：每个交易日最多处理 N 只股票")
    parser.add_argument("--min-sector-flow-rows", type=int, default=50)
    parser.add_argument("--min-sector-price-rows", type=int, default=50)
    parser.add_argument("--min-stock-moneyflow-rows", type=int, default=1000)
    parser.add_argument("--min-stock-score-rows", type=int, default=1000)
    return parser


def main() -> int:
    load_dotenv(override=True)
    args = build_parser().parse_args()
    start_date = normalize_date(args.start_date, "start_date")
    end_date = normalize_date(args.end_date, "end_date")
    if start_date > end_date:
        raise ValueError("start_date 不能晚于 end_date")
    sector_ohlc_start = normalize_date(args.sector_ohlc_start_date, "sector_ohlc_start_date") if args.sector_ohlc_start_date else infer_sector_ohlc_start_date(start_date)
    steps = parse_steps(args.steps)

    engine = get_db_engine()
    trade_dates = get_trade_dates(engine, start_date, end_date)
    sector_trade_dates = get_trade_dates(engine, sector_ohlc_start, end_date)

    print("========================================")
    print("选股3号数据回填总控")
    print(f"回测区间: {start_date} -> {end_date} | 交易日: {len(trade_dates)}")
    print(f"板块OHLC区间: {sector_ohlc_start} -> {end_date} | 交易日: {len(sector_trade_dates)}")
    print(f"模式: {args.mode} | dry_run={bool(args.dry_run)} | steps={','.join(steps)}")
    print("========================================")

    rules = {
        "sector_flow": CoverageRule(
            table_name="sector_moneyflow",
            date_column="trade_date",
            min_rows=int(args.min_sector_flow_rows),
            where_sql="sector_type = '行业'",
        ),
        "sector_ohlc": CoverageRule(
            table_name="sector_index_price",
            date_column="trade_date",
            min_rows=int(args.min_sector_price_rows),
            where_sql="sector_type = '行业'",
        ),
        "stock_moneyflow": CoverageRule(
            table_name="stock_moneyflow_daily",
            date_column="trade_date",
            min_rows=int(args.min_stock_moneyflow_rows),
        ),
        "stock_score": CoverageRule(
            table_name="daily_stock_screener",
            date_column="trade_date",
            min_rows=int(args.min_stock_score_rows),
        ),
    }

    reports: Dict[str, CoverageReport] = {}
    print("\n覆盖率检查:")
    for step in steps:
        report_dates = sector_trade_dates if step == "sector_ohlc" else trade_dates
        report_start = sector_ohlc_start if step == "sector_ohlc" else start_date
        reports[step] = build_coverage_report(
            engine=engine,
            name=step,
            rule=rules[step],
            trade_dates=report_dates,
            start_date=report_start,
            end_date=end_date,
            mode=args.mode,
        )
        print_coverage_report(reports[step])

    print("\n执行计划:")
    for step in steps:
        missing = reports[step].missing_dates
        if not missing:
            print(f"   {step}: 已完整，跳过")
            continue
        if args.dry_run:
            print(f"   {describe_dry_run_step(step, missing, sector_ohlc_start, end_date)}")
            continue
        if step == "sector_flow":
            _run_sector_flow(missing, sleep_sec=float(args.date_sleep), dry_run=False)
        elif step == "sector_ohlc":
            _run_sector_ohlc(
                start_date=sector_ohlc_start,
                end_date=end_date,
                sector_type=str(args.sector_type or DEFAULT_SECTOR_TYPE).strip() or DEFAULT_SECTOR_TYPE,
                sleep_sec=float(args.sector_ohlc_sleep),
                dry_run=False,
            )
        elif step == "stock_moneyflow":
            _run_stock_moneyflow(missing, sleep_sec=float(args.date_sleep), dry_run=False)
        elif step == "stock_score":
            _run_stock_score(
                start_date=start_date,
                end_date=end_date,
                mode=args.mode,
                max_stocks=int(args.max_stock_score_stocks),
                retries=int(args.stock_score_retries),
                sleep_sec=float(args.stock_score_sleep),
                dry_run=False,
            )

    print("\n✅ 总控脚本结束")
    if args.dry_run:
        print("   dry-run 只做检查和计划，未写入数据库。去掉 --dry-run 后才会正式补数据。")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
