import argparse
import os
import time
from collections import Counter, defaultdict
from datetime import datetime
from typing import Dict, List, Optional, Tuple

import pandas as pd
import tushare as ts
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

import kline_algo


def _build_engine():
    db_url = (
        f"mysql+pymysql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}"
        f"@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
    )
    return create_engine(db_url, pool_recycle=3600, pool_pre_ping=True)


def _fetch_daily_with_retry(pro, ts_code: str, end_date: str, limit: int = 100, retries: int = 2):
    last_err = None
    for i in range(retries + 1):
        try:
            return pro.daily(ts_code=ts_code, end_date=end_date, limit=limit)
        except Exception as e:
            last_err = e
            time.sleep(0.25 * (i + 1))
    raise last_err


def _prepare_kline_df(df: pd.DataFrame) -> pd.DataFrame:
    out = df.rename(
        columns={
            "close": "close_price",
            "open": "open_price",
            "high": "high_price",
            "low": "low_price",
            "vol": "volume",
        }
    )
    out = out.sort_values("trade_date").reset_index(drop=True)
    return out


def _build_record(row: pd.Series, latest: pd.Series, result: Dict) -> Dict:
    return {
        "trade_date": str(latest["trade_date"]),
        "ts_code": row["ts_code"],
        "name": row.get("name"),
        "industry": row.get("industry"),
        "close": float(latest["close_price"]),
        "pct_chg": float(latest["pct_chg"]) if pd.notna(latest.get("pct_chg")) else None,
        "ma_trend": ",".join(result.get("trends", [])),
        "pattern": ",".join(result.get("patterns", [])),
        "score": int(result.get("score", 50)),
        "ai_summary": f"趋势:{','.join(result.get('trends', []))}。形态:{','.join(result.get('patterns', []))}。",
    }


def _upsert_records(engine, records: List[Dict]) -> Tuple[int, List[str]]:
    if not records:
        return 0, []
    sql = text(
        """
        REPLACE INTO daily_stock_screener
        (trade_date, ts_code, name, industry, close, pct_chg, ma_trend, pattern, score, ai_summary)
        VALUES
        (:trade_date, :ts_code, :name, :industry, :close, :pct_chg, :ma_trend, :pattern, :score, :ai_summary)
        """
    )
    try:
        with engine.begin() as conn:
            conn.execute(sql, records)
        return len(records), []
    except Exception as batch_exc:
        print(f"   ⚠️ 批量写入失败，自动降级为逐条写入: {batch_exc}")

    written = 0
    write_errors: List[str] = []
    for row in records:
        try:
            with engine.begin() as conn:
                conn.execute(sql, row)
            written += 1
        except Exception as exc:
            if len(write_errors) < 8:
                write_errors.append(f"{row.get('ts_code')}: {exc}")
    return written, write_errors


def _add_skip(
    skip_stats: Counter,
    skip_samples: Dict[str, List[str]],
    reason: str,
    ts_code: str,
    detail: str = "",
) -> None:
    skip_stats[reason] += 1
    samples = skip_samples[reason]
    if len(samples) < 5:
        samples.append(f"{ts_code}{' | ' + detail if detail else ''}")


def _print_skip_summary(skip_stats: Counter, skip_samples: Dict[str, List[str]], indent: str = "   ") -> None:
    if not skip_stats:
        return
    reason_names = {
        "empty_daily": "Tushare无日线",
        "insufficient_kline": "K线不足30根",
        "not_trade_date": "当天无交易/最新K线非当日",
        "calc_error": "计算异常",
        "write_error": "写库异常",
    }
    parts = [f"{reason_names.get(k, k)} {v}" for k, v in skip_stats.most_common()]
    print(f"{indent}↪ 跳过统计: " + "；".join(parts))
    for reason, samples in skip_samples.items():
        if samples:
            print(f"{indent}   - {reason_names.get(reason, reason)}样例: " + "；".join(samples))


def _get_trade_dates(pro, start_date: str, end_date: str) -> List[str]:
    cal = pro.trade_cal(exchange="SSE", start_date=start_date, end_date=end_date)
    if cal is None or cal.empty:
        return []
    cal["is_open"] = pd.to_numeric(cal["is_open"], errors="coerce").fillna(0).astype(int)
    dates = cal[cal["is_open"] == 1]["cal_date"].astype(str).tolist()
    return sorted(dates)


def _get_existing_codes(engine, trade_date: str) -> set:
    with engine.connect() as conn:
        df = pd.read_sql(
            text("SELECT ts_code FROM daily_stock_screener WHERE trade_date = :d"),
            conn,
            params={"d": trade_date},
        )
    if df.empty:
        return set()
    return set(df["ts_code"].astype(str).tolist())


def run_backfill(
    start_date: str,
    end_date: str,
    mode: str = "missing",
    max_stocks: int = 0,
    retries: int = 2,
    sleep_sec: float = 0.015,
    dry_run: bool = False,
):
    load_dotenv(override=True)
    ts.set_token(os.getenv("TUSHARE_TOKEN"))
    pro = ts.pro_api()
    engine = _build_engine()

    stock_list = pro.stock_basic(exchange="", list_status="L", fields="ts_code,name,industry")
    if stock_list is None or stock_list.empty:
        print("❌ 未获取到股票列表")
        return

    trade_dates = _get_trade_dates(pro, start_date, end_date)
    if not trade_dates:
        print("❌ 区间内无交易日")
        return

    print(f"🎯 回补区间: {start_date} ~ {end_date} | 交易日数: {len(trade_dates)} | 模式: {mode}")
    print(f"📋 股票池: {len(stock_list)} 只 | dry_run={dry_run}")

    total_written = 0
    total_attempt = 0
    total_error = 0
    total_skip_stats: Counter = Counter()

    for d in trade_dates:
        print(f"\n📆 处理交易日 {d}")
        if mode == "all":
            candidates = stock_list.copy()
            if not dry_run:
                with engine.begin() as conn:
                    conn.execute(text("DELETE FROM daily_stock_screener WHERE trade_date = :d"), {"d": d})
                print("   🗑️ 已清理当日历史评分，准备全量重算")
        else:
            existing = _get_existing_codes(engine, d)
            candidates = stock_list[~stock_list["ts_code"].isin(existing)].copy()
            print(f"   ℹ️ 现有 {len(existing)} 条，待补 {len(candidates)} 条")

        if max_stocks > 0:
            candidates = candidates.head(max_stocks)
            print(f"   ⚠️ 测试模式，仅处理前 {len(candidates)} 只")

        records: List[Dict] = []
        error_samples: List[str] = []
        skip_stats: Counter = Counter()
        skip_samples: Dict[str, List[str]] = defaultdict(list)

        for i, (_, row) in enumerate(candidates.iterrows(), start=1):
            ts_code = row["ts_code"]
            try:
                df = _fetch_daily_with_retry(pro, ts_code=ts_code, end_date=d, limit=100, retries=retries)
                if df is None or df.empty:
                    _add_skip(skip_stats, skip_samples, "empty_daily", ts_code)
                    continue
                if len(df) < 30:
                    latest_trade_date = str(df.iloc[0].get("trade_date", "")) if not df.empty else "-"
                    _add_skip(skip_stats, skip_samples, "insufficient_kline", ts_code, f"rows={len(df)}, latest={latest_trade_date}")
                    continue
                kdf = _prepare_kline_df(df)
                latest = kdf.iloc[-1]
                if str(latest["trade_date"]) != d:
                    _add_skip(skip_stats, skip_samples, "not_trade_date", ts_code, f"latest={latest['trade_date']}")
                    continue
                result = kline_algo.calculate_kline_signals(kdf)
                records.append(_build_record(row, latest, result))
                total_attempt += 1
            except Exception as e:
                total_error += 1
                _add_skip(skip_stats, skip_samples, "calc_error", ts_code, str(e)[:120])
                if len(error_samples) < 8:
                    error_samples.append(f"{ts_code}: {e}")
            if i % 200 == 0:
                print(f"   ...进度 {i}/{len(candidates)}")
            if sleep_sec > 0:
                time.sleep(sleep_sec)

        if dry_run:
            print(f"   ✅ dry_run: 预计写入 {len(records)} 条")
        else:
            written, write_errors = _upsert_records(engine, records)
            if write_errors:
                skip_stats["write_error"] += len(write_errors)
                total_error += len(write_errors)
                skip_samples["write_error"].extend(write_errors[:5])
            total_written += written
            print(f"   ✅ 当日写入 {written} 条")

        total_skip_stats.update(skip_stats)
        _print_skip_summary(skip_stats, skip_samples)

        if error_samples:
            print("   ⚠️ 失败样例:")
            for msg in error_samples:
                print(f"      - {msg}")

    print("\n========================================")
    print(f"🏁 回补结束 | 实际写入: {total_written} | 计算记录: {total_attempt} | 错误数: {total_error}")
    if total_skip_stats:
        print("📌 全局跳过统计: " + "；".join([f"{k}={v}" for k, v in total_skip_stats.most_common()]))
    print("========================================")


def _parse_args():
    p = argparse.ArgumentParser(description="回补 daily_stock_screener 技术评分")
    p.add_argument("--start-date", required=True, help="开始日期 YYYYMMDD")
    p.add_argument("--end-date", required=True, help="结束日期 YYYYMMDD")
    p.add_argument(
        "--mode",
        choices=["missing", "all"],
        default="missing",
        help="missing=只补缺失, all=清空当天后重算",
    )
    p.add_argument("--max-stocks", type=int, default=0, help="仅处理前 N 只（测试用）")
    p.add_argument("--retries", type=int, default=2, help="单股行情请求重试次数")
    p.add_argument("--sleep-sec", type=float, default=0.015, help="每股请求间隔秒数")
    p.add_argument("--dry-run", action="store_true", help="只计算不写库")
    return p.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    run_backfill(
        start_date=args.start_date,
        end_date=args.end_date,
        mode=args.mode,
        max_stocks=args.max_stocks,
        retries=args.retries,
        sleep_sec=args.sleep_sec,
        dry_run=args.dry_run,
    )
