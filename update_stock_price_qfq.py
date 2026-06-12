from __future__ import annotations

import argparse
import os
import re
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Sequence

import pandas as pd
import tushare as ts
from dotenv import load_dotenv
from sqlalchemy import create_engine, text, types


TARGET_COLS = [
    "trade_date",
    "ts_code",
    "name",
    "open_price",
    "high_price",
    "low_price",
    "close_price",
    "pct_chg",
    "vol",
    "amount",
    "adj_factor",
    "anchor_trade_date",
    "source",
    "updated_at",
]


@dataclass
class UpdateResult:
    ts_code: str
    source: str = ""
    rows: int = 0
    dry_run: bool = False
    error: str = ""

    @property
    def ok(self) -> bool:
        return self.rows > 0 and not self.error


def clean_date(value: str, label: str = "date") -> str:
    cleaned = re.sub(r"[^0-9]", "", str(value or ""))[:8]
    if len(cleaned) != 8:
        raise argparse.ArgumentTypeError(f"{label} must look like YYYYMMDD")
    datetime.strptime(cleaned, "%Y%m%d")
    return cleaned


def normalize_symbol(symbol: str) -> str:
    raw = str(symbol or "").strip().upper().replace(" ", "")
    if not raw:
        return ""
    raw = raw.replace(".XSHG", ".SH").replace(".XSHE", ".SZ")
    m = re.match(r"^(\d{6})(?:\.(SH|SZ|BJ))?$", raw)
    if not m:
        return raw
    code, suffix = m.group(1), m.group(2)
    if suffix:
        return f"{code}.{suffix}"
    if code.startswith(("6", "5", "9")):
        return f"{code}.SH"
    if code.startswith(("0", "1", "2", "3")):
        return f"{code}.SZ"
    return f"{code}.BJ"


def is_a_share_symbol(symbol: str) -> bool:
    code = normalize_symbol(symbol).split(".")[0]
    return bool(re.match(r"^\d{6}$", code)) and not code.startswith(
        ("510", "511", "512", "513", "515", "516", "518", "588", "159")
    )


def get_engine():
    load_dotenv(override=True)
    db_user = os.getenv("DB_USER")
    db_password = os.getenv("DB_PASSWORD")
    db_host = os.getenv("DB_HOST")
    db_port = os.getenv("DB_PORT", "3306")
    db_name = os.getenv("DB_NAME")
    if not all([db_user, db_password, db_host, db_name]):
        raise RuntimeError("数据库配置缺失，请先 source .env 或检查 DB_USER/DB_PASSWORD/DB_HOST/DB_NAME")
    return create_engine(
        f"mysql+pymysql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}",
        pool_pre_ping=True,
        pool_recycle=3600,
    )


def get_pro():
    load_dotenv(override=True)
    token = os.getenv("TUSHARE_TOKEN")
    if not token:
        raise RuntimeError("缺少 TUSHARE_TOKEN")
    ts.set_token(token)
    return ts.pro_api()


def latest_stock_price_date(engine) -> str:
    with engine.connect() as conn:
        value = conn.execute(
            text(
                """
                SELECT MAX(trade_date)
                FROM stock_price
                WHERE close_price IS NOT NULL AND close_price > 0
                """
            )
        ).scalar()
    if not value:
        raise RuntimeError("stock_price 中找不到可用的最新交易日，无法解析 --date latest")
    return clean_date(str(value), "latest stock_price trade_date")


def ensure_stock_price_qfq_table(engine) -> None:
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS stock_price_qfq (
                    trade_date VARCHAR(8) NOT NULL,
                    ts_code VARCHAR(16) NOT NULL,
                    name VARCHAR(80) DEFAULT '',
                    open_price DOUBLE DEFAULT NULL,
                    high_price DOUBLE DEFAULT NULL,
                    low_price DOUBLE DEFAULT NULL,
                    close_price DOUBLE DEFAULT NULL,
                    pct_chg DOUBLE DEFAULT NULL,
                    vol DOUBLE DEFAULT NULL,
                    amount DOUBLE DEFAULT NULL,
                    adj_factor DOUBLE DEFAULT NULL,
                    anchor_trade_date VARCHAR(8) DEFAULT '',
                    source VARCHAR(32) DEFAULT 'tushare_adj_factor',
                    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (trade_date, ts_code),
                    KEY idx_stock_price_qfq_code_date (ts_code, trade_date)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
                """
            )
        )


def _read_raw_price(engine, ts_code: str, start_date: str, end_date: str) -> pd.DataFrame:
    sql = text(
        """
        SELECT trade_date, ts_code, name, open_price, high_price, low_price,
               close_price, pct_chg, vol, amount
        FROM stock_price
        WHERE ts_code = :ts_code
          AND trade_date >= :start_date
          AND trade_date <= :end_date
          AND close_price IS NOT NULL
          AND close_price > 0
        ORDER BY trade_date
        """
    )
    with engine.connect() as conn:
        return pd.read_sql(sql, conn, params={"ts_code": ts_code, "start_date": start_date, "end_date": end_date})


def _read_symbols_for_range(
    engine,
    start_date: str,
    end_date: str,
    portfolio_id: str = "",
    include_watchlist: bool = True,
) -> List[str]:
    if portfolio_id:
        queries = [
            text(
                """
                SELECT DISTINCT symbol AS ts_code
                FROM ai_sim_trades
                WHERE portfolio_id = :pid
                  AND trade_date >= :start_date
                  AND trade_date <= :end_date
                """
            ),
            text(
                """
                SELECT DISTINCT symbol AS ts_code
                FROM ai_sim_positions
                WHERE portfolio_id = :pid
                  AND trade_date >= :start_date
                  AND trade_date <= :end_date
                """
            ),
        ]
        if include_watchlist:
            queries.append(
                text(
                    """
                    SELECT DISTINCT symbol AS ts_code
                    FROM ai_sim_watchlist
                    WHERE portfolio_id = :pid
                    """
                )
            )
        rows = []
        with engine.connect() as conn:
            for sql in queries:
                try:
                    rows.extend(conn.execute(sql, {"pid": portfolio_id, "start_date": start_date, "end_date": end_date}).fetchall())
                except Exception as exc:
                    print(f"⚠️ 读取组合代码失败，跳过该来源: {exc}")
    else:
        sql = text(
            """
            SELECT DISTINCT ts_code
            FROM stock_price
            WHERE trade_date >= :start_date
              AND trade_date <= :end_date
              AND close_price IS NOT NULL
              AND close_price > 0
            """
        )
        with engine.connect() as conn:
            rows = conn.execute(sql, {"start_date": start_date, "end_date": end_date}).fetchall()
    return sorted({normalize_symbol(r[0]) for r in rows if r and normalize_symbol(r[0])})


def _latest_screener_date(engine, trade_date: str) -> str:
    sql = text(
        """
        SELECT MAX(trade_date)
        FROM daily_stock_screener
        WHERE trade_date <= :trade_date
        """
    )
    with engine.connect() as conn:
        value = conn.execute(sql, {"trade_date": trade_date}).scalar()
    return clean_date(str(value), "screener_date") if value else ""


def _read_v3_daily_candidate_symbols(engine, trade_date: str, limit: int = 800) -> List[str]:
    td = clean_date(trade_date, "candidate-date")
    screener_date = _latest_screener_date(engine, td)
    rows = []
    if screener_date:
        sql = text(
            """
            SELECT m.ts_code, MAX(m.main_net_amount) AS main_net_amount
            FROM stock_moneyflow_daily m
            JOIN daily_stock_screener s
              ON s.ts_code = m.ts_code
             AND s.trade_date = :screener_date
            WHERE m.trade_date = :trade_date
              AND m.main_net_amount > 0
            GROUP BY m.ts_code
            ORDER BY main_net_amount DESC
            LIMIT :limit
            """
        )
        with engine.connect() as conn:
            rows.extend(
                conn.execute(
                    sql,
                    {
                        "trade_date": td,
                        "screener_date": screener_date,
                        "limit": int(max(1, limit)),
                    },
                ).fetchall()
            )

    # Always include current v3 holdings/watchlist/trade symbols so existing
    # positions can be priced even when the day's candidate pool is empty.
    rows.extend(
        [(symbol,) for symbol in _read_symbols_for_range(engine, "19000101", td, "official_cn_a_etf_v3")]
    )
    return sorted({normalize_symbol(r[0]) for r in rows if r and normalize_symbol(r[0])})


def _fetch_adj_factor_tushare(pro, ts_code: str, start_date: str, end_date: str) -> pd.DataFrame:
    df = pro.adj_factor(ts_code=ts_code, start_date=start_date, end_date=end_date)
    if df is None or df.empty:
        return pd.DataFrame(columns=["trade_date", "ts_code", "adj_factor"])
    out = df[["trade_date", "ts_code", "adj_factor"]].copy()
    out["trade_date"] = out["trade_date"].astype(str).str.replace(r"[^0-9]", "", regex=True).str[:8]
    out["ts_code"] = out["ts_code"].map(normalize_symbol)
    out["adj_factor"] = pd.to_numeric(out["adj_factor"], errors="coerce")
    return out.dropna(subset=["adj_factor"])


def _build_qfq_from_raw_and_factor(raw: pd.DataFrame, factors: pd.DataFrame, end_date: str) -> pd.DataFrame:
    if raw.empty or factors.empty:
        return pd.DataFrame(columns=TARGET_COLS)
    work = raw.copy()
    work["trade_date"] = work["trade_date"].astype(str).str.replace(r"[^0-9]", "", regex=True).str[:8]
    for col in ["open_price", "high_price", "low_price", "close_price", "pct_chg", "vol", "amount"]:
        work[col] = pd.to_numeric(work[col], errors="coerce")
    merged = work.merge(factors[["trade_date", "adj_factor"]], on="trade_date", how="inner")
    merged = merged.dropna(subset=["adj_factor", "close_price"]).sort_values("trade_date")
    if merged.empty:
        return pd.DataFrame(columns=TARGET_COLS)

    anchor_rows = merged[merged["trade_date"] <= end_date]
    if anchor_rows.empty:
        return pd.DataFrame(columns=TARGET_COLS)
    anchor = anchor_rows.iloc[-1]
    anchor_factor = float(anchor["adj_factor"])
    if anchor_factor <= 0:
        return pd.DataFrame(columns=TARGET_COLS)
    anchor_trade_date = str(anchor["trade_date"])

    ratio = pd.to_numeric(merged["adj_factor"], errors="coerce") / anchor_factor
    for col in ["open_price", "high_price", "low_price", "close_price"]:
        merged[col] = (pd.to_numeric(merged[col], errors="coerce") * ratio).round(4)
    merged["pct_chg"] = merged["close_price"].pct_change().fillna(pd.to_numeric(merged["pct_chg"], errors="coerce") / 100.0) * 100.0
    merged["pct_chg"] = pd.to_numeric(merged["pct_chg"], errors="coerce").fillna(0.0).round(4)
    merged["anchor_trade_date"] = anchor_trade_date
    merged["source"] = "tushare_adj_factor"
    merged["updated_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    for col in TARGET_COLS:
        if col not in merged.columns:
            merged[col] = ""
    return merged[TARGET_COLS].copy()


def _fetch_akshare_qfq(ts_code: str, start_date: str, end_date: str, name: str = "") -> pd.DataFrame:
    try:
        import akshare as ak
    except Exception as exc:
        raise RuntimeError(f"AkShare import failed: {exc}") from exc

    symbol = normalize_symbol(ts_code).split(".")[0]
    raw = ak.stock_zh_a_hist(symbol=symbol, period="daily", start_date=start_date, end_date=end_date, adjust="qfq")
    if raw is None or raw.empty:
        return pd.DataFrame(columns=TARGET_COLS)
    out = raw.rename(
        columns={
            "日期": "trade_date",
            "开盘": "open_price",
            "最高": "high_price",
            "最低": "low_price",
            "收盘": "close_price",
            "成交量": "vol",
            "成交额": "amount",
            "涨跌幅": "pct_chg",
        }
    ).copy()
    out["trade_date"] = out["trade_date"].astype(str).str.replace(r"[^0-9]", "", regex=True).str[:8]
    out["ts_code"] = normalize_symbol(ts_code)
    out["name"] = name or normalize_symbol(ts_code)
    if "amount" in out.columns:
        out["amount"] = pd.to_numeric(out["amount"], errors="coerce") / 1000.0
    for col in ["open_price", "high_price", "low_price", "close_price", "pct_chg", "vol", "amount"]:
        out[col] = pd.to_numeric(out.get(col), errors="coerce")
    out["adj_factor"] = None
    out["anchor_trade_date"] = end_date
    out["source"] = "akshare_qfq"
    out["updated_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    out = out.dropna(subset=["close_price"])
    for col in TARGET_COLS:
        if col not in out.columns:
            out[col] = ""
    return out[TARGET_COLS].copy()


def save_qfq_df(engine, ts_code: str, start_date: str, end_date: str, df: pd.DataFrame, dry_run: bool) -> int:
    if df is None or df.empty:
        return 0
    if dry_run:
        return int(len(df))
    ensure_stock_price_qfq_table(engine)
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                DELETE FROM stock_price_qfq
                WHERE ts_code = :ts_code
                  AND trade_date >= :start_date
                  AND trade_date <= :end_date
                """
            ),
            {"ts_code": ts_code, "start_date": start_date, "end_date": end_date},
        )
    df.to_sql(
        "stock_price_qfq",
        engine,
        if_exists="append",
        index=False,
        dtype={
            "trade_date": types.VARCHAR(8),
            "ts_code": types.VARCHAR(16),
            "name": types.VARCHAR(80),
            "open_price": types.Float(),
            "high_price": types.Float(),
            "low_price": types.Float(),
            "close_price": types.Float(),
            "pct_chg": types.Float(),
            "vol": types.Float(),
            "amount": types.Float(),
            "adj_factor": types.Float(),
            "anchor_trade_date": types.VARCHAR(8),
            "source": types.VARCHAR(32),
            "updated_at": types.DateTime(),
        },
    )
    return int(len(df))


def update_symbol_qfq(engine, pro, ts_code: str, start_date: str, end_date: str, dry_run: bool = False) -> UpdateResult:
    ts_code = normalize_symbol(ts_code)
    result = UpdateResult(ts_code=ts_code, dry_run=dry_run)
    raw = _read_raw_price(engine, ts_code, start_date, end_date)
    if raw.empty:
        result.error = "stock_price 原始行情缺失"
        return result
    name = str(raw.iloc[-1].get("name") or ts_code)

    if is_a_share_symbol(ts_code):
        try:
            factors = _fetch_adj_factor_tushare(pro, ts_code, start_date, end_date)
            df = _build_qfq_from_raw_and_factor(raw, factors, end_date)
            result.source = "tushare_adj_factor"
        except Exception as exc:
            df = pd.DataFrame(columns=TARGET_COLS)
            result.error = f"Tushare adj_factor 失败: {exc}"
        if df.empty:
            try:
                df = _fetch_akshare_qfq(ts_code, start_date, end_date, name=name)
                result.source = "akshare_qfq"
                result.error = ""
            except Exception as exc:
                result.error = f"{result.error}; AkShare qfq 失败: {exc}" if result.error else f"AkShare qfq 失败: {exc}"
    else:
        df = raw.copy()
        df["adj_factor"] = None
        df["anchor_trade_date"] = end_date
        df["source"] = "raw_non_stock"
        df["updated_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        for col in TARGET_COLS:
            if col not in df.columns:
                df[col] = ""
        df = df[TARGET_COLS].copy()
        result.source = "raw_non_stock"

    if df.empty:
        if not result.error:
            result.error = "前复权数据为空"
        return result

    result.rows = save_qfq_df(engine, ts_code, start_date, end_date, df, dry_run=dry_run)
    return result


def parse_symbols(value: str) -> List[str]:
    return [normalize_symbol(x) for x in re.split(r"[,，\s]+", str(value or "")) if normalize_symbol(x)]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="回补 AI 模拟专用前复权价格到 stock_price_qfq")
    parser.add_argument("--start-date", default=None, type=lambda v: clean_date(v, "start-date"))
    parser.add_argument("--end-date", default=None, type=lambda v: clean_date(v, "end-date"))
    parser.add_argument("--date", default="", help="结束日期，支持 YYYYMMDD 或 latest；与 --end-date 二选一")
    parser.add_argument("--lookback-days", type=int, default=0, help="未传 --start-date 时，按结束日期向前回看 N 个自然日")
    parser.add_argument("--symbols", default="", help="逗号或空格分隔的代码，如 002837.SZ,600519.SH")
    parser.add_argument("--portfolio-id", default="", help="按组合历史交易/持仓/自选池收集代码")
    parser.add_argument(
        "--portfolio-symbol-scope",
        choices=["all", "trades_positions"],
        default="all",
        help="组合代码范围：all 包含自选池；trades_positions 只含历史交易和持仓",
    )
    parser.add_argument("--all-stock-price-symbols", action="store_true", help="按 stock_price 区间内全部代码补齐")
    parser.add_argument("--v3-daily-candidates", action="store_true", help="加入3号当日资金候选池预备代码")
    parser.add_argument("--candidate-date", default="", help="3号候选池日期，默认 end-date")
    parser.add_argument("--candidate-limit", type=int, default=800, help="3号资金候选池预备代码数量上限")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--sleep-sec", type=float, default=0.2)
    return parser


def resolve_cli_dates(args: argparse.Namespace) -> tuple[str, str]:
    if args.end_date and args.date:
        raise RuntimeError("--end-date 和 --date 只能传一个")
    if args.date:
        date_value = str(args.date or "").strip().lower()
        if date_value == "latest":
            end_date = latest_stock_price_date(get_engine())
        else:
            end_date = clean_date(date_value, "date")
    elif args.end_date:
        end_date = args.end_date
    else:
        raise RuntimeError("请传 --end-date YYYYMMDD，或传 --date latest")

    if args.start_date:
        start_date = args.start_date
    elif int(args.lookback_days or 0) > 0:
        start_dt = datetime.strptime(end_date, "%Y%m%d") - timedelta(days=int(args.lookback_days))
        start_date = start_dt.strftime("%Y%m%d")
    else:
        start_date = end_date
    return start_date, end_date


def resolve_symbols(engine, args: argparse.Namespace) -> List[str]:
    symbols = parse_symbols(args.symbols)
    if args.portfolio_id:
        include_watchlist = str(getattr(args, "portfolio_symbol_scope", "all") or "all") == "all"
        symbols.extend(
            _read_symbols_for_range(
                engine,
                args.start_date,
                args.end_date,
                args.portfolio_id,
                include_watchlist=include_watchlist,
            )
        )
    if args.all_stock_price_symbols:
        symbols.extend(_read_symbols_for_range(engine, args.start_date, args.end_date))
    if getattr(args, "v3_daily_candidates", False):
        candidate_date = clean_date(getattr(args, "candidate_date", "") or args.end_date, "candidate-date")
        symbols.extend(_read_v3_daily_candidate_symbols(engine, candidate_date, limit=int(getattr(args, "candidate_limit", 800) or 800)))
    unique = sorted({s for s in symbols if s})
    if not unique:
        raise RuntimeError("没有可补的代码，请传 --symbols、--portfolio-id 或 --all-stock-price-symbols")
    return unique


def run_update(
    start_date: str,
    end_date: str,
    symbols: Optional[Sequence[str]] = None,
    portfolio_id: str = "",
    all_stock_price_symbols: bool = False,
    v3_daily_candidates: bool = False,
    candidate_date: str = "",
    candidate_limit: int = 800,
    portfolio_symbol_scope: str = "all",
    dry_run: bool = False,
    sleep_sec: float = 0.2,
) -> List[UpdateResult]:
    start_date = clean_date(start_date, "start_date")
    end_date = clean_date(end_date, "end_date")
    if start_date > end_date:
        raise ValueError("start_date 不能晚于 end_date")
    engine = get_engine()
    ensure_stock_price_qfq_table(engine)
    pro = get_pro()

    class Args:
        pass

    args = Args()
    args.start_date = start_date
    args.end_date = end_date
    args.symbols = ",".join(symbols or [])
    args.portfolio_id = portfolio_id
    args.portfolio_symbol_scope = portfolio_symbol_scope
    args.all_stock_price_symbols = all_stock_price_symbols
    args.v3_daily_candidates = bool(v3_daily_candidates)
    args.candidate_date = candidate_date or end_date
    args.candidate_limit = int(candidate_limit or 800)
    target_symbols = resolve_symbols(engine, args)

    print(
        f"前复权补数: {start_date} -> {end_date} | symbols={len(target_symbols)} "
        f"| portfolio_id={portfolio_id or '-'} | v3_daily_candidates={bool(v3_daily_candidates)} "
        f"| dry_run={dry_run}"
    )
    results: List[UpdateResult] = []
    for idx, symbol in enumerate(target_symbols, start=1):
        result = update_symbol_qfq(engine, pro, symbol, start_date, end_date, dry_run=dry_run)
        results.append(result)
        status = "OK" if result.ok else "ERR"
        print(f"[{idx}/{len(target_symbols)}] {symbol} {status} rows={result.rows} source={result.source or '-'} error={result.error}")
        if sleep_sec > 0 and not dry_run:
            time.sleep(float(sleep_sec))
    ok_count = sum(1 for r in results if r.ok)
    print(f"完成: ok={ok_count} error={len(results) - ok_count}")
    return results


def main() -> int:
    args = build_parser().parse_args()
    start_date, end_date = resolve_cli_dates(args)
    run_update(
        start_date=start_date,
        end_date=end_date,
        symbols=parse_symbols(args.symbols),
        portfolio_id=args.portfolio_id,
        all_stock_price_symbols=args.all_stock_price_symbols,
        v3_daily_candidates=bool(args.v3_daily_candidates),
        candidate_date=args.candidate_date or end_date,
        candidate_limit=int(args.candidate_limit),
        portfolio_symbol_scope=args.portfolio_symbol_scope,
        dry_run=args.dry_run,
        sleep_sec=args.sleep_sec,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
