from __future__ import annotations

import argparse
import os
import re
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Dict, Iterable, List, Optional

import pandas as pd
import tushare as ts
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv(override=True)


DEFAULT_SECTOR_TYPE = "行业"
MATCHED = "matched"
UNMATCHED = "unmatched"
AMBIGUOUS = "ambiguous"
DEFAULT_THS_DAILY_INTERVAL_SEC = 0.36


@dataclass(frozen=True)
class SectorIndexMatch:
    sector_name: str
    sector_type: str
    ths_code: str
    ths_name: str
    match_status: str


def get_db_engine():
    user = os.getenv("DB_USER")
    pwd = os.getenv("DB_PASSWORD")
    host = os.getenv("DB_HOST")
    port = os.getenv("DB_PORT", "3306")
    name = os.getenv("DB_NAME")
    if not all([user, pwd, host, name]):
        return None
    url = f"mysql+pymysql://{user}:{pwd}@{host}:{port}/{name}"
    return create_engine(url, pool_pre_ping=True, pool_recycle=3600)


def get_tushare_pro():
    token = os.getenv("TUSHARE_TOKEN")
    if not token:
        return None
    return ts.pro_api(token)


def normalize_trade_date(value: Optional[str]) -> str:
    if not value:
        return datetime.now().strftime("%Y%m%d")
    cleaned = re.sub(r"[^0-9]", "", str(value or ""))[:8]
    if len(cleaned) != 8:
        raise ValueError(f"非法日期格式: {value}, 期望 YYYYMMDD")
    return cleaned


def date_days_ago(end_date: str, days: int) -> str:
    end_dt = datetime.strptime(normalize_trade_date(end_date), "%Y%m%d").date()
    return (end_dt - timedelta(days=max(0, int(days)))).strftime("%Y%m%d")


def resolve_update_dates(
    *,
    date: str = "",
    start_date: str = "",
    end_date: str = "",
    lookback_days: int = 0,
) -> tuple[str, str, str]:
    """Return (start_date, end_date, trade_date_for_sector_names)."""
    if date:
        single = normalize_trade_date(date)
        return single, single, single

    resolved_end = normalize_trade_date(end_date or None)
    if start_date:
        resolved_start = normalize_trade_date(start_date)
    elif int(lookback_days or 0) > 0:
        resolved_start = date_days_ago(resolved_end, int(lookback_days))
    else:
        resolved_start = resolved_end
    return resolved_start, resolved_end, resolved_end


def normalize_sector_name(value: Any) -> str:
    text_value = str(value or "").strip().lower()
    text_value = re.sub(r"\s+", "", text_value)
    text_value = re.sub(r"[()（）【】\[\]·,，/\\-]", "", text_value)
    return text_value


def strip_sector_suffix(value: Any) -> str:
    out = normalize_sector_name(value)
    for suffix in ("同花顺指数", "同花顺", "申万", "行业指数", "概念指数", "板块指数", "行业", "概念", "板块", "指数"):
        if out.endswith(suffix):
            out = out[: -len(suffix)]
    return out


def ensure_sector_index_tables(engine) -> None:
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS sector_index_map (
                  sector_name VARCHAR(128) NOT NULL,
                  sector_type VARCHAR(20) NOT NULL DEFAULT '行业',
                  ths_code VARCHAR(32) NOT NULL DEFAULT '',
                  ths_name VARCHAR(128) NOT NULL DEFAULT '',
                  match_status VARCHAR(20) NOT NULL DEFAULT 'unmatched',
                  updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                  PRIMARY KEY (sector_name, sector_type)
                ) DEFAULT CHARSET=utf8mb4
                """
            )
        )
        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS sector_index_price (
                  trade_date VARCHAR(8) NOT NULL,
                  ths_code VARCHAR(32) NOT NULL,
                  sector_name VARCHAR(128) NOT NULL,
                  sector_type VARCHAR(20) NOT NULL DEFAULT '行业',
                  open_price DOUBLE DEFAULT 0,
                  high_price DOUBLE DEFAULT 0,
                  low_price DOUBLE DEFAULT 0,
                  close_price DOUBLE DEFAULT 0,
                  pct_chg DOUBLE DEFAULT 0,
                  vol DOUBLE DEFAULT 0,
                  amount DOUBLE DEFAULT 0,
                  updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                  PRIMARY KEY (trade_date, sector_name, sector_type),
                  KEY idx_sector_index_price_sector_date (sector_name, sector_type, trade_date),
                  KEY idx_sector_index_price_code_date (ths_code, trade_date)
                ) DEFAULT CHARSET=utf8mb4
                """
            )
        )
        keys = conn.execute(text("SHOW KEYS FROM sector_index_price WHERE Key_name='PRIMARY'")).mappings().fetchall()
        key_cols = [str(row.get("Column_name") or "") for row in sorted(keys, key=lambda x: int(x.get("Seq_in_index") or 0))]
        if key_cols == ["trade_date", "ths_code"]:
            conn.execute(
                text(
                    """
                    ALTER TABLE sector_index_price
                      DROP PRIMARY KEY,
                      ADD PRIMARY KEY (trade_date, sector_name, sector_type)
                    """
                )
            )


def fetch_sector_names(engine, sector_type: str = DEFAULT_SECTOR_TYPE, trade_date: Optional[str] = None) -> List[str]:
    if trade_date:
        date_sql = text(
            """
            SELECT DISTINCT industry
            FROM sector_moneyflow
            WHERE trade_date = :td AND sector_type = :sector_type
              AND industry IS NOT NULL AND industry != ''
            ORDER BY industry
            """
        )
        params = {"td": normalize_trade_date(trade_date), "sector_type": sector_type}
    else:
        date_sql = text(
            """
            SELECT DISTINCT industry
            FROM sector_moneyflow
            WHERE trade_date = (SELECT MAX(trade_date) FROM sector_moneyflow)
              AND sector_type = :sector_type
              AND industry IS NOT NULL AND industry != ''
            ORDER BY industry
            """
        )
        params = {"sector_type": sector_type}

    with engine.connect() as conn:
        df = pd.read_sql(date_sql, conn, params=params)
    if df.empty:
        return []
    return [str(x).strip() for x in df["industry"].tolist() if str(x).strip()]


def fetch_ths_index_catalog(pro) -> pd.DataFrame:
    if pro is None:
        return pd.DataFrame(columns=["ts_code", "name"])
    primary_attempts = [
        {"exchange": "A", "type": "I", "fields": "ts_code,name"},
        {"exchange": "", "type": "I", "fields": "ts_code,name"},
        {"type": "I", "fields": "ts_code,name"},
        {"exchange": "A", "type": "N", "fields": "ts_code,name"},
        {"exchange": "", "type": "N", "fields": "ts_code,name"},
        {"type": "N", "fields": "ts_code,name"},
    ]

    frames: List[pd.DataFrame] = []
    for kwargs in primary_attempts:
        try:
            df = pro.ths_index(**kwargs)
        except Exception:
            continue
        if df is None or df.empty or "ts_code" not in df.columns or "name" not in df.columns:
            continue
        out = df[["ts_code", "name"]].copy()
        out["ts_code"] = out["ts_code"].astype(str).str.strip().str.upper()
        out["name"] = out["name"].astype(str).str.strip()
        out = out[(out["ts_code"] != "") & (out["name"] != "")]
        if not out.empty:
            frames.append(out)

    if frames:
        return (
            pd.concat(frames, ignore_index=True)
            .drop_duplicates(subset=["ts_code"], keep="last")
            .reset_index(drop=True)
        )

    # 兜底只用于接口字段组合变化；匹配阶段仍会保守处理无匹配/多匹配。
    for kwargs in ({"fields": "ts_code,name"}, {}):
        try:
            df = pro.ths_index(**kwargs)
        except Exception:
            continue
        if df is None or df.empty or "ts_code" not in df.columns or "name" not in df.columns:
            continue
        out = df[["ts_code", "name"]].copy()
        out["ts_code"] = out["ts_code"].astype(str).str.strip().str.upper()
        out["name"] = out["name"].astype(str).str.strip()
        out = out[(out["ts_code"] != "") & (out["name"] != "")]
        if not out.empty:
            return out.drop_duplicates(subset=["ts_code"], keep="last").reset_index(drop=True)
    return pd.DataFrame(columns=["ts_code", "name"])


def _unique_catalog_rows(catalog: pd.DataFrame, mask: pd.Series) -> pd.DataFrame:
    rows = catalog[mask].copy()
    if rows.empty:
        return rows
    return rows.drop_duplicates(subset=["ts_code"], keep="last").reset_index(drop=True)


def match_sector_to_ths_index(sector_name: str, sector_type: str, catalog: pd.DataFrame) -> SectorIndexMatch:
    if catalog is None or catalog.empty:
        return SectorIndexMatch(sector_name, sector_type, "", "", UNMATCHED)

    work = catalog.copy()
    work["name_norm"] = work["name"].map(normalize_sector_name)
    work["name_core"] = work["name"].map(strip_sector_suffix)
    sector_norm = normalize_sector_name(sector_name)
    sector_core = strip_sector_suffix(sector_name)

    stages = [
        work["name_norm"] == sector_norm,
        work["name_core"] == sector_core,
        work["name_core"].map(lambda x: bool(sector_core and (x in sector_core or sector_core in x))),
    ]
    for mask in stages:
        rows = _unique_catalog_rows(work, mask)
        if rows.empty:
            continue
        if len(rows) == 1:
            row = rows.iloc[0]
            return SectorIndexMatch(
                sector_name=sector_name,
                sector_type=sector_type,
                ths_code=str(row["ts_code"]),
                ths_name=str(row["name"]),
                match_status=MATCHED,
            )
        return SectorIndexMatch(sector_name, sector_type, "", "", AMBIGUOUS)

    return SectorIndexMatch(sector_name, sector_type, "", "", UNMATCHED)


def match_sector_index_catalog(sector_names: Iterable[str], sector_type: str, catalog: pd.DataFrame) -> List[SectorIndexMatch]:
    return [match_sector_to_ths_index(str(name).strip(), sector_type, catalog) for name in sector_names if str(name).strip()]


def save_sector_index_matches(engine, matches: List[SectorIndexMatch]) -> None:
    if not matches:
        return
    df = pd.DataFrame([m.__dict__ for m in matches])
    with engine.begin() as conn:
        for _, row in df.iterrows():
            conn.execute(
                text(
                    """
                    INSERT INTO sector_index_map (
                      sector_name, sector_type, ths_code, ths_name, match_status, updated_at
                    ) VALUES (
                      :sector_name, :sector_type, :ths_code, :ths_name, :match_status, CURRENT_TIMESTAMP
                    )
                    ON DUPLICATE KEY UPDATE
                      ths_code=VALUES(ths_code),
                      ths_name=VALUES(ths_name),
                      match_status=VALUES(match_status),
                      updated_at=CURRENT_TIMESTAMP
                    """
                ),
                row.to_dict(),
            )


def transform_ths_daily_df(raw_df: pd.DataFrame, match: SectorIndexMatch) -> pd.DataFrame:
    cols = [
        "trade_date",
        "ths_code",
        "sector_name",
        "sector_type",
        "open_price",
        "high_price",
        "low_price",
        "close_price",
        "pct_chg",
        "vol",
        "amount",
    ]
    if raw_df is None or raw_df.empty:
        return pd.DataFrame(columns=cols)

    df = raw_df.copy()
    rename_map = {
        "ts_code": "ths_code",
        "open": "open_price",
        "high": "high_price",
        "low": "low_price",
        "close": "close_price",
        "pct_change": "pct_chg",
    }
    df = df.rename(columns=rename_map)
    if "ths_code" not in df.columns:
        df["ths_code"] = match.ths_code
    df["sector_name"] = match.sector_name
    df["sector_type"] = match.sector_type
    for col in ["open_price", "high_price", "low_price", "close_price", "pct_chg", "vol", "amount"]:
        if col not in df.columns:
            df[col] = 0.0
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)
    df["trade_date"] = df["trade_date"].astype(str).str.replace("-", "", regex=False).str[:8]
    df["ths_code"] = df["ths_code"].astype(str).str.strip().str.upper()
    out = df[cols].copy()
    out = out[(out["trade_date"].str.len() == 8) & (out["ths_code"] != "")]
    out = out.drop_duplicates(subset=["trade_date", "ths_code"], keep="last")
    return out.sort_values(["ths_code", "trade_date"]).reset_index(drop=True)


def fetch_ths_daily_with_retry(pro, ths_code: str, start_date: str, end_date: str, retries: int = 3) -> pd.DataFrame:
    last_error = None
    for i in range(retries + 1):
        try:
            df = pro.ths_daily(ts_code=ths_code, start_date=start_date, end_date=end_date)
            return df if df is not None else pd.DataFrame()
        except Exception as exc:
            last_error = exc
            time.sleep(1.0 * (i + 1))
    raise RuntimeError(f"拉取 ths_daily 失败: {ths_code} {last_error}")


def save_sector_index_prices(
    engine,
    price_df: pd.DataFrame,
    sector_name: str,
    sector_type: str,
    start_date: str,
    end_date: str,
) -> int:
    if price_df is None or price_df.empty:
        return 0
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                DELETE FROM sector_index_price
                WHERE sector_name=:sector_name
                  AND sector_type=:sector_type
                  AND trade_date >= :start_date
                  AND trade_date <= :end_date
                """
            ),
            {
                "sector_name": sector_name,
                "sector_type": sector_type,
                "start_date": start_date,
                "end_date": end_date,
            },
        )
        price_df.to_sql("sector_index_price", conn, if_exists="append", index=False, chunksize=500, method="multi")
    return len(price_df)


def run_update(
    start_date: str,
    end_date: str,
    sector_type: str = DEFAULT_SECTOR_TYPE,
    dry_run: bool = False,
    trade_date_for_sectors: Optional[str] = None,
    daily_interval_sec: float = DEFAULT_THS_DAILY_INTERVAL_SEC,
) -> Dict[str, Any]:
    engine = get_db_engine()
    if engine is None:
        raise RuntimeError("数据库配置缺失，无法建立连接")
    pro = get_tushare_pro()
    if pro is None:
        raise RuntimeError("TUSHARE_TOKEN 缺失，无法拉取同花顺板块指数")

    start_date = normalize_trade_date(start_date)
    end_date = normalize_trade_date(end_date)
    ensure_sector_index_tables(engine)

    sector_names = fetch_sector_names(engine, sector_type=sector_type, trade_date=trade_date_for_sectors)
    catalog = fetch_ths_index_catalog(pro)
    matches = match_sector_index_catalog(sector_names, sector_type=sector_type, catalog=catalog)
    if not dry_run:
        save_sector_index_matches(engine, matches)

    matched = [m for m in matches if m.match_status == MATCHED and m.ths_code]
    unmatched = [m for m in matches if m.match_status == UNMATCHED]
    ambiguous = [m for m in matches if m.match_status == AMBIGUOUS]

    rows_written = 0
    price_errors: List[str] = []
    empty_price_sectors: List[str] = []
    for idx, match in enumerate(matched):
        try:
            if idx > 0 and daily_interval_sec > 0:
                time.sleep(daily_interval_sec)
            raw = fetch_ths_daily_with_retry(pro, match.ths_code, start_date=start_date, end_date=end_date)
            price_df = transform_ths_daily_df(raw, match)
            if price_df.empty:
                empty_price_sectors.append(match.sector_name)
            if dry_run:
                rows_written += len(price_df)
            else:
                rows_written += save_sector_index_prices(
                    engine,
                    price_df,
                    match.sector_name,
                    match.sector_type,
                    start_date,
                    end_date,
                )
        except Exception as exc:
            price_errors.append(f"{match.sector_name}({match.ths_code}): {exc}")

    return {
        "sector_type": sector_type,
        "start_date": start_date,
        "end_date": end_date,
        "sector_count": len(sector_names),
        "matched_count": len(matched),
        "unmatched": [m.sector_name for m in unmatched],
        "ambiguous": [m.sector_name for m in ambiguous],
        "rows_written": rows_written,
        "dry_run": dry_run,
        "price_errors": price_errors,
        "empty_price_count": len(empty_price_sectors),
        "empty_price_sectors": empty_price_sectors[:30],
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="更新行业板块同花顺指数 OHLC 数据")
    parser.add_argument("--date", default="", help="指定单日 YYYYMMDD")
    parser.add_argument("--start-date", default="", help="起始日期 YYYYMMDD")
    parser.add_argument("--end-date", default="", help="结束日期 YYYYMMDD")
    parser.add_argument("--sector-type", default=DEFAULT_SECTOR_TYPE, help="板块类型，默认行业")
    parser.add_argument("--dry-run", action="store_true", help="只拉取和转换，不写入数据库")
    parser.add_argument(
        "--lookback-days",
        type=int,
        default=0,
        help="未指定 --date/--start-date 时，从结束日向前滚动补最近 N 个自然日，避免接口延迟造成永久缺口",
    )
    parser.add_argument(
        "--sleep",
        type=float,
        default=DEFAULT_THS_DAILY_INTERVAL_SEC,
        help="批量拉取 ths_daily 的请求间隔秒数，默认 0.36 以避开 200次/分钟限频",
    )
    return parser


def main() -> int:
    args = build_parser().parse_args()
    start_date, end_date, trade_date_for_sectors = resolve_update_dates(
        date=str(args.date or ""),
        start_date=str(args.start_date or ""),
        end_date=str(args.end_date or ""),
        lookback_days=int(args.lookback_days or 0),
    )

    result = run_update(
        start_date=start_date,
        end_date=end_date,
        sector_type=str(args.sector_type or DEFAULT_SECTOR_TYPE).strip() or DEFAULT_SECTOR_TYPE,
        dry_run=bool(args.dry_run),
        trade_date_for_sectors=trade_date_for_sectors,
        daily_interval_sec=max(0.0, float(args.sleep or 0.0)),
    )
    print("✅ 板块价格更新结果:")
    print(result)
    if result.get("price_errors"):
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
