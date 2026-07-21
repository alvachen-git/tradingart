from __future__ import annotations

import argparse
import datetime as dt
import json
import logging
import os
import sys
import time
from dataclasses import replace
from pathlib import Path
from typing import Any, Sequence

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import URL, create_engine, text

from global_index_valuation import (
    INDEX_SPEC_BY_CODE,
    GlobalValuationRecord,
    PublicGlobalValuationClient,
    extract_pdf_text,
    parse_cni_snapshot,
    parse_csindex_payload,
    parse_hsi_archive,
    parse_hsi_pdf_text,
    parse_issuer_snapshot_html,
    parse_world_pe_html,
    store_global_valuation_records,
)


CSI_CODES = ("000300", "000688", "000905", "000852", "932000")
US_CODES = ("NASDAQ100", "SP500", "RUSSELL2000")
CSI_URL = "https://www.csindex.com.cn/csindex-home/perf/index-perf"
CNI_URL = "https://www.cnindex.com.cn/index/search"
HSI_ARCHIVE_URL = "https://www.hsi.com.hk/data/eng/download/monthly-roundup.json"
SPY_ISSUER_URL = "https://www.ssga.com/us/en/individual/etfs/state-street-spdr-sp-500-etf-trust-spy"
IWM_ISSUER_URL = "https://www.ishares.com/us/products/239710/IWM"
QQQ_ISSUER_URL = "https://www.invesco.com/us/financial-products/etfs/product-detail?audienceType=Investor&productId=ETF-QQQ"
LOGGER = logging.getLogger("global_valuation_update")


def configure_logging() -> None:
    """Show useful progress on stdout and hide noisy PDF layout diagnostics."""
    if not LOGGER.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(logging.Formatter(
            "%(asctime)s | %(levelname)s | %(message)s",
            datefmt="%H:%M:%S",
        ))
        LOGGER.addHandler(handler)
    LOGGER.setLevel(logging.INFO)
    LOGGER.propagate = False
    logging.getLogger("pypdf").setLevel(logging.ERROR)


def _record_summary(records: Sequence[GlobalValuationRecord]) -> str:
    if not records:
        return "0条"
    dates = [record.trade_date for record in records]
    return f"{len(records)}条，日期{min(dates)}–{max(dates)}"


def _elapsed_seconds(started_at: float) -> str:
    return f"{time.perf_counter() - started_at:.1f}秒"


def parse_date(value: Any) -> str:
    parsed = pd.to_datetime(value, errors="coerce")
    if pd.isna(parsed):
        raise ValueError(f"非法日期 {value!r}，期望 YYYYMMDD 或 YYYY-MM-DD")
    return parsed.strftime("%Y%m%d")


def create_engine_from_env() -> Any:
    load_dotenv(Path(__file__).resolve().parent / ".env", override=True)
    required = {name: os.getenv(name) for name in ("DB_USER", "DB_PASSWORD", "DB_HOST", "DB_NAME")}
    missing = [name for name, value in required.items() if not value]
    if missing:
        raise RuntimeError(f"数据库配置缺失: {', '.join(missing)}")
    url = URL.create(
        "mysql+pymysql",
        username=required["DB_USER"],
        password=required["DB_PASSWORD"],
        host=required["DB_HOST"],
        port=int(os.getenv("DB_PORT", "3306")),
        database=required["DB_NAME"],
    )
    return create_engine(url, pool_pre_ping=True, pool_recycle=3600)


def fetch_csindex_records(
    client: PublicGlobalValuationClient,
    code: str,
    start_date: str,
    end_date: str,
) -> list[GlobalValuationRecord]:
    response = client.request(
        "GET",
        CSI_URL,
        params={"indexCode": code, "startDate": start_date, "endDate": end_date},
        headers={"Referer": "https://www.csindex.com.cn/"},
    )
    return parse_csindex_payload(response.json(), INDEX_SPEC_BY_CODE[code])


def fetch_world_pe_records(
    client: PublicGlobalValuationClient,
    code: str,
) -> list[GlobalValuationRecord]:
    spec = INDEX_SPEC_BY_CODE[code]
    response = client.request("GET", spec.source_url)
    return parse_world_pe_html(response.text, spec)


def parse_spy_issuer_pe(html: str) -> float | None:
    import re

    match = re.search(
        r"Price/Earnings\s*</th>\s*<td class=\"data\">\s*(\d+(?:\.\d+)?)",
        html or "",
        re.I,
    )
    return float(match.group(1)) if match else None


def parse_iwm_issuer_pe(html: str) -> float | None:
    import html as html_lib
    import re

    decoded = html_lib.unescape(html or "")
    marker = decoded.find('"fullName":"fundamentalsAndRisk.priceEarnings"')
    if marker < 0:
        return None
    nearby_values = re.findall(
        r'"value":(\d+(?:\.\d+)?)',
        decoded[max(0, marker - 2500):marker],
    )
    return float(nearby_values[-1]) if nearby_values else None


def parse_qqq_issuer_pe(html: str) -> float | None:
    snapshot = parse_issuer_snapshot_html(html, "QQQ")
    return snapshot[0] if snapshot else None


def validate_us_proxy_snapshots(
    records: Sequence[GlobalValuationRecord],
    client: PublicGlobalValuationClient,
    tolerance_pct: float = 10.0,
) -> tuple[list[GlobalValuationRecord], list[str]]:
    """Soft-check current SPY/IWM proxy PE against issuer pages; history is never spliced."""
    validators = {
        "NASDAQ100": (QQQ_ISSUER_URL, parse_qqq_issuer_pe, "Invesco QQQ"),
        "SP500": (SPY_ISSUER_URL, parse_spy_issuer_pe, "State Street SPY"),
        "RUSSELL2000": (IWM_ISSUER_URL, parse_iwm_issuer_pe, "iShares IWM"),
    }
    output = list(records)
    warnings: list[str] = []
    for code, (url, parser, label) in validators.items():
        positions = [index for index, record in enumerate(output) if record.index_code == code]
        if not positions:
            continue
        latest_position = max(positions, key=lambda index: output[index].trade_date)
        latest = output[latest_position]
        try:
            issuer_pe = parser(client.request("GET", url).text)
            if issuer_pe is None:
                raise ValueError("未找到PE字段")
            gap_pct = abs(latest.pe_ttm / issuer_pe - 1) * 100
            quality = "source_mismatch" if gap_pct > tolerance_pct else latest.quality_status
            output[latest_position] = replace(
                latest,
                quality_status=quality,
                raw_detail={
                    **(latest.raw_detail or {}),
                    "issuer_snapshot": label,
                    "issuer_pe": issuer_pe,
                    "issuer_gap_pct": round(gap_pct, 4),
                },
            )
            if gap_pct > tolerance_pct:
                warnings.append(f"{latest.index_name}代理PE与{label}偏差{gap_pct:.1f}%")
        except Exception as exc:
            warnings.append(f"{label}快照核验失败: {exc}")
    return output, warnings


def fetch_cni_snapshot(
    client: PublicGlobalValuationClient,
) -> tuple[str, float] | None:
    response = client.request(
        "POST",
        CNI_URL,
        data={"content": "399006", "rows": "20", "pageNum": "1"},
        headers={"Referer": "https://www.cnindex.com.cn/"},
    )
    return parse_cni_snapshot(response.json())


def load_chinext_history(engine: Any, start_date: str, end_date: str) -> list[GlobalValuationRecord]:
    frame = pd.read_sql(
        text(
            """
            SELECT trade_date, pe_ttm, pe
            FROM index_valuation
            WHERE ts_code = '399006.SZ'
              AND REPLACE(REPLACE(trade_date, '-', ''), '/', '') BETWEEN :start_date AND :end_date
            ORDER BY trade_date
            """
        ),
        engine,
        params={"start_date": start_date, "end_date": end_date},
    )
    spec = INDEX_SPEC_BY_CODE["399006"]
    records: list[GlobalValuationRecord] = []
    for row in frame.itertuples(index=False):
        pe = pd.to_numeric(getattr(row, "pe_ttm", None), errors="coerce")
        if pd.isna(pe) or float(pe) <= 0:
            pe = pd.to_numeric(getattr(row, "pe", None), errors="coerce")
        day = parse_date(getattr(row, "trade_date"))
        if pd.isna(pe) or float(pe) <= 0:
            continue
        records.append(GlobalValuationRecord(
            day, spec.code, spec.name, spec.market, float(pe),
            spec.source_name, spec.source_url, "本地连续PE（国证指数核对）",
            False, "ok", {"local_table": "index_valuation"},
        ))
    return records


def fetch_chinext_history(start_date: str, end_date: str) -> list[GlobalValuationRecord]:
    """读取Tushare连续历史；本地旧表仅在接口失败时作为同口径回退。"""
    import tushare as ts

    token = os.getenv("TUSHARE_TOKEN", "")
    if token:
        ts.set_token(token)
    frame = ts.pro_api().index_dailybasic(
        ts_code="399006.SZ",
        start_date=start_date,
        end_date=end_date,
        fields="ts_code,trade_date,pe_ttm",
    )
    spec = INDEX_SPEC_BY_CODE["399006"]
    records: list[GlobalValuationRecord] = []
    for row in frame.to_dict("records") if frame is not None else []:
        pe = pd.to_numeric(row.get("pe_ttm"), errors="coerce")
        if pd.isna(pe) or float(pe) <= 0:
            continue
        records.append(GlobalValuationRecord(
            parse_date(row.get("trade_date")), spec.code, spec.name, spec.market,
            float(pe), spec.source_name, spec.source_url,
            "Tushare连续滚动PE（国证指数核对）", False, "ok", row,
        ))
    return sorted(records, key=lambda item: item.trade_date)


def validate_chinext_snapshot(
    records: Sequence[GlobalValuationRecord],
    snapshot: tuple[str, float] | None,
    tolerance_pct: float = 5.0,
) -> tuple[list[GlobalValuationRecord], str | None]:
    if not records or snapshot is None:
        return list(records), "创业板国证快照未取得，保留本地连续序列"
    latest = max(records, key=lambda item: item.trade_date)
    official_pe = snapshot[1]
    gap_pct = abs(latest.pe_ttm / official_pe - 1) * 100
    if gap_pct <= tolerance_pct:
        return list(records), None
    warning = f"创业板本地PE与国证快照偏差{gap_pct:.1f}%，超过{tolerance_pct:.0f}%"
    output = [
        replace(
            item,
            quality_status="source_mismatch" if item == latest else item.quality_status,
            raw_detail={
                **(item.raw_detail or {}),
                "cni_snapshot_pe": official_pe,
                "gap_pct": round(gap_pct, 4),
            },
        )
        for item in records
    ]
    return output, warning


def fetch_hsi_archive(client: PublicGlobalValuationClient) -> list[dict[str, str]]:
    response = client.request("GET", HSI_ARCHIVE_URL)
    return parse_hsi_archive(response.json())


def select_hsi_reports(
    resources: Sequence[dict[str, str]],
    start_date: str,
    end_date: str,
    backfill: bool,
) -> list[dict[str, str]]:
    eligible = [
        resource for resource in resources
        if start_date <= resource.get("trade_date", "") <= end_date
    ]
    eligible.sort(key=lambda item: item["trade_date"])
    return eligible if backfill else eligible[-1:]


def fetch_hsi_report_records(
    client: PublicGlobalValuationClient,
    resource: dict[str, str],
) -> list[GlobalValuationRecord]:
    response = client.request("GET", resource["url"])
    pdf_text = extract_pdf_text(response.content)
    return parse_hsi_pdf_text(pdf_text, resource["trade_date"])


def collect_valuation_records(
    engine: Any,
    client: PublicGlobalValuationClient,
    start_date: str,
    end_date: str,
    backfill: bool,
) -> tuple[list[GlobalValuationRecord], list[str]]:
    records: list[GlobalValuationRecord] = []
    warnings: list[str] = []

    LOGGER.info("开始读取中证指数官方估值，共%d个指数", len(CSI_CODES))
    for position, code in enumerate(CSI_CODES, start=1):
        spec = INDEX_SPEC_BY_CODE[code]
        started_at = time.perf_counter()
        LOGGER.info("中证指数 [%d/%d] %s：正在读取", position, len(CSI_CODES), spec.name)
        try:
            fetched = fetch_csindex_records(client, code, start_date, end_date)
            selected = fetched if backfill else fetched[-1:]
            records.extend(selected)
            LOGGER.info(
                "中证指数 [%d/%d] %s：完成，%s，耗时%s",
                position, len(CSI_CODES), spec.name,
                _record_summary(selected), _elapsed_seconds(started_at),
            )
        except Exception as exc:
            warning = f"{spec.name}读取失败: {exc}"
            warnings.append(warning)
            LOGGER.warning("中证指数 [%d/%d] %s", position, len(CSI_CODES), warning)

    LOGGER.info("创业板指：正在读取Tushare连续历史")
    started_at = time.perf_counter()
    try:
        try:
            chinext = fetch_chinext_history(start_date, end_date)
            chinext_source = "Tushare"
        except Exception as exc:
            warning = f"创业板Tushare读取失败，回退本地连续序列: {exc}"
            warnings.append(warning)
            LOGGER.warning("%s", warning)
            chinext = load_chinext_history(engine, start_date, end_date)
            chinext_source = "本地index_valuation回退"
        if not backfill:
            chinext = chinext[-1:]
        end_age_days = abs((dt.date.today() - pd.to_datetime(end_date).date()).days)
        if end_age_days <= 7:
            try:
                snapshot = fetch_cni_snapshot(client)
            except Exception as exc:
                snapshot = None
                warning = f"创业板国证快照核验失败: {exc}"
                warnings.append(warning)
                LOGGER.warning("%s", warning)
            chinext, warning = validate_chinext_snapshot(chinext, snapshot)
        else:
            warning = None
        records.extend(chinext)
        LOGGER.info(
            "创业板指：完成，来源=%s，%s，耗时%s",
            chinext_source, _record_summary(chinext), _elapsed_seconds(started_at),
        )
        if warning:
            warnings.append(warning)
            LOGGER.warning("%s", warning)
    except Exception as exc:
        warning = f"创业板历史读取失败: {exc}"
        warnings.append(warning)
        LOGGER.warning("%s", warning)

    LOGGER.info("开始读取美国ETF代理估值，共%d个指数", len(US_CODES))
    for position, code in enumerate(US_CODES, start=1):
        spec = INDEX_SPEC_BY_CODE[code]
        started_at = time.perf_counter()
        LOGGER.info("美国代理 [%d/%d] %s：正在读取", position, len(US_CODES), spec.name)
        try:
            fetched = [
                record for record in fetch_world_pe_records(client, code)
                if start_date <= record.trade_date <= end_date
            ]
            selected = fetched if backfill else fetched[-1:]
            records.extend(selected)
            LOGGER.info(
                "美国代理 [%d/%d] %s：完成，%s，耗时%s",
                position, len(US_CODES), spec.name,
                _record_summary(selected), _elapsed_seconds(started_at),
            )
        except Exception as exc:
            warning = f"{spec.name}代理序列读取失败: {exc}"
            warnings.append(warning)
            LOGGER.warning("美国代理 [%d/%d] %s", position, len(US_CODES), warning)
    if abs((dt.date.today() - pd.to_datetime(end_date).date()).days) <= 45:
        LOGGER.info("美国代理：正在核验QQQ、SPY、IWM发行方快照")
        records, proxy_warnings = validate_us_proxy_snapshots(records, client)
        warnings.extend(proxy_warnings)
        if proxy_warnings:
            for warning in proxy_warnings:
                LOGGER.warning("美国发行方核验：%s", warning)
        else:
            LOGGER.info("美国代理：发行方快照核验完成，无超阈值偏差")

    LOGGER.info("恒生月报：正在读取官方月报目录")
    try:
        resources = fetch_hsi_archive(client)
        selected = select_hsi_reports(resources, start_date, end_date, backfill)
        LOGGER.info(
            "恒生月报：目录共%d份，目标范围%d份%s",
            len(resources), len(selected), "，开始逐份解析" if selected else "",
        )
        if not selected:
            warning = "恒生月报目录中没有目标月份"
            warnings.append(warning)
            LOGGER.warning("%s", warning)
        for position, resource in enumerate(selected, start=1):
            started_at = time.perf_counter()
            title = resource.get("title", "恒生月报")
            LOGGER.info(
                "恒生月报 [%d/%d] %s：下载并解析中",
                position, len(selected), title,
            )
            try:
                fetched = fetch_hsi_report_records(client, resource)
                records.extend(fetched)
                pe_values = "、".join(
                    f"{record.index_name} PE {record.pe_ttm:.2f}" for record in fetched
                ) or "未提取到目标PE"
                LOGGER.info(
                    "恒生月报 [%d/%d] %s：完成，%s，耗时%s",
                    position, len(selected), title, pe_values, _elapsed_seconds(started_at),
                )
            except Exception as exc:
                warning = f"{title}解析失败: {exc}"
                warnings.append(warning)
                LOGGER.warning("恒生月报 [%d/%d] %s", position, len(selected), warning)
    except Exception as exc:
        warning = f"恒生月报目录读取失败: {exc}"
        warnings.append(warning)
        LOGGER.warning("%s", warning)

    unique = {
        (record.trade_date, record.index_code): record
        for record in records
        if start_date <= record.trade_date <= end_date
    }
    output = sorted(unique.values(), key=lambda item: (item.index_code, item.trade_date))
    LOGGER.info("全部来源读取完成：去重后%d条，警告%d条", len(output), len(warnings))
    return output, warnings


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="更新全球指数估值本地缓存")
    parser.add_argument("--date", default="", help="截止日期，YYYYMMDD 或 YYYY-MM-DD")
    parser.add_argument("--backfill-start", default="", help="历史回补起点；不传时只更新最近数据")
    parser.add_argument("--dry-run", action="store_true", help="拉取并校验，但不建表、不写数据库")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    configure_logging()
    args = build_parser().parse_args(argv)
    end_date = parse_date(args.date) if args.date else dt.date.today().strftime("%Y%m%d")
    backfill = bool(args.backfill_start)
    start_date = (
        parse_date(args.backfill_start)
        if backfill
        else (pd.to_datetime(end_date) - pd.DateOffset(months=3)).strftime("%Y%m%d")
    )
    if start_date > end_date:
        raise ValueError("回补起点不能晚于截止日期")

    run_started_at = time.perf_counter()
    LOGGER.info(
        "全球估值更新开始：模式=%s，日期=%s–%s，dry_run=%s",
        "历史回补" if backfill else "日常更新",
        start_date,
        end_date,
        bool(args.dry_run),
    )
    LOGGER.info("正在连接本地数据库")
    engine = create_engine_from_env()
    client = PublicGlobalValuationClient(timeout=15, max_attempts=3)
    records, warnings = collect_valuation_records(engine, client, start_date, end_date, backfill)
    if args.dry_run:
        written = 0
        LOGGER.info("dry-run：跳过数据库写入")
    else:
        LOGGER.info("数据库写入开始：待写入%d条", len(records))
        write_started_at = time.perf_counter()
        written = store_global_valuation_records(engine, records)
        LOGGER.info("数据库写入完成：写入%d条，耗时%s", written, _elapsed_seconds(write_started_at))
    summary = {
        "mode": "backfill" if backfill else "daily",
        "dry_run": bool(args.dry_run),
        "start_date": start_date,
        "end_date": end_date,
        "fetched": len(records),
        "written": written,
        "by_index": {
            code: sum(record.index_code == code for record in records)
            for code in INDEX_SPEC_BY_CODE
        },
        "warnings": warnings,
    }
    LOGGER.info(
        "全球估值更新完成：抓取%d条，写入%d条，警告%d条，总耗时%s",
        len(records), written, len(warnings), _elapsed_seconds(run_started_at),
    )
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0 if records else 2


if __name__ == "__main__":
    raise SystemExit(main())
