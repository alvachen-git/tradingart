"""全球主要指数 PE 数据的本地存储、解析和看板计算。"""

from __future__ import annotations

import json
import re
import shutil
import subprocess
import tempfile
import time
from dataclasses import asdict, dataclass
from datetime import date
from io import BytesIO
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

import pandas as pd
import requests
from sqlalchemy import inspect, text


TABLE_NAME = "global_index_valuation_daily"
MIN_PERCENTILE_MONTHS = 36


@dataclass(frozen=True)
class IndexSpec:
    code: str
    name: str
    market: str
    source_code: str
    source_name: str
    source_url: str
    proxy: bool = False
    history_mode: str = "ten_year"
    note: str = ""


INDEX_SPECS: tuple[IndexSpec, ...] = (
    IndexSpec(
        "NASDAQ100", "纳斯达克100", "美国", "QQQ", "公开历史（QQQ代理）",
        "https://worldperatio.com/index/nasdaq-100/", True,
        note="ETF代理口径；用于观察指数自身估值位置。",
    ),
    IndexSpec(
        "SP500", "标普500", "美国", "SPY", "公开历史（SPY代理）",
        "https://worldperatio.com/index/sp-500/", True,
        note="ETF代理口径；用于观察指数自身估值位置。",
    ),
    IndexSpec(
        "RUSSELL2000", "罗素2000", "美国", "IWM", "公开历史（IWM代理）",
        "https://worldperatio.com/index/russell-2000/", True,
        note="ETF代理口径；亏损公司处理方式可能令PE差异较大。",
    ),
    IndexSpec(
        "000300", "沪深300", "A股", "000300", "中证指数",
        "https://www.csindex.com.cn/csindex-home/perf/index-perf",
    ),
    IndexSpec(
        "399006", "创业板指", "A股", "399006.SZ", "Tushare（国证指数核对）",
        "https://www.cnindex.com.cn/module/index-series.html?act_menu=1&index_type=-1",
    ),
    IndexSpec(
        "000688", "科创50", "A股", "000688", "中证指数",
        "https://www.csindex.com.cn/csindex-home/perf/index-perf", history_mode="since_inception",
    ),
    IndexSpec(
        "000905", "中证500", "A股", "000905", "中证指数",
        "https://www.csindex.com.cn/csindex-home/perf/index-perf",
    ),
    IndexSpec(
        "000852", "中证1000", "A股", "000852", "中证指数",
        "https://www.csindex.com.cn/csindex-home/perf/index-perf",
    ),
    IndexSpec(
        "932000", "中证2000", "A股", "932000", "中证指数",
        "https://www.csindex.com.cn/csindex-home/perf/index-perf", note="含官方回溯历史。",
    ),
    IndexSpec(
        "HSI", "恒生指数", "香港", "Hang Seng Index", "恒生指数公司月度报告",
        "https://www.hsi.com.hk/eng/resources-education/monthly-roundup",
        history_mode="available_history",
    ),
    IndexSpec(
        "HSTECH", "恒生科技指数", "香港", "Hang Seng TECH Index", "恒生指数公司月度报告",
        "https://www.hsi.com.hk/eng/resources-education/monthly-roundup",
        history_mode="since_inception",
    ),
)
INDEX_SPEC_BY_CODE = {item.code: item for item in INDEX_SPECS}


@dataclass(frozen=True)
class GlobalValuationRecord:
    trade_date: str
    index_code: str
    index_name: str
    market: str
    pe_ttm: float
    source_name: str
    source_url: str
    methodology: str
    is_proxy: bool = False
    quality_status: str = "ok"
    raw_detail: Mapping[str, Any] | None = None


def _normalize_date(value: Any) -> str:
    parsed = pd.to_datetime(value, errors="coerce")
    return "" if pd.isna(parsed) else parsed.strftime("%Y%m%d")


def _positive_float(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if pd.notna(number) and number > 0 else None


def ensure_global_index_valuation_table(engine) -> None:
    ddl = f"""
    CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
        trade_date VARCHAR(8) NOT NULL,
        index_code VARCHAR(24) NOT NULL,
        index_name VARCHAR(64) NOT NULL,
        market VARCHAR(16) NOT NULL,
        pe_ttm DOUBLE NOT NULL,
        source_name VARCHAR(96) NOT NULL,
        source_url TEXT,
        methodology VARCHAR(200),
        is_proxy INTEGER NOT NULL DEFAULT 0,
        quality_status VARCHAR(32) NOT NULL DEFAULT 'ok',
        raw_detail_json TEXT,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (trade_date, index_code)
    )
    """
    with engine.begin() as conn:
        conn.execute(text(ddl))


def get_global_index_valuation_cache_version(engine) -> str:
    """Return a cheap cache key that changes after a successful data refresh."""
    try:
        if not inspect(engine).has_table(TABLE_NAME):
            return "missing"
        with engine.connect() as conn:
            count, latest_date, updated_at = conn.execute(text(f"""
                SELECT COUNT(*), MAX(trade_date), MAX(updated_at)
                FROM {TABLE_NAME}
            """)).one()
        return f"{int(count or 0)}:{latest_date or ''}:{updated_at or ''}"
    except Exception:
        return "unavailable"


def store_global_valuation_records(engine, records: Iterable[GlobalValuationRecord]) -> int:
    """按日期和指数代码幂等写入。"""
    clean_records = [record for record in records if _positive_float(record.pe_ttm)]
    if not clean_records:
        return 0
    ensure_global_index_valuation_table(engine)
    delete_sql = text(
        f"DELETE FROM {TABLE_NAME} WHERE trade_date = :trade_date AND index_code = :index_code"
    )
    insert_sql = text(f"""
        INSERT INTO {TABLE_NAME} (
            trade_date, index_code, index_name, market, pe_ttm,
            source_name, source_url, methodology, is_proxy,
            quality_status, raw_detail_json
        ) VALUES (
            :trade_date, :index_code, :index_name, :market, :pe_ttm,
            :source_name, :source_url, :methodology, :is_proxy,
            :quality_status, :raw_detail_json
        )
    """)
    with engine.begin() as conn:
        for record in clean_records:
            payload = asdict(record)
            payload["is_proxy"] = int(record.is_proxy)
            payload["raw_detail_json"] = json.dumps(
                record.raw_detail or {}, ensure_ascii=False, default=str
            )
            payload.pop("raw_detail", None)
            conn.execute(delete_sql, payload)
            conn.execute(insert_sql, payload)
    return len(clean_records)


def parse_csindex_payload(payload: Mapping[str, Any], spec: IndexSpec) -> list[GlobalValuationRecord]:
    """解析中证指数接口；接口字段 peg 为滚动市盈率。"""
    rows = payload.get("data") or payload.get("rows") or []
    if isinstance(rows, Mapping):
        rows = rows.get("list") or rows.get("rows") or rows.get("data") or []
    records: list[GlobalValuationRecord] = []
    for row in rows if isinstance(rows, Sequence) else []:
        if not isinstance(row, Mapping):
            continue
        trade_date = _normalize_date(row.get("tradeDate") or row.get("trade_date"))
        pe = _positive_float(row.get("peg") or row.get("pe") or row.get("peTtm"))
        if not trade_date or pe is None:
            continue
        records.append(GlobalValuationRecord(
            trade_date, spec.code, spec.name, spec.market, pe,
            spec.source_name, spec.source_url, "滚动市盈率",
            spec.proxy, "ok", dict(row),
        ))
    return records


_WORLD_PE_POINT_RE = re.compile(
    r"Date\.UTC\(\s*(\d{4})\s*,\s*(\d{1,2})\s*,\s*(\d{1,2})\s*\)\s*,\s*(-?\d+(?:\.\d+)?)"
)


def _extract_javascript_array(html: str, variable_name: str) -> str:
    assignment = re.search(rf"\b{re.escape(variable_name)}\s*=", html or "")
    if not assignment:
        raise ValueError(f"未找到目标PE序列 {variable_name}")
    start = (html or "").find("[", assignment.end())
    if start < 0:
        raise ValueError(f"目标PE序列 {variable_name} 缺少数组起点")
    depth = 0
    for index in range(start, len(html)):
        character = html[index]
        if character == "[":
            depth += 1
        elif character == "]":
            depth -= 1
            if depth == 0:
                return html[start:index + 1]
    raise ValueError(f"目标PE序列 {variable_name} 缺少数组终点")


def parse_world_pe_html(html: str, spec: IndexSpec) -> list[GlobalValuationRecord]:
    series_text = _extract_javascript_array(html or "", "detailPE_data")
    records: list[GlobalValuationRecord] = []
    for year, zero_month, day, raw_pe in _WORLD_PE_POINT_RE.findall(series_text):
        try:
            trade_date = date(int(year), int(zero_month) + 1, int(day)).strftime("%Y%m%d")
        except ValueError:
            continue
        pe = _positive_float(raw_pe)
        if pe is None:
            continue
        records.append(GlobalValuationRecord(
            trade_date, spec.code, spec.name, spec.market, pe,
            spec.source_name, spec.source_url, "ETF代理PE",
            True, "proxy", {"proxy_ticker": spec.source_code},
        ))
    if not records:
        raise ValueError("目标PE序列 detailPE_data 为空")
    return records


def parse_issuer_snapshot_html(html: str, ticker: str) -> tuple[float, str] | None:
    """Parse current PE shown by the ETF issuer; it is a cross-check, not a history patch."""
    ticker = str(ticker or "").upper()
    patterns = {
        "SPY": (
            r"Price/Earnings Ratio FY1.*?<td[^>]*class=[\"']data[\"'][^>]*>\s*(\d+(?:\.\d+)?)",
            "发行方FY1市盈率",
        ),
        "IWM": (
            r"data-id=[\"']fundamentalsAndRisk-priceEarnings(?:-data)?[\"'][^>]*>\s*(\d+(?:\.\d+)?)",
            "发行方组合市盈率",
        ),
        "QQQ": (
            r"Weighted Harmonic (?:Average|Avg).*?P/E.*?(\d+(?:\.\d+)?)",
            "发行方加权调和平均市盈率",
        ),
    }
    if ticker not in patterns:
        return None
    pattern, methodology = patterns[ticker]
    match = re.search(pattern, html or "", re.I | re.S)
    pe = _positive_float(match.group(1)) if match else None
    return (pe, methodology) if pe is not None else None


def parse_cni_snapshot(payload: Mapping[str, Any]) -> tuple[str, float] | None:
    """提取国证指数创业板PE，只用于Tushare序列的口径核对。"""
    data = payload.get("data") or payload.get("rows") or payload.get("list") or []
    if isinstance(data, Mapping):
        data = data.get("rows") or data.get("list") or data.get("data") or []
    for row in data if isinstance(data, Sequence) else []:
        if not isinstance(row, Mapping):
            continue
        code = row.get("indexcode") or row.get("indexCode") or row.get("index_code")
        if str(code) != "399006":
            continue
        pe = _positive_float(row.get("peDynamic") or row.get("pe") or row.get("peTtm"))
        if pe is not None:
            return "399006", pe
    return None


_MONTH_NAMES = {name: number for number, name in enumerate(
    ("January", "February", "March", "April", "May", "June", "July", "August",
     "September", "October", "November", "December"), start=1
)}


def parse_hsi_archive(payload: Mapping[str, Any]) -> list[dict[str, str]]:
    descriptors: list[dict[str, str]] = []
    for content in payload.get("contentList") or []:
        for resource in content.get("resourcesList") or []:
            title = str(resource.get("title") or resource.get("name") or "")
            match = re.search(r"Monthly Roundup\s*\((\w+)\s+(\d{4})\)", title, re.I)
            if not match or match.group(1).title() not in _MONTH_NAMES:
                continue
            month = _MONTH_NAMES[match.group(1).title()]
            month_end = (pd.Timestamp(int(match.group(2)), month, 1) + pd.offsets.MonthEnd()).strftime("%Y%m%d")
            url = resource.get("url") or resource.get("link") or resource.get("fileUrl")
            if url:
                absolute_url = str(url)
                if absolute_url.startswith("/"):
                    absolute_url = f"https://www.hsi.com.hk{absolute_url}"
                descriptors.append({"trade_date": month_end, "url": absolute_url, "title": title})
    return sorted(descriptors, key=lambda item: item["trade_date"])


def parse_hsi_pdf_text(pdf_text: str, trade_date: str) -> list[GlobalValuationRecord]:
    """解析保留列布局的恒指公司月报文本。"""
    records: list[GlobalValuationRecord] = []
    patterns = {
        "HSI": r"^\s*Hang Seng Index\s+.*?\s(\d+(?:\.\d+)?)\s+\d+(?:\.\d+)?%\s*$",
        "HSTECH": r"^\s*Hang Seng TECH Index\s+.*?\s(\d+(?:\.\d+)?)\s+\d+(?:\.\d+)?%\s*$",
    }
    for code, pattern in patterns.items():
        match = re.search(pattern, pdf_text or "", re.I | re.M)
        if not match:
            continue
        pe = _positive_float(match.group(1))
        if pe is None:
            continue
        spec = INDEX_SPEC_BY_CODE[code]
        records.append(GlobalValuationRecord(
            _normalize_date(trade_date), code, spec.name, spec.market, pe,
            spec.source_name, spec.source_url, "月度报告滚动市盈率",
            False, "ok", {"report_month_end": _normalize_date(trade_date)},
        ))
    return records


def extract_pdf_text(pdf_bytes: bytes) -> str:
    """提取月报文本；仅由离线更新脚本调用。"""
    if not pdf_bytes.startswith(b"%PDF"):
        raise ValueError("恒生月报不是有效PDF")
    try:
        from pypdf import PdfReader

        reader = PdfReader(BytesIO(pdf_bytes))
        chunks = []
        for page in reader.pages:
            try:
                chunks.append(page.extract_text(extraction_mode="layout") or "")
            except TypeError:
                chunks.append(page.extract_text() or "")
        return "\n".join(chunks)
    except ImportError:
        executable = shutil.which("pdftotext")
        if not executable:
            raise RuntimeError("解析恒生月报需要 pypdf 或 pdftotext")
        with tempfile.TemporaryDirectory(prefix="hsi_valuation_") as temp_dir:
            pdf_path = Path(temp_dir) / "report.pdf"
            text_path = Path(temp_dir) / "report.txt"
            pdf_path.write_bytes(pdf_bytes)
            result = subprocess.run(
                [executable, "-layout", str(pdf_path), str(text_path)],
                capture_output=True,
                text=True,
                timeout=30,
                check=False,
            )
            if result.returncode != 0 or not text_path.exists():
                raise RuntimeError(f"pdftotext解析恒生月报失败: {result.stderr.strip()}")
            return text_path.read_text(encoding="utf-8", errors="replace")


class PublicGlobalValuationClient:
    """固定禁用系统代理、15秒超时、最多3次尝试的HTTP适配器。"""

    def __init__(self, timeout: float = 15.0, max_attempts: int = 3):
        self.timeout = timeout
        self.max_attempts = max_attempts
        self.session = requests.Session()
        self.session.trust_env = False
        self.session.headers.update({"User-Agent": "Mozilla/5.0 TradingArt valuation updater"})

    def request(self, method: str, url: str, **kwargs) -> requests.Response:
        last_error: Exception | None = None
        for attempt in range(1, self.max_attempts + 1):
            try:
                response = self.session.request(method, url, timeout=self.timeout, **kwargs)
                response.raise_for_status()
                return response
            except requests.RequestException as exc:
                last_error = exc
                if attempt < self.max_attempts:
                    time.sleep(0.35 * attempt)
        raise RuntimeError(f"请求失败（{self.max_attempts}次）: {url}: {last_error}")


def _empty_card(spec: IndexSpec) -> dict[str, Any]:
    if spec.history_mode == "since_inception":
        history_label = "成立以来分位"
    elif spec.history_mode == "available_history":
        history_label = "可用历史分位"
    else:
        history_label = "近10年分位"
    return {
        "code": spec.code, "name": spec.name, "market": spec.market,
        "current_pe": None, "percentile": None, "percentile_label": "暂无数据",
        "history_label": history_label,
        "median_pe": None, "median_deviation_pct": None, "sample_count": 0,
        "data_date": "", "source_name": spec.source_name, "source_url": spec.source_url,
        "is_proxy": spec.proxy, "quality_status": "missing",
        "quality_message": "本地尚无可用数据", "note": spec.note,
        "p20": None, "p80": None,
    }


def percentile_label(value: float | None) -> str:
    if value is None or pd.isna(value):
        return "样本不足"
    if value <= 20:
        return "历史低位"
    if value <= 40:
        return "偏低"
    if value <= 60:
        return "中性"
    if value <= 80:
        return "偏高"
    return "历史高位"


def empirical_percentile(values: Sequence[float], current: float) -> float | None:
    clean = pd.to_numeric(pd.Series(values), errors="coerce")
    clean = clean[(clean > 0) & clean.notna()]
    if len(clean) < MIN_PERCENTILE_MONTHS or current <= 0:
        return None
    return round(float((clean <= current).sum() / len(clean) * 100), 1)


def _month_end_frame(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return frame.copy()
    result = frame.copy()
    result["date"] = pd.to_datetime(result["trade_date"], format="%Y%m%d", errors="coerce")
    result["pe_ttm"] = pd.to_numeric(result["pe_ttm"], errors="coerce")
    result = result[result["date"].notna() & (result["pe_ttm"] > 0)].sort_values("date")
    if result.empty:
        return result
    result["month"] = result["date"].dt.to_period("M")
    return result.groupby("month", as_index=False).tail(1).drop(columns=["month"])


def _history_label(spec: IndexSpec, monthly: pd.DataFrame, latest_date: pd.Timestamp) -> str:
    if spec.history_mode == "since_inception":
        return "成立以来分位"
    if spec.history_mode == "available_history":
        if monthly.empty or monthly["date"].min() > latest_date - pd.DateOffset(years=9, months=10):
            return "可用历史分位"
    return "近10年分位"


def _stale_message(market: str, latest: pd.Timestamp, anchor: pd.Timestamp) -> str:
    if anchor <= latest:
        return ""
    if market == "A股":
        lag = len(pd.bdate_range(latest + pd.Timedelta(days=1), anchor))
        return f"数据已滞后约{lag}个交易日" if lag > 3 else ""
    lag = (anchor.normalize() - latest.normalize()).days
    return f"月度数据已滞后{lag}天" if lag > 45 else ""


def build_global_index_valuation_dashboard(
    engine, as_of_date: str = "", window_years: int = 10
) -> dict[str, Any]:
    """固定返回11张卡、排行榜、单指数历史序列和质量提示。"""
    cards = [_empty_card(spec) for spec in INDEX_SPECS]
    payload: dict[str, Any] = {
        "status": "missing",
        "as_of_date": _normalize_date(as_of_date) if as_of_date else date.today().strftime("%Y%m%d"),
        "cards": cards, "ranking": [],
        "series_by_code": {spec.code: [] for spec in INDEX_SPECS},
        "quality_notes": [],
    }
    try:
        if not inspect(engine).has_table(TABLE_NAME):
            payload["quality_notes"].append("本地估值表尚未创建，请先运行更新脚本。")
            return payload
        query = f"SELECT * FROM {TABLE_NAME}"
        params: dict[str, Any] = {}
        if as_of_date:
            query += " WHERE trade_date <= :as_of_date"
            params["as_of_date"] = _normalize_date(as_of_date)
        frame = pd.read_sql(text(query), engine, params=params)
    except Exception as exc:
        payload["status"] = "error"
        payload["quality_notes"].append(f"读取本地估值数据失败：{exc}")
        return payload
    if frame.empty:
        payload["quality_notes"].append("本地估值表暂无记录，请先运行更新脚本。")
        return payload

    anchor = pd.to_datetime(payload["as_of_date"], format="%Y%m%d")
    card_map = {card["code"]: card for card in cards}
    for spec in INDEX_SPECS:
        raw = frame[frame["index_code"].astype(str) == spec.code].copy()
        monthly_all = _month_end_frame(raw)
        if monthly_all.empty:
            continue
        latest_rows = raw.assign(
            date=pd.to_datetime(raw["trade_date"], format="%Y%m%d", errors="coerce"),
            pe_num=pd.to_numeric(raw["pe_ttm"], errors="coerce"),
        )
        latest_rows = latest_rows[latest_rows["date"].notna() & (latest_rows["pe_num"] > 0)].sort_values("date")
        latest_row = latest_rows.iloc[-1]
        latest_date = pd.Timestamp(latest_row["date"])
        window_start = latest_date - pd.DateOffset(years=max(1, int(window_years)))
        monthly = monthly_all[monthly_all["date"] >= window_start].copy()
        values = monthly["pe_ttm"].astype(float).tolist()
        current_pe = float(latest_row["pe_num"])
        percentile = empirical_percentile(values, current_pe)
        median_pe = float(monthly["pe_ttm"].median()) if not monthly.empty else None
        deviation = ((current_pe / median_pe) - 1) * 100 if median_pe else None
        stale = _stale_message(spec.market, latest_date, anchor)
        raw_quality = str(latest_row.get("quality_status") or "ok")
        quality_status = "stale" if stale else raw_quality
        quality_message = stale
        if percentile is None and not quality_message:
            quality_status = "insufficient"
            quality_message = "有效月度历史不足36个月"
        if raw_quality == "source_mismatch":
            quality_status = raw_quality
            quality_message = "创业板PE与国证指数核对偏差超过5%"
        card = card_map[spec.code]
        card.update({
            "current_pe": round(current_pe, 2), "percentile": percentile,
            "percentile_label": percentile_label(percentile),
            "history_label": _history_label(spec, monthly, latest_date),
            "median_pe": round(median_pe, 2) if median_pe else None,
            "median_deviation_pct": round(deviation, 1) if deviation is not None else None,
            "sample_count": len(monthly), "data_date": latest_date.strftime("%Y-%m-%d"),
            "source_name": str(latest_row.get("source_name") or spec.source_name),
            "source_url": str(latest_row.get("source_url") or spec.source_url),
            "is_proxy": bool(latest_row.get("is_proxy")),
            "quality_status": quality_status, "quality_message": quality_message,
            "p20": round(float(monthly["pe_ttm"].quantile(0.2)), 2) if len(monthly) else None,
            "p80": round(float(monthly["pe_ttm"].quantile(0.8)), 2) if len(monthly) else None,
        })
        payload["series_by_code"][spec.code] = [
            {"date": row.date.strftime("%Y-%m-%d"), "pe": round(float(row.pe_ttm), 4)}
            for row in monthly.itertuples()
        ]

    payload["ranking"] = sorted([
        {"code": card["code"], "name": card["name"], "market": card["market"],
         "percentile": card["percentile"], "label": card["percentile_label"]}
        for card in cards if card["percentile"] is not None
    ], key=lambda item: item["percentile"], reverse=True)
    payload["status"] = "ok" if payload["ranking"] else "insufficient"
    if any(card["quality_status"] == "stale" for card in cards):
        payload["quality_notes"].append("部分市场数据已超过更新时限，页面保留最后有效值并明确标记。")
    return payload
