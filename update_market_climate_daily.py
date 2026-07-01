from __future__ import annotations

import argparse
import csv
import io
import json
import os
import re
from dataclasses import asdict, dataclass
from datetime import date, datetime, timedelta
from typing import Any, Iterable

import pandas as pd
import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from sqlalchemy import create_engine, text


DEFAULT_CBOE_URLS = {
    "VIX9D": "https://cdn.cboe.com/api/global/us_indices/daily_prices/VIX9D_History.csv",
    "VIX": "https://cdn.cboe.com/api/global/us_indices/daily_prices/VIX_History.csv",
    "VIX3M": "https://cdn.cboe.com/api/global/us_indices/daily_prices/VIX3M_History.csv",
}
DEFAULT_CME_FEDWATCH_URL = "https://www.cmegroup.com/CmeWS/mvc/Markets/FedWatch/Tool/565"
DEFAULT_AAII_SENTIMENT_URL = "https://www.aaii.com/sentimentsurvey"
DEFAULT_AAII_SENTIMENT_XLS_URL = "https://www.aaii.com/files/surveys/sentiment.xls"
DEFAULT_CFTC_TFF_URL = (
    "https://publicreporting.cftc.gov/resource/gpe5-46if.json"
    "?$limit=5000&$order=report_date_as_yyyy_mm_dd%20DESC"
)
DEFAULT_CFTC_TFF_TXT_URL = "https://www.cftc.gov/dea/newcot/FinFutWk.txt"
DEFAULT_GSCPI_URL = "https://www.newyorkfed.org/medialibrary/research/interactives/gscpi/downloads/gscpi_data.xlsx"


@dataclass(frozen=True)
class MarketClimateRecord:
    indicator_code: str
    as_of_date: date
    value: float
    secondary_value: float | None
    unit: str
    source: str
    payload: dict[str, Any]

    def db_params(self) -> dict[str, Any]:
        return {
            "indicator_code": self.indicator_code,
            "as_of_date": self.as_of_date,
            "value": self.value,
            "secondary_value": self.secondary_value,
            "unit": self.unit,
            "source": self.source,
            "payload_json": json.dumps(self.payload, ensure_ascii=False, sort_keys=True),
        }


def clean_number(value: Any) -> float | None:
    if value in (None, ""):
        return None
    if isinstance(value, str):
        value = value.replace(",", "").replace("%", "").strip()
    try:
        out = float(value)
    except (TypeError, ValueError):
        return None
    if pd.isna(out):
        return None
    return out


def parse_date(value: Any) -> date | None:
    parsed = pd.to_datetime(value, errors="coerce")
    if pd.isna(parsed):
        return None
    return parsed.date()


def make_session() -> requests.Session:
    session = requests.Session()
    session.trust_env = False
    session.headers.update(
        {
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126 Safari/537.36"
            ),
            "Accept": "text/html,application/json,text/csv,*/*",
        }
    )
    return session


def http_get_text(session: requests.Session, url: str, timeout: int = 25) -> str:
    resp = session.get(url, timeout=timeout)
    resp.raise_for_status()
    return resp.text


def http_get_bytes(session: requests.Session, url: str, timeout: int = 30) -> bytes:
    resp = session.get(url, timeout=timeout)
    resp.raise_for_status()
    return resp.content


def _first_matching_col(columns: Iterable[Any], patterns: list[str]) -> str | None:
    normalized = [(str(col).strip(), str(col).strip().lower()) for col in columns]
    for pattern in patterns:
        pattern_lower = pattern.lower()
        for original, lower in normalized:
            if pattern_lower == lower or pattern_lower in lower:
                return original
    return None


def parse_cboe_history_csv(csv_text: str) -> pd.DataFrame:
    df = pd.read_csv(io.StringIO(csv_text))
    if df.empty:
        return pd.DataFrame(columns=["as_of_date", "close"])
    df.columns = [str(col).strip() for col in df.columns]
    date_col = _first_matching_col(df.columns, ["date"])
    close_col = _first_matching_col(df.columns, ["close", "vix close", "vix9d close", "vix3m close"])
    if close_col is None:
        numeric_candidates = []
        for col in df.columns:
            if col == date_col:
                continue
            numeric = pd.to_numeric(df[col], errors="coerce")
            if numeric.notna().any():
                numeric_candidates.append(col)
        close_col = numeric_candidates[-1] if numeric_candidates else None
    if date_col is None or close_col is None:
        return pd.DataFrame(columns=["as_of_date", "close"])
    out = df[[date_col, close_col]].rename(columns={date_col: "as_of_date", close_col: "close"})
    out["as_of_date"] = pd.to_datetime(out["as_of_date"], errors="coerce")
    out["close"] = pd.to_numeric(out["close"], errors="coerce")
    out = out.dropna(subset=["as_of_date", "close"]).sort_values("as_of_date").reset_index(drop=True)
    return out


def build_vix_term_record(vix9d: pd.DataFrame, vix: pd.DataFrame, vix3m: pd.DataFrame) -> MarketClimateRecord | None:
    if vix9d.empty or vix.empty or vix3m.empty:
        return None
    merged = (
        vix9d.rename(columns={"close": "vix9d"})
        .merge(vix.rename(columns={"close": "vix"}), on="as_of_date", how="inner")
        .merge(vix3m.rename(columns={"close": "vix3m"}), on="as_of_date", how="inner")
        .sort_values("as_of_date")
    )
    if merged.empty:
        return None
    row = merged.iloc[-1]
    as_of = parse_date(row["as_of_date"])
    if as_of is None:
        return None
    vix9d_value = float(row["vix9d"])
    vix_value = float(row["vix"])
    vix3m_value = float(row["vix3m"])
    return MarketClimateRecord(
        indicator_code="VIX_TERM",
        as_of_date=as_of,
        value=vix9d_value - vix3m_value,
        secondary_value=vix_value,
        unit="vol_points",
        source="cboe",
        payload={"vix9d": vix9d_value, "vix": vix_value, "vix3m": vix3m_value},
    )


def fetch_vix_term_record(session: requests.Session) -> MarketClimateRecord | None:
    frames = {}
    for code, default_url in DEFAULT_CBOE_URLS.items():
        url = os.getenv(f"CBOE_{code}_CSV_URL", default_url)
        frames[code] = parse_cboe_history_csv(http_get_text(session, url))
    return build_vix_term_record(frames["VIX9D"], frames["VIX"], frames["VIX3M"])


def _aaii_pct(value: Any) -> float | None:
    number = clean_number(value)
    if number is None:
        return None
    return number * 100 if abs(number) <= 1 else number


def build_aaii_sentiment_record(
    as_of: date,
    bullish: Any,
    neutral: Any,
    bearish: Any,
    *,
    source: str,
) -> MarketClimateRecord | None:
    bullish_pct = _aaii_pct(bullish)
    neutral_pct = _aaii_pct(neutral)
    bearish_pct = _aaii_pct(bearish)
    if bullish_pct is None or bearish_pct is None:
        return None
    return MarketClimateRecord(
        indicator_code="AAII_BULL_BEAR",
        as_of_date=as_of,
        value=bullish_pct - bearish_pct,
        secondary_value=None,
        unit="pp",
        source=source,
        payload={
            "bullish_pct": bullish_pct,
            "neutral_pct": neutral_pct,
            "bearish_pct": bearish_pct,
        },
    )


def parse_aaii_sentiment_frame(frame: pd.DataFrame, *, source: str = "aaii_xls") -> MarketClimateRecord | None:
    if frame is None or frame.empty or frame.shape[1] < 4:
        return None
    data = frame.copy()
    data["_as_of"] = pd.to_datetime(data.iloc[:, 0], errors="coerce", format="mixed")
    data = data.dropna(subset=["_as_of"])
    if data.empty:
        return None
    for _, row in data.iloc[::-1].iterrows():
        record = build_aaii_sentiment_record(
            row["_as_of"].date(),
            row.iloc[1],
            row.iloc[2] if len(row) > 2 else None,
            row.iloc[3] if len(row) > 3 else None,
            source=source,
        )
        if record is not None:
            return record
    return None


def parse_aaii_sentiment_workbook(content: bytes) -> MarketClimateRecord | None:
    frame = pd.read_excel(io.BytesIO(content), sheet_name="SENTIMENT", header=None)
    return parse_aaii_sentiment_frame(frame, source="aaii_xls")


def _soup_text(html: str) -> str:
    return re.sub(r"\s+", " ", BeautifulSoup(html, "html.parser").get_text(" ", strip=True))


def parse_aaii_sentiment_html(html: str) -> MarketClimateRecord | None:
    soup = BeautifulSoup(html, "html.parser")
    table_values: dict[str, float] = {}
    table_date: date | None = None
    for row in soup.find_all("tr"):
        cells = [cell.get_text(" ", strip=True) for cell in row.find_all(["th", "td"])]
        lower_cells = [cell.lower() for cell in cells]
        if not {"bullish", "bearish"}.issubset(set(lower_cells)):
            continue
        headers = lower_cells
        sibling = row.find_next_sibling("tr")
        while sibling is not None:
            values = [cell.get_text(" ", strip=True) for cell in sibling.find_all(["th", "td"])]
            if len(values) >= len(headers):
                mapped = dict(zip(headers, values))
                bullish = clean_number(mapped.get("bullish"))
                bearish = clean_number(mapped.get("bearish"))
                neutral = clean_number(mapped.get("neutral"))
                if bullish is not None and bearish is not None:
                    table_values = {"bullish": bullish, "bearish": bearish}
                    if neutral is not None:
                        table_values["neutral"] = neutral
                    for value in values:
                        parsed_date = parse_date(value)
                        if parsed_date is not None:
                            table_date = parsed_date
                            break
                    break
            sibling = sibling.find_next_sibling("tr")
        if table_values:
            break

    text_value = _soup_text(html)
    values: dict[str, float] = {}
    values.update(table_values)
    for label in ("Bullish", "Neutral", "Bearish"):
        if label.lower() in values:
            continue
        match = re.search(rf"{label}\s+(-?\d+(?:\.\d+)?)\s*%?", text_value, re.IGNORECASE)
        if match:
            values[label.lower()] = float(match.group(1))
    if "bullish" not in values or "bearish" not in values:
        return None
    date_match = re.search(r"(\d{1,2}/\d{1,2}/\d{4})", text_value)
    as_of = table_date or (parse_date(date_match.group(1)) if date_match else date.today())
    if as_of is None:
        as_of = date.today()
    return build_aaii_sentiment_record(
        as_of,
        values.get("bullish"),
        values.get("neutral"),
        values.get("bearish"),
        source="aaii",
    )


def fetch_aaii_sentiment_record(session: requests.Session) -> MarketClimateRecord | None:
    local_file = os.getenv("AAII_SENTIMENT_FILE")
    if local_file:
        with open(local_file, "rb") as fh:
            return parse_aaii_sentiment_workbook(fh.read())

    try:
        record = parse_aaii_sentiment_workbook(
            http_get_bytes(session, os.getenv("AAII_SENTIMENT_XLS_URL", DEFAULT_AAII_SENTIMENT_XLS_URL))
        )
        if record is not None:
            return record
    except Exception:
        pass

    return parse_aaii_sentiment_html(http_get_text(session, os.getenv("AAII_SENTIMENT_URL", DEFAULT_AAII_SENTIMENT_URL)))


def _walk_dicts(value: Any) -> Iterable[dict[str, Any]]:
    if isinstance(value, dict):
        yield value
        for child in value.values():
            yield from _walk_dicts(child)
    elif isinstance(value, list):
        for item in value:
            yield from _walk_dicts(item)


def _find_number_by_key(row: dict[str, Any], keywords: tuple[str, ...]) -> float | None:
    for key, value in row.items():
        key_lower = str(key).lower()
        if any(keyword in key_lower for keyword in keywords):
            number = clean_number(value)
            if number is not None:
                return number * 100 if 0 < number <= 1 else number
    return None


def _find_text_by_key(row: dict[str, Any], keywords: tuple[str, ...]) -> str | None:
    for key, value in row.items():
        key_lower = str(key).lower()
        if any(keyword in key_lower for keyword in keywords) and value not in (None, ""):
            return str(value)
    return None


def _fedwatch_action_label(row: dict[str, Any]) -> str:
    raw = _find_text_by_key(row, ("action", "move", "direction", "label", "scenario"))
    if raw:
        lowered = raw.lower()
        if any(word in lowered for word in ("cut", "lower", "ease", "down", "decrease")):
            return "降息"
        if any(word in lowered for word in ("hike", "raise", "tighten", "up", "increase")):
            return "加息"
        if any(word in lowered for word in ("hold", "unchanged", "no change", "维持")):
            return "维持"
        return raw[:8]
    change = _find_number_by_key(row, ("change", "bp", "bps"))
    if change is not None:
        if change < 0:
            return "降息"
        if change > 0:
            return "加息"
    return "维持"


def build_fedwatch_record(
    action_label: str,
    probability: Any,
    meeting_date: Any = None,
    *,
    as_of: date | None = None,
    source: str = "cme_manual",
) -> MarketClimateRecord | None:
    probability_value = clean_number(probability)
    if probability_value is None:
        return None
    meeting_label = str(meeting_date).strip() if meeting_date not in (None, "") else None
    return MarketClimateRecord(
        indicator_code="FEDWATCH",
        as_of_date=as_of or date.today(),
        value=probability_value,
        secondary_value=None,
        unit="%",
        source=source,
        payload={
            "action_label": str(action_label or "最高概率"),
            "meeting_date": meeting_label,
            "probability": probability_value,
        },
    )


def parse_fedwatch_payload(payload: Any, today: date | None = None) -> MarketClimateRecord | None:
    candidates: list[dict[str, Any]] = []
    for row in _walk_dicts(payload):
        probability = _find_number_by_key(row, ("probability", "prob", "pct", "percent"))
        if probability is None:
            continue
        meeting = _find_text_by_key(row, ("meeting", "fomc", "event", "date"))
        candidates.append(
            {
                "probability": probability,
                "action_label": _fedwatch_action_label(row),
                "meeting_date": meeting,
                "raw": row,
            }
        )
    if not candidates:
        return None
    best = max(candidates, key=lambda item: float(item["probability"]))
    as_of = today or date.today()
    return build_fedwatch_record(
        best["action_label"],
        best["probability"],
        best.get("meeting_date"),
        as_of=as_of,
        source="cme",
    )


def fetch_fedwatch_record(session: requests.Session) -> MarketClimateRecord | None:
    manual_probability = os.getenv("FEDWATCH_PROBABILITY")
    if manual_probability:
        record = build_fedwatch_record(
            os.getenv("FEDWATCH_ACTION_LABEL", "维持"),
            manual_probability,
            os.getenv("FEDWATCH_MEETING_DATE"),
            source="cme_manual",
        )
        if record is not None:
            return record

    url = os.getenv("CME_FEDWATCH_URL", DEFAULT_CME_FEDWATCH_URL)
    resp = session.get(url, timeout=25, headers={"Accept": "application/json,text/plain,*/*"})
    resp.raise_for_status()
    return parse_fedwatch_payload(resp.json())


def _normalized_key_map(row: dict[str, Any]) -> dict[str, Any]:
    return {re.sub(r"[^a-z0-9]", "", str(key).lower()): value for key, value in row.items()}


def _field_number(row: dict[str, Any], candidates: list[str]) -> float | None:
    normalized = _normalized_key_map(row)
    for candidate in candidates:
        key = re.sub(r"[^a-z0-9]", "", candidate.lower())
        if key in normalized:
            number = clean_number(normalized[key])
            if number is not None:
                return number
    return None


def _field_text(row: dict[str, Any], candidates: list[str]) -> str:
    normalized = _normalized_key_map(row)
    for candidate in candidates:
        key = re.sub(r"[^a-z0-9]", "", candidate.lower())
        if key in normalized and normalized[key] not in (None, ""):
            return str(normalized[key])
    return ""


def parse_cftc_vix_json(rows: list[dict[str, Any]]) -> MarketClimateRecord | None:
    matches = []
    for row in rows:
        market_name = _field_text(row, ["market_and_exchange_names", "market_name", "contract_market_name"])
        if "VIX" not in market_name.upper():
            continue
        report_date = parse_date(_field_text(row, ["report_date_as_yyyy_mm_dd", "report_date", "date"]))
        long_pos = _field_number(
            row,
            [
                "levered_funds_positions_long_all",
                "leveraged_funds_positions_long_all",
                "lev_funds_positions_long_all",
            ],
        )
        short_pos = _field_number(
            row,
            [
                "levered_funds_positions_short_all",
                "leveraged_funds_positions_short_all",
                "lev_funds_positions_short_all",
            ],
        )
        open_interest = _field_number(row, ["open_interest_all", "open_interest"])
        if report_date is None or long_pos is None or short_pos is None or not open_interest:
            continue
        matches.append((report_date, market_name, long_pos, short_pos, open_interest))
    if not matches:
        return None
    report_date, market_name, long_pos, short_pos, open_interest = sorted(matches, key=lambda item: item[0])[-1]
    net_contracts = long_pos - short_pos
    return MarketClimateRecord(
        indicator_code="CFTC_VIX_LEV_NET",
        as_of_date=report_date,
        value=net_contracts / open_interest * 100,
        secondary_value=net_contracts,
        unit="%_oi",
        source="cftc",
        payload={
            "market": market_name,
            "leveraged_funds_long": long_pos,
            "leveraged_funds_short": short_pos,
            "open_interest": open_interest,
        },
    )


def parse_cftc_vix_text(text_body: str) -> MarketClimateRecord | None:
    """Parse CFTC Traders in Financial Futures weekly TXT fallback.

    FinFutWk.txt is headerless. For futures-only rows the needed columns are:
    0 market, 2 report date, 7 open interest, 14 leveraged funds long, and
    15 leveraged funds short.
    """
    matches = []
    for row in csv.reader(io.StringIO(text_body)):
        if len(row) <= 15:
            continue
        market_name = row[0].strip()
        if "VIX" not in market_name.upper():
            continue
        report_date = parse_date(row[2].strip())
        open_interest = clean_number(row[7])
        long_pos = clean_number(row[14])
        short_pos = clean_number(row[15])
        if report_date is None or open_interest is None or long_pos is None or short_pos is None or not open_interest:
            continue
        matches.append((report_date, market_name, long_pos, short_pos, open_interest))
    if not matches:
        return None
    report_date, market_name, long_pos, short_pos, open_interest = sorted(matches, key=lambda item: item[0])[-1]
    net_contracts = long_pos - short_pos
    return MarketClimateRecord(
        indicator_code="CFTC_VIX_LEV_NET",
        as_of_date=report_date,
        value=net_contracts / open_interest * 100,
        secondary_value=net_contracts,
        unit="%_oi",
        source="cftc_txt",
        payload={
            "market": market_name,
            "leveraged_funds_long": long_pos,
            "leveraged_funds_short": short_pos,
            "open_interest": open_interest,
            "fallback": "FinFutWk.txt",
        },
    )


def fetch_cftc_vix_record(session: requests.Session) -> MarketClimateRecord | None:
    try:
        resp = session.get(os.getenv("CFTC_TFF_JSON_URL", DEFAULT_CFTC_TFF_URL), timeout=30)
        resp.raise_for_status()
        data = resp.json()
        record = parse_cftc_vix_json(data if isinstance(data, list) else [])
        if record is not None:
            return record
    except requests.RequestException:
        pass

    txt = http_get_text(session, os.getenv("CFTC_TFF_TXT_URL", DEFAULT_CFTC_TFF_TXT_URL), timeout=30)
    return parse_cftc_vix_text(txt)


def build_gscpi_record(df: pd.DataFrame) -> MarketClimateRecord | None:
    if df is None or df.empty:
        return None
    frame = df.copy()
    frame.columns = [str(col).strip() for col in frame.columns]
    date_col = _first_matching_col(frame.columns, ["date", "month"])
    if date_col is None:
        date_col = frame.columns[0]
    value_col = _first_matching_col(frame.columns, ["gscpi", "supply chain pressure", "index"])
    if value_col is None or value_col == date_col:
        numeric_candidates = [
            col
            for col in frame.columns
            if col != date_col and pd.to_numeric(frame[col], errors="coerce").notna().any()
        ]
        value_col = numeric_candidates[-1] if numeric_candidates else None
    if value_col is None:
        return None
    out = frame[[date_col, value_col]].rename(columns={date_col: "as_of_date", value_col: "value"})
    out["as_of_date"] = pd.to_datetime(out["as_of_date"], errors="coerce")
    out["value"] = pd.to_numeric(out["value"], errors="coerce")
    out = out.dropna(subset=["as_of_date", "value"]).sort_values("as_of_date").reset_index(drop=True)
    if out.empty:
        return None
    latest = out.iloc[-1]
    latest_date = parse_date(latest["as_of_date"])
    latest_value = float(latest["value"])
    if latest_date is None:
        return None
    target = latest_date - timedelta(days=80)
    prior = out[out["as_of_date"].dt.date <= target]
    if prior.empty and len(out) >= 4:
        prior = out.iloc[:-3]
    change_3m = latest_value - float(prior.iloc[-1]["value"]) if not prior.empty else None
    return MarketClimateRecord(
        indicator_code="GSCPI",
        as_of_date=latest_date,
        value=latest_value,
        secondary_value=change_3m,
        unit="index",
        source="ny_fed",
        payload={"change_3m": change_3m},
    )


def parse_gscpi_workbook(content: bytes) -> MarketClimateRecord | None:
    sheets = pd.read_excel(io.BytesIO(content), sheet_name=None)
    for df in sheets.values():
        record = build_gscpi_record(df)
        if record is not None:
            return record
    return None


def fetch_gscpi_record(session: requests.Session) -> MarketClimateRecord | None:
    return parse_gscpi_workbook(http_get_bytes(session, os.getenv("NYFED_GSCPI_URL", DEFAULT_GSCPI_URL)))


def create_engine_from_env():
    load_dotenv(override=True)
    db_user = os.getenv("DB_USER")
    db_password = os.getenv("DB_PASSWORD")
    db_host = os.getenv("DB_HOST")
    db_port = os.getenv("DB_PORT", "3306")
    db_name = os.getenv("DB_NAME")
    if not all([db_user, db_password, db_host, db_name]):
        raise RuntimeError("数据库配置缺失，请检查 DB_USER/DB_PASSWORD/DB_HOST/DB_NAME")
    db_url = f"mysql+pymysql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
    return create_engine(db_url, pool_pre_ping=True, pool_recycle=3600)


def ensure_market_climate_table(engine) -> None:
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS market_climate_daily (
                    indicator_code VARCHAR(64) NOT NULL,
                    as_of_date DATE NOT NULL,
                    value DOUBLE NULL,
                    secondary_value DOUBLE NULL,
                    unit VARCHAR(32) NULL,
                    source VARCHAR(64) NULL,
                    payload_json TEXT NULL,
                    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                    PRIMARY KEY (indicator_code, as_of_date),
                    KEY idx_market_climate_code_date (indicator_code, as_of_date)
                )
                """
            )
        )


def save_records(engine, records: list[MarketClimateRecord]) -> int:
    if not records:
        return 0
    ensure_market_climate_table(engine)
    sql = text(
        """
        REPLACE INTO market_climate_daily
        (indicator_code, as_of_date, value, secondary_value, unit, source, payload_json)
        VALUES (:indicator_code, :as_of_date, :value, :secondary_value, :unit, :source, :payload_json)
        """
    )
    with engine.begin() as conn:
        conn.execute(sql, [record.db_params() for record in records])
    return len(records)


FETCHERS = {
    "vix_term": fetch_vix_term_record,
    "fedwatch": fetch_fedwatch_record,
    "aaii": fetch_aaii_sentiment_record,
    "cftc_vix": fetch_cftc_vix_record,
    "gscpi": fetch_gscpi_record,
}


def fetch_records(only: set[str] | None = None) -> tuple[list[MarketClimateRecord], dict[str, str]]:
    session = make_session()
    records: list[MarketClimateRecord] = []
    errors: dict[str, str] = {}
    for name, fetcher in FETCHERS.items():
        if only and name not in only:
            continue
        try:
            record = fetcher(session)
            if record is None:
                errors[name] = "no usable data"
                continue
            records.append(record)
        except Exception as exc:
            errors[name] = str(exc)
    return records, errors


def main() -> int:
    parser = argparse.ArgumentParser(description="Update cached market-climate indicators for the US options page.")
    parser.add_argument("--dry-run", action="store_true", help="Fetch and parse data without writing to the database.")
    parser.add_argument(
        "--only",
        default="",
        help="Comma-separated source names: vix_term,fedwatch,aaii,cftc_vix,gscpi",
    )
    args = parser.parse_args()

    only = {item.strip() for item in args.only.split(",") if item.strip()} or None
    records, errors = fetch_records(only)
    output = {
        "saved": 0,
        "records": [asdict(record) | {"as_of_date": record.as_of_date.isoformat()} for record in records],
        "errors": errors,
    }
    if not args.dry_run and records:
        engine = create_engine_from_env()
        output["saved"] = save_records(engine, records)
    print(json.dumps(output, ensure_ascii=False, indent=2, default=str))
    return 0 if records else 1


if __name__ == "__main__":
    raise SystemExit(main())
