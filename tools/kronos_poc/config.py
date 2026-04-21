from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Tuple


ROOT_DIR = Path(__file__).resolve().parents[2]
CACHE_DIR = ROOT_DIR / ".kronos_poc_cache"

DEFAULT_LOOKBACK = 120
DEFAULT_HORIZON = 3
DEFAULT_QUANTILES: Tuple[float, ...] = (0.1, 0.5, 0.9)
MIN_SAFETY_MARGIN = 20
SERVICE_HOST = "0.0.0.0"
SERVICE_PORT = 8791
SERVICE_BASE_URL = f"http://127.0.0.1:{SERVICE_PORT}"
MODEL_VERSION = "kronos-pretrained-v0"
PREPROCESS_VERSION = "ohlcv-v2-core60"


@dataclass(frozen=True)
class PilotAsset:
    key: str
    label: str
    asset_type: str  # index | future | etf
    db_code: str
    legacy_code: str | None = None
    risk_note: str | None = None


PILOT_ASSETS = (
    PilotAsset("hs300", "沪深300指数", "index", "000300.SH"),
    PilotAsset("zz1000", "中证1000指数", "index", "000852.SH"),
    PilotAsset("cyb", "创业板指数", "index", "399006.SZ"),
    PilotAsset("kc50etf", "科创50ETF", "etf", "588000.SH"),
    PilotAsset("sz50etf", "上证50ETF", "etf", "510050.SH"),
    PilotAsset("au", "黄金", "future", "AU", legacy_code="au0"),
    PilotAsset("sc", "原油", "future", "SC", legacy_code="sc0", risk_note="事件驱动较强，预测区间可能快速失效。"),
    PilotAsset("cu", "铜", "future", "CU", legacy_code="cu0"),
    PilotAsset("ta", "PTA", "future", "TA", legacy_code="ta0"),
)

ASSET_BY_KEY: Dict[str, PilotAsset] = {a.key: a for a in PILOT_ASSETS}
ASSET_BY_LABEL: Dict[str, PilotAsset] = {a.label: a for a in PILOT_ASSETS}
ASSET_BY_DB_CODE: Dict[str, PilotAsset] = {a.db_code.upper(): a for a in PILOT_ASSETS}


def resolve_pilot_asset(value: str) -> PilotAsset:
    if not value:
        raise KeyError("empty symbol")
    v = str(value).strip()
    if v in ASSET_BY_KEY:
        return ASSET_BY_KEY[v]
    if v in ASSET_BY_LABEL:
        return ASSET_BY_LABEL[v]
    upper = v.upper()
    if upper in ASSET_BY_DB_CODE:
        return ASSET_BY_DB_CODE[upper]
    raise KeyError(v)
