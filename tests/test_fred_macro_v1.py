import importlib
import os
from datetime import datetime, timedelta

import pandas as pd


os.environ.setdefault("DB_USER", "test")
os.environ.setdefault("DB_PASSWORD", "test")
os.environ.setdefault("DB_HOST", "localhost")
os.environ.setdefault("DB_PORT", "3306")
os.environ.setdefault("DB_NAME", "test")

update_micro_daily = importlib.import_module("update_micro_daily")
macro_tools = importlib.import_module("macro_tools")


class DummyConn:
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class DummyEngine:
    def connect(self):
        return DummyConn()


def test_fred_core_series_codes_complete():
    expected = {
        "FEDFUNDS",
        "SOFR",
        "DGS2",
        "DGS10",
        "T10Y3M",
        "CPIAUCSL",
        "PCEPILFE",
        "DFII10",
        "UNRATE",
        "PAYEMS",
        "BAMLH0A0HYM2",
        "WALCL",
    }
    assert set(update_micro_daily.FRED_CORE_SERIES.keys()) == expected


def test_stale_flag_thresholds_by_frequency():
    now = datetime.now()
    stale_flag_d, stale_days_d = update_micro_daily._get_stale_flag(now - timedelta(days=8), "D")
    stale_flag_w, stale_days_w = update_micro_daily._get_stale_flag(now - timedelta(days=8), "W")
    stale_flag_missing, stale_days_missing = update_micro_daily._get_stale_flag(None, "D")

    assert stale_flag_d == "Y"
    assert stale_days_d >= 8
    assert stale_flag_w == "N"
    assert stale_days_w >= 8
    assert stale_flag_missing == "UNKNOWN"
    assert stale_days_missing == -1


def test_fetch_fred_core_macro_partial_success(monkeypatch):
    monkeypatch.setattr(update_micro_daily, "FRED_API_KEY", "dummy-key")
    monkeypatch.setattr(
        update_micro_daily,
        "FRED_CORE_SERIES",
        {
            "DGS10": {
                "series_id": "DGS10",
                "name": "美国10年期国债收益率(FRED)",
                "category": "bond",
                "frequency": "D",
                "unit": "%",
            },
            "WALCL": {
                "series_id": "WALCL",
                "name": "美联储总资产",
                "category": "liquidity",
                "frequency": "W",
                "unit": "million_usd",
            },
        },
    )
    monkeypatch.setattr(update_micro_daily, "_upsert_indicator_meta", lambda _: None)

    df_ok = pd.DataFrame(
        {
            "trade_date": [datetime.now() - timedelta(days=2), datetime.now() - timedelta(days=1)],
            "close_value": [4.12, 4.16],
        }
    )

    def fake_retry(name, fn, attempts=3, base_sleep=1.5):
        if "DGS10" in name:
            return df_ok
        raise RuntimeError("mock failure")

    monkeypatch.setattr(update_micro_daily, "_retry_call", fake_retry)

    results, ok_codes, fail_codes = update_micro_daily.fetch_fred_core_macro()

    assert ok_codes == ["DGS10"]
    assert fail_codes == ["WALCL"]
    assert "DGS10" in results
    assert not results["DGS10"]["df"].empty


def test_macro_freshness_helper():
    now = datetime.now()
    status_d, stale_days_d, threshold_d = macro_tools._freshness(now - timedelta(days=9), "D")
    status_m, stale_days_m, threshold_m = macro_tools._freshness(now - timedelta(days=20), "M")

    assert status_d == "stale"
    assert stale_days_d >= 9
    assert threshold_d == 7

    assert status_m == "fresh"
    assert stale_days_m >= 20
    assert threshold_m == 45


def test_get_macro_indicator_contains_source_and_freshness(monkeypatch):
    monkeypatch.setattr(macro_tools, "engine", DummyEngine())
    monkeypatch.setattr(
        macro_tools,
        "_load_meta_from_db",
        lambda: {"DGS10": {"source": "fred", "frequency": "D", "unit": "%"}},
    )

    def fake_read_sql(sql, conn, params=None):
        code = params["code"]
        if code == "DGS10":
            return pd.DataFrame(
                {
                    "trade_date": [datetime.now() - timedelta(days=20), datetime.now() - timedelta(days=21)],
                    "indicator_name": ["美国10年期国债收益率(FRED)", "美国10年期国债收益率(FRED)"],
                    "category": ["bond", "bond"],
                    "close_value": [4.12, 4.03],
                    "change_value": [0.09, -0.01],
                    "change_pct": [2.23, -0.24],
                }
            )
        return pd.DataFrame()

    monkeypatch.setattr(macro_tools.pd, "read_sql", fake_read_sql)

    out = macro_tools.get_macro_indicator.invoke({"indicator_code": "DGS10,UNKNOWN", "days": 30})

    assert "source:" in out
    assert "as_of_date:" in out
    assert "freshness_status:" in out
    assert "stale_days:" in out
    assert "UNKNOWN" in out
    assert "missing" in out


def test_get_macro_health_snapshot_reports_missing(monkeypatch):
    monkeypatch.setattr(macro_tools, "engine", DummyEngine())
    monkeypatch.setattr(
        macro_tools,
        "_load_meta_from_db",
        lambda: {"FEDFUNDS": {"source": "fred", "frequency": "M", "unit": "%"}},
    )

    def fake_read_sql(sql, conn, params=None):
        code = params["code"]
        if code == "FEDFUNDS":
            return pd.DataFrame(
                {
                    "indicator_name": ["联邦基金利率"],
                    "category": ["bond"],
                    "close_value": [4.33],
                    "trade_date": [datetime.now() - timedelta(days=35)],
                }
            )
        return pd.DataFrame()

    monkeypatch.setattr(macro_tools.pd, "read_sql", fake_read_sql)

    out = macro_tools.get_macro_health_snapshot.invoke({"indicator_code": "FEDFUNDS,SOFR"})

    assert "宏观健康快照" in out
    assert "FEDFUNDS" in out
    assert "SOFR" in out
    assert "异常与建议" in out
