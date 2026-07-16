import datetime

import pandas as pd
import pytest

import update_stock_tiingo as updater


class _FakeTiingoClient:
    def __init__(self, *, exchange="NASDAQ"):
        self.exchange = exchange

    def get_ticker_metadata(self, symbol):
        return {"ticker": symbol, "exchangeCode": self.exchange}

    def get_ticker_price(self, symbol, **_kwargs):
        return [
            {
                "date": "2026-06-11T00:00:00.000Z",
                "open": 2400.0,
                "high": 2420.0,
                "low": 2380.0,
                "close": 2410.0,
                "volume": 1000,
                "adjOpen": 240.0,
                "adjHigh": 242.0,
                "adjLow": 238.0,
                "adjClose": 241.0,
                "adjVolume": 10000,
                "splitFactor": 1.0,
            },
            {
                "date": "2026-06-12T00:00:00.000Z",
                "open": 250.0,
                "high": 260.0,
                "low": 248.0,
                "close": 254.0,
                "volume": 12000,
                "adjOpen": 250.0,
                "adjHigh": 260.0,
                "adjLow": 248.0,
                "adjClose": 254.0,
                "adjVolume": 12000,
                "splitFactor": 10.0,
            },
        ]


def _provider(client):
    provider = updater.TiingoProvider.__new__(updater.TiingoProvider)
    updater.BaseProvider.__init__(
        provider,
        name="tiingo",
        sleep_seconds=0,
        cooldown_seconds=0,
        max_retries=0,
    )
    provider.api_key = "test"
    provider.client = client
    provider._metadata_cache = {}
    return provider


def test_tiingo_adapter_persists_adjusted_ohlcv_and_split_factor():
    provider = _provider(_FakeTiingoClient())

    frame = provider._fetch_impl("KLAC", datetime.date(2026, 6, 1))

    assert frame["close"].tolist() == [241.0, 254.0]
    assert frame["volume"].tolist() == [10000, 12000]
    assert frame["adjClose"].tolist() == [241.0, 254.0]
    assert frame["splitFactor"].tolist() == [1.0, 10.0]
    assert updater._has_split_event(frame)


def test_tiingo_rejects_same_ticker_non_us_listing():
    provider = _provider(_FakeTiingoClient(exchange="TSX"))

    with pytest.raises(updater.ProviderError, match="non-US listing rejected"):
        provider._fetch_impl("BK", datetime.date(2026, 6, 1))


def test_incremental_start_keeps_overlap_for_split_detection(monkeypatch):
    monkeypatch.setattr(updater, "US_INCREMENTAL_LOOKBACK_DAYS", 7)
    today = datetime.date(2026, 7, 14)

    start = updater._get_incremental_start(datetime.datetime(2026, 7, 14), today)

    assert start == datetime.date(2026, 7, 7)


def test_split_ratio_match_does_not_accept_ordinary_price_move():
    assert updater._nearest_common_split_ratio(0.1) == pytest.approx(0.1)
    assert updater._nearest_common_split_ratio(4.0) == pytest.approx(4.0)
    assert updater._nearest_common_split_ratio(0.80) is None


def test_unadjusted_fallback_history_is_rejected_for_full_repair():
    frame = pd.DataFrame({"close": [1000.0, 1010.0, 105.0, 108.0]})
    clean = pd.DataFrame({"close": [100.0, 101.0, 105.0, 108.0]})

    assert updater._has_unresolved_scale_break(frame)
    assert not updater._has_unresolved_scale_break(clean)


def test_default_source_priority_uses_adjusted_provider_first():
    assert updater.US_SOURCE_PRIORITY[0] == "tiingo"
    assert updater.US_BACKFILL_SOURCE_PRIORITY[0] == "tiingo"
