import json
import time
import unittest
from unittest.mock import patch

import pandas as pd

_IMPORT_ERROR = None
try:
    import mobile_api
except Exception as exc:  # pragma: no cover
    mobile_api = None
    _IMPORT_ERROR = exc


class _FakeRedis:
    def __init__(self):
        self.data = {}

    def setex(self, key, ttl, value):
        self.data[key] = value
        return True

    def get(self, key):
        return self.data.get(key)

    def set(self, key, value, nx=False, ex=None):
        if nx and key in self.data:
            return False
        self.data[key] = value
        return True


@unittest.skipIf(mobile_api is None, f"mobile_api import failed: {_IMPORT_ERROR}")
class TestMobilePricesRefreshLoop(unittest.TestCase):
    def test_should_use_live_when_trading_day_is_newer_than_db(self):
        live = {"price": 621.0, "trading_day": "20260408"}
        out = mobile_api._should_use_live_contract_for_display(live, "20260407", fresh=False)
        self.assertTrue(out)

    def test_should_not_use_live_when_same_day_and_not_fresh(self):
        live = {"price": 621.0, "trading_day": "20260408"}
        out = mobile_api._should_use_live_contract_for_display(live, "20260408", fresh=False)
        self.assertFalse(out)

    def test_market_prices_touches_consumer_heartbeat(self):
        fake_redis = _FakeRedis()
        fake_redis.setex(
            mobile_api._PRICES_KEY,
            30,
            json.dumps({"items": [], "contracts": {}, "is_trading": False, "refreshed_at": ""}, ensure_ascii=False),
        )
        with patch.object(mobile_api, "_redis", fake_redis):
            mobile_api.market_prices(username="u1")

        self.assertIn(mobile_api._PRICES_CONSUMER_HEARTBEAT_KEY, fake_redis.data)

    def test_run_once_skips_fetch_without_consumer(self):
        with patch.object(mobile_api, "_is_trading_hours", return_value=True), patch.object(
            mobile_api, "_has_active_prices_consumer", return_value=False
        ), patch.object(mobile_api, "_fetch_sina_prices") as mocked_fetch:
            interval, outcome = mobile_api._run_prices_refresh_once(session=object())

        self.assertEqual(outcome, "refresh_skip_no_consumer")
        self.assertEqual(interval, float(mobile_api._PRICES_REFRESH_INTERVAL_IDLE_SEC))
        mocked_fetch.assert_not_called()

    def test_run_once_post_close_with_consumer_still_fetches(self):
        fake_redis = _FakeRedis()
        contracts = {
            "SC2605": {
                "open": 640.0,
                "high": 650.0,
                "low": 620.0,
                "price": 621.0,
                "pct": -12.5,
                "volume": 100,
                "updated_at": "",
            }
        }
        with patch.object(mobile_api, "_redis", fake_redis), patch.object(
            mobile_api, "_is_trading_hours", return_value=False
        ), patch.object(
            mobile_api, "_has_active_prices_consumer", return_value=True
        ), patch.object(
            mobile_api, "_is_post_close_capture_window", return_value=True
        ), patch.object(
            mobile_api, "_try_acquire_prices_refresh_lock", return_value=(True, "acquired")
        ), patch.object(
            mobile_api, "_fetch_sina_prices", return_value=contracts
        ), patch.object(
            mobile_api, "_contract_cache", ["SC2605"]
        ), patch.object(
            mobile_api, "_contract_cache_ts", time.time()
        ):
            interval, outcome = mobile_api._run_prices_refresh_once(session=object())

        self.assertEqual(outcome, "refresh_ok")
        self.assertEqual(interval, float(mobile_api._PRICES_REFRESH_INTERVAL_IDLE_SEC))
        saved = json.loads(fake_redis.data[mobile_api._PRICES_KEY])
        self.assertEqual(saved["contracts"]["SC2605"]["price"], 621.0)

    def test_run_once_skips_fetch_on_lock_miss(self):
        with patch.object(mobile_api, "_is_trading_hours", return_value=True), patch.object(
            mobile_api, "_has_active_prices_consumer", return_value=True
        ), patch.object(
            mobile_api, "_try_acquire_prices_refresh_lock", return_value=(False, "miss")
        ), patch.object(mobile_api, "_fetch_sina_prices") as mocked_fetch:
            interval, outcome = mobile_api._run_prices_refresh_once(session=object())

        self.assertEqual(outcome, "refresh_skip_lock_miss")
        self.assertEqual(interval, float(mobile_api._PRICES_REFRESH_INTERVAL_TRADING_SEC))
        mocked_fetch.assert_not_called()

    def test_try_acquire_lock_strict_redis_error(self):
        class _RaisingRedis:
            @staticmethod
            def set(*args, **kwargs):
                raise RuntimeError("redis down")

        with patch.object(mobile_api, "_redis", _RaisingRedis()), patch.object(
            mobile_api, "_PRICES_REQUIRE_REDIS_LOCK", True
        ):
            acquired, status = mobile_api._try_acquire_prices_refresh_lock()
        self.assertFalse(acquired)
        self.assertEqual(status, "redis_error_strict")

    def test_run_once_success_refresh_writes_prices_snapshot(self):
        fake_redis = _FakeRedis()
        contracts = {
            "PK2605": {
                "open": 8200.0,
                "high": 8300.0,
                "low": 8100.0,
                "price": 8250.0,
                "pct": 1.2,
                "volume": 12345,
                "updated_at": "",
            }
        }
        with patch.object(mobile_api, "_redis", fake_redis), patch.object(
            mobile_api, "_is_trading_hours", return_value=True
        ), patch.object(
            mobile_api, "_has_active_prices_consumer", return_value=True
        ), patch.object(
            mobile_api, "_try_acquire_prices_refresh_lock", return_value=(True, "acquired")
        ), patch.object(
            mobile_api, "_fetch_sina_prices", return_value=contracts
        ), patch.object(
            mobile_api, "_contract_cache", ["PK2605"]
        ), patch.object(
            mobile_api, "_contract_cache_ts", time.time()
        ):
            interval, outcome = mobile_api._run_prices_refresh_once(session=object())

        self.assertEqual(outcome, "refresh_ok")
        self.assertEqual(interval, float(mobile_api._PRICES_REFRESH_INTERVAL_TRADING_SEC))
        self.assertIn(mobile_api._PRICES_KEY, fake_redis.data)
        saved = json.loads(fake_redis.data[mobile_api._PRICES_KEY])
        self.assertIn("items", saved)
        self.assertTrue(any(x.get("code") == "pk" for x in saved.get("items", [])))

    def test_run_once_skip_no_consumer_keeps_last_snapshot_hot(self):
        fake_redis = _FakeRedis()
        seed_payload = {
            "items": [{"code": "sr", "name": "SR2605", "price": 5307, "pct": 0.12, "volume": 123, "updated_at": ""}],
            "contracts": {"SR2605": {"price": 5307, "pct": 0.12, "volume": 123, "trading_day": "20260407"}},
            "is_trading": False,
            "refreshed_at": "15:00:00",
            "refreshed_ts": int(time.time()),
        }
        with patch.object(mobile_api, "_redis", fake_redis), patch.object(
            mobile_api, "_is_trading_hours", return_value=False
        ), patch.object(
            mobile_api, "_has_active_prices_consumer", return_value=False
        ):
            mobile_api._save_last_prices_payload(seed_payload)
            interval, outcome = mobile_api._run_prices_refresh_once(session=object())

        self.assertEqual(outcome, "refresh_skip_no_consumer")
        self.assertEqual(interval, float(mobile_api._PRICES_REFRESH_INTERVAL_IDLE_SEC))
        self.assertIn(mobile_api._PRICES_KEY, fake_redis.data)

    def test_market_options_overlays_live_price_and_pct(self):
        df = pd.DataFrame([
            {
                "合约": "SR2605 (白糖)",
                "当前IV": 16.1,
                "IV Rank": 89,
                "IV变动(日)": 1.9,
                "涨跌%(日)": 0.0,
                "涨跌%(5日)": -2.28,
                "散户变动(日)": 0,
                "机构变动(日)": 0,
            }
        ])
        empty_df = pd.DataFrame()
        live_payload = {
            "items": [],
            "contracts": {"SR2605": {"price": 5317.0, "pct": 0.63, "trading_day": "20260407"}},
            "is_trading": False,
            "refreshed_at": "15:00:00",
            "refreshed_ts": int(time.time()),
        }
        with patch.object(mobile_api.de, "get_comprehensive_market_data", return_value=df), patch.object(
            mobile_api, "_get_option_product_codes", return_value={"sr"}
        ), patch.object(
            mobile_api, "_load_shared_prices_payload", return_value=live_payload
        ), patch(
            "pandas.read_sql", side_effect=[empty_df, empty_df]
        ):
            out = mobile_api.market_options(username="u1")

        self.assertTrue(out.get("items"))
        row = out["items"][0]
        self.assertEqual(row["cur_price"], 5317.0)
        self.assertEqual(row["pct_1d"], 0.63)

    def test_market_options_uses_stale_live_when_db_day_is_behind(self):
        df = pd.DataFrame([
            {
                "合约": "SC2605 (原油)",
                "当前IV": 0.0,
                "IV Rank": 97,
                "IV变动(日)": 0.0,
                "涨跌%(日)": 0.0,
                "涨跌%(5日)": -1.0,
                "散户变动(日)": 0,
                "机构变动(日)": 0,
            }
        ])
        iv_recent_df = pd.DataFrame()
        price_df = pd.DataFrame([
            {"code": "sc2605", "td": "20260407", "close_price": 710.0},
        ])
        stale_live_payload = {
            "items": [],
            "contracts": {"SC2605": {"price": 621.0, "pct": -12.31, "trading_day": "20260408"}},
            "is_trading": False,
            "refreshed_at": "15:00:00",
            "refreshed_ts": int(time.time()) - 99999,
        }
        with patch.object(mobile_api.de, "get_comprehensive_market_data", return_value=df), patch.object(
            mobile_api, "_get_option_product_codes", return_value={"sc"}
        ), patch.object(
            mobile_api, "_load_shared_prices_payload", return_value=stale_live_payload
        ), patch.object(
            mobile_api, "_PRICES_LIVE_OVERRIDE_MAX_AGE_SEC", 60
        ), patch(
            "pandas.read_sql", side_effect=[iv_recent_df, price_df]
        ):
            out = mobile_api.market_options(username="u1")

        self.assertTrue(out.get("items"))
        row = out["items"][0]
        self.assertEqual(row["cur_price"], 621.0)
        self.assertEqual(row["pct_1d"], -12.31)

    def test_market_options_keeps_rank_for_index_when_catalog_missing(self):
        df = pd.DataFrame([
            {
                "合约": "IF2606 (沪深300)",
                "当前IV": 16.89,
                "IV Rank": 9,
                "IV变动(日)": 0.0,
                "涨跌%(日)": -0.89,
                "涨跌%(5日)": 1.14,
                "散户变动(日)": 0,
                "机构变动(日)": 0,
            }
        ])
        iv_recent_df = pd.DataFrame([
            {"code": "if2606", "td": "20260408", "iv": 16.89},
            {"code": "if2606", "td": "20260407", "iv": 16.20},
        ])
        price_df = pd.DataFrame([
            {"code": "if2606", "td": "20260408", "close_price": 4499.0},
        ])
        with patch.object(mobile_api.de, "get_comprehensive_market_data", return_value=df), patch.object(
            mobile_api, "_get_option_product_codes", return_value=set()
        ), patch.object(
            mobile_api, "_load_shared_prices_payload", return_value={"items": [], "contracts": {}, "refreshed_ts": 0}
        ), patch(
            "pandas.read_sql", side_effect=[iv_recent_df, price_df]
        ):
            out = mobile_api.market_options(username="u1")

        self.assertTrue(out.get("items"))
        row = out["items"][0]
        self.assertEqual(row["iv_rank"], 9.0)
        self.assertEqual(row["iv_chg_1d"], 0.69)

    def test_fresh_live_contracts_map_drops_stale_snapshot(self):
        stale_payload = {
            "contracts": {"SC2605": {"price": 622.6, "pct": 0.0}},
            "refreshed_ts": int(time.time()) - 99999,
        }
        with patch.object(mobile_api, "_PRICES_LIVE_OVERRIDE_MAX_AGE_SEC", 60):
            out = mobile_api._get_fresh_live_contracts_map(stale_payload)
        self.assertEqual(out, {})


if __name__ == "__main__":
    unittest.main()
