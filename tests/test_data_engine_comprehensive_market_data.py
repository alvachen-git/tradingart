import unittest
import tempfile
from pathlib import Path
from unittest.mock import patch

import pandas as pd

import data_engine


class ComprehensiveMarketDataSnapshotTests(unittest.TestCase):
    def setUp(self):
        self._tmpdir = tempfile.TemporaryDirectory()
        self._disk_cache_patch = patch.object(
            data_engine,
            "_MARKET_DISK_SNAPSHOT_PATH",
            Path(self._tmpdir.name) / "market_snapshot.json",
        )
        self._disk_cache_patch.start()
        data_engine.clear_comprehensive_market_data_snapshot()

    def tearDown(self):
        data_engine.clear_comprehensive_market_data_snapshot()
        self._disk_cache_patch.stop()
        self._tmpdir.cleanup()

    def _clear_memory_snapshot_only(self):
        with data_engine._MARKET_SNAPSHOT_LOCK:
            data_engine._MARKET_SNAPSHOT_LAST_DF = None
            data_engine._MARKET_SNAPSHOT_LAST_AT = 0.0

    def _fake_read_sql(self, sql, engine, params=None):
        sql_text = str(sql)

        if "SELECT DISTINCT trade_date FROM futures_price" in sql_text:
            return pd.DataFrame(
                {
                    "trade_date": [
                        "2026-04-28",
                        "2026-04-27",
                        "2026-04-24",
                        "2026-04-23",
                        "2026-04-22",
                        "2026-04-21",
                    ]
                }
            )

        if "FROM futures_price" in sql_text and "REPLACE(trade_date, '-', '') IN" in sql_text:
            return pd.DataFrame(
                [
                    {"ts_code": "MA2609", "close_price": 200.0, "oi": 1200.0, "trade_date": "2026-04-28"},
                    {"ts_code": "MA2606", "close_price": 198.0, "oi": 900.0, "trade_date": "2026-04-28"},
                    {"ts_code": "RB2608", "close_price": 300.0, "oi": 100.0, "trade_date": "2026-04-28"},
                    {"ts_code": "RB2609", "close_price": 302.0, "oi": 200.0, "trade_date": "2026-04-28"},
                    {"ts_code": "RB2610", "close_price": 304.0, "oi": 1000.0, "trade_date": "2026-04-28"},
                    {"ts_code": "HC2609", "close_price": 310.0, "oi": 500.0, "trade_date": "2026-04-28"},
                    {"ts_code": "MA2609", "close_price": 190.0, "oi": 1100.0, "trade_date": "2026-04-27"},
                    {"ts_code": "MA2606", "close_price": 188.0, "oi": 880.0, "trade_date": "2026-04-27"},
                    {"ts_code": "RB2608", "close_price": 298.0, "oi": 90.0, "trade_date": "2026-04-27"},
                    {"ts_code": "RB2609", "close_price": 299.0, "oi": 190.0, "trade_date": "2026-04-27"},
                    {"ts_code": "RB2610", "close_price": 301.0, "oi": 950.0, "trade_date": "2026-04-27"},
                    {"ts_code": "HC2609", "close_price": 308.0, "oi": 480.0, "trade_date": "2026-04-27"},
                    {"ts_code": "MA2609", "close_price": 180.0, "oi": 1000.0, "trade_date": "2026-04-21"},
                    {"ts_code": "MA2606", "close_price": 178.0, "oi": 860.0, "trade_date": "2026-04-21"},
                    {"ts_code": "RB2608", "close_price": 296.0, "oi": 80.0, "trade_date": "2026-04-21"},
                    {"ts_code": "RB2609", "close_price": 297.0, "oi": 180.0, "trade_date": "2026-04-21"},
                    {"ts_code": "RB2610", "close_price": 300.0, "oi": 900.0, "trade_date": "2026-04-21"},
                    {"ts_code": "HC2609", "close_price": 306.0, "oi": 460.0, "trade_date": "2026-04-21"},
                    {"ts_code": "TA2609", "close_price": 100.0, "oi": 1500.0, "trade_date": "2026-04-27"},
                    {"ts_code": "TA2606", "close_price": 98.0, "oi": 1000.0, "trade_date": "2026-04-27"},
                    {"ts_code": "TA2609", "close_price": 95.0, "oi": 1400.0, "trade_date": "2026-04-24"},
                    {"ts_code": "TA2606", "close_price": 96.0, "oi": 950.0, "trade_date": "2026-04-24"},
                ]
            )

        if "FROM commodity_iv_history" in sql_text:
            return pd.DataFrame(
                [
                    {"ts_code": "MA", "iv": 20.0, "trade_date": "2026-04-21"},
                    {"ts_code": "MA", "iv": 40.0, "trade_date": "2026-04-27"},
                    {"ts_code": "MA2609", "iv": 25.0, "trade_date": "2026-04-28"},
                    {"ts_code": "MA2609", "iv": 30.0, "trade_date": "2026-04-27"},
                    {"ts_code": "MA2609", "iv": 35.0, "trade_date": "2026-04-21"},
                    {"ts_code": "MA2606", "iv": 31.0, "trade_date": "2026-04-28"},
                    {"ts_code": "MA2606", "iv": 27.0, "trade_date": "2026-04-27"},
                    {"ts_code": "MA2606", "iv": 24.0, "trade_date": "2026-04-21"},
                    {"ts_code": "RB2609", "iv": 12.0, "trade_date": "2026-04-28"},
                    {"ts_code": "RB2609", "iv": 11.0, "trade_date": "2026-04-27"},
                    {"ts_code": "RB2610", "iv": 13.0, "trade_date": "2026-04-28"},
                    {"ts_code": "RB2610", "iv": 12.5, "trade_date": "2026-04-27"},
                    {"ts_code": "TA2609", "iv": 22.0, "trade_date": "2026-04-27"},
                    {"ts_code": "TA2609", "iv": 20.0, "trade_date": "2026-04-24"},
                    {"ts_code": "TA2606", "iv": 18.0, "trade_date": "2026-04-27"},
                    {"ts_code": "TA2606", "iv": 16.0, "trade_date": "2026-04-24"},
                ]
            )

        if "FROM futures_holding" in sql_text:
            return pd.DataFrame(columns=["ts_code", "broker", "long_vol", "short_vol", "trade_date"])

        raise AssertionError(f"Unexpected SQL in test: {sql_text}")

    def test_product_uses_own_latest_trade_day_when_global_latest_missing(self):
        with patch.object(data_engine, "engine", object()), \
             patch.object(data_engine, "check_expiry_validity", return_value=True), \
             patch.object(data_engine.pd, "read_sql", side_effect=self._fake_read_sql):
            df = data_engine.get_comprehensive_market_data()

        self.assertFalse(df.empty)
        ta_rows = df[df["合约"].astype(str).str.contains("PTA", na=False)]
        self.assertFalse(ta_rows.empty, "PTA should remain visible when it lacks the global latest trade day")

        ta_main = ta_rows[ta_rows["合约"] == "TA2609 (PTA)"]
        self.assertFalse(ta_main.empty)
        self.assertAlmostEqual(float(ta_main.iloc[0]["涨跌%(日)"]), round((100.0 - 95.0) / 95.0 * 100, 2))

    def test_iv_rank_prefers_product_continuous_history(self):
        with patch.object(data_engine, "engine", object()), \
             patch.object(data_engine, "check_expiry_validity", return_value=True), \
             patch.object(data_engine.pd, "read_sql", side_effect=self._fake_read_sql):
            df = data_engine.get_comprehensive_market_data()

        ma_main = df[df["合约"] == "MA2609 (甲醇)"]
        self.assertFalse(ma_main.empty)
        self.assertEqual(ma_main.iloc[0]["IV Rank"], 25)

    def test_near_contract_prefers_available_iv_over_blank_near_month(self):
        with patch.object(data_engine, "engine", object()), \
             patch.object(data_engine, "check_expiry_validity", return_value=True), \
             patch.object(data_engine.pd, "read_sql", side_effect=self._fake_read_sql):
            df = data_engine.get_comprehensive_market_data()

        labels = set(df["合约"].astype(str).tolist())
        self.assertIn("RB2610 (螺纹钢)", labels)
        self.assertIn("RB2609 (螺纹钢)", labels)
        self.assertNotIn("RB2608 (螺纹钢)", labels)

    def test_missing_iv_outputs_na_rank_instead_of_zero(self):
        with patch.object(data_engine, "engine", object()), \
             patch.object(data_engine, "check_expiry_validity", return_value=True), \
             patch.object(data_engine.pd, "read_sql", side_effect=self._fake_read_sql):
            df = data_engine.get_comprehensive_market_data()

        hc = df[df["合约"] == "HC2609 (热卷)"]
        self.assertFalse(hc.empty)
        self.assertTrue(pd.isna(hc.iloc[0]["当前IV"]))
        self.assertEqual(hc.iloc[0]["IV Rank"], "N/A")

    def test_result_carries_latest_data_date_for_ui_header(self):
        with patch.object(data_engine, "engine", object()), \
             patch.object(data_engine, "check_expiry_validity", return_value=True), \
             patch.object(data_engine.pd, "read_sql", side_effect=self._fake_read_sql):
            df = data_engine.get_comprehensive_market_data()

        self.assertIn("_数据日期", df.columns)
        self.assertEqual(set(df["_数据日期"].dropna().astype(str)), {"20260428"})

    def test_disk_snapshot_serves_after_process_memory_is_empty(self):
        with patch.object(data_engine, "engine", object()), \
             patch.object(data_engine, "check_expiry_validity", return_value=True), \
             patch.object(data_engine.pd, "read_sql", side_effect=self._fake_read_sql):
            first_df = data_engine.get_comprehensive_market_data()

        self.assertFalse(first_df.empty)
        self.assertTrue(data_engine._MARKET_DISK_SNAPSHOT_PATH.exists())

        self._clear_memory_snapshot_only()
        with patch.object(data_engine, "engine", object()), \
             patch.object(data_engine.pd, "read_sql", side_effect=AssertionError("disk hit should not query SQL")):
            second_df = data_engine.get_comprehensive_market_data()

        self.assertEqual(len(second_df), len(first_df))
        self.assertEqual(set(second_df["_数据日期"].dropna().astype(str)), {"20260428"})

    def test_clear_market_snapshot_removes_disk_snapshot(self):
        with patch.object(data_engine, "engine", object()), \
             patch.object(data_engine, "check_expiry_validity", return_value=True), \
             patch.object(data_engine.pd, "read_sql", side_effect=self._fake_read_sql):
            df = data_engine.get_comprehensive_market_data()

        self.assertFalse(df.empty)
        self.assertTrue(data_engine._MARKET_DISK_SNAPSHOT_PATH.exists())

        data_engine.clear_comprehensive_market_data_snapshot()

        self.assertFalse(data_engine._MARKET_DISK_SNAPSHOT_PATH.exists())


if __name__ == "__main__":
    unittest.main()
