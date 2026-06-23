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
        try:
            data_engine.get_static_maturity_map.clear()
        except Exception:
            pass

    def tearDown(self):
        data_engine.clear_comprehensive_market_data_snapshot()
        try:
            data_engine.get_static_maturity_map.clear()
        except Exception:
            pass
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

        if "FROM commodity_option_basic" in sql_text:
            return pd.DataFrame(
                [
                    {"ts_code": "MA2609", "maturity_date": "20260818"},
                    {"ts_code": "MA2606", "maturity_date": "2026-08-18"},
                    {"ts_code": "RB2609", "maturity_date": "2026-08-18"},
                    {"ts_code": "RB2610", "maturity_date": "2026-08-18"},
                    {"ts_code": "TA2609", "maturity_date": "2026-08-18"},
                    {"ts_code": "TA2606", "maturity_date": "2026-08-18"},
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

    def test_result_carries_option_expiry_days_from_data_date(self):
        with patch.object(data_engine, "engine", object()), \
             patch.object(data_engine, "check_expiry_validity", return_value=True), \
             patch.object(data_engine, "get_static_maturity_map", return_value={"MA2609": "2026-05-08"}), \
             patch.object(data_engine.pd, "read_sql", side_effect=self._fake_read_sql):
            df = data_engine.get_comprehensive_market_data()

        ma_main = df[df["合约"] == "MA2609 (甲醇)"]
        self.assertFalse(ma_main.empty)
        self.assertIn("期权到期日", df.columns)
        self.assertIn("到期剩余天数", df.columns)
        self.assertEqual(ma_main.iloc[0]["期权到期日"], "2026-05-08")
        self.assertEqual(int(ma_main.iloc[0]["到期剩余天数"]), 10)

    def test_missing_option_expiry_stays_blank_without_days(self):
        with patch.object(data_engine, "engine", object()), \
             patch.object(data_engine, "check_expiry_validity", return_value=True), \
             patch.object(data_engine, "get_static_maturity_map", return_value={}), \
             patch.object(data_engine.pd, "read_sql", side_effect=self._fake_read_sql):
            df = data_engine.get_comprehensive_market_data()

        hc = df[df["合约"] == "HC2609 (热卷)"]
        self.assertFalse(hc.empty)
        self.assertEqual(hc.iloc[0]["期权到期日"], "")
        self.assertTrue(pd.isna(hc.iloc[0]["到期剩余天数"]))

    def test_expiry_day_calculation_accepts_compact_and_dashed_dates(self):
        dashed_date, dashed_days = data_engine._get_contract_expiry_fields(
            "M2609", "20260428", {"M2609": "2026-05-01"}
        )
        compact_date, compact_days = data_engine._get_contract_expiry_fields(
            "M2609", "2026-04-28", {"M2609": "20260501"}
        )

        self.assertEqual(dashed_date, "2026-05-01")
        self.assertEqual(dashed_days, 3)
        self.assertEqual(compact_date, "2026-05-01")
        self.assertEqual(compact_days, 3)

    def test_expiry_day_calculation_accepts_index_option_alias(self):
        expiry_date, days_left = data_engine._get_contract_expiry_fields(
            "IF2609", "20260901", {"IO2609": "2026-09-18"}
        )

        self.assertEqual(expiry_date, "2026-09-18")
        self.assertEqual(days_left, 17)

    def test_expiry_day_calculation_marks_less_than_three_day_window(self):
        for maturity_date, expected_days in [
            ("2026-04-28", 0),
            ("2026-04-29", 1),
            ("2026-04-30", 2),
        ]:
            with self.subTest(maturity_date=maturity_date):
                _, days_left = data_engine._get_contract_expiry_fields(
                    "RB2610", "20260428", {"RB2610": maturity_date}
                )
                self.assertLess(days_left, 3)
                self.assertEqual(days_left, expected_days)

        _, days_left = data_engine._get_contract_expiry_fields(
            "RB2610", "20260428", {"RB2610": "2026-05-01"}
        )
        self.assertEqual(days_left, 3)

    def test_expiry_validity_prefers_real_maturity_date_over_estimate(self):
        maturity_map = {
            "AU2607": "2026-06-24",
            "CU2607": "2026-06-24",
        }
        for join_key in ["AU2607", "CU2607"]:
            with self.subTest(join_key=join_key):
                row = {"join_key": join_key, "product": join_key[:2].rstrip("2")}
                self.assertTrue(data_engine.check_expiry_validity(row, "20260622", maturity_map))
                self.assertFalse(data_engine.check_expiry_validity(row, "20260625", maturity_map))

    def test_static_maturity_map_normalizes_zce_three_digit_month_code(self):
        self.assertEqual(data_engine._extract_option_maturity_join_key("TA609C4000.ZCE"), "TA2609")
        self.assertEqual(data_engine._extract_option_maturity_join_key("MA609P2000.ZCE"), "MA2609")
        self.assertEqual(data_engine._extract_option_maturity_join_key("SR603MSC4600.ZCE"), "SR2603")
        self.assertEqual(data_engine._extract_option_maturity_join_key("IO2609-C-4200.CFX"), "IO2609")

        def fake_read_sql(sql, engine, params=None):
            sql_text = str(sql)
            if "FROM commodity_option_basic" in sql_text:
                return pd.DataFrame(
                    [
                        {"ts_code": "TA609C4000.ZCE", "maturity_date": "20260812"},
                        {"ts_code": "MA609P2000.ZCE", "maturity_date": "2026-08-12"},
                        {"ts_code": "IO2609-C-4200.CFX", "maturity_date": "2026-09-18"},
                        {"ts_code": "HO2609-P-3000.CFX", "maturity_date": "2026-09-18"},
                        {"ts_code": "MO2609-C-6000.CFX", "maturity_date": "2026-09-18"},
                    ]
                )
            raise AssertionError(f"Unexpected SQL in test: {sql_text}")

        data_engine.get_static_maturity_map.clear()
        with patch.object(data_engine, "get_db_engine", return_value=object()), \
             patch.object(data_engine.pd, "read_sql", side_effect=fake_read_sql):
            maturity_map = data_engine.get_static_maturity_map()

        self.assertEqual(maturity_map["TA2609"].strftime("%Y-%m-%d"), "2026-08-12")
        self.assertEqual(maturity_map["MA2609"].strftime("%Y-%m-%d"), "2026-08-12")
        self.assertEqual(maturity_map["IF2609"].strftime("%Y-%m-%d"), "2026-09-18")
        self.assertEqual(maturity_map["IH2609"].strftime("%Y-%m-%d"), "2026-09-18")
        self.assertEqual(maturity_map["IM2609"].strftime("%Y-%m-%d"), "2026-09-18")
        self.assertNotIn("IC2609", maturity_map)

    def test_real_maturity_keeps_shfe_july_contract_until_actual_expiry(self):
        def fake_read_sql(sql, engine, params=None):
            sql_text = str(sql)
            trade_dates = [
                "2026-06-22",
                "2026-06-19",
                "2026-06-18",
                "2026-06-17",
                "2026-06-16",
                "2026-06-15",
            ]
            if "SELECT DISTINCT trade_date FROM futures_price" in sql_text:
                return pd.DataFrame({"trade_date": trade_dates})

            if "FROM futures_price" in sql_text and "REPLACE(trade_date, '-', '') IN" in sql_text:
                rows = []
                for idx, trade_date in enumerate(trade_dates):
                    rows.extend(
                        [
                            {
                                "ts_code": "AU2607",
                                "close_price": 500.0 - idx,
                                "oi": 1200.0 - idx,
                                "trade_date": trade_date,
                            },
                            {
                                "ts_code": "AU2608",
                                "close_price": 510.0 - idx,
                                "oi": 3000.0 - idx,
                                "trade_date": trade_date,
                            },
                        ]
                    )
                return pd.DataFrame(rows)

            if "FROM commodity_iv_history" in sql_text:
                return pd.DataFrame(
                    [
                        {"ts_code": "AU2607", "iv": 20.0, "trade_date": "2026-06-15"},
                        {"ts_code": "AU2607", "iv": 22.0, "trade_date": "2026-06-19"},
                        {"ts_code": "AU2607", "iv": 24.0, "trade_date": "2026-06-22"},
                        {"ts_code": "AU2608", "iv": 18.0, "trade_date": "2026-06-15"},
                        {"ts_code": "AU2608", "iv": 19.0, "trade_date": "2026-06-19"},
                        {"ts_code": "AU2608", "iv": 21.0, "trade_date": "2026-06-22"},
                    ]
                )

            if "FROM commodity_option_basic" in sql_text:
                return pd.DataFrame(
                    [
                        {"ts_code": "AU2607C500.SHF", "maturity_date": "2026-06-24"},
                        {"ts_code": "AU2608C500.SHF", "maturity_date": "2026-07-27"},
                    ]
                )

            if "FROM futures_holding" in sql_text:
                return pd.DataFrame(columns=["ts_code", "broker", "long_vol", "short_vol", "trade_date"])

            raise AssertionError(f"Unexpected SQL in test: {sql_text}")

        data_engine.get_static_maturity_map.clear()
        with patch.object(data_engine, "engine", object()), \
             patch.object(data_engine, "get_db_engine", return_value=object()), \
             patch.object(data_engine.pd, "read_sql", side_effect=fake_read_sql):
            df = data_engine.get_comprehensive_market_data()

        au_july = df[df["合约"] == "AU2607 (黄金)"]
        self.assertFalse(au_july.empty)
        self.assertEqual(au_july.iloc[0]["期权到期日"], "2026-06-24")
        self.assertEqual(int(au_july.iloc[0]["到期剩余天数"]), 2)

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

    def test_market_monitor_iv_trend_prefers_exact_contract(self):
        with patch.object(data_engine, "engine", object()), \
             patch.object(data_engine.pd, "read_sql", side_effect=self._fake_read_sql):
            trend = data_engine.get_market_monitor_iv_trend("MA2609 (甲醇)", points=5)

        self.assertFalse(trend.empty)
        self.assertEqual(set(trend["source"]), {"contract"})
        self.assertEqual(trend["trade_date"].tolist(), ["20260421", "20260427", "20260428"])
        self.assertEqual(trend["iv"].tolist(), [35.0, 30.0, 25.0])

    def test_market_monitor_iv_trend_falls_back_to_product_history(self):
        with patch.object(data_engine, "engine", object()), \
             patch.object(data_engine.pd, "read_sql", side_effect=self._fake_read_sql):
            trend = data_engine.get_market_monitor_iv_trend("MA2610 (甲醇)", points=5)

        self.assertFalse(trend.empty)
        self.assertEqual(set(trend["source"]), {"product"})
        self.assertEqual(trend["trade_date"].tolist(), ["20260421", "20260427"])
        self.assertEqual(trend["iv"].tolist(), [20.0, 40.0])

    def test_market_monitor_iv_trend_falls_back_to_suffixed_contract_code(self):
        def fake_read_sql(sql, engine, params=None):
            sql_text = str(sql)
            if "UPPER(ts_code) IN" in sql_text:
                return pd.DataFrame(columns=["ts_code", "iv", "trade_date"])
            if "FROM commodity_iv_history" in sql_text:
                return pd.DataFrame(
                    [
                        {"ts_code": "CU2609.SHFE", "iv": 18.0, "trade_date": "2026-04-24"},
                        {"ts_code": "CU2609.SHFE", "iv": 20.0, "trade_date": "2026-04-27"},
                        {"ts_code": "CU2609.SHFE", "iv": 21.0, "trade_date": "2026-04-28"},
                    ]
                )
            raise AssertionError(f"Unexpected SQL in test: {sql_text}")

        with patch.object(data_engine, "engine", object()), \
             patch.object(data_engine.pd, "read_sql", side_effect=fake_read_sql):
            trend = data_engine.get_market_monitor_iv_trend("CU2609 (沪铜)", points=5)

        self.assertFalse(trend.empty)
        self.assertEqual(set(trend["source"]), {"contract"})
        self.assertEqual(trend["trade_date"].tolist(), ["20260424", "20260427", "20260428"])
        self.assertEqual(trend["iv"].tolist(), [18.0, 20.0, 21.0])

    def test_market_monitor_holding_trend_aggregates_dumb_and_smart_brokers(self):
        def fake_read_sql(sql, engine, params=None):
            sql_text = str(sql)
            if "FROM futures_holding" in sql_text:
                rows = []
                for idx, trade_date in enumerate(
                    ["2026-04-21", "2026-04-22", "2026-04-23", "2026-04-24", "2026-04-27", "2026-04-28"]
                ):
                    rows.extend(
                        [
                            {
                                "ts_code": "M2609",
                                "broker": "东方财富",
                                "long_vol": 100 + idx * 10,
                                "short_vol": 20,
                                "trade_date": trade_date,
                            },
                            {
                                "ts_code": "M2609",
                                "broker": "国泰君安",
                                "long_vol": 200 + idx * 20,
                                "short_vol": 50,
                                "trade_date": trade_date,
                            },
                            {
                                "ts_code": "MA2609",
                                "broker": "东方财富",
                                "long_vol": 999,
                                "short_vol": 1,
                                "trade_date": trade_date,
                            },
                        ]
                    )
                return pd.DataFrame(rows)
            raise AssertionError(f"Unexpected SQL in test: {sql_text}")

        with patch.object(data_engine, "engine", object()), \
             patch.object(data_engine.pd, "read_sql", side_effect=fake_read_sql):
            trend = data_engine.get_market_monitor_holding_trend("M2609 (豆粕)", points=5, data_date="20260428")

        self.assertEqual(trend["trade_date"].tolist(), ["20260422", "20260423", "20260424", "20260427", "20260428"])
        self.assertEqual(trend["dumb_net"].tolist(), [90.0, 100.0, 110.0, 120.0, 130.0])
        self.assertEqual(trend["smart_net"].tolist(), [170.0, 190.0, 210.0, 230.0, 250.0])


if __name__ == "__main__":
    unittest.main()
