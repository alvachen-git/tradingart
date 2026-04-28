import unittest
from unittest.mock import patch

import pandas as pd

import data_engine


class ComprehensiveMarketDataSnapshotTests(unittest.TestCase):
    def setUp(self):
        data_engine.clear_comprehensive_market_data_snapshot()

    def tearDown(self):
        data_engine.clear_comprehensive_market_data_snapshot()

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
                    {"ts_code": "MA2609", "close_price": 190.0, "oi": 1100.0, "trade_date": "2026-04-27"},
                    {"ts_code": "MA2606", "close_price": 188.0, "oi": 880.0, "trade_date": "2026-04-27"},
                    {"ts_code": "MA2609", "close_price": 180.0, "oi": 1000.0, "trade_date": "2026-04-21"},
                    {"ts_code": "MA2606", "close_price": 178.0, "oi": 860.0, "trade_date": "2026-04-21"},
                    {"ts_code": "TA2609", "close_price": 100.0, "oi": 1500.0, "trade_date": "2026-04-27"},
                    {"ts_code": "TA2606", "close_price": 98.0, "oi": 1000.0, "trade_date": "2026-04-27"},
                    {"ts_code": "TA2609", "close_price": 95.0, "oi": 1400.0, "trade_date": "2026-04-24"},
                    {"ts_code": "TA2606", "close_price": 96.0, "oi": 950.0, "trade_date": "2026-04-24"},
                ]
            )

        if "FROM commodity_iv_history" in sql_text:
            return pd.DataFrame(
                [
                    {"ts_code": "MA2609", "iv": 35.0, "trade_date": "2026-04-28"},
                    {"ts_code": "MA2609", "iv": 30.0, "trade_date": "2026-04-27"},
                    {"ts_code": "MA2609", "iv": 25.0, "trade_date": "2026-04-21"},
                    {"ts_code": "MA2606", "iv": 31.0, "trade_date": "2026-04-28"},
                    {"ts_code": "MA2606", "iv": 27.0, "trade_date": "2026-04-27"},
                    {"ts_code": "MA2606", "iv": 24.0, "trade_date": "2026-04-21"},
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


if __name__ == "__main__":
    unittest.main()
