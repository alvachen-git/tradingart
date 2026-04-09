import unittest
from unittest.mock import patch


_IMPORT_ERROR = None
try:
    import pandas as pd
    import mobile_api
except Exception as exc:  # pragma: no cover
    mobile_api = None
    pd = None
    _IMPORT_ERROR = exc


@unittest.skipIf(mobile_api is None, f"mobile_api import failed: {_IMPORT_ERROR}")
class TestMobileApiChartContractParam(unittest.TestCase):
    class _FakeRedis:
        def __init__(self):
            self.data = {}

        def get(self, key):
            return self.data.get(key)

        def setex(self, key, ttl, value):
            self.data[key] = value
            return True

    def test_chart_uses_selected_contract(self):
        ohlc_df = pd.DataFrame(
            [
                {
                    "dt": "20260327",
                    "o": 3200.0,
                    "h": 3300.0,
                    "l": 3100.0,
                    "c": 3250.0,
                    "pct_chg": 1.5,
                    "oi": 123456,
                }
            ]
        )
        iv_df = pd.DataFrame([{"dt": "20260327", "iv": 42.8}])
        hold_df = pd.DataFrame(columns=["dt", "broker", "long_vol", "short_vol"])

        with patch.object(mobile_api.de, "engine", object()), patch("pandas.read_sql") as mock_read_sql:
            mock_read_sql.side_effect = [ohlc_df, iv_df, hold_df]
            res = mobile_api.market_chart("ma", contract="MA2609", username="tester")

        self.assertEqual(res["main_contract"], "MA2609")
        self.assertEqual(res["cur_price"], 3250.0)
        self.assertTrue(mock_read_sql.call_args_list)
        first_sql = str(mock_read_sql.call_args_list[0].args[0]).upper()
        self.assertIn("WHERE UPPER(TS_CODE) = 'MA2609'", first_sql)

    def test_chart_uses_stale_live_when_live_day_newer_than_db(self):
        ohlc_df = pd.DataFrame(
            [
                {
                    "dt": "20260407",
                    "o": 720.0,
                    "h": 735.0,
                    "l": 700.0,
                    "c": 710.0,
                    "pct_chg": -1.0,
                    "oi": 123456,
                }
            ]
        )
        iv_df = pd.DataFrame([{"dt": "20260407", "iv": 40.0}])
        hold_df = pd.DataFrame(columns=["dt", "broker", "long_vol", "short_vol"])
        live_payload = {
            "contracts": {"SC2605": {"price": 621.0, "pct": -12.31, "trading_day": "20260408"}},
            "refreshed_ts": 1,  # 故意过期，验证“交易日领先仍覆盖”
        }

        with patch.object(mobile_api.de, "engine", object()), patch.object(
            mobile_api, "_redis", self._FakeRedis()
        ), patch.object(
            mobile_api, "_is_trading_hours", return_value=False
        ), patch.object(
            mobile_api, "_load_shared_prices_payload", return_value=live_payload
        ), patch.object(
            mobile_api, "_PRICES_LIVE_OVERRIDE_MAX_AGE_SEC", 60
        ), patch("pandas.read_sql") as mock_read_sql:
            mock_read_sql.side_effect = [ohlc_df, iv_df, hold_df]
            res = mobile_api.market_chart("sc", contract="SC2605", username="tester")

        self.assertEqual(res["main_contract"], "SC2605")
        self.assertEqual(res["cur_price"], 621.0)
        self.assertTrue(res.get("ohlc"))
        self.assertEqual(res["ohlc"][-1]["dt"], "20260408")
        self.assertEqual(res["ohlc"][-1]["c"], 621.0)


if __name__ == "__main__":
    unittest.main()
