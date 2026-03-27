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


if __name__ == "__main__":
    unittest.main()
