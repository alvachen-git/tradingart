import ast
import math
import unittest
from pathlib import Path
from typing import Any

import pandas as pd

from us_market_dashboard_data import selected_underlying_price


def _load_page_chart_helpers():
    source = Path("pages/29_美股期权.py").read_text(encoding="utf-8")
    module = ast.parse(source)
    wanted = {
        "_clean_float",
        "_empty_chart_ohlc_frame",
        "_chart_adjusted_ohlc_frame",
        "_lightweight_chart_data",
        "_chart_line_records",
        "_chart_payload",
    }
    nodes = [node for node in module.body if isinstance(node, ast.FunctionDef) and node.name in wanted]
    namespace = {"pd": pd, "math": math, "Any": Any, "CHART_RENDER_WINDOW": 260}
    exec(compile(ast.Module(body=nodes, type_ignores=[]), filename="<chart_helpers>", mode="exec"), namespace)
    return namespace


class USOptionsKlineDisplayTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.helpers = _load_page_chart_helpers()

    def test_adjusted_ohlc_uses_adj_close_ratio_for_display(self):
        df = pd.DataFrame(
            [
                {
                    "date": "2026-07-01",
                    "open": 790.0,
                    "high": 820.0,
                    "low": 780.0,
                    "close": 800.0,
                    "adjClose": 200.0,
                    "volume": 123,
                }
            ]
        )

        out = self.helpers["_chart_adjusted_ohlc_frame"](df)
        row = out.iloc[0]

        self.assertAlmostEqual(row["open"], 197.5)
        self.assertAlmostEqual(row["high"], 205.0)
        self.assertAlmostEqual(row["low"], 195.0)
        self.assertAlmostEqual(row["close"], 200.0)
        self.assertAlmostEqual(row["raw_close"], 800.0)
        self.assertTrue(bool(row["is_adjusted"]))

    def test_adjusted_ohlc_falls_back_when_adj_close_is_missing(self):
        df = pd.DataFrame(
            [{"date": "2026-07-01", "open": 99.0, "high": 103.0, "low": 98.0, "close": 100.0, "volume": 50}]
        )

        out = self.helpers["_chart_adjusted_ohlc_frame"](df)
        row = out.iloc[0]

        self.assertEqual(row["open"], 99.0)
        self.assertEqual(row["high"], 103.0)
        self.assertEqual(row["low"], 98.0)
        self.assertEqual(row["close"], 100.0)
        self.assertFalse(bool(row["is_adjusted"]))

    def test_adjusted_ohlc_falls_back_when_adj_close_is_invalid(self):
        df = pd.DataFrame(
            [
                {"date": "2026-07-01", "open": 99.0, "high": 103.0, "low": 98.0, "close": 100.0, "adjClose": 0.0},
                {"date": "2026-07-02", "open": 98.0, "high": 102.0, "low": 97.0, "close": 101.0, "adjClose": -1.0},
                {"date": "2026-07-03", "open": 97.0, "high": 101.0, "low": 96.0, "close": 102.0, "adjClose": math.nan},
            ]
        )

        out = self.helpers["_chart_adjusted_ohlc_frame"](df)

        self.assertEqual(out["close"].tolist(), [100.0, 101.0, 102.0])
        self.assertFalse(out["is_adjusted"].any())

    def test_lightweight_chart_ma_uses_adjusted_close(self):
        df = pd.DataFrame(
            [
                {
                    "date": pd.Timestamp("2026-01-01") + pd.Timedelta(days=idx),
                    "open": 100.0,
                    "high": 102.0,
                    "low": 98.0,
                    "close": 100.0,
                    "adjClose": 50.0,
                    "volume": 1000,
                }
                for idx in range(20)
            ]
        )

        _candles, lines = self.helpers["_lightweight_chart_data"](df)

        self.assertAlmostEqual(float(lines["ma20"].iloc[-1]["MA20"]), 50.0)

    def test_chart_payload_latest_uses_raw_close(self):
        df = pd.DataFrame(
            [
                {
                    "date": "2026-07-01",
                    "open": 790.0,
                    "high": 820.0,
                    "low": 780.0,
                    "close": 800.0,
                    "adjClose": 200.0,
                    "volume": 100,
                },
                {
                    "date": "2026-07-02",
                    "open": 801.0,
                    "high": 812.0,
                    "low": 799.0,
                    "close": 808.0,
                    "adjClose": 202.0,
                    "volume": 120,
                },
            ]
        )
        candles, lines = self.helpers["_lightweight_chart_data"](df)

        payload = self.helpers["_chart_payload"](candles, lines, pd.DataFrame(columns=["date", "IV"]), "CRWD")

        self.assertAlmostEqual(payload["candles"][-1]["close"], 202.0)
        self.assertAlmostEqual(payload["latest"]["close"], 808.0)
        self.assertAlmostEqual(payload["latest"]["change"], 8.0)

    def test_selected_underlying_price_still_uses_raw_close(self):
        df = pd.DataFrame(
            [
                {
                    "date": pd.Timestamp("2026-07-01"),
                    "open": 790.0,
                    "high": 820.0,
                    "low": 780.0,
                    "close": 800.0,
                    "adjClose": 200.0,
                    "volume": 123,
                }
            ]
        )

        self.assertEqual(selected_underlying_price(df, "20260701"), 800.0)


if __name__ == "__main__":
    unittest.main()
