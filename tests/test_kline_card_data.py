import unittest

import pandas as pd

import kline_card_data as card_data


def _make_df(rows=120, start=100.0):
    dates = pd.date_range("2024-01-01", periods=rows, freq="D")
    return pd.DataFrame(
        {
            "open_price": [start + i * 0.1 for i in range(rows)],
            "high_price": [start + i * 0.1 + 1.0 for i in range(rows)],
            "low_price": [start + i * 0.1 - 1.0 for i in range(rows)],
            "close_price": [start + i * 0.1 + 0.4 for i in range(rows)],
            "vol": [1000 + i for i in range(rows)],
        },
        index=dates,
    )


class TestKlineCardData(unittest.TestCase):
    def setUp(self):
        self.orig_get_random = card_data.kg.get_random_kline_data

        self.call_index = 0
        symbols = ["AAA", "BBB", "CCC", "DDD", "EEE"]

        def _stub_get_random_kline_data(bars=100, history_bars=20, _attempt=1, _max_attempts=12):
            idx = self.call_index % len(symbols)
            self.call_index += 1
            sym = symbols[idx]
            return sym, sym, "stock", _make_df(rows=bars + history_bars, start=100 + idx * 10)

        card_data.kg.get_random_kline_data = _stub_get_random_kline_data

    def tearDown(self):
        card_data.kg.get_random_kline_data = self.orig_get_random

    def test_get_stage_candidates_returns_requested_count(self):
        out = card_data.get_stage_candidates(stage_no=1, count=3, seed=10)
        self.assertEqual(len(out), 3)
        self.assertEqual(len({x["symbol"] for x in out}), 3)
        self.assertTrue(all(len(x["bars"]) == 120 for x in out))

    def test_get_boss_stage_candidate_returns_single_pack(self):
        out = card_data.get_boss_stage_candidate(stage_no=5, seed=99)
        self.assertTrue(out["symbol"])
        self.assertEqual(len(out["bars"]), 120)
        self.assertIn("symbol_type", out)


if __name__ == "__main__":
    unittest.main()

