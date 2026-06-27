import unittest
from unittest.mock import patch

import pandas as pd

import data_engine


def _snapshot(rows):
    base_rows = []
    for row in rows:
        item = {
            "合约": row.get("contract", "SN2608 (锡)"),
            "当前IV": row.get("iv", 30.0),
            "IV Rank": row.get("rank", 50),
            "涨跌%(日)": row.get("price_1d", 0.0),
            "涨跌%(5日)": row.get("price_5d", 0.0),
            "IV变动(日)": row.get("iv_1d", 0.0),
            "IV变动(5日)": row.get("iv_5d", 0.0),
            "_数据日期": row.get("date", "20260625"),
        }
        base_rows.append(item)
    return pd.DataFrame(base_rows)


def _table_data_rows(output: str):
    return [
        line
        for line in str(output).splitlines()
        if line.startswith("| ") and not line.startswith("| 排名") and not line.startswith("|---")
    ]


class VolatilityDivergenceScannerTest(unittest.TestCase):
    def _invoke(self, rows, **kwargs):
        with patch.object(data_engine, "engine", object()), patch.object(
            data_engine, "get_comprehensive_market_data", return_value=_snapshot(rows)
        ):
            return data_engine.scan_volatility_divergence.invoke(kwargs)

    def test_detects_price_up_iv_down(self):
        output = self._invoke(
            [{"contract": "SN2608 (锡)", "price_1d": 1.2, "iv_1d": -1.5}],
            window="1d",
        )

        self.assertIn("上涨降波", output)
        self.assertIn("SN2608", output)
        self.assertIn("+1.20%", output)
        self.assertIn("-1.50", output)

    def test_detects_price_down_iv_up(self):
        output = self._invoke(
            [{"contract": "AG2608 (白银)", "price_1d": -2.1, "iv_1d": 1.3}],
            window="1d",
        )

        self.assertIn("下跌升波", output)
        self.assertIn("AG2608", output)

    def test_detects_flat_price_iv_up(self):
        output = self._invoke(
            [{"contract": "CU2608 (铜)", "price_1d": 0.12, "iv_1d": 2.0}],
            window="1d",
        )

        self.assertIn("横盘升波", output)
        self.assertIn("CU2608", output)

    def test_detects_large_price_move_without_iv_confirmation(self):
        output = self._invoke(
            [{"contract": "LC2608 (碳酸锂)", "price_1d": -1.6, "iv_1d": 0.2}],
            window="1d",
        )

        self.assertIn("大波动IV未确认", output)
        self.assertIn("LC2608", output)

    def test_no_divergence_returns_clear_message(self):
        output = self._invoke(
            [{"contract": "M2608 (豆粕)", "price_1d": 0.8, "iv_1d": 1.2}],
            window="1d",
        )

        self.assertIn("未发现符合阈值的波动率背离", output)
        self.assertIn("阈值", output)

    def test_chinese_symbol_filters_to_matching_product(self):
        output = self._invoke(
            [
                {"contract": "SN2608 (锡)", "price_1d": 1.2, "iv_1d": -1.5},
                {"contract": "AG2608 (白银)", "price_1d": -2.1, "iv_1d": 1.3},
            ],
            symbol="沪锡",
        )

        self.assertIn("SN2608", output)
        self.assertNotIn("AG2608", output)

    def test_limit_is_applied_after_ranking(self):
        rows = [
            {"contract": f"SN260{i} (锡)", "price_1d": 1.0 + i, "iv_1d": -1.1}
            for i in range(1, 5)
        ]
        output = self._invoke(rows, limit=2)

        self.assertEqual(len(_table_data_rows(output)), 2)
        self.assertIn("SN2604", output)
        self.assertIn("SN2603", output)
        self.assertNotIn("SN2602", output)

    def test_uses_five_day_window(self):
        output = self._invoke(
            [{"contract": "AU2608 (黄金)", "price_5d": -3.0, "iv_5d": 2.2}],
            window="5d",
        )

        self.assertIn("近5日", output)
        self.assertIn("下跌升波", output)
        self.assertIn("AU2608", output)


if __name__ == "__main__":
    unittest.main()
