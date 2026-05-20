import unittest
from unittest.mock import patch

import pandas as pd

import data_engine


def _holding_df(rows):
    return pd.DataFrame(
        rows,
        columns=["broker", "long_vol", "short_vol", "trade_date_key"],
    )


class BrokerPositionSignalTest(unittest.TestCase):
    def _run_with_rows(self, rows):
        with patch.object(data_engine.pd, "read_sql", return_value=_holding_df(rows)):
            return data_engine._build_futures_broker_position_signal(
                "RB",
                start_date="20260513",
                end_date="20260520",
                lookback_days=5,
            )

    def test_broker_position_signal_bullish_agreement(self):
        output = self._run_with_rows(
            [
                ("东证期货", 100, 80, "20260513"),
                ("东证期货", 150, 80, "20260520"),
                ("中信期货（代客）", 50, 50, "20260513"),
                ("中信期货（代客）", 70, 40, "20260520"),
                ("中信建投期货（代客）", 90, 80, "20260513"),
                ("中信建投期货（代客）", 80, 120, "20260520"),
                ("东方财富", 30, 20, "20260513"),
                ("东方财富", 20, 50, "20260520"),
            ]
        )

        self.assertIn("结论：偏多", output)
        self.assertIn("正指标证据", output)
        self.assertIn("合计净变化+80", output)
        self.assertIn("反指标证据", output)
        self.assertIn("合计净变化-90", output)
        self.assertIn("反向偏多", output)

    def test_broker_position_signal_bearish_agreement(self):
        output = self._run_with_rows(
            [
                ("海通期货(代客)", 150, 70, "20260513"),
                ("海通期货(代客)", 110, 120, "20260520"),
                ("东证期货", 80, 60, "20260513"),
                ("东证期货", 60, 100, "20260520"),
                ("中信建投", 30, 40, "20260513"),
                ("中信建投", 100, 40, "20260520"),
                ("方正中期（代客）", 20, 30, "20260513"),
                ("方正中期（代客）", 80, 20, "20260520"),
            ]
        )

        self.assertIn("结论：偏空", output)
        self.assertIn("合计净变化-150", output)
        self.assertIn("合计净变化+140", output)
        self.assertIn("反向偏空", output)

    def test_broker_position_signal_conflict(self):
        output = self._run_with_rows(
            [
                ("东证期货", 100, 80, "20260513"),
                ("东证期货", 150, 80, "20260520"),
                ("海通期货", 60, 50, "20260513"),
                ("海通期货", 90, 50, "20260520"),
                ("中信建投", 30, 40, "20260513"),
                ("中信建投", 100, 40, "20260520"),
                ("东方财富期货（代客）", 20, 20, "20260513"),
                ("东方财富期货（代客）", 50, 20, "20260520"),
            ]
        )

        self.assertIn("结论：分歧/警惕", output)
        self.assertIn("正指标与反指标方向打架", output)
        self.assertIn("反向偏空", output)

    def test_broker_position_signal_includes_recent_trend_steps(self):
        output = self._run_with_rows(
            [
                ("东证期货", 100, 100, "20260513"),
                ("东证期货", 110, 100, "20260514"),
                ("东证期货", 130, 100, "20260520"),
                ("中信建投", 100, 100, "20260513"),
                ("中信建投", 90, 100, "20260514"),
                ("中信建投", 80, 100, "20260520"),
            ]
        )

        self.assertIn("最近5日趋势", output)
        self.assertIn("0513->0514 偏多", output)
        self.assertIn("0514->0520 偏多", output)

    def test_broker_position_signal_missing_pairs_not_zero_filled(self):
        output = self._run_with_rows(
            [
                ("东证期货", 150, 50, "20260513"),
                ("中信期货", 50, 50, "20260513"),
                ("中信期货", 60, 50, "20260520"),
                ("方正中期", 40, 40, "20260513"),
                ("方正中期", 30, 40, "20260520"),
            ]
        )

        self.assertIn("结论：偏多", output)
        self.assertIn("合计净变化+10", output)
        self.assertIn("已按未知剔除", output)
        self.assertIn("未填0", output)
        self.assertNotIn("合计净变化-90", output)

    def test_reverse_broker_profile_does_not_treat_add_long_as_bullish(self):
        output = data_engine._build_futures_broker_indicator_profile("中信建投")

        self.assertIn("结论：中信建投属于反指标期货商", output)
        self.assertIn("做多/加多解读：按反指标口径是一种利空", output)
        self.assertIn("做空/加空解读：按反指标口径是一种利多", output)

    def test_positive_broker_profile_treats_add_long_as_positive_bullish(self):
        output = data_engine._build_futures_broker_indicator_profile("中信期货")

        self.assertIn("结论：中信期货属于正指标期货商", output)
        self.assertIn("正向偏多", output)


if __name__ == "__main__":
    unittest.main()
