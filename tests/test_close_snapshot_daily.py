import unittest
from unittest.mock import Mock, patch

import pandas as pd

import update_close_snapshot_daily as close_snapshot


class CloseSnapshotDailyTest(unittest.TestCase):
    def test_normalize_etf_symbols_dedupes_and_rejects_invalid(self):
        got = close_snapshot.normalize_etf_symbols("510050.sh, 510050.SH,159915.sz")
        self.assertEqual(got, ["510050.SH", "159915.SZ"])

        with self.assertRaises(ValueError):
            close_snapshot.normalize_etf_symbols("510050,159915.SZ")

    def test_parse_sina_etf_line_extracts_daily_fields(self):
        line = (
            'var hq_str_sh510050="上证50ETF,2.900,2.800,2.940,2.950,2.880,'
            '0,0,12345,67890,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,'
            '2026-05-22,15:00:00,00";'
        )

        sina_code, data = close_snapshot.parse_sina_etf_line(line)

        self.assertEqual(sina_code, "sh510050")
        self.assertEqual(data["name"], "上证50ETF")
        self.assertAlmostEqual(data["close_price"], 2.94)
        self.assertEqual(data["quote_date"], "20260522")
        self.assertAlmostEqual(data["pct_chg"], 5.0)

    def test_build_etf_rows_skips_stale_quotes(self):
        quote_map = {
            "sh510050": {
                "name": "上证50ETF",
                "open_price": 2.9,
                "high_price": 2.95,
                "low_price": 2.88,
                "close_price": 2.94,
                "vol": 123.0,
                "amount": 456.0,
                "pct_chg": 5.0,
                "quote_date": "20260521",
            }
        }

        rows = close_snapshot.build_etf_rows(["510050.SH"], "20260522", quote_map)

        self.assertEqual(rows, [])

    def test_parse_sina_futures_text_keeps_pta_as_commodity(self):
        raw = (
            'var hq_str_nf_TA2609="PTA2609,150000,6018.000,6066.000,5974.000,'
            '6044.000,6042.000,6044.000,6044.000,6022.000,5992.000,153,292,'
            '1066186.000,788666,郑,PTA,2026-05-27,1,,,,,,,,,6022.000,0.000";'
        )

        got = close_snapshot.parse_sina_futures_text(raw)

        self.assertEqual(len(got), 1)
        row = got.iloc[0]
        self.assertEqual(row["sina_code"], "nf_TA2609")
        self.assertEqual(row["price"], 6044.0)
        self.assertEqual(row["open"], 6018.0)
        self.assertEqual(row["position"], 1066186.0)
        self.assertEqual(row["volume"], 788666.0)
        self.assertFalse(close_snapshot.is_cffex_sina_code("nf_TA2609"))
        self.assertTrue(close_snapshot.is_cffex_sina_code("nf_T2609"))

    def test_dedupe_futures_rows_keeps_larger_oi_then_vol(self):
        df = pd.DataFrame(
            [
                {"trade_date": "20260522", "ts_code": "CU2606", "oi": 10, "vol": 100},
                {"trade_date": "20260522", "ts_code": "CU2606", "oi": 20, "vol": 80},
                {"trade_date": "20260522", "ts_code": "CU", "oi": 20, "vol": 90},
            ]
        )

        got = close_snapshot.dedupe_futures_rows(df)

        self.assertEqual(len(got), 2)
        row = got[got["ts_code"] == "CU2606"].iloc[0]
        self.assertEqual(row["oi"], 20)
        self.assertEqual(row["vol"], 80)

    def test_prepare_futures_snapshot_skips_mismatched_trade_date(self):
        stale_df = pd.DataFrame(
            [
                {
                    "trade_date": "20260521",
                    "ts_code": "CU2606",
                    "open_price": 1.0,
                    "high_price": 1.0,
                    "low_price": 1.0,
                    "close_price": 1.0,
                    "settle_price": 1.0,
                    "vol": 1.0,
                    "oi": 1.0,
                    "pct_chg": 0.0,
                }
            ]
        )

        with patch.object(close_snapshot, "EXCHANGE_LIST", ["SHFE"]), patch.object(
            close_snapshot, "fetch_futures_exchange_snapshot", return_value=stale_df
        ):
            got = close_snapshot.prepare_futures_snapshot(Mock(), Mock(), "20260522")

        self.assertTrue(got.empty)

    def test_test_mode_does_not_build_engine_or_write(self):
        pro = Mock()
        pro.trade_cal.return_value = pd.DataFrame([{"cal_date": "20260522", "is_open": 1}])
        futures_df = pd.DataFrame(
            [
                {
                    "trade_date": "20260522",
                    "ts_code": "CU2606",
                    "open_price": 1.0,
                    "high_price": 2.0,
                    "low_price": 0.5,
                    "close_price": 1.5,
                    "settle_price": 1.5,
                    "vol": 100.0,
                    "oi": 200.0,
                    "pct_chg": 1.0,
                }
            ]
        )
        etf_rows = [
            {
                "trade_date": "20260522",
                "ts_code": "510050.SH",
                "name": "上证50ETF",
                "open_price": 2.9,
                "high_price": 3.0,
                "low_price": 2.8,
                "close_price": 2.95,
                "vol": 1.0,
                "amount": 2.0,
                "pct_chg": 1.0,
            }
        ]

        with patch.object(
            close_snapshot, "prepare_futures_snapshot", return_value=futures_df
        ), patch.object(
            close_snapshot, "prepare_etf_snapshot", return_value=etf_rows
        ), patch.object(
            close_snapshot, "build_engine"
        ) as build_engine, patch.object(
            close_snapshot, "save_close_snapshot"
        ) as save_snapshot:
            rc = close_snapshot.run_close_snapshot(
                trade_date="20260522",
                etf_symbols=["510050.SH"],
                test_mode=True,
                pro=pro,
                session=Mock(),
            )

        self.assertEqual(rc, 0)
        build_engine.assert_not_called()
        save_snapshot.assert_not_called()

    def test_empty_etf_rows_fail_before_write(self):
        pro = Mock()
        pro.trade_cal.return_value = pd.DataFrame([{"cal_date": "20260522", "is_open": 1}])
        futures_df = pd.DataFrame([{"trade_date": "20260522", "ts_code": "CU2606"}])

        with patch.object(
            close_snapshot, "prepare_futures_snapshot", return_value=futures_df
        ), patch.object(
            close_snapshot, "prepare_etf_snapshot", return_value=[]
        ), patch.object(
            close_snapshot, "save_close_snapshot"
        ) as save_snapshot:
            with self.assertRaises(RuntimeError):
                close_snapshot.run_close_snapshot(
                    trade_date="20260522",
                    etf_symbols=["510050.SH"],
                    test_mode=False,
                    pro=pro,
                    engine=Mock(),
                    session=Mock(),
                )

        save_snapshot.assert_not_called()


if __name__ == "__main__":
    unittest.main()
