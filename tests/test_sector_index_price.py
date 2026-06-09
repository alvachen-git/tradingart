import unittest

import pandas as pd
from sqlalchemy import create_engine, text

import update_sector_index_price as upd


class TestSectorIndexPrice(unittest.TestCase):
    def test_resolve_update_dates_uses_lookback_when_start_missing(self):
        start, end, sector_date = upd.resolve_update_dates(end_date="20260608", lookback_days=7)

        self.assertEqual(start, "20260601")
        self.assertEqual(end, "20260608")
        self.assertEqual(sector_date, "20260608")

    def test_resolve_update_dates_date_overrides_lookback(self):
        start, end, sector_date = upd.resolve_update_dates(date="20260608", lookback_days=7)

        self.assertEqual(start, "20260608")
        self.assertEqual(end, "20260608")
        self.assertEqual(sector_date, "20260608")

    def test_match_sector_catalog_exact_and_suffix(self):
        catalog = pd.DataFrame(
            [
                {"ts_code": "881001.TI", "name": "半导体"},
                {"ts_code": "881002.TI", "name": "证券行业指数"},
            ]
        )

        exact = upd.match_sector_to_ths_index("半导体", "行业", catalog)
        suffix = upd.match_sector_to_ths_index("证券", "行业", catalog)

        self.assertEqual(exact.match_status, upd.MATCHED)
        self.assertEqual(exact.ths_code, "881001.TI")
        self.assertEqual(suffix.match_status, upd.MATCHED)
        self.assertEqual(suffix.ths_code, "881002.TI")

    def test_match_sector_catalog_unmatched_and_ambiguous(self):
        catalog = pd.DataFrame(
            [
                {"ts_code": "881001.TI", "name": "机器人指数"},
                {"ts_code": "881002.TI", "name": "机器人概念指数"},
                {"ts_code": "881003.TI", "name": "光伏设备"},
            ]
        )

        unmatched = upd.match_sector_to_ths_index("银行", "行业", catalog)
        ambiguous = upd.match_sector_to_ths_index("机器人", "行业", catalog)

        self.assertEqual(unmatched.match_status, upd.UNMATCHED)
        self.assertEqual(ambiguous.match_status, upd.AMBIGUOUS)
        self.assertEqual(ambiguous.ths_code, "")

    def test_transform_ths_daily_df_renames_fields_and_fills_missing(self):
        match = upd.SectorIndexMatch("半导体", "行业", "881001.TI", "半导体", upd.MATCHED)
        raw = pd.DataFrame(
            [
                {
                    "ts_code": "881001.TI",
                    "trade_date": "2026-03-19",
                    "open": "100",
                    "high": "108",
                    "low": "99",
                    "close": "106",
                    "pct_change": "2.5",
                }
            ]
        )

        out = upd.transform_ths_daily_df(raw, match)

        self.assertEqual(out.iloc[0]["trade_date"], "20260319")
        self.assertEqual(out.iloc[0]["sector_name"], "半导体")
        self.assertEqual(float(out.iloc[0]["close_price"]), 106.0)
        self.assertEqual(float(out.iloc[0]["vol"]), 0.0)

    def test_save_sector_index_prices_replaces_same_sector_date_range(self):
        engine = create_engine("sqlite:///:memory:")
        with engine.begin() as conn:
            conn.execute(
                text(
                    """
                    CREATE TABLE sector_index_price (
                      trade_date TEXT NOT NULL,
                      ths_code TEXT NOT NULL,
                      sector_name TEXT NOT NULL,
                      sector_type TEXT NOT NULL,
                      open_price REAL,
                      high_price REAL,
                      low_price REAL,
                      close_price REAL,
                      pct_chg REAL,
                      vol REAL,
                      amount REAL,
                      PRIMARY KEY (trade_date, sector_name, sector_type)
                    )
                    """
                )
            )

        first = pd.DataFrame(
            [
                {
                    "trade_date": "20260319",
                    "ths_code": "881001.TI",
                    "sector_name": "半导体",
                    "sector_type": "行业",
                    "open_price": 100,
                    "high_price": 108,
                    "low_price": 99,
                    "close_price": 106,
                    "pct_chg": 2.5,
                    "vol": 1,
                    "amount": 2,
                }
            ]
        )
        second = first.copy()
        second["close_price"] = 109

        upd.save_sector_index_prices(engine, first, "半导体", "行业", "20260319", "20260319")
        upd.save_sector_index_prices(engine, second, "半导体", "行业", "20260319", "20260319")

        with engine.connect() as conn:
            rows = conn.execute(text("SELECT close_price FROM sector_index_price")).fetchall()
        self.assertEqual(len(rows), 1)
        self.assertEqual(float(rows[0][0]), 109.0)

    def test_save_sector_index_prices_keeps_aliases_sharing_same_code(self):
        engine = create_engine("sqlite:///:memory:")
        with engine.begin() as conn:
            conn.execute(
                text(
                    """
                    CREATE TABLE sector_index_price (
                      trade_date TEXT NOT NULL,
                      ths_code TEXT NOT NULL,
                      sector_name TEXT NOT NULL,
                      sector_type TEXT NOT NULL,
                      open_price REAL,
                      high_price REAL,
                      low_price REAL,
                      close_price REAL,
                      pct_chg REAL,
                      vol REAL,
                      amount REAL,
                      PRIMARY KEY (trade_date, sector_name, sector_type)
                    )
                    """
                )
            )

        base = {
            "trade_date": "20260319",
            "ths_code": "881001.TI",
            "sector_type": "行业",
            "open_price": 100,
            "high_price": 108,
            "low_price": 99,
            "close_price": 106,
            "pct_chg": 2.5,
            "vol": 1,
            "amount": 2,
        }
        first = pd.DataFrame([{**base, "sector_name": "半导体"}])
        alias = pd.DataFrame([{**base, "sector_name": "半导体设备"}])

        upd.save_sector_index_prices(engine, first, "半导体", "行业", "20260319", "20260319")
        upd.save_sector_index_prices(engine, alias, "半导体设备", "行业", "20260319", "20260319")

        with engine.connect() as conn:
            rows = conn.execute(text("SELECT sector_name, ths_code FROM sector_index_price ORDER BY sector_name")).fetchall()
        self.assertEqual(len(rows), 2)
        self.assertEqual({r[0] for r in rows}, {"半导体", "半导体设备"})


if __name__ == "__main__":
    unittest.main()
