import unittest
from unittest.mock import Mock

import backfill_stock_score as bss


class BackfillStockScoreTests(unittest.TestCase):
    def test_upsert_records_falls_back_to_row_by_row(self):
        records = [
            {"ts_code": "000001.SZ", "trade_date": "20260101"},
            {"ts_code": "000002.SZ", "trade_date": "20260101"},
            {"ts_code": "000003.SZ", "trade_date": "20260101"},
        ]

        class FakeBegin:
            calls = 0

            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

            def execute(self, _sql, params):
                FakeBegin.calls += 1
                if isinstance(params, list):
                    raise RuntimeError("batch failed")
                if params["ts_code"] == "000002.SZ":
                    raise RuntimeError("bad row")

        engine = Mock()
        engine.begin.side_effect = lambda: FakeBegin()

        written, errors = bss._upsert_records(engine, records)

        self.assertEqual(written, 2)
        self.assertEqual(len(errors), 1)
        self.assertIn("000002.SZ", errors[0])


if __name__ == "__main__":
    unittest.main()
