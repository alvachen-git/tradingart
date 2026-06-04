import unittest

from sqlalchemy import create_engine, text

import backfill_ai_stock_v3_data as backfill


class TestBackfillAiStockV3Data(unittest.TestCase):
    def test_normalize_date_accepts_compact_and_dashed(self):
        self.assertEqual(backfill.normalize_date("20250601"), "20250601")
        self.assertEqual(backfill.normalize_date("2025-06-01"), "20250601")

    def test_normalize_date_rejects_invalid(self):
        with self.assertRaises(ValueError):
            backfill.normalize_date("2025/06/01")

    def test_infer_sector_ohlc_start_date_uses_lookback_days(self):
        self.assertEqual(
            backfill.infer_sector_ohlc_start_date("20250601", lookback_days=10),
            "20250522",
        )

    def test_find_undercovered_dates_uses_threshold_in_missing_mode(self):
        dates = ["20250602", "20250603", "20250604"]
        counts = {"20250602": 120, "20250603": 20}

        missing = backfill.find_undercovered_dates(dates, counts, min_rows=50, mode="missing")

        self.assertEqual(missing, ["20250603", "20250604"])

    def test_find_undercovered_dates_all_mode_forces_all_dates(self):
        dates = ["20250602", "20250603"]
        counts = {"20250602": 120, "20250603": 120}

        missing = backfill.find_undercovered_dates(dates, counts, min_rows=50, mode="all")

        self.assertEqual(missing, dates)

    def test_build_coverage_report_filters_sector_type(self):
        engine = create_engine("sqlite:///:memory:")
        with engine.begin() as conn:
            conn.execute(
                text(
                    """
                    CREATE TABLE sector_moneyflow (
                      trade_date TEXT,
                      industry TEXT,
                      sector_type TEXT
                    )
                    """
                )
            )
            conn.execute(
                text(
                    """
                    INSERT INTO sector_moneyflow (trade_date, industry, sector_type)
                    VALUES
                      ('20250602', '半导体', '行业'),
                      ('20250602', '证券', '行业'),
                      ('20250603', '机器人', '概念')
                    """
                )
            )

        rule = backfill.CoverageRule(
            table_name="sector_moneyflow",
            date_column="trade_date",
            min_rows=2,
            where_sql="sector_type = '行业'",
        )
        report = backfill.build_coverage_report(
            engine=engine,
            name="sector_flow",
            rule=rule,
            trade_dates=["20250602", "20250603"],
            start_date="20250602",
            end_date="20250603",
            mode="missing",
        )

        self.assertEqual(report.covered_dates, 1)
        self.assertEqual(report.missing_dates, ["20250603"])

    def test_parse_steps_rejects_unknown_step(self):
        with self.assertRaises(ValueError):
            backfill.parse_steps("sector_flow,unknown")

    def test_describe_dry_run_step_keeps_sector_ohlc_as_range(self):
        message = backfill.describe_dry_run_step(
            "sector_ohlc",
            ["20250602", "20250603"],
            sector_ohlc_start="20250101",
            end_date="20260529",
        )

        self.assertIn("20250101 -> 20260529", message)
        self.assertNotIn("2 个交易日", message)


if __name__ == "__main__":
    unittest.main()
