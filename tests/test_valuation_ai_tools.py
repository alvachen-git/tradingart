import unittest
from unittest.mock import patch

import pandas as pd
from sqlalchemy import create_engine, text

from global_index_valuation import (
    GlobalValuationRecord,
    INDEX_SPEC_BY_CODE,
    store_global_valuation_records,
)
from valuation_ai_tools import (
    benchmark_codes_for_security,
    build_global_index_valuation_context,
    build_stock_valuation_profile,
    match_index_codes,
)


def _global_records(code: str, *, periods: int = 48, quality: str = "ok"):
    spec = INDEX_SPEC_BY_CODE[code]
    records = []
    for index, month_end in enumerate(pd.date_range("2022-01-31", periods=periods, freq="ME"), 1):
        records.append(GlobalValuationRecord(
            trade_date=month_end.strftime("%Y%m%d"),
            index_code=spec.code,
            index_name=spec.name,
            market=spec.market,
            pe_ttm=float(index),
            source_name=spec.source_name,
            source_url=spec.source_url,
            methodology="PE-TTM",
            is_proxy=spec.proxy,
            quality_status=quality,
            raw_detail={},
        ))
    return records


class GlobalIndexValuationAIToolTest(unittest.TestCase):
    def setUp(self):
        self.engine = create_engine("sqlite:///:memory:")

    def tearDown(self):
        self.engine.dispose()

    def test_aliases_market_scope_and_fixed_order(self):
        self.assertEqual(match_index_codes("比较SPY、创业板指和恒生科技指数"), [
            "SP500", "399006", "HSTECH",
        ])
        records = []
        for code in ("000300", "399006", "000688", "000905", "000852", "932000"):
            records.extend(_global_records(code))
        store_global_valuation_records(self.engine, records)

        result = build_global_index_valuation_context(
            query="A股主要指数估值",
            as_of_date="2025-12-31",
            engine=self.engine,
        )

        self.assertEqual(result["status"], "ok")
        self.assertEqual([card["code"] for card in result["cards"]], [
            "000300", "399006", "000688", "000905", "000852", "932000",
        ])
        self.assertEqual(len(result["ranking"]), 6)
        self.assertTrue(all(item["percentile"] == 100.0 for item in result["ranking"]))

    def test_all_eleven_index_aliases(self):
        alias_by_code = {
            "NASDAQ100": "QQQ",
            "SP500": "SPX",
            "RUSSELL2000": "IWM",
            "000300": "510300",
            "399006": "创业板ETF",
            "000688": "588000",
            "000905": "510500",
            "000852": "512100",
            "932000": "中证2000",
            "HSI": "恒指",
            "HSTECH": "恒科",
        }
        for code, alias in alias_by_code.items():
            with self.subTest(alias=alias):
                self.assertEqual(match_index_codes(alias), [code])

    def test_explicit_etf_uses_underlying_index_and_explains_reference(self):
        store_global_valuation_records(
            self.engine, _global_records("000300") + _global_records("NASDAQ100")
        )

        result = build_global_index_valuation_context(
            query="300ETF当前估值分位多少",
            as_of_date="20251231",
            engine=self.engine,
        )

        self.assertEqual([card["code"] for card in result["cards"]], ["000300"])
        self.assertIn("ETF跟踪指数", result["benchmark_note"])
        self.assertIn("ETF跟踪指数", result["report"])

        qqq = build_global_index_valuation_context(
            query="QQQ当前PE分位", as_of_date="20251231", engine=self.engine,
        )
        self.assertEqual([card["code"] for card in qqq["cards"]], ["NASDAQ100"])
        self.assertIn("ETF跟踪指数", qqq["benchmark_note"])

    def test_stale_and_quality_mismatch_do_not_enter_current_ranking(self):
        records = _global_records("SP500")
        mismatch = _global_records("399006", quality="source_mismatch")
        store_global_valuation_records(self.engine, records + mismatch)

        result = build_global_index_valuation_context(
            query="标普500和创业板指",
            as_of_date="20271231",
            engine=self.engine,
        )

        self.assertEqual(result["status"], "insufficient")
        self.assertEqual(result["ranking"], [])
        self.assertIn("历史参考", result["report"])
        self.assertIn("暂不参与当前排名", result["report"])

    def test_invalid_date_and_missing_table_degrade_without_exception(self):
        invalid = build_global_index_valuation_context(
            query="全球", as_of_date="2026-99-99", engine=self.engine,
        )
        self.assertEqual(invalid["status"], "invalid_request")

        missing = build_global_index_valuation_context(
            query="全球", as_of_date="20260722", engine=self.engine,
        )
        self.assertEqual(missing["status"], "no_data")
        self.assertIn("数据不足", missing["report"])

    def test_style_reference_mapping_never_claims_membership(self):
        self.assertEqual(benchmark_codes_for_security("300750.SZ")[0], [
            "399006", "000300", "000852",
        ])
        self.assertEqual(benchmark_codes_for_security("688981.SH")[0], [
            "000688", "000300", "000852",
        ])
        bj_codes, bj_note = benchmark_codes_for_security("920001.BJ")
        self.assertEqual(bj_codes, ["000300", "000905", "000852", "932000"])
        self.assertIn("暂无北交所专属指数", bj_note)

    @patch("valuation_ai_tools._resolve_security", return_value=("300750.SZ", "stock"))
    def test_stock_question_selects_style_references_instead_of_all_indices(self, _resolver):
        records = []
        for code in ("000300", "399006", "000852"):
            records.extend(_global_records(code))
        store_global_valuation_records(self.engine, records)

        result = build_global_index_valuation_context(
            query="宁德时代适合长期价值投资吗",
            as_of_date="20251231",
            engine=self.engine,
        )

        self.assertEqual(result["subject_code"], "300750.SZ")
        self.assertEqual(
            [card["code"] for card in result["cards"]],
            ["000300", "399006", "000852"],
        )
        self.assertIn("风格参考", result["benchmark_note"])


class StockValuationProfileTest(unittest.TestCase):
    def setUp(self):
        self.engine = create_engine("sqlite:///:memory:")
        with self.engine.begin() as conn:
            conn.execute(text("""
                CREATE TABLE stock_valuation (
                    trade_date TEXT,
                    ts_code TEXT,
                    pe_ttm DOUBLE,
                    pb DOUBLE,
                    dv_ratio DOUBLE,
                    total_mv DOUBLE
                )
            """))

    def tearDown(self):
        self.engine.dispose()

    def _insert_history(self, *, latest_pe: float = 48.0, latest_pb: float | None = 4.8):
        rows = []
        dates = pd.date_range("2022-01-31", periods=48, freq="ME")
        for index, month_end in enumerate(dates, 1):
            # Two daily observations verify that the history is sampled once per month.
            rows.extend([
                {
                    "trade_date": (month_end - pd.Timedelta(days=10)).strftime("%Y%m%d"),
                    "ts_code": "300750.SZ", "pe_ttm": index - 0.5, "pb": index / 10,
                    "dv_ratio": 1.2, "total_mv": 100_000_000,
                },
                {
                    "trade_date": month_end.strftime("%Y%m%d"),
                    "ts_code": "300750.SZ", "pe_ttm": latest_pe if index == 48 else index,
                    "pb": latest_pb if index == 48 else index / 10,
                    "dv_ratio": 1.2, "total_mv": 100_000_000,
                },
            ])
        with self.engine.begin() as conn:
            conn.execute(text("""
                INSERT INTO stock_valuation
                    (trade_date, ts_code, pe_ttm, pb, dv_ratio, total_mv)
                VALUES
                    (:trade_date, :ts_code, :pe_ttm, :pb, :dv_ratio, :total_mv)
            """), rows)

    @patch("valuation_ai_tools._resolve_security", return_value=("300750.SZ", "stock"))
    def test_stock_uses_month_end_ten_year_method_and_style_references(self, _resolver):
        self._insert_history()

        result = build_stock_valuation_profile(
            "宁德时代", as_of_date="20251231", engine=self.engine,
        )

        self.assertEqual(result["status"], "ok")
        self.assertEqual(result["metrics"]["pe_month_count"], 48)
        self.assertEqual(result["metrics"]["pe_percentile"], 100.0)
        self.assertEqual(result["metrics"]["pb_percentile"], 100.0)
        self.assertEqual(result["benchmark_codes"], ["399006", "000300", "000852"])
        self.assertNotIn("地板价", result["report"])
        self.assertIn("历史分位衡量的是估值位置", result["report"])

    @patch("valuation_ai_tools._resolve_security", return_value=("300750.SZ", "stock"))
    def test_negative_pe_keeps_pb_and_dividend_but_skips_pe_percentile(self, _resolver):
        self._insert_history(latest_pe=-5.0)

        result = build_stock_valuation_profile("宁德时代", engine=self.engine)

        self.assertEqual(result["status"], "loss_making")
        self.assertIsNone(result["metrics"]["pe_percentile"])
        self.assertEqual(result["metrics"]["pb_percentile"], 100.0)
        self.assertEqual(result["metrics"]["dividend_yield_pct"], 1.2)
        self.assertIn("当前盈利为负", result["report"])

    @patch("valuation_ai_tools._resolve_security", return_value=("300750.SZ", "stock"))
    def test_missing_pb_is_reported_without_losing_pe(self, _resolver):
        self._insert_history(latest_pb=None)

        result = build_stock_valuation_profile("宁德时代", engine=self.engine)

        self.assertEqual(result["status"], "partial")
        self.assertEqual(result["metrics"]["pe_percentile"], 100.0)
        self.assertIsNone(result["metrics"]["pb_percentile"])
        self.assertIn("当前PB缺失或无效", result["report"])

    @patch("valuation_ai_tools._resolve_security", return_value=("AAPL.US", "stock"))
    def test_us_company_reports_local_history_gap(self, _resolver):
        result = build_stock_valuation_profile("苹果", engine=self.engine)

        self.assertEqual(result["status"], "unsupported_market")
        self.assertIn("美股可提供指数估值环境", result["report"])

    def test_index_subject_is_delegated_by_legacy_tool_wrapper(self):
        result = build_stock_valuation_profile("标普500", engine=self.engine)

        self.assertEqual(result["status"], "index_subject")
        self.assertEqual(result["index_codes"], ["SP500"])

    @patch("valuation_ai_tools._resolve_security", return_value=("300750.SZ", "stock"))
    def test_legacy_tool_name_uses_structured_profile(self, _resolver):
        from data_engine import get_stock_valuation

        self._insert_history()
        with patch("data_engine.engine", self.engine):
            report = get_stock_valuation.invoke({"symbol": "宁德时代"})

        self.assertIn("【个股估值】", report)
        self.assertIn("近10年分位", report)
        self.assertIn("创业板指", report)


if __name__ == "__main__":
    unittest.main()
