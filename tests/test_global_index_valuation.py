import logging
import unittest
from types import SimpleNamespace
from unittest.mock import patch

import pandas as pd
from sqlalchemy import create_engine, text

from global_index_valuation import (
    INDEX_SPEC_BY_CODE,
    GlobalValuationRecord,
    PublicGlobalValuationClient,
    build_global_index_valuation_dashboard,
    empirical_percentile,
    get_global_index_valuation_cache_version,
    parse_cni_snapshot,
    parse_csindex_payload,
    parse_hsi_archive,
    parse_hsi_pdf_text,
    parse_issuer_snapshot_html,
    parse_world_pe_html,
    percentile_label,
    store_global_valuation_records,
)
from update_global_index_valuation import (
    collect_valuation_records,
    configure_logging,
    fetch_chinext_history,
    load_chinext_history,
    main as update_main,
    parse_iwm_issuer_pe,
    parse_spy_issuer_pe,
    select_hsi_reports,
    validate_us_proxy_snapshots,
    validate_chinext_snapshot,
)


class GlobalIndexValuationParserTest(unittest.TestCase):
    def test_csindex_parser_uses_positive_rolling_pe(self):
        payload = {"data": [
            {"tradeDate": "2026-07-20", "peg": "14.29"},
            {"tradeDate": "2026-07-19", "peg": "-1"},
            {"tradeDate": "bad", "peg": "13"},
        ]}
        records = parse_csindex_payload(payload, INDEX_SPEC_BY_CODE["000300"])
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0].trade_date, "20260720")
        self.assertEqual(records[0].pe_ttm, 14.29)

    def test_world_pe_parser_converts_zero_based_month_and_drops_nonpositive(self):
        html = """
        decoy_data = [[Date.UTC(2026, 0, 31),1.48]];
        detailPE_data = [[Date.UTC(2026, 0, 31),25.50],
                         [Date.UTC(2026, 1, 28),26.25],
                         [Date.UTC(2026, 2, 31),-2.0]];
        another_data = [[Date.UTC(2026, 3, 30),0.65]];
        """
        records = parse_world_pe_html(html, INDEX_SPEC_BY_CODE["SP500"])
        self.assertEqual([item.trade_date for item in records], ["20260131", "20260228"])
        self.assertEqual([item.pe_ttm for item in records], [25.5, 26.25])
        self.assertTrue(all(item.is_proxy for item in records))

    def test_issuer_snapshot_parsers_keep_methodology_explicit(self):
        spy = '<b>Price/Earnings Ratio FY1</b><td class="data">22.24</td>'
        iwm = '<span data-id="fundamentalsAndRisk-priceEarnings-data">19.21</span>'
        self.assertEqual(parse_issuer_snapshot_html(spy, "SPY"), (22.24, "发行方FY1市盈率"))
        self.assertEqual(parse_issuer_snapshot_html(iwm, "IWM"), (19.21, "发行方组合市盈率"))
        self.assertIsNone(parse_issuer_snapshot_html("no pe", "QQQ"))

    def test_world_pe_parser_rejects_page_without_target_series(self):
        with self.assertRaisesRegex(ValueError, "detailPE_data"):
            parse_world_pe_html(
                "other_data = [[Date.UTC(2026, 0, 31),25.50]];",
                INDEX_SPEC_BY_CODE["SP500"],
            )

    def test_cni_snapshot(self):
        payload = {"data": {"rows": [
            {"indexcode": "399006", "peDynamic": "38.2007"}
        ]}}
        self.assertEqual(parse_cni_snapshot(payload), ("399006", 38.2007))
        self.assertEqual(
            parse_cni_snapshot({"rows": [{"indexCode": "399006", "peDynamic": "38.2"}]}),
            ("399006", 38.2),
        )

    def test_hsi_archive_and_pdf_text(self):
        archive = {"contentList": [{"resourcesList": [{
            "title": "Monthly Roundup (June 2026)",
            "url": "https://example.com/june.pdf",
        }]}]}
        parsed_archive = parse_hsi_archive(archive)
        self.assertEqual(parsed_archive[0]["trade_date"], "20260630")
        self.assertEqual(parsed_archive[0]["url"], "https://example.com/june.pdf")
        relative = {"contentList": [{"resourcesList": [{
            "title": "Monthly Roundup (May 2026)", "url": "/reports/may.pdf",
        }]}]}
        self.assertEqual(
            parse_hsi_archive(relative)[0]["url"],
            "https://www.hsi.com.hk/reports/may.pdf",
        )
        pdf_text = """
        Hang Seng Index             4.2%  3.1%  8.3%  14.2%  12.68  3.27%
        Hang Seng TECH Index        5.2%  4.1%  9.3%  15.2%  27.56  1.08%
        """
        records = parse_hsi_pdf_text(pdf_text, "20260630")
        self.assertEqual({item.index_code: item.pe_ttm for item in records}, {
            "HSI": 12.68, "HSTECH": 27.56,
        })

    def test_http_client_disables_environment_proxy(self):
        client = PublicGlobalValuationClient()
        self.assertFalse(client.session.trust_env)
        self.assertEqual(client.timeout, 15.0)
        self.assertEqual(client.max_attempts, 3)


class GlobalIndexValuationCalculationTest(unittest.TestCase):
    def test_cache_version_changes_after_table_is_populated(self):
        engine = create_engine("sqlite:///:memory:")
        self.assertEqual(get_global_index_valuation_cache_version(engine), "missing")
        spec = INDEX_SPEC_BY_CODE["000300"]
        store_global_valuation_records(engine, [GlobalValuationRecord(
            "20260721", spec.code, spec.name, spec.market, 14.2,
            spec.source_name, spec.source_url, "滚动市盈率",
        )])
        version = get_global_index_valuation_cache_version(engine)
        self.assertNotEqual(version, "missing")
        self.assertTrue(version.startswith("1:20260721:"))

    def test_percentile_keeps_sign_and_requires_36_positive_months(self):
        self.assertIsNone(empirical_percentile(list(range(1, 36)), 20))
        self.assertEqual(empirical_percentile(list(range(1, 37)), 18), 50.0)
        values = [-99, 0] + list(range(1, 37))
        self.assertEqual(empirical_percentile(values, 36), 100.0)

    def test_labels_cover_fixed_boundaries(self):
        self.assertEqual(percentile_label(20), "历史低位")
        self.assertEqual(percentile_label(40), "偏低")
        self.assertEqual(percentile_label(60), "中性")
        self.assertEqual(percentile_label(80), "偏高")
        self.assertEqual(percentile_label(80.1), "历史高位")

    def test_idempotent_write_and_fixed_dashboard_order(self):
        engine = create_engine("sqlite:///:memory:")
        spec = INDEX_SPEC_BY_CODE["000300"]
        records = []
        for month_end in pd.date_range("2023-01-31", periods=42, freq="ME"):
            records.append(GlobalValuationRecord(
                month_end.strftime("%Y%m%d"), spec.code, spec.name, spec.market,
                10 + len(records) / 10, spec.source_name, spec.source_url,
                "滚动市盈率", False, "ok", {},
            ))
        self.assertEqual(store_global_valuation_records(engine, records), 42)
        replacement = records[-1]
        self.assertEqual(store_global_valuation_records(engine, [replacement]), 1)
        with engine.connect() as conn:
            count = conn.execute(text(
                "SELECT COUNT(*) FROM global_index_valuation_daily"
            )).scalar_one()
        self.assertEqual(count, 42)

        payload = build_global_index_valuation_dashboard(engine, as_of_date="20260630")
        self.assertEqual([card["name"] for card in payload["cards"]], [
            "纳斯达克100", "标普500", "罗素2000", "沪深300", "创业板指", "科创50",
            "中证500", "中证1000", "中证2000", "恒生指数", "恒生科技指数",
        ])
        card = next(item for item in payload["cards"] if item["code"] == "000300")
        self.assertEqual(card["percentile"], 100.0)
        self.assertEqual(len(payload["series_by_code"]["000300"]), 42)

    def test_month_end_sampling_and_insufficient_history(self):
        engine = create_engine("sqlite:///:memory:")
        spec = INDEX_SPEC_BY_CODE["000688"]
        records = []
        for month in range(1, 13):
            for day, pe in ((5, 20 + month), (25, 30 + month)):
                records.append(GlobalValuationRecord(
                    f"2025{month:02d}{day:02d}", spec.code, spec.name, spec.market,
                    pe, spec.source_name, spec.source_url, "滚动市盈率",
                ))
        store_global_valuation_records(engine, records)
        payload = build_global_index_valuation_dashboard(engine, as_of_date="20251231")
        card = next(item for item in payload["cards"] if item["code"] == "000688")
        self.assertIsNone(card["percentile"])
        self.assertEqual(card["sample_count"], 12)
        self.assertEqual(card["history_label"], "成立以来分位")
        self.assertEqual(len(payload["series_by_code"]["000688"]), 12)

    def test_stale_gate_keeps_value_but_marks_quality(self):
        engine = create_engine("sqlite:///:memory:")
        spec = INDEX_SPEC_BY_CODE["SP500"]
        records = [GlobalValuationRecord(
            pd.Timestamp("2023-01-31") .strftime("%Y%m%d"), spec.code, spec.name,
            spec.market, 20, spec.source_name, spec.source_url, "ETF代理PE", True, "proxy",
        )]
        for month_end in pd.date_range("2023-02-28", periods=35, freq="ME"):
            records.append(GlobalValuationRecord(
                month_end.strftime("%Y%m%d"), spec.code, spec.name, spec.market,
                20, spec.source_name, spec.source_url, "ETF代理PE", True, "proxy",
            ))
        store_global_valuation_records(engine, records)
        payload = build_global_index_valuation_dashboard(engine, as_of_date="20270131")
        card = next(item for item in payload["cards"] if item["code"] == "SP500")
        self.assertEqual(card["quality_status"], "stale")
        self.assertIsNotNone(card["current_pe"])


class GlobalIndexValuationUpdaterTest(unittest.TestCase):
    def setUp(self):
        self.engine = create_engine("sqlite:///:memory:")

    def tearDown(self):
        self.engine.dispose()

    def test_chinext_local_history_and_official_mismatch_gate(self):
        with self.engine.begin() as conn:
            conn.execute(text(
                "CREATE TABLE index_valuation (trade_date TEXT, ts_code TEXT, pe_ttm DOUBLE, pe DOUBLE)"
            ))
            conn.execute(text(
                "INSERT INTO index_valuation VALUES ('2026-07-17', '399006.SZ', 40.0, 39.0)"
            ))
        records = load_chinext_history(self.engine, "20260701", "20260720")
        checked, warning = validate_chinext_snapshot(records, ("399006", 30.0))
        self.assertIn("超过5%", warning)
        self.assertEqual(checked[-1].quality_status, "source_mismatch")
        self.assertEqual(checked[-1].raw_detail["cni_snapshot_pe"], 30.0)

    def test_chinext_primary_history_comes_from_tushare(self):
        api = SimpleNamespace(index_dailybasic=lambda **kwargs: pd.DataFrame([
            {"ts_code": "399006.SZ", "trade_date": "20260720", "pe_ttm": 38.5},
            {"ts_code": "399006.SZ", "trade_date": "20260719", "pe_ttm": -1},
        ]))
        fake_tushare = SimpleNamespace(set_token=lambda token: None, pro_api=lambda: api)
        with patch.dict("sys.modules", {"tushare": fake_tushare}):
            records = fetch_chinext_history("20260701", "20260720")
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0].trade_date, "20260720")
        self.assertEqual(records[0].pe_ttm, 38.5)
        self.assertIn("Tushare连续滚动PE", records[0].methodology)

    def test_hsi_daily_update_selects_latest_available_report(self):
        resources = [
            {"trade_date": "20260531", "title": "May", "url": "may.pdf"},
            {"trade_date": "20260630", "title": "June", "url": "june.pdf"},
        ]
        selected = select_hsi_reports(resources, "20260101", "20260720", False)
        self.assertEqual([item["trade_date"] for item in selected], ["20260630"])

    def test_us_issuer_snapshot_parsers_and_soft_validation(self):
        self.assertEqual(
            parse_spy_issuer_pe('Price/Earnings</th><td class="data">26.92</td>'),
            26.92,
        )
        iwm_html = (
            '&quot;label&quot;:&quot;Expense Ratio&quot;,&quot;value&quot;:0.19,'
            '&quot;label&quot;:&quot;P/E Ratio&quot;,'
            '&quot;value&quot;:19.21235,&quot;returnType&quot;:false,'
            '&quot;fullName&quot;:&quot;fundamentalsAndRisk.priceEarnings&quot;'
        )
        self.assertEqual(parse_iwm_issuer_pe(iwm_html), 19.21235)
        spy = INDEX_SPEC_BY_CODE["SP500"]
        iwm = INDEX_SPEC_BY_CODE["RUSSELL2000"]
        records = [
            GlobalValuationRecord("20260701", spy.code, spy.name, spy.market, 26.75, spy.source_name, spy.source_url, "ETF代理PE", True, "proxy"),
            GlobalValuationRecord("20260701", iwm.code, iwm.name, iwm.market, 18.54, iwm.source_name, iwm.source_url, "ETF代理PE", True, "proxy"),
        ]
        class FakeClient:
            def request(self, method, url):
                if "ssga.com" in url:
                    return SimpleNamespace(text='Price/Earnings</th><td class="data">26.92</td>')
                return SimpleNamespace(text=iwm_html)
        checked, warnings = validate_us_proxy_snapshots(records, FakeClient())
        self.assertEqual(warnings, [])
        self.assertEqual(checked[0].raw_detail["issuer_snapshot"], "State Street SPY")
        self.assertEqual(checked[1].raw_detail["issuer_snapshot"], "iShares IWM")

    @patch("update_global_index_valuation.fetch_hsi_archive", return_value=[])
    @patch("update_global_index_valuation.fetch_world_pe_records", return_value=[])
    @patch("update_global_index_valuation.fetch_cni_snapshot")
    @patch("update_global_index_valuation.fetch_chinext_history")
    @patch("update_global_index_valuation.fetch_csindex_records", return_value=[])
    def test_historical_as_of_does_not_compare_with_current_cni_snapshot(
        self, csindex_mock, chinext_mock, snapshot_mock, world_mock, hsi_mock,
    ):
        spec = INDEX_SPEC_BY_CODE["399006"]
        chinext_mock.return_value = [GlobalValuationRecord(
            "20200131", spec.code, spec.name, spec.market, 40.0,
            spec.source_name, spec.source_url, "历史PE",
        )]
        with self.assertLogs("global_valuation_update", level="INFO") as captured:
            records, _ = collect_valuation_records(
                self.engine, object(), "20200101", "20200131", False,
            )
        self.assertEqual([record.index_code for record in records], ["399006"])
        snapshot_mock.assert_not_called()
        log_text = "\n".join(captured.output)
        self.assertIn("中证指数 [1/5] 沪深300：正在读取", log_text)
        self.assertIn("创业板指：完成", log_text)
        self.assertIn("恒生月报：目录共0份", log_text)
        self.assertIn("全部来源读取完成", log_text)

    def test_logging_suppresses_noisy_pypdf_layout_messages(self):
        configure_logging()
        self.assertEqual(logging.getLogger("pypdf").level, logging.ERROR)

    @patch("update_global_index_valuation.store_global_valuation_records")
    @patch("update_global_index_valuation.collect_valuation_records")
    @patch("update_global_index_valuation.PublicGlobalValuationClient")
    @patch("update_global_index_valuation.create_engine_from_env")
    def test_dry_run_never_writes(self, engine_mock, client_mock, collect_mock, store_mock):
        spec = INDEX_SPEC_BY_CODE["000300"]
        engine_mock.return_value = self.engine
        collect_mock.return_value = ([GlobalValuationRecord(
            "20260720", spec.code, spec.name, spec.market, 14.2,
            spec.source_name, spec.source_url, "滚动市盈率",
        )], [])
        self.assertEqual(update_main(["--date", "2026-07-20", "--dry-run"]), 0)
        store_mock.assert_not_called()


if __name__ == "__main__":
    unittest.main()
