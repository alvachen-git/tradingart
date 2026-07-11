import unittest
from unittest.mock import patch

import pandas as pd

import update_market_climate_daily as climate


class MarketClimateDailyTests(unittest.TestCase):
    def test_remove_local_dependency_override_drops_codex_temp_path(self):
        original_path = list(climate.sys.path)
        blocked = str((climate.Path(climate.__file__).resolve().parent / ".codex_pydeps").resolve())
        try:
            climate.sys.path.insert(0, blocked)

            removed = climate.remove_local_dependency_override()

            self.assertTrue(removed)
            self.assertNotIn(blocked, climate.sys.path)
        finally:
            climate.sys.path[:] = original_path

    def test_parse_cboe_csv_and_build_vix_term_record(self):
        vix9d = climate.parse_cboe_history_csv("DATE,CLOSE\n2026-06-28,14.5\n2026-06-30,15.1\n")
        vix = climate.parse_cboe_history_csv("DATE,VIX Close\n2026-06-30,16.2\n")
        vix3m = climate.parse_cboe_history_csv("DATE,CLOSE\n2026-06-30,17.4\n")

        record = climate.build_vix_term_record(vix9d, vix, vix3m)

        self.assertIsNotNone(record)
        self.assertEqual(record.indicator_code, "VIX_TERM")
        self.assertEqual(record.as_of_date.isoformat(), "2026-06-30")
        self.assertAlmostEqual(record.value, -2.3)
        self.assertAlmostEqual(record.secondary_value, 16.2)
        self.assertEqual(record.payload["vix9d"], 15.1)

    def test_build_vix_term_history_returns_all_common_dates(self):
        vix9d = climate.parse_cboe_history_csv("DATE,CLOSE\n2026-06-29,14.8\n2026-06-30,15.1\n")
        vix = climate.parse_cboe_history_csv("DATE,CLOSE\n2026-06-29,16.0\n2026-06-30,16.2\n")
        vix3m = climate.parse_cboe_history_csv("DATE,CLOSE\n2026-06-29,17.0\n2026-06-30,17.4\n")

        records = climate.build_vix_term_history(vix9d, vix, vix3m)

        self.assertEqual([record.as_of_date.isoformat() for record in records], ["2026-06-29", "2026-06-30"])
        self.assertAlmostEqual(records[0].value, -2.2)
        self.assertAlmostEqual(records[1].value, -2.3)

    def test_parse_aaii_sentiment_html(self):
        html = """
        <table>
          <tr><th>Date</th><th>Bullish</th><th>Neutral</th><th>Bearish</th></tr>
          <tr><td>6/25/2026</td><td>36.3%</td><td>26.7%</td><td>37.0%</td></tr>
        </table>
        """

        record = climate.parse_aaii_sentiment_html(html)

        self.assertIsNotNone(record)
        self.assertEqual(record.indicator_code, "AAII_BULL_BEAR")
        self.assertEqual(record.as_of_date.isoformat(), "2026-06-25")
        self.assertAlmostEqual(record.value, -0.7)
        self.assertEqual(record.payload["bearish_pct"], 37.0)

    def test_parse_aaii_sentiment_frame_uses_latest_percent_row(self):
        frame = pd.DataFrame(
            [
                ["Reported Date", "Bullish", "Neutral", "Bearish"],
                [pd.Timestamp("2026-06-18"), 0.31, 0.24, 0.45],
                [pd.Timestamp("2026-06-25"), 0.449339, 0.189427, 0.361233],
                ["Avg. '26", 0.38, 0.28, 0.34],
            ]
        )

        record = climate.parse_aaii_sentiment_frame(frame)

        self.assertIsNotNone(record)
        self.assertEqual(record.indicator_code, "AAII_BULL_BEAR")
        self.assertEqual(record.as_of_date.isoformat(), "2026-06-25")
        self.assertAlmostEqual(record.value, 8.8106, places=4)
        self.assertAlmostEqual(record.payload["bullish_pct"], 44.9339, places=4)
        self.assertAlmostEqual(record.payload["bearish_pct"], 36.1233, places=4)

    def test_parse_aaii_sentiment_history_frame_returns_all_surveys(self):
        frame = pd.DataFrame(
            [
                ["Reported Date", "Bullish", "Neutral", "Bearish"],
                [pd.Timestamp("2026-06-18"), 0.31, 0.24, 0.45],
                [pd.Timestamp("2026-06-25"), 0.449339, 0.189427, 0.361233],
                ["Avg. '26", 0.38, 0.28, 0.34],
            ]
        )

        records = climate.parse_aaii_sentiment_history_frame(frame)

        self.assertEqual([record.as_of_date.isoformat() for record in records], ["2026-06-18", "2026-06-25"])
        self.assertAlmostEqual(records[0].value, -14.0)
        self.assertAlmostEqual(records[1].value, 8.8106, places=4)

    def test_parse_aaii_insights_feed_uses_latest_survey_release(self):
        xml_text = """<?xml version="1.0" encoding="UTF-8"?>
        <rss xmlns:content="http://purl.org/rss/1.0/modules/content/" version="2.0">
          <channel>
            <item>
              <title>AAII Sentiment Survey: Optimism Leaps</title>
              <link>https://insights.aaii.com/p/older</link>
              <pubDate>Sat, 27 Jun 2026 15:30:39 GMT</pubDate>
              <content:encoded><![CDATA[
                <p>This week’s Sentiment Survey results:</p>
                <p>Bullish: 44.9%</p><p>Neutral: 19.0%</p><p>Bearish: 36.1%</p>
              ]]></content:encoded>
            </item>
            <item>
              <title>AAII Sentiment Survey: Optimism Plummets</title>
              <link>https://insights.aaii.com/p/latest</link>
              <pubDate>Sat, 04 Jul 2026 15:30:14 GMT</pubDate>
              <content:encoded><![CDATA[
                <p>This week’s Sentiment Survey results:</p>
                <p>Bullish: 31.4%, down 13.6 points</p>
                <p>Neutral: 26.4%, up 7.4 points</p>
                <p>Bearish: 42.3%, up 6.1 points</p>
              ]]></content:encoded>
            </item>
          </channel>
        </rss>"""

        record = climate.parse_aaii_insights_feed(xml_text)

        self.assertIsNotNone(record)
        self.assertEqual(record.as_of_date.isoformat(), "2026-07-02")
        self.assertAlmostEqual(record.value, -10.9)
        self.assertEqual(record.source, "aaii_insights")
        self.assertEqual(record.payload["article_url"], "https://insights.aaii.com/p/latest")

    def test_parse_aaii_insights_archive_returns_weekly_history(self):
        rows = [
            {
                "title": "AAII Sentiment Survey: Optimism Rises",
                "post_date": "2026-06-20T15:30:00.000Z",
                "canonical_url": "https://insights.aaii.com/p/older",
                "body_html": "<p>Bullish: 38.0%</p><p>Neutral: 27.0%</p><p>Bearish: 35.0%</p>",
            },
            {
                "title": "AAII Sentiment Survey: Pessimism Rises",
                "post_date": "2026-06-27T15:30:00.000Z",
                "canonical_url": "https://insights.aaii.com/p/latest",
                "body_html": "<p>Bullish: 31.0%</p><p>Neutral: 24.0%</p><p>Bearish: 45.0%</p>",
            },
        ]

        records = climate.parse_aaii_insights_archive(rows)

        self.assertEqual([record.as_of_date.isoformat() for record in records], ["2026-06-18", "2026-06-25"])
        self.assertAlmostEqual(records[0].value, 3.0)
        self.assertAlmostEqual(records[1].value, -14.0)
        self.assertEqual(records[1].source, "aaii_insights_archive")

    def test_fetch_aaii_sentiment_history_falls_back_to_insights_archive(self):
        archive_records = [
            climate.build_aaii_sentiment_record(
                pd.Timestamp("2026-06-25").date(), 31.0, 24.0, 45.0, source="aaii_insights_archive"
            )
        ]
        with (
            patch.object(climate, "http_get_bytes", side_effect=RuntimeError("403 Forbidden")),
            patch.object(climate, "fetch_aaii_insights_archive_history", return_value=archive_records),
        ):
            records = climate.fetch_aaii_sentiment_history(climate.make_session())

        self.assertEqual(records, archive_records)

    def test_fetch_aaii_sentiment_record_prefers_newer_insights_record(self):
        feed = """<?xml version="1.0" encoding="UTF-8"?>
        <rss xmlns:content="http://purl.org/rss/1.0/modules/content/" version="2.0">
          <channel><item>
            <title>AAII Sentiment Survey: Optimism Plummets</title>
            <link>https://insights.aaii.com/p/latest</link>
            <pubDate>Sat, 04 Jul 2026 15:30:14 GMT</pubDate>
            <content:encoded><![CDATA[
              Bullish: 31.4% Neutral: 26.4% Bearish: 42.3%
            ]]></content:encoded>
          </item></channel>
        </rss>"""
        old_record = climate.build_aaii_sentiment_record(
            pd.Timestamp("2026-06-25").date(),
            44.9,
            19.0,
            36.1,
            source="aaii_xls",
        )
        old_html = """
        <table><tr><th>Date</th><th>Bullish</th><th>Neutral</th><th>Bearish</th></tr>
        <tr><td>6/3/2026</td><td>36.3%</td><td>26.7%</td><td>37.0%</td></tr></table>
        """

        with (
            patch.object(climate, "http_get_text", side_effect=[feed, old_html]),
            patch.object(climate, "http_get_bytes", return_value=b"old-workbook"),
            patch.object(climate, "parse_aaii_sentiment_workbook", return_value=old_record),
        ):
            record = climate.fetch_aaii_sentiment_record(climate.make_session())

        self.assertIsNotNone(record)
        self.assertEqual(record.as_of_date.isoformat(), "2026-07-02")
        self.assertEqual(record.source, "aaii_insights")

    def test_update_result_status_marks_partial_source_failure(self):
        record = climate.build_aaii_sentiment_record(
            pd.Timestamp("2026-07-02").date(),
            31.4,
            26.4,
            42.3,
            source="aaii_insights",
        )

        status, exit_code = climate.update_result_status([record], {"gscpi": "timeout"})

        self.assertEqual(status, "partial_error")
        self.assertEqual(exit_code, 2)

    def test_fetch_records_rejects_stale_fallback_data(self):
        stale_record = climate.build_aaii_sentiment_record(
            pd.Timestamp("2026-06-25").date(),
            44.9,
            19.0,
            36.1,
            source="aaii_xls",
        )

        with patch.dict(climate.FETCHERS, {"aaii": lambda _session: stale_record}, clear=True):
            records, errors = climate.fetch_records(
                {"aaii"},
                today=pd.Timestamp("2026-07-10").date(),
            )

        self.assertEqual(records, [])
        self.assertIn("stale data", errors["aaii"])
        self.assertIn("age_days=15", errors["aaii"])

    def test_parse_fedwatch_payload_selects_highest_probability(self):
        payload = {
            "meetings": [
                {"meetingDate": "2026-07-29", "action": "Cut", "probability": 31.0},
                {"meetingDate": "2026-07-29", "action": "Hold", "probability": 62.4},
            ]
        }

        record = climate.parse_fedwatch_payload(payload, today=pd.Timestamp("2026-07-01").date())

        self.assertIsNotNone(record)
        self.assertEqual(record.indicator_code, "FEDWATCH")
        self.assertEqual(record.as_of_date.isoformat(), "2026-07-01")
        self.assertAlmostEqual(record.value, 62.4)
        self.assertEqual(record.payload["action_label"], "维持")

    def test_build_fedwatch_record_from_manual_probability(self):
        record = climate.build_fedwatch_record(
            "维持",
            "66.3%",
            "2026-07-29",
            as_of=pd.Timestamp("2026-07-01").date(),
        )

        self.assertIsNotNone(record)
        self.assertEqual(record.indicator_code, "FEDWATCH")
        self.assertEqual(record.as_of_date.isoformat(), "2026-07-01")
        self.assertAlmostEqual(record.value, 66.3)
        self.assertEqual(record.payload["action_label"], "维持")
        self.assertEqual(record.payload["meeting_date"], "2026-07-29")

    def test_parse_cftc_vix_json_calculates_leveraged_funds_net_oi(self):
        rows = [
            {
                "market_and_exchange_names": "S&P 500 STOCK INDEX - CHICAGO MERCANTILE EXCHANGE",
                "report_date_as_yyyy_mm_dd": "2026-06-24",
                "levered_funds_positions_long_all": "100",
                "levered_funds_positions_short_all": "80",
                "open_interest_all": "1000",
            },
            {
                "market_and_exchange_names": "VIX FUTURES - CBOE FUTURES EXCHANGE",
                "report_date_as_yyyy_mm_dd": "2026-06-24",
                "levered_funds_positions_long_all": "20,000",
                "levered_funds_positions_short_all": "35,000",
                "open_interest_all": "150,000",
            },
        ]

        record = climate.parse_cftc_vix_json(rows)

        self.assertIsNotNone(record)
        self.assertEqual(record.indicator_code, "CFTC_VIX_LEV_NET")
        self.assertEqual(record.as_of_date.isoformat(), "2026-06-24")
        self.assertAlmostEqual(record.value, -10.0)
        self.assertAlmostEqual(record.secondary_value, -15000)

    def test_parse_cftc_vix_json_history_returns_weekly_rows(self):
        rows = [
            {
                "market_and_exchange_names": "VIX FUTURES - CBOE FUTURES EXCHANGE",
                "report_date_as_yyyy_mm_dd": "2026-06-17",
                "levered_funds_positions_long_all": "20,000",
                "levered_funds_positions_short_all": "32,000",
                "open_interest_all": "150,000",
            },
            {
                "market_and_exchange_names": "S&P 500 STOCK INDEX - CHICAGO MERCANTILE EXCHANGE",
                "report_date_as_yyyy_mm_dd": "2026-06-24",
                "levered_funds_positions_long_all": "100",
                "levered_funds_positions_short_all": "80",
                "open_interest_all": "1000",
            },
            {
                "market_and_exchange_names": "VIX FUTURES - CBOE FUTURES EXCHANGE",
                "report_date_as_yyyy_mm_dd": "2026-06-24",
                "levered_funds_positions_long_all": "20,000",
                "levered_funds_positions_short_all": "35,000",
                "open_interest_all": "150,000",
            },
        ]

        records = climate.parse_cftc_vix_json_history(rows)

        self.assertEqual([record.as_of_date.isoformat() for record in records], ["2026-06-17", "2026-06-24"])
        self.assertAlmostEqual(records[0].value, -8.0)
        self.assertAlmostEqual(records[1].value, -10.0)

    def test_fetch_history_records_dedupes_by_indicator_and_date(self):
        older = climate.build_aaii_sentiment_record(
            pd.Timestamp("2026-06-18").date(),
            31.0,
            24.0,
            45.0,
            source="aaii_xls",
        )
        newer = climate.build_aaii_sentiment_record(
            pd.Timestamp("2026-06-25").date(),
            44.9,
            19.0,
            36.1,
            source="aaii_xls",
        )

        with patch.dict(
            climate.HISTORY_FETCHERS,
            {"aaii": lambda _session: [older, newer, newer]},
            clear=True,
        ):
            records, errors = climate.fetch_history_records({"aaii"})

        self.assertEqual(errors, {})
        self.assertEqual(len(records), 2)
        self.assertEqual(records[-1].as_of_date.isoformat(), "2026-06-25")

    def test_history_summary_reports_count_and_date_range(self):
        records = [
            climate.build_aaii_sentiment_record(
                pd.Timestamp("2026-06-18").date(), 31.0, 24.0, 45.0, source="aaii_xls"
            ),
            climate.build_aaii_sentiment_record(
                pd.Timestamp("2026-06-25").date(), 44.9, 19.0, 36.1, source="aaii_xls"
            ),
        ]

        summary = climate.history_summary(records)

        self.assertEqual(
            summary["AAII_BULL_BEAR"],
            {"count": 2, "start": "2026-06-18", "end": "2026-06-25"},
        )

    def test_parse_cftc_vix_text_calculates_leveraged_funds_net_oi(self):
        text_body = (
            '"VIX FUTURES - CBOE FUTURES EXCHANGE",260623,2026-06-23,1170E1,E   ,00,117 ,'
            '353236,81181,35494,22789,45448,70067,48294,71314,90177,58414\n'
        )

        record = climate.parse_cftc_vix_text(text_body)

        self.assertIsNotNone(record)
        self.assertEqual(record.indicator_code, "CFTC_VIX_LEV_NET")
        self.assertEqual(record.as_of_date.isoformat(), "2026-06-23")
        self.assertAlmostEqual(record.value, -5.340056, places=5)
        self.assertAlmostEqual(record.secondary_value, -18863)
        self.assertEqual(record.source, "cftc_txt")

    def test_build_gscpi_record_uses_latest_and_three_month_change(self):
        df = pd.DataFrame(
            {
                "Date": pd.to_datetime(["2026-02-01", "2026-03-01", "2026-04-01", "2026-05-01"]),
                "GSCPI": [-0.30, -0.20, -0.10, 0.15],
            }
        )

        record = climate.build_gscpi_record(df)

        self.assertIsNotNone(record)
        self.assertEqual(record.indicator_code, "GSCPI")
        self.assertEqual(record.as_of_date.isoformat(), "2026-05-01")
        self.assertAlmostEqual(record.value, 0.15)
        self.assertAlmostEqual(record.secondary_value, 0.45)


if __name__ == "__main__":
    unittest.main()
