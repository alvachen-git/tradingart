import unittest

import pandas as pd

import update_market_climate_daily as climate


class MarketClimateDailyTests(unittest.TestCase):
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
