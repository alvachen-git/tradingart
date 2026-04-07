import unittest

from sqlalchemy import create_engine, text

import term_structure_service as svc


def _seed(engine, rows):
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                CREATE TABLE futures_price (
                    trade_date TEXT,
                    ts_code TEXT,
                    close_price REAL,
                    oi REAL
                )
                """
            )
        )
        for row in rows:
            conn.execute(
                text(
                    """
                    INSERT INTO futures_price (trade_date, ts_code, close_price, oi)
                    VALUES (:trade_date, :ts_code, :close_price, :oi)
                    """
                ),
                row,
            )


class TestTermStructureService(unittest.TestCase):
    def test_normalize_contract_month_supports_3_and_4_digits(self):
        self.assertEqual(svc.normalize_contract_month("2605"), "2605")
        self.assertEqual(svc.normalize_contract_month("605"), "2605")
        self.assertIsNone(svc.normalize_contract_month("26A5"))

    def test_extract_contract_meta_parses_contract_code(self):
        meta = svc.extract_contract_meta("MA605.ZCE")
        self.assertIsNotNone(meta)
        self.assertEqual(meta["product_code"], "MA")
        self.assertEqual(meta["month"], "2605")
        self.assertEqual(meta["contract_code"], "MA605")

    def test_build_payload_has_3_anchors_and_7_slots(self):
        engine = create_engine("sqlite:///:memory:")
        rows = []
        dates = ["2026-04-07", "2026-04-03", "2026-04-02"]
        for di, d in enumerate(dates):
            for mi in range(5, 14):  # 2605~2613 (9 contracts)
                rows.append(
                    {
                        "trade_date": d,
                        "ts_code": f"PX26{mi:02d}",
                        "close_price": 10000 - di * 50 - mi * 10,
                        "oi": 1000 - mi,
                    }
                )
        _seed(engine, rows)

        payload = svc.build_term_structure_payload(engine, product_code="PX", window_key="3d", contract_slots=7)
        self.assertNotIn("error", payload)
        self.assertEqual(len(payload["anchors"]), 3)
        self.assertEqual([x["label"] for x in payload["anchors"]], ["窗口起点", "窗口中点", "最新"])
        self.assertEqual(len(payload["contracts"]), 7)
        self.assertEqual(payload["contracts"], ["2605", "2606", "2607", "2608", "2609", "2610", "2611"])
        self.assertEqual(len(payload["series"]), 3)
        self.assertEqual(payload["series"][0]["label"], "窗口起点")
        self.assertEqual(payload["series"][-1]["label"], "最新")

    def test_build_payload_czce_3_digit_month_sorting(self):
        engine = create_engine("sqlite:///:memory:")
        rows = [
            {"trade_date": "2026-04-07", "ts_code": "MA608", "close_price": 2500, "oi": 200},
            {"trade_date": "2026-04-07", "ts_code": "MA605", "close_price": 2400, "oi": 300},
            {"trade_date": "2026-04-07", "ts_code": "MA607", "close_price": 2450, "oi": 260},
            {"trade_date": "2026-04-03", "ts_code": "MA608", "close_price": 2490, "oi": 180},
            {"trade_date": "2026-04-03", "ts_code": "MA605", "close_price": 2410, "oi": 310},
            {"trade_date": "2026-04-03", "ts_code": "MA607", "close_price": 2460, "oi": 255},
            {"trade_date": "2026-04-02", "ts_code": "MA608", "close_price": 2480, "oi": 170},
            {"trade_date": "2026-04-02", "ts_code": "MA605", "close_price": 2420, "oi": 320},
            {"trade_date": "2026-04-02", "ts_code": "MA607", "close_price": 2470, "oi": 240},
        ]
        _seed(engine, rows)

        payload = svc.build_term_structure_payload(engine, product_code="MA", window_key="3d")
        self.assertEqual(payload["contracts"], ["2605", "2607", "2608"])

    def test_single_letter_prefix_should_not_match_multi_letter_product(self):
        engine = create_engine("sqlite:///:memory:")
        rows = [
            {"trade_date": "2026-04-07", "ts_code": "M2605", "close_price": 3000, "oi": 500},
            {"trade_date": "2026-04-07", "ts_code": "M2609", "close_price": 3100, "oi": 450},
            {"trade_date": "2026-04-07", "ts_code": "MA2605", "close_price": 2400, "oi": 600},
            {"trade_date": "2026-04-03", "ts_code": "M2605", "close_price": 2990, "oi": 505},
            {"trade_date": "2026-04-03", "ts_code": "M2609", "close_price": 3090, "oi": 455},
            {"trade_date": "2026-04-02", "ts_code": "M2605", "close_price": 2980, "oi": 510},
            {"trade_date": "2026-04-02", "ts_code": "M2609", "close_price": 3080, "oi": 460},
        ]
        _seed(engine, rows)

        payload = svc.build_term_structure_payload(engine, product_code="M", window_key="3d")
        self.assertEqual(payload["contracts"], ["2605", "2609"])
        for s in payload["series"]:
            self.assertEqual(len(s["points"]), 2)

    def test_build_payload_filters_stale_months(self):
        engine = create_engine("sqlite:///:memory:")
        rows = [
            {"trade_date": "2026-04-07", "ts_code": "IH2510", "close_price": 2990, "oi": 2000},  # stale
            {"trade_date": "2026-04-07", "ts_code": "IH2604", "close_price": 3040, "oi": 2500},
            {"trade_date": "2026-04-07", "ts_code": "IH2605", "close_price": 3032, "oi": 2600},
            {"trade_date": "2026-04-07", "ts_code": "IH2606", "close_price": 3028, "oi": 2300},
            {"trade_date": "2026-04-03", "ts_code": "IH2604", "close_price": 3036, "oi": 2400},
            {"trade_date": "2026-04-03", "ts_code": "IH2605", "close_price": 3025, "oi": 2500},
            {"trade_date": "2026-04-02", "ts_code": "IH2604", "close_price": 3030, "oi": 2380},
            {"trade_date": "2026-04-02", "ts_code": "IH2605", "close_price": 3018, "oi": 2460},
        ]
        _seed(engine, rows)

        payload = svc.build_term_structure_payload(engine, product_code="IH", window_key="3d", contract_slots=7)
        self.assertNotIn("2510", payload["contracts"])
        self.assertTrue(all(int(x) >= 2603 for x in payload["contracts"]))

    def test_build_payload_drops_single_point_contract(self):
        engine = create_engine("sqlite:///:memory:")
        rows = [
            {"trade_date": "2026-04-07", "ts_code": "AG2604", "close_price": 18100, "oi": 3000},
            {"trade_date": "2026-04-07", "ts_code": "AG2605", "close_price": 18080, "oi": 2900},
            {"trade_date": "2026-04-07", "ts_code": "AG2606", "close_price": 18050, "oi": 2800},
            {"trade_date": "2026-04-07", "ts_code": "AG2607", "close_price": 18030, "oi": 2700},
            {"trade_date": "2026-04-07", "ts_code": "AG2608", "close_price": 20500, "oi": 2600},  # only latest
            {"trade_date": "2026-04-03", "ts_code": "AG2604", "close_price": 17860, "oi": 3100},
            {"trade_date": "2026-04-03", "ts_code": "AG2605", "close_price": 17850, "oi": 3000},
            {"trade_date": "2026-04-03", "ts_code": "AG2606", "close_price": 17820, "oi": 2950},
            {"trade_date": "2026-04-02", "ts_code": "AG2604", "close_price": 17650, "oi": 3200},
            {"trade_date": "2026-04-02", "ts_code": "AG2605", "close_price": 17620, "oi": 3050},
            {"trade_date": "2026-04-02", "ts_code": "AG2606", "close_price": 17600, "oi": 3000},
        ]
        _seed(engine, rows)

        payload = svc.build_term_structure_payload(engine, product_code="AG", window_key="3d", contract_slots=7)
        self.assertNotIn("2608", payload["contracts"])
        self.assertIn("2604", payload["contracts"])


if __name__ == "__main__":
    unittest.main()
