import unittest
from unittest.mock import patch

_IMPORT_ERROR = None
try:
    from fastapi import HTTPException
    import mobile_api
except Exception as exc:  # pragma: no cover
    HTTPException = Exception
    mobile_api = None
    _IMPORT_ERROR = exc


def _payload(product="IH", window="3d"):
    return {
        "anchors": [],
        "contracts": [],
        "series": [],
        "summary": {"structure_type": "InsufficientData"},
        "meta": {"product_code": product, "window_key": window},
        "error": "no_trade_dates",
    }


def _payload_with_data(product="IH", window="3d"):
    return {
        "anchors": [{"label": "最新", "trade_date": "20260401", "display_date": "2026-04-01"}],
        "contracts": ["2604", "2605"],
        "series": [
            {
                "label": "最新",
                "trade_date": "20260401",
                "display_date": "2026-04-01",
                "points": [
                    {"contract": "2604", "close_price": 100.0, "oi": 10},
                    {"contract": "2605", "close_price": 102.0, "oi": 8},
                ],
            }
        ],
        "summary": {"structure_type": "Contango", "spread_abs": 2.0},
        "meta": {"product_code": product, "window_key": window},
    }


@unittest.skipIf(mobile_api is None, f"mobile_api import failed: {_IMPORT_ERROR}")
class TestMobileApiTermStructure(unittest.TestCase):
    def test_products_returns_defaults_and_windows(self):
        out = mobile_api.market_term_structure_products(username="u1")

        self.assertEqual(out["default_product"], "IH")
        self.assertEqual(out["default_window"], "3d")
        self.assertTrue(any(x["code"] == "IH" and x["is_index"] for x in out["items"]))
        self.assertEqual([x["key"] for x in out["windows"]], ["3d", "1w", "2w", "1m"])

    def test_index_product_returns_main_and_basis_payloads(self):
        with patch.object(mobile_api, "build_term_structure_payload", return_value=_payload_with_data("IH")) as main, patch.object(
            mobile_api, "build_index_basis_term_structure_payload", return_value={"series": []}
        ) as basis, patch.object(
            mobile_api, "build_index_basis_longterm_payload", return_value={"points": []}
        ) as longterm:
            out = mobile_api.market_term_structure(product="ih", window="1w", slots=7, username="u1")

        self.assertEqual(out["product"], "IH")
        self.assertEqual(out["product_name"], "上证50")
        self.assertTrue(out["is_index"])
        self.assertTrue(out["has_data"])
        self.assertEqual(out["window"], "1w")
        self.assertIsNotNone(out["basis_anchor"])
        self.assertIsNotNone(out["basis_longterm"])
        self.assertEqual(main.call_args.kwargs["product_code"], "IH")
        self.assertEqual(basis.call_args.kwargs["window_key"], "1w")
        self.assertEqual(longterm.call_args.kwargs["lookback_years"], 1)

    def test_commodity_product_does_not_request_basis_payloads(self):
        with patch.object(mobile_api, "build_term_structure_payload", return_value=_payload("RB")) as main, patch.object(
            mobile_api, "build_index_basis_term_structure_payload"
        ) as basis, patch.object(
            mobile_api, "build_index_basis_longterm_payload"
        ) as longterm:
            out = mobile_api.market_term_structure(product="rb", window="3d", slots=7, username="u1")

        self.assertEqual(out["product"], "RB")
        self.assertFalse(out["is_index"])
        self.assertIsNone(out["basis_anchor"])
        self.assertIsNone(out["basis_longterm"])
        self.assertEqual(main.call_args.kwargs["product_code"], "RB")
        basis.assert_not_called()
        longterm.assert_not_called()

    def test_no_data_payload_marks_has_data_false(self):
        with patch.object(mobile_api, "build_term_structure_payload", return_value=_payload("CU")):
            out = mobile_api.market_term_structure(product="cu", window="3d", slots=7, username="u1")

        self.assertFalse(out["has_data"])
        self.assertEqual(out["main"]["error"], "no_trade_dates")

    def test_invalid_window_defaults_and_slots_are_clamped(self):
        with patch.object(mobile_api, "build_term_structure_payload", return_value=_payload("CU")) as main:
            out = mobile_api.market_term_structure(product="cu", window="bad", slots=99, username="u1")

        self.assertEqual(out["window"], "3d")
        self.assertEqual(out["slots"], 12)
        self.assertEqual(main.call_args.kwargs["window_key"], "3d")
        self.assertEqual(main.call_args.kwargs["contract_slots"], 12)

    def test_invalid_product_rejects_request(self):
        with self.assertRaises(HTTPException) as cm:
            mobile_api.market_term_structure(product="NOT_REAL", window="3d", slots=7, username="u1")

        self.assertEqual(cm.exception.status_code, 400)


if __name__ == "__main__":
    unittest.main()
