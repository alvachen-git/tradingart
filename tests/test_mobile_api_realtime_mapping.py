import unittest

_IMPORT_ERROR = None
try:
    import mobile_api
except Exception as exc:  # pragma: no cover
    mobile_api = None
    _IMPORT_ERROR = exc


@unittest.skipIf(mobile_api is None, f"mobile_api import failed: {_IMPORT_ERROR}")
class TestMobileApiRealtimeMapping(unittest.TestCase):
    def test_new_products_have_sina_mapping(self):
        cases = {
            "PK2605": "nf_PK2605",
            "BR2605": "nf_BR2605",
            "LG2605": "nf_LG2605",
            "EC2606": "nf_EC2606",
            "PT2606": "nf_PT2606",
            "PX2605": "nf_PX2605",
        }
        for contract, expected in cases.items():
            with self.subTest(contract=contract):
                self.assertEqual(mobile_api._to_sina_code(contract), expected)


if __name__ == "__main__":
    unittest.main()
