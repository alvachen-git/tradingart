import unittest

import update_stock_tiingo
import us_options_polygon


class UsOptionsUnderlyingStockPoolTests(unittest.TestCase):
    def test_option_equity_underlyings_have_stock_price_pool_coverage(self):
        stock_symbols = set(update_stock_tiingo.SYMBOLS)
        option_symbols = set(us_options_polygon.DEFAULT_UNDERLYINGS)
        etf_symbols = {
            "SPY",
            "QQQ",
            "DIA",
            "IWM",
            "GLD",
            "TLT",
            "SLV",
            "XLF",
            "XLE",
            "HYG",
            "SMH",
            "EEM",
            "FXI",
            "USO",
            "KRE",
            "XBI",
            "XLI",
            "XLK",
            "XLV",
            "XLY",
        }

        self.assertFalse((option_symbols - etf_symbols) - stock_symbols)


if __name__ == "__main__":
    unittest.main()
