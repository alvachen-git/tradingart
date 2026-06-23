import unittest

import pandas as pd
from st_aggrid import GridOptionsBuilder

from market_monitor_grid import (
    AG_GRID_LOCALE_ZH_CN,
    GRID_NUMBER_FILTER_PARAMS,
    format_contract_expiry_suffix,
    format_contract_for_grid,
    make_grid_number_filter_value_getter,
)


class MarketMonitorGridTest(unittest.TestCase):
    def test_filter_locale_contains_visible_chinese_labels(self):
        self.assertEqual(AG_GRID_LOCALE_ZH_CN["greaterThan"], "大于")
        self.assertEqual(AG_GRID_LOCALE_ZH_CN["lessThan"], "小于")
        self.assertEqual(AG_GRID_LOCALE_ZH_CN["equals"], "等于")
        self.assertEqual(AG_GRID_LOCALE_ZH_CN["andCondition"], "且")
        self.assertEqual(AG_GRID_LOCALE_ZH_CN["orCondition"], "或")
        self.assertEqual(AG_GRID_LOCALE_ZH_CN["filterOoo"], "筛选...")

    def test_numeric_filter_config_is_explicit(self):
        df = pd.DataFrame({"散户变动(日)": [3910.0, 11784.0]})
        gb = GridOptionsBuilder.from_dataframe(df)
        gb.configure_column(
            "散户变动(日)",
            filter="agNumberColumnFilter",
            filterParams=GRID_NUMBER_FILTER_PARAMS,
            filterValueGetter=make_grid_number_filter_value_getter("散户变动(日)"),
        )

        column_def = gb.build()["columnDefs"][0]
        self.assertEqual(column_def["filter"], "agNumberColumnFilter")
        self.assertIn("greaterThan", column_def["filterParams"]["filterOptions"])
        self.assertIn("lessThan", column_def["filterParams"]["filterOptions"])
        self.assertEqual(column_def["filterParams"]["defaultOption"], "greaterThan")
        self.assertEqual(column_def["filterParams"]["maxNumConditions"], 1)
        self.assertEqual(column_def["filterParams"]["numAlwaysVisibleConditions"], 1)
        self.assertIn("散户变动(日)", column_def["filterValueGetter"].js_code)
        self.assertIn("replace(/[%+,\\s]/g, '')", column_def["filterValueGetter"].js_code)

    def test_contract_expiry_suffix_ignores_missing_values(self):
        self.assertEqual(format_contract_expiry_suffix(None), "")
        self.assertEqual(format_contract_expiry_suffix(float("nan")), "")
        self.assertEqual(
            format_contract_for_grid({"合约": "HC2610 (热卷)", "到期剩余天数": None}),
            "HC2610 (热卷)",
        )

    def test_contract_expiry_suffix_formats_warning_and_normal_days(self):
        self.assertEqual(format_contract_expiry_suffix(0), "⚠ D-0")
        self.assertEqual(format_contract_expiry_suffix(2), "⚠ D-2")
        self.assertEqual(format_contract_expiry_suffix(3), "D-3")


if __name__ == "__main__":
    unittest.main()
