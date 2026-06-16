import unittest

import pandas as pd
from st_aggrid import GridOptionsBuilder

from market_monitor_grid import (
    AG_GRID_LOCALE_ZH_CN,
    GRID_NUMBER_FILTER_PARAMS,
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


if __name__ == "__main__":
    unittest.main()
