import unittest
import importlib.util
import sys
import types
from unittest.mock import patch


def _install_missing_import_stubs():
    if importlib.util.find_spec("streamlit") is None:
        st = types.ModuleType("streamlit")

        def cache_resource(func=None, **_kwargs):
            if func is None:
                return lambda wrapped: wrapped
            return func

        st.cache_resource = cache_resource
        sys.modules["streamlit"] = st

    if importlib.util.find_spec("dotenv") is None:
        dotenv = types.ModuleType("dotenv")
        dotenv.load_dotenv = lambda *args, **kwargs: None
        sys.modules["dotenv"] = dotenv

    if importlib.util.find_spec("sqlalchemy") is None:
        sqlalchemy = types.ModuleType("sqlalchemy")
        sqlalchemy.create_engine = lambda *args, **kwargs: None
        sqlalchemy.text = lambda sql, *args, **kwargs: sql
        sys.modules["sqlalchemy"] = sqlalchemy

    if importlib.util.find_spec("langchain_core") is None:
        langchain_core = types.ModuleType("langchain_core")
        tools_mod = types.ModuleType("langchain_core.tools")

        class _ToolWrapper:
            def __init__(self, func):
                self.func = func
                self.__name__ = getattr(func, "__name__", "tool")

            def invoke(self, payload):
                if isinstance(payload, dict):
                    return self.func(**payload)
                return self.func(payload)

            def __call__(self, *args, **kwargs):
                return self.func(*args, **kwargs)

        tools_mod.tool = lambda func=None, **_kwargs: _ToolWrapper(func) if func else lambda wrapped: _ToolWrapper(wrapped)
        sys.modules["langchain_core"] = langchain_core
        sys.modules["langchain_core.tools"] = tools_mod

    if importlib.util.find_spec("tushare") is None:
        sys.modules["tushare"] = types.ModuleType("tushare")


_install_missing_import_stubs()

try:
    import pandas as pd
    import kline_tools
except Exception as exc:  # pragma: no cover
    pd = None
    kline_tools = None
    _IMPORT_ERROR = exc
else:
    _IMPORT_ERROR = None


def _make_df(last_bar, base_close=10.0, drift=0.02):
    rows = []
    for i in range(59):
        close = base_close + i * drift
        rows.append(
            {
                "trade_date": f"202601{i + 1:02d}",
                "open_price": close - 0.03,
                "high_price": close + 0.08,
                "low_price": close - 0.08,
                "close_price": close,
            }
        )
    rows.append({"trade_date": "20260301", **last_bar})
    return pd.DataFrame(rows)


def _make_qfq_ex_rights_df():
    rows = []
    for i in range(50):
        close = 50.0 + i * 0.18
        rows.append(
            {
                "trade_date": f"202603{i + 1:02d}",
                "open_price": close - 0.1,
                "high_price": close + 0.5,
                "low_price": close - 0.5,
                "close_price": close,
            }
        )

    tail = [
        ("20260601", 64.6828),
        ("20260602", 69.9845),
        ("20260603", 71.4454),
        ("20260604", 78.8493),
        ("20260605", 74.4169),
        ("20260608", 75.0369),
        ("20260609", 81.0227),
        ("20260610", 76.9609),
        ("20260611", 77.5),
        ("20260612", 80.36),
    ]
    for trade_date, close in tail:
        rows.append(
            {
                "trade_date": trade_date,
                "open_price": close - 0.5,
                "high_price": close + 1.0,
                "low_price": close - 1.0,
                "close_price": close,
            }
        )
    return pd.DataFrame(rows)


@unittest.skipIf(kline_tools is None, f"kline_tools import failed: {_IMPORT_ERROR}")
class TestKlineToolsShadowFacts(unittest.TestCase):
    def _invoke_with_df(self, df):
        with patch.object(kline_tools, "engine", object()), patch.object(
            kline_tools, "STOCK_DAILY_SOURCE", "stock_price"
        ), patch.object(
            kline_tools.symbol_map, "resolve_symbol", return_value=("TEST.SH", "stock")
        ), patch.object(
            kline_tools.pd, "read_sql", return_value=df
        ):
            return kline_tools.analyze_kline_pattern.invoke({"query": "测试标的"})

    def test_hanging_man_reports_long_lower_shadow_not_upper(self):
        df = _make_df(
            {
                "open_price": 11.20,
                "high_price": 11.22,
                "low_price": 10.70,
                "close_price": 11.30,
            },
            base_close=10.0,
            drift=0.02,
        )

        out = self._invoke_with_df(df)

        self.assertIn("【吊人线】(长下影小实体，高位反转预警)", out)
        self.assertIn("主导影线：下影显著长于上影", out)
        self.assertNotIn("吊人线】(长上影", out)

    def test_hammer_reports_long_lower_shadow(self):
        df = _make_df(
            {
                "open_price": 10.00,
                "high_price": 10.02,
                "low_price": 9.50,
                "close_price": 10.10,
            },
            base_close=10.8,
            drift=-0.005,
        )

        out = self._invoke_with_df(df)

        self.assertIn("【锤子线】(长下影小实体，多头抵抗)", out)
        self.assertIn("主导影线：下影显著长于上影", out)

    def test_shooting_star_reports_long_upper_shadow(self):
        df = _make_df(
            {
                "open_price": 11.20,
                "high_price": 11.80,
                "low_price": 11.18,
                "close_price": 11.30,
            },
            base_close=10.0,
            drift=0.02,
        )

        out = self._invoke_with_df(df)

        self.assertIn("【射击之星】(长上影小实体，高位卖压沉重)", out)
        self.assertIn("主导影线：上影显著长于下影", out)

    def test_zero_body_does_not_divide_by_zero(self):
        df = _make_df(
            {
                "open_price": 11.20,
                "high_price": 11.40,
                "low_price": 11.00,
                "close_price": 11.20,
            },
            base_close=10.0,
            drift=0.02,
        )

        out = self._invoke_with_df(df)

        self.assertIn("**二、今日K线事实**", out)
        self.assertIn("实体占全日振幅 0.0%", out)

    def test_a_share_uses_qfq_for_moving_average_across_ex_rights_gap(self):
        qfq_df = _make_qfq_ex_rights_df()
        raw_df = qfq_df.copy()
        raw_df.loc[raw_df["trade_date"] == "20260608", "close_price"] = 105.30
        raw_df.loc[raw_df["trade_date"] == "20260609", "close_price"] = 113.70
        raw_df.loc[raw_df["trade_date"] == "20260610", "close_price"] = 108.00

        def fake_read_sql(sql, *_args, **_kwargs):
            sql_text = str(sql)
            if "stock_price_qfq" in sql_text:
                return qfq_df.copy()
            return raw_df.copy()

        with patch.object(kline_tools, "engine", object()), patch.object(
            kline_tools, "STOCK_DAILY_SOURCE", "stock_price"
        ), patch.object(
            kline_tools.symbol_map, "resolve_symbol", return_value=("688257.SH", "stock")
        ), patch.object(
            kline_tools.pd, "read_sql", side_effect=fake_read_sql
        ):
            out = kline_tools.analyze_kline_pattern.invoke({"query": "新锐股份技术面分析"})

        self.assertIn("数据源：stock_price_qfq(前复权)", out)
        self.assertIn("MA5: 78.18", out)
        self.assertNotIn("MA5: 96.97", out)

    def test_bj_stock_uses_qfq_for_moving_average(self):
        qfq_df = _make_qfq_ex_rights_df()
        raw_df = qfq_df.copy()
        raw_df.loc[raw_df["trade_date"] == "20260608", "close_price"] = 105.30
        raw_df.loc[raw_df["trade_date"] == "20260609", "close_price"] = 113.70
        raw_df.loc[raw_df["trade_date"] == "20260610", "close_price"] = 108.00

        def fake_read_sql(sql, *_args, **_kwargs):
            sql_text = str(sql)
            if "stock_price_qfq" in sql_text:
                return qfq_df.copy()
            return raw_df.copy()

        with patch.object(kline_tools, "engine", object()), patch.object(
            kline_tools, "STOCK_DAILY_SOURCE", "stock_price"
        ), patch.object(
            kline_tools.symbol_map, "resolve_symbol", return_value=("920001.BJ", "stock")
        ), patch.object(
            kline_tools.pd, "read_sql", side_effect=fake_read_sql
        ):
            out = kline_tools.analyze_kline_pattern.invoke({"query": "920001.BJ技术面分析"})

        self.assertIn("数据源：stock_price_qfq(前复权)", out)
        self.assertIn("MA5: 78.18", out)
        self.assertNotIn("MA5: 96.97", out)

    def test_a_share_missing_qfq_fails_closed_instead_of_raw_ma(self):
        raw_df = _make_qfq_ex_rights_df()
        raw_df.loc[raw_df["trade_date"] == "20260608", "close_price"] = 105.30
        raw_df.loc[raw_df["trade_date"] == "20260609", "close_price"] = 113.70
        raw_df.loc[raw_df["trade_date"] == "20260610", "close_price"] = 108.00

        def fake_read_sql(sql, *_args, **_kwargs):
            sql_text = str(sql)
            if "stock_price_qfq" in sql_text:
                return pd.DataFrame(columns=["trade_date", "open_price", "high_price", "low_price", "close_price"])
            return raw_df.copy()

        with patch.object(kline_tools, "engine", object()), patch.object(
            kline_tools, "STOCK_DAILY_SOURCE", "stock_price"
        ), patch.object(
            kline_tools.symbol_map, "resolve_symbol", return_value=("688257.SH", "stock")
        ), patch.object(
            kline_tools.pd, "read_sql", side_effect=fake_read_sql
        ):
            out = kline_tools.analyze_kline_pattern.invoke({"query": "新锐股份技术面分析"})

        self.assertIn("前复权日线数据缺失", out)
        self.assertIn("已拒绝使用未复权 stock_price", out)
        self.assertNotIn("MA5:", out)

    def test_a_share_stale_qfq_fails_closed_instead_of_using_old_latest_bar(self):
        qfq_df = _make_qfq_ex_rights_df()
        raw_df = pd.concat(
            [
                qfq_df.copy(),
                pd.DataFrame(
                    [
                        {
                            "trade_date": "20260615",
                            "open_price": 82.00,
                            "high_price": 83.00,
                            "low_price": 81.00,
                            "close_price": 82.50,
                        }
                    ]
                ),
            ],
            ignore_index=True,
        )

        def fake_read_sql(sql, *_args, **_kwargs):
            sql_text = str(sql)
            if "stock_price_qfq" in sql_text:
                return qfq_df.copy()
            return raw_df.copy()

        with patch.object(kline_tools, "engine", object()), patch.object(
            kline_tools, "STOCK_DAILY_SOURCE", "stock_price"
        ), patch.object(
            kline_tools.symbol_map, "resolve_symbol", return_value=("510300.SH", "stock")
        ), patch.object(
            kline_tools.pd, "read_sql", side_effect=fake_read_sql
        ):
            out = kline_tools.analyze_kline_pattern.invoke({"query": "300ETF技术面分析"})

        self.assertIn("前复权滞后", out)
        self.assertIn("最新20260612", out)
        self.assertIn("stock_price最新20260615", out)
        self.assertIn("已拒绝使用未复权或滞后的前复权数据", out)
        self.assertNotIn("今日收盘", out)
        self.assertNotIn("MA5:", out)


if __name__ == "__main__":
    unittest.main()
