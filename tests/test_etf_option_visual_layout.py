import ast
from pathlib import Path
import unittest
from unittest.mock import patch

import pandas as pd
import plotly.express as px

ROOT = Path(__file__).resolve().parents[1]
PAGE_PATH = ROOT / "pages" / "01_ETF期权.py"
SHARED_CHART_PATH = ROOT / "option_kline_chart.py"
TOOL_PATH = ROOT / "etf_option_tool.py"
CLIMATE_PATH = ROOT / "cn_market_climate_data.py"


def _load_etf_chart_helpers():
    source = PAGE_PATH.read_text(encoding="utf-8")
    module = ast.parse(source)
    wanted = {
        "_prepare_price_iv_frames",
        "_line_records",
        "_build_etf_kline_dataset",
        "_build_etf_kline_payload",
        "_build_defense_figure",
    }
    nodes = [
        node
        for node in module.body
        if isinstance(node, ast.FunctionDef) and node.name in wanted
    ]
    namespace = {"pd": pd, "px": px}
    exec(
        compile(ast.Module(body=nodes, type_ignores=[]), filename="<etf_chart_helpers>", mode="exec"),
        namespace,
    )
    return namespace


def _load_etf_tool_helpers():
    module = ast.parse(TOOL_PATH.read_text(encoding="utf-8"))
    nodes = [
        node
        for node in module.body
        if isinstance(node, ast.FunctionDef) and node.name == "_read_kline_with_volume"
    ]
    namespace = {"pd": pd, "engine": object()}
    exec(
        compile(ast.Module(body=nodes, type_ignores=[]), filename="<etf_tool_helpers>", mode="exec"),
        namespace,
    )
    return namespace


class EtfOptionVisualLayoutTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.source = PAGE_PATH.read_text(encoding="utf-8")
        cls.shared_source = SHARED_CHART_PATH.read_text(encoding="utf-8")
        cls.helpers = _load_etf_chart_helpers()
        cls.tool_helpers = _load_etf_tool_helpers()
        cls.climate_source = CLIMATE_PATH.read_text(encoding="utf-8")

    def test_overview_defense_and_global_valuation_view_order(self):
        self.assertIn('view_options = ["总览", "持仓防线", "股市估值"]', self.source)
        self.assertNotIn('view_options = ["总览", "价格与IV", "持仓防线"]', self.source)
        self.assertIn('st.session_state["etf_option_active_view"] = "总览"', self.source)
        self.assertIn(') or "总览"', self.source)
        self.assertIn('key="etf_option_active_view"', self.source)

    def test_global_valuation_stops_before_etf_data_requests(self):
        self.assertIn("build_global_index_valuation_dashboard", self.source)
        self.assertIn("get_global_index_valuation_cache_version", self.source)
        self.assertIn("_cached_global_index_valuation_dashboard", self.source)
        self.assertIn(
            "def _cached_global_index_valuation_dashboard(cache_version: str):",
            self.source,
        )
        self.assertNotIn(
            "def _cached_global_index_valuation_dashboard(_cache_version: str):",
            self.source,
        )
        self.assertIn(
            "_cached_global_index_valuation_dashboard(valuation_cache_version)",
            self.source,
        )
        self.assertIn('if active_view == "股市估值":', self.source)
        self.assertIn('stage="global_valuation_done"', self.source)
        valuation_branch = self.source.rfind('if active_view == "股市估值":')
        etf_fetch = self.source.index("df = _cached_etf_option_analysis")
        self.assertLess(valuation_branch, etf_fetch)
        self.assertIn("st.stop()", self.source[valuation_branch:etf_fetch])
        self.assertIn("全球主要指数 · PE历史分位", self.source)

    def test_global_valuation_uses_summary_table_and_history_without_methodology_clutter(self):
        self.assertIn('class="global-valuation-summary"', self.source)
        self.assertIn('class="global-valuation-table-shell"', self.source)
        self.assertIn('class="global-valuation-table-group"', self.source)
        self.assertIn("历史分位（0–100）", self.source)
        self.assertIn("市盈率（PE）历史走势", self.source)
        self.assertIn("历史中位数", self.source)
        self.assertIn("20%分位", self.source)
        self.assertIn("80%分位", self.source)
        self.assertIn('default_name = "科创50"', self.source)
        self.assertIn('range_options = ["近1年", "近3年", "近5年", "近10年", "全部"]', self.source)
        self.assertIn("data_date", self.source)
        self.assertNotIn("ETF代理", self.source)
        self.assertNotIn("含官方回溯历史", self.source)
        self.assertNotIn("创业板PE与国证指数核对偏差超过5%", self.source)
        self.assertNotIn("source_name", self.source)
        self.assertNotIn("quality_message", self.source)
        self.assertNotIn('card.get("note")', self.source)

    def test_each_view_renders_only_its_primary_chart(self):
        self.assertEqual(self.source.count("_render_price_iv_chart("), 2)
        self.assertEqual(self.source.count("_render_defense_chart("), 2)
        self.assertNotIn("etf-lab-defense-section", self.source)

    def test_defense_view_restores_detail_table_without_duplicating_overview(self):
        self.assertEqual(self.source.count("_render_defense_detail_table("), 2)
        self.assertIn(
            'if active_view == "持仓防线":\n    _render_defense_detail_table(df)',
            self.source,
        )
        self.assertIn('with st.expander("查看详细数据表")', self.source)
        for field in ("date_str", "type", "strike", "oi", "price", "code"):
            self.assertIn(f"'{field}'", self.source)
        self.assertIn('NumberColumn("持仓量(张)", format="%d")', self.source)

    def test_defense_chart_shows_open_interest_beside_every_point(self):
        frame = pd.DataFrame(
            {
                "date_obj": pd.to_datetime(["2026-07-16", "2026-07-17"] * 2),
                "strike": [5.0, 5.1, 4.8, 4.9],
                "oi": [12500, 13200, 9800, 10100],
                "type": ["认购 (压力)", "认购 (压力)", "认沽 (支撑)", "认沽 (支撑)"],
            }
        )

        figure = self.helpers["_build_defense_figure"](frame, "510300 (300ETF)")
        traces = {trace.name: trace for trace in figure.data}

        self.assertEqual(set(traces), {"认购 (压力)", "认沽 (支撑)"})
        for trace in traces.values():
            self.assertIn("markers", trace.mode)
            self.assertIn("text", trace.mode)
            self.assertEqual(trace.texttemplate, "%{text:,.0f}")
            self.assertFalse(trace.cliponaxis)
        self.assertEqual(list(traces["认购 (压力)"].text), [12500, 13200])
        self.assertEqual(list(traces["认沽 (支撑)"].text), [9800, 10100])
        self.assertEqual(traces["认购 (压力)"].textposition, "top center")
        self.assertEqual(traces["认沽 (支撑)"].textposition, "bottom center")
        self.assertIn("点旁数字为持仓量", self.source)

    def test_market_climate_strip_replaces_the_five_duplicate_metrics(self):
        self.assertIn('class="etf-lab-kpi-strip"', self.source)
        self.assertIn('grid-template-columns: repeat(auto-fit, minmax(150px, 1fr))', self.source)
        self.assertIn("load_cn_market_climate_strip", self.source)
        self.assertIn("ttl=600", self.source)
        self.assertEqual(self.source.count("_cached_cn_market_climate_strip()"), 2)
        for label in (
            "沪深融资杠杆",
            "融资5日动能",
            "沪深成交额",
            "50/1000强弱",
            "科创/创业强弱",
            "创业/1000强弱",
            "中国10Y利率",
            "IM贴水压力",
        ):
            self.assertIn(label, self.climate_source)
        self.assertNotIn('class="etf-lab-summary"', self.source)
        self.assertNotIn('grid-template-columns: repeat(5, minmax(0, 1fr))', self.source)
        self.assertNotIn("_render_summary_strip", self.source)

    def test_market_climate_copy_is_plain_language_and_hides_sample_counts(self):
        self.assertNotIn("样本{sample_count}", self.climate_source)
        self.assertIn("融资升温", self.climate_source)
        self.assertIn("放量升温", self.climate_source)
        self.assertIn("利率上行", self.climate_source)
        self.assertIn("value=_fmt_percentile(percentile)", self.climate_source)
        self.assertIn(".etf-lab-kpi:nth-child(4n) .etf-lab-kpi-tooltip", self.source)
        self.assertIn(".etf-lab-kpi:nth-child(3n) .etf-lab-kpi-tooltip", self.source)

    def test_main_chart_is_full_width_shared_us_style_without_outer_header(self):
        self.assertIn("max-width: 100% !important", self.source)
        self.assertIn('st.columns([2.55, 1.05], gap="small")', self.source)
        self.assertIn("render_option_kline_chart(", self.source)
        self.assertIn("height=650", self.source)
        self.assertNotIn("StreamlitChart", self.source)
        self.assertNotIn("<h2>价格与波动率</h2>", self.source)
        self.assertNotIn("最新价&nbsp;", self.source)
        self.assertNotIn("etf-lab-chart-note", self.source)

    def test_chart_controls_are_internal_and_keep_etf_specific_reference_lines(self):
        self.assertNotIn('key="etf_option_price_period"', self.source)
        self.assertNotIn('key="etf_option_show_iv"', self.source)
        self.assertIn('"enablePeriodSwitch": True', self.source)
        self.assertIn('"showTitle": False', self.source)
        self.assertIn('"showLatest": False', self.source)
        self.assertIn('"priceDigits": 3', self.source)
        self.assertIn('"storageNamespace": "etf-options-chart-drawings"', self.source)
        self.assertIn('"ivLabel": "平均IV"', self.source)
        self.assertIn('const ivLabel = String(config.ivLabel || "ATM IV")', self.shared_source)
        self.assertIn('addToggleLine("iv", ivLabel', self.shared_source)
        self.assertIn('addReadoutItem(ivLabel', self.shared_source)
        self.assertIn('"title": "压力"', self.source)
        self.assertIn('"title": "支撑"', self.source)
        for control in ("MA5", "MA20", "MA60", "水平线", "趋势线", "删除", "清空"):
            self.assertIn(control, self.shared_source)

    def test_weekly_ohlcv_is_aggregated_and_moving_averages_are_recomputed(self):
        dates = pd.bdate_range("2026-01-05", periods=35)
        kline = pd.DataFrame(
            {
                "trade_date": dates,
                "open": [value - 0.5 for value in range(1, 36)],
                "high": [value + 0.5 for value in range(1, 36)],
                "low": [value - 0.75 for value in range(1, 36)],
                "close": list(range(1, 36)),
                "volume": [100.0] * 35,
            }
        )
        iv = pd.DataFrame({"trade_date": dates, "iv": list(range(10, 45))})

        weekly, weekly_iv = self.helpers["_prepare_price_iv_frames"](kline, iv, "weekly")

        self.assertEqual(len(weekly), 7)
        self.assertEqual(float(weekly.iloc[0]["open"]), 0.5)
        self.assertEqual(float(weekly.iloc[0]["high"]), 5.5)
        self.assertEqual(float(weekly.iloc[0]["low"]), 0.25)
        self.assertEqual(float(weekly.iloc[0]["close"]), 5.0)
        self.assertEqual(float(weekly.iloc[0]["volume"]), 500.0)
        self.assertAlmostEqual(float(weekly.iloc[-1]["ma5"]), weekly["close"].tail(5).mean())
        self.assertEqual(float(weekly_iv.iloc[-1]["iv"]), 44.0)

    def test_missing_volume_falls_back_to_zero_without_blocking_prices(self):
        dates = pd.bdate_range("2026-07-01", periods=5)
        kline = pd.DataFrame(
            {
                "trade_date": dates,
                "open": [1, 2, 3, 4, 5],
                "high": [2, 3, 4, 5, 6],
                "low": [0, 1, 2, 3, 4],
                "close": [1.5, 2.5, 3.5, 4.5, 5.5],
            }
        )
        daily, _ = self.helpers["_prepare_price_iv_frames"](
            kline,
            pd.DataFrame(columns=["trade_date", "iv"]),
            "daily",
        )

        self.assertEqual(daily["volume"].tolist(), [0.0] * 5)
        self.assertAlmostEqual(float(daily.iloc[-1]["ma5"]), 3.5)

    def test_payload_contains_daily_weekly_data_and_three_digit_reference_lines(self):
        dates = pd.bdate_range("2026-01-05", periods=65)
        kline = pd.DataFrame(
            {
                "trade_date": dates,
                "open": [4.8] * 65,
                "high": [5.1] * 65,
                "low": [4.7] * 65,
                "close": [4.9] * 65,
                "volume": [1000.0] * 65,
            }
        )
        iv = pd.DataFrame({"trade_date": dates, "iv": [20.0] * 65})

        payload = self.helpers["_build_etf_kline_payload"](
            kline,
            iv,
            symbol="510300.SH",
            pressure=5.25,
            support=4.7,
        )

        self.assertTrue(payload["datasets"]["daily"]["candles"])
        self.assertTrue(payload["datasets"]["weekly"]["candles"])
        self.assertEqual(payload["config"]["priceDigits"], 3)
        self.assertTrue(payload["config"]["useTimeVisibleRange"])
        self.assertEqual(payload["referenceLines"][0]["price"], 5.25)
        self.assertEqual(payload["referenceLines"][1]["price"], 4.7)
        self.assertIn("referenceExtentSeries", self.shared_source)
        self.assertIn("setVisibleRange", self.shared_source)

    def test_tool_volume_query_has_schema_fallback(self):
        fallback_frame = pd.DataFrame(
            [{"trade_date": "20260714", "open": 1.0, "high": 2.0, "low": 0.5, "close": 1.5}]
        )
        with patch.object(
            pd,
            "read_sql",
            side_effect=[RuntimeError("unknown column vol"), fallback_frame],
        ) as read_sql:
            result = self.tool_helpers["_read_kline_with_volume"]("index_price", "000300.SH", 20)

        self.assertIn("COALESCE(vol, 0) as volume", read_sql.call_args_list[0].args[0])
        self.assertEqual(result["volume"].tolist(), [0.0])

    def test_layout_has_desktop_tablet_and_mobile_breakpoints(self):
        self.assertIn("@media (max-width: 1360px)", self.source)
        self.assertIn("@media (max-width: 900px)", self.source)
        self.assertIn("@media (max-width: 640px)", self.source)
        self.assertIn("flex-direction: column !important", self.source)
        self.assertIn("grid-template-columns: minmax(0, 1fr)", self.source)


if __name__ == "__main__":
    unittest.main()
