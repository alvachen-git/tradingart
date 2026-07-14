import unittest
from pathlib import Path
from unittest.mock import patch

import ui_components


ROOT_DIR = Path(__file__).resolve().parents[1]
US_OPTIONS_PAGE = ROOT_DIR / "pages" / "29_美股期权.py"
OPTION_KLINE_CHART = ROOT_DIR / "option_kline_chart.py"


class OptionPageEdgeCompatibilityTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.us_options_source = US_OPTIONS_PAGE.read_text(encoding="utf-8")
        cls.chart_source = OPTION_KLINE_CHART.read_text(encoding="utf-8")

    def test_shared_option_controls_style_actual_baseweb_inner_nodes(self):
        with patch.object(ui_components.st, "markdown") as markdown:
            ui_components.inject_option_page_header_style()

        css = markdown.call_args.args[0]
        self.assertIn('[data-testid="stMain"] div[data-testid="stSelectbox"] [data-baseweb="select"] > div', css)
        self.assertIn("color-scheme: light !important", css)
        self.assertIn("background-color: #ffffff !important", css)
        self.assertIn("-webkit-text-fill-color: #0f172a !important", css)
        self.assertIn('button:not([aria-pressed="true"])', css)
        self.assertNotIn("forced-color-adjust: none", css)

    def test_us_options_controls_have_main_canvas_edge_fallback(self):
        source = self.us_options_source
        self.assertIn('[data-testid="stMain"] div[data-testid="stSelectbox"] [data-baseweb="select"] > div', source)
        self.assertIn("color-scheme: light !important", source)
        self.assertIn("background-color: #ffffff !important", source)
        self.assertIn("-webkit-text-fill-color: #0f172a !important", source)
        self.assertIn('button:not([aria-pressed="true"])', source)
        self.assertNotIn("forced-color-adjust: none", source)

    def test_us_options_rail_has_compact_and_stacked_breakpoints(self):
        source = self.us_options_source
        self.assertIn(
            "grid-template-columns: minmax(0, 1fr) minmax(76px, auto) minmax(70px, auto) 44px",
            source,
        )
        self.assertIn("box-sizing: border-box", source)
        self.assertIn("@media (max-width: 1600px)", source)
        self.assertIn('"main value thermo"', source)
        self.assertIn('"pct pct thermo"', source)
        self.assertIn("@media (max-width: 1360px)", source)
        self.assertIn(':has(.us-lab-rail)', source)
        self.assertIn("flex: 1 1 100% !important", source)
        self.assertIn("grid-template-columns: repeat(2, minmax(0, 1fr))", source)
        self.assertIn("@media (max-width: 768px)", source)

    def test_lightweight_chart_controls_cannot_collapse_into_vertical_text(self):
        chart_source = self.chart_source
        self.assertIn("color-scheme: light", chart_source)
        self.assertIn("flex: 0 0 auto", chart_source)
        self.assertIn("white-space: nowrap", chart_source)
        self.assertIn("word-break: keep-all", chart_source)
        self.assertIn("writing-mode: horizontal-tb", chart_source)
        self.assertIn("@media (max-width: 1040px)", chart_source)
        self.assertIn("flex-direction: column", chart_source)
        self.assertIn("top: 88px", chart_source)


if __name__ == "__main__":
    unittest.main()
