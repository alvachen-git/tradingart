import unittest
from datetime import date, datetime

import macro_risk_radar_generator as radar


def _sample_context():
    return {
        "yield_curve": {
            "current": {"2Y": 4.10, "10Y": 4.35, "30Y": 4.80},
            "prev7": {"2Y": 4.05, "10Y": 4.20, "30Y": 4.70},
            "delta1d": {"2Y": 0.02, "10Y": 0.03, "30Y": 0.02},
            "spread_10_2": 0.25,
            "spread_10_2_prev7": 0.15,
            "as_of_date": date(2026, 4, 10),
        },
        "gold_silver_ratio": {
            "latest_value": 88.2,
            "previous_value": 87.9,
            "change": 0.3,
            "as_of_date": date(2026, 4, 10),
            "series": [(date(2026, 4, 9), 87.9), (date(2026, 4, 10), 88.2)],
        },
        "cpi": {"latest_value": 330.2, "change": 0.5, "as_of_date": date(2026, 4, 9), "recently_updated": True},
        "nfp": {"latest_value": 158000.0, "change": -50.0, "as_of_date": date(2026, 3, 1), "recently_updated": False},
        "fed": {
            "funds_rate": {"latest_value": 3.75, "change": 0.0, "as_of_date": date(2026, 3, 1), "series": [(date(2026, 3, 1), 3.75), (date(2026, 4, 1), 3.95)]},
            "balance_sheet": {"latest_value": 6693000.0, "change": -12000.0, "as_of_date": date(2026, 4, 8), "series": [(date(2026, 4, 1), 6705000.0), (date(2026, 4, 8), 6693000.0)]},
        },
        "calendar": {"next_fomc_date": date(2026, 4, 29), "next_cpi_date": date(2026, 4, 13), "next_nfp_date": date(2026, 5, 1)},
        "news": {
            "lines": ["headline a", "headline b"],
            "items": [
                {"source": "财联社", "title": "中东冲突再起，油价波动", "time": "2026-04-11 09:30", "summary": "地缘冲突扰动能源价格。"},
                {"source": "东方财富", "title": "美国国债收益率上行", "time": "2026-04-11 10:05", "summary": "市场重新交易降息路径。"},
            ],
        },
        "reporter_cards": {"cards": [{"module": "收益率记者", "facts": ["10Y-2Y: +0.25%"]}], "digest": "收益率记者:10Y-2Y:+0.25%"},
        "freshness_rows": [{"name": "美国10Y国债", "value": 4.35, "as_of_date": "2026-04-10", "source": "macro_daily", "status": "fresh", "stale_days": 1}],
    }


class TestMacroRiskRadarGenerator(unittest.TestCase):
    def test_news_dedup(self):
        items = [
            {"source": "A", "title": "美元指数走强", "time": "", "summary": ""},
            {"source": "B", "title": "美元指数走强", "time": "", "summary": "重复"},
            {"source": "C", "title": "美债收益率上行", "time": "", "summary": ""},
        ]
        out = radar._dedupe_news_items(items, limit=10)
        self.assertEqual(len(out), 2)

    def test_rule_based_analysis_keys(self):
        out = radar._build_rule_based_analysis(_sample_context(), event_window_days=7)
        for key in radar.REQUIRED_ANALYSIS_KEYS:
            self.assertIn(key, out)
            self.assertTrue(str(out[key]).strip())
        self.assertIn("风险偏好", out["macro_news_comment"])
        self.assertIn("股票", out["allocation_advice_comment"])

    def test_llm_disabled_uses_fallback(self):
        out, source = radar._build_llm_analysis(_sample_context(), event_window_days=7, use_llm=False)
        self.assertEqual(source, "disabled_by_flag")
        self.assertIn("allocation_advice_comment", out)

    def test_extract_json_obj_handles_fenced_json(self):
        raw = (
            "```json\n"
            "{\"overview\":\"ok\",\"yield_curve_comment\":\"a\",\"gold_silver_comment\":\"b\","
            "\"cpi_nfp_comment\":\"c\",\"fed_policy_comment\":\"d\",\"macro_news_comment\":\"e\","
            "\"allocation_advice_comment\":\"f\"}\n```"
        )
        out = radar._extract_json_obj(raw)
        self.assertEqual(out.get("overview"), "ok")

    def test_render_html_contains_updated_sections(self):
        html_text = radar.render_macro_radar_html(
            generated_at=datetime(2026, 4, 11, 20, 30, 0),
            context=_sample_context(),
            analysis=radar._build_rule_based_analysis(_sample_context(), event_window_days=7),
            analysis_source="fallback",
            event_window_days=7,
            chart_lookback_days=90,
        )
        self.assertIn("解释来源：交易汇AI宏观主编", html_text)
        self.assertIn("美联储利率与资产负债表（近 120 天）", html_text)
        self.assertIn("下次美联储议息会议", html_text)
        self.assertIn("联邦基金利率:", html_text)
        self.assertIn("资产负债表:", html_text)
        self.assertIn("下次 CPI 日期", html_text)
        self.assertIn("下次非农日期", html_text)
        self.assertIn("cpi-nfp-chart", html_text)
        self.assertIn("投资建议（示例配置）", html_text)
        self.assertIn("建议依据：", html_text)
        self.assertNotIn("下一阶段推演", html_text)
        self.assertNotIn("reporter-grid", html_text)

    def test_extract_summary(self):
        summary = radar.extract_summary_from_html("<h1>Title</h1><p>summary text</p>")
        self.assertTrue(summary.startswith("Title"))

    def test_next_release_dates_use_official_calendar(self):
        as_of = date(2026, 4, 11)
        self.assertEqual(radar._next_cpi_date(as_of), date(2026, 5, 12))
        self.assertEqual(radar._next_nfp_date(as_of), date(2026, 5, 8))


if __name__ == "__main__":
    unittest.main()
