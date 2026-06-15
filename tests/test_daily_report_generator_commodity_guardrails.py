import unittest
from unittest.mock import patch

import daily_report_generator as drg


class FakeKlineTool:
    def __init__(self):
        self.calls = []

    def invoke(self, payload):
        self.calls.append(payload)
        query = payload["query"]
        if query == "碳酸锂":
            return """
**一、今日形态信号**
普通震荡K线，无明显形态。

**二、今日K线事实**
开176000 高178740 低174600 收175300
"""
        if query == "原油":
            return """
**一、今日形态信号**
【极致压缩破位】(压缩比2.2ATR)

**二、今日K线事实**
开585 高591.1 低553.3 收557
"""
        return ""


class DailyReportCommodityGuardrailTest(unittest.TestCase):
    def test_extract_kline_shape_from_report_uses_today_signal(self):
        lithium_report = """
**一、今日形态信号**
普通震荡K线，无明显形态。

**二、今日K线事实**
上影显著长于下影
"""
        crude_report = """
**一、今日形态信号**
【极致压缩破位】(压缩比2.2ATR)

**二、今日K线事实**
实体占比74.1%
"""

        self.assertEqual(drg._extract_kline_shape_from_report(lithium_report), "普通震荡K线")
        self.assertEqual(drg._extract_kline_shape_from_report(crude_report), "极致压缩破位")

    def test_commodity_card_validation_rejects_made_up_shapes(self):
        html = """
        <div>🔋 碳酸锂</div><p>形态：三只乌鸦形态<br>隐含波动率：38.59%（偏低）</p>
        <div>🛢️ 原油</div><p>形态：长下影十字星<br>隐含波动率：42.68%（低）</p>
        """
        expected_iv = {
            "碳酸锂": {"iv": 38.59},
            "原油": {"iv": 42.68},
        }
        expected_kline = {
            "碳酸锂": {"shape": "普通震荡K线"},
            "原油": {"shape": "极致压缩破位"},
        }

        with patch.object(drg, "COMMODITY_CARD_LIST", ["碳酸锂", "原油"]):
            is_valid, anomalies = drg.validate_commodity_cards(html, expected_iv, expected_kline)

        self.assertFalse(is_valid)
        self.assertTrue(any("碳酸锂 形态与真值不一致" in item for item in anomalies))
        self.assertTrue(any("原油 形态与真值不一致" in item for item in anomalies))

    def test_commodity_card_validation_accepts_programmatic_shapes(self):
        html = """
        <div>🔋 碳酸锂</div><p>形态：普通震荡K线<br>隐含波动率：38.59%（偏低）</p>
        <div>🛢️ 原油</div><p>形态：极致压缩破位<br>隐含波动率：42.68%（低）</p>
        """
        expected_iv = {
            "碳酸锂": {"iv": 38.59},
            "原油": {"iv": 42.68},
        }
        expected_kline = {
            "碳酸锂": {"shape": "普通震荡K线"},
            "原油": {"shape": "极致压缩破位"},
        }

        with patch.object(drg, "COMMODITY_CARD_LIST", ["碳酸锂", "原油"]):
            is_valid, anomalies = drg.validate_commodity_cards(html, expected_iv, expected_kline)

        self.assertTrue(is_valid, anomalies)

    def test_kline_snapshot_passes_report_trade_date_to_tool(self):
        fake_tool = FakeKlineTool()

        with patch.object(drg, "COMMODITY_CARD_LIST", ["碳酸锂", "原油"]):
            with patch.object(drg, "analyze_kline_pattern", fake_tool):
                snapshot, snapshot_text = drg._fetch_programmatic_commodity_kline_snapshot("20260612")

        self.assertEqual(snapshot["碳酸锂"]["shape"], "普通震荡K线")
        self.assertEqual(snapshot["原油"]["shape"], "极致压缩破位")
        self.assertIn("形态=普通震荡K线", snapshot_text)
        self.assertEqual(
            fake_tool.calls,
            [
                {"query": "碳酸锂", "trade_date": "20260612"},
                {"query": "原油", "trade_date": "20260612"},
            ],
        )


if __name__ == "__main__":
    unittest.main()
