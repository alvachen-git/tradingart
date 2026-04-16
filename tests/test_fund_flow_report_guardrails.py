import unittest

import fund_flow_report_generator as generator


class FundFlowReportGuardrailTest(unittest.TestCase):
    def test_extract_stock_codes_infers_a_share_suffix(self):
        text = "寒武纪（688256）与宁德时代(300750.SZ)，另有中际旭创(300308)"

        codes = generator._extract_stock_codes(text)

        self.assertIn("688256.SH", codes)
        self.assertIn("300750.SZ", codes)
        self.assertIn("300308.SZ", codes)

    def test_positive_stock_cannot_be_written_as_falling_in_risk_section(self):
        html = """
        <div>⚠️ 风险预警（规避品种）</div>
        <p>
          ▸ 寒武纪（688256） • 量能：量比0.41x，板块涨+2.5%而个股跌-3.8%
          • 风险：主力单日净流出1.2亿，逆市下跌+资金撤离双重警讯
          ▸ 宁德时代（300750） • 技术形态：万亿市值阴跌，北向连续3日净卖出
        </p>
        <div>💡 明日作战计划</div>
        """
        moves = {
            "688256.SH": {"name": "寒武纪", "pct_chg": 2.5903},
            "300750.SZ": {"name": "宁德时代", "pct_chg": 5.3318},
        }

        violations = generator.validate_fund_flow_report_direction(html, moves)

        self.assertEqual(len(violations), 2)
        self.assertTrue(any("寒武纪" in item and "逆市下跌" in item for item in violations))
        self.assertTrue(any("宁德时代" in item and "阴跌" in item for item in violations))

    def test_positive_stock_allows_conditional_risk_wording(self):
        html = """
        <div>⚠️ 风险预警（规避品种）</div>
        <p>
          ▸ 寒武纪（688256） • 量能：当日上涨但量能不足，资金分歧扩大
          • 风险：若次日跌破10日线，再降低观察仓位
        </p>
        <div>💡 明日作战计划</div>
        """
        moves = {
            "688256.SH": {"name": "寒武纪", "pct_chg": 2.5903},
        }

        violations = generator.validate_fund_flow_report_direction(html, moves)

        self.assertEqual(violations, [])


if __name__ == "__main__":
    unittest.main()
