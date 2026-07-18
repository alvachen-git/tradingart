import unittest
from unittest.mock import patch

import pandas as pd

import daily_report_generator as drg
import fund_flow_tools as fft


def _snapshot():
    sector_rows = {
        "国防军工": {"display_name": "国防军工", "main_flow_yi": 77.5},
        "医药生物": {"display_name": "医药生物", "main_flow_yi": 40.8},
        "汽车": {"display_name": "汽车", "main_flow_yi": 30.4},
        "电子": {"display_name": "电子", "main_flow_yi": -435.1},
        "半导体": {"display_name": "半导体", "main_flow_yi": -248.8},
        "数字芯片设计": {"display_name": "数字芯片设计", "main_flow_yi": -155.9},
    }
    return {
        "report_date": "20260710",
        "indices": {
            "创业板指": {"pct_change": -4.37},
        },
        "etfs": {
            "创业板": {"pct_change": -4.41},
        },
        "sectors": sector_rows,
        "sector_top_in": [sector_rows[x] for x in ["国防军工", "医药生物", "汽车"]],
        "sector_top_out": [sector_rows[x] for x in ["电子", "半导体", "数字芯片设计"]],
        "etf_iv": {
            "沪深300": {"iv_rank": 71.7, "level": "偏高"},
            "中证500": {"iv_rank": 61.0, "level": "偏高"},
            "创业板": {"iv_rank": 82.9, "level": "高"},
            "科创50": {"iv_rank": 78.9, "level": "偏高"},
            "上证50": {"iv_rank": 61.7, "level": "偏高"},
        },
    }


class DailyReportAStockGuardrailTest(unittest.TestCase):
    def test_historical_report_date_controls_header_date_and_weekday(self):
        date_key, title_date, weekday = drg._report_date_context("20260710")

        self.assertEqual(date_key, "20260710")
        self.assertEqual(title_date, "2026年07月10日")
        self.assertEqual(weekday, "周五")

    def test_rejects_published_report_direction_flow_and_iv_conflicts(self):
        html = """
        <h4>股票板块</h4>
        <p>银行(-38.9亿)、房地产(-27.4亿)流出；半导体(+42.3亿)、新能源(+28.7亿)领跑净流入。</p>
        <h4>期货商持仓</h4>
        <h2>期权波动率</h2>
        <div>沪深300 45% 中；中证500 25% 低；创业板 65% 高；科创50 45% 中；上证50 25% 低。</div>
        <p>创业板高IV叠加突破年线大阳线，建议构建牛市看涨价差。</p>
        <h2>每日牛股</h2>
        """

        violations = drg.validate_a_share_report_facts(html, _snapshot())

        self.assertTrue(any("半导体主力净额与真值不一致" in item for item in violations))
        self.assertTrue(any("创业板ETF当日涨跌幅" in item and "大阳线" in item for item in violations))
        self.assertTrue(any("创业板 IV Rank与真值不一致" in item for item in violations))
        self.assertTrue(any("主力资金Top3必选行业" in item for item in violations))

    def test_accepts_programmatic_sector_direction_and_iv_facts(self):
        html = """
        <h4>股票板块</h4>
        <p>
          主力净流入：国防军工(+77.5亿)、医药生物(+40.8亿)、汽车(+30.4亿)；
          主力净流出：电子(-435.1亿)、半导体(-248.8亿)、数字芯片设计(-155.9亿)。
        </p>
        <h4>期货商持仓</h4>
        <h2>期权波动率</h2>
        <div>
          沪深300 71.7% 偏高；中证500 61.0% 偏高；创业板 82.9% 高；
          科创50 78.9% 偏高；上证50 61.7% 偏高。
        </div>
        <p>创业板ETF当日下跌4.41%，收出大阴线。</p>
        <h2>每日牛股</h2>
        """

        violations = drg.validate_a_share_report_facts(html, _snapshot())

        self.assertEqual(violations, [])

    def test_accepts_common_etf_and_sector_name_suffixes(self):
        html = """
        <h4>股票板块</h4>
        <p>
          主力净流入：国防军工板块(+77.5亿)、医药生物行业(+40.8亿)、汽车板块(+30.4亿)；
          主力净流出：电子行业(-435.1亿)、半导体板块(-248.8亿)、数字芯片设计行业(-155.9亿)。
        </p>
        <h4>期货商持仓</h4>
        <h2>期权波动率</h2>
        <div>
          沪深300ETF 71.7%（偏高）；中证500ETF 61.0%（偏高）；
          创业板ETF 82.9%（高）；科创50ETF 78.9%（偏高）；上证50ETF 61.7%（偏高）。
        </div>
        <h2>每日牛股</h2>
        """

        violations = drg.validate_a_share_report_facts(html, _snapshot())

        self.assertEqual(violations, [])

    def test_long_sector_name_does_not_match_nested_short_sector(self):
        snapshot = _snapshot()
        long_name_cases = (
            ("国有大型银行", 5.1, "银行", -11.2),
            ("光学光电子", 8.6, "电子", -435.1),
            ("消费电子", 7.4, "电子", -435.1),
        )
        for long_name, long_amount, short_name, short_amount in long_name_cases:
            with self.subTest(long_name=long_name, short_name=short_name):
                case_snapshot = dict(snapshot)
                sectors = dict(snapshot["sectors"])
                long_row = {"display_name": long_name, "main_flow_yi": long_amount}
                sectors[long_name] = long_row
                sectors[short_name] = {"display_name": short_name, "main_flow_yi": short_amount}
                case_snapshot["sectors"] = sectors
                case_snapshot["sector_top_in"] = [
                    long_row,
                    snapshot["sector_top_in"][1],
                    snapshot["sector_top_in"][2],
                ]
                html = f"""
                <h4>股票板块</h4>
                <p>
                  主力净流入：{long_name}({long_amount:+.1f}亿)、医药生物(+40.8亿)、汽车(+30.4亿)；
                  主力净流出：电子(-435.1亿)、半导体(-248.8亿)、数字芯片设计(-155.9亿)。
                </p>
                <h4>期货商持仓</h4>
                <h2>期权波动率</h2>
                <div>
                  沪深300 71.7% 偏高；中证500 61.0% 偏高；创业板 82.9% 高；
                  科创50 78.9% 偏高；上证50 61.7% 偏高。
                </div>
                <h2>每日牛股</h2>
                """

                violations = drg.validate_a_share_report_facts(html, case_snapshot)

                self.assertFalse(
                    any(f"{short_name}主力净额与真值不一致" in item for item in violations),
                    violations,
                )

    def test_wrong_short_sector_amount_is_still_rejected(self):
        snapshot = _snapshot()
        bank_row = {"display_name": "国有大型银行", "main_flow_yi": 5.1}
        snapshot["sectors"] = {
            **snapshot["sectors"],
            "国有大型银行": bank_row,
            "银行": {"display_name": "银行", "main_flow_yi": -11.2},
        }
        snapshot["sector_top_in"] = [bank_row, *_snapshot()["sector_top_in"][1:]]
        html = """
        <h4>股票板块</h4>
        <p>
          主力净流入：银行(+5.1亿)、医药生物(+40.8亿)、汽车(+30.4亿)；
          主力净流出：电子(-435.1亿)、半导体(-248.8亿)、数字芯片设计(-155.9亿)。
        </p>
        <h4>期货商持仓</h4>
        <h2>期权波动率</h2>
        <div>
          沪深300 71.7% 偏高；中证500 61.0% 偏高；创业板 82.9% 高；
          科创50 78.9% 偏高；上证50 61.7% 偏高。
        </div>
        <h2>每日牛股</h2>
        """

        violations = drg.validate_a_share_report_facts(html, snapshot)

        self.assertTrue(any("银行主力净额与真值不一致" in item for item in violations))

    def test_data_date_gate_fails_closed_on_stale_dataset(self):
        with self.assertRaises(drg.ReportDataNotReadyError) as ctx:
            drg._require_report_date("sector_moneyflow", "20260709", "20260710")

        self.assertIn("禁止用旧交易日数据", str(ctx.exception))

    def test_injects_visible_market_data_date(self):
        html = "<h1>爱波塔复盘晚报</h1><p>2026年07月10日 周五 | 深度复盘</p>"

        result = drg._inject_report_data_provenance(html, _snapshot())

        self.assertIn('data-market-trade-date="20260710"', result)
        self.assertIn("数据日：20260710", result)


class RetailMoneyFlowDateAndMetricTest(unittest.TestCase):
    def test_uses_main_net_inflow_metric_and_formats_billions(self):
        dates = pd.DataFrame([{"trade_date": "20260710"}])
        ranked = pd.DataFrame([
            {"trade_date": "20260710", "industry": "国防军工", "main_net_inflow": 775000.0, "pct_change": 3.43, "net_rate": 1.0},
            {"trade_date": "20260710", "industry": "医药生物", "main_net_inflow": 408000.0, "pct_change": 2.82, "net_rate": 1.0},
            {"trade_date": "20260710", "industry": "汽车", "main_net_inflow": 304000.0, "pct_change": 1.13, "net_rate": 1.0},
            {"trade_date": "20260710", "industry": "数字芯片设计", "main_net_inflow": -1559000.0, "pct_change": -4.44, "net_rate": -1.0},
            {"trade_date": "20260710", "industry": "半导体", "main_net_inflow": -2488000.0, "pct_change": -5.38, "net_rate": -1.0},
            {"trade_date": "20260710", "industry": "电子", "main_net_inflow": -4351000.0, "pct_change": -3.13, "net_rate": -1.0},
        ])

        with patch.object(fft, "engine", object()), patch.object(
            fft.pd,
            "read_sql",
            side_effect=[dates, ranked],
        ):
            result = fft.tool_get_retail_money_flow.func(days=1, as_of_date="20260710")

        self.assertIn("主力净流入", result)
        self.assertIn("国防军工**: +77.5亿", result)
        self.assertIn("半导体**: -248.8亿", result)
        self.assertNotIn("hidden_flow", result)

    def test_hierarchy_duplicates_use_one_top3_position(self):
        dates = pd.DataFrame([{"trade_date": "20260716"}])
        rows = pd.DataFrame([
            {"trade_date": "20260716", "industry": "计算机", "main_net_inflow": 208533.0, "pct_change": 1.0, "net_rate": 1.0},
            {"trade_date": "20260716", "industry": "IT服务Ⅱ", "main_net_inflow": 145097.0, "pct_change": 2.0, "net_rate": 1.0},
            {"trade_date": "20260716", "industry": "IT服务Ⅲ", "main_net_inflow": 145097.0, "pct_change": 2.0, "net_rate": 1.0},
            {"trade_date": "20260716", "industry": "印制电路板", "main_net_inflow": 94415.2, "pct_change": 3.0, "net_rate": 1.0},
            {"trade_date": "20260716", "industry": "电子", "main_net_inflow": -3697520.0, "pct_change": -3.0, "net_rate": -1.0},
            {"trade_date": "20260716", "industry": "半导体", "main_net_inflow": -2653660.0, "pct_change": -2.0, "net_rate": -1.0},
            {"trade_date": "20260716", "industry": "通信", "main_net_inflow": -1153770.0, "pct_change": -1.0, "net_rate": -1.0},
        ])

        with patch.object(fft, "engine", object()), patch.object(
            fft.pd,
            "read_sql",
            side_effect=[dates, rows, dates, rows],
        ):
            snapshot = fft.build_sector_money_flow_snapshot(days=1, as_of_date="20260716")
            result = fft.tool_get_retail_money_flow.func(days=1, as_of_date="20260716")

        top_names = [row["display_name"] for row in snapshot["sector_top_in"]]
        self.assertEqual(top_names, ["计算机", "IT服务", "印制电路板"])
        self.assertEqual(snapshot["collapsed_duplicate_count"], 1)
        self.assertEqual(result.count("**IT服务**"), 1)
        self.assertNotIn("IT服务Ⅱ", result)
        self.assertNotIn("IT服务Ⅲ", result)

    def test_stale_as_of_date_is_reported_without_running_flow_query(self):
        dates = pd.DataFrame([{"trade_date": "20260709"}])
        with patch.object(fft, "engine", object()), patch.object(
            fft.pd,
            "read_sql",
            return_value=dates,
        ) as read_sql:
            result = fft.tool_get_retail_money_flow.func(days=1, as_of_date="20260710")

        self.assertIn("数据未就绪", result)
        self.assertIn("禁止将旧数据写成当日资金流", result)
        self.assertEqual(read_sql.call_count, 1)


if __name__ == "__main__":
    unittest.main()
