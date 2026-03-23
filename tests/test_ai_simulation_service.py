import unittest

import ai_simulation_service as svc
import pandas as pd


class TestAISimulationService(unittest.TestCase):
    def test_normalize_symbol_appends_market_suffix(self):
        self.assertEqual(svc._normalize_symbol("600519"), "600519.SH")
        self.assertEqual(svc._normalize_symbol("000001"), "000001.SZ")
        self.assertEqual(svc._normalize_symbol("830001"), "830001.BJ")
        self.assertEqual(svc._normalize_symbol("000300.SH"), "000300.SH")

    def test_apply_risk_gates_enforces_caps_and_universe(self):
        raw_actions = [
            {
                "symbol": "600519.SH",
                "action": "buy",
                "target_weight": 0.45,
                "reason": "high score",
                "confidence": 0.9,
            },
            {
                "symbol": "688001.SH",
                "action": "buy",
                "target_weight": 0.15,
                "reason": "unknown",
                "confidence": 0.8,
            },
            {
                "symbol": "510300.SH",
                "action": "sell",
                "target_weight": 0.0,
                "reason": "risk off",
                "confidence": 0.6,
            },
        ]
        current_weights = {"510300.SH": 0.25}
        candidate_symbols = {"600519.SH", "510300.SH"}
        config = {
            "max_single_weight_hard": 0.30,
            "max_single_weight_soft": 0.20,
            "max_positions": 5,
        }
        csi500_regime = {"regime": "neutral", "score": 0}
        style_map = {"600519.SH": "steady", "510300.SH": "steady", "688001.SH": "aggressive"}
        candidate_score_map = {"600519.SH": 90.0, "510300.SH": 80.0}

        audited, targets, notes = svc._apply_risk_gates(
            raw_actions,
            current_weights,
            candidate_symbols,
            config,
            csi500_regime,
            style_map,
            candidate_score_map,
        )

        status_map = {x["symbol"]: x["gate_status"] for x in audited}
        self.assertEqual(status_map["600519.SH"], "adjusted")
        self.assertEqual(status_map["688001.SH"], "rejected")
        self.assertLessEqual(targets["600519.SH"], 0.20 + 1e-9)
        self.assertTrue("510300.SH" not in targets or targets["510300.SH"] == 0.0)
        self.assertIsInstance(notes, list)

    def test_plan_trades_limits_count_and_turnover(self):
        target_weights = {
            "600519.SH": 0.25,
            "000001.SZ": 0.25,
            "510300.SH": 0.25,
            "159915.SZ": 0.25,
        }
        current_positions = {
            "510300.SH": {"quantity": 5000, "avg_cost": 3.8, "name": "300ETF"},
            "159915.SZ": {"quantity": 3000, "avg_cost": 2.0, "name": "创业板ETF"},
        }
        price_map = {
            "600519.SH": {"close": 1500.0},
            "000001.SZ": {"close": 12.0},
            "510300.SH": {"close": 4.0},
            "159915.SZ": {"close": 2.5},
        }

        trades = svc._plan_trades(
            target_weights=target_weights,
            current_positions=current_positions,
            price_map=price_map,
            nav_prev=1_000_000.0,
            max_daily_trades=2,
            max_turnover_hard=0.10,
            has_prev_day=True,
        )

        self.assertLessEqual(len(trades), 2)
        turnover = sum(float(x["amount"]) for x in trades) / (2 * 1_000_000.0)
        self.assertLessEqual(turnover, 0.10 + 1e-9)
        for t in trades:
            self.assertEqual(t["quantity"] % 100, 0)

    def test_build_review_payload_diary_style_fallback_non_trigger_has_no_persona(self):
        candidates_df = pd.DataFrame(
            [
                {"symbol": "600519.SH", "name": "贵州茅台", "industry": "白酒", "score": 92, "pct_chg": 1.2, "amount": 5.5e9},
                {"symbol": "300750.SZ", "name": "宁德时代", "industry": "新能源", "score": 88, "pct_chg": 0.4, "amount": 4.8e9},
                {"symbol": "601012.SH", "name": "隆基绿能", "industry": "新能源", "score": 85, "pct_chg": -0.3, "amount": 3.1e9},
            ]
        )
        executed_trades = [
            {"symbol": "600519.SH", "side": "buy", "quantity": 100.0, "price": 1678.0, "amount": 167800.0},
            {"symbol": "300750.SZ", "side": "sell", "quantity": 200.0, "price": 188.5, "amount": 37700.0},
        ]
        orders_audited = [
            {"symbol": "600519.SH", "action": "buy", "reason": "资金回流消费龙头，技术形态转强", "gate_status": "passed"},
            {"symbol": "300750.SZ", "action": "sell", "reason": "短线资金分歧扩大，先落袋并降波动", "gate_status": "adjusted"},
        ]
        out = svc._build_review_payload(
            trade_date="20260312",
            nav_prev=1_000_000.0,
            nav_now=1_005_000.0,
            executed_trades=executed_trades,
            orders_audited=orders_audited,
            risk_notes=["单票权重触发软上限，已降权"],
            ai_payload={"summary": "大盘震荡，资金偏好高景气+高现金流龙头", "risk_notes": "追高风险仍在"},
            candidates_df=candidates_df,
            final_positions={"600519.SH": {"quantity": 100.0}},
            config={"review_use_llm": 0, "model_name": "qwen3.5-plus"},
            tool_calls=[],
        )

        self.assertIn("复盘日记", out["summary_md"])
        for kw in ["宏观", "政策", "资金流", "板块", "技术面"]:
            self.assertIn(kw, out["summary_md"])
        for kw in ["单身", "运动", "游艇"]:
            self.assertNotIn(kw, out["summary_md"])
        self.assertIn("今天为什么买", out["buys_md"])
        self.assertIn("600519.SH", out["buys_md"])
        self.assertIn("今天为什么卖", out["sells_md"])
        self.assertIn("300750.SZ", out["sells_md"])
        self.assertIn("明天继续盯什么", out["risk_md"])
        self.assertIsInstance(out.get("next_watchlist"), list)

    def test_build_review_payload_persona_trigger_mentions_once(self):
        candidates_df = pd.DataFrame(
            [
                {"symbol": "600519.SH", "name": "贵州茅台", "industry": "白酒", "score": 92, "pct_chg": 1.2, "amount": 5.5e9},
            ]
        )
        out = svc._build_review_payload(
            trade_date="20260313",
            nav_prev=1_000_000.0,
            nav_now=980_000.0,
            executed_trades=[],
            orders_audited=[],
            risk_notes=[],
            ai_payload={"summary": "市场波动较大", "risk_notes": "短线噪音偏多"},
            candidates_df=candidates_df,
            final_positions={},
            config={"review_use_llm": 0, "model_name": "qwen3.5-plus"},
            tool_calls=[],
        )
        persona_hits = sum(out["summary_md"].count(k) for k in ["跑", "游艇", "运动", "单身"])
        self.assertGreaterEqual(persona_hits, 1)
        self.assertLessEqual(persona_hits, 4)
        self.assertNotIn("我还是那个单身交易员，喜欢运动，早上跑步、晚上复盘，心里一直惦记着那天能开着自己的游艇去环游世界", out["summary_md"])
        self.assertTrue(any(k in out["summary_md"] for k in ["交了点学费", "并不顺", "吃了点亏"]))

    def test_should_inject_persona_rules(self):
        flag1, reason1 = svc._should_inject_persona(
            trade_date="20260312",
            pnl_pct=0.013,
            executed_trades=[],
            risk_notes=[],
        )
        self.assertTrue(flag1)
        self.assertIn("盈亏", reason1)

        flag2, reason2 = svc._should_inject_persona(
            trade_date="20260312",
            pnl_pct=0.001,
            executed_trades=[],
            risk_notes=["触发止损，先收缩风险"],
        )
        self.assertTrue(flag2)
        self.assertIn("情绪事件", reason2)

        flag3, _ = svc._should_inject_persona(
            trade_date="20260310",
            pnl_pct=0.001,
            executed_trades=[],
            risk_notes=[],
        )
        self.assertFalse(flag3)

    def test_fallback_diary_varies_across_days(self):
        candidates_df = pd.DataFrame(
            [
                {"symbol": "600519.SH", "name": "贵州茅台", "industry": "白酒", "score": 92, "pct_chg": 1.2, "amount": 5.5e9},
                {"symbol": "300750.SZ", "name": "宁德时代", "industry": "新能源", "score": 88, "pct_chg": 0.4, "amount": 4.8e9},
            ]
        )
        out1 = svc._build_review_payload(
            trade_date="20260310",
            nav_prev=1_000_000.0,
            nav_now=999_000.0,
            executed_trades=[],
            orders_audited=[],
            risk_notes=[],
            ai_payload={"summary": "市场分化", "risk_notes": "控制回撤"},
            candidates_df=candidates_df,
            final_positions={},
            config={"review_use_llm": 0, "model_name": "qwen3.5-plus"},
            tool_calls=[],
        )
        out2 = svc._build_review_payload(
            trade_date="20260311",
            nav_prev=1_000_000.0,
            nav_now=999_000.0,
            executed_trades=[],
            orders_audited=[],
            risk_notes=[],
            ai_payload={"summary": "市场分化", "risk_notes": "控制回撤"},
            candidates_df=candidates_df,
            final_positions={},
            config={"review_use_llm": 0, "model_name": "qwen3.5-plus"},
            tool_calls=[],
        )
        out3 = svc._build_review_payload(
            trade_date="20260312",
            nav_prev=1_000_000.0,
            nav_now=999_000.0,
            executed_trades=[],
            orders_audited=[],
            risk_notes=[],
            ai_payload={"summary": "市场分化", "risk_notes": "控制回撤"},
            candidates_df=candidates_df,
            final_positions={},
            config={"review_use_llm": 0, "model_name": "qwen3.5-plus"},
            tool_calls=[],
        )
        s1, s2, s3 = out1["summary_md"], out2["summary_md"], out3["summary_md"]
        self.assertNotEqual(s1, s2)
        self.assertNotEqual(s2, s3)
        self.assertNotEqual(s1, s3)

    def test_format_recent_trade_memory_uses_latest_five_days(self):
        rows = []
        base_days = ["20260312", "20260311", "20260310", "20260307", "20260306", "20260305"]
        for i, td in enumerate(base_days):
            rows.append(
                {
                    "trade_date": td,
                    "symbol": f"6000{i}0.SH",
                    "side": "buy" if i % 2 == 0 else "sell",
                    "quantity": 1000.0,
                    "price": 10.0 + i,
                    "amount": 100000.0 + i * 1000.0,
                    "reason_short": "测试原因",
                }
            )
        df = pd.DataFrame(rows)
        summary = svc._format_recent_trade_memory(df, max_days=5)
        self.assertIn("2026-03-12", summary)
        self.assertIn("2026-03-06", summary)
        self.assertNotIn("2026-03-05", summary)
        self.assertIn("主要动作", summary)


if __name__ == "__main__":
    unittest.main()
