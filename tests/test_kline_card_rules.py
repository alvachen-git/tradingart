import unittest

import kline_card_rules as rules


def _bar(o, h, l, c, v=1000):
    return {"open": o, "high": h, "low": l, "close": c, "volume": v}


class TestKlineCardRulesV2(unittest.TestCase):
    def test_shortline_sequence_example_now_triggers_short_misfire(self):
        # 你的文档示例：
        # 日内短线多-新手 -> 日内短线空-熟练 -> 日内短线多-新手
        # 第二版规则：该未来窗口触发多头突破，且组合内没有突破追多牌，
        # 短线牌严重失误，短线总计直接覆盖为 -8。
        context = [_bar(100, 101, 99, 100) for _ in range(20)]
        future = [
            _bar(100, 101.2, 99.8, 101.0),   # up
            _bar(101.0, 101.2, 100.4, 100.6),  # down
            _bar(100.6, 101.4, 100.5, 101.1),  # up
            _bar(101.1, 101.3, 100.7, 100.9),
            _bar(100.9, 101.2, 100.6, 101.0),
        ]
        out = rules.resolve_turn_combo(
            card_ids=["short_long_novice", "short_short_skilled", "short_long_novice"],
            context_bars=context,
            future_bars=future,
            stage_no=1,
            run_effects={"momentum": 0},
            seed=17,
        )
        self.assertEqual(out["turn_score"], -8)
        self.assertTrue(out["mechanics"]["short_breakout_misfire_applied"])

    def test_breakout_uses_only_history_not_future_baseline(self):
        # 第二版突破看收盘价，不看 wick。这里让收盘也突破以命中。
        context = [_bar(100, 110, 95, 102) for _ in range(20)]
        future = [
            _bar(102, 111, 100, 111.2),
            _bar(103, 109, 101, 102),
            _bar(102, 108, 100, 101),
            _bar(101, 107, 99, 100),
            _bar(100, 106, 98, 99),
        ]
        out = rules.resolve_turn_combo(
            card_ids=["breakout_long_novice"],
            context_bars=context,
            future_bars=future,
            stage_no=1,
            run_effects={"momentum": 0},
            seed=7,
        )
        self.assertEqual(out["turn_score"], 20)
        self.assertTrue(out["card_results"][0]["hit"])

    def test_breakout_uses_recent_15_history_window(self):
        # 老数据前5根里有极端高点 150，但最近15根最高只有120；
        # 未来收盘 130 应按“最近15根”判定为命中。
        context = []
        for _ in range(5):
            context.append(_bar(100, 150, 90, 100))
        for _ in range(15):
            context.append(_bar(100, 120, 95, 100))
        future = [
            _bar(100, 130, 98, 130),
            _bar(101, 115, 99, 100),
            _bar(100, 114, 98, 99),
            _bar(99, 113, 97, 98),
            _bar(98, 112, 96, 97),
        ]
        out = rules.resolve_turn_combo(
            card_ids=["breakout_long_novice"],
            context_bars=context,
            future_bars=future,
            stage_no=1,
            run_effects={"momentum": 0},
            seed=19,
        )
        self.assertTrue(out["card_results"][0]["hit"])
        self.assertEqual(out["turn_score"], 20)

    def test_trend_direction_conflict_validation(self):
        v = rules.validate_combo_direction_conflict(["trend_long_novice", "trend_short_novice"])
        self.assertFalse(v["ok"])
        self.assertEqual(v["error_code"], "trend_direction_conflict")

    def test_trend_breakout_conflict_long_vs_short_blocked(self):
        v = rules.validate_combo_direction_conflict(["breakout_long_novice", "trend_short_novice"])
        self.assertFalse(v["ok"])
        self.assertEqual(v["error_code"], "trend_breakout_direction_conflict")

    def test_trend_breakout_conflict_short_vs_long_blocked(self):
        v = rules.validate_combo_direction_conflict(["breakout_short_novice", "trend_long_novice"])
        self.assertFalse(v["ok"])
        self.assertEqual(v["error_code"], "trend_breakout_direction_conflict")

    def test_breakout_long_short_conflict_blocked(self):
        v = rules.validate_combo_direction_conflict(["breakout_short_novice", "breakout_long_novice"])
        self.assertFalse(v["ok"])
        self.assertEqual(v["error_code"], "breakout_direction_conflict")

    def test_breakout_short_equal_history_low_is_miss(self):
        context = [_bar(100, 105, 95, 100) for _ in range(20)]
        future = [
            _bar(100, 103, 95, 101),
            _bar(101, 104, 95, 102),
            _bar(102, 103, 95, 101),
            _bar(101, 102, 95, 100),
            _bar(100, 101, 95, 99),
        ]
        out = rules.resolve_turn_combo(
            card_ids=["breakout_short_novice"],
            context_bars=context,
            future_bars=future,
            stage_no=1,
            run_effects={"momentum": 0},
            seed=23,
        )
        self.assertFalse(out["card_results"][0]["hit"])
        self.assertEqual(out["turn_score"], -20)
        self.assertEqual(out["card_results"][0]["history_low"], 95)
        self.assertEqual(out["card_results"][0]["future_low"], 95)

    def test_tactic_chain_order_is_deterministic(self):
        # 避免触发突破严重失误，抬高历史高低边界。
        context = [_bar(100, 200, 50, 100) for _ in range(20)]
        future = [
            _bar(100, 101, 99, 100.6),  # up
            _bar(100.6, 101, 100, 100.7),  # up
            _bar(100.7, 101.2, 100.5, 100.9),  # up
            _bar(100.9, 101.0, 100.8, 100.95),
            _bar(100.95, 101.1, 100.7, 101.0),
        ]
        out = rules.resolve_turn_combo(
            card_ids=[
                "short_long_novice",      # +1
                "short_short_novice",     # miss -2, 与前一张配对 +1 => short_total=0
                "tactic_leverage",        # running_total==0 => confidence -40
                "tactic_meditation",      # confidence +[5,15]
            ],
            context_bars=context,
            future_bars=future,
            stage_no=1,
            run_effects={"momentum": 0},
            seed=3,
        )
        self.assertEqual(out["turn_score"], 0)
        mechanics = out["mechanics"]
        self.assertEqual(mechanics["short_pair_bonus"], 1)
        self.assertLessEqual(mechanics["confidence_delta_from_cards"], -25)
        self.assertGreaterEqual(mechanics["confidence_delta_from_cards"], -35)

    def test_trend_novice_and_skilled_miss_lose_two_momentum(self):
        context = [_bar(100, 101, 99, 100) for _ in range(20)]
        # 明确下跌，确保 trend_long 失手；同时确保 trend_short 命中与否不影响该断言。
        future_down = [
            _bar(100, 101, 99, 99),
            _bar(99, 100, 98, 98.5),
            _bar(98.5, 99, 97.8, 98),
            _bar(98, 98.6, 97.5, 97.6),
            _bar(97.6, 98.0, 97.0, 97.2),
        ]
        out_novice = rules.resolve_turn_combo(
            card_ids=["trend_long_novice"],
            context_bars=context,
            future_bars=future_down,
            stage_no=1,
            run_effects={"momentum": 3},
            seed=21,
        )
        self.assertEqual(out_novice["mechanics"]["trend_loss"], 2)
        self.assertEqual(out_novice["mechanics"]["momentum_after"], 1)

        out_skilled = rules.resolve_turn_combo(
            card_ids=["trend_long_skilled"],
            context_bars=context,
            future_bars=future_down,
            stage_no=1,
            run_effects={"momentum": 4},
            seed=22,
        )
        self.assertEqual(out_skilled["mechanics"]["trend_loss"], 2)
        self.assertEqual(out_skilled["mechanics"]["momentum_after"], 2)

    def test_trend_card_result_contains_first_last_close(self):
        context = [_bar(100, 101, 99, 100) for _ in range(20)]
        future = [
            _bar(100, 101, 99, 100.8),
            _bar(100.8, 101, 100.2, 100.7),
            _bar(100.7, 101, 100.1, 100.6),
            _bar(100.6, 100.9, 100.0, 100.5),
            _bar(100.5, 100.8, 99.9, 100.4),
        ]
        out = rules.resolve_turn_combo(
            card_ids=["trend_short_novice"],
            context_bars=context,
            future_bars=future,
            stage_no=1,
            run_effects={"momentum": 0},
            seed=27,
        )
        first = out["card_results"][0].get("first_close")
        last = out["card_results"][0].get("last_close")
        delta = out["card_results"][0].get("trend_delta_pct")
        self.assertAlmostEqual(float(first), 100.8, places=6)
        self.assertAlmostEqual(float(last), 100.4, places=6)
        self.assertLess(float(delta), 0.0)

    def test_trend_momentum_gain_applies_only_once_max_card(self):
        context = [_bar(100, 101, 99, 100) for _ in range(20)]
        future_up = [
            _bar(100, 101, 99.8, 100.6),
            _bar(100.6, 101.2, 100.5, 100.9),
            _bar(100.9, 101.4, 100.8, 101.1),
            _bar(101.1, 101.5, 101.0, 101.3),
            _bar(101.3, 101.7, 101.2, 101.5),
        ]
        out = rules.resolve_turn_combo(
            card_ids=["trend_long_novice", "trend_long_veteran"],
            context_bars=context,
            future_bars=future_up,
            stage_no=1,
            run_effects={"momentum": 0},
            seed=31,
        )
        self.assertEqual(out["mechanics"]["trend_gain"], 2)
        self.assertEqual(out["mechanics"]["momentum_after"], 2)

    def test_short_streak_confidence_same_direction_only(self):
        context = [_bar(100, 101, 99, 100) for _ in range(20)]
        future = [
            _bar(100, 102, 99, 101),
            _bar(101, 102, 100, 101.5),
            _bar(101.5, 103, 101, 102),
            _bar(102, 103, 101, 102.5),
            _bar(102.5, 104, 102, 103),
        ]
        out = rules.resolve_turn_combo(
            card_ids=[
                "short_long_novice",
                "short_short_master",
                "short_long_novice",
                "short_short_novice",
            ],
            context_bars=context,
            future_bars=future,
            stage_no=1,
            run_effects={"momentum": 0},
            seed=10,
        )
        self.assertEqual(out["mechanics"]["short_streak_conf_bonus"], 0)

    def test_short_streak_confidence_same_direction_triggers(self):
        context = [_bar(100, 200, 50, 100) for _ in range(20)]
        future = [
            _bar(100, 102, 99, 101),
            _bar(101, 102, 100, 101.5),
            _bar(101.5, 103, 101, 102),
            _bar(102, 103, 101, 102.5),
            _bar(102.5, 104, 102, 103),
        ]
        out = rules.resolve_turn_combo(
            card_ids=[
                "short_short_novice",
                "short_short_master",
                "short_short_novice",
                "short_long_novice",
            ],
            context_bars=context,
            future_bars=future,
            stage_no=1,
            run_effects={"momentum": 0},
            seed=10,
        )
        self.assertEqual(out["mechanics"]["short_streak_conf_bonus"], 10)

    def test_short_hit_thresholds_by_tier(self):
        context = [_bar(100, 200, 50, 100) for _ in range(20)]
        future_two_up = [
            _bar(100, 101, 99, 101),   # up
            _bar(101, 102, 100, 100.5),  # down
            _bar(100.5, 101.5, 100, 101),  # up
            _bar(101, 101.2, 100.6, 100.8),  # down
            _bar(100.8, 101.1, 100.5, 100.7),  # down
        ]
        novice = rules.resolve_turn_combo(["short_long_novice"], context, future_two_up, 1, {"momentum": 0}, seed=1)
        skilled = rules.resolve_turn_combo(["short_long_skilled"], context, future_two_up, 1, {"momentum": 0}, seed=1)
        master = rules.resolve_turn_combo(["short_long_master"], context, future_two_up, 1, {"momentum": 0}, seed=1)
        self.assertEqual(novice["turn_score"], -2)
        self.assertEqual(skilled["turn_score"], 2)
        self.assertEqual(master["turn_score"], 4)
        self.assertEqual(skilled["card_results"][0]["hit_count_bars"], 2)
        self.assertEqual(skilled["card_results"][0]["hit_need_bars"], 2)

    def test_short_breakout_misfire_immune_with_matching_breakout_card(self):
        context = [_bar(100, 110, 90, 100) for _ in range(20)]
        future = [
            _bar(100, 112, 99, 111),  # bullish close breakout
            _bar(111, 112, 108, 109),
            _bar(109, 110, 106, 107),
            _bar(107, 108, 105, 106),
            _bar(106, 107, 104, 105),
        ]
        no_immune = rules.resolve_turn_combo(
            ["short_short_master"],
            context,
            future,
            1,
            {"momentum": 0},
            seed=1,
        )
        immune = rules.resolve_turn_combo(
            ["short_short_master", "breakout_long_novice"],
            context,
            future,
            1,
            {"momentum": 0},
            seed=1,
        )
        self.assertEqual(no_immune["turn_score"], -8)
        self.assertTrue(no_immune["mechanics"]["short_breakout_misfire_applied"])
        self.assertFalse(immune["mechanics"]["short_breakout_misfire_applied"])
        self.assertGreater(immune["turn_score"], no_immune["turn_score"])

    def test_breakout_uses_close_not_wick(self):
        context = [_bar(100, 110, 95, 100) for _ in range(20)]
        future = [
            _bar(100, 112, 99, 109),  # wick breaks, close does not
            _bar(109, 110, 107, 108),
            _bar(108, 109, 106, 107),
            _bar(107, 108, 105, 106),
            _bar(106, 107, 104, 105),
        ]
        out = rules.resolve_turn_combo(["breakout_long_novice"], context, future, 1, {"momentum": 0}, seed=2)
        self.assertFalse(out["card_results"][0]["hit"])
        self.assertEqual(out["turn_score"], -20)

    def test_breakout_momentum_bonus_values_updated(self):
        context = [_bar(100, 110, 90, 100) for _ in range(20)]
        future = [
            _bar(100, 112, 99, 111),
            _bar(111, 112, 109, 110),
            _bar(110, 111, 108, 109),
            _bar(109, 110, 107, 108),
            _bar(108, 109, 106, 107),
        ]
        novice = rules.resolve_turn_combo(["breakout_long_novice"], context, future, 1, {"momentum": 1}, seed=2)
        veteran = rules.resolve_turn_combo(["breakout_long_veteran"], context, future, 1, {"momentum": 1}, seed=2)
        self.assertEqual(novice["turn_score"], 30)
        self.assertEqual(veteran["turn_score"], 60)

    def test_arbitrage_chain_count_and_multiplier(self):
        context = [_bar(100, 200, 50, 100) for _ in range(20)]
        future = [_bar(100, 100.5, 99.8, 100.1) for _ in range(5)]  # low volatility success
        out = rules.resolve_turn_combo(
            ["arb_east_novice", "arb_west_novice", "arb_south_novice"],
            context,
            future,
            1,
            {"momentum": 0},
            seed=5,
        )
        # 东西南 => 链长3，成功倍率x2；每张成功+2，共12
        self.assertEqual(out["turn_score"], 12)
        segs = out["mechanics"]["arbitrage_segments"]
        self.assertEqual(len(segs), 1)
        self.assertEqual(segs[0]["chain_count"], 3)
        self.assertEqual(segs[0]["success_multiplier"], 2)
        self.assertEqual(out["card_results"][0]["arb_chain_count"], 3)

    def test_arbitrage_single_card_fails(self):
        context = [_bar(100, 200, 50, 100) for _ in range(20)]
        future = [_bar(100, 100.4, 99.7, 100.1) for _ in range(5)]
        out = rules.resolve_turn_combo(
            ["arb_north_novice"],
            context,
            future,
            1,
            {"momentum": 0},
            seed=5,
        )
        # 单牌链长1，不成对，按失败规则每张-2
        self.assertEqual(out["turn_score"], -2)

    def test_arbitrage_volatility_failure_and_severe_failure(self):
        context = [_bar(100, 200, 50, 100) for _ in range(20)]
        future_fail = [
            _bar(100, 104, 99, 103.6),  # 3.6%
            _bar(103.6, 104, 103, 103.7),
            _bar(103.7, 104, 103, 103.8),
            _bar(103.8, 104, 103, 103.9),
            _bar(103.9, 104, 103, 104.0),
        ]
        future_severe = [
            _bar(100, 107, 99, 106.2),  # 6.2%
            _bar(106.2, 107, 106, 106.3),
            _bar(106.3, 107, 106, 106.4),
            _bar(106.4, 107, 106, 106.5),
            _bar(106.5, 107, 106, 106.6),
        ]
        cards = ["arb_east_novice", "arb_west_novice"]
        fail = rules.resolve_turn_combo(cards, context, future_fail, 1, {"momentum": 0}, seed=5)
        severe = rules.resolve_turn_combo(cards, context, future_severe, 1, {"momentum": 0}, seed=5)
        self.assertEqual(fail["turn_score"], -4)
        self.assertEqual(severe["turn_score"], -8)
        self.assertTrue(fail["mechanics"]["arbitrage_volatility_gt_3pct"])
        self.assertTrue(severe["mechanics"]["arbitrage_volatility_gt_5pct"])

    def test_option_conflicts_validate(self):
        v_call = rules.validate_combo_direction_conflict(["option_buy_call_novice", "option_sell_call_novice"])
        v_put = rules.validate_combo_direction_conflict(["option_buy_put_novice", "option_sell_put_master"])
        v_arb = rules.validate_combo_direction_conflict(["arb_east_novice", "arb_east_veteran"])
        self.assertFalse(v_call["ok"])
        self.assertEqual(v_call["error_code"], "option_call_direction_conflict")
        self.assertFalse(v_put["ok"])
        self.assertEqual(v_put["error_code"], "option_put_direction_conflict")
        self.assertFalse(v_arb["ok"])
        self.assertEqual(v_arb["error_code"], "arbitrage_region_duplicate")

    def test_buy_option_rounding_and_crit(self):
        context = [_bar(100, 200, 50, 100) for _ in range(20)]
        future = [
            _bar(100, 101, 99, 101),       # up
            _bar(101, 102.5, 100.5, 102.2),  # up
            _bar(102.2, 106.0, 102.0, 106.0),  # up; vs prev close >3%
            _bar(106.0, 106.2, 104.0, 105.0),
            _bar(105.0, 105.1, 104.0, 104.5),
        ]
        out = rules.resolve_turn_combo(["option_buy_call_novice"], context, future, 1, {"momentum": 0}, seed=9)
        cr = out["card_results"][0]
        # 新规则：先扣5，再(6-2)*4=16，净值11，暴击后按当前实现净值*2 => 22
        self.assertTrue(cr["option_success"])
        self.assertTrue(cr["option_crit"])
        self.assertEqual(cr["option_metric_yz"], 6)
        self.assertEqual(int(cr.get("option_reward_units", -1)), 4)
        self.assertEqual(int(cr.get("option_reward_before_crit", -1)), 16)
        self.assertEqual(out["turn_score"], 22)

    def test_buy_option_crit_requires_consecutive_rising_closes_not_just_green_candles(self):
        context = [_bar(100, 200, 50, 100) for _ in range(20)]
        future = [
            _bar(100, 104.0, 99.0, 103.0),   # green, close 103
            _bar(102.0, 105.0, 101.0, 102.5),  # green, but close lower than prev close (103 -> 102.5)
            _bar(102.5, 107.0, 102.0, 106.0),  # green and > prev close by >3%
            _bar(106.0, 106.5, 104.0, 105.0),
            _bar(105.0, 105.5, 104.0, 104.5),
        ]
        out = rules.resolve_turn_combo(["option_buy_call_novice"], context, future, 1, {"momentum": 0}, seed=10)
        cr = out["card_results"][0]
        self.assertTrue(cr["option_success"])
        self.assertFalse(cr["option_crit"])
        # 新规则：Y=7, no crit => -5 + (7-2)*4 = 15
        self.assertEqual(cr["option_metric_yz"], 7)
        self.assertEqual(int(cr.get("option_reward_units", -1)), 5)
        self.assertEqual(out["turn_score"], 15)

    def test_buy_option_success_with_y_below_offset_only_pays_cost(self):
        context = [_bar(100, 200, 50, 100) for _ in range(20)]
        future = [
            _bar(100, 101.6, 99.5, 100.4),  # high > first open, so success; Y rounds to 2
            _bar(100.4, 100.8, 99.8, 100.2),
            _bar(100.2, 100.6, 99.9, 100.1),
            _bar(100.1, 100.5, 99.8, 100.0),
            _bar(100.0, 100.3, 99.7, 99.9),
        ]
        out = rules.resolve_turn_combo(["option_buy_call_novice"], context, future, 1, {"momentum": 0}, seed=11)
        cr = out["card_results"][0]
        self.assertTrue(cr["option_success"])
        self.assertEqual(cr["option_metric_yz"], 2)
        self.assertEqual(int(cr.get("option_reward_units", -1)), 0)
        self.assertEqual(out["turn_score"], -5)

    def test_fast_stop_blocks_breakout_and_protects_final_negative(self):
        context = [_bar(100, 200, 50, 100) for _ in range(20)]
        future = [
            _bar(100, 101, 99, 100.2),
            _bar(100.2, 100.5, 99.7, 100.1),
            _bar(100.1, 100.3, 99.8, 100.0),
            _bar(100.0, 100.2, 99.6, 99.9),
            _bar(99.9, 100.0, 99.5, 99.8),
        ]
        out = rules.resolve_turn_combo(
            [
                "tactic_fast_stop",
                "breakout_long_novice",   # fail -20, 不可保护但占位
                "short_long_novice",      # 2根以上上涨不足，失败 -2；可保护
                "tactic_leverage",        # running_total 负，不翻倍
            ],
            context,
            future,
            1,
            {"momentum": 0},
            seed=7,
        )
        by_order = {int(x["order"]): x for x in out["card_results"]}
        self.assertEqual(by_order[2]["fast_stop_blocked_reason"], "breakout")
        self.assertTrue(by_order[3]["fast_stop_protected"])
        self.assertEqual(by_order[3]["final_score"], 0)

    def test_self_confidence_requires_threshold_and_uses_running_total(self):
        context = [_bar(100, 200, 50, 100) for _ in range(20)]
        future = [_bar(100, 101, 99, 101) for _ in range(5)]  # 5 up
        low_conf = rules.resolve_turn_combo(
            ["short_long_master", "tactic_self_confidence"],
            context,
            future,
            1,
            {"momentum": 0, "confidence": 79},
            seed=3,
        )
        self.assertFalse(low_conf["ok"])
        ok = rules.resolve_turn_combo(
            ["short_long_master", "tactic_self_confidence"],
            context,
            future,
            1,
            {"momentum": 0, "confidence": 80},
            seed=3,
        )
        self.assertEqual(ok["turn_score"], 8)  # short大师命中+4，再*2
        checks = ok["mechanics"]["self_confidence_checks"]
        self.assertEqual(len(checks), 1)
        self.assertGreater(checks[0]["running_total_before"], 0)

    def test_dynamic_adjust_marks_once_per_turn(self):
        context = [_bar(100, 200, 50, 100) for _ in range(20)]
        future = [_bar(100, 100.5, 99.9, 100.1) for _ in range(5)]
        out = rules.resolve_turn_combo(
            ["tactic_dynamic_adjust", "tactic_dynamic_adjust"],
            context,
            future,
            1,
            {"momentum": 0, "confidence": 100},
            seed=3,
        )
        self.assertTrue(out["mechanics"]["dynamic_adjust_next_turn"])
        nodes = [n for n in out["mechanics"]["tactic_chain"] if n.get("effect") == "dynamic_adjust"]
        self.assertEqual(len(nodes), 2)
        self.assertTrue(nodes[0]["applied"])
        self.assertFalse(nodes[1]["applied"])


if __name__ == "__main__":
    unittest.main()
