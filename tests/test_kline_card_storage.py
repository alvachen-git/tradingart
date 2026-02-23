import unittest
from datetime import datetime, timedelta

from sqlalchemy import create_engine, text

import kline_card_storage as storage


def _fake_bars(count=120, start=100.0):
    bars = []
    d0 = datetime(2024, 1, 1)
    for i in range(count):
        o = start + i * 0.2
        c = o + 0.5
        bars.append(
            {
                "open": o,
                "high": c + 0.2,
                "low": o - 0.2,
                "close": c,
                "volume": 1000 + i,
                "date": (d0 + timedelta(days=i)).strftime("%Y-%m-%d"),
            }
        )
    return bars


class TestKlineCardStorage(unittest.TestCase):
    def setUp(self):
        self.orig_engine = storage.engine
        self.orig_candidates = storage.card_data.get_stage_candidates
        self.orig_boss = storage.card_data.get_boss_stage_candidate

        storage.engine = create_engine("sqlite:///:memory:", future=True)
        storage.card_data.get_stage_candidates = lambda stage_no, count=3, seed=None: [
            {"symbol": "AAA", "symbol_name": "AAA", "symbol_type": "stock", "bars": _fake_bars()},
            {"symbol": "BBB", "symbol_name": "BBB", "symbol_type": "index", "bars": _fake_bars(start=200.0)},
            {"symbol": "CCC", "symbol_name": "CCC", "symbol_type": "future", "bars": _fake_bars(start=300.0)},
        ][:count]
        storage.card_data.get_boss_stage_candidate = lambda stage_no, seed=None: {
            "symbol": "BOSS",
            "symbol_name": "Boss",
            "symbol_type": "future",
            "bars": _fake_bars(start=500.0),
        }
        storage.init_card_game_schema()

    def tearDown(self):
        storage.engine = self.orig_engine
        storage.card_data.get_stage_candidates = self.orig_candidates
        storage.card_data.get_boss_stage_candidate = self.orig_boss

    def test_create_run_and_resume_flow(self):
        run_id = storage.create_run("tester", seed=7)
        self.assertTrue(run_id > 0)

        choose = storage.start_stage(run_id, 1, None)
        self.assertTrue(choose["ok"])
        self.assertTrue(choose.get("need_choice"))
        self.assertEqual(len(choose["candidates"]), 3)

        started = storage.start_stage(run_id, 1, "AAA")
        self.assertTrue(started["ok"])
        self.assertEqual(started["stage_no"], 1)
        self.assertEqual(len(started["visible_bars"]), 20)

        played = storage.play_turn(run_id, {"type": "pass"})
        self.assertTrue(played["ok"])
        self.assertEqual(played["turn_no"], 1)

        resumed = storage.get_resume_run("tester")
        self.assertIsNotNone(resumed)
        self.assertEqual(resumed["run_id"], run_id)
        self.assertEqual(resumed["current_stage"], 1)
        self.assertIsNotNone(resumed["stage_state"])
        self.assertEqual(resumed["run_effects"]["rules_version"], storage.rules.RULE_VERSION)

    def test_finish_run_grants_independent_meta_exp(self):
        run_id = storage.create_run("meta_user", seed=11)
        with storage.engine.begin() as conn:
            conn.execute(
                text(
                    """
                    UPDATE kline_card_runs
                    SET status='failed', total_score=360, cleared_stages=2
                    WHERE run_id=:rid
                    """
                ),
                {"rid": run_id},
            )

        done = storage.finish_run(run_id)
        self.assertTrue(done["ok"])
        self.assertEqual(done["status"], "failed")
        self.assertGreater(done["reward_exp"], 0)
        meta = done["meta"]
        self.assertTrue(meta["ok"])
        self.assertGreaterEqual(meta["exp"], done["reward_exp"])

    def test_combo_play_turn_consumes_multiple_cards(self):
        run_id = storage.create_run("combo_user", seed=13)
        choose = storage.start_stage(run_id, 1, None)
        self.assertTrue(choose["ok"])
        started = storage.start_stage(run_id, 1, "AAA")
        self.assertTrue(started["ok"])

        state = storage.get_run_state(run_id)
        hand = list(state["run"]["hand"])
        if len(hand) < 2:
            self.skipTest("initial hand less than 2 cards")
        combo_cards = None
        for i in range(len(hand)):
            for j in range(i + 1, len(hand)):
                cand = [hand[i], hand[j]]
                if storage.rules.validate_combo_direction_conflict(cand).get("ok", False):
                    combo_cards = cand
                    break
            if combo_cards:
                break
        if not combo_cards:
            self.skipTest("no valid 2-card combo found in initial hand")

        out = storage.play_turn(run_id, {"type": "combo", "cards": combo_cards})
        self.assertTrue(out["ok"])
        self.assertEqual(out["action_type"], "combo")
        self.assertEqual(len(out["played_cards"]), 2)
        self.assertEqual(len(out.get("drawn_cards", [])), 2)
        self.assertEqual(len(out["hand"]), len(hand))
        self.assertIn("mechanics", out)
        self.assertIn("trend_gain", out["mechanics"])
        self.assertIn("trend_loss", out["mechanics"])
        self.assertIn("momentum_delta", out["mechanics"])

    def test_trend_direction_conflict_blocked(self):
        run_id = storage.create_run("conflict_user", seed=15)
        storage.start_stage(run_id, 1, None)
        storage.start_stage(run_id, 1, "AAA")
        with storage.engine.begin() as conn:
            conn.execute(
                text(
                    """
                    UPDATE kline_card_runs
                    SET hand_json = :hand
                    WHERE run_id = :rid
                    """
                ),
                {
                    "rid": run_id,
                    "hand": storage._json_dump(
                        [
                            "trend_long_novice",
                            "trend_short_novice",
                            "short_long_novice",
                        ]
                    ),
                },
            )
        out = storage.play_turn(
            run_id,
            {"type": "combo", "cards": ["trend_long_novice", "trend_short_novice"]},
        )
        self.assertFalse(out["ok"])
        self.assertIn("方向冲突", out["message"])

    def test_trend_breakout_direction_conflict_blocked(self):
        run_id = storage.create_run("trend_breakout_conflict_user", seed=31)
        storage.start_stage(run_id, 1, None)
        storage.start_stage(run_id, 1, "AAA")
        with storage.engine.begin() as conn:
            conn.execute(
                text(
                    """
                    UPDATE kline_card_runs
                    SET hand_json = :hand
                    WHERE run_id = :rid
                    """
                ),
                {
                    "rid": run_id,
                    "hand": storage._json_dump(
                        [
                            "breakout_long_novice",
                            "trend_short_novice",
                            "short_long_novice",
                        ]
                    ),
                },
            )
        out = storage.play_turn(
            run_id,
            {"type": "combo", "cards": ["breakout_long_novice", "trend_short_novice"]},
        )
        self.assertFalse(out["ok"])
        self.assertIn("方向冲突", out["message"])

    def test_breakout_long_short_conflict_blocked(self):
        run_id = storage.create_run("breakout_conflict_user", seed=35)
        storage.start_stage(run_id, 1, None)
        storage.start_stage(run_id, 1, "AAA")
        with storage.engine.begin() as conn:
            conn.execute(
                text(
                    """
                    UPDATE kline_card_runs
                    SET hand_json = :hand
                    WHERE run_id = :rid
                    """
                ),
                {
                    "rid": run_id,
                    "hand": storage._json_dump(
                        [
                            "breakout_long_novice",
                            "breakout_short_novice",
                            "short_long_novice",
                        ]
                    ),
                },
            )
        out = storage.play_turn(
            run_id,
            {"type": "combo", "cards": ["breakout_long_novice", "breakout_short_novice"]},
        )
        self.assertFalse(out["ok"])
        self.assertIn("方向冲突", out["message"])

    def test_turn_draw_two_and_discard_when_hand_over_limit(self):
        run_id = storage.create_run("discard_user", seed=17)
        choose = storage.start_stage(run_id, 1, None)
        self.assertTrue(choose["ok"])
        started = storage.start_stage(run_id, 1, "AAA")
        self.assertTrue(started["ok"])

        with storage.engine.begin() as conn:
            conn.execute(
                text(
                    """
                    UPDATE kline_card_runs
                    SET hand_limit = 10,
                        hand_json = :hand,
                        run_effects_json = :effects
                    WHERE run_id = :rid
                    """
                ),
                {
                    "rid": run_id,
                    "hand": storage._json_dump(
                        [
                            "trend_long_novice",
                            "trend_long_novice",
                            "trend_short_novice",
                            "trend_short_novice",
                            "breakout_long_novice",
                            "breakout_short_novice",
                            "short_long_novice",
                            "short_short_novice",
                            "tactic_quick_cancel",
                            "tactic_meditation",
                        ]
                    ),
                    "effects": storage._json_dump({"rules_version": storage.rules.RULE_VERSION}),
                },
            )

        played = storage.play_turn(run_id, {"type": "pass"})
        self.assertTrue(played["ok"])
        self.assertTrue(played["need_discard"])
        self.assertEqual(played["pending_discard"], 2)
        self.assertEqual(len(played["hand"]), 12)

        blocked = storage.play_turn(run_id, {"type": "pass"})
        self.assertFalse(blocked["ok"])
        self.assertTrue(blocked["need_discard"])

        hand_after = list(played["hand"])
        d1 = storage.play_turn(run_id, {"type": "discard", "cards": [hand_after[0]]})
        self.assertTrue(d1["ok"])
        self.assertEqual(d1["action_type"], "discard")
        self.assertTrue(d1["need_discard"])
        self.assertEqual(d1["pending_discard"], 1)

        d2 = storage.play_turn(run_id, {"type": "discard", "cards": [hand_after[1]]})
        self.assertTrue(d2["ok"])
        self.assertEqual(d2["pending_discard"], 0)
        self.assertFalse(d2["need_discard"])

    def test_quick_cancel_extra_draw_only_applies_next_turn_once(self):
        run_id = storage.create_run("quick_cancel_user", seed=23)
        choose = storage.start_stage(run_id, 1, None)
        self.assertTrue(choose["ok"])
        started = storage.start_stage(run_id, 1, "AAA")
        self.assertTrue(started["ok"])

        with storage.engine.begin() as conn:
            conn.execute(
                text(
                    """
                    UPDATE kline_card_runs
                    SET hand_limit = 20,
                        hand_json = :hand,
                        deck_json = :deck,
                        run_effects_json = :effects
                    WHERE run_id = :rid
                    """
                ),
                {
                    "rid": run_id,
                    "hand": storage._json_dump(
                        [
                            "tactic_quick_cancel",
                            "short_long_novice",
                            "short_short_novice",
                        ]
                    ),
                    "deck": storage._json_dump(
                        [
                            "short_long_novice",
                            "short_short_novice",
                            "trend_long_novice",
                            "trend_short_novice",
                            "breakout_long_novice",
                            "breakout_short_novice",
                            "tactic_meditation",
                            "short_long_novice",
                            "short_short_novice",
                            "trend_long_novice",
                            "trend_short_novice",
                            "short_long_novice",
                        ]
                    ),
                    "effects": storage._json_dump(
                        {
                            "rules_version": storage.rules.RULE_VERSION,
                            "extra_draw_next_turn": 0,
                            "extra_draw_pending_turn": 0,
                        }
                    ),
                },
            )

        t1 = storage.play_turn(run_id, {"type": "combo", "cards": ["tactic_quick_cancel"]})
        self.assertTrue(t1["ok"])
        self.assertEqual(len(t1.get("drawn_cards", [])), 2)
        self.assertEqual(int(t1["mechanics"].get("extra_draw_next_turn_gain", 0)), 1)

        t2 = storage.play_turn(run_id, {"type": "pass"})
        self.assertTrue(t2["ok"])
        self.assertEqual(int(t2["mechanics"].get("extra_draw_applied_this_turn", 0)), 1)
        self.assertEqual(len(t2.get("drawn_cards", [])), 3)

        t3 = storage.play_turn(run_id, {"type": "pass"})
        self.assertTrue(t3["ok"])
        self.assertEqual(int(t3["mechanics"].get("extra_draw_applied_this_turn", 0)), 0)
        self.assertEqual(len(t3.get("drawn_cards", [])), 2)

    def test_negative_total_penalty_checked_after_settlement(self):
        run_id = storage.create_run("negative_total_rule_user", seed=29)
        choose = storage.start_stage(run_id, 1, None)
        self.assertTrue(choose["ok"])
        started = storage.start_stage(run_id, 1, "AAA")
        self.assertTrue(started["ok"])

        with storage.engine.begin() as conn:
            conn.execute(
                text(
                    """
                    UPDATE kline_card_runs
                    SET total_score = -8,
                        confidence = 80,
                        hand_json = :hand
                    WHERE run_id = :rid
                    """
                ),
                {
                    "rid": run_id,
                    "hand": storage._json_dump(
                        [
                            "breakout_long_novice",
                            "short_long_novice",
                        ]
                    ),
                },
            )

        out = storage.play_turn(run_id, {"type": "combo", "cards": ["breakout_long_novice"]})
        self.assertTrue(out["ok"])
        self.assertGreater(int(out.get("turn_score", 0)), 0)
        self.assertGreaterEqual(int(out.get("total_score", -9999)), 0)
        self.assertEqual(int(out.get("confidence_delta", -999)), 0)
        events = out.get("mechanics", {}).get("confidence_events", [])
        event_codes = [str(e.get("code", "")) for e in events]
        self.assertNotIn("total_score_negative_end", event_codes)

    def test_option_conflict_blocked_in_storage(self):
        run_id = storage.create_run("option_conflict_user", seed=52)
        storage.start_stage(run_id, 1, None)
        storage.start_stage(run_id, 1, "AAA")
        with storage.engine.begin() as conn:
            conn.execute(
                text(
                    """
                    UPDATE kline_card_runs
                    SET hand_json = :hand
                    WHERE run_id = :rid
                    """
                ),
                {
                    "rid": run_id,
                    "hand": storage._json_dump(
                        [
                            "option_buy_call_novice",
                            "option_sell_call_novice",
                            "short_long_novice",
                        ]
                    ),
                },
            )
        out = storage.play_turn(run_id, {"type": "combo", "cards": ["option_buy_call_novice", "option_sell_call_novice"]})
        self.assertFalse(out["ok"])
        self.assertIn("期权冲突", out["message"])

    def test_dynamic_adjust_discards_remaining_hand_and_redraws_before_action(self):
        run_id = storage.create_run("dynamic_adjust_user", seed=61)
        storage.start_stage(run_id, 1, None)
        storage.start_stage(run_id, 1, "AAA")
        with storage.engine.begin() as conn:
            conn.execute(
                text(
                    """
                    UPDATE kline_card_runs
                    SET hand_limit = 20,
                        hand_json = :hand,
                        deck_json = :deck,
                        discard_json = :discard,
                        run_effects_json = :effects
                    WHERE run_id = :rid
                    """
                ),
                {
                    "rid": run_id,
                    "hand": storage._json_dump(
                        [
                            "short_long_novice",
                            "short_short_novice",
                            "tactic_meditation",
                        ]
                    ),
                    "deck": storage._json_dump(
                        [
                            "trend_long_novice",
                            "trend_short_novice",
                            "breakout_long_novice",
                            "breakout_short_novice",
                            "tactic_quick_cancel",
                            "tactic_dynamic_adjust",
                            "tactic_self_confidence",
                            "option_buy_call_novice",
                            "arb_east_novice",
                            "arb_west_novice",
                        ]
                    ),
                    "discard": storage._json_dump([]),
                    "effects": storage._json_dump(
                        {
                            "rules_version": storage.rules.RULE_VERSION,
                            "extra_draw_next_turn": 1,
                            "extra_draw_pending_turn": 1,
                            "dynamic_adjust_pending_turn": 1,
                            "dynamic_adjust_pending_once": 1,
                            "momentum": 0,
                            "score_streak": 0,
                        }
                    ),
                },
            )
        out = storage.play_turn(run_id, {"type": "pass"})
        self.assertTrue(out["ok"])
        self.assertEqual(int(out["mechanics"].get("dynamic_adjust_applied_this_turn", 0)), 1)
        self.assertEqual(int(out["mechanics"].get("extra_draw_applied_this_turn", 0)), 1)
        self.assertEqual(int(out["mechanics"].get("dynamic_adjust_discarded_count", 0)), 3)
        # 动态调整前置：先重抽 3+2+1，再执行本回合 pass 结束后再抽 2，至少应比默认 pass 手牌更多。
        self.assertGreaterEqual(len(out["hand"]), 5)

    def test_abort_run_marks_failed_and_is_idempotent(self):
        run_id = storage.create_run("abort_user", seed=41)
        choose = storage.start_stage(run_id, 1, None)
        self.assertTrue(choose["ok"])
        started = storage.start_stage(run_id, 1, "AAA")
        self.assertTrue(started["ok"])

        aborted = storage.abort_run(run_id, reason="test_abort")
        self.assertTrue(aborted["ok"])
        self.assertTrue(aborted["aborted"])
        self.assertEqual(aborted["status"], "failed")

        state = storage.get_run_state(run_id)
        self.assertTrue(state["ok"])
        self.assertEqual(str(state["run"].get("status")), "failed")

        again = storage.abort_run(run_id, reason="test_abort_again")
        self.assertTrue(again["ok"])
        self.assertTrue(again.get("already_final", False))


if __name__ == "__main__":
    unittest.main()
