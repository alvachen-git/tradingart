import unittest
from datetime import datetime, timedelta

from sqlalchemy import create_engine, text

import kline_card_data as card_data
import kline_card_map_storage as map_storage
import kline_card_rules as rules
import kline_card_storage as card_storage


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


class TestKlineCardMapStorage(unittest.TestCase):
    def setUp(self):
        self.orig_engine = card_storage.engine
        self.orig_candidates = card_data.get_stage_candidates
        self.orig_boss = card_data.get_boss_stage_candidate

        card_storage.engine = create_engine("sqlite:///:memory:", future=True)
        card_data.get_stage_candidates = lambda stage_no, count=3, seed=None: [
            {"symbol": "AAA", "symbol_name": "AAA", "symbol_type": "stock", "bars": _fake_bars()},
            {"symbol": "BBB", "symbol_name": "BBB", "symbol_type": "index", "bars": _fake_bars(start=200.0)},
            {"symbol": "CCC", "symbol_name": "CCC", "symbol_type": "future", "bars": _fake_bars(start=300.0)},
        ][:count]
        card_data.get_boss_stage_candidate = lambda stage_no, seed=None: {
            "symbol": "BOSS",
            "symbol_name": "Boss",
            "symbol_type": "future",
            "bars": _fake_bars(start=500.0),
        }
        card_storage.init_card_game_schema()
        map_storage.init_map_schema()

    def tearDown(self):
        card_storage.engine = self.orig_engine
        card_data.get_stage_candidates = self.orig_candidates
        card_data.get_boss_stage_candidate = self.orig_boss

    def test_create_move_and_resume_map_run(self):
        map_run_id = map_storage.create_map_run("map_user", seed=7)
        self.assertTrue(map_run_id > 0)

        state = map_storage.get_map_state(map_run_id)
        self.assertTrue(state["ok"])
        self.assertEqual(state["map_run"]["location"], "home")
        self.assertEqual(int(state["map_run"]["stamina"]), 100)

        moved = map_storage.move_location(map_run_id, "association")
        self.assertTrue(moved["ok"])
        self.assertEqual(moved["map_run"]["location"], "association")
        self.assertEqual(int(moved["map_run"]["stamina"]), 90)

        back_home = map_storage.move_location(map_run_id, "home")
        self.assertTrue(back_home["ok"])
        self.assertEqual(back_home["map_run"]["location"], "home")
        self.assertEqual(int(back_home["map_run"]["stamina"]), 90)

        resumed = map_storage.get_resume_map_run("map_user")
        self.assertIsNotNone(resumed)
        self.assertEqual(int(resumed["map_run_id"]), map_run_id)

    def test_rest_turn_advance_and_lock_at_turn_72(self):
        map_run_id = map_storage.create_map_run("map_turn_user", seed=11)
        with card_storage.engine.begin() as conn:
            conn.execute(
                text(
                    """
                    UPDATE kline_card_map_runs
                    SET turn_index = 71,
                        year_no = 3,
                        month_no = 12,
                        month_half = '上',
                        date_label = '2032年12月上',
                        location = 'home',
                        status = 'playing',
                        stamina = 40,
                        action_points = 15
                    WHERE map_run_id = :rid
                    """
                ),
                {"rid": map_run_id},
            )

        rested = map_storage.rest_and_advance_turn(map_run_id)
        self.assertTrue(rested["ok"])
        self.assertTrue(rested["locked"])
        self.assertEqual(rested["map_run"]["status"], "ended")
        self.assertEqual(int(rested["map_run"]["turn_index"]), 72)
        self.assertEqual(int(rested["map_run"]["stamina"]), 100)
        self.assertEqual(int(rested["map_run"]["action_points"]), 20)

        blocked = map_storage.move_location(map_run_id, "association")
        self.assertFalse(blocked["ok"])
        self.assertTrue(blocked.get("locked", False))

    def test_save_deck_and_commit_battle_result(self):
        map_run_id = map_storage.create_map_run("map_battle_user", seed=19)

        invalid = map_storage.save_home_deck(map_run_id, list(rules.CARD_LIBRARY.keys())[:9])
        self.assertFalse(invalid["ok"])

        selected_deck = list(rules.CARD_LIBRARY.keys())[:10]
        saved = map_storage.save_home_deck(map_run_id, selected_deck)
        self.assertTrue(saved["ok"])
        self.assertTrue(saved["map_run"]["deck_pending_apply"])

        started = map_storage.start_battle_from_map(map_run_id)
        self.assertTrue(started["ok"])
        battle_run_id = int(started["battle_run_id"])
        self.assertTrue(battle_run_id > 0)

        battle_state = card_storage.get_run_state(battle_run_id)
        self.assertTrue(battle_state["ok"])
        run = battle_state["run"]
        combined = list(run.get("hand", [])) + list(run.get("deck", [])) + list(run.get("discard", []))
        self.assertEqual(len(combined), len(selected_deck))
        self.assertTrue(all(cid in selected_deck for cid in combined))

        with card_storage.engine.begin() as conn:
            conn.execute(
                text(
                    """
                    UPDATE kline_card_runs
                    SET status = 'failed'
                    WHERE run_id = :rid
                    """
                ),
                {"rid": battle_run_id},
            )

        committed = map_storage.commit_battle_result(map_run_id, battle_run_id)
        self.assertTrue(committed["ok"])
        self.assertEqual(committed["battle_status"], "failed")
        delta = committed["resource_delta"]
        self.assertEqual(int(delta.get("money", 0)), -120)
        self.assertEqual(int(delta.get("stamina", 0)), -20)
        self.assertEqual(int(delta.get("fame", 0)), -4)

    def test_create_map_run_with_setup_and_restart_replaces_old_active(self):
        first_id = map_storage.create_map_run("setup_user", seed=21)
        self.assertTrue(first_id > 0)
        setup = {
            "player_name": "阿晨",
            "traits": ["外向", "谦虚", "喜欢规则", "看重自由"],
            "style_answers": {
                "horizon_preference": "short",
                "risk_preference": "seek_profit",
                "priority_preference": "mindset",
            },
            "god_mode": False,
        }
        second_id = map_storage.create_map_run("setup_user", seed=22, setup=setup, restart_existing=True)
        self.assertTrue(second_id > 0)
        self.assertNotEqual(first_id, second_id)

        first_state = map_storage.get_map_state(first_id)
        self.assertTrue(first_state["ok"])
        self.assertEqual(first_state["map_run"]["status"], "ended")
        self.assertEqual(first_state["map_run"]["ended_reason"], "restart_replaced")

        second_state = map_storage.get_map_state(second_id)
        self.assertTrue(second_state["ok"])
        run = second_state["map_run"]
        self.assertEqual(run["player_name"], "阿晨")
        self.assertEqual(int(run["money"]), 100000)
        self.assertEqual(int(run["management_aum"]), 2000000)
        self.assertEqual(int(run["action_points"]), 10)
        self.assertEqual(int(run["stress"]), 0)
        self.assertEqual(int(run["confidence"]), 60)
        self.assertEqual(int(run["fame"]), 0)
        self.assertEqual(int(run["exp"]), 0)
        self.assertEqual(list(run.get("traits", [])), setup["traits"])
        self.assertEqual(dict(run.get("style_answers", {})).get("horizon_preference"), "short")
        self.assertEqual(len(list(run.get("home_deck", []))), 15)
        deck_counts = {}
        for cid in list(run.get("home_deck", [])):
            deck_counts[cid] = int(deck_counts.get(cid, 0)) + 1
        self.assertEqual(deck_counts.get("short_short_novice", 0), 3)
        self.assertEqual(deck_counts.get("short_long_novice", 0), 3)
        self.assertEqual(deck_counts.get("trend_short_novice", 0), 3)
        self.assertEqual(deck_counts.get("trend_long_novice", 0), 3)
        self.assertEqual(deck_counts.get("tactic_meditation", 0), 3)
        self.assertEqual(deck_counts.get("tactic_quick_cancel", 0), 0)
        self.assertEqual(deck_counts.get("tactic_risk_control", 0), 0)

    def test_commit_battle_result_ends_map_when_confidence_zero(self):
        map_run_id = map_storage.create_map_run("conf_zero_user", seed=29)
        started = map_storage.start_battle_from_map(map_run_id)
        self.assertTrue(started["ok"])
        battle_run_id = int(started["battle_run_id"])
        with card_storage.engine.begin() as conn:
            conn.execute(
                text(
                    """
                    UPDATE kline_card_runs
                    SET status = 'failed',
                        confidence = 0
                    WHERE run_id = :rid
                    """
                ),
                {"rid": battle_run_id},
            )
        committed = map_storage.commit_battle_result(map_run_id, battle_run_id)
        self.assertTrue(committed["ok"])
        self.assertTrue(committed.get("locked", False))
        self.assertEqual(committed["map_run"]["status"], "ended")
        self.assertEqual(committed["map_run"]["ended_reason"], "confidence_zero")
        self.assertEqual(int(committed["map_run"]["confidence"]), 0)

    def test_create_map_run_with_god_mode_starts_with_full_card_library(self):
        setup = {
            "player_name": "管理员",
            "traits": ["内向", "谦虚", "喜欢规则", "看重自由"],
            "style_answers": {
                "horizon_preference": "short",
                "risk_preference": "seek_profit",
                "priority_preference": "skill",
            },
            "god_mode": True,
        }
        map_run_id = map_storage.create_map_run("god_user", seed=31, setup=setup, restart_existing=True)
        state = map_storage.get_map_state(map_run_id)
        self.assertTrue(state["ok"])
        run = state["map_run"]
        home_deck = list(run.get("home_deck", []))
        self.assertTrue(bool(run.get("god_mode", False)))
        self.assertEqual(len(home_deck), len(rules.CARD_LIBRARY))
        self.assertEqual(set(home_deck), set(rules.CARD_LIBRARY.keys()))

if __name__ == "__main__":
    unittest.main()
