import unittest

from sqlalchemy import create_engine, text

import user_profile_memory as upm


class TestUserProfileMemory(unittest.TestCase):
    def setUp(self):
        self.engine = create_engine("sqlite:///:memory:", future=True)
        upm.ensure_profile_memory_table(self.engine)

    def _active_value(self, key: str) -> str:
        memories = upm.get_active_profile_memories(self.engine, "u1")
        by_key = {m["memory_key"]: m["memory_value"] for m in memories}
        return by_key.get(key, "")

    def _active_confidence(self, key: str) -> float:
        memories = upm.get_active_profile_memories(self.engine, "u1")
        by_key = {m["memory_key"]: m["confidence"] for m in memories}
        return float(by_key.get(key, 0.0) or 0.0)

    def test_direct_update_supersedes_old_memory(self):
        upm.upsert_profile_memory(
            self.engine,
            user_id="u1",
            memory_key=upm.KEY_RISK_PREFERENCE,
            memory_value="偏保守",
            source_text="旧画像",
        )

        out = upm.build_profile_memory_context(
            self.engine,
            user_id="u1",
            prompt_text="把我的风险偏好改成偏激进",
        )

        self.assertTrue(out["should_short_circuit"])
        self.assertEqual(out["memory_action"], "updated")
        self.assertIn("偏激进", out["confirmation"])
        self.assertIn("策略、仓位和风险提示", out["confirmation"])
        self.assertEqual(self._active_value(upm.KEY_RISK_PREFERENCE), "偏激进")
        with self.engine.begin() as conn:
            old_rows = conn.execute(
                text(
                    "SELECT COUNT(*) FROM user_profile_memory "
                    "WHERE user_id='u1' AND memory_key='risk_preference' AND status='superseded'"
                )
            ).scalar()
        self.assertEqual(old_rows, 1)

    def test_single_conflict_uses_temporary_override_without_updating_active(self):
        upm.upsert_profile_memory(
            self.engine,
            user_id="u1",
            memory_key=upm.KEY_RISK_PREFERENCE,
            memory_value="偏保守",
            source_text="我比较保守",
        )

        out = upm.build_profile_memory_context(
            self.engine,
            user_id="u1",
            prompt_text="这次我想激进一点",
        )

        self.assertFalse(out["should_short_circuit"])
        self.assertIn("本轮当前表达优先", out["profile_context"])
        self.assertIn("偏激进", out["profile_context"])
        self.assertEqual(self._active_value(upm.KEY_RISK_PREFERENCE), "偏保守")

    def test_repeated_conflict_upgrades_long_term_memory(self):
        upm.upsert_profile_memory(
            self.engine,
            user_id="u1",
            memory_key=upm.KEY_RISK_PREFERENCE,
            memory_value="偏保守",
            source_text="我比较保守",
        )

        upm.build_profile_memory_context(
            self.engine,
            user_id="u1",
            prompt_text="我现在能接受高波动",
        )
        out = upm.build_profile_memory_context(
            self.engine,
            user_id="u1",
            prompt_text="我现在能接受高波动",
        )

        self.assertEqual(self._active_value(upm.KEY_RISK_PREFERENCE), "偏激进")
        self.assertIn("重复表达更新", out["profile_context"])

    def test_temporary_only_does_not_write_long_term_memory(self):
        out = upm.build_profile_memory_context(
            self.engine,
            user_id="u1",
            prompt_text="这只是今天试一下，不用记，我想激进一点",
        )

        self.assertEqual(out["memory_action"], "temporary_only")
        self.assertIn("不写入长期画像", out["confirmation"])
        self.assertEqual(self._active_value(upm.KEY_RISK_PREFERENCE), "")
        self.assertIn("偏激进", out["profile_context"])

    def test_stable_profile_statement_can_auto_create_memory(self):
        out = upm.build_profile_memory_context(
            self.engine,
            user_id="u1",
            prompt_text="我比较保守，期权策略不喜欢太激进",
        )

        self.assertFalse(out["should_short_circuit"])
        self.assertEqual(self._active_value(upm.KEY_RISK_PREFERENCE), "偏保守")

    def test_answer_style_update_and_safety_downgrade(self):
        out = upm.build_profile_memory_context(
            self.engine,
            user_id="u1",
            prompt_text="以后别提示风险",
        )

        self.assertTrue(out["should_short_circuit"])
        self.assertEqual(
            self._active_value(upm.KEY_ANSWER_STYLE),
            "风险提示保持简洁，但必要风险边界仍需保留",
        )
        self.assertIn("必要风险边界", out["profile_context"])

    def test_preferred_products_update(self):
        out = upm.build_profile_memory_context(
            self.engine,
            user_id="u1",
            prompt_text="记住我主要做 ETF期权",
        )

        self.assertTrue(out["should_short_circuit"])
        self.assertEqual(self._active_value(upm.KEY_PREFERRED_PRODUCTS), "ETF期权")
        self.assertIn("优先按ETF期权的口径", out["confirmation"])

    def test_stable_fact_keys_update_existing_profile_table(self):
        out = upm.build_profile_memory_context(
            self.engine,
            user_id="u1",
            prompt_text=(
                "记住交易约束：资金量未知时，不要给金额级调仓建议，只给方向、条件和补数清单。"
                "客户担忧是回撤过大和数据不完整。"
            ),
        )

        self.assertTrue(out["should_short_circuit"])
        self.assertEqual(self._active_value(upm.KEY_TRADING_CONSTRAINT), "资金量未知时，不要给金额级调仓建议，只给方向、条件和补数清单")
        self.assertEqual(self._active_value(upm.KEY_CLIENT_CONCERN), "回撤过大和数据不完整")
        self.assertIn("【稳定事实】", out["profile_context"])
        self.assertIn("交易约束", out["profile_context"])
        self.assertIn("客户担忧", out["profile_context"])

    def test_account_capital_update_query_and_replace(self):
        out = upm.build_profile_memory_context(
            self.engine,
            user_id="u1",
            prompt_text="我现在资金量是200万，要记住",
        )

        self.assertTrue(out["should_short_circuit"])
        self.assertEqual(self._active_value(upm.KEY_ACCOUNT_CAPITAL), "200万")
        self.assertIn("账户资金", out["profile_context"])

        query = upm.build_profile_memory_context(
            self.engine,
            user_id="u1",
            prompt_text="还记得我有多少资金吗",
        )

        self.assertTrue(query["should_short_circuit"])
        self.assertEqual(query["memory_action"], "query")
        self.assertIn("200万", query["confirmation"])

        upm.build_profile_memory_context(
            self.engine,
            user_id="u1",
            prompt_text="把我的资金量改成150万",
        )

        self.assertEqual(self._active_value(upm.KEY_ACCOUNT_CAPITAL), "150万")

    def test_account_capital_colloquial_phrases_persist_and_query(self):
        cases = [
            ("记住我有资金200万", "200万"),
            ("记住我有200万资金", "200万"),
            ("记住我的资金300万", "300万"),
            ("记住我现在有350万", "350万"),
            ("记住我有现金200万", "200万"),
            ("记住我有200万现金", "200万"),
            ("记住我的现金300万", "300万"),
            ("记住我可用现金是150万", "150万"),
        ]
        for prompt, expected in cases:
            with self.subTest(prompt=prompt):
                engine = create_engine("sqlite:///:memory:", future=True)
                upm.ensure_profile_memory_table(engine)
                out = upm.build_profile_memory_context(
                    engine,
                    user_id="u1",
                    prompt_text=prompt,
                )

                self.assertTrue(out["should_short_circuit"])
                memories = upm.get_active_profile_memories(engine, "u1")
                by_key = {m["memory_key"]: m["memory_value"] for m in memories}
                self.assertEqual(by_key.get(upm.KEY_ACCOUNT_CAPITAL), expected)

                query = upm.build_profile_memory_context(
                    engine,
                    user_id="u1",
                    prompt_text="你知道我有多少资金吗",
                )
                self.assertEqual(query["memory_action"], "query")
                self.assertIn(expected, query["confirmation"])

                cash_query = upm.build_profile_memory_context(
                    engine,
                    user_id="u1",
                    prompt_text="你知道我有多少现金吗",
                )
                self.assertEqual(cash_query["memory_action"], "query")
                self.assertIn(expected, cash_query["confirmation"])

    def test_natural_question_updates_current_capital_and_goal(self):
        upm.upsert_profile_memory(
            self.engine,
            user_id="u1",
            memory_key=upm.KEY_ACCOUNT_CAPITAL,
            memory_value="300万",
            source_text="历史资金画像",
        )

        out = upm.build_profile_memory_context(
            self.engine,
            user_id="u1",
            prompt_text="我有资金500万，目标是靠买期权赚到可以在上海买房",
        )

        self.assertFalse(out["should_short_circuit"])
        self.assertEqual(out["memory_action"], "context")
        self.assertEqual(self._active_value(upm.KEY_ACCOUNT_CAPITAL), "500万")
        self.assertEqual(self._active_value(upm.KEY_INVESTMENT_GOAL), "靠买期权赚到可以在上海买房")
        self.assertIn("账户资金", out["profile_context"])
        self.assertIn("目标/用途", out["profile_context"])

        query = upm.build_profile_memory_context(
            self.engine,
            user_id="u1",
            prompt_text="你记得我的投资目标吗",
        )

        self.assertTrue(query["should_short_circuit"])
        self.assertEqual(query["memory_action"], "query")
        self.assertIn("靠买期权赚到可以在上海买房", query["confirmation"])

    def test_decision_style_and_investment_goal_update_profile(self):
        out = upm.build_profile_memory_context(
            self.engine,
            user_id="u1",
            prompt_text="记住我的决策风格是先给结论再看细节；投资目标是稳健增值和现金增强。",
        )

        self.assertTrue(out["should_short_circuit"])
        self.assertEqual(self._active_value(upm.KEY_DECISION_STYLE), "先给结论再看细节")
        self.assertEqual(self._active_value(upm.KEY_INVESTMENT_GOAL), "稳健增值和现金增强")
        self.assertIn("决策风格", out["profile_context"])
        self.assertIn("目标/用途", out["profile_context"])

    def test_natural_goal_and_decision_style_can_create_context_memory(self):
        out = upm.build_profile_memory_context(
            self.engine,
            user_id="u1",
            prompt_text="我喜欢先看结论再看细节，这笔钱半年后要用，主要想做现金增强。",
        )

        self.assertFalse(out["should_short_circuit"])
        self.assertEqual(self._active_value(upm.KEY_DECISION_STYLE), "结论再看细节")
        self.assertEqual(self._active_value(upm.KEY_INVESTMENT_GOAL), "半年后要用，主要想做现金增强")
        self.assertIn("决策风格", out["profile_context"])
        self.assertIn("目标/用途", out["profile_context"])

    def test_stable_fact_without_memory_marker_does_not_write(self):
        out = upm.build_profile_memory_context(
            self.engine,
            user_id="u1",
            prompt_text="今天数据不是实时，先帮我看看创业板ETF期权怎么调。",
        )

        self.assertFalse(out["should_short_circuit"])
        self.assertNotIn("【稳定事实】", out["profile_context"])

    def test_recurring_focus_explicit_update_ignores_data_limit_as_profile(self):
        out = upm.build_profile_memory_context(
            self.engine,
            user_id="u1",
            prompt_text="记住我经常关注创业板ETF期权；数据限制是资金流数据T+1，不要说成实时。",
        )

        self.assertTrue(out["should_short_circuit"])
        self.assertEqual(self._active_value(upm.KEY_RECURRING_FOCUS), "创业板ETF期权")
        self.assertIn("高频关注", out["profile_context"])
        self.assertNotIn("数据限制", out["profile_context"])

    def test_query_risk_preference_is_deterministic(self):
        upm.upsert_profile_memory(
            self.engine,
            user_id="u1",
            memory_key=upm.KEY_RISK_PREFERENCE,
            memory_value="偏保守",
            source_text="把我的风险偏好改成偏保守",
        )

        out = upm.build_profile_memory_context(
            self.engine,
            user_id="u1",
            prompt_text="我的风险偏好是什么",
        )

        self.assertTrue(out["should_short_circuit"])
        self.assertEqual(out["memory_action"], "query")
        self.assertIn("你当前记录的风险偏好是：偏保守", out["confirmation"])
        self.assertNotIn("卖认购", out["confirmation"])

    def test_query_all_profile_memories_lists_active_fields(self):
        upm.upsert_profile_memory(
            self.engine,
            user_id="u1",
            memory_key=upm.KEY_RISK_PREFERENCE,
            memory_value="偏保守",
        )
        upm.upsert_profile_memory(
            self.engine,
            user_id="u1",
            memory_key=upm.KEY_PREFERRED_PRODUCTS,
            memory_value="ETF期权",
        )

        out = upm.build_profile_memory_context(
            self.engine,
            user_id="u1",
            prompt_text="你记住了我什么",
        )

        self.assertTrue(out["should_short_circuit"])
        self.assertIn("风险偏好：偏保守", out["confirmation"])
        self.assertIn("常看品种：ETF期权", out["confirmation"])

    def test_personal_profile_updates_age_gender_hobbies_fears_and_dislikes(self):
        cases = [
            ("记住我是35岁", upm.KEY_AGE, "35岁"),
            ("我是女性", upm.KEY_GENDER, "女性"),
            ("我喜欢徒步和咖啡", upm.KEY_HOBBIES, "徒步和咖啡"),
            ("我害怕大亏", upm.KEY_FEARS, "大亏"),
            ("我讨厌长篇大论", upm.KEY_DISLIKES, "长篇大论"),
        ]

        for prompt, key, expected in cases:
            upm.build_profile_memory_context(self.engine, user_id="u1", prompt_text=prompt)
            self.assertEqual(self._active_value(key), expected)
            self.assertGreaterEqual(self._active_confidence(key), 0.9)

    def test_personal_profile_natural_sentence_updates_structured_fields(self):
        out = upm.build_profile_memory_context(
            self.engine,
            user_id="u1",
            prompt_text="我1987年生，男性，喜欢打网球，你要记住",
        )

        self.assertTrue(out["should_short_circuit"])
        self.assertEqual(out["memory_action"], "updated")
        self.assertEqual(self._active_value(upm.KEY_AGE), "1987年生")
        self.assertEqual(self._active_value(upm.KEY_GENDER), "男性")
        self.assertEqual(self._active_value(upm.KEY_HOBBIES), "打网球")
        self.assertIn("【个人画像】", out["profile_context"])
        self.assertIn("年龄：1987年生", out["profile_context"])
        self.assertIn("性别：男性", out["profile_context"])
        self.assertIn("爱好：打网球", out["profile_context"])

    def test_inferred_personal_profile_uses_lower_confidence(self):
        out = upm.build_profile_memory_context(
            self.engine,
            user_id="u1",
            prompt_text="一看到大波动就慌",
        )

        self.assertEqual(self._active_value(upm.KEY_FEARS), "看到大波动容易慌")
        self.assertAlmostEqual(self._active_confidence(upm.KEY_FEARS), 0.55)
        self.assertIn("【个人画像】", out["profile_context"])
        self.assertIn("害怕/担心（推断）：看到大波动容易慌", out["profile_context"])

    def test_hobby_reference_does_not_create_temporary_override(self):
        upm.upsert_profile_memory(
            self.engine,
            user_id="u1",
            memory_key=upm.KEY_HOBBIES,
            memory_value="打网球",
        )

        out = upm.build_profile_memory_context(
            self.engine,
            user_id="u1",
            prompt_text="用我喜欢的运动打个比方，解释一下牛市价差",
        )

        self.assertFalse(out["should_short_circuit"])
        self.assertNotIn("本轮当前表达优先", out["profile_context"])
        self.assertNotIn("的运动打个比方", out["profile_context"])
        self.assertEqual(self._active_value(upm.KEY_HOBBIES), "打网球")

    def test_query_personality_answer_uses_structured_profile_only(self):
        upm.upsert_profile_memory(
            self.engine,
            user_id="u1",
            memory_key=upm.KEY_RISK_PREFERENCE,
            memory_value="偏积极",
            confidence=0.9,
        )
        upm.upsert_profile_memory(
            self.engine,
            user_id="u1",
            memory_key=upm.KEY_FEARS,
            memory_value="看到大波动容易慌",
            confidence=0.55,
        )

        out = upm.build_profile_memory_context(
            self.engine,
            user_id="u1",
            prompt_text="你觉得我是怎样的人",
        )

        self.assertTrue(out["should_short_circuit"])
        self.assertIn("明确记录", out["confirmation"])
        self.assertIn("推断印象", out["confirmation"])
        self.assertIn("风险偏好：偏积极", out["confirmation"])
        self.assertIn("害怕/担心：看到大波动容易慌", out["confirmation"])
        self.assertNotIn("卖认购", out["confirmation"])

    def test_portfolio_status_query_with_snapshot_short_circuits(self):
        out = upm.build_profile_memory_context(
            self.engine,
            user_id="u1",
            prompt_text="你记得我持仓吗",
            portfolio_snapshot_loader=lambda uid: {
                "recognized_count": 3,
                "summary_text": "识别到3只股票，行业最大暴露为半导体。",
                "updated_at": "2026-05-07 20:40:00",
            },
        )

        self.assertTrue(out["should_short_circuit"])
        self.assertEqual(out["memory_action"], "portfolio_status_query")
        self.assertIn("识别到 3 只", out["confirmation"])
        self.assertIn("你要我分析或判断时再展开", out["confirmation"])

    def test_portfolio_status_query_without_snapshot_short_circuits(self):
        out = upm.build_profile_memory_context(
            self.engine,
            user_id="u1",
            prompt_text="我上传过持仓吗",
            portfolio_snapshot_loader=lambda uid: {},
        )

        self.assertTrue(out["should_short_circuit"])
        self.assertIn("没有查到你的结构化持仓记录", out["confirmation"])

    def test_portfolio_analysis_request_does_not_short_circuit(self):
        out = upm.build_profile_memory_context(
            self.engine,
            user_id="u1",
            prompt_text="帮我分析我的持仓风险大吗",
            portfolio_snapshot_loader=lambda uid: {"recognized_count": 3},
        )

        self.assertFalse(out["should_short_circuit"])

    def test_query_empty_profile_has_clear_answer(self):
        out = upm.build_profile_memory_context(
            self.engine,
            user_id="u1",
            prompt_text="你记住了我什么",
        )

        self.assertTrue(out["should_short_circuit"])
        self.assertEqual(out["memory_action"], "query")
        self.assertIn("还没有记录到", out["confirmation"])

    def test_challenge_unverified_trade_short_circuits(self):
        upm.upsert_profile_memory(
            self.engine,
            user_id="u1",
            memory_key=upm.KEY_RISK_PREFERENCE,
            memory_value="偏保守",
        )

        out = upm.build_profile_memory_context(
            self.engine,
            user_id="u1",
            prompt_text="我什么时候做了卖认购3.6",
        )

        self.assertTrue(out["should_short_circuit"])
        self.assertEqual(out["memory_action"], "challenge")
        self.assertIn("不应该把未确认内容当成你的历史操作", out["confirmation"])
        self.assertIn("不在结构化画像里", out["confirmation"])

    def test_challenge_delete_matching_active_memory(self):
        upm.upsert_profile_memory(
            self.engine,
            user_id="u1",
            memory_key=upm.KEY_RISK_PREFERENCE,
            memory_value="偏保守",
        )

        out = upm.build_profile_memory_context(
            self.engine,
            user_id="u1",
            prompt_text="这不是我的意思，删掉偏保守，别再提",
        )

        self.assertTrue(out["should_short_circuit"])
        self.assertEqual(self._active_value(upm.KEY_RISK_PREFERENCE), "")
        self.assertIn("停用", out["confirmation"])

    def test_guest_user_never_writes_long_term_memory(self):
        out = upm.build_profile_memory_context(
            self.engine,
            user_id="访客",
            prompt_text="记住我主要做 ETF期权",
        )

        self.assertEqual(out["memory_action"], "guest_skip")
        self.assertEqual(upm.get_active_profile_memories(self.engine, "访客"), [])


if __name__ == "__main__":
    unittest.main()
