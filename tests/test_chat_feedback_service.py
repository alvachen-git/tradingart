import unittest

from sqlalchemy import create_engine
from sqlalchemy import text

import chat_feedback_service as feedback_service


class TestChatFeedbackService(unittest.TestCase):
    def setUp(self):
        feedback_service._CHAT_FEEDBACK_SCHEMA_READY = False
        feedback_service._CHAT_FEEDBACK_SCHEMA_ENGINE_ID = ""
        self.engine = create_engine("sqlite:///:memory:", future=True)

    def test_save_answer_event_and_read_back(self):
        saved = feedback_service.save_chat_answer_event(
            self.engine,
            task_id="task-1",
            user_id="u1",
            trace_id="trace_1",
            answer_id="answer_1",
            prompt_text="review my holdings",
            response_text="This is a concrete answer",
            intent_domain="stock_portfolio",
            feedback_allowed=True,
        )

        self.assertTrue(saved)
        row = feedback_service.get_chat_answer_event(self.engine, "answer_1")
        self.assertEqual(row["user_id"], "u1")
        self.assertEqual(row["trace_id"], "trace_1")
        self.assertEqual(row["intent_domain"], "stock_portfolio")

    def test_submit_down_feedback_validates_reason_code(self):
        feedback_service.save_chat_answer_event(
            self.engine,
            task_id="task-2",
            user_id="u1",
            trace_id="trace_2",
            answer_id="answer_2",
            prompt_text="tell me what to do",
            response_text="generic answer",
            intent_domain="general",
            feedback_allowed=True,
        )

        invalid = feedback_service.submit_chat_feedback(
            self.engine,
            answer_id="answer_2",
            trace_id="trace_2",
            user_id="u1",
            feedback_type="down",
            reason_code="bad_code",
        )
        valid = feedback_service.submit_chat_feedback(
            self.engine,
            answer_id="answer_2",
            trace_id="trace_2",
            user_id="u1",
            feedback_type="down",
            reason_code="not_actionable",
            feedback_text="Please give me a concrete next step",
        )

        self.assertEqual(invalid["code"], "invalid_reason_code")
        self.assertTrue(valid["ok"])

    def test_submit_feedback_rejects_wrong_user(self):
        feedback_service.save_chat_answer_event(
            self.engine,
            task_id="task-3",
            user_id="u1",
            trace_id="trace_3",
            answer_id="answer_3",
            prompt_text="review my trade",
            response_text="answer",
            intent_domain="general",
            feedback_allowed=True,
        )

        result = feedback_service.submit_chat_feedback(
            self.engine,
            answer_id="answer_3",
            trace_id="trace_3",
            user_id="u2",
            feedback_type="up",
        )

        self.assertEqual(result["code"], "forbidden")

    def test_submit_feedback_is_idempotent_for_same_user_and_answer(self):
        feedback_service.save_chat_answer_event(
            self.engine,
            task_id="task-4",
            user_id="u1",
            trace_id="trace_4",
            answer_id="answer_4",
            prompt_text="review my trade",
            response_text="answer",
            intent_domain="general",
            feedback_allowed=True,
        )

        first = feedback_service.submit_chat_feedback(
            self.engine,
            answer_id="answer_4",
            trace_id="trace_4",
            user_id="u1",
            feedback_type="up",
        )
        second = feedback_service.submit_chat_feedback(
            self.engine,
            answer_id="answer_4",
            trace_id="trace_4",
            user_id="u1",
            feedback_type="up",
        )

        self.assertTrue(first["ok"])
        self.assertEqual(second["code"], "already_submitted")

    def test_list_failure_candidates_groups_repeated_prompts(self):
        for idx, user_id in enumerate(["u1", "u2"], start=1):
            answer_id = f"answer_{idx}"
            trace_id = f"trace_{idx}"
            feedback_service.save_chat_answer_event(
                self.engine,
                task_id=f"task-{idx}",
                user_id=user_id,
                trace_id=trace_id,
                answer_id=answer_id,
                prompt_text="review my holdings",
                response_text=f"answer {idx}",
                intent_domain="stock_portfolio",
                feedback_allowed=True,
            )
            feedback_service.submit_chat_feedback(
                self.engine,
                answer_id=answer_id,
                trace_id=trace_id,
                user_id=user_id,
                feedback_type="down",
                reason_code="too_generic",
                feedback_text="more detail please",
            )

        out = feedback_service.list_chat_feedback_failure_candidates(self.engine, limit=10)

        self.assertEqual(len(out), 1)
        self.assertEqual(out[0]["reason_code"], "too_generic")
        self.assertEqual(out[0]["occurrence_count"], 2)

    def test_list_feedback_events_supports_reason_keyword_and_time_filters(self):
        feedback_service.save_chat_answer_event(
            self.engine,
            task_id="task-10",
            user_id="u1",
            trace_id="trace_10",
            answer_id="answer_10",
            prompt_text="review my holdings",
            response_text="generic answer",
            intent_domain="stock_portfolio",
            feedback_allowed=True,
        )
        feedback_service.submit_chat_feedback(
            self.engine,
            answer_id="answer_10",
            trace_id="trace_10",
            user_id="u1",
            feedback_type="down",
            reason_code="too_generic",
            feedback_text="need a position plan",
        )

        feedback_service.save_chat_answer_event(
            self.engine,
            task_id="task-11",
            user_id="u2",
            trace_id="trace_11",
            answer_id="answer_11",
            prompt_text="what is the next action",
            response_text="action answer",
            intent_domain="trade_plan",
            feedback_allowed=True,
        )
        feedback_service.submit_chat_feedback(
            self.engine,
            answer_id="answer_11",
            trace_id="trace_11",
            user_id="u2",
            feedback_type="down",
            reason_code="not_actionable",
            feedback_text="still not specific enough",
        )

        with self.engine.begin() as conn:
            conn.execute(
                text(
                    """
                    UPDATE chat_feedback_events
                    SET created_at = :created_at
                    WHERE answer_id = :answer_id
                    """
                ),
                {"created_at": "2026-04-20T12:00:00", "answer_id": "answer_10"},
            )
            conn.execute(
                text(
                    """
                    UPDATE chat_feedback_events
                    SET created_at = :created_at
                    WHERE answer_id = :answer_id
                    """
                ),
                {"created_at": "2026-04-02T12:00:00", "answer_id": "answer_11"},
            )

        out = feedback_service.list_chat_feedback_events(
            self.engine,
            limit=20,
            feedback_type="down",
            reason_code="too_generic",
            keyword="position plan",
            start_at="2026-04-15T00:00:00",
            end_at="2026-04-21T23:59:59",
        )

        self.assertEqual(len(out), 1)
        self.assertEqual(out[0]["answer_id"], "answer_10")

    def test_list_failure_candidates_supports_domain_and_min_occurrence_filters(self):
        samples = [
            ("u1", "answer_21", "trace_21", "stock_portfolio", "review my holdings", "too_generic"),
            ("u2", "answer_22", "trace_22", "stock_portfolio", "review my holdings", "too_generic"),
            ("u3", "answer_23", "trace_23", "macro_view", "review my holdings", "too_generic"),
        ]
        for user_id, answer_id, trace_id, domain, prompt, reason_code in samples:
            feedback_service.save_chat_answer_event(
                self.engine,
                task_id=f"task-{answer_id}",
                user_id=user_id,
                trace_id=trace_id,
                answer_id=answer_id,
                prompt_text=prompt,
                response_text=f"response for {answer_id}",
                intent_domain=domain,
                feedback_allowed=True,
            )
            feedback_service.submit_chat_feedback(
                self.engine,
                answer_id=answer_id,
                trace_id=trace_id,
                user_id=user_id,
                feedback_type="down",
                reason_code=reason_code,
                feedback_text="need more detail",
            )

        out = feedback_service.list_chat_feedback_failure_candidates(
            self.engine,
            limit=10,
            intent_domain="stock_portfolio",
            reason_code="too_generic",
            min_occurrence=2,
        )

        self.assertEqual(len(out), 1)
        self.assertEqual(out[0]["intent_domain"], "stock_portfolio")
        self.assertEqual(out[0]["occurrence_count"], 2)

    def test_upsert_feedback_sample_creates_and_updates_existing_row(self):
        created = feedback_service.upsert_chat_feedback_sample(
            self.engine,
            prompt_text="review my holdings",
            reason_code="too_generic",
            intent_domain="stock_portfolio",
            occurrence_count=2,
            latest_feedback_at="2026-04-20T10:00:00",
            latest_feedback_text="please be more specific",
            sample_answer_id="answer_31",
            sample_trace_id="trace_31",
            sample_response_text="generic answer",
            created_by="mike0919",
            sample_status="new",
            optimization_type="prompt",
            review_notes="first pass",
        )
        updated = feedback_service.upsert_chat_feedback_sample(
            self.engine,
            prompt_text="review my holdings",
            reason_code="too_generic",
            intent_domain="stock_portfolio",
            occurrence_count=5,
            latest_feedback_at="2026-04-21T11:00:00",
            latest_feedback_text="still generic",
            sample_answer_id="answer_32",
            sample_trace_id="trace_32",
            sample_response_text="another answer",
            created_by="mike0919",
        )

        self.assertEqual(created["code"], "created")
        self.assertEqual(updated["code"], "updated")
        sample = updated["sample"]
        self.assertEqual(sample["occurrence_count"], 5)
        self.assertEqual(sample["sample_status"], "new")
        self.assertEqual(sample["optimization_type"], "prompt")
        self.assertEqual(sample["latest_feedback_text"], "still generic")

    def test_update_feedback_sample_persists_status_and_reviewer(self):
        create_result = feedback_service.upsert_chat_feedback_sample(
            self.engine,
            prompt_text="what is the next action",
            reason_code="not_actionable",
            intent_domain="trade_plan",
            occurrence_count=3,
            latest_feedback_at="2026-04-21T08:00:00",
            latest_feedback_text="need clear triggers",
            sample_answer_id="answer_41",
            sample_trace_id="trace_41",
            sample_response_text="watch the market",
            created_by="mike0919",
        )
        sample_key = create_result["sample"]["sample_key"]

        updated = feedback_service.update_chat_feedback_sample(
            self.engine,
            sample_key=sample_key,
            sample_status="accepted",
            optimization_type="rule",
            review_notes="needs mandatory trigger checklist",
            reviewed_by="mike0919",
        )

        self.assertTrue(updated["ok"])
        sample = updated["sample"]
        self.assertEqual(sample["sample_status"], "accepted")
        self.assertEqual(sample["optimization_type"], "rule")
        self.assertEqual(sample["reviewed_by"], "mike0919")
        self.assertEqual(sample["review_notes"], "needs mandatory trigger checklist")

    def test_list_feedback_samples_supports_filters(self):
        feedback_service.upsert_chat_feedback_sample(
            self.engine,
            prompt_text="review my holdings",
            reason_code="too_generic",
            intent_domain="stock_portfolio",
            occurrence_count=2,
            latest_feedback_at="2026-04-21T10:00:00",
            latest_feedback_text="need more position detail",
            sample_answer_id="answer_51",
            sample_trace_id="trace_51",
            sample_response_text="generic answer",
            created_by="mike0919",
            sample_status="accepted",
            optimization_type="prompt",
        )
        feedback_service.upsert_chat_feedback_sample(
            self.engine,
            prompt_text="is this fact correct",
            reason_code="wrong_fact",
            intent_domain="macro_view",
            occurrence_count=1,
            latest_feedback_at="2026-04-21T11:00:00",
            latest_feedback_text="numbers do not match",
            sample_answer_id="answer_52",
            sample_trace_id="trace_52",
            sample_response_text="wrong answer",
            created_by="mike0919",
            sample_status="reviewed",
            optimization_type="rag",
        )

        out = feedback_service.list_chat_feedback_samples(
            self.engine,
            limit=20,
            sample_status="accepted",
            optimization_type="prompt",
            keyword="position detail",
        )

        self.assertEqual(len(out), 1)
        self.assertEqual(out[0]["reason_code"], "too_generic")


if __name__ == "__main__":
    unittest.main()
