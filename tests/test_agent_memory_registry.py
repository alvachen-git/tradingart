import unittest

from sqlalchemy import create_engine, text

import agent_memory_registry as registry
import chat_feedback_service
import user_profile_memory


class _FakeChromaStore:
    def get(self, limit=20, where=None):
        return {
            "ids": ["chroma-1"],
            "documents": ["[2026-06-01 10:00] user asked\nAI answered"],
            "metadatas": [
                {
                    "user_id": "u1",
                    "timestamp": "2026-06-01 10:00",
                    "topic": "option",
                    "source": "test",
                    "memory_source_id": "src-chroma-1",
                }
            ],
        }


class TestAgentMemoryRegistry(unittest.TestCase):
    def setUp(self):
        registry._SCHEMA_READY_ENGINE_IDS.clear()
        chat_feedback_service._CHAT_FEEDBACK_SCHEMA_READY = False
        chat_feedback_service._CHAT_FEEDBACK_SCHEMA_ENGINE_ID = ""
        self.engine = create_engine("sqlite:///:memory:", future=True)

    def test_upsert_list_and_record_use(self):
        result = registry.upsert_agent_memory(
            self.engine,
            user_id="u1",
            namespace="user:u1:profile",
            memory_type=registry.MEMORY_TYPE_SEMANTIC,
            domain="profile",
            memory_key="risk_preference",
            value={"memory_value": "conservative"},
            text_summary="conservative",
            source_type=registry.SOURCE_PROFILE_MEMORY,
            source_id="u1:risk",
        )

        self.assertTrue(result["ok"])
        rows = registry.list_agent_memories(self.engine, user_id="u1")
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["value"]["memory_value"], "conservative")

        used = registry.record_agent_memory_use(
            self.engine,
            source_type=registry.SOURCE_PROFILE_MEMORY,
            source_id="u1:risk",
        )
        self.assertTrue(used)
        rows = registry.list_agent_memories(self.engine, user_id="u1")
        self.assertEqual(rows[0]["use_count"], 1)
        self.assertTrue(rows[0]["last_used_at"])

    def test_profile_registration_supersedes_previous_active_value(self):
        registry.register_profile_memory(
            self.engine,
            user_id="u1",
            memory_key="risk_preference",
            memory_value="conservative",
        )
        registry.register_profile_memory(
            self.engine,
            user_id="u1",
            memory_key="risk_preference",
            memory_value="aggressive",
        )

        active = registry.list_agent_memories(
            self.engine,
            user_id="u1",
            namespace="user:u1:profile",
            memory_type=registry.MEMORY_TYPE_SEMANTIC,
        )
        all_rows = registry.list_agent_memories(
            self.engine,
            user_id="u1",
            namespace="user:u1:profile",
            memory_type=registry.MEMORY_TYPE_SEMANTIC,
            status="",
        )

        self.assertEqual(len(active), 1)
        self.assertEqual(active[0]["value"]["memory_value"], "aggressive")
        self.assertEqual(len(all_rows), 2)

    def test_backfill_legacy_profile_feedback_and_chroma_records(self):
        user_profile_memory.ensure_profile_memory_table(self.engine)
        chat_feedback_service.ensure_chat_feedback_tables(self.engine)
        with self.engine.begin() as conn:
            conn.execute(
                text(
                    """
                    INSERT INTO user_profile_memory (
                        user_id, memory_key, memory_value, confidence, source_text,
                        status, occurrence_count, created_at, updated_at
                    )
                    VALUES (
                        'u1', 'risk_preference', 'conservative', 0.9, 'legacy profile',
                        'active', 1, '2026-06-01T10:00:00', '2026-06-01T10:00:00'
                    )
                    """
                )
            )
            conn.execute(
                text(
                    """
                    INSERT INTO chat_feedback_events (
                        answer_id, trace_id, user_id, prompt_text, response_text, intent_domain,
                        feedback_type, reason_code, feedback_text, created_at
                    )
                    VALUES (
                        'answer-1', 'trace-1', 'u1', 'prompt', 'response', 'option',
                        'down', 'too_generic', 'more detail', '2026-06-01T10:00:00'
                    )
                    """
                )
            )
            conn.execute(
                text(
                    """
                    INSERT INTO chat_feedback_samples (
                        sample_key, prompt_text, reason_code, intent_domain, sample_answer_id,
                        sample_trace_id, sample_response_text, latest_feedback_text, occurrence_count,
                        sample_status, optimization_type, review_notes, created_by, reviewed_by,
                        first_seen_at, last_seen_at, reviewed_at, created_at, updated_at
                    )
                    VALUES (
                        'sample-1', 'prompt', 'too_generic', 'option', 'answer-1',
                        'trace-1', 'response', 'more detail', 2,
                        'new', 'prompt', '', '', '',
                        '2026-06-01T10:00:00', '2026-06-01T10:00:00', NULL,
                        '2026-06-01T10:00:00', '2026-06-01T10:00:00'
                    )
                    """
                )
            )

        out = registry.backfill_agent_memories(
            self.engine,
            include_chroma=True,
            vector_store=_FakeChromaStore(),
        )

        self.assertTrue(out["ok"])
        self.assertEqual(out["counts"]["profile"], 1)
        self.assertEqual(out["counts"]["feedback_events"], 1)
        self.assertEqual(out["counts"]["feedback_samples"], 1)
        self.assertEqual(out["counts"]["conversation"], 1)
        rows = registry.list_agent_memories(self.engine, status="", limit=20)
        self.assertEqual(len(rows), 4)


if __name__ == "__main__":
    unittest.main()
