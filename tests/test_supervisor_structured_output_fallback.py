import unittest

import agent_core
from langchain_core.messages import HumanMessage
from langchain_core.runnables import RunnableLambda


def _state(query: str) -> dict:
    return {
        "user_query": query,
        "messages": [HumanMessage(content=query)],
        "has_portfolio": False,
    }


class _NeverCalledLLM:
    def with_structured_output(self, _schema):
        raise AssertionError("hard-override tasks must bypass the structured planner")


class _BrokenStructuredOutputLLM:
    def __init__(self):
        self.calls = 0

    def with_structured_output(self, _schema):
        self.calls += 1

        def raise_parse_error(_prompt_value):
            raise ValueError("malformed PlanningOutput tool arguments")

        return RunnableLambda(raise_parse_error)


class SupervisorStructuredOutputFallbackTest(unittest.TestCase):
    def test_stock_selection_bypasses_structured_planner(self):
        query = "美股有哪些技术面止跌，波动率偏高，适合卖看跌期权的呢"

        result = agent_core.supervisor_node(_state(query), _NeverCalledLLM())

        self.assertEqual(result["plan"], ["screener"])
        self.assertEqual(result["symbol"], "")
        self.assertIn("统一任务分类", result["route_decision"]["reason"])

    def test_option_decision_uses_policy_when_json_parsing_fails(self):
        query = "AAPL现在适合卖put吗"
        llm = _BrokenStructuredOutputLLM()

        result = agent_core.supervisor_node(_state(query), llm)

        self.assertEqual(llm.calls, 1)
        self.assertEqual(result["plan"], ["analyst", "monitor", "strategist"])
        self.assertEqual(result["symbol"], "AAPL")
        self.assertIn("结构化输出失败", result["route_decision"]["reason"])

    def test_unknown_task_falls_back_to_clarification_instead_of_raising(self):
        llm = _BrokenStructuredOutputLLM()

        result = agent_core.supervisor_node(_state("帮我看看"), llm)

        self.assertEqual(llm.calls, 1)
        self.assertEqual(result["plan"], ["chatter"])
        self.assertEqual(result["symbol"], "")


if __name__ == "__main__":
    unittest.main()
