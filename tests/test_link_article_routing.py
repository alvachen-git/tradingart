from types import SimpleNamespace

from langchain_core.messages import HumanMessage

import agent_core


LINK_QUERY = "https://wallstreetcn.com/articles/3774521 根据这篇文章，利好哪些A股呢"


def _link_state():
    return {
        "user_query": LINK_QUERY,
        "messages": [HumanMessage(content=LINK_QUERY)],
        "symbol": "",
        "link_context": {
            "ok": True,
            "url": "https://wallstreetcn.com/articles/3774521",
            "title": "六氟化钨涨价",
            "snippet": "六氟化钨价格上涨，文章主线指向含氟电子特气和高纯钨制品。",
            "snippet_len": 32,
            "source": "url_preprocess",
        },
    }


def test_link_article_mapping_policy_overrides_screener_plan():
    plan, symbol = agent_core._apply_analysis_task_policy(LINK_QUERY, ["screener"], "688008")

    assert plan == ["researcher"]
    assert symbol == ""


def test_article_event_profile_parses_structured_json():
    class FakeLLM:
        def invoke(self, prompt):
            assert "输出 JSON" in prompt
            return SimpleNamespace(
                content=(
                    "```json\n"
                    '{"main_event":"六氟化钨涨价","industry_chain_items":["含氟电子特气"],'
                    '"mentioned_companies":[],"key_claims":["价格上涨"],'
                    '"missing_info":["公司业务占比"]}'
                    "\n```"
                )
            )

    profile = agent_core.build_article_event_profile(_link_state()["link_context"], FakeLLM())

    assert profile["main_event"] == "六氟化钨涨价"
    assert profile["industry_chain_items"] == ["含氟电子特气"]
    assert profile["mentioned_companies"] == []
    assert profile["source"] == "llm_structured"


def test_article_event_profile_fallback_does_not_invent_companies():
    profile = agent_core.build_article_event_profile(_link_state()["link_context"], llm=None)

    assert profile["main_event"]
    assert profile["mentioned_companies"] == []
    assert any("电子特气" in item or "高纯钨" in item for item in profile["industry_chain_items"])
    assert "候选公司的主营业务占比" in profile["missing_info"]


def test_article_mapping_keeps_inferred_direction_out_of_article_mentions():
    profile = {
        "main_event": "六氟化钨价格上涨",
        "industry_chain_items": ["含氟电子特气"],
        "mentioned_companies": [],
        "key_claims": ["价格上涨"],
        "missing_info": ["业务占比"],
    }

    candidates = agent_core.map_article_to_a_share_candidates(
        profile,
        search_result="补证结果提到昊华科技可能涉及电子特气业务。",
    )

    assert candidates[0]["name_or_direction"] == "含氟电子特气"
    assert candidates[0]["source_type"] == "产业链推导"
    assert all(item["source_type"] != "文章明确提到" for item in candidates)
    assert any(item["name_or_direction"] == "昊华科技" and item["source_type"] == "待核验" for item in candidates)


def test_candidate_verification_handles_search_failure():
    class FailingSearch:
        def invoke(self, _payload):
            raise RuntimeError("network down")

    candidates = [
        {
            "name_or_direction": "昊华科技",
            "source_type": "待核验",
            "benefit_logic": "补证搜索结果中出现该公司。",
            "verification_needed": "主营业务",
            "confidence": "低",
        }
    ]

    verified = agent_core.verify_a_share_candidates(candidates, search_tool=FailingSearch())

    assert verified[0]["confidence"] == "低"
    assert "补证搜索暂不可用" in verified[0]["verification_summary"]


def test_link_article_answer_survives_search_failure(monkeypatch):
    class FailingSearch:
        def invoke(self, _payload):
            raise RuntimeError("network down")

    class FakeLLM:
        def invoke(self, _prompt):
            return SimpleNamespace(content="not json")

    monkeypatch.setattr(agent_core, "search_web", FailingSearch())

    out = agent_core._answer_link_article_stock_mapping(_link_state(), FakeLLM())
    content = out["messages"][0].content

    assert content.startswith("【情报与舆情】")
    assert "一句话先说" in content
    assert "### 这篇文章在说什么" in content
    assert "### 候选公司/方向映射" in content
    assert "### 核验线索" in content
    assert "补证搜索暂不可用" in content
    assert "### 风险边界" not in content
    assert "风险股票警示" not in content


def test_screener_guard_returns_article_first_answer(monkeypatch):
    class FakeSearch:
        def invoke(self, _payload):
            return "补证：昊华科技可能涉及电子特气业务，仍需核验主营占比。"

    class FakeLLM:
        def invoke(self, prompt):
            assert "输出 JSON" in prompt
            return SimpleNamespace(
                content=(
                    '{"main_event":"六氟化钨涨价",'
                    '"industry_chain_items":["含氟电子特气","高纯钨制品"],'
                    '"mentioned_companies":[],'
                    '"key_claims":["六氟化钨价格上涨"],'
                    '"missing_info":["公司主营业务占比"]}'
                )
            )

    monkeypatch.setattr(agent_core, "search_web", FakeSearch())

    out = agent_core.screener_node(_link_state(), FakeLLM())
    content = out["messages"][0].content

    assert content.startswith("【情报与舆情】")
    assert "一句话先说" in content
    assert "### 这篇文章在说什么" in content
    assert "### A股可以顺着这些方向看" in content
    assert "### 候选公司/方向映射" in content
    assert "### 风险边界" not in content
    assert "产业链推导" in content
    assert "待核验" in content
    assert "风险股票警示" not in content
    assert "空头吞噬" not in content
