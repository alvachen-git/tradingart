import unittest

from chat_routing import (
    CHAT_MODE_ANALYSIS,
    CHAT_MODE_KNOWLEDGE,
    CHAT_MODE_SIMPLE,
    classify_chat_mode,
    is_pure_option_data_query,
)


class TestChatRouting(unittest.TestCase):
    def test_classify_simple_chat(self):
        self.assertEqual(classify_chat_mode("你好"), CHAT_MODE_SIMPLE)
        self.assertEqual(classify_chat_mode("谢谢你"), CHAT_MODE_SIMPLE)
        self.assertEqual(classify_chat_mode("法国大革命是什么"), CHAT_MODE_SIMPLE)
        self.assertEqual(classify_chat_mode("怎么缓解焦虑"), CHAT_MODE_SIMPLE)
        self.assertEqual(classify_chat_mode("帮我写一段生日祝福"), CHAT_MODE_SIMPLE)

    def test_classify_knowledge_chat(self):
        self.assertEqual(classify_chat_mode("什么是牛市价差"), CHAT_MODE_KNOWLEDGE)
        self.assertEqual(classify_chat_mode("牛市价差策略是什么"), CHAT_MODE_KNOWLEDGE)
        self.assertEqual(classify_chat_mode("你知道牛市价差吗"), CHAT_MODE_KNOWLEDGE)
        self.assertEqual(classify_chat_mode("你了解牛市价差吗"), CHAT_MODE_KNOWLEDGE)
        self.assertEqual(classify_chat_mode("听过牛市价差吗"), CHAT_MODE_KNOWLEDGE)
        self.assertEqual(classify_chat_mode("解释一下IV"), CHAT_MODE_KNOWLEDGE)
        self.assertEqual(classify_chat_mode("delta和gamma有什么区别"), CHAT_MODE_KNOWLEDGE)
        self.assertEqual(classify_chat_mode("棉花期货交易是不是有季节性"), CHAT_MODE_KNOWLEDGE)
        self.assertEqual(classify_chat_mode("什么是真假突破四原则"), CHAT_MODE_KNOWLEDGE)
        self.assertEqual(classify_chat_mode("什么是支撑位和阻力位"), CHAT_MODE_KNOWLEDGE)
        self.assertEqual(classify_chat_mode("K线和均线有什么区别"), CHAT_MODE_KNOWLEDGE)

    def test_classify_analysis_chat(self):
        self.assertEqual(classify_chat_mode("黄金怎么看"), CHAT_MODE_ANALYSIS)
        self.assertEqual(classify_chat_mode("创业板期权做什么策略"), CHAT_MODE_ANALYSIS)
        self.assertEqual(classify_chat_mode("这条新闻对铜价影响大吗"), CHAT_MODE_ANALYSIS)
        self.assertEqual(classify_chat_mode("美国如果非农大超预期，纳指一般先交易什么？"), CHAT_MODE_ANALYSIS)
        self.assertEqual(classify_chat_mode("汇川技术现在估值高不高"), CHAT_MODE_ANALYSIS)
        self.assertEqual(classify_chat_mode("K线怎么看"), CHAT_MODE_ANALYSIS)
        self.assertEqual(classify_chat_mode("为什么今晚英特尔涨这么多？"), CHAT_MODE_ANALYSIS)
        self.assertEqual(classify_chat_mode("牛市价差怎么做"), CHAT_MODE_ANALYSIS)
        self.assertEqual(classify_chat_mode("牛市价差适合做吗"), CHAT_MODE_ANALYSIS)

    def test_company_recent_news_routes_to_knowledge(self):
        self.assertEqual(classify_chat_mode("汇川技术最近有什么好消息吗"), CHAT_MODE_KNOWLEDGE)
        self.assertEqual(classify_chat_mode("汇川技术的机器人业务，最近有没有好消息"), CHAT_MODE_KNOWLEDGE)

    def test_company_recent_news_followup_can_stay_knowledge(self):
        self.assertEqual(
            classify_chat_mode(
                "他的机器人或汽车业务",
                is_followup=True,
                recent_context="用户: 汇川技术最近有什么好消息吗\nAI: 最近我检到两条和机器人业务相关的动态。",
                focus_entity="汇川技术",
                focus_topic="公司近期动态",
                focus_aspect="机器人业务",
                focus_mode_hint="company_news",
            ),
            CHAT_MODE_KNOWLEDGE,
        )
        self.assertEqual(
            classify_chat_mode(
                "你详细展开",
                is_followup=True,
                recent_context="用户: 汇川技术的机器人业务，最近有没有好消息\nAI: 最近有一条机器人业务相关订单动态。",
                focus_entity="汇川技术",
                focus_topic="公司近期动态",
                focus_aspect="机器人业务",
                focus_mode_hint="company_news",
            ),
            CHAT_MODE_KNOWLEDGE,
        )

    def test_company_recent_news_followup_can_upgrade_to_analysis(self):
        self.assertEqual(
            classify_chat_mode(
                "那对股价影响大吗",
                is_followup=True,
                recent_context="用户: 汇川技术最近有什么好消息吗\nAI: 最近有一条机器人业务相关动态。",
                focus_entity="汇川技术",
                focus_topic="公司近期动态",
                focus_aspect="机器人业务",
                focus_mode_hint="company_news",
            ),
            CHAT_MODE_ANALYSIS,
        )

    def test_non_finance_followup_can_stay_simple_chat(self):
        self.assertEqual(
            classify_chat_mode("继续说说", is_followup=True, recent_context="用户: 法国大革命是什么\nAI: ..."),
            CHAT_MODE_SIMPLE,
        )

    def test_non_finance_followup_with_finance_like_words_stays_simple_chat(self):
        self.assertEqual(
            classify_chat_mode(
                "再展开说说",
                is_followup=True,
                recent_context=(
                    "用户: 法国大革命是什么\n"
                    "AI: 法国大革命起因于社会不平等、财政危机和启蒙思想的影响，"
                    "最终推动了法国政治制度重构。"
                ),
            ),
            CHAT_MODE_SIMPLE,
        )

    def test_finance_followup_does_not_fall_into_simple_chat(self):
        self.assertEqual(
            classify_chat_mode("那为什么", is_followup=True, recent_context="用户: 黄金怎么看\nAI: ..."),
            CHAT_MODE_ANALYSIS,
        )

    def test_finance_followup_can_stay_knowledge_chat(self):
        self.assertEqual(
            classify_chat_mode(
                "详细说明下",
                is_followup=True,
                recent_context="用户: 棉花期货交易是不是有季节性\nAI: 棉花期货确实有季节性，主要体现在种植、收获和出口节奏上。",
            ),
            CHAT_MODE_KNOWLEDGE,
        )

    def test_finance_followup_can_upgrade_to_analysis_chat(self):
        self.assertEqual(
            classify_chat_mode(
                "那现在适合做吗",
                is_followup=True,
                recent_context="用户: 什么是牛市价差\nAI: 牛市价差是一种偏温和看涨的期权策略。",
            ),
            CHAT_MODE_ANALYSIS,
        )

    def test_price_move_reason_followup_stays_analysis(self):
        self.assertEqual(
            classify_chat_mode(
                "那你帮我查一下具体是因为什么？",
                is_followup=True,
                recent_context="用户: 为什么今晚英特尔涨这么多？\nAI: 可能和业绩超预期或行业消息有关。",
                focus_entity="英特尔",
                focus_topic="异动原因",
                focus_mode_hint="price_move_reason",
            ),
            CHAT_MODE_ANALYSIS,
        )

    def test_simple_followup_numeric_request_upgrades_to_analysis(self):
        self.assertEqual(
            classify_chat_mode(
                "我要详细数值",
                is_followup=True,
                recent_context="用户: 澜起科技跟科创50的相关度有多少\nAI: 澜起科技和科创50有一定关联。",
                focus_entity="澜起科技",
                focus_topic="相关度",
                followup_goal="fetch_numeric",
            ),
            CHAT_MODE_ANALYSIS,
        )
        self.assertEqual(
            classify_chat_mode(
                "对，要澜起科技和科创50的具体相关度数值",
                is_followup=True,
                recent_context="用户: 澜起科技跟科创50的相关度有多少\nAI: 澜起科技和科创50有一定关联。",
                focus_entity="澜起科技",
                focus_topic="相关度",
                followup_goal="fetch_numeric",
            ),
            CHAT_MODE_ANALYSIS,
        )

    def test_non_finance_fact_followup_can_upgrade_to_knowledge(self):
        self.assertEqual(
            classify_chat_mode(
                "我要详细年份和关键节点",
                is_followup=True,
                recent_context="用户: 法国大革命是什么\nAI: 法国大革命是18世纪末法国发生的政治社会革命。",
                focus_topic="概念解释",
                followup_goal="fetch_facts",
            ),
            CHAT_MODE_KNOWLEDGE,
        )

    def test_correction_followup_for_entity_or_fact_upgrades_to_knowledge(self):
        self.assertEqual(
            classify_chat_mode(
                "不是中微公司，就叫中微半导",
                is_followup=True,
                recent_context=(
                    "用户: 中微半导是做什么的，有什么护城河，有什么隐忧，有什么竞争对手\n"
                    "AI: 中微半导应该是指中微公司吧。"
                ),
                focus_entity="中微半导",
                focus_topic="概念解释",
                correction_intent=True,
            ),
            CHAT_MODE_KNOWLEDGE,
        )
        self.assertEqual(
            classify_chat_mode(
                "有这家公司，你仔细思考下",
                is_followup=True,
                recent_context=(
                    "用户: 中微半导是做什么的，有什么护城河，有什么隐忧，有什么竞争对手\n"
                    "AI: 中微半导应该是指中微公司吧。"
                ),
                focus_entity="中微半导",
                focus_topic="概念解释",
                correction_intent=True,
            ),
            CHAT_MODE_KNOWLEDGE,
        )

    def test_correction_followup_for_analysis_judgment_upgrades_to_analysis(self):
        self.assertEqual(
            classify_chat_mode(
                "你这个护城河判断不对",
                is_followup=True,
                recent_context=(
                    "用户: 中微半导是做什么的，有什么护城河，有什么隐忧，有什么竞争对手\n"
                    "AI: 它的护城河主要来自技术壁垒和客户粘性。"
                ),
                focus_entity="中微半导",
                focus_topic="盘面分析",
                correction_intent=True,
            ),
            CHAT_MODE_ANALYSIS,
        )
        self.assertEqual(
            classify_chat_mode(
                "你这个年份不对",
                is_followup=True,
                recent_context="用户: 法国大革命是什么\nAI: 法国大革命是18世纪末法国发生的政治社会革命。",
                focus_topic="概念解释",
                correction_intent=True,
            ),
            CHAT_MODE_KNOWLEDGE,
        )

    def test_explain_more_followup_can_stay_knowledge(self):
        self.assertEqual(
            classify_chat_mode(
                "再举个例子",
                is_followup=True,
                recent_context="用户: 什么是牛市价差\nAI: 牛市价差是一种偏温和看涨的期权策略。",
                focus_topic="概念解释",
                followup_goal="explain_more",
            ),
            CHAT_MODE_KNOWLEDGE,
        )

    def test_detect_pure_option_data_query(self):
        self.assertTrue(is_pure_option_data_query("300ETF期权波动率高吗"))
        self.assertTrue(is_pure_option_data_query("创业板ETF期权IV现在多少"))
        self.assertTrue(is_pure_option_data_query("这个期权还有几天到期"))

    def test_option_strategy_or_market_question_is_not_pure_data_query(self):
        self.assertFalse(is_pure_option_data_query("300ETF期权波动率高吗，适合卖方吗"))
        self.assertFalse(is_pure_option_data_query("创业板卖认沽如何处理"))
        self.assertFalse(is_pure_option_data_query("300ETF期权怎么看"))


if __name__ == "__main__":
    unittest.main()
