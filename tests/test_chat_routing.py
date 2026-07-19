import unittest

from chat_routing import (
    CHAT_MODE_ANALYSIS,
    CHAT_MODE_KNOWLEDGE,
    CHAT_MODE_SIMPLE,
    classify_chat_mode,
    is_cn_margin_data_query,
    is_market_data_query,
    is_pure_option_data_query,
    is_us_option_market_profile_query,
    is_volatility_divergence_query,
    is_volatility_mechanism_knowledge_query,
    is_volatility_market_view_query,
)


class TestChatRouting(unittest.TestCase):
    def test_classify_simple_chat(self):
        self.assertEqual(classify_chat_mode("你好"), CHAT_MODE_SIMPLE)
        self.assertEqual(classify_chat_mode("谢谢你"), CHAT_MODE_SIMPLE)
        self.assertEqual(classify_chat_mode("法国大革命是什么"), CHAT_MODE_SIMPLE)
        self.assertEqual(classify_chat_mode("怎么缓解焦虑"), CHAT_MODE_SIMPLE)
        self.assertEqual(classify_chat_mode("帮我写一段生日祝福"), CHAT_MODE_SIMPLE)
        self.assertEqual(classify_chat_mode("你记得我持仓吗"), CHAT_MODE_SIMPLE)

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
        self.assertEqual(classify_chat_mode("什么是比例认购策略"), CHAT_MODE_KNOWLEDGE)

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
        self.assertEqual(classify_chat_mode("查看甲醇2609的iv波动率"), CHAT_MODE_ANALYSIS)
        self.assertEqual(classify_chat_mode("甲醇2609价格多少"), CHAT_MODE_ANALYSIS)
        self.assertEqual(classify_chat_mode("帮我分析我的持仓风险大吗"), CHAT_MODE_ANALYSIS)
        self.assertEqual(classify_chat_mode("中证500现在上涨是会升波还是降波呢"), CHAT_MODE_ANALYSIS)

    def test_volatility_direction_view_routes_to_analysis_not_knowledge(self):
        self.assertTrue(is_volatility_market_view_query("中证500现在上涨是会升波还是降波呢"))
        self.assertTrue(is_volatility_market_view_query("500ETF最近反弹后IV会升还是会降"))
        self.assertEqual(classify_chat_mode("中证500现在上涨是会升波还是降波呢"), CHAT_MODE_ANALYSIS)
        self.assertEqual(classify_chat_mode("什么是升波和降波"), CHAT_MODE_KNOWLEDGE)

    def test_volatility_mechanism_question_stays_knowledge(self):
        self.assertTrue(is_volatility_mechanism_knowledge_query("波动率什么情况下会上升"))
        self.assertTrue(is_volatility_mechanism_knowledge_query("隐含波动率哪些因素会导致上升"))
        self.assertEqual(classify_chat_mode("波动率什么情况下会上升"), CHAT_MODE_KNOWLEDGE)
        self.assertEqual(classify_chat_mode("隐含波动率哪些因素会导致上升"), CHAT_MODE_KNOWLEDGE)
        self.assertFalse(is_volatility_mechanism_knowledge_query("中证500现在上涨是会升波还是降波呢"))

    def test_broker_signal_questions_route_to_analysis(self):
        self.assertEqual(
            classify_chat_mode("螺纹钢现在从期货商正反指标看偏多还是偏空？"),
            CHAT_MODE_ANALYSIS,
        )
        self.assertEqual(
            classify_chat_mode("螺纹刚现在从期货商正反指标看偏多还是偏空？"),
            CHAT_MODE_ANALYSIS,
        )
        self.assertEqual(classify_chat_mode("RB 席位信号有没有分歧？"), CHAT_MODE_ANALYSIS)
        self.assertEqual(classify_chat_mode("中信建投加多是不是利多？"), CHAT_MODE_ANALYSIS)
        self.assertEqual(classify_chat_mode("中信建投的持仓如果持续加多是不是利多？"), CHAT_MODE_ANALYSIS)
        self.assertEqual(classify_chat_mode("反指标最近在哪些商品上做多"), CHAT_MODE_ANALYSIS)

    def test_product_directional_view_routes_to_analysis(self):
        self.assertEqual(classify_chat_mode("螺纹钢现在从成交量和持仓看偏多还是偏空？"), CHAT_MODE_ANALYSIS)
        self.assertEqual(classify_chat_mode("白银现在多空方向怎么看？"), CHAT_MODE_ANALYSIS)
        self.assertEqual(classify_chat_mode("RB从资金流和持仓看方向"), CHAT_MODE_ANALYSIS)
        self.assertEqual(classify_chat_mode("纯碱近期信号偏多还是偏空？"), CHAT_MODE_ANALYSIS)

    def test_stock_selection_routes_to_analysis(self):
        self.assertEqual(classify_chat_mode("帮我找放量突破的股票"), CHAT_MODE_ANALYSIS)
        self.assertEqual(classify_chat_mode("帮我选几只半导体里技术形态比较强的股票"), CHAT_MODE_ANALYSIS)
        self.assertEqual(classify_chat_mode("AI概念股有哪些"), CHAT_MODE_ANALYSIS)
        self.assertEqual(classify_chat_mode("推荐一些美股"), CHAT_MODE_ANALYSIS)
        self.assertEqual(classify_chat_mode("推荐一些美股，最好是从底部起来刚突破的"), CHAT_MODE_ANALYSIS)
        self.assertEqual(classify_chat_mode("推荐一些美股，偏技术面强一点"), CHAT_MODE_ANALYSIS)
        self.assertEqual(classify_chat_mode("帮我找适合做空的美股，给我3只名称"), CHAT_MODE_ANALYSIS)
        self.assertEqual(classify_chat_mode("推荐一些A股，最好是从底部起来刚突破的"), CHAT_MODE_ANALYSIS)

    def test_current_stock_selection_overrides_followup_knowledge_policy(self):
        self.assertEqual(
            classify_chat_mode(
                "帮我找适合做空的美股",
                is_followup=True,
                recent_context="用户: 适合做空的标的有什么特征\nAI: 做空要看基本面、估值和技术面。",
                followup_task_policy={
                    "recommended_chat_mode": CHAT_MODE_KNOWLEDGE,
                    "override_level": "force",
                },
            ),
            CHAT_MODE_ANALYSIS,
        )

    def test_current_option_scenario_overrides_followup_knowledge_policy(self):
        self.assertEqual(
            classify_chat_mode(
                "如果创业板ETF周一-10%开盘，IV会到多少，平值认沽涨多少",
                is_followup=True,
                recent_context="用户: 解释一下IV\nAI: IV是隐含波动率。",
                followup_task_policy={
                    "recommended_chat_mode": CHAT_MODE_KNOWLEDGE,
                    "override_level": "force",
                },
            ),
            CHAT_MODE_ANALYSIS,
        )

    def test_stock_selection_followup_routes_to_analysis(self):
        self.assertEqual(
            classify_chat_mode(
                "帮我找放量突破的股票",
                is_followup=True,
                recent_context="用户: 澜起科技的基本面和技术面分析下\nAI: 澜起科技基本面和技术面报告。",
            ),
            CHAT_MODE_ANALYSIS,
        )

    def test_stock_selection_does_not_capture_concept_explanation(self):
        self.assertEqual(classify_chat_mode("什么是放量突破"), CHAT_MODE_KNOWLEDGE)
        self.assertEqual(classify_chat_mode("什么是底部突破"), CHAT_MODE_KNOWLEDGE)
        self.assertEqual(classify_chat_mode("如何判断放量突破真假"), CHAT_MODE_KNOWLEDGE)

    def test_concept_explanation_overrides_followup_analysis_context(self):
        self.assertEqual(
            classify_chat_mode(
                "什么是比例认购策略",
                is_followup=True,
                recent_context="用户: 帮我找几只放量突破的股票\nAI: 当前市场暂无明显放量突破形态股票。",
            ),
            CHAT_MODE_KNOWLEDGE,
        )

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
        self.assertFalse(is_pure_option_data_query("如果创业板ETF周一-10%开盘，IV会到多少，平值认沽涨多少"))

    def test_detect_market_data_query(self):
        self.assertTrue(is_market_data_query("查看甲醇2609的iv波动率"))
        self.assertTrue(is_market_data_query("甲醇2609价格多少"))
        self.assertTrue(is_market_data_query("白银最新价多少"))
        self.assertTrue(is_market_data_query("创业板ETF期权IV现在多少"))
        self.assertFalse(is_market_data_query("甲醇2609波动率高吗，适合卖方吗"))
        self.assertFalse(is_market_data_query("如果创业板ETF周一-10%开盘，IV会到多少，平值认沽涨多少"))
        self.assertFalse(is_market_data_query("如果黄金跌破支撑，后面怎么看"))
        self.assertFalse(is_market_data_query("解释一下IV"))

    def test_cn_margin_data_query_routes_to_analysis(self):
        queries = (
            "2026年7月16日融资余额和5日动能是多少？",
            "融资融券最近数据",
            "两融余额多少",
        )
        for query in queries:
            with self.subTest(query=query):
                self.assertTrue(is_cn_margin_data_query(query))
                self.assertEqual(classify_chat_mode(query), CHAT_MODE_ANALYSIS)

    def test_company_financing_queries_do_not_trigger_margin_data_route(self):
        self.assertFalse(is_cn_margin_data_query("某公司再融资计划怎么样"))
        self.assertFalse(is_cn_margin_data_query("A轮融资估值多少"))

    def test_cn_market_risk_preference_query_remains_analysis(self):
        self.assertEqual(classify_chat_mode("现在A股大盘的风险偏好怎么看？"), CHAT_MODE_ANALYSIS)

    def test_us_option_market_profile_routes_only_us_option_data(self):
        self.assertTrue(is_us_option_market_profile_query("SPY期权IV高吗"))
        self.assertTrue(is_us_option_market_profile_query("NVDA 0DTE 活跃吗"))
        self.assertTrue(is_us_option_market_profile_query("QQQ skew 怎么看"))
        self.assertTrue(is_market_data_query("QQQ skew 怎么看"))
        self.assertEqual(classify_chat_mode("SPY期权IV高吗"), CHAT_MODE_ANALYSIS)
        self.assertEqual(classify_chat_mode("NVDA 0DTE 活跃吗"), CHAT_MODE_ANALYSIS)
        self.assertEqual(classify_chat_mode("QQQ skew 怎么看"), CHAT_MODE_ANALYSIS)

        self.assertFalse(is_us_option_market_profile_query("创业板ETF期权IV现在多少"))
        self.assertFalse(is_us_option_market_profile_query("甲醇期权skew怎么看"))
        self.assertFalse(is_us_option_market_profile_query("什么是 skew"))
        self.assertEqual(classify_chat_mode("什么是 skew"), CHAT_MODE_KNOWLEDGE)

    def test_us_option_market_profile_does_not_steal_strategy_questions(self):
        self.assertFalse(is_us_option_market_profile_query("SPY期权IV高吗，适合卖put吗"))
        self.assertFalse(is_us_option_market_profile_query("SPY期权IV高吗，适合铁鹰吗"))

    def test_us_option_strategy_routes_to_analysis_not_knowledge_or_pure_monitor(self):
        self.assertEqual(
            classify_chat_mode("aapl现在适合用什么期权策略操作，详细教我"),
            CHAT_MODE_ANALYSIS,
        )
        self.assertEqual(classify_chat_mode("AAPL现在适合卖put吗"), CHAT_MODE_ANALYSIS)
        self.assertEqual(classify_chat_mode("SPY铁鹰适合吗"), CHAT_MODE_ANALYSIS)
        self.assertEqual(classify_chat_mode("AAPL卖宽跨可以吗"), CHAT_MODE_ANALYSIS)
        self.assertEqual(classify_chat_mode("NVDA short strangle 适合做吗"), CHAT_MODE_ANALYSIS)
        self.assertFalse(is_us_option_market_profile_query("AAPL现在适合卖put吗"))
        self.assertFalse(is_market_data_query("AAPL现在适合卖put吗"))
        self.assertFalse(is_market_data_query("SPY铁鹰适合吗"))
        self.assertEqual(classify_chat_mode("什么是 covered call"), CHAT_MODE_KNOWLEDGE)

    def test_detect_iv_scanner_market_data_query(self):
        self.assertTrue(is_market_data_query("列出2026年6月11日到2026年6月12日 ATM IV增幅由大到小的合约"))
        self.assertTrue(is_market_data_query("IV 降幅最大的前5个合约"))
        self.assertEqual(
            classify_chat_mode("列出2026年6月11日到2026年6月12日 ATM IV增幅由大到小的合约"),
            CHAT_MODE_ANALYSIS,
        )

    def test_detect_volatility_divergence_query(self):
        self.assertTrue(is_volatility_divergence_query("有没有什么品种的波动率背离"))
        self.assertTrue(is_volatility_divergence_query("哪些商品出现价格和IV背离"))
        self.assertTrue(is_market_data_query("有没有什么品种的波动率背离"))
        self.assertEqual(classify_chat_mode("有没有什么品种的波动率背离"), CHAT_MODE_ANALYSIS)


    def test_execute_suggested_stock_screening_followup_routes_to_analysis(self):
        recent_context = (
            "用户: 股票要做对冲避险的话有什么好方法吗\n"
            "AI: 需要我帮你筛选一下目前综合评分较高或高股息/防御性板块的股票名单吗？"
        )
        self.assertEqual(
            classify_chat_mode(
                "好，帮我筛选",
                is_followup=True,
                recent_context=recent_context,
                followup_goal="execute_suggested_action",
            ),
            CHAT_MODE_ANALYSIS,
        )

    def test_followup_task_policy_can_force_knowledge_route(self):
        self.assertEqual(
            classify_chat_mode(
                "详细说说",
                is_followup=True,
                recent_context="用户: 什么是牛市价差\nAI: 牛市价差是一种期权概念。",
                followup_task_policy={
                    "followup_intent": "continue_explanation",
                    "recommended_chat_mode": CHAT_MODE_KNOWLEDGE,
                    "recommended_plan": ["chatter"],
                    "override_level": "suggest",
                },
            ),
            CHAT_MODE_KNOWLEDGE,
        )

    def test_non_finance_screening_phrase_stays_simple(self):
        self.assertEqual(classify_chat_mode("帮我筛选一下餐厅"), CHAT_MODE_SIMPLE)


    def test_non_finance_screening_requests_ignore_analysis_policy_override(self):
        policy = {
            "followup_intent": "execute_suggestion",
            "recommended_chat_mode": CHAT_MODE_ANALYSIS,
            "recommended_plan": ["generalist"],
            "override_level": "force",
        }
        self.assertEqual(
            classify_chat_mode("中午不知道吃什么，帮我筛选一些餐厅", followup_task_policy=policy),
            CHAT_MODE_SIMPLE,
        )
        self.assertEqual(
            classify_chat_mode("帮我筛选一下上海适合约会的餐厅", followup_task_policy=policy),
            CHAT_MODE_SIMPLE,
        )
        self.assertEqual(
            classify_chat_mode("帮我筛选几部周末看的电影", followup_task_policy=policy),
            CHAT_MODE_SIMPLE,
        )
        self.assertEqual(
            classify_chat_mode("帮我推荐几本书", followup_task_policy=policy),
            CHAT_MODE_SIMPLE,
        )

    def test_finance_screening_request_still_routes_to_analysis(self):
        self.assertEqual(classify_chat_mode("帮我筛选一些高股息股票"), CHAT_MODE_ANALYSIS)


    def test_option_question_with_target_word_does_not_route_as_stock_screening(self):
        query = "如果用卖出认购的方式来做空，应该遵循什么原则，卖虚值，平值，还是实值，希腊字母有什么要求，还是根据对应标的的K线形态操作"
        self.assertEqual(classify_chat_mode(query), CHAT_MODE_ANALYSIS)

    def test_generic_target_phrases_do_not_trigger_stock_screening_route(self):
        self.assertEqual(classify_chat_mode("这个策略有什么标的要求"), CHAT_MODE_ANALYSIS)
        self.assertEqual(classify_chat_mode("有什么标的需要注意"), CHAT_MODE_SIMPLE)
        self.assertEqual(classify_chat_mode("有什么原则"), CHAT_MODE_SIMPLE)

    def test_explicit_stock_screening_phrases_still_route_to_analysis(self):
        self.assertEqual(classify_chat_mode("有哪些高股息股票"), CHAT_MODE_ANALYSIS)
        self.assertEqual(classify_chat_mode("有什么防御性板块个股"), CHAT_MODE_ANALYSIS)
        self.assertEqual(classify_chat_mode("帮我筛选放量突破的股票"), CHAT_MODE_ANALYSIS)
        self.assertEqual(classify_chat_mode("给我几个候选股"), CHAT_MODE_ANALYSIS)


if __name__ == "__main__":
    unittest.main()
