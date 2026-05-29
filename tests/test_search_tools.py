import os
import unittest
from unittest.mock import Mock, patch

import search_tools

_SEARCH_ENV_DEFAULTS = {
    "ZHIPU_SEARCH_MODEL": "",
    "ZHIPU_SEARCH_ENGINE": "",
    "ZHIPU_SEARCH_MAX_ATTEMPTS": "",
    "ZHIPU_SEARCH_COUNT": "",
    "ZHIPU_SEARCH_DEEP_COUNT": "",
    "ENABLE_OFFICIAL_FILING_PROBE": "",
    "FILING_PROBE_TIMEOUT_SECONDS": "",
    "FILING_PROBE_MAX_RESULTS": "",
    "ENABLE_FILING_PDF_PARSE": "",
    "FILING_PDF_PARSE_TIMEOUT_SECONDS": "",
    "FILING_PDF_MAX_PAGES": "",
    "FILING_PDF_MAX_BYTES": "",
}


class TestSearchTools(unittest.TestCase):
    def test_precise_finance_query_skips_keyword_optimization(self):
        with patch.object(search_tools, "_optimize_search_query", side_effect=AssertionError("should not optimize")):
            queries = search_tools._build_search_queries("汇川技术最近有什么好消息吗")
        self.assertGreaterEqual(len(queries), 1)
        self.assertIn("汇川技术", queries[0])

    def test_precise_finance_query_builds_multiple_template_queries(self):
        queries = search_tools._build_search_queries("汇川技术的机器人业务，最近有没有好消息")
        self.assertLessEqual(len(queries), 3)
        self.assertTrue(any("机器人业务" in query for query in queries))
        self.assertTrue(any(("最近动态" in query) or ("公告" in query) or ("财报" in query) for query in queries))
        self.assertFalse(any(query.startswith("site:") for query in queries))

    def test_precise_finance_filing_query_uses_broad_search_by_default(self):
        queries = search_tools._build_search_queries("汇川技术最近财报怎么样")
        self.assertEqual(len(queries), 2)
        joined = " ".join(queries)
        self.assertFalse(queries[0].startswith("site:"))
        self.assertIn("年度报告", joined)
        self.assertIn("一季度报告", joined)
        self.assertTrue(any(str(search_tools._current_year()) in query for query in queries))
        self.assertTrue(any(str(search_tools._current_year() - 1) in query for query in queries))
        self.assertFalse(queries[1].startswith("site:"))

    def test_authority_filing_query_can_use_site_filters(self):
        queries = search_tools._build_search_queries("汇川技术最近财报原文PDF")
        self.assertEqual(len(queries), 2)
        self.assertTrue(queries[0].startswith("site:cninfo.com.cn "))
        self.assertTrue(queries[1].startswith("site:szse.cn "))

    def test_suffixless_company_filing_query_uses_latest_template(self):
        with patch.object(search_tools, "_optimize_search_query", side_effect=AssertionError("should not optimize")):
            queries = search_tools._build_search_queries("寒武纪最近财报怎么样")
        self.assertEqual(len(queries), 2)
        self.assertFalse(any(query.startswith("site:") for query in queries))
        self.assertIn("寒武纪", queries[0])
        joined = " ".join(queries)
        self.assertIn(str(search_tools._current_year() - 1), joined)
        self.assertIn(str(search_tools._current_year()), joined)
        self.assertIn("一季度报告", joined)

    def test_filing_query_with_particle_between_recent_and_report_uses_latest_template(self):
        with patch.object(search_tools, "_optimize_search_query", side_effect=AssertionError("should not optimize")):
            queries = search_tools._build_search_queries("寒武纪最近的财报好吗")
        self.assertEqual(len(queries), 2)
        self.assertIn("寒武纪", queries[0])
        joined = " ".join(queries)
        self.assertIn(str(search_tools._current_year() - 1), joined)
        self.assertIn(str(search_tools._current_year()), joined)

    def test_explicit_historical_filing_query_does_not_force_latest(self):
        queries = search_tools._build_search_queries("汇川技术2023年年报怎么样")
        joined = " ".join(queries)
        self.assertIn("2023年年报", joined)
        self.assertNotIn("最新 定期报告", joined)

    def test_generic_query_still_uses_optimizer_and_keeps_fallback_raw_query(self):
        with patch.object(search_tools, "_optimize_search_query", return_value="铝价 走势 原因 2026年4月"):
            queries = search_tools._build_search_queries("最近的铝价格走势原因")
        self.assertEqual(queries[0], f"铝价 走势 原因 2026年4月 {search_tools._current_year()}年{search_tools.date.today().month}月")
        self.assertIn(f"最近的铝价格走势原因 {search_tools._current_year()}年{search_tools.date.today().month}月", queries)

    def test_generic_relative_time_query_adds_current_month_context(self):
        with patch.object(search_tools, "_optimize_search_query", return_value="美联储 降息预期"):
            queries = search_tools._build_search_queries("美联储最近降息预期有什么变化")
        self.assertIn(str(search_tools._current_year()), queries[0])
        self.assertIn(f"{search_tools.date.today().month}月", queries[0])

    def test_current_quarter_query_adds_current_year(self):
        with patch.object(search_tools, "_optimize_search_query", return_value="美联储 一季度 利率"):
            queries = search_tools._build_search_queries("一季度美联储利率有什么变化")
        self.assertIn(f"{search_tools._current_year()} 第一季度", queries[0])

    def test_search_web_impl_tries_multiple_queries_until_hit(self):
        with patch.dict(os.environ, _SEARCH_ENV_DEFAULTS), \
             patch.object(search_tools, "ZHIPU_API_KEY", "fake-key"), \
             patch.object(search_tools, "_build_search_queries", return_value=["q1", "q2", "q3"]), \
             patch.object(search_tools, "ZhipuAI", return_value=object()), \
             patch.object(search_tools, "_invoke_search_once", side_effect=["📭 未搜索到相关内容。", "命中结果", "不该走到第三次"]):
            answer = search_tools._search_web_impl("汇川技术最近有什么好消息吗")
        self.assertEqual(answer, "命中结果")

    def test_search_web_impl_stops_after_first_hit(self):
        with patch.dict(os.environ, _SEARCH_ENV_DEFAULTS), \
             patch.object(search_tools, "ZHIPU_API_KEY", "fake-key"), \
             patch.object(search_tools, "_build_search_queries", return_value=["q1", "q2"]), \
             patch.object(search_tools, "ZhipuAI", return_value=object()), \
             patch.object(search_tools, "_invoke_search_once", side_effect=["命中结果", "不该搜索第二次"]) as invoke:
            answer = search_tools._search_web_impl("最近的铝价格走势原因")
        self.assertEqual(answer, "命中结果")
        self.assertEqual(invoke.call_count, 1)

    def test_search_web_impl_returns_first_non_empty_fallback_when_all_miss(self):
        with patch.dict(os.environ, _SEARCH_ENV_DEFAULTS), \
             patch.object(search_tools, "ZHIPU_API_KEY", "fake-key"), \
             patch.object(search_tools, "_build_search_queries", return_value=["q1", "q2"]), \
             patch.object(search_tools, "ZhipuAI", return_value=object()), \
             patch.object(search_tools, "_invoke_search_once", side_effect=["暂未查到清晰利好", "📭 未搜索到相关内容。"]):
            answer = search_tools._search_web_impl("汇川技术最近有什么好消息吗")
        self.assertEqual(answer, "暂未查到清晰利好")

    def test_latest_filing_stale_answer_triggers_second_attempt(self):
        stale = "目前能确认的最新完整财报主要停留在2023年年报和2024年一季报阶段。建议去官网查看。"
        fresh = "最新披露包括2025年年度报告和2026年第一季度报告，公告日期为2026年4月，营业收入增长，归母净利润为正。"
        with patch.dict(os.environ, _SEARCH_ENV_DEFAULTS), \
             patch.object(search_tools, "ZHIPU_API_KEY", "fake-key"), \
             patch.object(search_tools, "_build_search_queries", return_value=["q1", "q2"]), \
             patch.object(search_tools, "ZhipuAI", return_value=object()), \
             patch.object(search_tools, "_invoke_search_once", side_effect=[stale, fresh]) as invoke:
            answer = search_tools._search_web_impl("汇川技术最近财报怎么样")
        self.assertEqual(answer, fresh)
        self.assertEqual(invoke.call_count, 2)

    def test_latest_filing_false_negative_answer_triggers_official_probe_immediately(self):
        stale = "根据搜索结果，目前无法获取汇川技术2025年度报告和2026年一季度报告的最新财务数据，因为这些报告尚未发布。"
        with patch.dict(os.environ, _SEARCH_ENV_DEFAULTS), \
             patch.object(search_tools, "ZHIPU_API_KEY", "fake-key"), \
             patch.object(search_tools, "_build_search_queries", return_value=["q1", "q2"]), \
             patch.object(search_tools, "ZhipuAI", return_value=object()), \
             patch.object(search_tools, "_invoke_search_once", return_value=stale) as invoke, \
             patch.object(search_tools, "_official_filing_probe", return_value=None) as probe:
            answer = search_tools._search_web_impl("汇川技术最近财报怎么样")
        self.assertEqual(answer, stale)
        self.assertEqual(invoke.call_count, 1)
        probe.assert_called_once_with("汇川技术最近财报怎么样")

    def test_false_negative_filing_answer_triggers_official_probe(self):
        stale = "根据搜索结果，目前无法获取寒武纪2025年度报告和2026年一季度报告的最新财务数据，因为这些报告尚未发布。"
        probe = search_tools.FilingProbeResult(
            company="寒武纪",
            reports=[
                search_tools.FilingReport(
                    title="寒武纪2026年第一季度报告",
                    date="2026-04-30",
                    url="https://static.cninfo.com.cn/finalpage/test.pdf",
                    report_type="一季报",
                )
            ],
        )
        with patch.dict(os.environ, _SEARCH_ENV_DEFAULTS), \
             patch.object(search_tools, "ZHIPU_API_KEY", "fake-key"), \
             patch.object(search_tools, "_build_search_queries", return_value=["q1", "q2"]), \
             patch.object(search_tools, "ZhipuAI", return_value=object()), \
             patch.object(search_tools, "_invoke_search_once", side_effect=[stale, "📭 未搜索到相关内容。"]) as invoke, \
             patch.object(search_tools, "_download_and_parse_filing_pdf", return_value=None) as parse_pdf, \
             patch.object(search_tools, "_official_filing_probe", return_value=probe) as probe_call:
            answer = search_tools._search_web_impl("寒武纪最近财报如何")

        probe_call.assert_called_once_with("寒武纪最近财报如何")
        parse_pdf.assert_called_once_with(probe.reports[0])
        self.assertIn("已找到最新披露文件", answer)
        self.assertIn("寒武纪2026年第一季度报告", answer)
        self.assertIn("披露日期：2026-04-30", answer)
        self.assertIn("未能在限时内解析出核心财务指标", answer)
        self.assertNotIn("未检索到足够新资料", answer)
        self.assertEqual(invoke.call_count, 2)

    def test_official_probe_metric_search_can_enrich_answer(self):
        stale = "尚未发布2026年一季度报告。"
        metric = "寒武纪2026年一季度营收同比增长，归母净利润仍为亏损，来源为一季度报告。"
        probe = search_tools.FilingProbeResult(
            company="寒武纪",
            reports=[
                search_tools.FilingReport(
                    title="寒武纪2026年第一季度报告",
                    date="2026-04-30",
                    url="https://static.cninfo.com.cn/finalpage/test.pdf",
                    report_type="一季报",
                )
            ],
        )
        with patch.dict(os.environ, _SEARCH_ENV_DEFAULTS), \
             patch.object(search_tools, "ZHIPU_API_KEY", "fake-key"), \
             patch.object(search_tools, "_build_search_queries", return_value=["q1", "q2"]), \
             patch.object(search_tools, "ZhipuAI", return_value=object()), \
             patch.object(search_tools, "_invoke_search_once", side_effect=[stale, metric]), \
             patch.object(search_tools, "_download_and_parse_filing_pdf", return_value=None), \
             patch.object(search_tools, "_official_filing_probe", return_value=probe):
            answer = search_tools._search_web_impl("寒武纪最近财报如何")

        self.assertIn("补充检索到的财务摘要", answer)
        self.assertIn(metric, answer)

    def test_official_probe_not_called_for_generic_market_query(self):
        with patch.dict(os.environ, _SEARCH_ENV_DEFAULTS), \
             patch.object(search_tools, "ZHIPU_API_KEY", "fake-key"), \
             patch.object(search_tools, "_build_search_queries", return_value=["q1", "q2"]), \
             patch.object(search_tools, "ZhipuAI", return_value=object()), \
             patch.object(search_tools, "_invoke_search_once", side_effect=["📭 未搜索到相关内容。", "暂未查到清晰结论"]), \
             patch.object(search_tools, "_official_filing_probe") as probe_call:
            answer = search_tools._search_web_impl("最近铝价走势")

        probe_call.assert_not_called()
        self.assertIn("未搜索到相关内容", answer)

    def test_official_filing_probe_parses_cninfo_announcements(self):
        current_year = search_tools._current_year()
        previous_year = current_year - 1
        payload = {
            "announcements": [
                {
                    "announcementTitle": f"寒武纪：{current_year}年第一季度报告",
                    "announcementTime": "2026-04-30",
                    "adjunctUrl": "finalpage/test-q1.pdf",
                },
                {
                    "announcementTitle": f"寒武纪：{previous_year}年年度报告",
                    "announcementTime": "2026-04-30",
                    "adjunctUrl": "finalpage/test-annual.pdf",
                },
            ]
        }
        response = Mock()
        response.json.return_value = payload
        response.raise_for_status.return_value = None
        with patch.dict(os.environ, _SEARCH_ENV_DEFAULTS), \
             patch.object(search_tools.requests, "post", return_value=response) as post:
            result = search_tools._official_filing_probe("寒武纪最近财报如何")

        self.assertIsNotNone(result)
        self.assertEqual(result.company, "寒武纪")
        self.assertEqual(len(result.reports), 2)
        self.assertTrue(result.reports[0].url.startswith("https://static.cninfo.com.cn/"))
        self.assertGreaterEqual(post.call_count, 1)

    def test_official_filing_probe_uses_resolved_stock_code_before_company_name(self):
        current_year = search_tools._current_year()
        payload = {
            "announcements": [
                {
                    "announcementTitle": f"汇川技术：{current_year}年第一季度报告",
                    "announcementTime": "2026-04-25",
                    "adjunctUrl": "finalpage/hc-q1.pdf",
                }
            ]
        }
        with patch.dict(os.environ, _SEARCH_ENV_DEFAULTS), \
             patch.object(search_tools, "_resolve_a_share_code", return_value="300124") as resolve, \
             patch.object(search_tools, "_fetch_cninfo_announcements", side_effect=[payload["announcements"]]) as fetch:
            result = search_tools._official_filing_probe("汇川技术今年第一季财报是赚钱吗")

        resolve.assert_called_once()
        fetch.assert_called_once()
        self.assertEqual(fetch.call_args.args[0], "300124")
        self.assertIsNotNone(result)
        self.assertEqual(result.reports[0].title, f"汇川技术：{current_year}年第一季度报告")

    def test_official_filing_probe_falls_back_to_company_when_code_search_has_no_report(self):
        current_year = search_tools._current_year()
        company_payload = [
            {
                "announcementTitle": f"汇川技术：{current_year}年第一季度报告",
                "announcementTime": "2026-04-25",
                "adjunctUrl": "finalpage/hc-q1.pdf",
            }
        ]
        with patch.dict(os.environ, _SEARCH_ENV_DEFAULTS), \
             patch.object(search_tools, "_resolve_a_share_code", return_value="300124"), \
             patch.object(search_tools, "_fetch_cninfo_announcements", side_effect=[[], company_payload]) as fetch:
            result = search_tools._official_filing_probe("汇川技术今年第一季财报是赚钱吗")

        self.assertEqual([call.args[0] for call in fetch.call_args_list], ["300124", "汇川技术"])
        self.assertIsNotNone(result)
        self.assertIn("第一季度报告", result.reports[0].title)

    def test_symbol_source_fallback_can_resolve_company_code_without_importing_symbol_map(self):
        with patch.object(search_tools, "_extract_company_or_query_entities", return_value=["汇川技术"]):
            code = search_tools._resolve_a_share_code_from_symbol_map_source(["汇川技术"])
        self.assertEqual(code, "300124")

    def test_parse_filing_pdf_extracts_core_metrics_and_profit_conclusion(self):
        report = search_tools.FilingReport(
            title=f"中芯国际{search_tools._current_year()}年第一季度报告",
            date="2026-05-15",
            url="https://static.cninfo.com.cn/finalpage/test.pdf",
            report_type="一季报",
        )
        text = """
        主要会计数据
        营业收入 14,731,091,404.57
        归属于上市公司股东的净利润 1,200,000,000.00
        归属于上市公司股东的扣除非经常性损益的净利润 900,000,000.00
        经营活动产生的现金流量净额 3,300,000,000.00
        基本每股收益 0.15
        """
        with patch.dict(os.environ, _SEARCH_ENV_DEFAULTS), \
             patch.object(search_tools, "_download_pdf_bytes", return_value=b"%PDF"), \
             patch.object(search_tools, "_extract_pdf_text", return_value=text):
            metrics = search_tools._download_and_parse_filing_pdf(report)

        self.assertIsNotNone(metrics)
        self.assertEqual(metrics.revenue, "14,731,091,404.57")
        self.assertEqual(metrics.net_profit_parent, "1,200,000,000.00")
        answer = search_tools._format_filing_probe_result(
            search_tools.FilingProbeResult(company="中芯国际", reports=[report]),
            metrics=metrics,
            original_query="中芯国际今年第一季财报是赚钱吗",
        )
        self.assertIn("是盈利的", answer)
        self.assertIn("营业收入", answer)
        self.assertIn("归母净利润", answer)

    def test_pdf_parse_disabled_returns_only_official_metadata(self):
        report = search_tools.FilingReport(
            title="中芯国际2026年第一季度报告",
            date="2026-05-15",
            url="https://static.cninfo.com.cn/finalpage/test.pdf",
            report_type="一季报",
        )
        with patch.dict(os.environ, {**_SEARCH_ENV_DEFAULTS, "ENABLE_FILING_PDF_PARSE": "false"}), \
             patch.object(search_tools, "_download_pdf_bytes") as download:
            metrics = search_tools._download_and_parse_filing_pdf(report)
        self.assertIsNone(metrics)
        download.assert_not_called()

    def test_tool_failure_answer_is_not_acceptable(self):
        answer = "我无法实时进行网络搜索，也无法直接访问巨潮资讯网或深交所的数据库。"
        self.assertFalse(search_tools.is_search_answer_acceptable("汇川技术最近财报怎么样", answer))

    def test_metric_intent_title_only_filing_answer_is_not_acceptable(self):
        answer = f"中芯国际{search_tools._current_year()}年第一季度报告已披露，披露日期为2026-05-15。"
        self.assertFalse(search_tools.is_search_answer_acceptable("中芯国际最近财报如何", answer))

    def test_partial_official_filing_result_is_acceptable(self):
        answer = (
            f"根据官方公告检索，已找到最新披露文件：中芯国际{search_tools._current_year()}年第一季度报告，"
            "披露日期：2026-05-15。\n"
            "已找到最新披露文件，但本轮未能在限时内解析出核心财务指标。"
        )
        self.assertTrue(search_tools.is_search_answer_acceptable("中芯国际最近财报如何", answer))

    def test_latest_filing_fresh_answer_stops_after_first_attempt(self):
        fresh = (
            f"最新披露包括{search_tools._current_year() - 1}年年度报告和{search_tools._current_year()}年第一季度报告，"
            "一季度营业收入同比增长，归母净利润为正。"
        )
        with patch.dict(os.environ, _SEARCH_ENV_DEFAULTS), \
             patch.object(search_tools, "ZHIPU_API_KEY", "fake-key"), \
             patch.object(search_tools, "_build_search_queries", return_value=["q1", "q2"]), \
             patch.object(search_tools, "ZhipuAI", return_value=object()), \
             patch.object(search_tools, "_invoke_search_once", side_effect=[fresh, "不该搜索第二次"]) as invoke:
            answer = search_tools._search_web_impl("汇川技术最近财报怎么样")
        self.assertEqual(answer, fresh)
        self.assertEqual(invoke.call_count, 1)

    def test_generic_web_search_options_use_light_defaults(self):
        with patch.dict(os.environ, _SEARCH_ENV_DEFAULTS):
            options = search_tools._build_web_search_options(
                "最近的铝价格走势原因",
                "铝价 走势 原因",
                attempt_index=0,
            )
        self.assertEqual(options["search_engine"], "search_std")
        self.assertEqual(options["count"], 4)
        self.assertEqual(options["content_size"], "medium")
        self.assertEqual(options["search_recency_filter"], "oneMonth")
        self.assertNotIn("search_domain_filter", options)
        self.assertIn("来源", options["search_prompt"])
        self.assertIn("优先检索并回答当前能找到的最新公开信息", options["search_prompt"])

    def test_deep_search_options_use_count_six_for_macro_and_concepts(self):
        with patch.dict(os.environ, _SEARCH_ENV_DEFAULTS):
            macro_options = search_tools._build_web_search_options("美联储最近降息预期有什么变化", "美联储 降息预期", attempt_index=0)
            concept_options = search_tools._build_web_search_options("低空经济概念股龙头有哪些", "低空经济 概念股 龙头", attempt_index=0)
            radar_options = search_tools._build_web_search_options("宏观风险雷达报告", "宏观 风险雷达 报告", attempt_index=0)
        self.assertEqual(macro_options["search_engine"], "search_pro")
        self.assertEqual(macro_options["count"], 6)
        self.assertEqual(macro_options["content_size"], "medium")
        self.assertEqual(concept_options["search_engine"], "search_pro")
        self.assertEqual(concept_options["count"], 6)
        self.assertEqual(radar_options["content_size"], "high")

    def test_filing_search_options_use_authority_domain_and_high_content(self):
        with patch.dict(os.environ, _SEARCH_ENV_DEFAULTS):
            options = search_tools._build_web_search_options(
                "汇川技术最近财报怎么样",
                "site:cninfo.com.cn 汇川技术 财报 公告",
                attempt_index=0,
            )
        self.assertEqual(options["count"], 6)
        self.assertEqual(options["content_size"], "high")
        self.assertEqual(options["search_recency_filter"], "oneYear")
        self.assertEqual(options["search_query"], "汇川技术 财报 公告")
        self.assertEqual(options["search_domain_filter"], "cninfo.com.cn")
        self.assertIn("最新报告期", options["search_prompt"])

    def test_second_attempt_upgrades_generic_search_to_pro(self):
        with patch.dict(os.environ, _SEARCH_ENV_DEFAULTS):
            first = search_tools._build_web_search_options("最近的铝价格走势原因", "铝价 走势 原因", attempt_index=0)
            second = search_tools._build_web_search_options("最近的铝价格走势原因", "最近的铝价格走势原因", attempt_index=1)
        self.assertEqual(first["search_engine"], "search_std")
        self.assertEqual(first["count"], 4)
        self.assertEqual(second["search_engine"], "search_pro")
        self.assertEqual(second["count"], 6)

    def test_env_overrides_model_engine_counts_and_attempts(self):
        env = {
            "ZHIPU_SEARCH_MODEL": "glm-4.7-flash",
            "ZHIPU_SEARCH_ENGINE": "search_pro",
            "ZHIPU_SEARCH_MAX_ATTEMPTS": "1",
            "ZHIPU_SEARCH_COUNT": "3",
            "ZHIPU_SEARCH_DEEP_COUNT": "5",
        }
        fake_response = Mock()
        fake_response.choices = [Mock(message=Mock(content="ok"))]
        fake_client = Mock()
        fake_client.chat.completions.create.return_value = fake_response

        with patch.dict(os.environ, env):
            options = search_tools._build_web_search_options("最近的铝价格走势原因", "铝价", attempt_index=0)
            deep_options = search_tools._build_web_search_options("低空经济概念股龙头有哪些", "低空经济 概念股 龙头", attempt_index=0)
            answer = search_tools._invoke_search_once(fake_client, original_query="最近的铝价格走势原因", search_query="铝价")
            max_attempts = search_tools._get_zhipu_search_max_attempts()

        self.assertEqual(options["search_engine"], "search_pro")
        self.assertEqual(options["count"], 3)
        self.assertEqual(deep_options["count"], 5)
        self.assertEqual(max_attempts, 1)
        self.assertEqual(answer, "ok")
        call_kwargs = fake_client.chat.completions.create.call_args.kwargs
        self.assertEqual(call_kwargs["model"], "glm-4.7-flash")
        self.assertEqual(call_kwargs["tools"][0]["web_search"]["count"], 3)


if __name__ == "__main__":
    unittest.main()
