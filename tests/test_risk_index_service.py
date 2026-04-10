from datetime import datetime, timedelta, timezone
import unittest

import risk_index_service as svc

BEIJING_TZ = timezone(timedelta(hours=8))


class TestRiskIndexService(unittest.TestCase):
    def test_normalize_probability_supports_percent_and_decimal(self):
        self.assertAlmostEqual(svc.normalize_probability(57), 0.57, places=6)
        self.assertAlmostEqual(svc.normalize_probability(0.57), 0.57, places=6)
        self.assertAlmostEqual(svc.normalize_probability("12.5"), 0.125, places=6)

    def test_liquidity_factor_is_clamped(self):
        self.assertGreaterEqual(svc.calc_liquidity_factor(0), 0.6)
        self.assertLessEqual(svc.calc_liquidity_factor(1_000_000_000), 1.2)

    def test_cap_category_scores_is_passthrough_in_wci_v1(self):
        values = {"military_conflict": 0.90, "economic_crisis": 0.10}
        capped, low_diversity = svc.cap_category_scores(values)
        self.assertFalse(low_diversity)
        self.assertEqual(capped, values)

    def test_relevant_event_filter_prefers_geopolitics_and_economy_tags(self):
        geopolitics_event = {
            "title": "US strike on Cuba by December 31?",
            "tags": [{"slug": "geopolitics"}, {"slug": "military-strikes"}],
        }
        economy_event = {
            "title": "Will the U.S. default on Treasury debt in 2026?",
            "tags": [{"slug": "economy"}, {"slug": "sovereign-debt"}],
        }
        sports_event = {
            "title": "2026 FIFA World Cup Winner",
            "tags": [{"slug": "sports"}, {"slug": "soccer"}],
        }
        self.assertTrue(svc._is_relevant_polymarket_event(geopolitics_event))
        self.assertTrue(svc._is_relevant_polymarket_event(economy_event))
        self.assertFalse(svc._is_relevant_polymarket_event(sports_event))

    def test_dynamic_conflict_candidate_detects_interstate_markets(self):
        candidate = {
            "market_title": "China x Japan military clash before 2027?",
            "event_title": "China x Japan military clash before 2027?",
            "market_slug": "china-x-japan-military-clash-before-2027",
            "event_slug": "china-x-japan-military-clash-before-2027",
        }
        self.assertTrue(svc._is_dynamic_conflict_candidate(candidate, now=datetime(2026, 4, 6, 12, 0, tzinfo=BEIJING_TZ)))
        self.assertEqual(set(svc._detect_candidate_countries(candidate)), {"CHN", "JPN"})

    def test_watchlist_hits_israel_turkey_conflict(self):
        event = {
            "title": "Israel x Turkey military clash before 2027?",
            "description": "",
            "slug": "israel-x-turkey-military-clash-before-2027",
            "tags": [{"slug": "geopolitics"}],
        }
        hits = set(svc._event_watch_hits(event))
        self.assertIn("ISR_TUR", hits)

    def test_event_priority_prefers_watchlist_hits(self):
        watched = {"volume24hr": 50_000, "_watch_hits": ["ISR_TUR"]}
        unwatched = {"volume24hr": 500_000, "_watch_hits": []}
        self.assertGreater(svc._event_priority_tuple(watched), svc._event_priority_tuple(unwatched))

    def test_dynamic_country_weight_prioritizes_usa_and_china(self):
        self.assertGreater(svc._dynamic_country_weight(["USA", "CUB"]), svc._dynamic_country_weight(["ISR", "TUR"]))
        self.assertGreater(svc._dynamic_country_weight(["CHN", "JPN"]), svc._dynamic_country_weight(["ISR", "TUR"]))
        self.assertGreater(svc._dynamic_country_weight(["RUS", "UKR"]), svc._dynamic_country_weight(["ISR", "TUR"]))

    def test_detect_candidate_countries_does_not_false_match_us_in_words(self):
        candidate = {
            "market_title": "Will Israel conduct military action in Greater Beirut on April 6, 2026?",
            "event_title": "Israel military action",
            "market_slug": "will-israel-conduct-military-action-in-greater-beirut-on-april-6-2026",
            "event_slug": "israel-military-action",
        }
        detected = set(svc._detect_candidate_countries(candidate))
        self.assertIn("ISR", detected)
        self.assertNotIn("USA", detected)

    def test_market_theme_key_collapses_date_variants(self):
        a = {"event_slug": "us-forces-enter-iran-by", "market_title": "US forces enter Iran by April 30?"}
        b = {"event_slug": "us-forces-enter-iran-by", "market_title": "US forces enter Iran by December 31?"}
        self.assertEqual(svc._market_theme_key(a), svc._market_theme_key(b))

    def test_dynamic_conflict_candidate_excludes_ceasefire_and_visit_style_markets(self):
        ceasefire = {
            "market_title": "US x Iran ceasefire by December 31?",
            "event_title": "US x Iran ceasefire by December 31?",
            "market_slug": "us-x-iran-ceasefire-by-december-31",
            "event_slug": "us-x-iran-ceasefire-by-december-31",
        }
        visit = {
            "market_title": "Will Donald Trump visit North Korea in 2026?",
            "event_title": "Will Donald Trump visit North Korea in 2026?",
            "market_slug": "will-donald-trump-visit-north-korea-in-2026",
            "event_slug": "which-countries-will-donald-trump-visit-in-2026",
        }
        self.assertFalse(svc._is_dynamic_conflict_candidate(ceasefire, now=datetime(2026, 4, 6, 12, 0, tzinfo=BEIJING_TZ)))
        self.assertFalse(svc._is_dynamic_conflict_candidate(visit, now=datetime(2026, 4, 6, 12, 0, tzinfo=BEIJING_TZ)))

    def test_dynamic_conflict_candidate_ignores_description_only_war_language(self):
        candidate = {
            "market_title": "US recognizes Russian sovereignty over Ukraine before 2027?",
            "event_title": "US recognizes Russian sovereignty over Ukraine before 2027?",
            "market_slug": "us-recognizes-russian-sovereignty-over-ukraine-before-2027",
            "event_slug": "us-recognizes-russian-sovereignty-over-ukraine-before-2027",
            "description": "This geopolitical market references war, attack and conflict outcomes in the background text.",
        }
        self.assertFalse(svc._is_dynamic_conflict_candidate(candidate, now=datetime(2026, 4, 6, 12, 0, tzinfo=BEIJING_TZ)))

    def test_dedupe_scored_markets_keeps_best_theme_once(self):
        items = [
            {"event_slug": "us-forces-enter-iran-by", "market_title": "US forces enter Iran by April 30?", "event_raw": 1.0, "is_dynamic_conflict": False},
            {"event_slug": "us-forces-enter-iran-by", "market_title": "US forces enter Iran by December 31?", "event_raw": 0.8, "is_dynamic_conflict": True},
        ]
        deduped = svc._dedupe_scored_markets(items)
        self.assertEqual(len(deduped), 1)
        self.assertEqual(deduped[0]["event_raw"], 1.0)

    def test_scoring_dedupe_key_collapses_same_pair_conflicts(self):
        a = {"pair_tag": "IRN_USA", "country_codes": ["IRN", "USA"], "event_raw": 0.5}
        b = {"pair_tag": "USA_IRN", "country_codes": ["USA", "IRN"], "event_raw": 0.7}
        self.assertEqual(svc._scoring_dedupe_key(a), svc._scoring_dedupe_key(b))

    def test_localized_dynamic_title_prefers_chinese(self):
        candidate = {
            "market_title": "US strike on Cuba by December 31?",
            "event_title": "US strike on Cuba by December 31?",
            "market_slug": "us-strike-on-cuba-by-december-31",
            "event_slug": "us-strike-on-cuba-by",
        }
        title = svc._localized_dynamic_title(candidate, ["USA", "CUB"])
        self.assertIn("美国", title)
        self.assertIn("古巴", title)

    def test_conditional_outcome_market_is_classified_and_localized_correctly(self):
        candidate = {
            "market_title": "Will the Iranian regime survive U.S. military strikes?",
            "event_title": "Will the Iranian regime survive U.S. military strikes?",
            "market_slug": "will-the-iranian-regime-survive-us-military-strikes",
            "event_slug": "will-the-iranian-regime-survive-us-military-strikes",
        }
        countries = svc._detect_candidate_countries(candidate)
        semantics = svc._dynamic_market_semantics(candidate, countries)
        title = svc._localized_dynamic_title(candidate, countries, market_semantics=semantics)
        self.assertEqual(semantics, "conditional_outcome")
        self.assertEqual(title, "美国打击伊朗后的政权存续风险")

    def test_direct_conflict_market_remains_direct_semantics(self):
        candidate = {
            "market_title": "Israel x Turkey military clash before 2027?",
            "event_title": "Israel x Turkey military clash before 2027?",
            "market_slug": "israel-x-turkey-military-clash-before-2027",
            "event_slug": "israel-x-turkey-military-clash-before-2027",
        }
        countries = svc._detect_candidate_countries(candidate)
        semantics = svc._dynamic_market_semantics(candidate, countries)
        self.assertEqual(semantics, "direct_conflict")

    def test_conditional_outcome_market_is_downweighted_in_dynamic_conflicts(self):
        candidates = [
            {
                "market_title": "Will the Iranian regime survive U.S. military strikes?",
                "event_title": "Will the Iranian regime survive U.S. military strikes?",
                "market_slug": "will-the-iranian-regime-survive-us-military-strikes",
                "event_slug": "will-the-iranian-regime-survive-us-military-strikes",
                "volume24hr": 300000,
                "outcomePrices": [0.90, 0.10],
            },
            {
                "market_title": "US strike on Cuba by December 31?",
                "event_title": "US strike on Cuba by December 31?",
                "market_slug": "us-strike-on-cuba-by-december-31",
                "event_slug": "us-strike-on-cuba-by-december-31",
                "volume24hr": 300000,
                "outcomePrices": [0.90, 0.10],
            },
        ]
        items = svc._build_dynamic_conflict_markets(candidates, set(), now=datetime(2026, 4, 9, 10, 0, tzinfo=BEIJING_TZ))
        by_slug = {item["event_slug"]: item for item in items}
        conditional_item = by_slug["will-the-iranian-regime-survive-us-military-strikes"]
        direct_item = by_slug["us-strike-on-cuba-by-december-31"]
        self.assertEqual(conditional_item["market_semantics"], "conditional_outcome")
        self.assertEqual(direct_item["market_semantics"], "direct_conflict")
        self.assertAlmostEqual(conditional_item["semantics_weight_multiplier"], 0.35, places=6)
        self.assertLess(conditional_item["event_raw"], direct_item["event_raw"])

    def test_conditional_outcome_explanation_mentions_consequence_pricing(self):
        item = {
            "event_key": "dynamic::test",
            "display_title": "美国打击伊朗后的政权存续风险",
            "probability": 0.90,
            "delta_24h": 0.02,
            "market_semantics": "conditional_outcome",
        }
        explanation, status = svc._make_explanation_result(item, use_external_news=True)
        self.assertEqual(status, "fallback")
        self.assertIn("冲突后果", explanation["one_line_reason"])
        self.assertIn("不等于冲突本身发生概率", explanation["one_line_reason"])

    def test_multi_outcome_market_uses_target_bucket_probability(self):
        event = {
            "event_key": "oil_price_spike_extreme",
            "display_title": "原油冲上120美元风险",
            "category": "economic_crisis",
            "region_tag": "global",
            "pair_tag": "GLOBAL_OIL",
            "impact_weight": 0.46,
            "query_keywords": ["crude oil", "wti"],
            "market_structure": "multi_outcome_range",
            "target_outcome_mode": "threshold_gte",
            "target_outcome_keywords": ["120"],
            "fallback_if_outcome_missing": "skip_scoring",
        }
        candidate = {
            "market_title": "What will WTI Crude Oil (WTI) hit in April 2026?",
            "event_title": "What will WTI Crude Oil (WTI) hit in April 2026?",
            "market_slug": "wti-crude-oil-hit-april-2026",
            "event_slug": "wti-crude-oil-hit-april-2026",
            "outcomes": ["↑ $140", "↑ $130", "↑ $120", "↓ $90", "↓ $80"],
            "outcomePrices": [0.13, 0.23, 0.38, 0.64, 0.28],
        }
        resolved = svc._resolve_target_outcome_selection(candidate, event)
        self.assertIsNotNone(resolved)
        self.assertEqual(resolved["outcome_label"], "↑ $120")
        self.assertAlmostEqual(svc.extract_probability_from_market(candidate, event=event), 0.38, places=6)

    def test_select_representative_market_prefers_target_bucket_submarket(self):
        event = {
            "event_key": "oil_price_spike_extreme",
            "display_title": "原油冲上120美元风险",
            "category": "economic_crisis",
            "region_tag": "global",
            "pair_tag": "GLOBAL_OIL",
            "impact_weight": 0.46,
            "query_keywords": ["crude oil", "wti"],
            "must_contain_any": ["crude oil"],
            "market_structure": "multi_outcome_range",
            "target_outcome_mode": "threshold_gte",
            "target_outcome_keywords": ["120"],
            "fallback_if_outcome_missing": "skip_scoring",
        }
        candidates = [
            {
                "groupItemTitle": "↓ $90",
                "market_title": "What will WTI Crude Oil (WTI) hit in April 2026?",
                "event_title": "What will WTI Crude Oil (WTI) hit in April 2026?",
                "market_slug": "wti-under-90-april-2026",
                "event_slug": "wti-crude-oil-hit-april-2026",
                "outcomePrices": [0.64, 0.36],
            },
            {
                "groupItemTitle": "↑ $120",
                "market_title": "What will WTI Crude Oil (WTI) hit in April 2026?",
                "event_title": "What will WTI Crude Oil (WTI) hit in April 2026?",
                "market_slug": "wti-above-120-april-2026",
                "event_slug": "wti-crude-oil-hit-april-2026",
                "outcomePrices": [0.38, 0.62],
            },
        ]
        selected = svc.select_representative_market(event, candidates)
        self.assertIsNotNone(selected)
        self.assertEqual(selected["groupItemTitle"], "↑ $120")

    def test_multi_outcome_market_without_target_is_skipped(self):
        event = {
            "event_key": "x",
            "display_title": "Test",
            "category": "economic_crisis",
            "region_tag": "global",
            "pair_tag": "X",
            "impact_weight": 1.0,
            "query_keywords": ["wti crude oil"],
            "market_structure": "multi_outcome_range",
            "fallback_if_outcome_missing": "skip_scoring",
        }
        candidate = {
            "market_title": "What will WTI Crude Oil (WTI) hit in April 2026?",
            "event_title": "What will WTI Crude Oil (WTI) hit in April 2026?",
            "market_slug": "wti-crude-oil-hit-april-2026",
            "event_slug": "wti-crude-oil-hit-april-2026",
            "outcomes": ["↑ $140", "↑ $130", "↑ $120", "↓ $90"],
            "outcomePrices": [0.13, 0.23, 0.38, 0.64],
        }
        self.assertIsNone(svc.select_representative_market(event, [candidate]))

    def test_multi_outcome_explanation_mentions_target_bucket(self):
        item = {
            "event_key": "oil_price_spike_extreme",
            "display_title": "原油冲上120美元风险",
            "probability": 0.38,
            "delta_24h": -0.14,
            "market_structure": "multi_outcome_range_market",
            "target_outcome_label": "↑ $120",
            "category": "economic_crisis",
        }
        explanation, status = svc._make_explanation_result(item, use_external_news=True)
        self.assertEqual(status, "fallback")
        self.assertIn("120", explanation["one_line_reason"])
        self.assertIn("回落", explanation["one_line_reason"])

    def test_country_detection_ignores_description_side_context(self):
        candidate = {
            "market_title": "Will Israel take military action in Gaza on April 5, 2026?",
            "event_title": "Israel military action against Gaza on...?",
            "market_slug": "will-israel-take-military-action-in-gaza-on-april-5-2026",
            "event_slug": "israel-military-action-against-gaza-on",
            "description": 'This market references foreign statements but does not mention the United States in the title.',
        }
        detected = set(svc._detect_candidate_countries(candidate))
        self.assertEqual(detected, {"ISR"})

    def test_select_representative_market_prefers_allowlist(self):
        event = {
            "event_key": "x",
            "display_title": "Test",
            "category": "military_conflict",
            "region_tag": "global",
            "pair_tag": "X_Y",
            "impact_weight": 1.0,
            "query_keywords": ["test keyword"],
            "event_slug_allowlist": ["event-b"],
            "market_slug_allowlist": ["market-a"],
            "active": True,
        }
        candidates = [
            {"market_slug": "market-b", "event_slug": "event-b", "market_title": "other", "volume24hr": 999999},
            {"market_slug": "market-a", "event_slug": "event-a", "market_title": "other", "volume24hr": 1},
        ]
        selected = svc.select_representative_market(event, candidates)
        self.assertEqual(selected["market_slug"], "market-a")

    def test_select_representative_market_filters_expired_title_windows(self):
        event = {
            "event_key": "saudi_iran_regional_escalation",
            "display_title": "Gulf Regional Escalation",
            "category": "military_conflict",
            "region_tag": "middle_east",
            "pair_tag": "GULF_REGIONAL",
            "impact_weight": 0.68,
            "query_keywords": ["middle east war", "gulf war"],
            "event_slug_allowlist": ["middle-east"],
            "market_slug_allowlist": ["middle-east"],
            "must_contain_any_group": [["saudi", "iran"], ["middle east", "war"], ["gulf", "war"]],
            "must_contain_any": ["middle east", "gulf", "saudi", "iran"],
            "active": True,
        }
        candidates = [
            {
                "market_slug": "will-iran-strike-saudi-arabia-in-march",
                "event_slug": "middle-east-war",
                "market_title": "Will Iran strike Saudi Arabia in March?",
                "event_title": "Middle East escalation",
                "volume24hr": 500000,
            }
        ]
        selected = svc.select_representative_market(
            event,
            candidates,
            now=datetime(2026, 4, 6, 12, 0, tzinfo=BEIJING_TZ),
        )
        self.assertIsNone(selected)

    def test_select_representative_market_requires_any_group(self):
        event = {
            "event_key": "us_china_direct_clash",
            "display_title": "US China Direct Clash",
            "category": "military_conflict",
            "region_tag": "east_asia",
            "pair_tag": "USA_CHN",
            "impact_weight": 0.95,
            "query_keywords": ["U.S. China war", "South China Sea war"],
            "event_slug_allowlist": ["usa-china", "south-china-sea"],
            "market_slug_allowlist": ["usa-china", "south-china-sea"],
            "must_contain_any_group": [["usa", "china"], ["american", "china"], ["south china sea"]],
            "must_contain_any": ["war", "clash", "conflict", "attack", "military"],
            "exclude_keywords": ["taiwan", "visit", "trade", "tariff", "deal"],
            "active": True,
        }
        candidates = [
            {
                "market_slug": "will-china-invade-taiwan-by-june-30-2027",
                "event_slug": "taiwan-crisis",
                "market_title": "Will China invade Taiwan by June 30, 2027?",
                "event_title": "Taiwan crisis",
                "volume24hr": 500000,
            }
        ]
        selected = svc.select_representative_market(event, candidates)
        self.assertIsNone(selected)

    def test_select_representative_market_ignores_description_leakage(self):
        event = {
            "event_key": "russia_nato_direct_clash",
            "display_title": "俄与北约直接冲突",
            "category": "military_conflict",
            "region_tag": "europe",
            "pair_tag": "RUS_NATO",
            "impact_weight": 1.0,
            "query_keywords": ["Russia NATO war", "Article 5 conflict"],
            "event_slug_allowlist": ["russia-nato", "article-5", "nato-russia"],
            "market_slug_allowlist": ["russia-nato", "article-5", "nato-russia"],
            "must_contain_any_group": [["russia", "nato"], ["article 5", "russia"], ["moscow", "nato"]],
            "must_contain_any": ["war", "clash", "attack", "strike", "article 5", "conflict"],
            "exclude_keywords": ["security guarantee", "recognizes sovereignty", "ceasefire", "visit", "meeting", "guarantee"],
            "active": True,
        }
        candidates = [
            {
                "market_slug": "nothing-ever-happens-2026",
                "event_slug": "nothing-ever-happens-2026",
                "market_title": "Nothing Ever Happens: 2026",
                "event_title": "Nothing Ever Happens: 2026",
                "description": "A meta market mentioning war, article 5 conflict, Russia and NATO in the background copy.",
                "volume24hr": 500000,
            }
        ]
        selected = svc.select_representative_market(
            event,
            candidates,
            now=datetime(2026, 4, 6, 12, 0, tzinfo=BEIJING_TZ),
        )
        self.assertIsNone(selected)

    def test_build_ongoing_baseline_uses_reverse_markets(self):
        candidates = [
            {
                "market_slug": "russia-x-ukraine-ceasefire-by-june-30-2026",
                "event_slug": "russia-x-ukraine-ceasefire-by-june-30-2026",
                "market_title": "Russia x Ukraine ceasefire by June 30, 2026?",
                "event_title": "Russia x Ukraine ceasefire by June 30, 2026?",
                "volume24hr": 200000,
                "outcomePrices": [0.22, 0.78],
            },
            {
                "market_slug": "russia-x-ukraine-ceasefire-before-2027",
                "event_slug": "russia-x-ukraine-ceasefire-before-2027",
                "market_title": "Russia x Ukraine ceasefire by end of 2026?",
                "event_title": "Russia x Ukraine ceasefire by end of 2026?",
                "volume24hr": 150000,
                "outcomePrices": [0.41, 0.59],
            },
        ]
        clusters, baseline = svc._build_ongoing_baseline(candidates, datetime(2026, 4, 6, 12, 0, tzinfo=BEIJING_TZ))
        self.assertTrue(clusters)
        self.assertGreater(baseline, 0.0)
        self.assertEqual(clusters[0]["cluster_key"], "ru_ua_war")

    def test_build_risk_snapshot_combines_baseline_and_escalation(self):
        candidates = [
            {
                "market_slug": "russia-x-ukraine-ceasefire-by-june-30-2026",
                "event_slug": "russia-x-ukraine-ceasefire-by-june-30-2026",
                "market_title": "Russia x Ukraine ceasefire by June 30, 2026?",
                "event_title": "Russia x Ukraine ceasefire by June 30, 2026?",
                "volume24hr": 200000,
                "outcomePrices": [0.20, 0.80],
            },
            {
                "market_slug": "us-x-iran-ceasefire-by-june-30-2026",
                "event_slug": "us-x-iran-ceasefire-by-june-30-2026",
                "market_title": "US x Iran ceasefire by June 30?",
                "event_title": "US x Iran ceasefire by June 30?",
                "volume24hr": 180000,
                "outcomePrices": [0.18, 0.82],
            },
            {
                "market_slug": "will-us-force-enter-iran-by-april-30",
                "event_slug": "us-forces-enter-iran-by",
                "market_title": "US forces enter Iran by April 30?",
                "event_title": "Middle East escalation",
                "volume24hr": 320000,
                "oneDayPriceChange": 0.08,
                "outcomePrices": [0.62, 0.38],
                "source_url": "https://example.com/a",
            },
        ]
        snapshot = svc.build_risk_snapshot(candidates, previous_snapshot={"score_display": 20}, use_news_explainer=False, now=datetime(2026, 4, 6, 12, 0, tzinfo=BEIJING_TZ))
        components = snapshot["source_status"]["score_components"]
        self.assertGreater(components["ongoing_baseline"], 0.0)
        self.assertGreater(components["escalation_pressure"], 0.0)
        self.assertIn("ongoing_clusters", snapshot["source_status"])
        self.assertGreater(snapshot["score_raw"], components["ongoing_baseline"])

    def test_build_risk_snapshot_skips_news_explainer_on_low_delta(self):
        candidates = [
            {
                "market_slug": "russia-x-ukraine-ceasefire-by-june-30-2026",
                "event_slug": "russia-x-ukraine-ceasefire-by-june-30-2026",
                "market_title": "Russia x Ukraine ceasefire by June 30, 2026?",
                "event_title": "Russia x Ukraine ceasefire by June 30, 2026?",
                "volume24hr": 200000,
                "outcomePrices": [0.20, 0.80],
            },
            {
                "market_slug": "will-us-force-enter-iran-by-april-30",
                "event_slug": "us-forces-enter-iran-by",
                "market_title": "US forces enter Iran by April 30?",
                "event_title": "Middle East escalation",
                "volume24hr": 320000,
                "oneDayPriceChange": 0.04,
                "outcomePrices": [0.62, 0.38],
                "source_url": "https://example.com/a",
            },
        ]
        original_helper = svc._make_explanation_result
        helper_flags = []
        try:
            def _fake_helper(item, use_external_news):
                helper_flags.append(bool(use_external_news))
                return (
                    {
                        "event_key": item.get("event_key", "x"),
                        "one_line_reason": "fallback",
                        "source_links": [],
                    },
                    "fallback",
                )

            svc._make_explanation_result = _fake_helper
            snapshot = svc.build_risk_snapshot(
                candidates,
                previous_snapshot={"score_display": 20},
                use_news_explainer=True,
                now=datetime(2026, 4, 6, 12, 0, tzinfo=BEIJING_TZ),
            )
            self.assertTrue(helper_flags)
            self.assertTrue(all(flag is False for flag in helper_flags))
            self.assertEqual(snapshot["source_status"]["news_explainer"], "skipped_low_delta")
        finally:
            svc._make_explanation_result = original_helper

    def test_build_risk_snapshot_runs_news_explainer_on_large_delta(self):
        candidates = [
            {
                "market_slug": "russia-x-ukraine-ceasefire-by-june-30-2026",
                "event_slug": "russia-x-ukraine-ceasefire-by-june-30-2026",
                "market_title": "Russia x Ukraine ceasefire by June 30, 2026?",
                "event_title": "Russia x Ukraine ceasefire by June 30, 2026?",
                "volume24hr": 200000,
                "outcomePrices": [0.20, 0.80],
            },
            {
                "market_slug": "will-us-force-enter-iran-by-april-30",
                "event_slug": "us-forces-enter-iran-by",
                "market_title": "US forces enter Iran by April 30?",
                "event_title": "Middle East escalation",
                "volume24hr": 320000,
                "oneDayPriceChange": 0.06,
                "outcomePrices": [0.62, 0.38],
                "source_url": "https://example.com/a",
            },
        ]
        original_helper = svc._make_explanation_result
        helper_flags = []
        try:
            def _fake_helper(item, use_external_news):
                helper_flags.append(bool(use_external_news))
                return (
                    {
                        "event_key": item.get("event_key", "x"),
                        "one_line_reason": "external",
                        "source_links": [],
                    },
                    "external" if use_external_news else "fallback",
                )

            svc._make_explanation_result = _fake_helper
            snapshot = svc.build_risk_snapshot(
                candidates,
                previous_snapshot={"score_display": 20},
                use_news_explainer=True,
                now=datetime(2026, 4, 6, 12, 0, tzinfo=BEIJING_TZ),
            )
            self.assertTrue(helper_flags)
            self.assertTrue(all(flag is True for flag in helper_flags))
            self.assertEqual(snapshot["source_status"]["news_explainer"], "ok")
        finally:
            svc._make_explanation_result = original_helper

    def test_refresh_falls_back_to_previous_snapshot_on_fetch_failure(self):
        original_fetch = svc.fetch_polymarket_candidates
        original_latest = svc.get_latest_geopolitical_risk_snapshot
        try:
            svc.fetch_polymarket_candidates = lambda **_: (_ for _ in ()).throw(RuntimeError("boom"))
            svc.get_latest_geopolitical_risk_snapshot = lambda engine: {
                "score_raw": 55.0,
                "score_display": 57.0,
                "stale": False,
                "source_status": {"polymarket": "ok"},
            }
            snapshot = svc.refresh_geopolitical_risk_snapshot(engine=None, persist=False)
            self.assertTrue(snapshot["stale"])
            self.assertIn("error:RuntimeError", snapshot["source_status"]["polymarket"])
        finally:
            svc.fetch_polymarket_candidates = original_fetch
            svc.get_latest_geopolitical_risk_snapshot = original_latest


if __name__ == "__main__":
    unittest.main()
