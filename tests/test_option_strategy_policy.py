import unittest

from option_strategy_policy import (
    build_option_strategy_policy,
    classify_dte,
    classify_iv_rank,
    normalize_option_risk_preference,
)


def _all_rules(policy):
    return "\n".join(policy.hard_rules + policy.preferred_strategies + policy.avoid_strategies)


class OptionStrategyPolicyTest(unittest.TestCase):
    def test_normalize_risk_preference_accepts_profile_memory_words(self):
        self.assertEqual(normalize_option_risk_preference("偏保守"), "conservative")
        self.assertEqual(normalize_option_risk_preference("偏积极"), "aggressive")
        self.assertEqual(
            normalize_option_risk_preference("", profile_context="- 风险偏好：偏激进"),
            "aggressive",
        )

    def test_profile_context_overrides_stale_risk_field(self):
        self.assertEqual(
            normalize_option_risk_preference("偏保守", profile_context="- 风险偏好：偏积极"),
            "aggressive",
        )

    def test_current_user_risk_override_handles_negated_conservative(self):
        self.assertEqual(
            normalize_option_risk_preference(
                "偏保守",
                profile_context="- 风险偏好：偏保守",
                user_query="我的风险偏好是积极，不是保守",
            ),
            "aggressive",
        )

    def test_iv_and_dte_tiers(self):
        self.assertEqual(classify_iv_rank(19.9), "ultra_low")
        self.assertEqual(classify_iv_rank(20), "low")
        self.assertEqual(classify_iv_rank(70), "neutral")
        self.assertEqual(classify_iv_rank(70.1), "high")
        self.assertEqual(classify_dte(3), "expiry")
        self.assertEqual(classify_dte(7), "ultra_short")
        self.assertEqual(classify_dte(45), "regular")
        self.assertEqual(classify_dte(46), "far")

    def test_conservative_expiry_does_not_allow_buying_expiry_options(self):
        policy = build_option_strategy_policy(
            risk_preference="偏保守",
            user_query="还剩3天到期，我能不能买认购",
            days_to_expiry=3,
        )
        rules = _all_rules(policy)
        self.assertIn("偏保守默认禁止买末日期权", rules)
        self.assertIn("买末日/超短期期权", rules)

    def test_conservative_seller_must_be_otm_or_limited_risk(self):
        policy = build_option_strategy_policy(risk_preference="偏保守", user_query="我想卖期权")
        rules = _all_rules(policy)
        self.assertIn("偏保守卖期权必须偏虚值", rules)
        self.assertIn("优先风险有限结构", rules)
        self.assertIn("裸卖平值/实值期权", rules)

    def test_range_conservative_can_short_strangle_unless_iv_rank_too_low(self):
        policy = build_option_strategy_policy(
            risk_preference="偏保守",
            user_query="行情震荡，没有突破，我适合做什么",
            iv_rank=25,
        )
        rules = _all_rules(policy)
        self.assertIn("震荡行情可考虑双卖", rules)
        self.assertIn("震荡双卖只允许偏虚值", rules)
        self.assertNotIn("不建议偏保守客户做双卖", rules)

    def test_range_conservative_avoids_short_strangle_when_iv_rank_below_20(self):
        policy = build_option_strategy_policy(
            risk_preference="偏保守",
            user_query="行情震荡，没有突破，我适合做什么",
            iv_rank=15,
        )
        rules = _all_rules(policy)
        self.assertIn("不建议偏保守客户做双卖", rules)
        self.assertNotIn("震荡行情可考虑双卖", rules)

    def test_range_aggressive_can_sell_shallow_otm(self):
        policy = build_option_strategy_policy(
            risk_preference="偏积极",
            user_query="行情震荡，没有明显趋势",
            iv_rank=18,
        )
        rules = _all_rules(policy)
        self.assertIn("震荡行情可考虑双卖", rules)
        self.assertIn("浅虚值", rules)

    def test_aggressive_breakout_far_dte_allows_directional_long_convexity(self):
        policy = build_option_strategy_policy(
            risk_preference="偏激进",
            user_query="行情突破了，到期还远，能不能用方向性买方策略？",
            days_to_expiry=60,
        )
        rules = _all_rules(policy)
        self.assertIn("方向性买方或高凸性买方", rules)
        self.assertIn("失效快、胜率低、仓位要轻", rules)
        self.assertIn("不得只用价差策略替代回答", rules)

    def test_aggressive_breakout_far_dte_high_iv_keeps_directional_long_as_satellite(self):
        policy = build_option_strategy_policy(
            risk_preference="偏积极",
            user_query="行情突破了，到期还远，能不能买较虚值期权？",
            iv_rank=80,
            days_to_expiry=60,
        )
        rules = _all_rules(policy)
        self.assertIn("方向性买方作小仓位卫星", rules)
        self.assertIn("不能把激进突破远期的方向性买方完全否定", rules)
        self.assertIn("默认单腿追高买权", rules)

    def test_far_dte_high_iv_prefers_short_or_spread(self):
        policy = build_option_strategy_policy(
            risk_preference="稳健型",
            user_query="趋势看多，远月期权怎么做",
            iv_rank=75,
            days_to_expiry=60,
        )
        rules = _all_rules(policy)
        self.assertIn("DTE大于45天且IV高时，优先顺势卖方或价差", rules)
        self.assertIn("默认单腿追高买权", rules)

    def test_high_iv_does_not_default_to_single_leg_long_options(self):
        policy = build_option_strategy_policy(
            risk_preference="偏积极",
            user_query="IV很高但方向看多，怎么做",
        )
        rules = _all_rules(policy)
        self.assertIn("IV偏高时优先价差", rules)
        self.assertIn("默认单腿追高买权", rules)

    def test_text_infers_far_dte_for_breakout_very_otm_rule(self):
        policy = build_option_strategy_policy(
            risk_preference="偏激进",
            user_query="行情突破了，到期还远，能不能买很虚值认购？",
        )
        rules = _all_rules(policy)
        self.assertIn("方向性买方或高凸性买方", rules)


if __name__ == "__main__":
    unittest.main()
