import unittest

import option_delta_tools as odt


class OptionDeltaRiskProfileTest(unittest.TestCase):
    def test_profile_memory_risk_words_map_to_delta_bands(self):
        conservative = odt.get_delta_target_band(trend_signal="看涨", risk_preference="偏保守")
        aggressive = odt.get_delta_target_band(trend_signal="看跌", risk_preference="偏激进")

        self.assertEqual(conservative["risk_key"], "conservative")
        self.assertEqual(aggressive["risk_key"], "aggressive")
        self.assertEqual(aggressive["low"], -2.0)
        self.assertEqual(aggressive["high"], -0.6)

    def test_negated_conservative_text_maps_to_aggressive(self):
        aggressive = odt.get_delta_target_band(
            trend_signal="看涨",
            risk_preference="我的风险偏好是积极，不是保守",
        )

        self.assertEqual(aggressive["risk_key"], "aggressive")


if __name__ == "__main__":
    unittest.main()
