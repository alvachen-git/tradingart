import unittest

import expiry_option_generator as gen


class ExpiryOptionSymbolLabelTest(unittest.TestCase):
    def test_etf_exchange_suffix_is_not_treated_as_commodity_symbol(self):
        sections = [
            {"underlying": "510050.SH", "name": "上证50ETF", "option_type": "ETF期权"},
            {"underlying": "510300.SH", "name": "沪深300ETF", "option_type": "ETF期权"},
            {"underlying": "510500.SH", "name": "中证500ETF", "option_type": "ETF期权"},
        ]
        html = "\n".join(
            [
                '<h2 class="section-title">🌐 510050.SH（上证50ETF）</h2>',
                '<h2 class="section-title">🌐 510300.SH（沪深300ETF）</h2>',
                '<h2 class="section-title">📊 510500.SH（中证500ETF）</h2>',
            ]
        )

        fixed = gen.enforce_symbol_label_consistency(html, sections)
        fixed = gen.enforce_section_title_symbol_order(fixed, sections)

        self.assertNotIn("510050.烧碱", fixed)
        self.assertNotIn("510300.烧碱", fixed)
        self.assertNotIn("510500.烧碱", fixed)
        self.assertNotIn("（SH）", fixed)
        self.assertIn("上证50ETF（510050.SH）", fixed)
        self.assertIn("沪深300ETF（510300.SH）", fixed)
        self.assertIn("中证500ETF（510500.SH）", fixed)

    def test_standalone_sh_commodity_symbol_still_normalizes(self):
        html = '<h2 class="section-title">🌱 SH（其他名称）</h2>'

        fixed = gen.enforce_section_title_symbol_order(html, [])

        self.assertIn("烧碱（SH）", fixed)


if __name__ == "__main__":
    unittest.main()
