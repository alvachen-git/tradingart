import os
import unittest

os.environ.setdefault("DB_USER", "test")
os.environ.setdefault("DB_PASSWORD", "test")
os.environ.setdefault("DB_HOST", "127.0.0.1")
os.environ.setdefault("DB_PORT", "3306")
os.environ.setdefault("DB_NAME", "test")

import data_engine


class ProfileAssetNormalizationTest(unittest.TestCase):
    def test_accepts_llm_list_assets(self):
        self.assertEqual(
            data_engine._normalize_focus_asset_items(["英伟达", "A股,美光", "未知", ["中证1000", "商品"]]),
            ["英伟达", "A股", "美光", "中证1000", "商品"],
        )

    def test_accepts_legacy_string_assets(self):
        self.assertEqual(
            data_engine._normalize_focus_asset_items("黄金, 白银，螺纹钢、无"),
            ["黄金", "白银", "螺纹钢"],
        )


if __name__ == "__main__":
    unittest.main()
