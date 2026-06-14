import unittest

import agent_core


def _tool_name_set(tools):
    names = set()
    for tool in tools:
        names.add(getattr(tool, "name", getattr(tool, "__name__", str(tool))))
    return names


class IVScannerToolMountingTest(unittest.TestCase):
    def test_generalist_tools_include_iv_scanner(self):
        self.assertIn("scan_iv_change_ranking", _tool_name_set(agent_core.build_generalist_tools()))

    def test_monitor_tools_include_iv_scanner(self):
        self.assertIn("scan_iv_change_ranking", _tool_name_set(agent_core.build_monitor_tools()))

    def test_strategist_tools_include_iv_scanner(self):
        self.assertIn("scan_iv_change_ranking", _tool_name_set(agent_core.build_strategist_tools()))


if __name__ == "__main__":
    unittest.main()
