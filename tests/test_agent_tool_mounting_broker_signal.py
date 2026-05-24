import unittest

import agent_core


def _tool_name_set(tools):
    names = set()
    for tool in tools:
        names.add(getattr(tool, "name", getattr(tool, "__name__", str(tool))))
    return names


class BrokerSignalToolMountingTest(unittest.TestCase):
    def test_generalist_tools_include_broker_signal_tool(self):
        names = _tool_name_set(agent_core.build_generalist_tools())
        self.assertIn("get_futures_broker_position_signal", names)
        self.assertIn("get_futures_broker_group_position_moves", names)
        self.assertIn("get_futures_broker_indicator_profile", names)

    def test_monitor_tools_include_broker_signal_tool(self):
        names = _tool_name_set(agent_core.build_monitor_tools())
        self.assertIn("get_futures_broker_position_signal", names)
        self.assertIn("get_futures_broker_group_position_moves", names)
        self.assertIn("get_futures_broker_indicator_profile", names)

    def test_strategist_tools_exclude_broker_signal_tool(self):
        names = _tool_name_set(agent_core.build_strategist_tools())
        self.assertNotIn("get_futures_broker_position_signal", names)
        self.assertNotIn("get_futures_broker_group_position_moves", names)
        self.assertNotIn("get_futures_broker_indicator_profile", names)


if __name__ == "__main__":
    unittest.main()
