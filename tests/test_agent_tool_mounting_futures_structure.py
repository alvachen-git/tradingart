import agent_core


NEW_TOOL_NAMES = {
    "get_futures_basis_profile",
    "get_futures_inventory_receipt_profile",
    "get_futures_delivery_tospot_profile",
}


def _tool_name_set(tools):
    names = set()
    for tool in tools:
        names.add(getattr(tool, "name", getattr(tool, "__name__", str(tool))))
    return names


def test_monitor_tools_include_futures_structure_tools():
    names = _tool_name_set(agent_core.build_monitor_tools())
    assert NEW_TOOL_NAMES.issubset(names)


def test_generalist_tools_include_futures_structure_tools():
    names = _tool_name_set(agent_core.build_generalist_tools())
    assert NEW_TOOL_NAMES.issubset(names)


def test_chatter_tools_include_futures_structure_tools():
    names = _tool_name_set(agent_core.build_chatter_tools())
    assert NEW_TOOL_NAMES.issubset(names)


def test_strategist_tools_excludes_futures_structure_tools():
    names = _tool_name_set(agent_core.build_strategist_tools())
    assert NEW_TOOL_NAMES.isdisjoint(names)
