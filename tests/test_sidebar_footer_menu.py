import sys
import types
import unittest


fake_streamlit = sys.modules.get("streamlit")
if fake_streamlit is None:
    fake_streamlit = types.ModuleType("streamlit")
    sys.modules["streamlit"] = fake_streamlit

if not hasattr(fake_streamlit, "context"):
    fake_streamlit.context = types.SimpleNamespace(headers={})
if not hasattr(fake_streamlit, "switch_page"):
    fake_streamlit.switch_page = lambda path: None
if not hasattr(fake_streamlit, "error"):
    fake_streamlit.error = lambda msg: None
if not hasattr(fake_streamlit, "dialog"):
    fake_streamlit.dialog = lambda *args, **kwargs: (lambda fn: fn)

if "streamlit.components" not in sys.modules:
    fake_components_pkg = types.ModuleType("streamlit.components")
    sys.modules["streamlit.components"] = fake_components_pkg

if "streamlit.components.v1" not in sys.modules:
    fake_components_v1 = types.ModuleType("streamlit.components.v1")
    fake_components_v1.html = lambda *args, **kwargs: None
    sys.modules["streamlit.components.v1"] = fake_components_v1

import sidebar_footer_menu as footer_menu


class TestSidebarFooterMenu(unittest.TestCase):
    def test_resolve_scheme_prefers_http_for_localhost(self):
        self.assertEqual(footer_menu._resolve_scheme("localhost:8501", ""), "http")
        self.assertEqual(footer_menu._resolve_scheme("127.0.0.1:8501", ""), "http")

    def test_resolve_scheme_prefers_forwarded_proto_when_present(self):
        self.assertEqual(footer_menu._resolve_scheme("www.aiprota.com", "https"), "https")
        self.assertEqual(footer_menu._resolve_scheme("example.com", "http"), "http")

    def test_build_invite_link_uses_root_path(self):
        url = footer_menu._build_invite_link("http://localhost:8501", "AIBE123456", "mike0919", preview_mode=False)
        self.assertEqual(url, "http://localhost:8501/?invite=AIBE123456")


if __name__ == "__main__":
    unittest.main()
