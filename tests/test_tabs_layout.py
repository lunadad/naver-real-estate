"""데스크톱 탭형 레이아웃: 첫 화면은 히어로가 있는 목록 탭, 언제든 기존 화면으로 복구 가능."""

import os
import re
import sys
import tempfile
import unittest
import unittest.mock
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "tests"))

from test_mobile_more_menu import _Collector  # noqa: E402

INDEX = (ROOT / "templates" / "index.html").read_text(encoding="utf-8")
APP_JS = (ROOT / "static" / "js" / "app.js").read_text(encoding="utf-8")
CSS = (ROOT / "static" / "css" / "style.css").read_text(encoding="utf-8")


def _elements(html=INDEX):
    parser = _Collector()
    parser.feed(html)
    return parser.elements


def _by_id(elem_id, html=INDEX):
    return [(t, a, anc) for t, a, anc in _elements(html) if a.get("id") == elem_id]


class TabNavMarkupTest(unittest.TestCase):
    def test_tab_nav_has_four_tabs_with_list_selected(self):
        tabs = [a for t, a, anc in _elements() if "tab-nav" in anc and a.get("role") == "tab"]
        self.assertEqual([a.get("data-tab") for a in tabs], ["list", "trend", "map", "alert"])
        selected = [a.get("data-tab") for a in tabs if a.get("aria-selected") == "true"]
        self.assertEqual(selected, ["list"])

    def test_restore_buttons_exist_in_both_layouts(self):
        to_classic = _by_id("btn-layout-classic")
        to_tabs = _by_id("btn-layout-tabs")
        self.assertEqual(len(to_classic), 1)
        self.assertEqual(len(to_tabs), 1)
        self.assertIn("tab-nav", to_classic[0][2])
        self.assertIn("app-header", to_tabs[0][2])

    def test_tag_filter_slot_sits_in_main_before_listings(self):
        slot = _by_id("tabs-tag-slot")
        self.assertEqual(len(slot), 1)
        self.assertIn("main-content", slot[0][2])
        self.assertLess(INDEX.index('id="tabs-tag-slot"'), INDEX.index('id="listings-toolbar"'))

    def test_hero_stays_first_in_main(self):
        main_start = INDEX.index('id="main-content"')
        self.assertLess(INDEX.index('id="hero-panel"', main_start), INDEX.index('id="map-wrap"', main_start))

    def test_layout_is_applied_before_first_paint(self):
        head = INDEX[: INDEX.index("</head>")]
        self.assertIn("data-layout", head)
        self.assertIn("default_layout", head)


class LayoutDefaultRouteTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls._tmp = tempfile.TemporaryDirectory()
        os.environ["FORCE_LOCAL_SQLITE"] = "1"
        os.environ["DATABASE_URL"] = ""
        os.environ["DB_PATH"] = os.path.join(cls._tmp.name, "layout.db")
        os.environ["ENABLE_SCHEDULER"] = "false"
        os.environ["SEED_DEMO_DATA"] = "false"
        import app as app_module  # noqa: E402

        cls.app_module = app_module
        cls.client = app_module.app.test_client()

    def _page(self):
        return self.client.get("/").get_data(as_text=True)

    def test_default_layout_is_tabs(self):
        with unittest.mock.patch.dict(os.environ, {}, clear=False):
            os.environ.pop("DEFAULT_LAYOUT", None)
            self.assertIn('var DEFAULT_LAYOUT = "tabs"', self._page())

    def test_env_can_restore_classic_for_everyone(self):
        with unittest.mock.patch.dict(os.environ, {"DEFAULT_LAYOUT": "classic"}):
            self.assertIn('var DEFAULT_LAYOUT = "classic"', self._page())

    def test_unknown_env_value_falls_back_to_tabs(self):
        with unittest.mock.patch.dict(os.environ, {"DEFAULT_LAYOUT": "<script>"}):
            page = self._page()
            self.assertIn('var DEFAULT_LAYOUT = "tabs"', page)
            self.assertNotIn('"<script>"', page)


class TabsBehaviourWiringTest(unittest.TestCase):
    def test_query_override_and_switch_buttons_persist_choice(self):
        head = INDEX[: INDEX.index("</head>")]
        self.assertIn("get('layout')", head)
        self.assertRegex(APP_JS, r"localStorage\.setItem\('layout'")

    def test_js_relayouts_map_when_map_tab_opens(self):
        body = APP_JS[APP_JS.index("function setTab"):]
        body = body[: body.index("\n}\n")]
        self.assertIn("relayout", body)

    def test_trend_click_returns_to_list_tab(self):
        block = APP_JS[APP_JS.index("// Trend / region lists"):]
        block = block[: block.index("});\n  });")]
        self.assertIn("setTab('list')", block)

    def test_tabs_css_is_desktop_only(self):
        rules = re.findall(r"@media \(min-width: 901px\)[^{]*\{", CSS)
        self.assertTrue(rules, "탭 레이아웃 CSS는 데스크톱 미디어쿼리 안에만 있어야 한다")
        outside = re.sub(r"@media \(min-width: 901px\)[^{]*\{.*?\n\}\n", "", CSS, flags=re.S)
        self.assertNotRegex(outside, r'html\[data-layout="tabs"\][^{]*#sidebar')


if __name__ == "__main__":
    import unittest.mock  # noqa: F401

    unittest.main()
