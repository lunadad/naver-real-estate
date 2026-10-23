"""모바일 하단 '더보기' 메뉴: 헤더 오른쪽이 숨겨지는 모바일에서도 히트맵·사라진 매물·테마 전환에 갈 수 있어야 한다."""

import re
import unittest
from html.parser import HTMLParser
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
INDEX = (ROOT / "templates" / "index.html").read_text(encoding="utf-8")
APP_JS = (ROOT / "static" / "js" / "app.js").read_text(encoding="utf-8")
CSS = (ROOT / "static" / "css" / "style.css").read_text(encoding="utf-8")


class _Collector(HTMLParser):
    """요소마다 (태그, 속성, 조상 id 목록)을 모은다."""

    VOID = {"meta", "link", "img", "input", "br", "hr", "line", "circle", "path"}

    def __init__(self):
        super().__init__()
        self.stack = []
        self.elements = []

    def handle_starttag(self, tag, attrs):
        attrs = dict(attrs)
        ancestors = [a.get("id") for _, a in self.stack if a.get("id")]
        self.elements.append((tag, attrs, ancestors))
        if tag not in self.VOID:
            self.stack.append((tag, attrs))

    def handle_endtag(self, tag):
        for i in range(len(self.stack) - 1, -1, -1):
            if self.stack[i][0] == tag:
                del self.stack[i:]
                break


def _elements():
    parser = _Collector()
    parser.feed(INDEX)
    return parser.elements


class MobileMoreMenuTest(unittest.TestCase):
    def test_quickbar_has_more_button_controlling_menu(self):
        buttons = [a for t, a, anc in _elements() if a.get("id") == "btn-mobile-more-fab"]
        self.assertEqual(len(buttons), 1)
        btn = buttons[0]
        self.assertEqual(btn.get("aria-controls"), "mobile-more-menu")
        self.assertEqual(btn.get("aria-expanded"), "false")
        ancestors = [anc for t, a, anc in _elements() if a.get("id") == "btn-mobile-more-fab"][0]
        self.assertIn("mobile-quickbar", ancestors)

    def test_menu_links_to_heatmap_disappeared_and_theme(self):
        inside = [(t, a) for t, a, anc in _elements() if "mobile-more-menu" in anc]
        hrefs = {a.get("href") for t, a in inside if t == "a"}
        self.assertIn("/heatmap", hrefs)
        self.assertIn("/disappeared", hrefs)
        ids = {a.get("id") for t, a in inside}
        self.assertIn("btn-mobile-theme", ids)

    def test_menu_starts_hidden(self):
        menu = [a for t, a, anc in _elements() if a.get("id") == "mobile-more-menu"][0]
        self.assertIn("hidden", (menu.get("class") or "").split())

    def test_app_js_wires_more_menu_and_theme(self):
        self.assertIn("btn-mobile-more-fab", APP_JS)
        self.assertIn("btn-mobile-theme", APP_JS)
        self.assertIn("aria-expanded", APP_JS)

    def test_quickbar_grid_fits_four_buttons(self):
        self.assertRegex(CSS, re.compile(r"#mobile-quickbar\s*\{[^}]*repeat\(4,", re.S))


if __name__ == "__main__":
    unittest.main()
