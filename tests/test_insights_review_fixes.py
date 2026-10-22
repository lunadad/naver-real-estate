"""최종 검토에서 나온 문제의 회귀 테스트."""

import asyncio
import importlib.util
import os
import sys
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "tests"))

import telegram_commands as cmds  # noqa: E402
from database import Database  # noqa: E402
from deal_query import parse_deal_args  # noqa: E402
from telegram_alerts import TelegramRuleStore, dispatch_telegram_deal_alerts  # noqa: E402
from test_bargain import listing_row  # noqa: E402

DISTRICTS = [
    {"region": "서울특별시", "district": "중구"},
    {"region": "부산광역시", "district": "중구"},
    {"region": "서울특별시", "district": "송파구"},
]
NOW = datetime(2026, 10, 23, 9, 40)


class WeeklyReportClockTest(unittest.TestCase):
    def test_db_naive_now_uses_kst_wall_clock_with_postgres(self):
        import app as app_module

        with mock.patch.object(app_module, "DATABASE_URL", "postgresql://x"):
            value = app_module.db_naive_now()
        kst_now = datetime.now(timezone(timedelta(hours=9))).replace(tzinfo=None)
        self.assertIsNone(value.tzinfo)
        self.assertLess(abs((value - kst_now).total_seconds()), 5)

    def test_weekly_report_route_passes_db_clock(self):
        import app as app_module

        marker = datetime(2026, 10, 23, 10, 0)
        fake_db = mock.Mock()
        fake_db.get_weekly_report_data.return_value = None
        with mock.patch.object(app_module, "db_naive_now", return_value=marker), \
                mock.patch.object(app_module, "db", fake_db):
            app_module.app.test_client().get("/api/weekly-report")
        fake_db.get_weekly_report_data.assert_called_once_with(marker)


class ComplexRatioTest(unittest.TestCase):
    def test_complex_reply_shows_fraction_as_percent(self):
        def fetch(path, params=None):
            if path == "/api/listings":
                return {"listings": [{"district": "송파구", "building_name": "헬리오시티"}]}
            return {"stats": {"current_count": 7, "week_ago_count": 7, "bargain_count": 1, "price_down_ratio": 2 / 7}}

        self.assertIn("가격인하 비율 28.6%", cmds.handle_complex("헬리오시티", fetch=fetch))


class RegionDisambiguationTest(unittest.TestCase):
    def test_region_token_narrows_same_named_district(self):
        parsed = parse_deal_args("부산 중구 10억", DISTRICTS)
        self.assertEqual((parsed["region"], parsed["district"], parsed["max_price"]), ("부산광역시", "중구", 100000))
        self.assertEqual(parse_deal_args("중구 부산", DISTRICTS)["region"], "부산광역시")
        self.assertEqual(parse_deal_args("중구", DISTRICTS)["region"], "서울특별시")

    def test_deals_sends_region(self):
        calls = []

        def fetch(path, params=None):
            calls.append(params)
            return {"listings": []}

        cmds.handle_deals("부산 중구", fetch=fetch, districts=DISTRICTS)
        self.assertEqual((calls[0]["region"], calls[0]["district"]), ("부산광역시", "중구"))

    def test_dispatch_passes_rule_region(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        store = TelegramRuleStore(Path(tmp.name) / "r.json")
        store.add(district="중구", region="부산광역시", now=NOW)
        db = mock.Mock()
        db.get_listings.return_value = {"listings": []}
        dispatch_telegram_deal_alerts(db, store, lambda t: True, now=NOW)
        self.assertEqual(db.get_listings.call_args.kwargs["region"], "부산광역시")


def bargain_row(no, discount):
    return {"article_no": no, "district": "송파구", "building_name": "A", "price": "1억",
            "price_sort_value": 10000, "bargain_discount": discount, "naver_url": ""}


class DealAlertStoreTest(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.path = Path(tmp.name) / "r.json"

    def test_failed_send_is_queued_once_and_not_resent_as_new(self):
        store = TelegramRuleStore(self.path)
        store.add(district="송파구", now=NOW)
        db = mock.Mock()
        db.get_listings.return_value = {"listings": [bargain_row("1", 0.2)]}
        self.assertEqual(len(dispatch_telegram_deal_alerts(db, store, lambda t: False, now=NOW)), 1)
        sent = []
        dispatch_telegram_deal_alerts(db, TelegramRuleStore(self.path), lambda t: sent.append(t) or True, now=NOW)
        self.assertEqual(sent, [])

    def test_mark_sent_keeps_rules_added_by_another_process(self):
        crawler_view = TelegramRuleStore(self.path)
        crawler_view.add(district="송파구", now=NOW)
        TelegramRuleStore(self.path).add(district="중구", region="부산광역시", now=NOW)  # Hermes 쪽
        crawler_view.mark_sent(1, ["x"], NOW)
        self.assertEqual([r["district"] for r in TelegramRuleStore(self.path).list()], ["송파구", "중구"])

    def test_remove_keeps_sent_marks_written_by_another_process(self):
        hermes_view = TelegramRuleStore(self.path)
        hermes_view.add(district="송파구", now=NOW)
        hermes_view.add(district="중구", now=NOW)
        TelegramRuleStore(self.path).mark_sent(1, ["x"], NOW)  # 크롤러 쪽
        hermes_view.remove(2)
        rule = TelegramRuleStore(self.path).list()[0]
        self.assertIn("x", rule["sent"])


class PluginImportFailureTest(unittest.TestCase):
    def test_broken_import_returns_error_reply_instead_of_raising(self):
        spec = importlib.util.spec_from_file_location(
            "re_plugin", ROOT / "integrations" / "hermes" / "real-estate" / "__init__.py"
        )
        plugin = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(plugin)
        handlers = {}
        plugin.register(type("Ctx", (), {"register_command": lambda self, n, h, **k: handlers.__setitem__(n, h)})())
        with mock.patch.dict(sys.modules, {"telegram_commands": None}):
            self.assertEqual(asyncio.run(handlers["deals"]("송파")), plugin.ERROR_REPLY)


class MinDiscountPrecisionTest(unittest.TestCase):
    def test_listing_exactly_at_threshold_matches_float4_stored_rule(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        db = Database(db_path=os.path.join(tmp.name, "p.db"), skip_price_backfill=True)
        self.addCleanup(db.close)
        rule = db.create_alert_rule("c1", bargain_only=True, min_discount=0.15)
        with db.get_connection() as conn:  # Postgres REAL(float4) 반올림 오차 재현
            conn.execute("UPDATE alert_rules SET min_discount = ? WHERE id = ?", (0.15000000596, rule["id"]))
        rows = [listing_row(f"Q{i}", v, f"{v}") for i, v in enumerate([40000, 40000, 40000, 34000])]
        db.insert_listings(rows, "s1")
        db.log_crawl("s1", 4, 4, "success", "naver")
        self.assertEqual([m["article_no"] for m in db.get_new_alert_matches("c1")], ["Q3"])


class FastComplexMinimumTest(unittest.TestCase):
    def test_tiny_complexes_are_not_ranked(self):
        from insights.tracking import fast_complexes

        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        db = Database(db_path=os.path.join(tmp.name, "f.db"), skip_price_backfill=True)
        self.addCleanup(db.close)
        base = datetime(2026, 10, 22, 9)
        with db.get_connection() as conn:
            for sid, at in (("old", base - timedelta(days=7)), ("new", base)):
                conn.execute(
                    "INSERT INTO crawl_history (session_id, crawled_at, total_count, urgent_count, status, source) "
                    "VALUES (?, ?, 1, 1, 'success', 'naver')", (sid, at.isoformat()))
            for sid, name, total in (("old", "작은", 2), ("new", "작은", 0), ("old", "큰", 20), ("new", "큰", 12)):
                conn.execute(
                    "INSERT INTO crawl_building_stats (session_id, region, district, building_name, total_count, price_down_count, created_at) "
                    "VALUES (?, '서울특별시', '송파구', ?, ?, 0, ?)", (sid, name, total, base.isoformat()))
            names = [i["building_name"] for i in fast_complexes(conn, 7)["items"]]
        self.assertEqual(names, ["큰"])


if __name__ == "__main__":
    unittest.main()
