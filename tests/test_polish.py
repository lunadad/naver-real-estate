"""미뤄 둔 작은 개선점 회귀 테스트."""

import os
import sys
import tempfile
import unittest
import urllib.error
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "tests"))

import telegram_commands as cmds  # noqa: E402
from database import Database  # noqa: E402
from deal_query import parse_deal_args  # noqa: E402
from test_bargain import SEED, listing_row  # noqa: E402

DISTRICTS = [{"region": "서울특별시", "district": "송파구"}]


class ApiInputTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls._tmp = tempfile.TemporaryDirectory()
        db_path = os.path.join(cls._tmp.name, "api.db")
        os.environ["FORCE_LOCAL_SQLITE"] = "1"
        os.environ["DATABASE_URL"] = ""
        os.environ["DB_PATH"] = db_path
        os.environ["ENABLE_SCHEDULER"] = "false"
        os.environ["SEED_DEMO_DATA"] = "false"
        import app as app_module  # noqa: E402

        cls.app_module = app_module
        cls._original_db = app_module.db
        app_module.db = Database(db_path=db_path, skip_price_backfill=True)
        app_module.db.insert_listings(SEED, "s1")
        app_module.db.log_crawl("s1", len(SEED), len(SEED), "success", "naver")
        cls.client = app_module.app.test_client()

    @classmethod
    def tearDownClass(cls):
        cls.app_module.db = cls._original_db

    def test_huge_max_price_is_rejected_with_400(self):
        response = self.client.post(
            "/api/alert-rules", json={"client_id": "p1", "bargain_only": True, "max_price": 10 ** 12}
        )
        self.assertEqual(response.status_code, 400)

    def test_huge_max_price_query_is_ignored_not_500(self):
        response = self.client.get(f"/api/listings?max_price={10 ** 20}")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.get_json()["total"], len(SEED))

    def test_bargain_only_false_strings_are_false(self):
        for value in ("false", "0", "no", "off", "", None, False):
            response = self.client.post(
                "/api/alert-rules", json={"client_id": "p2", "district": "강남구", "bargain_only": value}
            )
            self.assertEqual(response.status_code, 200, value)
            self.assertEqual(response.get_json()["rule"]["bargain_only"], 0, value)
        for value in ("true", "1", "on", True, 1):
            response = self.client.post(
                "/api/alert-rules", json={"client_id": "p2", "bargain_only": value}
            )
            self.assertEqual(response.get_json()["rule"]["bargain_only"], 1, value)


def http_404(*_args, **_kwargs):
    raise urllib.error.HTTPError("https://x", 404, "Not Found", {}, None)


class BotErrorMessageTest(unittest.TestCase):
    def test_weekly_404_says_no_crawl_yet(self):
        self.assertIn("아직 크롤링 데이터가 없습니다", cmds.handle_weekly("", fetch=http_404))

    def test_complex_404_says_no_data(self):
        def fetch(path, params=None):
            if path == "/api/listings":
                return {"listings": [{"district": "송파구", "building_name": "헬리오시티"}]}
            http_404()

        self.assertIn("단지 데이터를 찾지 못했습니다", cmds.handle_complex("헬리오시티", fetch=fetch))

    def test_network_error_keeps_offline_message(self):
        def offline(*_a, **_k):
            raise urllib.error.URLError("offline")

        self.assertEqual(cmds.handle_weekly("", fetch=offline), cmds.OFFLINE_REPLY)


class AmbiguousNumberTest(unittest.TestCase):
    def test_bare_small_number_is_flagged(self):
        parsed = parse_deal_args("송파 15", DISTRICTS)
        self.assertIsNone(parsed["max_price"])
        self.assertEqual(parsed["ambiguous"], ["15"])
        self.assertEqual(parse_deal_args("송파 9000", DISTRICTS)["max_price"], 9000)
        self.assertEqual(parse_deal_args("송파 15억 20%", DISTRICTS)["ambiguous"], [])

    def test_deals_and_dealalert_explain_ambiguity(self):
        reply = cmds.handle_deals("송파 15", fetch=lambda *a, **k: {"listings": []}, districts=DISTRICTS)
        self.assertIn("'15'", reply)
        self.assertIn("15%", reply)
        self.assertIn("15억", reply)
        store = mock.Mock()
        reply = cmds.handle_dealalert("add 송파 15", store=store, districts=DISTRICTS)
        self.assertIn("'15'", reply)
        store.add.assert_not_called()


class DemoInsertSkipsTrackingTest(unittest.TestCase):
    def test_record_tracking_false_writes_no_tracking_rows(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        db = Database(db_path=os.path.join(tmp.name, "d.db"), skip_price_backfill=True)
        self.addCleanup(db.close)
        db.insert_listings([listing_row("R1", 50000, "5억")], "real")
        db.insert_listings([listing_row("D1", 50000, "5억")], "demo", record_tracking=False)
        with db.get_connection() as conn:
            disappeared = conn.execute("SELECT COUNT(*) AS n FROM listing_disappearances").fetchone()["n"]
            tracked = [r["article_no"] for r in conn.execute("SELECT article_no FROM listing_tracking").fetchall()]
        self.assertEqual(disappeared, 0)
        self.assertEqual(tracked, ["R1"])

    def test_crawler_degraded_path_passes_record_tracking_false(self):
        import crawler as crawler_module

        db = mock.Mock()
        crawler = crawler_module.NaverRealEstateCrawler(db)
        with mock.patch.object(crawler, "_crawl_with_playwright", return_value=[]), \
                mock.patch.object(crawler, "generate_demo_data", return_value=[{"article_no": "D"}]), \
                mock.patch.dict(os.environ, {"ALLOW_DEMO_FALLBACK": "true"}):
            crawler.crawl_all()
        self.assertEqual(db.insert_listings.call_args.kwargs.get("record_tracking"), False)


if __name__ == "__main__":
    unittest.main()
