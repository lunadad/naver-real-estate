"""사라진 매물 추적 — listing_disappearances, /api/disappeared, 크롤러 failed_scopes."""

import os
import sys
import tempfile
import unittest
from datetime import datetime, timedelta
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
sys.path.insert(0, str(Path(__file__).resolve().parent))

import crawler as crawler_module  # noqa: E402
from database import Database  # noqa: E402
from test_price_history import make_listing  # noqa: E402
from test_refactor_characterization import _FakePage, _FakePlaywright, _article  # noqa: E402


class DisappearanceDBTest(unittest.TestCase):
    def setUp(self):
        tmpdir = tempfile.TemporaryDirectory()
        self.addCleanup(tmpdir.cleanup)
        self.db = Database(db_path=os.path.join(tmpdir.name, "d.db"), skip_price_backfill=True)
        self.addCleanup(self.db.close)

    def rows(self):
        with self.db.get_connection() as conn:
            rs = conn.execute("SELECT * FROM listing_disappearances ORDER BY article_no").fetchall()
        return [dict(r) for r in rs]

    def test_listing_missing_from_next_session_is_recorded(self):
        self.db.insert_listings([make_listing("A1"), make_listing("A2")], "s1")
        self.db.insert_listings([make_listing("A1")], "s2")
        rows = self.rows()
        self.assertEqual([r["article_no"] for r in rows], ["A2"])
        r = rows[0]
        self.assertEqual(r["session_id"], "s2")
        self.assertEqual(r["district"], "강남구")
        self.assertEqual(r["building_name"], "래미안")
        self.assertEqual(r["price"], "10억")
        self.assertEqual(r["naver_url"], "https://example.com/A2")
        self.assertTrue(r["first_seen_at"])
        self.assertTrue(r["disappeared_at"])
        self.assertEqual(r["was_bargain"], 0)
        self.assertEqual(r["had_price_down"], 0)
        self.assertIsNone(r["reappeared_at"])

    def test_listing_with_null_tags_does_not_break_recording(self):
        self.db.insert_listings([make_listing("N1", tags=None)], "s1")
        self.db.insert_listings([make_listing("N2")], "s2")
        self.assertEqual([r["article_no"] for r in self.rows()], ["N1"])

    def test_nothing_recorded_on_first_session(self):
        self.db.insert_listings([make_listing("A1")], "s1")
        self.assertEqual(self.rows(), [])

    def test_was_bargain_and_had_price_down_flags(self):
        base = [make_listing(f"G{i}", "10억", building_name="힐스") for i in range(3)]
        cheap = make_listing("CHEAP", "8억", building_name="힐스")  # 중앙값 10억 대비 20% 싸다
        down_tag = make_listing("DOWN", "10억", building_name="다른단지", tags=["가격인하"])
        self.db.insert_listings(base + [cheap, down_tag], "s1")
        self.db.insert_listings(base, "s2")
        by_no = {r["article_no"]: r for r in self.rows()}
        self.assertEqual(by_no["CHEAP"]["was_bargain"], 1)
        self.assertEqual(by_no["DOWN"]["had_price_down"], 1)
        self.assertEqual(by_no["DOWN"]["was_bargain"], 0)

    def test_price_cut_in_tracking_counts_as_had_price_down(self):
        self.db.insert_listings([make_listing("A1", "10억")], "s1")
        self.db.insert_listings([make_listing("A1", "9억"), make_listing("A2")], "s2")
        self.db.insert_listings([make_listing("A2")], "s3")
        self.assertEqual(self.rows()[0]["had_price_down"], 1)

    def test_incomplete_scope_is_not_recorded(self):
        self.db.insert_listings(
            [
                make_listing("OK", district="강남구"),
                make_listing("GONE", district="강남구"),
                make_listing("FAILED", district="서초구"),
                make_listing("FAILED2", district="서초구", trade_type="전세"),
            ],
            "s1",
        )
        self.db.insert_listings(
            [make_listing("OK", district="강남구")],
            "s2",
            incomplete_scopes=[["서초구", "아파트", "매매"]],
        )
        self.assertEqual(
            sorted(r["article_no"] for r in self.rows()), ["FAILED2", "GONE"]
        )

    def test_incomplete_scopes_accept_tuples(self):
        self.db.insert_listings([make_listing("F")], "s1")
        self.db.insert_listings([make_listing("X", district="마포구")], "s2",
                                incomplete_scopes=[("강남구", "아파트", "매매")])
        self.assertEqual(self.rows(), [])

    def test_reappearance_sets_reappeared_at(self):
        self.db.insert_listings([make_listing("A1"), make_listing("A2")], "s1")
        self.db.insert_listings([make_listing("A1")], "s2")
        self.assertIsNone(self.rows()[0]["reappeared_at"])
        self.db.insert_listings([make_listing("A1"), make_listing("A2")], "s3")
        self.assertTrue(self.rows()[0]["reappeared_at"])

    def test_same_session_reinsert_does_not_mark_everything_disappeared(self):
        self.db.insert_listings([make_listing("A1"), make_listing("A2")], "s1")
        self.db.insert_listings([make_listing("A1"), make_listing("A2")], "s2")
        self.db.insert_listings([make_listing("A1"), make_listing("A2")], "s2")
        self.db.insert_listings([make_listing("A1"), make_listing("A2")], "s2")
        self.assertEqual(self.rows(), [])

    def test_same_session_reinsert_keeps_existing_records_without_duplicates(self):
        self.db.insert_listings([make_listing("A1"), make_listing("A2")], "s1")
        self.db.insert_listings([make_listing("A1")], "s2")
        self.db.insert_listings([make_listing("A1")], "s2")
        self.assertEqual([r["article_no"] for r in self.rows()], ["A2"])

    def test_disappeared_only_from_most_recent_previous_session(self):
        self.db.insert_listings([make_listing("A1"), make_listing("A2")], "s1")
        self.db.insert_listings([make_listing("A1")], "s2")
        self.db.insert_listings([make_listing("A1")], "s3")
        self.assertEqual([r["article_no"] for r in self.rows()], ["A2"])

    def test_old_disappearances_are_pruned_after_90_days(self):
        old = (datetime.now() - timedelta(days=91)).isoformat()
        with self.db.get_connection() as conn:
            conn.execute(
                "INSERT INTO listing_disappearances (article_no, session_id, disappeared_at) VALUES (?,?,?)",
                ("OLD", "s0", old),
            )
        self.db.insert_listings([make_listing("A1")], "s1")
        self.assertEqual(self.rows(), [])

    def test_large_batch_with_reappearance_is_chunked(self):
        many = [make_listing(f"B{i}") for i in range(1200)]
        self.db.insert_listings(many, "s1")
        self.db.insert_listings([], "s2")
        self.assertEqual(len(self.rows()), 1200)
        self.db.insert_listings(many, "s3")
        self.assertTrue(all(r["reappeared_at"] for r in self.rows()))


class DisappearanceAPITest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls._tmpdir = tempfile.TemporaryDirectory()
        db_path = os.path.join(cls._tmpdir.name, "dis-api.db")
        os.environ["FORCE_LOCAL_SQLITE"] = "1"
        os.environ["DATABASE_URL"] = ""
        os.environ["DB_PATH"] = db_path
        os.environ["ENABLE_SCHEDULER"] = "false"
        os.environ["SEED_DEMO_DATA"] = "false"
        import app as app_module  # noqa: E402

        cls.app_module = app_module
        app_module.db = Database(db_path=db_path, skip_price_backfill=True)
        cls.client = app_module.app.test_client()

    def setUp(self):
        with self.app_module.db.get_connection() as conn:
            for table in ("listing_disappearances", "crawl_building_stats", "crawl_history"):
                conn.execute(f"DELETE FROM {table}")

    def _dis(self, article_no, days_ago, *, was_bargain=0, had_price_down=0, reappeared=False,
             building="래미안", district="강남구", hours_ago=None):
        when = datetime.now() - (timedelta(hours=hours_ago) if hours_ago is not None else timedelta(days=days_ago))
        with self.app_module.db.get_connection() as conn:
            conn.execute(
                """INSERT INTO listing_disappearances
                (article_no, session_id, disappeared_at, region, district, building_name, property_type,
                 trade_type, area, floor, price, naver_url, first_seen_at, was_bargain, had_price_down,
                 reappeared_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (article_no, f"s-{article_no}", when.isoformat(), "서울특별시", district, building,
                 "아파트", "매매", "84A", "10/20", "9억", f"https://x/{article_no}",
                 (when - timedelta(days=5)).isoformat(), was_bargain, had_price_down,
                 when.isoformat() if reappeared else None),
            )

    def test_day_filters_and_ordering(self):
        self._dis("T", 0, hours_ago=2)
        self._dis("W", 5)
        self._dis("M", 20)
        self._dis("OLD", 45)

        def ids(days):
            body = self.client.get(f"/api/disappeared?days={days}").get_json()
            return [i["article_no"] for i in body["items"]]

        self.assertEqual(ids(1), ["T"])
        self.assertEqual(ids(7), ["T", "W"])
        self.assertEqual(ids(30), ["T", "W", "M"])
        self.assertEqual(self.client.get("/api/disappeared").get_json()["days"], 7)
        self.assertEqual(self.client.get("/api/disappeared?days=999").get_json()["days"], 30)
        self.assertEqual(self.client.get("/api/disappeared?days=abc").get_json()["days"], 7)

    def test_limit_and_item_tags(self):
        self._dis("R", 1, reappeared=True, was_bargain=1)
        self._dis("B", 2, was_bargain=1)
        self._dis("D", 3, had_price_down=1)
        self._dis("P", 4)
        body = self.client.get("/api/disappeared?days=7").get_json()
        tags = {i["article_no"]: i["tag"] for i in body["items"]}
        self.assertEqual(tags, {"R": "철회 후 재등록", "B": "진짜 급매였음", "D": "인하 후 소진", "P": "급매"})
        self.assertEqual(body["count"], 4)
        limited = self.client.get("/api/disappeared?days=7&limit=2").get_json()
        self.assertEqual([i["article_no"] for i in limited["items"]], ["R", "B"])
        item = body["items"][0]
        for key in ("district", "building_name", "area", "floor", "price", "naver_url", "disappeared_at",
                    "days_listed"):
            self.assertIn(key, item)
        self.assertEqual(item["days_listed"], 5)

    def _stats(self, session_id, days_ago, rows):
        when = (datetime.now() - timedelta(days=days_ago)).replace(microsecond=0)
        total = sum(c for _, _, c in rows)
        with self.app_module.db.get_connection() as conn:
            conn.execute(
                "INSERT INTO crawl_history (session_id, crawled_at, total_count, urgent_count, status, source) "
                "VALUES (?,?,?,?,?,?)",
                (session_id, when.isoformat(), total, total, "success", "naver"),
            )
            for district, name, count in rows:
                conn.execute(
                    "INSERT INTO crawl_building_stats (session_id, region, district, building_name, "
                    "total_count, price_down_count, created_at) VALUES (?,?,?,?,?,?,?)",
                    (session_id, "서울특별시", district, name, count, 0, when.isoformat()),
                )

    def test_fast_complexes_orders_by_drop_ratio(self):
        self._stats("old", 7, [("강남구", "A", 10), ("강남구", "B", 4), ("서초구", "C", 8),
                               ("송파구", "D", 5), ("마포구", "E", 6), ("용산구", "F", 9),
                               ("종로구", "G", 3)])
        self._stats("new", 0, [("강남구", "A", 5), ("강남구", "B", 1), ("서초구", "C", 6),
                               ("송파구", "D", 5), ("마포구", "E", 3), ("용산구", "F", 1),
                               ("종로구", "G", 4)])
        body = self.client.get("/api/disappeared/fast-complexes?days=7").get_json()
        names = [i["building_name"] for i in body["items"]]
        # 감소율: F 0.889, B 0.75, E 0.5, A 0.5, C 0.25 (동률은 감소 폭 큰 순)
        self.assertEqual(names, ["F", "B", "A", "E", "C"])
        top = body["items"][0]
        self.assertEqual((top["prev_count"], top["count"], top["drop"]), (9, 1, 8))
        self.assertAlmostEqual(top["drop_ratio"], 8 / 9, places=3)
        self.assertEqual(len(body["items"]), 5)

    def test_fast_complexes_empty_without_comparison_session(self):
        self._stats("only", 0, [("강남구", "A", 5)])
        body = self.client.get("/api/disappeared/fast-complexes?days=7").get_json()
        self.assertEqual(body["items"], [])

    def test_index_links_to_new_pages_and_scripts(self):
        html = self.client.get("/").get_data(as_text=True)
        self.assertIn('href="/disappeared"', html)
        self.assertIn("price-history.js", html)

    def test_pages_render(self):
        res = self.client.get("/disappeared")
        self.assertEqual(res.status_code, 200)
        self.assertIn("disappeared.js", res.get_data(as_text=True))


class CrawlerFailedScopesTest(unittest.TestCase):
    TWO = {
        "서울특별시": {
            "districts": {
                "강남구": {"code": "111", "lat": 37.5, "lng": 127.0},
                "서초구": {"code": "222", "lat": 37.4, "lng": 127.2},
            }
        }
    }

    def _crawl(self, responses, regions):
        page = _FakePage(responses)
        crawler = crawler_module.NaverRealEstateCrawler(db=mock.Mock())
        with mock.patch.object(crawler_module, "PLAYWRIGHT_AVAILABLE", True), \
                mock.patch.object(crawler_module, "sync_playwright", create=True,
                                  return_value=_FakePlaywright(page)), \
                mock.patch.object(crawler_module.time, "sleep"), \
                mock.patch.object(type(crawler), "REGIONS", regions):
            crawler._crawl_with_playwright()
        return crawler

    def test_failed_combo_is_reported_with_korean_names(self):
        crawler = self._crawl(
            {("111", "OPST", "B2", 1): {"ok": False, "status": 401}}, self.TWO
        )
        self.assertEqual(crawler.last_crawl_stats["failed_scopes"], [["강남구", "오피스텔", "월세"]])
        self.assertEqual(crawler.last_crawl_stats["failed_combos"], 1)

    def test_no_failures_gives_empty_list(self):
        crawler = self._crawl({("111", "APT", "A1", 1): {"articleList": [_article(1)], "isMoreData": False}},
                              self.TWO)
        self.assertEqual(crawler.last_crawl_stats["failed_scopes"], [])

    def test_unattempted_combos_after_abort_are_listed(self):
        responses = {
            (c, p, t, 1): {"ok": False, "status": 401}
            for c in ("111", "222") for p in ("APT", "OPST", "VL") for t in ("A1", "B1", "B2")
        }
        crawler = self._crawl(responses, self.TWO)
        scopes = crawler.last_crawl_stats["failed_scopes"]
        self.assertEqual(len(scopes), 18)
        self.assertIn(["서초구", "빌라/연립", "월세"], scopes)
        self.assertEqual(len({tuple(s) for s in scopes}), 18)

    def test_persist_live_crawl_passes_failed_scopes_to_insert_listings(self):
        db = mock.Mock()
        db.get_last_successful_live_crawl.return_value = None
        crawler = crawler_module.NaverRealEstateCrawler(db)
        listing = crawler._to_listing({"articleNo": "9", "tagList": []}, "서울특별시", "강남구",
                                      {"lat": 1, "lng": 2}, "APT", "A1")

        def fake_crawl():
            crawler.last_crawl_stats = {
                "failed_combos": 1, "total_combos": 100,
                "failed_scopes": [["서초구", "아파트", "매매"]],
            }
            return [listing]

        with mock.patch.object(crawler, "_crawl_with_playwright", side_effect=fake_crawl):
            result = crawler.crawl_all()
        self.assertEqual(result["status"], "success")
        _, kwargs = db.insert_listings.call_args
        self.assertEqual(kwargs["incomplete_scopes"], [["서초구", "아파트", "매매"]])


if __name__ == "__main__":
    unittest.main()
