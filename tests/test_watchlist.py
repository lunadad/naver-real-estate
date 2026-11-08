"""관심 매물(찜) 상태 · 매물 비교 · 적정가 판단."""

import os
import sys
import tempfile
import unittest
from datetime import datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from database import Database  # noqa: E402
from insights import watchlist  # noqa: E402


def make_listing(article_no, price="10억", **overrides):
    listing = {
        "article_no": article_no,
        "region": "서울특별시",
        "district": "강남구",
        "property_type": "아파트",
        "trade_type": "매매",
        "price": price,
        "area": "84A",
        "floor": "10/20",
        "building_name": "래미안",
        "description": "설명",
        "is_urgent": 1,
        "tags": [],
        "confirmed_date": "20261001",
        "latitude": 37.5,
        "longitude": 127.0,
        "naver_url": f"https://example.com/{article_no}",
    }
    listing.update(overrides)
    return listing


class PureFunctionTest(unittest.TestCase):
    def test_price_per_pyeong_uses_area_number(self):
        # 10억 = 100000만원, 84㎡ = 25.41평 → 3935만원/평
        self.assertEqual(watchlist.price_per_pyeong(100000, "84A"), 3935)

    def test_price_per_pyeong_none_without_area_or_price(self):
        self.assertIsNone(watchlist.price_per_pyeong(100000, ""))
        self.assertIsNone(watchlist.price_per_pyeong(None, "84A"))
        self.assertIsNone(watchlist.price_per_pyeong(0, "84A"))

    def test_watch_change_price_down(self):
        change = watchlist.watch_change({"status": "active", "price_sort_value": 94000}, 98000)
        self.assertEqual(change, {"kind": "price_down", "diff": -4000, "pct": -4.1})

    def test_watch_change_price_up_and_none(self):
        self.assertEqual(
            watchlist.watch_change({"status": "active", "price_sort_value": 99000}, 98000)["kind"], "price_up"
        )
        self.assertEqual(
            watchlist.watch_change({"status": "active", "price_sort_value": 98000}, 98000)["kind"], "none"
        )
        self.assertEqual(watchlist.watch_change({"status": "active", "price_sort_value": 98000}, None)["kind"], "none")

    def test_watch_change_gone(self):
        self.assertEqual(watchlist.watch_change({"status": "gone"}, 98000)["kind"], "gone")

    def test_negotiation_high_with_two_signals(self):
        result = watchlist.negotiation(days_listed=31, complex_avg_days=18, cut_count=2, sell_days=11)
        self.assertEqual(result["level"], "high")
        self.assertEqual(len(result["signals"]), 3)

    def test_negotiation_low_without_signals(self):
        result = watchlist.negotiation(days_listed=3, complex_avg_days=18, cut_count=0, sell_days=11)
        self.assertEqual(result, {"level": "low", "signals": []})

    def test_negotiation_some_with_one_signal(self):
        result = watchlist.negotiation(days_listed=3, complex_avg_days=None, cut_count=1, sell_days=None)
        self.assertEqual(result["level"], "some")


class WatchlistDBTest(unittest.TestCase):
    def setUp(self):
        tmpdir = tempfile.TemporaryDirectory()
        self.addCleanup(tmpdir.cleanup)
        self.db = Database(db_path=os.path.join(tmpdir.name, "w.db"), skip_price_backfill=True)
        self.addCleanup(self.db.close)

    def _set_first_seen(self, article_no, days_ago):
        when = (datetime.now() - timedelta(days=days_ago)).isoformat()
        with self.db.get_connection() as conn:
            conn.execute("UPDATE listings SET first_seen_at = ? WHERE article_no = ?", (when, article_no))
            conn.execute("UPDATE listing_tracking SET first_seen_at = ? WHERE article_no = ?", (when, article_no))

    def _session(self, conn):
        return self.db._get_latest_visible_session_id(conn)

    def test_lookup_active_listing_fields(self):
        self.db.insert_listings([make_listing("A1", "9억 4,000", tags=["역세권"])], "s1")
        self._set_first_seen("A1", 5)
        with self.db.get_connection() as conn:
            items = watchlist.lookup(conn, self._session(conn), ["A1"], now=datetime.now())
        item = items["A1"]
        self.assertEqual(item["status"], "active")
        self.assertEqual(item["price_sort_value"], 94000)
        self.assertEqual(item["tags"], ["역세권"])
        self.assertEqual(item["days_listed"], 5)
        self.assertEqual(item["cut_count"], 0)
        self.assertEqual(item["price_per_pyeong"], watchlist.price_per_pyeong(94000, "84A"))

    def test_lookup_gone_listing_uses_disappearance_snapshot(self):
        self.db.insert_listings([make_listing("A1"), make_listing("A2")], "s1")
        self.db.insert_listings([make_listing("A2")], "s2")
        with self.db.get_connection() as conn:
            items = watchlist.lookup(conn, self._session(conn), ["A1", "ZZ"], now=datetime.now())
        self.assertEqual(items["A1"]["status"], "gone")
        self.assertEqual(items["A1"]["building_name"], "래미안")
        self.assertTrue(items["A1"]["disappeared_at"])
        self.assertEqual(items["ZZ"]["status"], "unknown")

    def test_new_bargains_in_watched_complexes(self):
        peers = [make_listing(f"P{i}", "10억") for i in range(4)]
        self.db.insert_listings(peers + [make_listing("A1", "10억")], "s1")
        saved_at = datetime.now().isoformat()
        self.db.insert_listings(peers + [make_listing("A1", "10억"), make_listing("NEW", "8억 5,000")], "s2")
        with self.db.get_connection() as conn:
            found = watchlist.new_bargains(
                conn, self._session(conn), [{"article_no": "A1", "saved_at": saved_at, "district": "강남구",
                                             "building_name": "래미안"}]
            )
        self.assertEqual([f["article_no"] for f in found], ["NEW"])

    def test_fair_price_rank_and_gap(self):
        prices = ["9억 4,000", "9억 8,000", "10억", "10억 2,000", "10억 5,000"]
        listings = [make_listing(f"P{i}", p) for i, p in enumerate(prices)]
        self.db.insert_listings(listings, "s1")
        with self.db.get_connection() as conn:
            result = watchlist.fair_price(conn, self._session(conn), "P0", now=datetime.now())
        self.assertTrue(result["available"])
        self.assertEqual(result["basis"], "same_area")
        self.assertEqual(result["count"], 5)
        self.assertEqual(result["rank"], 1)
        self.assertLess(result["gap_pct"], 0)
        self.assertEqual(len(result["points"]), 5)
        self.assertEqual(sum(1 for p in result["points"] if p["is_self"]), 1)

    def test_fair_price_unavailable_with_few_peers(self):
        self.db.insert_listings([make_listing("A1"), make_listing("A2", building_name="자이")], "s1")
        with self.db.get_connection() as conn:
            result = watchlist.fair_price(conn, self._session(conn), "A1", now=datetime.now())
        self.assertFalse(result["available"])

    def test_fair_price_unavailable_for_rent(self):
        self.db.insert_listings([make_listing("A1", "5억", trade_type="전세")], "s1")
        with self.db.get_connection() as conn:
            result = watchlist.fair_price(conn, self._session(conn), "A1", now=datetime.now())
        self.assertFalse(result["available"])

    def test_fair_price_missing_listing_returns_none(self):
        with self.db.get_connection() as conn:
            self.assertIsNone(watchlist.fair_price(conn, None, "NOPE", now=datetime.now()))

    def test_bargain_sell_days_from_complex_disappearances(self):
        self.db.insert_listings([make_listing("A1")], "s1")
        now = datetime.now()
        with self.db.get_connection() as conn:
            for i, days in enumerate((10, 12)):
                conn.execute(
                    "INSERT INTO listing_disappearances (article_no, session_id, disappeared_at, district, "
                    "building_name, first_seen_at, was_bargain) VALUES (?, ?, ?, ?, ?, ?, 1)",
                    (f"D{i}", "sx", now.isoformat(), "강남구", "래미안", (now - timedelta(days=days)).isoformat()),
                )
            sell = watchlist.bargain_sell_days(conn, "강남구", "래미안", now)
        self.assertEqual(sell, {"days": 11, "scope": "complex", "count": 2})


class WatchlistAPITest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls._tmpdir = tempfile.TemporaryDirectory()
        db_path = os.path.join(cls._tmpdir.name, "w-api.db")
        os.environ["FORCE_LOCAL_SQLITE"] = "1"
        os.environ["DATABASE_URL"] = ""
        os.environ["DB_PATH"] = db_path
        os.environ["ENABLE_SCHEDULER"] = "false"
        os.environ["SEED_DEMO_DATA"] = "false"
        import app as app_module  # noqa: E402

        cls.app_module = app_module
        cls.prev_db = app_module.db
        app_module.db = Database(db_path=db_path, skip_price_backfill=True)
        cls.client = app_module.app.test_client()
        prices = ["9억 4,000", "9억 8,000", "10억", "10억 2,000"]
        app_module.db.insert_listings([make_listing(f"P{i}", p) for i, p in enumerate(prices)], "s1")

    @classmethod
    def tearDownClass(cls):
        cls.app_module.db.close()
        cls.app_module.db = cls.prev_db
        cls._tmpdir.cleanup()

    def test_watchlist_page_renders(self):
        res = self.client.get("/watchlist")
        self.assertEqual(res.status_code, 200)
        self.assertIn("관심 매물", res.get_data(as_text=True))

    def test_watchlist_status(self):
        res = self.client.post(
            "/api/watchlist/status",
            json={"items": [{"article_no": "P0", "saved_at": "2020-01-01T00:00:00", "saved_price_value": 98000}]},
        )
        self.assertEqual(res.status_code, 200)
        data = res.get_json()
        self.assertEqual(data["items"][0]["article_no"], "P0")
        self.assertEqual(data["items"][0]["change"]["kind"], "price_down")
        self.assertIn("new_bargains", data)

    def test_watchlist_status_rejects_bad_body(self):
        self.assertEqual(self.client.post("/api/watchlist/status", json={"items": "x"}).status_code, 400)

    def test_watchlist_status_caps_items(self):
        items = [{"article_no": f"X{i}"} for i in range(80)]
        data = self.client.post("/api/watchlist/status", json={"items": items}).get_json()
        self.assertEqual(len(data["items"]), watchlist.MAX_WATCH_ITEMS)

    def test_compare_keeps_requested_order(self):
        res = self.client.get("/api/listings/compare?ids=P2,P0")
        self.assertEqual(res.status_code, 200)
        self.assertEqual([i["article_no"] for i in res.get_json()["items"]], ["P2", "P0"])

    def test_compare_requires_ids_and_caps_three(self):
        self.assertEqual(self.client.get("/api/listings/compare").status_code, 400)
        data = self.client.get("/api/listings/compare?ids=P0,P1,P2,P3").get_json()
        self.assertEqual(len(data["items"]), 3)

    def test_fair_price_endpoint(self):
        res = self.client.get("/api/listings/P0/fair-price")
        self.assertEqual(res.status_code, 200)
        self.assertTrue(res.get_json()["available"])
        self.assertEqual(self.client.get("/api/listings/NOPE/fair-price").status_code, 404)


if __name__ == "__main__":
    unittest.main()
