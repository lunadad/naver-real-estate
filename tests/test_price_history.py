"""매물 가격 이력 — listing_tracking / listing_price_events."""

import os
import sys
import tempfile
import unittest
from datetime import datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from database import Database  # noqa: E402


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


class PriceHistoryDBTest(unittest.TestCase):
    def setUp(self):
        tmpdir = tempfile.TemporaryDirectory()
        self.addCleanup(tmpdir.cleanup)
        self.db = Database(db_path=os.path.join(tmpdir.name, "t.db"), skip_price_backfill=True)
        self.addCleanup(self.db.close)

    def tracking(self, article_no="A1"):
        with self.db.get_connection() as conn:
            row = conn.execute(
                "SELECT * FROM listing_tracking WHERE article_no = ?", (article_no,)
            ).fetchone()
        return dict(row) if row else None

    def events(self, article_no="A1"):
        with self.db.get_connection() as conn:
            rows = conn.execute(
                "SELECT * FROM listing_price_events WHERE article_no = ? ORDER BY id",
                (article_no,),
            ).fetchall()
        return [dict(r) for r in rows]

    def listing_row(self, article_no="A1"):
        with self.db.get_connection() as conn:
            row = conn.execute(
                "SELECT * FROM listings WHERE article_no = ?", (article_no,)
            ).fetchone()
        return dict(row)

    def test_first_sighting_creates_tracking_and_initial_event(self):
        self.db.insert_listings([make_listing("A1")], "s1")
        t = self.tracking()
        self.assertEqual(t["last_price"], "10억")
        self.assertEqual(t["last_price_sort_value"], 100000)
        self.assertEqual(t["price_change_count"], 0)
        self.assertEqual(t["price_cut_count"], 0)
        self.assertEqual(t["first_seen_at"], t["last_seen_at"])
        events = self.events()
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0]["session_id"], "s1")
        self.assertIsNone(events[0]["prev_price"])
        self.assertIsNone(events[0]["change_pct"])
        row = self.listing_row()
        self.assertEqual(row["first_seen_at"], t["first_seen_at"])
        self.assertEqual(row["price_cut_count"], 0)

    def test_unchanged_price_adds_no_event_but_updates_last_seen(self):
        self.db.insert_listings([make_listing("A1")], "s1")
        first = self.tracking()
        self.db.insert_listings([make_listing("A1")], "s2")
        t = self.tracking()
        self.assertEqual(len(self.events()), 1)
        self.assertEqual(t["price_change_count"], 0)
        self.assertEqual(t["first_seen_at"], first["first_seen_at"])
        self.assertGreaterEqual(t["last_seen_at"], first["last_seen_at"])

    def test_price_cut_records_event_counts_and_change_pct(self):
        self.db.insert_listings([make_listing("A1", "10억")], "s1")
        self.db.insert_listings([make_listing("A1", "9억")], "s2")
        t = self.tracking()
        self.assertEqual((t["price_change_count"], t["price_cut_count"]), (1, 1))
        self.assertEqual(t["last_price"], "9억")
        self.assertEqual(t["last_price_sort_value"], 90000)
        ev = self.events()[-1]
        self.assertEqual(ev["prev_price"], "10억")
        self.assertEqual(ev["prev_price_sort_value"], 100000)
        self.assertEqual(ev["price_sort_value"], 90000)
        self.assertAlmostEqual(ev["change_pct"], -10.0, places=2)
        self.assertEqual(self.listing_row()["price_cut_count"], 1)

    def test_price_raise_counts_change_but_not_cut(self):
        self.db.insert_listings([make_listing("A1", "10억")], "s1")
        self.db.insert_listings([make_listing("A1", "11억")], "s2")
        t = self.tracking()
        self.assertEqual((t["price_change_count"], t["price_cut_count"]), (1, 0))
        self.assertAlmostEqual(self.events()[-1]["change_pct"], 10.0, places=2)
        self.assertEqual(self.listing_row()["price_cut_count"], 0)

    def test_cut_count_accumulates_across_sessions(self):
        for sid, price in [("s1", "10억"), ("s2", "9억 5,000"), ("s3", "9억 5,000"), ("s4", "9억")]:
            self.db.insert_listings([make_listing("A1", price)], sid)
        t = self.tracking()
        self.assertEqual((t["price_change_count"], t["price_cut_count"]), (2, 2))
        self.assertEqual(len(self.events()), 3)
        self.assertEqual(self.listing_row()["price_cut_count"], 2)

    def test_reinserting_same_session_is_idempotent(self):
        self.db.insert_listings([make_listing("A1", "10억")], "s1")
        self.db.insert_listings([make_listing("A1", "9억")], "s2")
        before = (self.tracking(), len(self.events()))
        self.db.insert_listings([make_listing("A1", "9억")], "s2")
        self.db.insert_listings([make_listing("A1", "9억")], "s2")
        t = self.tracking()
        self.assertEqual(len(self.events()), before[1])
        self.assertEqual((t["price_change_count"], t["price_cut_count"]), (1, 1))

    def test_same_session_second_insert_with_other_price_does_not_duplicate_event(self):
        self.db.insert_listings([make_listing("A1", "10억")], "s1")
        self.db.insert_listings([make_listing("A1", "9억")], "s2")
        self.db.insert_listings([make_listing("A1", "8억")], "s2")
        sessions = [e["session_id"] for e in self.events()]
        self.assertEqual(sessions.count("s2"), 1)

    def test_prunes_tracking_and_events_not_seen_for_90_days(self):
        old = (datetime.now() - timedelta(days=91)).isoformat()
        with self.db.get_connection() as conn:
            conn.execute(
                "INSERT INTO listing_tracking (article_no, first_seen_at, last_seen_at, last_price, "
                "last_price_sort_value, price_change_count, price_cut_count) VALUES (?,?,?,?,?,?,?)",
                ("OLD", old, old, "5억", 50000, 0, 0),
            )
            conn.execute(
                "INSERT INTO listing_price_events (article_no, session_id, observed_at, price, "
                "price_sort_value) VALUES (?,?,?,?,?)",
                ("OLD", "s0", old, "5억", 50000),
            )
        self.db.insert_listings([make_listing("A1")], "s1")
        self.assertIsNone(self.tracking("OLD"))
        self.assertEqual(self.events("OLD"), [])
        self.assertIsNotNone(self.tracking("A1"))

    def test_recently_seen_tracking_survives_pruning(self):
        recent = (datetime.now() - timedelta(days=30)).isoformat()
        with self.db.get_connection() as conn:
            conn.execute(
                "INSERT INTO listing_tracking (article_no, first_seen_at, last_seen_at, last_price, "
                "last_price_sort_value, price_change_count, price_cut_count) VALUES (?,?,?,?,?,?,?)",
                ("MID", recent, recent, "5억", 50000, 0, 0),
            )
        self.db.insert_listings([make_listing("A1")], "s1")
        self.assertIsNotNone(self.tracking("MID"))

    def test_large_batch_is_chunked(self):
        listings = [make_listing(f"B{i}", "5억") for i in range(1200)]
        self.db.insert_listings(listings, "s1")
        self.db.insert_listings([make_listing(f"B{i}", "4억") for i in range(1200)], "s2")
        with self.db.get_connection() as conn:
            cuts = conn.execute(
                "SELECT COUNT(*) AS c FROM listing_tracking WHERE price_cut_count = 1"
            ).fetchone()["c"]
        self.assertEqual(cuts, 1200)

    def test_old_two_argument_call_still_works(self):
        self.db.insert_listings([make_listing("A1")], "s1")
        self.assertIsNotNone(self.tracking())


class PriceHistoryAPITest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls._tmpdir = tempfile.TemporaryDirectory()
        db_path = os.path.join(cls._tmpdir.name, "ph-api.db")
        os.environ["FORCE_LOCAL_SQLITE"] = "1"
        os.environ["DATABASE_URL"] = ""
        os.environ["DB_PATH"] = db_path
        os.environ["ENABLE_SCHEDULER"] = "false"
        os.environ["SEED_DEMO_DATA"] = "false"
        import app as app_module  # noqa: E402

        cls.app_module = app_module
        app_module.db = Database(db_path=db_path, skip_price_backfill=True)
        cls.client = app_module.app.test_client()

    def test_history_endpoint_returns_events_and_negotiation_clues(self):
        db = self.app_module.db
        db.insert_listings([make_listing("H1", "10억")], "h1")
        db.insert_listings([make_listing("H1", "9억 5,000")], "h2")
        db.insert_listings([make_listing("H1", "9억")], "h3")
        res = self.client.get("/api/listings/H1/history")
        self.assertEqual(res.status_code, 200)
        body = res.get_json()
        self.assertEqual(body["article_no"], "H1")
        self.assertEqual([e["price"] for e in body["events"]], ["10억", "9억 5,000", "9억"])
        self.assertEqual(body["tracking"]["price_cut_count"], 2)
        insight = body["insight"]
        self.assertEqual(insight["cut_count"], 2)
        self.assertAlmostEqual(insight["total_change_pct"], -10.0, places=2)
        self.assertEqual(insight["days_since_first_seen"], 0)
        self.assertEqual(insight["days_since_last_change"], 0)
        self.assertEqual(body["listing"]["building_name"], "래미안")

    def test_history_endpoint_unknown_article_is_404(self):
        res = self.client.get("/api/listings/NOPE/history")
        self.assertEqual(res.status_code, 404)
        self.assertEqual(res.get_json()["status"], "error")

    def test_listings_api_exposes_price_cut_count(self):
        db = self.app_module.db
        db.insert_listings([make_listing("H2", "10억", building_name="유니크H2단지")], "x1")
        db.log_crawl("x1", 1, 1, "success", "naver")
        db.insert_listings([make_listing("H2", "9억", building_name="유니크H2단지")], "x2")
        db.log_crawl("x2", 1, 1, "success", "naver")
        body = self.client.get("/api/listings?search=유니크H2단지").get_json()
        row = [r for r in body["listings"] if r["article_no"] == "H2"][0]
        self.assertEqual(row["price_cut_count"], 1)
        self.assertTrue(row["first_seen_at"])


class PriceHistoryInsightTest(unittest.TestCase):
    def test_build_insight_pure(self):
        from insights.price_history import build_insight

        now = datetime(2026, 10, 22, 12, 0)
        tracking = {
            "first_seen_at": (now - timedelta(days=10)).isoformat(),
            "price_cut_count": 2,
        }
        events = [
            {"observed_at": (now - timedelta(days=10)).isoformat(), "price_sort_value": 100000},
            {"observed_at": (now - timedelta(days=4)).isoformat(), "price_sort_value": 95000},
            {"observed_at": (now - timedelta(days=3)).isoformat(), "price_sort_value": 90000},
        ]
        out = build_insight(tracking, events, bargain_discount=0.15, now=now)
        self.assertEqual(out["days_since_first_seen"], 10)
        self.assertEqual(out["days_since_last_change"], 3)
        self.assertEqual(out["cut_count"], 2)
        self.assertAlmostEqual(out["total_change_pct"], -10.0)
        self.assertAlmostEqual(out["bargain_discount_pct"], 15.0)
        self.assertIsNone(build_insight(tracking, events, bargain_discount=0.5, now=now)["bargain_discount_pct"])


if __name__ == "__main__":
    unittest.main()
