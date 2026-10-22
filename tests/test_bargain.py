"""진짜 급매 점수 — 단지 중앙값 대비 할인율."""

import os
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from bargain import area_number, compute_bargain_scores, is_bargain  # noqa: E402
from database import Database  # noqa: E402


def sale(article_no, value, *, area="84A", building="래미안", district="강남구",
         property_type="아파트", trade_type="매매", **extra):
    listing = {
        "article_no": article_no,
        "region": "서울특별시",
        "district": district,
        "building_name": building,
        "property_type": property_type,
        "trade_type": trade_type,
        "area": area,
        "price_sort_value": value,
    }
    listing.update(extra)
    return listing


class ComputeBargainScoresTest(unittest.TestCase):
    def test_same_area_group_of_three_uses_price_median(self):
        scores = compute_bargain_scores([sale("a", 50000), sale("b", 50000), sale("c", 40000)])
        self.assertEqual(scores["c"], (0.2, "same_area"))
        self.assertEqual(scores["a"], (0.0, "same_area"))

    def test_group_of_two_falls_back_to_complex_m2(self):
        listings = [
            sale("a", 84000, area="84A"),
            sale("b", 84000, area="84B"),
            sale("c", 59000 * 0.8, area="59"),
        ]
        scores = compute_bargain_scores(listings)
        # ㎡당 1000(84A·84B)·800(59) → 중앙값 1000, c는 20% 쌈
        self.assertEqual(scores["c"], (0.2, "complex_m2"))
        self.assertEqual(scores["a"], (0.0, "complex_m2"))

    def test_no_score_when_complex_has_fewer_than_three(self):
        self.assertEqual(compute_bargain_scores([sale("a", 50000), sale("b", 40000)]), {})

    def test_excludes_rent_generic_names_missing_area_and_nonpositive_price(self):
        base = [sale("a", 50000), sale("b", 50000), sale("c", 50000)]
        excluded = [
            sale("jeonse", 10000, trade_type="전세"),
            sale("villa-type", 10000, property_type="빌라/연립"),
            sale("generic", 10000, building=" 아파트 "),
            sale("blank-name", 10000, building=""),
            sale("no-area", 10000, area=""),
            sale("none-price", None),
            sale("zero-price", 0),
        ]
        scores = compute_bargain_scores(base + excluded)
        self.assertEqual(set(scores), {"a", "b", "c"})

    def test_same_name_in_other_district_is_separate_complex(self):
        listings = [sale(str(i), 50000) for i in range(3)] + [
            sale("x", 30000, district="서초구"), sale("y", 30000, district="서초구"),
        ]
        scores = compute_bargain_scores(listings)
        self.assertNotIn("x", scores)

    def test_area_number_parses_type_suffix(self):
        self.assertEqual(area_number("84A"), 84.0)
        self.assertEqual(area_number("113E"), 113.0)
        self.assertEqual(area_number(" 59.5"), 59.5)
        self.assertIsNone(area_number(""))
        self.assertIsNone(area_number(None))
        self.assertIsNone(area_number("B1"))

    def test_is_bargain_bounds(self):
        self.assertFalse(is_bargain(0.0999))
        self.assertTrue(is_bargain(0.10))
        self.assertTrue(is_bargain(0.40))
        self.assertFalse(is_bargain(0.4001))
        self.assertFalse(is_bargain(None))
        self.assertFalse(is_bargain(-0.2))


def listing_row(article_no, value, price, **overrides):
    row = sale(article_no, value, price=price, floor="5/20", description="", is_urgent=1,
               tags=["급매"], confirmed_date="20261022", latitude=37.5, longitude=127.0,
               naver_url=f"https://example.com/{article_no}")
    row.pop("price_sort_value")
    row.update(overrides)
    return row


SEED = [
    listing_row("A1", 50000, "5억"),
    listing_row("A2", 50000, "5억"),
    listing_row("A5", 50000, "5억"),            # 같은 평형 5건, 중앙값 5억
    listing_row("A3", 40000, "4억"),            # −20% → 진짜 급매
    listing_row("A4", 25000, "2억 5,000"),      # −50% → 이상치
    listing_row("R1", 0, "3억", trade_type="전세"),
]


class BargainStorageTest(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.db = Database(db_path=os.path.join(tmp.name, "b.db"), skip_price_backfill=True)
        self.addCleanup(self.db.close)
        self.db.insert_listings(SEED, "s1")
        self.db.log_crawl("s1", len(SEED), len(SEED), "success", "naver")

    def _scores(self):
        with self.db.get_connection() as conn:
            rows = conn.execute(
                "SELECT article_no, bargain_discount, bargain_basis FROM listings ORDER BY article_no"
            ).fetchall()
        return {r["article_no"]: (r["bargain_discount"], r["bargain_basis"]) for r in rows}

    def test_insert_listings_stores_scores_and_filter(self):
        scores = self._scores()
        self.assertEqual(scores["A3"], (0.2, "same_area"))
        self.assertEqual(scores["A4"], (0.5, "same_area"))
        self.assertEqual(scores["R1"], (None, None))

        result = self.db.get_listings(bargain_only=True)
        self.assertEqual([l["article_no"] for l in result["listings"]], ["A3"])
        self.assertEqual(result["bargain_count"], 1)
        self.assertEqual(self.db.get_listings()["bargain_count"], 1)
        self.assertEqual(self.db.get_listings(trade_type="전세")["bargain_count"], 0)

        map_rows = self.db.get_map_listings(bargain_only=True)
        self.assertEqual([r["article_no"] for r in map_rows], ["A3"])
        self.assertEqual(map_rows[0]["bargain_discount"], 0.2)

    def test_backfill_fills_existing_session(self):
        with self.db.get_connection() as conn:
            conn.execute("UPDATE listings SET bargain_discount = NULL, bargain_basis = NULL")
        self.assertEqual(self.db.backfill_bargain_scores(), len(SEED))
        self.assertEqual(self._scores()["A3"], (0.2, "same_area"))

    def test_existing_db_gets_bargain_columns_migrated(self):
        import sqlite3

        path = self.db.db_path
        self.db.close()
        conn = sqlite3.connect(path)
        conn.execute("ALTER TABLE listings DROP COLUMN bargain_discount")
        conn.execute("ALTER TABLE listings DROP COLUMN bargain_basis")
        conn.commit()
        conn.close()
        db = Database(db_path=path, skip_price_backfill=True)
        self.addCleanup(db.close)
        with db.get_connection() as conn:
            cols = db._get_table_columns(conn, "listings")
        self.assertTrue({"bargain_discount", "bargain_basis"} <= cols)


class BargainApiTest(unittest.TestCase):
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

    def test_listings_api_bargain_only_param(self):
        body = self.client.get("/api/listings?bargain_only=true").get_json()
        self.assertEqual([l["article_no"] for l in body["listings"]], ["A3"])
        self.assertEqual(body["listings"][0]["bargain_basis"], "same_area")
        self.assertEqual(body["bargain_count"], 1)
        body = self.client.get("/api/map-listings?bargain_only=true").get_json()
        self.assertEqual([l["article_no"] for l in body["listings"]], ["A3"])
        self.assertEqual(self.client.get("/api/listings").get_json()["total"], len(SEED))


if __name__ == "__main__":
    unittest.main()
