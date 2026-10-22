"""지역 히트맵 — /api/heatmap, /heatmap 테스트."""

import os
import sys
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from database import Database  # noqa: E402

SEOUL = "서울특별시"
BUSAN = "부산광역시"


class HeatmapTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls._tmpdir = tempfile.TemporaryDirectory()
        db_path = os.path.join(cls._tmpdir.name, "heatmap.db")
        os.environ["FORCE_LOCAL_SQLITE"] = "1"
        os.environ["DATABASE_URL"] = ""
        os.environ["DB_PATH"] = db_path
        os.environ["ENABLE_SCHEDULER"] = "false"
        os.environ["SEED_DEMO_DATA"] = "false"

        import app as app_module  # noqa: E402

        cls.app_module = app_module
        app_module.db = Database(db_path=db_path, skip_price_backfill=True)
        cls.db = app_module.db
        cls.client = app_module.app.test_client()

    @classmethod
    def tearDownClass(cls):
        # app.py는 테스트 프로세스 전체에서 공유되므로 DB를 닫지 않는다.
        pass

    def setUp(self):
        with self.db.get_connection() as conn:
            for table in ("listings", "crawl_history", "crawl_region_stats", "crawl_building_stats"):
                conn.execute(f"DELETE FROM {table}")
        self.now = datetime.now(timezone.utc).replace(tzinfo=None, microsecond=0)

    # ── seed helpers ────────────────────────────────────────────────────────
    def _session(self, session_id, days_ago):
        crawled_at = (self.now - timedelta(days=days_ago)).isoformat()
        with self.db.get_connection() as conn:
            conn.execute(
                "INSERT INTO crawl_history (session_id, crawled_at, total_count, urgent_count, status, source) "
                "VALUES (?, ?, 0, 0, 'success', 'naver')",
                (session_id, crawled_at),
            )
        return crawled_at

    def _region_stat(self, session_id, crawled_at, district, total, down, region=SEOUL):
        with self.db.get_connection() as conn:
            conn.execute(
                "INSERT INTO crawl_region_stats (session_id, region, district, total_count, price_down_count, created_at) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                (session_id, region, district, total, down, crawled_at),
            )

    def _listing(self, article_no, session_id, crawled_at, district, discount, region=SEOUL):
        with self.db.get_connection() as conn:
            conn.execute(
                """
                INSERT INTO listings
                (article_no, region, district, property_type, trade_type, price, area, floor,
                 building_name, description, is_urgent, tags, crawled_at, crawl_session,
                 naver_url, price_sort_value, bargain_discount)
                VALUES (?, ?, ?, '아파트', '매매', '10억', '84A', '1/10', '단지', '', 1, '[]', ?, ?, '', 100000, ?)
                """,
                (article_no, region, district, crawled_at, session_id, discount),
            )

    def _seed(self, with_week_ago=True):
        if with_week_ago:
            old_at = self._session("s-old", 7)
            self._region_stat("s-old", old_at, "강남구", 2, 0)
            self._region_stat("s-old", old_at, "서초구", 4, 1)
        now_at = self._session("s-now", 0)
        self._region_stat("s-now", now_at, "강남구", 4, 2)
        self._region_stat("s-now", now_at, "서초구", 2, 0)
        for i, disc in enumerate([0.20, 0.15, 0.05, None]):
            self._listing(f"g{i}", "s-now", now_at, "강남구", disc)
        for i, disc in enumerate([0.05, 0.45]):  # 0.45는 지분·오류 의심 → 진짜 급매 아님
            self._listing(f"s{i}", "s-now", now_at, "서초구", disc)
        self._region_stat("s-now", now_at, "해운대구", 3, 1, region=BUSAN)
        self._listing("b0", "s-now", now_at, "해운대구", 0.30, region=BUSAN)
        return now_at

    def _get(self, **params):
        return self.client.get("/api/heatmap", query_string=params)

    def _by_district(self, data):
        return {d["district"]: d for d in data["districts"]}

    # ── tests ───────────────────────────────────────────────────────────────
    def test_ratios_per_district(self):
        self._seed()
        data = self._get(region=SEOUL).get_json()
        self.assertEqual(data["region"], SEOUL)
        districts = self._by_district(data)
        gangnam = districts["강남구"]
        self.assertEqual(gangnam["total"], 4)
        self.assertEqual(gangnam["bargain_count"], 2)
        self.assertAlmostEqual(gangnam["bargain_ratio"], 0.5)
        self.assertAlmostEqual(gangnam["price_down_ratio"], 0.5)
        seocho = districts["서초구"]
        self.assertEqual(seocho["bargain_count"], 0)
        self.assertEqual(seocho["bargain_ratio"], 0.0)
        self.assertEqual(seocho["price_down_ratio"], 0.0)

    def test_districts_without_data_are_listed_with_null_ratios(self):
        self._seed()
        districts = self._by_district(self._get(region=SEOUL).get_json())
        self.assertEqual(len(districts), 25)
        empty = districts["은평구"]
        self.assertEqual(empty["total"], 0)
        self.assertIsNone(empty["bargain_ratio"])
        self.assertIsNone(empty["price_down_ratio"])
        self.assertIsNone(empty["weekly_change_pct"])

    def test_weekly_change_vs_7_day_old_session(self):
        self._seed()
        districts = self._by_district(self._get(region=SEOUL).get_json())
        self.assertEqual(districts["강남구"]["weekly_change_pct"], 100.0)   # 2 → 4
        self.assertEqual(districts["서초구"]["weekly_change_pct"], -50.0)   # 4 → 2

    def test_weekly_change_is_none_without_7_day_old_session(self):
        self._seed(with_week_ago=False)
        districts = self._by_district(self._get(region=SEOUL).get_json())
        self.assertIsNone(districts["강남구"]["weekly_change_pct"])
        self.assertEqual(districts["강남구"]["total"], 4)

    def test_weekly_change_is_none_when_district_missing_a_week_ago(self):
        old_at = self._session("s-old", 7)
        self._region_stat("s-old", old_at, "서초구", 4, 0)
        now_at = self._session("s-now", 0)
        self._region_stat("s-now", now_at, "강남구", 4, 0)
        districts = self._by_district(self._get(region=SEOUL).get_json())
        self.assertIsNone(districts["강남구"]["weekly_change_pct"])

    def test_region_filter(self):
        self._seed()
        data = self._get(region=BUSAN).get_json()
        self.assertEqual(data["region"], BUSAN)
        districts = self._by_district(data)
        self.assertEqual(set(districts), {"해운대구", "수영구", "남구", "부산진구", "동래구"})
        self.assertEqual(districts["해운대구"]["bargain_count"], 1)
        self.assertAlmostEqual(districts["해운대구"]["bargain_ratio"], 1 / 3, places=3)
        self.assertNotIn("강남구", districts)

    def test_default_region_is_seoul(self):
        self._seed()
        self.assertEqual(self._get().get_json()["region"], SEOUL)

    def test_unknown_region_returns_404(self):
        response = self._get(region="아틀란티스")
        self.assertEqual(response.status_code, 404)
        self.assertEqual(response.get_json()["status"], "error")

    def test_regions_list_and_metric_defs(self):
        self._seed()
        data = self._get(region=SEOUL).get_json()
        self.assertIn(SEOUL, data["regions"])
        self.assertIn(BUSAN, data["regions"])
        self.assertEqual(
            [m["key"] for m in data["metric_defs"]],
            ["bargain_ratio", "price_down_ratio", "weekly_change_pct"],
        )

    def test_empty_database_still_returns_districts(self):
        data = self._get(region=SEOUL).get_json()
        self.assertEqual(len(data["districts"]), 25)
        self.assertTrue(all(d["total"] == 0 for d in data["districts"]))

    def test_response_is_cacheable(self):
        self._seed()
        self.assertEqual(self._get().headers["Cache-Control"], "public, max-age=300")

    def test_page_route_renders(self):
        response = self.client.get("/heatmap")
        self.assertEqual(response.status_code, 200)
        html = response.get_data(as_text=True)
        self.assertIn("js/heatmap.js", html)
        self.assertIn("급매 히트맵", html)


if __name__ == "__main__":
    unittest.main()
