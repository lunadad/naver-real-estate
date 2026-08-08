"""단지 급변 배지 — 배치 조회와 listings API 결합 테스트."""

import os
import tempfile
import unittest
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from database import Database  # noqa: E402


class BuildingChangeBadgeAPITest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls._tmpdir = tempfile.TemporaryDirectory()
        db_path = os.path.join(cls._tmpdir.name, "building-change-badge.db")
        os.environ["FORCE_LOCAL_SQLITE"] = "1"
        os.environ["DATABASE_URL"] = ""
        os.environ["DB_PATH"] = db_path
        os.environ["ENABLE_SCHEDULER"] = "false"
        os.environ["SEED_DEMO_DATA"] = "false"

        import app as app_module  # noqa: E402

        cls.app_module = app_module
        app_module.db = Database(db_path=db_path, skip_price_backfill=True)
        cls.client = app_module.app.test_client()

    @classmethod
    def tearDownClass(cls):
        # app 모듈의 process-wide db 참조를 뒤 테스트가 재바인딩할 때까지 유지한다.
        pass

    def setUp(self):
        with self.app_module.db.get_connection() as conn:
            conn.execute("DELETE FROM listings")
            conn.execute("DELETE FROM crawl_building_stats")
            conn.execute("DELETE FROM crawl_history")

    def _seed_snapshot(self, session_id, crawled_at, buildings):
        with self.app_module.db.get_connection() as conn:
            conn.execute(
                """
                INSERT INTO crawl_history
                (session_id, crawled_at, total_count, urgent_count, status, source)
                VALUES (?, ?, ?, ?, 'success', 'naver')
                """,
                (session_id, crawled_at, sum(v[0] for v in buildings.values()), 0),
            )
            conn.executemany(
                """
                INSERT INTO crawl_building_stats
                (session_id, region, district, building_name, total_count, price_down_count, created_at)
                VALUES (?, '서울특별시', '서초구', ?, ?, ?, ?)
                """,
                [
                    (session_id, name, counts[0], counts[1], crawled_at)
                    for name, counts in buildings.items()
                ],
            )

    def _seed_current_listings(self, building_names):
        with self.app_module.db.get_connection() as conn:
            conn.executemany(
                """
                INSERT INTO listings
                (article_no, region, district, property_type, trade_type, price,
                 building_name, description, is_urgent, tags, confirmed_date,
                 crawled_at, crawl_session, naver_url, price_sort_value)
                VALUES (?, '서울특별시', '서초구', '아파트', '매매', '10억',
                        ?, '', 1, '[]', '20260808', ?, 'today', '', 100000)
                """,
                [(f"article-{index}", name, "2026-08-08T09:00:00") for index, name in enumerate(building_names)],
            )

    def test_listings_api_adds_badges_from_one_page_batch(self):
        previous = {
            "비율급증": (10, 0),
            "절대급증": (30, 0),
            "가격인하급증": (20, 1),
            "소수오탐방지": (2, 0),
            "변화없음": (30, 0),
        }
        current = {
            "비율급증": (13, 0),       # +3 and +30%
            "절대급증": (35, 0),       # +5
            "가격인하급증": (20, 4),   # 가격인하 +3
            "소수오탐방지": (3, 0),    # +50%지만 +1뿐
            "변화없음": (34, 0),       # +4지만 +20% 미만
        }
        self._seed_snapshot("yesterday", "2026-08-07T09:00:00", previous)
        self._seed_snapshot("today", "2026-08-08T09:00:00", current)
        self._seed_current_listings(current.keys())

        response = self.client.get("/api/listings?per_page=20&sort_by=recent")

        self.assertEqual(response.status_code, 200)
        rows = {row["building_name"]: row for row in response.get_json()["listings"]}
        self.assertEqual(rows["비율급증"]["building_change_badge"]["total_diff"], 3)
        self.assertEqual(rows["절대급증"]["building_change_badge"]["total_diff"], 5)
        self.assertEqual(rows["가격인하급증"]["building_change_badge"]["price_down_diff"], 3)
        self.assertNotIn("building_change_badge", rows["소수오탐방지"])
        self.assertNotIn("building_change_badge", rows["변화없음"])

    def test_batch_lookup_ignores_failed_and_demo_snapshots(self):
        self._seed_snapshot("old", "2026-08-06T09:00:00", {"테스트단지": (10, 0)})
        self._seed_snapshot("current", "2026-08-07T09:00:00", {"테스트단지": (15, 0)})
        with self.app_module.db.get_connection() as conn:
            conn.execute(
                "INSERT INTO crawl_history (session_id, crawled_at, status, source) VALUES (?, ?, ?, ?)",
                ("demo", "2026-08-08T09:00:00", "success", "demo"),
            )
            conn.execute(
                """
                INSERT INTO crawl_building_stats
                (session_id, region, district, building_name, total_count, price_down_count, created_at)
                VALUES (?, '서울특별시', '서초구', '테스트단지', 99, 99, ?)
                """,
                ("demo", "2026-08-08T09:00:00"),
            )

        changes = self.app_module.db.get_latest_building_changes([("서초구", "테스트단지")])

        self.assertEqual(changes[("서초구", "테스트단지")]["current_total"], 15)
        self.assertEqual(changes[("서초구", "테스트단지")]["previous_total"], 10)


if __name__ == "__main__":
    unittest.main()
