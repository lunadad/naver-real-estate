"""오늘의 급매 온도 브리핑 — DB 집계, 가공 로직, API 테스트."""

import os
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from database import Database  # noqa: E402


class HeroInsightTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls._tmpdir = tempfile.TemporaryDirectory()
        db_path = os.path.join(cls._tmpdir.name, "hero-insight.db")
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
            conn.execute("DELETE FROM crawl_building_stats")
            conn.execute("DELETE FROM crawl_region_stats")
            conn.execute("DELETE FROM crawl_history")

    def _seed_session(
        self,
        session_id,
        crawled_at,
        *,
        region_rows=(),
        building_rows=(),
        status="success",
        source="naver",
    ):
        with self.db.get_connection() as conn:
            conn.execute(
                """
                INSERT INTO crawl_history
                (session_id, crawled_at, total_count, urgent_count, status, source)
                VALUES (?, ?, ?, 0, ?, ?)
                """,
                (
                    session_id,
                    crawled_at,
                    sum(row[2] for row in region_rows),
                    status,
                    source,
                ),
            )
            conn.executemany(
                """
                INSERT INTO crawl_region_stats
                (session_id, region, district, total_count, price_down_count, created_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                [
                    (session_id, region, district, total, price_down, crawled_at)
                    for region, district, total, price_down in region_rows
                ],
            )
            conn.executemany(
                """
                INSERT INTO crawl_building_stats
                (session_id, region, district, building_name, total_count, price_down_count, created_at)
                VALUES (?, '서울특별시', ?, ?, ?, ?, ?)
                """,
                [
                    (session_id, district, building, total, price_down, crawled_at)
                    for district, building, total, price_down in building_rows
                ],
            )

    def test_price_down_ratio_trend_compares_today_and_yesterday(self):
        self._seed_session(
            "yesterday",
            "2026-08-08T09:00:00",
            region_rows=(
                ("서울특별시", "서초구", 60, 6),
                ("서울특별시", "강남구", 40, 4),
            ),
        )
        self._seed_session(
            "today",
            "2026-08-09T09:00:00",
            region_rows=(
                ("서울특별시", "서초구", 100, 20),
                ("서울특별시", "강남구", 100, 10),
            ),
        )

        trend = self.db.get_price_down_ratio_trend()

        self.assertEqual(trend["today_ratio"], 15.0)
        self.assertEqual(trend["yesterday_ratio"], 10.0)
        self.assertEqual(trend["diff_pp"], 5.0)

    def test_price_down_ratio_trend_without_yesterday(self):
        self._seed_session(
            "today",
            "2026-08-09T09:00:00",
            region_rows=(("서울특별시", "서초구", 20, 5),),
        )

        trend = self.db.get_price_down_ratio_trend()

        self.assertEqual(trend["today_ratio"], 25.0)
        self.assertIsNone(trend["yesterday_ratio"])
        self.assertIsNone(trend["diff_pp"])

    def test_price_down_ratio_trend_without_live_session(self):
        self._seed_session(
            "demo",
            "2026-08-09T09:00:00",
            region_rows=(("서울특별시", "서초구", 20, 5),),
            source="demo",
        )
        self._seed_session(
            "failed",
            "2026-08-09T10:00:00",
            region_rows=(("서울특별시", "강남구", 10, 4),),
            status="failed",
        )

        self.assertIsNone(self.db.get_price_down_ratio_trend())

    def test_top_building_movers_requires_both_recent_sessions(self):
        self._seed_session(
            "yesterday",
            "2026-08-08T09:00:00",
            building_rows=(
                ("서초구", "공통단지", 10, 1),
                ("서초구", "어제만단지", 8, 0),
            ),
        )
        self._seed_session(
            "today",
            "2026-08-09T09:00:00",
            building_rows=(
                ("서초구", "공통단지", 16, 4),
                ("강남구", "오늘만단지", 9, 1),
            ),
        )

        movers = self.db.get_top_building_movers()

        self.assertEqual(len(movers), 1)
        self.assertEqual(movers[0]["building_name"], "공통단지")
        self.assertEqual(movers[0]["total_diff"], 6)
        self.assertEqual(movers[0]["price_down_diff"], 3)

    def test_build_top_building_movers_filters_sorts_and_limits(self):
        raw_changes = [
            self._raw_change("임계치미만", total_diff=2, price_down_diff=2),
            self._raw_change("총량급증", total_diff=5, price_down_diff=0),
            self._raw_change("인하급증", total_diff=0, price_down_diff=4),
            self._raw_change("복합급증", total_diff=6, price_down_diff=3),
        ]

        movers = self.app_module.build_top_building_movers(raw_changes, limit=2)

        self.assertEqual([mover["building_name"] for mover in movers], ["복합급증", "총량급증"])
        self.assertEqual([mover["kind"] for mover in movers], ["both", "total"])

    def _raw_change(self, building_name, *, total_diff, price_down_diff):
        return {
            "district": "서초구",
            "building_name": building_name,
            "current_crawled_at": "2026-08-09T09:00:00",
            "previous_crawled_at": "2026-08-08T09:00:00",
            "current_total": 20 + total_diff,
            "previous_total": 20,
            "total_diff": total_diff,
            "current_price_down": 5 + price_down_diff,
            "previous_price_down": 5,
            "price_down_diff": price_down_diff,
        }

    def test_hero_insight_route_returns_payload_and_cache_header(self):
        response = self.client.get("/api/hero-insight")

        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertIn("price_down_ratio", payload)
        self.assertIn("building_movers", payload)
        self.assertIn("max-age=300", response.headers.get("Cache-Control", ""))

    def test_get_trends_keeps_existing_response_shape_and_values(self):
        self._seed_session(
            "yesterday",
            "2026-08-08T09:00:00",
            region_rows=(
                ("서울특별시", "서초구", 10, 1),
                ("서울특별시", "강남구", 7, 0),
            ),
        )
        self._seed_session(
            "today",
            "2026-08-09T09:00:00",
            region_rows=(
                ("서울특별시", "서초구", 14, 3),
                ("서울특별시", "송파구", 5, 2),
            ),
        )

        trends = self.db.get_trends()

        self.assertEqual(
            trends,
            [
                {
                    "region": "서울특별시",
                    "district": "송파구",
                    "display_name": "서울특별시 송파구",
                    "current_cnt": 5,
                    "prev_cnt": 0,
                    "diff": 5,
                    "price_down_count": 2,
                    "current_date": "2026-08-09",
                    "previous_date": "2026-08-08",
                },
                {
                    "region": "서울특별시",
                    "district": "서초구",
                    "display_name": "서울특별시 서초구",
                    "current_cnt": 14,
                    "prev_cnt": 10,
                    "diff": 4,
                    "price_down_count": 3,
                    "current_date": "2026-08-09",
                    "previous_date": "2026-08-08",
                },
                {
                    "region": "서울특별시",
                    "district": "강남구",
                    "display_name": "서울특별시 강남구",
                    "current_cnt": 0,
                    "prev_cnt": 7,
                    "diff": -7,
                    "price_down_count": 0,
                    "current_date": "2026-08-09",
                    "previous_date": "2026-08-08",
                },
            ],
        )


if __name__ == "__main__":
    unittest.main()
