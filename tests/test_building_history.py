"""단지별 일별 매물수 추이 — app.py 계층 + API 라우트 테스트."""

import os
import sys
import tempfile
import unittest
from datetime import datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from database import Database  # noqa: E402


class BuildingHistoryAPITest(unittest.TestCase):
    """app.py를 임포트하기 전에 환경변수로 로컬 SQLite 모드를 강제한다."""

    @classmethod
    def setUpClass(cls):
        cls._tmpdir = tempfile.TemporaryDirectory()
        db_path = os.path.join(cls._tmpdir.name, "building-history-test.db")

        os.environ["FORCE_LOCAL_SQLITE"] = "1"
        os.environ["DATABASE_URL"] = ""
        os.environ["DB_PATH"] = db_path
        os.environ["ENABLE_SCHEDULER"] = "false"
        os.environ["SEED_DEMO_DATA"] = "false"

        import app as app_module  # noqa: E402

        # `app` is a process-wide sys.modules-cached singleton: `import app` only
        # re-executes on the *first* import in the process, so whichever test file's
        # setUpClass runs first "wins" the shared app_module.db instance and later
        # imports' env vars (DB_PATH etc.) are silently inert. Rebind db explicitly
        # here so this file's tests always use their own isolated tempdir/db,
        # independent of import order or what any other test file already did to
        # the shared `app` module.
        cls.app_module = app_module
        app_module.db = Database(db_path=db_path, skip_price_backfill=True)
        cls.client = app_module.app.test_client()
        cls.today = datetime.now(app_module.KST).date()

    @classmethod
    def tearDownClass(cls):
        # NOTE: `app` is a process-wide singleton module (sys.modules cache), and other
        # test files (e.g. test_tag_filter.py) import it the same way and reuse this same
        # cached module/db instance when running later in the same `unittest discover`
        # process. Closing the connection and deleting the backing tempdir here would
        # break those later imports (they'd hit a deleted sqlite file). So we deliberately
        # leave the shared db/tempdir alive for the rest of the test process; the OS
        # reclaims the tempdir on process exit.
        pass

    def setUp(self):
        # 각 테스트 전 통계 관련 테이블을 비워 테스트 간 데이터가 섞이지 않게 한다.
        with self.app_module.db.get_connection() as conn:
            conn.execute("DELETE FROM crawl_building_stats")
            conn.execute("DELETE FROM crawl_history")

    def _seed_day(self, session_id, days_ago, district, building_name, total_count, price_down_count):
        crawled_at = datetime.combine(
            self.today - timedelta(days=days_ago), datetime.min.time()
        ).replace(hour=9).isoformat()
        with self.app_module.db.get_connection() as conn:
            conn.execute(
                """
                INSERT INTO crawl_history
                (session_id, crawled_at, total_count, urgent_count, status, source)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (session_id, crawled_at, total_count, total_count, "success", "naver"),
            )
            conn.execute(
                """
                INSERT INTO crawl_building_stats
                (session_id, region, district, building_name, total_count, price_down_count, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (session_id, "서울특별시", district, building_name, total_count, price_down_count, crawled_at),
            )

    def test_series_fills_missing_days_with_none(self):
        self._seed_day("s-2days", 2, "서초구", "래미안원베일리", 5, 1)
        self._seed_day("s-today", 0, "서초구", "래미안원베일리", 7, 2)

        series = self.app_module.build_building_history_series("서초구", "래미안원베일리", days=5)

        self.assertEqual(len(series), 5)
        self.assertEqual(series[-1]["total_count"], 7)  # 오늘
        self.assertEqual(series[-3]["total_count"], 5)  # 2일 전
        self.assertIsNone(series[-2]["total_count"])    # 1일 전 데이터 없음
        self.assertIsNone(series[0]["total_count"])     # 4일 전 데이터 없음

    def test_series_picks_latest_session_within_same_day(self):
        self._seed_day("s-morning", 0, "서초구", "래미안원베일리", 5, 0)
        with self.app_module.db.get_connection() as conn:
            # NOTE: naive crawled_at strings are interpreted per NAIVE_DB_TZ (UTC here,
            # since FORCE_LOCAL_SQLITE + empty DATABASE_URL selects UTC), then converted
            # to KST by coerce_kst_datetime (+9h). hour=12 -> 21:00 KST, safely later than
            # the morning seed (hour=9 -> 18:00 KST) while staying within the same KST day.
            later = datetime.combine(self.today, datetime.min.time()).replace(hour=12).isoformat()
            conn.execute(
                """
                INSERT INTO crawl_history
                (session_id, crawled_at, total_count, urgent_count, status, source)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                ("s-evening", later, 8, 8, "success", "naver"),
            )
            conn.execute(
                """
                INSERT INTO crawl_building_stats
                (session_id, region, district, building_name, total_count, price_down_count, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                ("s-evening", "서울특별시", "서초구", "래미안원베일리", 8, 3, later),
            )

        series = self.app_module.build_building_history_series("서초구", "래미안원베일리", days=3)
        self.assertEqual(series[-1]["total_count"], 8)

    def test_series_for_unknown_building_is_all_none(self):
        series = self.app_module.build_building_history_series("서초구", "존재하지않는단지", days=5)
        self.assertEqual(len(series), 5)
        self.assertTrue(all(item["total_count"] is None for item in series))

    def test_route_requires_district_and_building_name(self):
        response = self.client.get("/api/building-history")
        self.assertEqual(response.status_code, 400)
        payload = response.get_json()
        self.assertEqual(payload["status"], "error")

    def test_route_returns_series_with_cache_header(self):
        self._seed_day("s-today", 0, "서초구", "래미안원베일리", 5, 1)

        response = self.client.get(
            "/api/building-history",
            query_string={"district": "서초구", "building_name": "래미안원베일리", "days": 5},
        )
        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertEqual(payload["district"], "서초구")
        self.assertEqual(payload["building_name"], "래미안원베일리")
        self.assertEqual(len(payload["days"]), 5)
        self.assertEqual(payload["days"][-1]["total_count"], 5)
        self.assertIn("max-age=300", response.headers.get("Cache-Control", ""))

    def test_route_returns_today_vs_previous_seven_day_average(self):
        for days_ago, total_count in enumerate([15, 14, 13, 12, 11, 10, 9, 8]):
            self._seed_day(
                f"s-{days_ago}",
                days_ago,
                "서초구",
                "래미안원베일리",
                total_count,
                0,
            )

        payload = self.client.get(
            "/api/building-history",
            query_string={"district": "서초구", "building_name": "래미안원베일리", "days": 14},
        ).get_json()

        self.assertEqual(payload["summary"]["today_count"], 15)
        self.assertEqual(payload["summary"]["average_count"], 11)
        self.assertEqual(payload["summary"]["difference"], 4)
        self.assertEqual(payload["summary"]["sample_count"], 7)

    def test_route_summary_is_none_without_today_snapshot(self):
        self._seed_day("s-yesterday", 1, "서초구", "래미안원베일리", 10, 0)

        payload = self.client.get(
            "/api/building-history",
            query_string={"district": "서초구", "building_name": "래미안원베일리", "days": 14},
        ).get_json()

        self.assertIsNone(payload["summary"])


if __name__ == "__main__":
    unittest.main()
