"""단지 일별 매물수 변화 알림 규칙과 delivery 테스트."""

import os
import sqlite3
import tempfile
import unittest
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from database import Database  # noqa: E402


class BuildingChangeAlertMigrationTest(unittest.TestCase):
    def test_existing_sqlite_alert_schema_is_migrated(self):
        tmpdir = tempfile.TemporaryDirectory()
        self.addCleanup(tmpdir.cleanup)
        db_path = os.path.join(tmpdir.name, "old-schema.db")
        conn = sqlite3.connect(db_path)
        try:
            conn.execute(
                """
                CREATE TABLE alert_rules (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    client_id TEXT NOT NULL,
                    name TEXT NOT NULL,
                    keyword TEXT,
                    district TEXT,
                    property_type TEXT,
                    trade_type TEXT,
                    enabled INTEGER DEFAULT 1,
                    created_at TEXT
                )
                """
            )
            conn.commit()
        finally:
            conn.close()

        db = Database(db_path=db_path, skip_price_backfill=True)
        self.addCleanup(db.close)
        with db.get_connection() as conn:
            columns = {row["name"] for row in conn.execute("PRAGMA table_info(alert_rules)").fetchall()}
            delivery_table = conn.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table' AND name = ?",
                ("building_alert_deliveries",),
            ).fetchone()

        self.assertIn("building_name", columns)
        self.assertIn("min_daily_change", columns)
        self.assertIsNotNone(delivery_table)


class BuildingChangeAlertTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls._tmpdir = tempfile.TemporaryDirectory()
        db_path = os.path.join(cls._tmpdir.name, "building-change-alert.db")
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
        pass

    def setUp(self):
        with self.db.get_connection() as conn:
            conn.execute("DELETE FROM building_alert_deliveries")
            conn.execute("DELETE FROM alert_deliveries")
            conn.execute("DELETE FROM alert_rules")
            conn.execute("DELETE FROM crawl_building_stats")
            conn.execute("DELETE FROM crawl_history")
            conn.execute("DELETE FROM listings")

    def _seed_snapshot(self, session_id, crawled_at, total_count):
        with self.db.get_connection() as conn:
            conn.execute(
                """
                INSERT INTO crawl_history
                (session_id, crawled_at, total_count, urgent_count, status, source)
                VALUES (?, ?, ?, 0, 'success', 'naver')
                """,
                (session_id, crawled_at, total_count),
            )
            conn.execute(
                """
                INSERT INTO crawl_building_stats
                (session_id, region, district, building_name, total_count, price_down_count, created_at)
                VALUES (?, '서울특별시', '서초구', '래미안원베일리', ?, 0, ?)
                """,
                (session_id, total_count, crawled_at),
            )

    def _create_rule(self, threshold):
        rule = self.db.create_alert_rule(
            client_id="client-building",
            district="서초구",
            building_name="래미안원베일리",
            min_daily_change=threshold,
        )
        with self.db.get_connection() as conn:
            conn.execute(
                "UPDATE alert_rules SET created_at = ? WHERE id = ?",
                ("2026-08-06T00:00:00", rule["id"]),
            )
        return rule

    def test_api_creates_building_change_rule(self):
        response = self.client.post(
            "/api/alert-rules",
            json={
                "client_id": "client-api",
                "district": "서초구",
                "building_name": "래미안원베일리",
                "min_daily_change": 4,
            },
        )

        self.assertEqual(response.status_code, 200)
        rule = response.get_json()["rule"]
        self.assertEqual(rule["building_name"], "래미안원베일리")
        self.assertEqual(rule["min_daily_change"], 4)

    def test_api_requires_complete_building_change_condition(self):
        response = self.client.post(
            "/api/alert-rules",
            json={"client_id": "client-api", "district": "서초구", "min_daily_change": 3},
        )
        self.assertEqual(response.status_code, 400)

    def test_absolute_daily_change_triggers_once_per_current_session(self):
        self._create_rule(threshold=5)
        self._seed_snapshot("previous", "2026-08-07T09:00:00", 15)
        self._seed_snapshot("current", "2026-08-08T09:00:00", 10)

        first = self.db.get_new_alert_matches("client-building")
        second = self.db.get_new_alert_matches("client-building")

        self.assertEqual(len(first), 1)
        self.assertEqual(first[0]["event_type"], "building_daily_change")
        self.assertEqual(first[0]["change_count"], -5)
        self.assertEqual(first[0]["current_session_id"], "current")
        self.assertEqual(second, [])

    def test_change_below_threshold_does_not_trigger(self):
        self._create_rule(threshold=5)
        self._seed_snapshot("previous", "2026-08-07T09:00:00", 10)
        self._seed_snapshot("current", "2026-08-08T09:00:00", 14)

        self.assertEqual(self.db.get_new_alert_matches("client-building"), [])

    def test_non_consecutive_snapshots_do_not_count_as_daily_change(self):
        self._create_rule(threshold=3)
        self._seed_snapshot("previous", "2026-08-05T09:00:00", 10)
        self._seed_snapshot("current", "2026-08-08T09:00:00", 20)

        self.assertEqual(self.db.get_new_alert_matches("client-building"), [])

    def test_existing_listing_rule_still_matches_new_listing(self):
        rule = self.db.create_alert_rule(client_id="client-listing", keyword="래미안")
        with self.db.get_connection() as conn:
            conn.execute(
                "UPDATE alert_rules SET created_at = ? WHERE id = ?",
                ("2026-08-07T00:00:00", rule["id"]),
            )
            conn.execute(
                """
                INSERT INTO crawl_history
                (session_id, crawled_at, total_count, urgent_count, status, source)
                VALUES ('current', '2026-08-08T09:00:00', 1, 1, 'success', 'naver')
                """
            )
            conn.execute(
                """
                INSERT INTO listings
                (article_no, region, district, property_type, trade_type, price,
                 building_name, description, is_urgent, tags, confirmed_date,
                 crawled_at, crawl_session)
                VALUES ('listing-1', '서울특별시', '서초구', '아파트', '매매', '10억',
                        '래미안원베일리', '', 1, '[]', '20260808',
                        '2026-08-08T09:00:00', 'current')
                """
            )

        matches = self.db.get_new_alert_matches("client-listing")

        self.assertEqual([match["article_no"] for match in matches], ["listing-1"])
        self.assertNotIn("event_type", matches[0])

    def test_push_payload_describes_building_change(self):
        payload = self.app_module.build_push_payload(
            [
                {
                    "event_type": "building_daily_change",
                    "article_no": "building-change:current",
                    "alert_names": ["래미안원베일리 하루 ±5건"],
                    "district": "서초구",
                    "building_name": "래미안원베일리",
                    "previous_count": 10,
                    "current_count": 15,
                    "change_count": 5,
                    "naver_url": "/",
                }
            ]
        )
        self.assertIn("10→15건", payload["body"])
        self.assertIn("+5", payload["body"])


if __name__ == "__main__":
    unittest.main()
