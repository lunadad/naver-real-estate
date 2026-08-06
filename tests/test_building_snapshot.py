"""건물 단위 일별 매물 스냅샷 — DB 계층 테스트 (표준 라이브러리 unittest)."""

import os
import sys
import tempfile
import unittest
from datetime import datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from database import Database  # noqa: E402


def make_listing(article_no, **overrides):
    """insert_listings()가 기대하는 형태의 매물 dict를 만든다.

    district·building_name 기본값이 같으므로, 같은 건물에 여러 매물을 만들려면
    article_no만 바꿔 호출하면 된다. 다른 건물을 만들려면 building_name을 override.
    """
    listing = {
        "article_no": article_no,
        "region": "서울특별시",
        "district": "서초구",
        "property_type": "아파트",
        "trade_type": "매매",
        "price": "10억",
        "area": "84㎡",
        "floor": "10/20",
        "building_name": "테스트단지",
        "description": "테스트 설명",
        "is_urgent": 1,
        "tags": [],
        "confirmed_date": "20260806",
        "latitude": 37.5,
        "longitude": 127.0,
        "naver_url": f"https://example.com/{article_no}",
    }
    listing.update(overrides)
    return listing


class BuildingSnapshotSchemaTest(unittest.TestCase):
    def setUp(self):
        tmpdir = tempfile.TemporaryDirectory()
        self.addCleanup(tmpdir.cleanup)
        self.db = Database(
            db_path=os.path.join(tmpdir.name, "test.db"),
            skip_price_backfill=True,
        )
        self.addCleanup(self.db.close)

    def test_crawl_building_stats_table_accepts_expected_columns(self):
        with self.db.get_connection() as conn:
            conn.execute(
                """
                INSERT INTO crawl_building_stats
                (session_id, region, district, building_name, total_count, price_down_count, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                ("session-1", "서울특별시", "서초구", "래미안원베일리", 5, 2, "2026-08-06T09:00:00"),
            )
        with self.db.get_connection() as conn:
            row = conn.execute(
                """
                SELECT session_id, region, district, building_name, total_count, price_down_count
                FROM crawl_building_stats
                """
            ).fetchone()
        self.assertEqual(row["session_id"], "session-1")
        self.assertEqual(row["district"], "서초구")
        self.assertEqual(row["building_name"], "래미안원베일리")
        self.assertEqual(row["total_count"], 5)
        self.assertEqual(row["price_down_count"], 2)


class BuildingStatsRowsTest(unittest.TestCase):
    def setUp(self):
        tmpdir = tempfile.TemporaryDirectory()
        self.addCleanup(tmpdir.cleanup)
        self.db = Database(
            db_path=os.path.join(tmpdir.name, "test.db"),
            skip_price_backfill=True,
        )
        self.addCleanup(self.db.close)

    def test_building_with_two_or_more_listings_is_included(self):
        listings = [
            make_listing("B1", building_name="래미안원베일리"),
            make_listing("B2", building_name="래미안원베일리"),
        ]
        rows = self.db._build_building_stats_rows("session-1", listings, "2026-08-06T09:00:00")
        self.assertEqual(len(rows), 1)
        row = rows[0]
        self.assertEqual(row["district"], "서초구")
        self.assertEqual(row["building_name"], "래미안원베일리")
        self.assertEqual(row["total_count"], 2)
        self.assertEqual(row["price_down_count"], 0)

    def test_building_with_single_listing_is_excluded(self):
        listings = [make_listing("B1", building_name="반포자이")]
        rows = self.db._build_building_stats_rows("session-1", listings, "2026-08-06T09:00:00")
        self.assertEqual(rows, [])

    def test_listing_missing_district_or_building_name_is_skipped(self):
        listings = [
            make_listing("B1", district="", building_name="반포자이"),
            make_listing("B2", district="서초구", building_name=""),
            make_listing("B3", building_name="반포자이"),
            make_listing("B4", building_name="반포자이"),
        ]
        rows = self.db._build_building_stats_rows("session-1", listings, "2026-08-06T09:00:00")
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["building_name"], "반포자이")
        self.assertEqual(rows[0]["total_count"], 2)  # B3, B4만 집계됨

    def test_price_down_tag_is_counted(self):
        listings = [
            make_listing("B1", building_name="반포자이", tags=["가격인하"]),
            make_listing("B2", building_name="반포자이", tags=[]),
        ]
        rows = self.db._build_building_stats_rows("session-1", listings, "2026-08-06T09:00:00")
        self.assertEqual(rows[0]["price_down_count"], 1)


class BuildingSnapshotInsertTest(unittest.TestCase):
    def setUp(self):
        tmpdir = tempfile.TemporaryDirectory()
        self.addCleanup(tmpdir.cleanup)
        self.db = Database(
            db_path=os.path.join(tmpdir.name, "test.db"),
            skip_price_backfill=True,
        )
        self.addCleanup(self.db.close)

    def _building_stats_rows(self):
        with self.db.get_connection() as conn:
            return conn.execute(
                """
                SELECT session_id, district, building_name, total_count, price_down_count, created_at
                FROM crawl_building_stats
                ORDER BY building_name
                """
            ).fetchall()

    def test_insert_listings_persists_stats_for_qualifying_buildings_only(self):
        listings = [
            make_listing("A1", building_name="래미안원베일리"),
            make_listing("A2", building_name="래미안원베일리"),
            make_listing("A3", building_name="반포자이"),  # 1건뿐 -> 제외되어야 함
        ]
        self.db.insert_listings(listings, "session-1")

        rows = self._building_stats_rows()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["building_name"], "래미안원베일리")
        self.assertEqual(rows[0]["total_count"], 2)

    def test_reinserting_same_session_upserts_instead_of_duplicating(self):
        listings_v1 = [
            make_listing("A1", building_name="래미안원베일리"),
            make_listing("A2", building_name="래미안원베일리"),
        ]
        self.db.insert_listings(listings_v1, "session-1")

        listings_v2 = [
            make_listing("A3", building_name="래미안원베일리"),
            make_listing("A4", building_name="래미안원베일리"),
            make_listing("A5", building_name="래미안원베일리"),
        ]
        self.db.insert_listings(listings_v2, "session-1")

        rows = self._building_stats_rows()
        self.assertEqual(len(rows), 1)  # 중복 행 없이 하나로 유지
        self.assertEqual(rows[0]["total_count"], 3)  # 최신 값으로 갱신

    def test_stats_older_than_180_days_are_pruned_on_next_insert(self):
        old_date = (datetime.now() - timedelta(days=200)).isoformat()
        with self.db.get_connection() as conn:
            conn.execute(
                """
                INSERT INTO crawl_building_stats
                (session_id, region, district, building_name, total_count, price_down_count, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                ("old-session", "서울특별시", "서초구", "오래된단지", 3, 0, old_date),
            )

        listings = [
            make_listing("A1", building_name="래미안원베일리"),
            make_listing("A2", building_name="래미안원베일리"),
        ]
        self.db.insert_listings(listings, "session-new")

        rows = self._building_stats_rows()
        names = [row["building_name"] for row in rows]
        self.assertNotIn("오래된단지", names)
        self.assertIn("래미안원베일리", names)


if __name__ == "__main__":
    unittest.main()
