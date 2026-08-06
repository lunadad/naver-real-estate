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


if __name__ == "__main__":
    unittest.main()
