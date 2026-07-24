"""태그 검색 필터 — DB/API 계층 테스트 (표준 라이브러리 unittest)."""

import os
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from database import Database  # noqa: E402


def make_listing(article_no, tags, **overrides):
    """insert_listings()가 기대하는 형태의 매물 dict를 만든다.

    tags는 파이썬 리스트로 넘긴다 (insert_listings 내부에서 json.dumps 된다).
    """
    listing = {
        "article_no": article_no,
        "region": "서울특별시",
        "district": "강남구",
        "property_type": "아파트",
        "trade_type": "매매",
        "price": "10억",
        "area": "84㎡",
        "floor": "10/20",
        "building_name": f"테스트아파트{article_no}",
        "description": "테스트 설명",
        "is_urgent": 1,
        "tags": tags,
        "confirmed_date": "20260724",
        "latitude": 37.5,
        "longitude": 127.0,
        "naver_url": f"https://example.com/{article_no}",
    }
    listing.update(overrides)
    return listing


class TagFilterDBTest(unittest.TestCase):
    def setUp(self):
        tmpdir = tempfile.TemporaryDirectory()
        self.addCleanup(tmpdir.cleanup)
        self.db = Database(
            db_path=os.path.join(tmpdir.name, "test.db"),
            skip_price_backfill=True,
        )
        self.addCleanup(self.db.close)

        listings = [
            make_listing("A1", ["역세권", "신축"]),
            make_listing("A2", ["역세권", "가격인하"]),
            make_listing("A3", ["대단지"], property_type="오피스텔"),
            make_listing("A4", []),
        ]
        self.db.insert_listings(listings, "session-1")
        self.db.log_crawl("session-1", len(listings), len(listings), "success", "naver")

    def test_tag_counts_sorted_by_count_then_name(self):
        counts = self.db.get_tag_counts()
        self.assertEqual(
            counts,
            [
                {"tag": "역세권", "count": 2},
                {"tag": "가격인하", "count": 1},
                {"tag": "대단지", "count": 1},
                {"tag": "신축", "count": 1},
            ],
        )

    def test_tag_counts_ignores_empty_tag_lists(self):
        tags = [entry["tag"] for entry in self.db.get_tag_counts()]
        self.assertNotIn("", tags)


if __name__ == "__main__":
    unittest.main()
