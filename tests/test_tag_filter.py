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

    def article_nos(self, result):
        return sorted(row["article_no"] for row in result["listings"])

    def test_single_tag_filters_listings(self):
        result = self.db.get_listings(tags=["역세권"])
        self.assertEqual(self.article_nos(result), ["A1", "A2"])
        self.assertEqual(result["total"], 2)

    def test_multiple_tags_use_or_matching(self):
        result = self.db.get_listings(tags=["역세권", "대단지"])
        self.assertEqual(self.article_nos(result), ["A1", "A2", "A3"])

    def test_empty_tag_list_returns_everything(self):
        result = self.db.get_listings(tags=[])
        self.assertEqual(self.article_nos(result), ["A1", "A2", "A3", "A4"])

    def test_unknown_tag_returns_no_rows(self):
        result = self.db.get_listings(tags=["존재하지않는태그"])
        self.assertEqual(result["listings"], [])
        self.assertEqual(result["total"], 0)

    def test_tag_matching_is_exact_not_substring(self):
        """'역'은 '역세권'의 부분 문자열이지만 매칭되면 안 된다."""
        result = self.db.get_listings(tags=["역"])
        self.assertEqual(result["listings"], [])

    def test_tags_combine_with_other_filters_as_and(self):
        result = self.db.get_listings(tags=["역세권", "대단지"], property_type="오피스텔")
        self.assertEqual(self.article_nos(result), ["A3"])

    def test_tags_combine_with_price_down_only(self):
        result = self.db.get_listings(tags=["역세권"], price_down_only=True)
        self.assertEqual(self.article_nos(result), ["A2"])

    def test_map_listings_filtered_by_tags(self):
        rows = self.db.get_map_listings(tags=["대단지"])
        self.assertEqual([row["article_no"] for row in rows], ["A3"])

    def test_map_listings_without_tags_returns_all_geocoded(self):
        rows = self.db.get_map_listings()
        self.assertEqual(len(rows), 4)


class TagFilterAPITest(unittest.TestCase):
    """app.py를 임포트하기 전에 환경변수로 로컬 SQLite 모드를 강제한다."""

    @classmethod
    def setUpClass(cls):
        cls._tmpdir = tempfile.TemporaryDirectory()
        db_path = os.path.join(cls._tmpdir.name, "api-test.db")

        os.environ["FORCE_LOCAL_SQLITE"] = "1"
        os.environ["DATABASE_URL"] = ""
        os.environ["DB_PATH"] = db_path
        os.environ["ENABLE_SCHEDULER"] = "false"
        os.environ["SEED_DEMO_DATA"] = "false"

        import app as app_module  # noqa: E402

        cls.app_module = app_module
        cls.client = app_module.app.test_client()

        listings = [
            make_listing("B1", ["역세권", "신축"]),
            make_listing("B2", ["대단지"]),
        ]
        app_module.db.insert_listings(listings, "api-session")
        app_module.db.log_crawl("api-session", 2, 2, "success", "naver")

    @classmethod
    def tearDownClass(cls):
        cls.app_module.db.close()
        cls._tmpdir.cleanup()

    def test_parse_tag_args_strips_and_drops_blanks(self):
        self.assertEqual(
            self.app_module.parse_tag_args(" 역세권 , ,신축,"),
            ["역세권", "신축"],
        )
        self.assertEqual(self.app_module.parse_tag_args(""), [])

    def test_tags_endpoint_returns_counts(self):
        response = self.client.get("/api/tags")
        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertEqual(
            payload["tags"],
            [
                {"tag": "대단지", "count": 1},
                {"tag": "신축", "count": 1},
                {"tag": "역세권", "count": 1},
            ],
        )

    def test_listings_endpoint_accepts_tags_param(self):
        payload = self.client.get("/api/listings?tags=대단지").get_json()
        self.assertEqual([row["article_no"] for row in payload["listings"]], ["B2"])

    def test_listings_endpoint_ignores_blank_tags_param(self):
        payload = self.client.get("/api/listings?tags=").get_json()
        self.assertEqual(payload["total"], 2)

    def test_map_listings_endpoint_accepts_tags_param(self):
        payload = self.client.get("/api/map-listings?tags=역세권").get_json()
        self.assertEqual([row["article_no"] for row in payload["listings"]], ["B1"])


if __name__ == "__main__":
    unittest.main()
