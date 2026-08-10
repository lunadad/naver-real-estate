"""매물 메타데이터 backfill — 일괄(batched) UPDATE 동작 검증.

`_backfill_price_sort_values`와 `_backfill_commercial_metadata`는 예전에
행(row) 하나당 UPDATE 쿼리를 하나씩 날리는 N+1 패턴이었다. 이 테스트는
(1) 배치로 바뀐 뒤에도 결과가 기존과 동일한지, (2) 실제로 라운드트립
횟수가 행 수가 아니라 청크(chunk) 수에 비례하는지를 검증한다.
"""

import os
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from database import Database


def make_listing(article_no, **overrides):
    """listings 테이블에 직접 넣을 수 있는 완전한 컬럼 dict를 만든다.

    insert_listings()를 거치지 않고 conn.execute()로 직접 삽입해, 어떤
    컬럼이 NULL인 상태를 테스트가 정확히 통제할 수 있게 한다.
    """
    row = {
        "article_no": article_no,
        "region": "서울특별시",
        "district": "강남구",
        "property_type": "상가",
        "trade_type": "월세",
        "price": "5000/300",
        "area": "33㎡",
        "floor": "1",
        "building_name": "테스트빌딩",
        "description": "",
        "is_urgent": 1,
        "tags": "[]",
        "confirmed_date": "20260810",
        "crawled_at": "2026-08-10T00:00:00",
        "crawl_session": "session-1",
        "latitude": 37.5,
        "longitude": 127.0,
        "naver_url": "",
        "price_sort_value": None,
        "rent_sort_value": None,
        "raw_property_code": None,
        "area_m2": None,
        "land_use_zone": None,
        "land_category": None,
        "road_access": None,
        "premium_info": None,
        "estimated_yield_rate": None,
        "price_drop_rate": None,
    }
    row.update(overrides)
    return row


class BackfillTestBase(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()
        self.db_path = os.path.join(self.tmpdir, "test.db")
        self.db = Database(db_path=self.db_path)

    def insert_row(self, listing):
        columns = list(listing.keys())
        placeholders = ", ".join(["?"] * len(columns))
        col_sql = ", ".join(columns)
        with self.db.get_connection() as conn:
            conn.execute(
                f"INSERT INTO listings ({col_sql}) VALUES ({placeholders})",
                [listing[c] for c in columns],
            )

    def fetch_by_article_no(self, article_no):
        with self.db.get_connection() as conn:
            return conn.execute(
                "SELECT * FROM listings WHERE article_no = ?", (article_no,)
            ).fetchone()


class PriceSortBackfillTest(BackfillTestBase):
    def test_fills_price_and_rent_for_monthly_rent_listing(self):
        self.insert_row(make_listing("A1", price="5000/300", trade_type="월세"))

        with self.db.get_connection() as conn:
            self.db._backfill_price_sort_values(conn)

        row = self.fetch_by_article_no("A1")
        self.assertEqual(row["price_sort_value"], 5000)
        self.assertEqual(row["rent_sort_value"], 300)

    def test_fills_price_only_for_sale_listing_and_leaves_rent_null(self):
        self.insert_row(make_listing("A2", price="30000", trade_type="매매"))

        with self.db.get_connection() as conn:
            self.db._backfill_price_sort_values(conn)

        row = self.fetch_by_article_no("A2")
        self.assertEqual(row["price_sort_value"], 30000)
        self.assertIsNone(row["rent_sort_value"])

    def test_skips_rows_that_already_have_values(self):
        self.insert_row(
            make_listing(
                "A3", price="9000/500", trade_type="월세",
                price_sort_value=1, rent_sort_value=1,
            )
        )

        with self.db.get_connection() as conn:
            self.db._backfill_price_sort_values(conn)

        row = self.fetch_by_article_no("A3")
        # 이미 값이 있으면 대상 SELECT에 잡히지 않으므로 그대로 유지된다.
        self.assertEqual(row["price_sort_value"], 1)
        self.assertEqual(row["rent_sort_value"], 1)

    def test_batches_updates_instead_of_one_round_trip_per_row(self):
        for i in range(1200):
            self.insert_row(make_listing(f"B{i}", price="10000", trade_type="매매"))

        with self.db.get_connection() as conn:
            calls = []
            original_execute = conn.execute

            def counting_execute(sql, params=None):
                if "UPDATE" in sql.upper():
                    calls.append(sql)
                return original_execute(sql, params)

            conn.execute = counting_execute
            self.db._backfill_price_sort_values(conn)

        # 1200개 행이라도 청크(500개 단위) 수만큼만 UPDATE가 실행돼야 한다.
        self.assertEqual(len(calls), 3)  # ceil(1200 / 500)

        with self.db.get_connection() as conn:
            remaining = conn.execute(
                "SELECT COUNT(*) AS c FROM listings WHERE price_sort_value IS NULL"
            ).fetchone()["c"]
        self.assertEqual(remaining, 0)


class CommercialMetadataBackfillTest(BackfillTestBase):
    def test_fills_raw_property_code_from_property_type(self):
        self.insert_row(make_listing("C1", property_type="상가"))

        with self.db.get_connection() as conn:
            self.db._backfill_commercial_metadata(conn)

        row = self.fetch_by_article_no("C1")
        self.assertEqual(row["raw_property_code"], "OBYG")

    def test_fills_area_m2_from_area_text(self):
        self.insert_row(make_listing("C2", area="10평"))

        with self.db.get_connection() as conn:
            self.db._backfill_commercial_metadata(conn)

        row = self.fetch_by_article_no("C2")
        self.assertAlmostEqual(row["area_m2"], 33.06, places=1)

    def test_coalesced_columns_preserve_existing_non_null_value(self):
        # road_access가 이미 채워져 있으면, 새로 추론한 값이 있어도 원래 값이 유지돼야
        # 한다 (COALESCE(new, existing) 순서).
        self.insert_row(
            make_listing(
                "C3",
                description="맹지 주의",  # 추론하면 "맹지 유의"가 나올 텍스트
                road_access="기존값",
                area_m2=None,  # area_m2 NULL이라 WHERE 조건에 걸림
            )
        )

        with self.db.get_connection() as conn:
            self.db._backfill_commercial_metadata(conn)

        row = self.fetch_by_article_no("C3")
        self.assertEqual(row["road_access"], "기존값")

    def test_area_m2_is_recomputed_even_when_other_stale_value_present(self):
        # 원본 로직: area_m2는 COALESCE 없이 매번 다시 계산해 덮어쓴다.
        # area_m2가 0(falsy)이면 area 텍스트에서 새로 파싱한 값으로 대체된다.
        self.insert_row(
            make_listing(
                "C4",
                property_type="상가",
                area="20평",
                area_m2=0,
                road_access=None,  # WHERE 조건을 만족시키기 위함
            )
        )

        with self.db.get_connection() as conn:
            self.db._backfill_commercial_metadata(conn)

        row = self.fetch_by_article_no("C4")
        self.assertGreater(row["area_m2"], 0)

    def test_batches_updates_instead_of_one_round_trip_per_row(self):
        for i in range(1200):
            self.insert_row(make_listing(f"D{i}", property_type="상가"))

        with self.db.get_connection() as conn:
            calls = []
            original_execute = conn.execute

            def counting_execute(sql, params=None):
                if "UPDATE" in sql.upper():
                    calls.append(sql)
                return original_execute(sql, params)

            conn.execute = counting_execute
            self.db._backfill_commercial_metadata(conn)

        self.assertEqual(len(calls), 3)  # ceil(1200 / 500)

        with self.db.get_connection() as conn:
            remaining = conn.execute(
                "SELECT COUNT(*) AS c FROM listings WHERE raw_property_code IS NULL"
            ).fetchone()["c"]
        self.assertEqual(remaining, 0)

    def test_no_rows_needing_backfill_issues_no_update(self):
        self.insert_row(
            make_listing(
                "E1",
                property_type="상가",
                raw_property_code="OBYG",
                area_m2=33.0,
                premium_info="무권리",
                road_access="대로변",
            )
        )

        with self.db.get_connection() as conn:
            calls = []
            original_execute = conn.execute

            def counting_execute(sql, params=None):
                if "UPDATE" in sql.upper():
                    calls.append(sql)
                return original_execute(sql, params)

            conn.execute = counting_execute
            self.db._backfill_commercial_metadata(conn)

        self.assertEqual(calls, [])


if __name__ == "__main__":
    unittest.main()
