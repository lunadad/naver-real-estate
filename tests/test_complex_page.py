"""단지 상세 페이지 — /api/complex, /complex 테스트."""

import os
import sys
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from database import Database  # noqa: E402

DISTRICT = "서초구"
COMPLEX = "래미안원베일리"


class ComplexPageTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls._tmpdir = tempfile.TemporaryDirectory()
        db_path = os.path.join(cls._tmpdir.name, "complex-page.db")
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
        # 로컬 SQLite의 naive 시각은 UTC로 해석되므로 UTC 기준 now로 심는다.
        self.now = datetime.now(timezone.utc).replace(tzinfo=None, microsecond=0)

    # ── seed helpers ────────────────────────────────────────────────────────
    def _session(self, session_id, days_ago, status="success", source="naver"):
        crawled_at = (self.now - timedelta(days=days_ago)).isoformat()
        with self.db.get_connection() as conn:
            conn.execute(
                "INSERT INTO crawl_history (session_id, crawled_at, total_count, urgent_count, status, source) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                (session_id, crawled_at, 0, 0, status, source),
            )
        return crawled_at

    def _building_stat(self, session_id, crawled_at, total, down=0, name=COMPLEX, district=DISTRICT):
        with self.db.get_connection() as conn:
            conn.execute(
                "INSERT INTO crawl_building_stats "
                "(session_id, region, district, building_name, total_count, price_down_count, created_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?)",
                (session_id, "서울특별시", district, name, total, down, crawled_at),
            )

    def _listing(self, article_no, session_id, crawled_at, *, price_value, area="84A", discount=None,
                 trade_type="매매", name=COMPLEX, district=DISTRICT, tags="[]", floor="10/20"):
        with self.db.get_connection() as conn:
            conn.execute(
                """
                INSERT INTO listings
                (article_no, region, district, property_type, trade_type, price, area, floor,
                 building_name, description, is_urgent, tags, confirmed_date, crawled_at,
                 crawl_session, naver_url, price_sort_value, bargain_discount, bargain_basis)
                VALUES (?, '서울특별시', ?, '아파트', ?, ?, ?, ?, ?, '', 1, ?, '26.10.20', ?, ?, ?, ?, ?, 'same_area')
                """,
                (
                    article_no, district, trade_type, f"{price_value}만", area, floor, name, tags,
                    crawled_at, session_id, f"https://new.land.naver.com/{article_no}",
                    price_value, discount,
                ),
            )

    def _seed_basic(self):
        old_at = self._session("s-old", 7)
        self._building_stat("s-old", old_at, 4)
        now_at = self._session("s-now", 0)
        self._building_stat("s-now", now_at, 6, down=2)
        # 84A: 5건, 중앙값 100000
        for i, (price, disc) in enumerate(
            [(80000, 0.20), (95000, 0.05), (100000, 0.0), (105000, -0.05), (110000, -0.10)]
        ):
            tags = '["가격인하"]' if i < 2 else "[]"
            self._listing(f"a{i}", "s-now", now_at, price_value=price, discount=disc, tags=tags)
        # 59B: 1건 (진짜 급매 아님), 전세는 분포 제외, 다른 단지는 제외
        self._listing("b0", "s-now", now_at, price_value=70000, area="59B", discount=None)
        self._listing("j0", "s-now", now_at, price_value=50000, trade_type="전세", discount=None)
        self._listing("x0", "s-now", now_at, price_value=1, name="다른단지", discount=0.3)
        return now_at

    def _get(self, **params):
        return self.client.get("/api/complex", query_string=params)

    # ── tests ───────────────────────────────────────────────────────────────
    def test_missing_params_returns_400(self):
        self.assertEqual(self._get(district=DISTRICT).status_code, 400)
        self.assertEqual(self._get(building_name=COMPLEX).status_code, 400)

    def test_unknown_complex_returns_404(self):
        self._seed_basic()
        response = self._get(district=DISTRICT, building_name="없는단지")
        self.assertEqual(response.status_code, 404)
        self.assertEqual(response.get_json()["status"], "error")

    def test_stats_include_week_ago_count_from_7_day_old_snapshot(self):
        self._seed_basic()
        data = self._get(district=DISTRICT, building_name=COMPLEX).get_json()
        stats = data["stats"]
        self.assertEqual(data["district"], DISTRICT)
        self.assertEqual(data["building_name"], COMPLEX)
        self.assertEqual(data["region"], "서울특별시")
        self.assertEqual(stats["current_count"], 7)  # 매매 6 + 전세 1
        self.assertEqual(stats["week_ago_count"], 4)
        self.assertEqual(stats["bargain_count"], 1)
        self.assertAlmostEqual(stats["price_down_ratio"], 2 / 7, places=3)

    def test_week_ago_count_is_none_without_old_session(self):
        now_at = self._session("s-now", 0)
        self._listing("a0", "s-now", now_at, price_value=100000)
        stats = self._get(district=DISTRICT, building_name=COMPLEX).get_json()["stats"]
        self.assertIsNone(stats["week_ago_count"])

    def test_week_ago_session_ignores_sessions_outside_12h_window(self):
        old_at = self._session("s-3d", 3)  # 7일 전 ±12h 밖
        self._building_stat("s-3d", old_at, 9)
        now_at = self._session("s-now", 0)
        self._listing("a0", "s-now", now_at, price_value=100000)
        stats = self._get(district=DISTRICT, building_name=COMPLEX).get_json()["stats"]
        self.assertIsNone(stats["week_ago_count"])

    def test_week_ago_count_is_zero_when_session_has_no_row_for_complex(self):
        self._session("s-old", 7)
        now_at = self._session("s-now", 0)
        self._listing("a0", "s-now", now_at, price_value=100000)
        stats = self._get(district=DISTRICT, building_name=COMPLEX).get_json()["stats"]
        self.assertEqual(stats["week_ago_count"], 0)

    def test_areas_grouped_with_median_and_bargain_flags(self):
        self._seed_basic()
        areas = self._get(district=DISTRICT, building_name=COMPLEX).get_json()["areas"]
        self.assertEqual([a["area"] for a in areas], ["59B", "84A"])
        by_area = {a["area"]: a for a in areas}
        self.assertEqual(by_area["84A"]["median"], 100000)
        self.assertEqual(
            [(p["value"], p["bargain"]) for p in by_area["84A"]["prices"]],
            [(80000, True), (95000, False), (100000, False), (105000, False), (110000, False)],
        )
        self.assertEqual(by_area["59B"]["median"], 70000)
        self.assertEqual(len(by_area["59B"]["prices"]), 1)  # 전세 제외

    def test_bargain_threshold_uses_is_bargain_range(self):
        now_at = self._session("s-now", 0)
        for i, disc in enumerate([0.10, 0.40, 0.41, 0.09]):
            self._listing(f"a{i}", "s-now", now_at, price_value=1000 + i, discount=disc)
        data = self._get(district=DISTRICT, building_name=COMPLEX).get_json()
        flags = {p["value"]: p["bargain"] for p in data["areas"][0]["prices"]}
        self.assertEqual(flags, {1000: True, 1001: True, 1002: False, 1003: False})
        self.assertEqual(data["stats"]["bargain_count"], 2)

    def test_listings_put_bargains_first_by_discount(self):
        self._seed_basic()
        listings = self._get(district=DISTRICT, building_name=COMPLEX).get_json()["listings"]
        self.assertEqual(len(listings), 7)
        self.assertEqual(listings[0]["article_no"], "a0")
        self.assertTrue(listings[0]["is_bargain"])
        self.assertEqual(listings[0]["bargain_discount"], 0.2)
        self.assertEqual(listings[0]["naver_url"], "https://new.land.naver.com/a0")
        self.assertTrue(all(not item["is_bargain"] for item in listings[1:]))
        rest = [item["bargain_discount"] for item in listings[1:] if item["bargain_discount"] is not None]
        self.assertEqual(rest, sorted(rest, reverse=True))

    def test_history_series_has_14_days(self):
        self._seed_basic()
        history = self._get(district=DISTRICT, building_name=COMPLEX).get_json()["history"]
        self.assertEqual(len(history), 14)
        self.assertEqual(history[-1]["total_count"], 6)

    def test_only_latest_session_listings_are_used(self):
        old_at = self._session("s-old", 1)
        self._listing("old1", "s-old", old_at, price_value=1)
        now_at = self._session("s-now", 0)
        self._listing("new1", "s-now", now_at, price_value=100000)
        data = self._get(district=DISTRICT, building_name=COMPLEX).get_json()
        self.assertEqual([l["article_no"] for l in data["listings"]], ["new1"])

    def test_response_is_cacheable(self):
        self._seed_basic()
        response = self._get(district=DISTRICT, building_name=COMPLEX)
        self.assertEqual(response.headers["Cache-Control"], "public, max-age=300")

    def test_page_route_renders(self):
        response = self.client.get("/complex", query_string={"district": DISTRICT, "name": COMPLEX})
        self.assertEqual(response.status_code, 200)
        html = response.get_data(as_text=True)
        self.assertIn("js/complex.js", html)
        self.assertIn("← 급매 목록", html)


if __name__ == "__main__":
    unittest.main()
