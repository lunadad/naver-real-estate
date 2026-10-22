"""리팩토링 안전망 — 리팩토링 전 코드의 관찰 동작을 골든 파일로 고정한다.

골든 파일(tests/golden/refactor_characterization.json)은 리팩토링 *전* 코드로
생성했다. 의도적으로 동작을 바꾸는 경우에만 재생성한다:

    UPDATE_GOLDEN=1 python -m unittest tests.test_refactor_characterization
"""

import json
import os
import sys
import tempfile
import unittest
from datetime import datetime, timedelta
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from database import Database  # noqa: E402

GOLDEN_PATH = Path(__file__).resolve().parent / "golden" / "refactor_characterization.json"
UPDATE_GOLDEN = os.getenv("UPDATE_GOLDEN") == "1"
VOLATILE_KEYS = {"crawled_at", "created_at", "delivered_at", "updated_at", "first_seen_at"}


def _load_golden():
    if GOLDEN_PATH.exists():
        return json.loads(GOLDEN_PATH.read_text(encoding="utf-8"))
    return {}


_GOLDEN = _load_golden()


def _save_golden():
    GOLDEN_PATH.parent.mkdir(parents=True, exist_ok=True)
    GOLDEN_PATH.write_text(
        json.dumps(_GOLDEN, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def _strip_volatile(value):
    if isinstance(value, dict):
        return {k: _strip_volatile(v) for k, v in value.items() if k not in VOLATILE_KEYS}
    if isinstance(value, (list, tuple)):
        return [_strip_volatile(item) for item in value]
    return value


class GoldenMixin:
    def assertGolden(self, key, actual):
        actual = json.loads(json.dumps(_strip_volatile(actual), ensure_ascii=False, default=str))
        if UPDATE_GOLDEN:
            _GOLDEN[key] = actual
            _save_golden()
            return
        self.assertIn(key, _GOLDEN, f"golden key missing: {key} (UPDATE_GOLDEN=1로 생성)")
        self.assertEqual(_GOLDEN[key], actual, key)


def make_listing(article_no, **fields):
    listing = {
        "article_no": article_no,
        "region": "서울특별시",
        "district": "강남구",
        "property_type": "아파트",
        "trade_type": "매매",
        "price": "10억",
        "area": "84㎡",
        "floor": "10/20",
        "building_name": f"단지{article_no}",
        "description": "설명",
        "is_urgent": 1,
        "tags": [],
        "confirmed_date": "20261020",
        "latitude": 37.5,
        "longitude": 127.0,
        "naver_url": f"https://example.com/{article_no}",
    }
    listing.update(fields)
    return listing


LISTINGS = [
    make_listing("L1", building_name="래미안", description="급매 남향", tags=["가격인하"], price="10억 5,000"),
    make_listing("L2", district="서초구", property_type="오피스텔", trade_type="전세",
                 price="3억 5,000", tags=["역세권"], is_urgent=0, latitude=37.48, longitude=127.02),
    make_listing("L3", region="경기도", district="성남시 분당구", property_type="빌라/연립",
                 trade_type="월세", price="1,000/50", tags=[], latitude=37.35, longitude=127.11),
    make_listing("L4", region="경기도", district="수원시", property_type="단독/다가구",
                 price="5억", tags=["가격인하", "신축"], latitude=None, longitude=None),
    make_listing("L5", property_type="상가/업무", trade_type="월세", price="2억/300",
                 tags=["대단지"], latitude=37.6, longitude=127.05, is_urgent=False),
    make_listing("L6", building_name="래미안", tags=["역세권", "신축"], price="9억",
                 description="역세권 대단지", latitude=37.51),
    make_listing("L7", district="서초구", building_name="", tags='["가격인하"]', price=""),
]

LISTING_QUERIES = [
    {},
    {"region": "서울"},
    {"district": "강남"},
    {"property_type": "아파트"},
    {"property_type": "__OTHER__"},
    {"trade_type": "월세"},
    {"urgent_only": True},
    {"price_down_only": True},
    {"tags": ["역세권", "신축"]},
    {"tags": []},
    {"search": "래미안"},
    {"search": "급매"},
    {"sort_by": "urgent"},
    {"sort_by": "recent"},
    {"sort_by": "price-asc"},
    {"sort_by": "price-desc"},
    {"sort_by": "bogus"},
    {"page": 2, "per_page": 2, "sort_by": "price-asc"},
    {"page": 9, "per_page": 3},
    {"region": "서울", "trade_type": "매매", "price_down_only": True, "urgent_only": True},
    {"district": "강남", "tags": ["가격인하"], "search": "래미안", "property_type": "아파트"},
]

MAP_QUERIES = [
    {},
    {"min_lat": 37.4, "max_lat": 37.55},
    {"min_lng": 127.01, "max_lng": 127.1},
    {"min_lat": 37.4, "max_lat": 37.7, "min_lng": 126.9, "max_lng": 127.1, "limit": 2},
    {"region": "경기"},
    {"district": "서초"},
    {"property_type": "__OTHER__"},
    {"property_type": "오피스텔"},
    {"trade_type": "월세"},
    {"price_down_only": True},
    {"tags": ["역세권"]},
    {"search": "래미안"},
    {"region": "서울", "tags": ["신축", "대단지"], "search": "단지", "min_lat": 37.0},
]


def _make_db(tmpdir_name, name="test.db"):
    return Database(db_path=os.path.join(tmpdir_name, name), skip_price_backfill=True)


class ListingQueryCharacterizationTest(GoldenMixin, unittest.TestCase):
    def setUp(self):
        tmpdir = tempfile.TemporaryDirectory()
        self.addCleanup(tmpdir.cleanup)
        self.db = _make_db(tmpdir.name)
        self.addCleanup(self.db.close)
        # 이전 세션 매물은 최신 세션 범위에서 제외되어야 한다.
        self.db.insert_listings([make_listing("OLD1", tags=["역세권"])], "s0")
        self.db.log_crawl("s0", 1, 1, "success", "naver")
        self.db.insert_listings(LISTINGS, "s1")
        self.db.log_crawl("s1", len(LISTINGS), len(LISTINGS), "success", "naver")

    def test_get_listings_matrix(self):
        results = [self.db.get_listings(**query) for query in LISTING_QUERIES]
        self.assertGolden("get_listings_matrix", results)

    def test_get_map_listings_matrix(self):
        results = [self.db.get_map_listings(**query) for query in MAP_QUERIES]
        self.assertGolden("get_map_listings_matrix", results)

    def test_get_listings_without_any_session(self):
        tmpdir = tempfile.TemporaryDirectory()
        self.addCleanup(tmpdir.cleanup)
        empty = _make_db(tmpdir.name, "empty.db")
        self.addCleanup(empty.close)
        self.assertGolden(
            "empty_db_queries",
            {
                "listings": empty.get_listings(),
                "listings_filtered": empty.get_listings(region="서울", tags=["x"]),
                "map": empty.get_map_listings(min_lat=1),
                "region_stats": empty.get_region_stats(),
                "tags": empty.get_tag_counts(),
            },
        )

    def test_region_stats_and_tag_counts(self):
        self.assertGolden(
            "region_stats_and_tags",
            {"region_stats": self.db.get_region_stats(), "tags": self.db.get_tag_counts()},
        )


STATS_EDGE_LISTINGS = [
    make_listing("S1", tags=["가격인하"]),
    make_listing("S2", tags='["가격인하", "역세권"]'),
    make_listing("S3", tags="가격인하"),
    make_listing("S4", tags='"가격인하됨"'),
    make_listing("S5", tags="not json"),
    make_listing("S6", tags=None),
    make_listing("S7", tags="{bad"),
    make_listing("S8", region="", tags=["가격인하"]),
    make_listing("S9", district="  ", tags=["가격인하"]),
    make_listing("S10", region=" 경기도 ", district=" 수원시 ", building_name=" 아파트 "),
    make_listing("S11", region="경기도", district="수원시", building_name="빌라"),
    make_listing("S12", region="경기도", district="수원시", building_name="빌라"),
    make_listing("S13", region=None, district="수원시", building_name="  힐스  ", tags=["가격인하"]),
    make_listing("S14", region="경기도", district="수원시", building_name="힐스"),
    make_listing("S15", building_name=None),
    make_listing("S16", tags=123),
]


class StatsRowsCharacterizationTest(GoldenMixin, unittest.TestCase):
    def setUp(self):
        tmpdir = tempfile.TemporaryDirectory()
        self.addCleanup(tmpdir.cleanup)
        self.tmp = tmpdir.name
        self.db = _make_db(tmpdir.name)
        self.addCleanup(self.db.close)

    def test_region_and_building_stats_rows(self):
        rows = {}
        for label, listings in (
            ("edge", STATS_EDGE_LISTINGS),
            ("edge_without_int_tags", STATS_EDGE_LISTINGS[:-1]),
            ("main", LISTINGS),
            ("empty", []),
        ):
            for builder in ("_build_region_stats_rows", "_build_building_stats_rows"):
                try:
                    result = getattr(self.db, builder)("sess", listings, "T")
                except Exception as exc:  # 예외 종류도 계약이다
                    result = {"raised": type(exc).__name__}
                rows[f"{label}:{builder}"] = result
        self.assertGolden("stats_rows", rows)

    def _dump_stats_tables(self, db=None):
        db = db or self.db
        with db.get_connection() as conn:
            region = conn.execute(
                "SELECT session_id, region, district, total_count, price_down_count, created_at "
                "FROM crawl_region_stats ORDER BY session_id, region, district"
            ).fetchall()
            building = conn.execute(
                "SELECT session_id, region, district, building_name, total_count, price_down_count "
                "FROM crawl_building_stats ORDER BY session_id, district, building_name"
            ).fetchall()
            sessions = conn.execute(
                "SELECT crawl_session, COUNT(*) AS cnt FROM listings "
                "GROUP BY crawl_session ORDER BY crawl_session"
            ).fetchall()
            sort_values = conn.execute(
                "SELECT article_no, price, price_sort_value, rent_sort_value, tags, is_urgent "
                "FROM listings ORDER BY article_no"
            ).fetchall()
        return {
            "region": [dict(r) for r in region],
            "building": [dict(r) for r in building],
            "sessions": [dict(r) for r in sessions],
            "sort_values": [dict(r) for r in sort_values],
        }

    def test_insert_listings_writes_stats_tables(self):
        self.db.insert_listings(LISTINGS, "s1")
        first = self._dump_stats_tables()
        self.db.insert_listings(STATS_EDGE_LISTINGS[:7] + STATS_EDGE_LISTINGS[9:-1], "s2")
        second = self._dump_stats_tables()
        # 세 번째 세션 삽입 시 가장 오래된 세션(s1) 매물이 삭제된다.
        self.db.insert_listings([make_listing("T1")], "s3")
        third = self._dump_stats_tables()
        # 빈 세션 ID도 그대로 기록된다.
        self.db.insert_listings([make_listing("E1")], "")
        empty_session = self._dump_stats_tables()
        self.assertGolden(
            "insert_listings_stats",
            {"first": first, "second": second, "third": third, "empty_session": empty_session},
        )

    def test_replace_and_rebuild_region_stats(self):
        self.db.insert_listings(LISTINGS, "s1")
        self.db.replace_crawl_region_stats("", [{"session_id": "", "region": "r", "district": "d", "total_count": 1}])
        after_empty = self._dump_stats_tables()["region"]
        self.db.replace_crawl_region_stats(
            "s1",
            [
                {"session_id": "s1", "region": "r", "district": "d", "total_count": 3},
                {"session_id": "s1", "region": "r2", "district": "d2", "total_count": 4,
                 "price_down_count": 2, "created_at": "2026-01-01T00:00:00"},
            ],
        )
        after_replace = self._dump_stats_tables()["region"]

        rebuilt_without_history = self.db.rebuild_crawl_region_stats_from_listings("s1")
        self.db.log_crawl("s1", 999, 999, "success", "naver")
        rebuilt_with_mismatch = self.db.rebuild_crawl_region_stats_from_listings("s1")
        self.db.log_crawl("s1", len(LISTINGS), len(LISTINGS), "success", "naver")
        rebuilt_ok = self.db.rebuild_crawl_region_stats_from_listings("s1")
        after_rebuild = self._dump_stats_tables()["region"]
        self.assertGolden(
            "replace_rebuild_region_stats",
            {
                "after_empty": after_empty,
                "after_replace": after_replace,
                "rebuilt_without_history": rebuilt_without_history,
                "rebuilt_with_mismatch": rebuilt_with_mismatch,
                "rebuilt_ok": rebuilt_ok,
                "rebuild_empty_session": self.db.rebuild_crawl_region_stats_from_listings(""),
                "after_rebuild": after_rebuild,
            },
        )

    def test_replace_region_stats_with_external_connection_rolls_into_caller_tx(self):
        self.db.insert_listings(LISTINGS, "s1")
        try:
            with self.db.get_connection() as conn:
                self.db.replace_crawl_region_stats(
                    "s1",
                    [{"session_id": "s1", "region": "x", "district": "y", "total_count": 1}],
                    conn,
                )
                raise RuntimeError("boom")
        except RuntimeError:
            pass
        rolled_back = self._dump_stats_tables()["region"]
        self.assertGolden("replace_region_stats_tx", {"rolled_back": rolled_back})

    def test_replace_region_stats_own_connection_commits_partial_work_on_error(self):
        # 자체 커넥션을 쓰는 경우 finally에서 __exit__(None, None, None)을 호출하므로
        # 내부 예외가 나도 DELETE는 커밋된다.
        self.db.insert_listings(LISTINGS, "s1")
        with self.assertRaises(KeyError):
            self.db.replace_crawl_region_stats("s1", [{"session_id": "s1"}])
        self.assertGolden(
            "replace_region_stats_partial_commit", self._dump_stats_tables()["region"]
        )


class AlertAndPushCharacterizationTest(GoldenMixin, unittest.TestCase):
    def setUp(self):
        tmpdir = tempfile.TemporaryDirectory()
        self.addCleanup(tmpdir.cleanup)
        self.db = _make_db(tmpdir.name)
        self.addCleanup(self.db.close)

    def test_create_alert_rule_variants(self):
        created = [
            self.db.create_alert_rule("c1"),
            self.db.create_alert_rule(" c1 ", keyword=" 래미안 ", district="강남구"),
            self.db.create_alert_rule("c1", property_type="아파트", trade_type="매매", name=" 내 알림 "),
            self.db.create_alert_rule("c1", district="강남구", building_name="래미안", min_daily_change="3"),
            self.db.create_alert_rule("c2", building_name="래미안", min_daily_change=-5),
            self.db.create_alert_rule("c2", keyword=None, min_daily_change=None),
        ]
        self.assertGolden(
            "create_alert_rule_variants",
            {
                "created": created,
                "rules_c1": self.db.get_alert_rules("c1"),
                "rule_missing": self.db.get_alert_rule("c1", 9999),
                "delete_other_client": self.db.delete_alert_rule("c2", created[0]["id"]),
                "delete_ok": self.db.delete_alert_rule("c1", created[0]["id"]),
                "delete_again": self.db.delete_alert_rule("c1", created[0]["id"]),
            },
        )

    def test_push_subscription_queries(self):
        sub = {"endpoint": "https://push/1", "keys": {"auth": "a", "p256dh": "p"}}
        self.db.save_push_subscription("c1", sub)
        self.db.save_push_subscription("c2", {**sub, "endpoint": "https://push/2"})
        with self.db.get_connection() as conn:
            conn.execute(
                "INSERT INTO push_subscriptions (client_id, endpoint, subscription_json, created_at, updated_at) "
                "VALUES ('c3', 'https://push/3', '{bad', '2026-01-01', '2026-01-01')"
            )
        self.assertGolden(
            "push_subscriptions",
            {
                "all": self.db.get_push_subscriptions(),
                "c1": self.db.get_push_subscriptions("c1"),
                "empty_client_arg": self.db.get_push_subscriptions(""),
            },
        )


class LastCrawlCharacterizationTest(GoldenMixin, unittest.TestCase):
    def setUp(self):
        tmpdir = tempfile.TemporaryDirectory()
        self.addCleanup(tmpdir.cleanup)
        self.db = _make_db(tmpdir.name)
        self.addCleanup(self.db.close)

    def _log(self, session_id, status, source, days_ago):
        with self.db.get_connection() as conn:
            conn.execute(
                "INSERT INTO crawl_history (session_id, crawled_at, total_count, urgent_count, status, source) "
                "VALUES (?, ?, 1, 1, ?, ?)",
                (session_id, (datetime(2026, 10, 20, 9) - timedelta(days=days_ago)).isoformat(), status, source),
            )

    def test_last_crawl_variants(self):
        empty = {
            "last": self.db.get_last_crawl(),
            "visible": self.db.get_last_crawl(prefer_visible=True),
            "live": self.db.get_last_successful_live_crawl(),
            "recent": self.db.get_recent_successful_crawls(),
        }
        self._log("ok-old", "success", "naver", 3)
        self._log("demo", "success", "demo", 2)
        self._log("nullsrc", "success", None, 1)
        self._log("fail", "failed", "naver", 0)
        filled = {
            "last": self.db.get_last_crawl(),
            "visible": self.db.get_last_crawl(prefer_visible=True),
            "live": self.db.get_last_successful_live_crawl(),
            "recent": self.db.get_recent_successful_crawls(),
            "recent_limit_1": self.db.get_recent_successful_crawls(limit=1),
            "recent_limit_0": self.db.get_recent_successful_crawls(limit=0),
        }
        with self.db.get_connection() as conn:
            conn.execute("DELETE FROM crawl_history WHERE status = 'success'")
        only_failed = {
            "last": self.db.get_last_crawl(),
            "visible": self.db.get_last_crawl(prefer_visible=True),
            "live": self.db.get_last_successful_live_crawl(),
        }
        self.assertGolden(
            "last_crawl_variants",
            {"empty": empty, "filled": filled, "only_failed": only_failed},
        )


class AppCharacterizationTest(GoldenMixin, unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls._tmpdir = tempfile.TemporaryDirectory()
        db_path = os.path.join(cls._tmpdir.name, "app-char.db")
        os.environ["FORCE_LOCAL_SQLITE"] = "1"
        os.environ["DATABASE_URL"] = ""
        os.environ["DB_PATH"] = db_path
        os.environ["ENABLE_SCHEDULER"] = "false"
        os.environ["SEED_DEMO_DATA"] = "false"

        import app as app_module  # noqa: E402

        cls.app_module = app_module
        cls._original_db = app_module.db
        app_module.db = Database(db_path=db_path, skip_price_backfill=True)
        cls.client = app_module.app.test_client()
        app_module.db.insert_listings(LISTINGS, "app-s1")
        app_module.db.log_crawl("app-s1", len(LISTINGS), len(LISTINGS), "success", "naver")

    @classmethod
    def tearDownClass(cls):
        # 다른 테스트 파일이 같은 app 모듈을 재사용하므로 원래 db로 되돌린다.
        cls.app_module.db = cls._original_db

    def _get(self, url):
        response = self.client.get(url)
        return {"status": response.status_code, "json": response.get_json()}

    def test_listing_routes(self):
        urls = [
            "/api/listings",
            "/api/listings?urgent_only=TRUE",
            "/api/listings?urgent_only=1",
            "/api/listings?price_down_only=True&sort_by=price-asc",
            "/api/listings?page=abc&per_page=5",
            "/api/listings?page=0&per_page=500",
            "/api/listings?per_page=-1",
            "/api/listings?tags=%EC%97%AD%EC%84%B8%EA%B6%8C,,%20&region=%EC%84%9C%EC%9A%B8",
            "/api/listings?property_type=__OTHER__&trade_type=%EC%9B%94%EC%84%B8",
            "/api/map-listings",
            "/api/map-listings?min_lat=abc&max_lat=&min_lng=127.01&limit=2",
            "/api/map-listings?limit=abc&price_down_only=TRUE",
            "/api/map-listings?limit=0&search=%EB%9E%98%EB%AF%B8%EC%95%88",
            "/api/map-listings?limit=9999&tags=%EC%8B%A0%EC%B6%95",
        ]
        self.assertGolden("listing_routes", {url: self._get(url) for url in urls})

    def test_client_id_required_routes(self):
        results = {
            "get_rules": self._get("/api/alert-rules"),
            "get_rules_blank": self._get("/api/alert-rules?client_id=%20"),
            "check": self._get("/api/alerts/check"),
        }
        for label, method, kwargs in (
            ("post_rule", "post", {"json": {}}),
            ("post_rule_blank", "post", {"json": {"client_id": "  "}}),
            ("delete_rule", "delete", {}),
            ("subscribe", "post", {"json": {}}),
            ("unsubscribe", "delete", {"json": {}}),
        ):
            url = "/api/alert-rules/1" if label == "delete_rule" else (
                "/api/push/subscribe" if "subscribe" in label else "/api/alert-rules"
            )
            response = getattr(self.client, method)(url, **kwargs)
            results[label] = {"status": response.status_code, "json": response.get_json()}

        response = self.client.post(
            "/api/alert-rules", json={"client_id": "c9", "min_daily_change": "x"}
        )
        results["bad_min_daily_change"] = {"status": response.status_code, "json": response.get_json()}
        response = self.client.post("/api/alert-rules", json={"client_id": "c9"})
        results["no_filter"] = {"status": response.status_code, "json": response.get_json()}
        response = self.client.post(
            "/api/alert-rules", json={"client_id": "c9", "building_name": "래미안"}
        )
        results["incomplete_building"] = {"status": response.status_code, "json": response.get_json()}
        self.assertGolden("client_id_routes", results)

    def test_crawl_disabled_responses(self):
        with mock.patch.object(self.app_module, "ENABLE_CRAWL_ENDPOINT", False):
            response = self.client.post("/api/crawl")
        self.assertEqual(response.status_code, 403)
        self.assertEqual(
            response.get_json(), {"status": "error", "message": "crawl endpoint disabled"}
        )

    def test_trigger_crawl_status_variants(self):
        results = {}
        for status in ("success", "degraded", "failed", None):
            fake = mock.Mock()
            result = {"total": 3, "urgent": 2, "source": "naver"}
            if status:
                result["status"] = status
            fake.crawl_all.return_value = result
            with mock.patch.object(self.app_module, "ENABLE_CRAWL_ENDPOINT", True), \
                    mock.patch.object(self.app_module, "get_crawler", return_value=fake), \
                    mock.patch.object(self.app_module, "dispatch_push_alerts",
                                      return_value={"clients": 1, "sent": 1, "matches": 1}):
                response = self.client.post("/api/crawl")
            body = response.get_json()
            body.pop("crawled_at", None)
            results[str(status)] = {"status": response.status_code, "json": body}
        self.assertGolden("trigger_crawl_variants", results)

    def test_dispatch_push_without_config(self):
        with mock.patch.object(self.app_module, "webpush", None):
            self.assertEqual(
                self.app_module.dispatch_push_alerts(),
                {"clients": 0, "sent": 0, "matches": 0},
            )

    def test_build_daily_crawl_series(self):
        app_module = self.app_module
        today = datetime.now(app_module.KST).date()
        db = app_module.db
        with db.get_connection() as conn:
            conn.execute("DELETE FROM crawl_history")

        def log(session_id, days_ago, hour, total, status="success", source="naver"):
            crawled = datetime.combine(today - timedelta(days=days_ago), datetime.min.time()).replace(hour=hour)
            with db.get_connection() as conn:
                conn.execute(
                    "INSERT INTO crawl_history (session_id, crawled_at, total_count, urgent_count, status, source) "
                    "VALUES (?, ?, ?, ?, ?, ?)",
                    (session_id, crawled.isoformat(), total, total - 1, status, source),
                )

        log("d0-early", 0, 1, 10)
        log("d0-late", 0, 5, 11)
        log("d2", 2, 3, 20)
        log("d3-demo", 3, 3, 30, source="demo")
        log("d4-fail", 4, 3, 40, status="failed")
        log("d9", 9, 3, 90)

        def describe(series):
            out = []
            for item in series:
                entry = dict(item)
                entry["date"] = (date_offset(entry["date"]))
                entry.pop("label")
                crawled_at = entry.pop("crawled_at")
                entry["crawled_at_kst_hour"] = crawled_at.hour if crawled_at else None
                out.append(entry)
            return out

        def date_offset(iso):
            return (today - datetime.fromisoformat(iso).date()).days

        series = {
            str(days): describe(app_module.build_daily_crawl_series(days))
            for days in (None, 0, 1, 3, 7, 14, 30)
        }
        labels_ok = all(
            item["label"] == f"{datetime.fromisoformat(item['date']).month}.{datetime.fromisoformat(item['date']).day}"
            for item in app_module.build_daily_crawl_series(14)
        )
        self.assertTrue(labels_ok)
        self.assertGolden("daily_crawl_series", series)


class _FakeRequest:
    def __init__(self, url, headers):
        self.url = url
        self.headers = headers


class _FakePage:
    def __init__(self, responses):
        self._responses = responses
        self._handlers = []
        self.evaluate_calls = []

    def set_default_timeout(self, _):
        pass

    def add_init_script(self, _):
        pass

    def on(self, event, handler):
        self._handlers.append(handler)

    def goto(self, url, **_):
        for handler in self._handlers:
            handler(_FakeRequest("https://new.land.naver.com/api/complexes", {"authorization": "Bearer TEST"}))

    def wait_for_timeout(self, _):
        pass

    def evaluate(self, _script, args):
        self.evaluate_calls.append(dict(args))
        key = (args["cortarNo"], args["ptype"], args["ttype"], args["page"])
        value = self._responses.get(key)
        if isinstance(value, list):  # 호출 순서대로 소비, 마지막 값은 계속 반복
            value = value.pop(0) if len(value) > 1 else value[0]
        if isinstance(value, Exception):
            raise value
        if isinstance(value, dict) and "ok" in value:
            return value
        return {"ok": True, "data": value}


class _FakePlaywright:
    def __init__(self, page):
        self.page = page
        self.chromium = self

    def launch(self, **_):
        return self

    def new_context(self, **_):
        return self

    def new_page(self):
        return self.page

    def close(self):
        pass

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


def _article(no, **fields):
    article = {
        "articleNo": no,
        "articleName": f"건물{no}",
        "tagList": ["급매"],
        "articleFeatureDesc": f"설명{no}",
        "dealOrWarrantPrc": "5억",
        "areaName": "84",
        "floorInfo": "3/10",
        "articleConfirmYmd": "20261020",
        "latitude": 37.1,
        "longitude": 127.1,
        "tradeTypeCode": "A1",
    }
    article.update(fields)
    return article


class CrawlerCharacterizationTest(GoldenMixin, unittest.TestCase):
    def test_crawl_with_fake_playwright(self):
        import crawler as crawler_module

        regions = {
            "서울특별시": {
                "districts": {
                    "강남구": {"code": "111", "lat": 37.5, "lng": 127.0},
                    "서초구": {"code": "222", "lat": 37.4, "lng": 127.2},
                }
            },
            "경기도": {"districts": {"수원시": {"code": "333", "lat": 37.2, "lng": 127.3}}},
        }
        responses = {
            ("111", "APT", "A1", 1): {
                "articleList": [
                    _article(1),
                    _article(2, priceChangeState="down", tagList=None, articleName=None,
                             latitude=None, longitude=None),
                ],
                "isMoreData": True,
            },
            ("111", "APT", "A1", 2): {
                "articleList": [_article(3, isPriceModification=True, tagList=["가격인하"])],
                "isMoreData": False,
            },
            ("111", "APT", "B2", 1): {
                "articleList": [
                    _article(4, tradeTypeCode="B2", rentPrc=50, dealOrWarrantPrc="1,000"),
                    _article(1),  # 다른 조합에서 중복 → 건너뜀
                    _article(5, tradeTypeCode="B2", rentPrc="", priceChangeState="DECREASE",
                             tagList=["가격인하", "역세권"]),
                ],
                "isMoreData": False,
            },
            ("111", "OPST", "A1", 1): RuntimeError("evaluate failed"),
            ("222", "VL", "B1", 1): {"articleList": [], "isMoreData": True},
            ("333", "OPST", "B1", 1): {
                "articleList": [_article(6, articleFeatureDesc=None, areaName=None, floorInfo=None,
                                         articleConfirmYmd=None, dealOrWarrantPrc=None)],
                "isMoreData": True,
            },
            ("333", "OPST", "B1", 2): None,
        }
        page = _FakePage(responses)
        crawler = crawler_module.NaverRealEstateCrawler(db=mock.Mock())
        with mock.patch.object(crawler_module, "PLAYWRIGHT_AVAILABLE", True), \
                mock.patch.object(crawler_module, "sync_playwright",
                                  create=True, return_value=_FakePlaywright(page)), \
                mock.patch.object(crawler_module.time, "sleep"), \
                mock.patch.object(type(crawler), "REGIONS", regions):
            listings = crawler._crawl_with_playwright()

        self.assertGolden(
            "crawler_fake_playwright",
            {
                "listings": listings,
                "evaluate_count": len(page.evaluate_calls),
                "first_call": page.evaluate_calls[0],
            },
        )


if __name__ == "__main__":
    unittest.main()
