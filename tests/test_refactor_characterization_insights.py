"""리팩토링 안전망 2 — insights/·views/·세션 조회의 관찰 동작을 골든 파일로 고정한다.

골든 파일(tests/golden/refactor_characterization_insights.json)은 리팩토링 *전* 코드로
생성했다. 의도적으로 동작을 바꾸는 경우에만 재생성한다:

    UPDATE_GOLDEN=1 python -m unittest tests.test_refactor_characterization_insights
"""

import json
import os
import sys
import tempfile
import unittest
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from database import Database  # noqa: E402
from insights import complex as complex_insight  # noqa: E402
from insights import heatmap as heatmap_insight  # noqa: E402
from insights import price_history, tracking, watchlist  # noqa: E402
from insights import sessions as session_helpers  # noqa: E402

GOLDEN_PATH = Path(__file__).resolve().parent / "golden" / "refactor_characterization_insights.json"
UPDATE_GOLDEN = os.getenv("UPDATE_GOLDEN") == "1"
KST = timezone(timedelta(hours=9))
NOW = datetime(2026, 10, 20, 10, 0, 0)


def _load_golden():
    if GOLDEN_PATH.exists():
        return json.loads(GOLDEN_PATH.read_text(encoding="utf-8"))
    return {}


_GOLDEN = _load_golden()


class GoldenMixin:
    def assertGolden(self, key, actual, strip=()):
        actual = json.loads(json.dumps(_strip(actual, set(strip)), ensure_ascii=False, default=str))
        if UPDATE_GOLDEN:
            _GOLDEN[key] = actual
            GOLDEN_PATH.parent.mkdir(parents=True, exist_ok=True)
            GOLDEN_PATH.write_text(
                json.dumps(_GOLDEN, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8"
            )
            return
        self.assertIn(key, _GOLDEN, f"golden key missing: {key} (UPDATE_GOLDEN=1로 생성)")
        self.assertEqual(_GOLDEN[key], actual, key)


def _strip(value, keys):
    if isinstance(value, dict):
        return {str(k): _strip(v, keys) for k, v in value.items() if k not in keys}
    if isinstance(value, (list, tuple)):
        return [_strip(item, keys) for item in value]
    return value


# 시각 값의 모든 입력 형태 — 문자열/aware/naive/날짜만/쓰레기/None.
TIME_INPUTS = {
    "naive_iso": "2026-10-10T09:00:00",
    "naive_iso_space": "2026-10-10 09:00:00.123456",
    "aware_kst_iso": "2026-10-10T09:00:00+09:00",
    "aware_z_iso": "2026-10-10T09:00:00Z",
    "date_only": "2026-10-10",
    "naive_datetime": datetime(2026, 10, 10, 9, 0, 0),
    "aware_datetime": datetime(2026, 10, 10, 9, 0, 0, tzinfo=KST),
    "date_object": date(2026, 10, 10),
    "future": "2026-12-25T00:00:00",
    "almost_one_day": "2026-10-19T10:00:01",
    "padded": " 2026-10-10T09:00:00 ",
    "garbage": "not-a-date",
    "empty": "",
    "none": None,
    "zero": 0,
}

# (session_id, crawled_at, total_count, status, source)
SESSIONS = {
    "s_old": ("2026-09-01T09:00:00", 40, "success", "naver"),
    "s_week": ("2026-10-13T09:10:00", 30, "success", None),  # source NULL도 라이브로 본다
    "s_week_late": ("2026-10-13T22:00:00", 31, "success", "naver"),  # 7일-12h 컷오프 뒤
    "s_demo": ("2026-10-19T09:00:00", 99, "success", "demo"),
    "s_prev": ("2026-10-19T09:30:00", 20, "success", "naver"),
    "s_fail": ("2026-10-19T12:00:00", 0, "error", "naver"),
    "s_new": ("2026-10-20T09:00:00", 25, "success", "naver"),
}

# session → [(district, building, total, price_down)]
BUILDING_STATS = {
    "s_old": [("강남구", "래미안", 9, 1), ("서초구", "자이", 8, 0), ("송파구", "헬리오", 2, 0)],
    "s_week": [("강남구", "래미안", 8, 2), ("서초구", "자이", 6, 1), ("마포구", "푸르지오", 4, 0)],
    "s_week_late": [("강남구", "래미안", 7, 2)],
    "s_demo": [("강남구", "래미안", 50, 50)],
    "s_prev": [("강남구", "래미안", 6, 3), ("서초구", "자이", 3, 1), ("마포구", "푸르지오", 5, 2)],
    "s_fail": [("강남구", "래미안", 1, 0)],
    "s_new": [("강남구", "래미안", 4, 3), ("서초구", "자이", 3, 0), ("송파구", "헬리오", 5, 1)],
}

# session → [(region, district, total, price_down)]
REGION_STATS = {
    "s_week": [("서울특별시", "강남구", 10, 2), ("서울특별시", "서초구", 20, 5)],
    "s_prev": [("서울특별시", "강남구", 12, 3), ("서울특별시", "서초구", 8, 2), ("세종특별자치시", "세종특별자치시", 2, 0)],
    "s_new": [("서울특별시", "강남구", 15, 6), ("서울특별시", "마포구", 10, 1)],
    "s_demo": [("서울특별시", "강남구", 99, 99)],
}

SCENARIOS = {
    "all": list(SESSIONS),
    "none": [],
    "only_new": ["s_new"],
    "new_and_prev": ["s_prev", "s_new"],
    "new_and_week_late": ["s_week_late", "s_new"],
    "only_demo_and_fail": ["s_demo", "s_fail"],
    "new_and_demo": ["s_demo", "s_new"],
}


def listing_row(article_no, session, **fields):
    row = {
        "article_no": article_no,
        "region": "서울특별시",
        "district": "강남구",
        "property_type": "아파트",
        "trade_type": "매매",
        "price": "10억",
        "area": "84A",
        "floor": "10/20",
        "building_name": "래미안",
        "tags": "[]",
        "crawled_at": SESSIONS.get(session, ("2026-10-20T09:00:00",))[0],
        "crawl_session": session,
        "naver_url": f"https://example.com/{article_no}",
        "price_sort_value": 100000,
        "bargain_discount": None,
        "bargain_basis": None,
        "first_seen_at": "2026-10-10T09:00:00",
        "price_cut_count": 0,
    }
    row.update(fields)
    return row


LISTING_ROWS = [
    listing_row("A1", "s_new", price_sort_value=80000, price="8억", bargain_discount=0.2, bargain_basis="same_area",
                tags='["가격인하"]', first_seen_at="2026-10-18T09:00:00", price_cut_count=2),
    listing_row("A2", "s_new", bargain_discount=0.0, first_seen_at="2026-09-20T09:00:00"),
    listing_row("A3", "s_new", price_sort_value=105000, bargain_discount=-0.05, first_seen_at="2026-10-01T09:00:00+09:00"),
    listing_row("A4", "s_new", price_sort_value=55000, bargain_discount=0.45, first_seen_at=None),
    listing_row("A5", "s_new", area="59B", price_sort_value=70000, bargain_discount=0.4,
                tags='["가격인하예정", "급매"]', first_seen_at="garbage"),
    listing_row("A6", "s_new", trade_type="전세", price_sort_value=50000, tags=None),
    listing_row("B1", "s_new", district="서초구", building_name="자이", bargain_discount=0.1, bargain_basis="complex_m2"),
    listing_row("B2", "s_new", district="서초구", building_name="", area="", price_sort_value=0),
    listing_row("OLD1", "s_prev", bargain_discount=0.3),
]

TRACKING_ROWS = [
    # (article_no, first_seen_at, last_seen_at, last_price, last_value, changes, cuts)
    ("A1", "2026-10-18T09:00:00", "2026-10-20T09:00:00", "8억", 80000, 2, 2),
    ("A2", "2026-09-20T09:00:00", "2026-10-20T09:00:00", "10억", 100000, 0, 0),
    ("G1", "2026-10-01T09:00:00", "2026-10-15T09:00:00", "9억", 90000, 1, 1),
]

EVENT_ROWS = [
    # (article_no, session_id, observed_at, price, value, prev_price, prev_value, change_pct)
    ("A1", "s_week", "2026-10-18T09:00:00", "10억", 100000, None, None, None),
    ("A1", "s_prev", "2026-10-19T09:30:00", "9억", 90000, "10억", 100000, -10.0),
    ("A1", "s_new", "2026-10-20T09:00:00", "8억", 80000, "9억", 90000, -11.11),
    ("A2", "s_week", "2026-09-20T09:00:00", "10억", 100000, None, None, None),
]

DISAPPEARANCE_ROWS = [
    # (article_no, session_id, disappeared_at, district, building, first_seen_at, was_bargain, had_down, reappeared_at)
    ("G1", "s_prev", "2026-10-19T09:30:00", "강남구", "래미안", "2026-10-01T09:00:00", 1, 1, None),
    ("G1", "s_week", "2026-10-13T09:10:00", "강남구", "래미안", "2026-10-01T09:00:00", 0, 0, None),
    ("G2", "s_prev", "2026-10-19T09:30:00", "강남구", "래미안", "2026-10-15T23:00:00", 1, 0, None),
    ("G3", "s_new", "2026-10-20T09:00:00", "강남구", "다른단지", "2026-10-10T09:00:00+09:00", 1, 0, None),
    ("G4", "s_new", "2026-10-20T09:00:00", "강남구", "다른단지", None, 1, 0, None),
    ("G5", "s_week", "2026-10-13T09:10:00", "강남구", "다른단지", "2026-10-14T09:00:00", 1, 0, None),
    ("G6", "s_week", "2026-10-13T09:10:00", "서초구", "자이", "2026-10-03T09:00:00", 0, 1, None),
    ("G7", "s_week", "2026-10-13T09:10:00", "서초구", "자이", "2026-10-03T09:00:00", 1, 0, "2026-10-19T09:30:00"),
    ("G8", "s_old", "2026-06-01T09:00:00", "강남구", "래미안", "2026-05-01T09:00:00", 1, 0, None),
]


def build_db(tmpdir, name, session_ids, with_rows=True):
    db = Database(db_path=os.path.join(tmpdir, f"{name}.db"), skip_price_backfill=True)
    with db.get_connection() as conn:
        for session_id in session_ids:
            crawled_at, total, status, source = SESSIONS[session_id]
            conn.execute(
                "INSERT INTO crawl_history (session_id, crawled_at, total_count, urgent_count, status, source) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                (session_id, crawled_at, total, total, status, source),
            )
            for district, building, total_count, down in BUILDING_STATS.get(session_id, []):
                conn.execute(
                    "INSERT INTO crawl_building_stats (session_id, region, district, building_name, total_count, "
                    "price_down_count, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
                    (session_id, "서울특별시", district, building, total_count, down, crawled_at),
                )
            for region, district, total_count, down in REGION_STATS.get(session_id, []):
                conn.execute(
                    "INSERT INTO crawl_region_stats (session_id, region, district, total_count, price_down_count, "
                    "created_at) VALUES (?, ?, ?, ?, ?, ?)",
                    (session_id, region, district, total_count, down, crawled_at),
                )
        if not with_rows:
            return db
        for row in LISTING_ROWS:
            if row["crawl_session"] not in session_ids:
                continue
            columns = list(row)
            conn.execute(
                f"INSERT INTO listings ({', '.join(columns)}) VALUES ({', '.join('?' * len(columns))})",
                [row[column] for column in columns],
            )
        for row in TRACKING_ROWS:
            conn.execute(
                "INSERT INTO listing_tracking (article_no, first_seen_at, last_seen_at, last_price, "
                "last_price_sort_value, price_change_count, price_cut_count) VALUES (?, ?, ?, ?, ?, ?, ?)",
                row,
            )
        for row in EVENT_ROWS:
            conn.execute(
                "INSERT INTO listing_price_events (article_no, session_id, observed_at, price, price_sort_value, "
                "prev_price, prev_price_sort_value, change_pct) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                row,
            )
        for no, session_id, gone_at, district, building, first_seen, bargain, down, reappeared in DISAPPEARANCE_ROWS:
            conn.execute(
                "INSERT INTO listing_disappearances (article_no, session_id, disappeared_at, region, district, "
                "building_name, property_type, trade_type, area, floor, price, naver_url, first_seen_at, "
                "was_bargain, had_price_down, reappeared_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (no, session_id, gone_at, "서울특별시", district, building, "아파트", "매매", "84A", "3/20", "9억",
                 f"https://example.com/{no}", first_seen, bargain, down, reappeared),
            )
    return db


class DateHandlingCharacterizationTest(GoldenMixin, unittest.TestCase):
    """시각 파싱·경과일 계산 — 입력 형태별 결과를 공개 함수로 고정한다."""

    def test_build_insight_time_inputs(self):
        result = {}
        for name, value in TIME_INPUTS.items():
            events = [
                {"price_sort_value": 100000, "observed_at": "2026-10-01T09:00:00"},
                {"price_sort_value": 90000, "observed_at": value},
            ]
            result[name] = price_history.build_insight(
                {"first_seen_at": value, "price_cut_count": 1}, events, 0.2, now=NOW
            )
        self.assertGolden("build_insight_time_inputs", result)

    def test_build_insight_event_shapes(self):
        cases = {
            "no_events": ({}, [], None),
            "single_event": ({"first_seen_at": "2026-10-10T09:00:00"}, [{"price_sort_value": 100000}], 0.05),
            "zero_first_value": ({"price_cut_count": "3"}, [{"price_sort_value": 0}, {"price_sort_value": 5}], 0.41),
            "missing_last_value": ({"price_cut_count": None}, [{"price_sort_value": 10}, {}], 0.1),
            "price_up": (
                {"first_seen_at": "2026-10-19T10:00:00"},
                [{"price_sort_value": 3, "observed_at": "2026-10-01T00:00:00"},
                 {"price_sort_value": 4, "observed_at": "2026-10-19T23:59:59"}],
                0.4,
            ),
        }
        result = {
            name: price_history.build_insight(tracking_row, events, discount, now=NOW)
            for name, (tracking_row, events, discount) in cases.items()
        }
        self.assertGolden("build_insight_event_shapes", result)

    def test_parse_dt_and_week_ago_pick(self):
        parsed = {name: session_helpers.parse_dt(value) for name, value in TIME_INPUTS.items()}
        sessions = [(sid, row[0]) for sid, row in SESSIONS.items()]
        picks = {
            name: session_helpers.pick_week_ago_session(sessions, "s_new", value)
            for name, value in {
                "latest": "2026-10-20T09:00:00",
                "latest_aware": "2026-10-20T18:00:00+09:00",
                "edge_inside": "2026-10-20T21:10:00",
                "edge_outside": "2026-10-21T10:00:01",
                "none": None,
                "garbage": "x",
            }.items()
        }
        self.assertGolden("parse_dt_and_week_ago_pick", {"parsed": parsed, "picks": picks})

    def test_watchlist_listing_payload_time_inputs(self):
        result = {}
        for name, value in TIME_INPUTS.items():
            payload = watchlist._listing_payload(
                {"article_no": 7, "first_seen_at": value, "bargain_discount": 0.123, "price_sort_value": 84000,
                 "area": "84A", "tags": '["가격인하", "급매"]'},
                None,
                NOW,
            )
            result[name] = {"days_listed": payload["days_listed"], "first_seen_at": payload["first_seen_at"]}
        self.assertGolden("watchlist_listing_payload_time_inputs", result)

    def test_watchlist_payload_tags_and_discount(self):
        tag_inputs = {
            "list": ["가격인하", "급매"], "tuple": ("급매",), "json": '["가격인하"]', "json_dict": '{"a": 1}',
            "json_scalar": "3", "broken": "[oops", "none": None, "empty": "", "blank_items": '[" ", "", "x"]',
            "number": 5,
        }
        result = {}
        for name, raw in tag_inputs.items():
            payload = watchlist._listing_payload(
                {"article_no": "T", "tags": raw, "bargain_discount": {"list": None, "tuple": 0.1, "json": 0.4}.get(name, 0.41)},
                "2" if name == "json" else None,
                NOW,
            )
            result[name] = payload
        self.assertGolden("watchlist_payload_tags_and_discount", result)


class PriceDownCharacterizationTest(GoldenMixin, unittest.TestCase):
    """가격인하 판별 — 단지/히트맵은 tags 원문 부분 문자열로 센다."""

    TAGS = {
        "list": ["가격인하"], "json": '["가격인하", "급매"]', "substring": '["가격인하예정"]', "plain": "가격인하",
        "other": '["급매"]', "none": None, "empty_list": [], "number": 0,
    }

    def test_complex_stats(self):
        listings = [
            {"tags": tags, "bargain_discount": discount, "price_sort_value": 100000 + index, "area": "84A",
             "trade_type": "매매", "article_no": f"P{index}"}
            for index, (tags, discount) in enumerate(
                zip(self.TAGS.values(), [0.1, 0.4, 0.41, None, 0.09, -0.2, 0.25, 0.0])
            )
        ]
        self.assertGolden(
            "complex_stats",
            {
                "has_price_down": {name: complex_insight.has_price_down({"tags": tags}) for name, tags in self.TAGS.items()},
                "missing_key": complex_insight.has_price_down({}),
                "stats": complex_insight.build_stats(listings, 3),
                "stats_empty": complex_insight.build_stats([], None),
                "areas": complex_insight.build_areas(listings),
                "ordered": complex_insight.order_listings(listings),
            },
        )

    def test_heatmap_districts(self):
        rows = [
            {"district": name_district, "tags": tags, "bargain_discount": discount}
            for (tags, discount), name_district in zip(
                zip(self.TAGS.values(), [0.1, 0.4, 0.41, None, 0.09, -0.2, 0.25, 0.0]),
                ["강남구", "강남구", "강남구", "서초구", "서초구", "마포구", "마포구", "기타구"],
            )
        ]
        stats_now = {"서초구": {"total_count": 10, "price_down_count": 4}, "송파구": {"total_count": None, "price_down_count": None}}
        stats_prev = {"강남구": {"total_count": 2}, "서초구": {"total_count": 0}, "마포구": {"total_count": None}}
        names = ["강남구", "서초구", "마포구", "송파구", "없는구"]
        self.assertGolden(
            "heatmap_districts",
            {
                "with_prev": heatmap_insight.build_districts(names, rows, stats_now, stats_prev),
                "without_prev": heatmap_insight.build_districts(names, rows, {}, None),
            },
        )


class SessionSelectionCharacterizationTest(GoldenMixin, unittest.TestCase):
    """라이브 세션 조건과 비교 세션 고르기 — 세션 구성별 결과."""

    @classmethod
    def setUpClass(cls):
        cls._tmpdir = tempfile.TemporaryDirectory()
        cls.dbs = {
            name: build_db(cls._tmpdir.name, name, session_ids) for name, session_ids in SCENARIOS.items()
        }

    @classmethod
    def tearDownClass(cls):
        for db in cls.dbs.values():
            db.close()
        cls._tmpdir.cleanup()

    def test_fast_complexes(self):
        result = {}
        for name, db in self.dbs.items():
            with db.get_connection() as conn:
                result[name] = {
                    str(days): tracking.fast_complexes(conn, days) for days in (1, 7, 30)
                }
                result[name]["limit_1"] = tracking.fast_complexes(conn, 7, limit=1)
        self.assertGolden("fast_complexes", result)

    def test_weekly_report_data(self):
        moments = {
            "after_latest": NOW,
            "before_latest": datetime(2026, 10, 19, 10, 0, 0),
            "exactly_latest": datetime(2026, 10, 20, 9, 0, 0),
            "before_everything": datetime(2026, 8, 1),
        }
        result = {
            name: {label: db.get_weekly_report_data(moment) for label, moment in moments.items()}
            for name, db in self.dbs.items()
        }
        self.assertGolden("weekly_report_data", result)

    def test_live_session_queries(self):
        result = {}
        for name, db in self.dbs.items():
            with db.get_connection() as conn:
                latest = session_helpers.get_latest_session(db, conn)
                week_ago = session_helpers.get_week_ago_session(conn, "s_new", "2026-10-20T09:00:00")
            result[name] = {
                "trends": db.get_trends(),
                "price_down_ratio_trend": db.get_price_down_ratio_trend(),
                "top_building_movers": db.get_top_building_movers(),
                "latest_building_changes": db.get_latest_building_changes(
                    [("강남구", "래미안"), (" 서초구 ", "자이"), ("", "x"), ("없는구", "없음")]
                ),
                "building_history": db.get_building_stats_history("강남구", "래미안", 3),
                "last_crawl": db.get_last_crawl(),
                "last_crawl_visible": db.get_last_crawl(prefer_visible=True),
                "last_live": db.get_last_successful_live_crawl(),
                "recent_live": db.get_recent_successful_crawls(3),
                "live_on": {
                    day: db.has_successful_live_crawl_on(date.fromisoformat(day))
                    for day in ("2026-10-19", "2026-10-20", "2026-10-21", "2026-10-13", "2026-10-12")
                },
                "latest_session": latest,
                "week_ago_session": week_ago,
            }
        self.assertGolden("live_session_queries", result, strip={"id"})

    def test_watchlist_queries(self):
        db = self.dbs["all"]
        nos = ["A1", "A2", "A3", "A4", "A5", "G1", "G3", "G4", "G5", "G7", "OLD1", "NOPE", "", " ", "A1", 123]
        watched = [
            {"article_no": "A2", "saved_at": "2026-10-15T00:00:00", "district": "강남구", "building_name": "래미안"},
            {"article_no": "X", "saved_at": "2026-10-19T00:00:00", "district": "강남구", "building_name": "래미안"},
            {"article_no": "Y", "saved_at": "garbage", "district": "서초구", "building_name": "자이"},
            {"article_no": "Z", "saved_at": None, "district": "", "building_name": "자이"},
        ]
        with db.get_connection() as conn:
            result = {
                "lookup": watchlist.lookup(conn, "s_new", nos, NOW),
                "lookup_no_session": watchlist.lookup(conn, None, ["A1", "G1"], NOW),
                "lookup_empty": watchlist.lookup(conn, "s_new", [], NOW),
                # new_bargains는 내부에서 datetime.now()로 days_listed를 계산한다
                "new_bargains": watchlist.new_bargains(conn, "s_new", watched),
                "new_bargains_no_session": watchlist.new_bargains(conn, None, watched),
                "sell_days_complex": watchlist.bargain_sell_days(conn, "강남구", "래미안", NOW),
                "sell_days_district": watchlist.bargain_sell_days(conn, "강남구", "다른단지", NOW),
                "sell_days_none": watchlist.bargain_sell_days(conn, "서초구", "자이", NOW),
                "fair_price": {
                    no: watchlist.fair_price(conn, "s_new", no, NOW)
                    for no in ("A1", "A2", "A5", "A6", "B1", "B2", "NOPE")
                },
                "fair_price_no_session": watchlist.fair_price(conn, None, "A1", NOW),
            }
        for item in result["new_bargains"]:
            item.pop("days_listed")
        self.assertGolden("watchlist_queries", result)

    def test_tracking_queries(self):
        db = self.dbs["all"]
        with db.get_connection() as conn:
            result = {
                "history": {no: tracking.get_history(conn, no) for no in ("A1", "A2", "G1", "NOPE", 123)},
                "disappeared": tracking.list_disappeared(conn, "2026-10-13T09:10:00", 4),
                "tags": [tracking.disappearance_tag(row) for row in tracking.list_disappeared(conn, "2026-01-01", 50)],
            }
        self.assertGolden("tracking_queries", result)


class ViewCharacterizationTest(GoldenMixin, unittest.TestCase):
    """블루프린트 라우트의 상태 코드·응답 본문."""

    @classmethod
    def setUpClass(cls):
        cls._tmpdir = tempfile.TemporaryDirectory()
        os.environ["FORCE_LOCAL_SQLITE"] = "1"
        os.environ["DATABASE_URL"] = ""
        os.environ["DB_PATH"] = os.path.join(cls._tmpdir.name, "view-char-boot.db")
        os.environ["ENABLE_SCHEDULER"] = "false"
        os.environ["SEED_DEMO_DATA"] = "false"

        import app as app_module  # noqa: E402

        cls.app_module = app_module
        cls._original_db = app_module.db
        cls.client = app_module.app.test_client()
        cls.db = cls._build_recent_db()
        cls.empty_db = Database(db_path=os.path.join(cls._tmpdir.name, "view-empty.db"), skip_price_backfill=True)

    @classmethod
    def tearDownClass(cls):
        # 다른 테스트 파일이 같은 app 모듈을 재사용하므로 원래 db로 되돌린다.
        cls.app_module.db = cls._original_db
        cls.db.close()
        cls.empty_db.close()

    @classmethod
    def _build_recent_db(cls):
        """라우트는 실제 현재 시각을 쓰므로 지금 기준 상대 시각으로 데이터를 넣는다."""
        db = Database(db_path=os.path.join(cls._tmpdir.name, "view-char.db"), skip_price_backfill=True)
        now = datetime.now().replace(microsecond=0)

        def ago(**delta):
            return (now - timedelta(**delta)).isoformat()

        with db.get_connection() as conn:
            for session_id, crawled_at, source in (
                ("v_week", ago(days=7, hours=1), "naver"),
                ("v_demo", ago(days=1), "demo"),
                ("v_new", ago(hours=2), "naver"),
            ):
                conn.execute(
                    "INSERT INTO crawl_history (session_id, crawled_at, total_count, urgent_count, status, source) "
                    "VALUES (?, ?, 5, 5, 'success', ?)",
                    (session_id, crawled_at, source),
                )
            for session_id, total, created in (("v_week", 6, ago(days=7, hours=1)), ("v_new", 3, ago(hours=2))):
                conn.execute(
                    "INSERT INTO crawl_building_stats (session_id, region, district, building_name, total_count, "
                    "price_down_count, created_at) VALUES (?, '서울특별시', '강남구', '래미안', ?, 1, ?)",
                    (session_id, total, created),
                )
                conn.execute(
                    "INSERT INTO crawl_region_stats (session_id, region, district, total_count, price_down_count, "
                    "created_at) VALUES (?, '서울특별시', '강남구', ?, 1, ?)",
                    (session_id, total * 2, created),
                )
            for row in (
                listing_row("V1", "v_new", price_sort_value=80000, price="8억", bargain_discount=0.2,
                            tags='["가격인하"]', first_seen_at=ago(days=10, hours=3), crawled_at=ago(hours=2)),
                listing_row("V2", "v_new", first_seen_at=ago(days=3, hours=3), crawled_at=ago(hours=2)),
                listing_row("V3", "v_new", price_sort_value=105000, bargain_discount=-0.05,
                            first_seen_at=ago(days=40, hours=3), crawled_at=ago(hours=2)),
            ):
                columns = list(row)
                conn.execute(
                    f"INSERT INTO listings ({', '.join(columns)}) VALUES ({', '.join('?' * len(columns))})",
                    [row[column] for column in columns],
                )
            conn.execute(
                "INSERT INTO listing_tracking (article_no, first_seen_at, last_seen_at, last_price, "
                "last_price_sort_value, price_change_count, price_cut_count) VALUES ('V1', ?, ?, '8억', 80000, 1, 1)",
                (ago(days=10, hours=3), ago(hours=2)),
            )
            for session_id, observed, price, value, prev_price, prev_value, pct in (
                ("v_week", ago(days=10, hours=3), "10억", 100000, None, None, None),
                ("v_new", ago(days=2, hours=3), "8억", 80000, "10억", 100000, -20.0),
            ):
                conn.execute(
                    "INSERT INTO listing_price_events (article_no, session_id, observed_at, price, price_sort_value, "
                    "prev_price, prev_price_sort_value, change_pct) VALUES ('V1', ?, ?, ?, ?, ?, ?, ?)",
                    (session_id, observed, price, value, prev_price, prev_value, pct),
                )
            for no, gone, first_seen, bargain, down, reappeared in (
                ("D1", ago(hours=5), ago(days=6), 1, 0, None),
                ("D2", ago(days=3), ago(days=20, hours=1), 0, 1, None),
                ("D3", ago(days=5), None, 0, 0, None),
                ("D4", ago(days=6), ago(days=2), 1, 1, ago(days=1)),
                ("D5", ago(days=20), "garbage", 0, 0, None),
                ("D6", ago(days=60), ago(days=70), 1, 0, None),
            ):
                conn.execute(
                    "INSERT INTO listing_disappearances (article_no, session_id, disappeared_at, region, district, "
                    "building_name, property_type, trade_type, area, floor, price, naver_url, first_seen_at, "
                    "was_bargain, had_price_down, reappeared_at) "
                    "VALUES (?, 'v_x', ?, '서울특별시', '강남구', '래미안', '아파트', '매매', '84A', '3/20', '9억', '', ?, ?, ?, ?)",
                    (no, gone, first_seen, bargain, down, reappeared),
                )
        return db

    VOLATILE = {"first_seen_at", "disappeared_at", "reappeared_at", "observed_at", "last_seen_at", "saved_at",
                "crawled_at", "history"}  # history(일별 시계열)는 실행 시각에 따라 칸이 밀린다

    def _call(self, method, url, **kwargs):
        response = getattr(self.client, method)(url, **kwargs)
        return {
            "status": response.status_code,
            "json": response.get_json(),
            "cache": response.headers.get("Cache-Control"),
        }

    def _run(self, db):
        self.app_module.db = db
        calls = {
            "history_known": ("get", "/api/listings/V1/history", {}),
            "history_unknown": ("get", "/api/listings/NOPE/history", {}),
            "disappeared_default": ("get", "/api/disappeared", {}),
            "disappeared_1": ("get", "/api/disappeared?days=1", {}),
            "disappeared_2_rounds_up": ("get", "/api/disappeared?days=2&limit=2", {}),
            "disappeared_30": ("get", "/api/disappeared?days=30", {}),
            "disappeared_huge": ("get", "/api/disappeared?days=999&limit=0", {}),
            "disappeared_bad_args": ("get", "/api/disappeared?days=abc&limit=xyz", {}),
            "disappeared_negative": ("get", "/api/disappeared?days=-5&limit=99999", {}),
            "fast_complexes": ("get", "/api/disappeared/fast-complexes", {}),
            "fast_complexes_30": ("get", "/api/disappeared/fast-complexes?days=30", {}),
            "watch_status": ("post", "/api/watchlist/status", {"json": {"items": [
                {"article_no": "V1", "saved_price_value": 90000, "saved_at": "2026-01-01T00:00:00"},
                {"article_no": "V2", "saved_price_value": "100000"},
                {"article_no": "V3", "saved_price_value": "abc", "district": "x", "building_name": "y"},
                {"article_no": "D1", "saved_price_value": -3},
                {"article_no": "NOPE", "district": "강남구", "building_name": "래미안"},
                {"article_no": " "}, "junk", {"no": 1},
            ]}}),
            "watch_status_not_list": ("post", "/api/watchlist/status", {"json": {"items": "V1"}}),
            "watch_status_no_body": ("post", "/api/watchlist/status", {"data": "x"}),
            "watch_status_empty": ("post", "/api/watchlist/status", {"json": {"items": []}}),
            "compare": ("get", "/api/listings/compare?ids=V2, V1,V2,,NOPE,V3,D1", {}),
            "compare_missing": ("get", "/api/listings/compare", {}),
            "compare_blank": ("get", "/api/listings/compare?ids=,%20,", {}),
            "fair_price": ("get", "/api/listings/V1/fair-price", {}),
            "fair_price_unknown": ("get", "/api/listings/NOPE/fair-price", {}),
            "complex": ("get", "/api/complex?district=강남구&building_name=래미안", {}),
            "complex_missing_args": ("get", "/api/complex?district=강남구", {}),
            "complex_unknown": ("get", "/api/complex?district=강남구&building_name=없음", {}),
            "heatmap": ("get", "/api/heatmap", {}),
            "heatmap_unknown": ("get", "/api/heatmap?region=없는도", {}),
        }
        return {name: self._call(method, url, **kwargs) for name, (method, url, kwargs) in calls.items()}

    def test_routes_with_data(self):
        self.assertGolden("view_routes_with_data", self._run(self.db), strip=self.VOLATILE)

    def test_routes_with_empty_db(self):
        self.assertGolden("view_routes_with_empty_db", self._run(self.empty_db), strip=self.VOLATILE)

    def test_pages_render(self):
        self.app_module.db = self.db
        result = {
            path: self.client.get(path).status_code
            for path in ("/complex", "/heatmap", "/disappeared", "/watchlist")
        }
        self.assertGolden("view_pages", result)


if __name__ == "__main__":
    unittest.main()
