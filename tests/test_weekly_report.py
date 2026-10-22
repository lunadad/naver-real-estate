"""주간 텔레그램 리포트: 데이터 조회, 메시지 포맷, 발송 스크립트, launchd plist."""

import os
import sys
import tempfile
import unittest
from datetime import datetime, timedelta
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "scripts"))

from database import Database  # noqa: E402
from weekly_report import format_weekly_report  # noqa: E402
import send_weekly_report  # noqa: E402
import install_launchd_weekly_report as installer  # noqa: E402

NOW = datetime(2026, 10, 26, 10, 30)
SITE = "https://example.test"


def add_session(db, session_id, crawled_at, total, *, status="success", source="naver",
                regions=None, buildings=None):
    with db.get_connection() as conn:
        conn.execute(
            "INSERT INTO crawl_history (session_id, crawled_at, total_count, urgent_count, status, source) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (session_id, crawled_at.isoformat(), total, 0, status, source),
        )
        for district, count, down in regions or []:
            conn.execute(
                "INSERT INTO crawl_region_stats (session_id, region, district, total_count, price_down_count, created_at) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                (session_id, "서울특별시", district, count, down, crawled_at.isoformat()),
            )
        for district, building, count in buildings or []:
            conn.execute(
                "INSERT INTO crawl_building_stats (session_id, region, district, building_name, total_count, "
                "price_down_count, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
                (session_id, "서울특별시", district, building, count, 0, crawled_at.isoformat()),
            )


def add_listing(db, session_id, article_no, discount, *, building="래미안", district="강남구",
                area="84A", floor="12", price="10억", url=None):
    with db.get_connection() as conn:
        conn.execute(
            "INSERT INTO listings (article_no, region, district, property_type, trade_type, price, area, floor, "
            "building_name, crawl_session, naver_url, price_sort_value, bargain_discount, bargain_basis) "
            "VALUES (?, '서울특별시', ?, '아파트', '매매', ?, ?, ?, ?, ?, ?, 100000, ?, 'same_area')",
            (article_no, district, price, area, floor, building, session_id,
             url or f"https://n.test/{article_no}", discount),
        )


class WeeklyReportTestBase(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.db = Database(db_path=os.path.join(tmp.name, "w.db"), skip_price_backfill=True)
        self.addCleanup(self.db.close)

    def seed_three_weeks(self):
        add_session(self.db, "s0", NOW - timedelta(days=14), 800,
                    regions=[("강남구", 800, 80)],
                    buildings=[("강남구", "구단지", 5)])
        add_session(self.db, "s1", NOW - timedelta(days=7), 1000,
                    regions=[("강남구", 600, 60), ("서초구", 400, 40)],
                    buildings=[("강남구", "A", 3), ("강남구", "B", 10)])
        add_session(self.db, "s2", NOW, 1100,
                    regions=[("강남구", 700, 105), ("서초구", 400, 40)],
                    buildings=[("강남구", "A", 8), ("강남구", "B", 4)])


class WeeklyReportDataTest(WeeklyReportTestBase):
    def test_report_compares_with_session_seven_days_ago(self):
        self.seed_three_weeks()
        data = self.db.get_weekly_report_data(NOW)
        self.assertEqual(data["latest"]["session_id"], "s2")
        self.assertEqual(data["latest"]["total_count"], 1100)
        self.assertEqual(data["previous"]["session_id"], "s1")
        self.assertEqual(data["previous"]["total_count"], 1000)
        self.assertAlmostEqual(data["ratio_now"], 145 / 1100 * 100)
        self.assertAlmostEqual(data["ratio_prev"], 100 / 1000 * 100)
        text = format_weekly_report(data, SITE)
        self.assertIn("10/19 → 10/26", text)
        self.assertIn("급매 1,100건 (+100, +10.0%)", text)
        self.assertIn("가격인하 비율 13.2% (+3.2%p)", text)

    def test_previous_picks_closest_session_within_twelve_hour_tolerance(self):
        add_session(self.db, "old", NOW - timedelta(days=7, hours=11), 900)
        add_session(self.db, "recent", NOW - timedelta(days=5), 950)
        add_session(self.db, "s2", NOW, 1000)
        add_session(self.db, "demo", NOW - timedelta(days=7), 1, source="demo")
        add_session(self.db, "bad", NOW - timedelta(days=7), 1, status="failed")
        data = self.db.get_weekly_report_data(NOW)
        # 7일 - 12시간 허용 범위 안에서 가장 최근인 'old' (demo/failed 제외)
        self.assertEqual(data["previous"]["session_id"], "old")

    def test_previous_falls_back_to_oldest_when_nothing_is_a_week_old(self):
        add_session(self.db, "a", NOW - timedelta(days=3), 900)
        add_session(self.db, "b", NOW - timedelta(days=1), 950)
        add_session(self.db, "c", NOW, 1000)
        data = self.db.get_weekly_report_data(NOW)
        self.assertEqual(data["previous"]["session_id"], "a")

    def test_report_lists_top_risers_and_fallers_limited_to_five(self):
        add_session(self.db, "p", NOW - timedelta(days=7), 100,
                    buildings=[("강남구", f"단지{i}", 10) for i in range(8)])
        add_session(self.db, "c", NOW, 100,
                    buildings=[("강남구", f"단지{i}", 10 + (i + 1) * (1 if i < 4 else -1)) for i in range(8)]
                    + [("서초구", "신규", 3)])
        data = self.db.get_weekly_report_data(NOW)
        self.assertEqual(len(data["risers"]), 5)
        self.assertEqual(len(data["fallers"]), 4)
        self.assertEqual(data["risers"][0], ("강남구", "단지3", 10, 14, 4))
        self.assertEqual([r[4] for r in data["risers"]], sorted((r[4] for r in data["risers"]), reverse=True))
        self.assertEqual(data["fallers"][0][1], "단지7")
        self.assertEqual(data["fallers"][0][4], -8)
        text = format_weekly_report(data, SITE)
        self.assertIn("📈 급매 늘어난 단지", text)
        self.assertIn("· 단지3 (강남구) 10→14건 (+4)", text)
        self.assertIn("📉 급매 줄어든 단지", text)
        self.assertIn("· 단지7 (강남구) 10→2건 (-8)", text)

    def test_report_bargains_use_stored_discount_and_count_anomalies(self):
        add_session(self.db, "s", NOW, 50)
        add_listing(self.db, "s", "n1", 0.25, building="A단지", price="7억 5,000", floor="8")
        add_listing(self.db, "s", "n2", 0.12)
        add_listing(self.db, "s", "n3", 0.05)   # 급매 아님
        add_listing(self.db, "s", "n4", 0.45)   # 이상치
        add_listing(self.db, "s", "n5", None)   # 점수 없음
        add_listing(self.db, "s", "n6", 0.4001)  # 이상치 경계
        add_listing(self.db, "old", "n7", 0.3)   # 다른 세션
        data = self.db.get_weekly_report_data(NOW)
        self.assertEqual(data["bargain_total"], 2)
        self.assertEqual(data["anomaly_count"], 2)
        self.assertEqual([b["article_no"] for b in data["bargains"]], ["n1", "n2"])
        text = format_weekly_report(data, SITE)
        self.assertIn("· A단지 (강남구) 84A㎡ 8층 7억 5,000 (−25%)\n  https://n.test/n1", text)
        self.assertIn("(40%↑ 차이 2건은 지분·오류 의심으로 제외)", text)
        self.assertNotIn("n4", text)

    def test_bargains_limited_to_top_five_by_discount(self):
        add_session(self.db, "s", NOW, 50)
        for i in range(8):
            add_listing(self.db, "s", f"n{i}", 0.10 + i * 0.02)
        data = self.db.get_weekly_report_data(NOW)
        self.assertEqual(data["bargain_total"], 8)
        self.assertEqual(len(data["bargains"]), 5)
        self.assertEqual(data["bargains"][0]["article_no"], "n7")
        self.assertIn("8건 중 상위 5", format_weekly_report(data, SITE))

    def test_report_without_previous_week_or_building_stats(self):
        add_session(self.db, "only", NOW, 120)
        add_listing(self.db, "only", "n1", 0.2)
        data = self.db.get_weekly_report_data(NOW)
        self.assertIsNone(data["previous"])
        self.assertIsNone(data["ratio_now"])
        text = format_weekly_report(data, SITE)
        self.assertTrue(text.startswith("📊 주간 급매 리포트"))
        self.assertIn("급매 120건", text)
        self.assertNotIn("📈", text)
        self.assertNotIn("📉", text)
        self.assertNotIn("%p", text)
        self.assertIn("진짜 급매", text)
        self.assertTrue(text.endswith(SITE))

    def test_report_with_previous_but_no_building_stats_omits_movers(self):
        add_session(self.db, "p", NOW - timedelta(days=7), 100)
        add_session(self.db, "c", NOW, 110, buildings=[("강남구", "A", 3)])
        data = self.db.get_weekly_report_data(NOW)
        text = format_weekly_report(data, SITE)
        self.assertIn("(+10, +10.0%)", text)
        self.assertNotIn("📈", text)

    def test_report_none_when_no_live_crawl(self):
        self.assertIsNone(self.db.get_weekly_report_data(NOW))
        add_session(self.db, "d", NOW, 5, source="demo")
        add_session(self.db, "f", NOW, 5, status="failed")
        self.assertIsNone(self.db.get_weekly_report_data(NOW))


class SendScriptTest(WeeklyReportTestBase):
    def test_send_script_dry_run_does_not_send(self):
        add_session(self.db, "s", NOW, 10)
        send = mock.Mock(return_value=True)
        out = []
        code = send_weekly_report.run(
            mock.Mock(database_url="x", dry_run=True),
            now_fn=lambda: NOW, db_factory=lambda url: self.db, send=send, out=out.append,
        )
        self.assertEqual(code, 0)
        send.assert_not_called()
        self.assertIn("📊 주간 급매 리포트", out[0])

    def test_send_script_sends_and_reports_failure(self):
        add_session(self.db, "s", NOW, 10)
        args = mock.Mock(database_url="x", dry_run=False)
        ok = mock.Mock(return_value=True)
        self.assertEqual(send_weekly_report.run(
            args, now_fn=lambda: NOW, db_factory=lambda u: self.db, send=ok, out=lambda s: None), 0)
        ok.assert_called_once()
        bad = mock.Mock(return_value=False)
        self.assertEqual(send_weekly_report.run(
            args, now_fn=lambda: NOW, db_factory=lambda u: self.db, send=bad, out=lambda s: None), 1)

    def test_send_script_exits_1_without_data(self):
        send = mock.Mock(return_value=True)
        code = send_weekly_report.run(
            mock.Mock(database_url="x", dry_run=False),
            now_fn=lambda: NOW, db_factory=lambda u: self.db, send=send, out=lambda s: None)
        self.assertEqual(code, 1)
        send.assert_not_called()


class InstallerTest(unittest.TestCase):
    def test_plist_payload_runs_weekly_on_monday_morning(self):
        args = installer.build_parser().parse_args(["--database-url", "postgresql://u:p@h/db", "--python", "/py"])
        plist = installer.build_plist_payload(args)
        self.assertEqual(plist["Label"], "com.lunadad.naver-real-estate-weekly-report")
        self.assertEqual(plist["StartCalendarInterval"], {"Weekday": 1, "Hour": 10, "Minute": 30})
        pa = plist["ProgramArguments"]
        self.assertEqual(pa[0], "/py")
        self.assertTrue(pa[1].endswith("scripts/send_weekly_report.py"))
        self.assertEqual(pa[pa.index("--database-url") + 1], "postgresql://u:p@h/db?sslmode=require")
        self.assertTrue(plist["StandardOutPath"].startswith(str(ROOT / "logs")))

    def test_database_url_read_from_existing_crawl_plist(self):
        import plistlib
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "crawl.plist"
            path.write_bytes(plistlib.dumps({"ProgramArguments": ["/bin/bash", "x.sh", "--database-url", "postgresql://a/b"]}))
            self.assertEqual(installer.read_database_url_from_plist(path), "postgresql://a/b")
            self.assertIsNone(installer.read_database_url_from_plist(Path(tmp) / "missing.plist"))


if __name__ == "__main__":
    unittest.main()
