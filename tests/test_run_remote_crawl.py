"""하루 3회 재시도, 오늘 성공 시 건너뛰기, 텔레그램 알림 규칙."""

import os
import plistlib
import sys
import tempfile
import unittest
from datetime import date, datetime
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "scripts"))

from database import Database  # noqa: E402
from notifier import AlertState  # noqa: E402
import run_remote_crawl  # noqa: E402
from run_remote_crawl import decide_alerts, run  # noqa: E402
import install_launchd_crawl  # noqa: E402


class HasSuccessfulLiveCrawlOnTest(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.db = Database(db_path=os.path.join(tmp.name, "t.db"), skip_price_backfill=True)
        self.addCleanup(self.db.close)

    def _log(self, crawled_at, status="success", source="naver"):
        with self.db.get_connection() as conn:
            conn.execute(
                "INSERT INTO crawl_history (session_id, crawled_at, total_count, urgent_count, status, source) "
                "VALUES ('s', ?, 1, 1, ?, ?)",
                (crawled_at, status, source),
            )

    def test_has_successful_live_crawl_on_uses_day_bounds(self):
        day = date(2026, 10, 22)
        self._log("2026-10-21T23:59:59")
        self._log("2026-10-22T09:00:00", status="failed")
        self._log("2026-10-22T10:00:00", source="demo")
        self._log("2026-10-23T00:00:00")
        self.assertFalse(self.db.has_successful_live_crawl_on(day))
        self._log("2026-10-22T00:00:00")
        self.assertTrue(self.db.has_successful_live_crawl_on(day))


AT_10 = datetime(2026, 10, 22, 10, 5)
AT_15 = datetime(2026, 10, 22, 15, 20)


def alerts(**overrides):
    params = dict(
        label="주거용", success=True, now=AT_10, final_hour=15,
        had_earlier_failure=False, failed_combos=0, total_combos=1773, error=None,
    )
    params.update(overrides)
    return decide_alerts(**params)


class DecideAlertsTest(unittest.TestCase):
    def test_normal_success_sends_nothing(self):
        self.assertEqual(alerts(), [])

    def test_failure_before_final_slot_sends_nothing(self):
        self.assertEqual(alerts(success=False, error="PoolTimeout: x"), [])

    def test_failure_at_final_slot_sends_final_failure(self):
        self.assertEqual(
            alerts(success=False, now=AT_15, error="PoolTimeout: no connection"),
            ["[주거용] ❌ 오늘 크롤링 최종 실패 (15:20)\n원인: PoolTimeout: no connection\n"
             "이전 정상 데이터가 계속 표시됩니다."],
        )

    def test_final_failure_without_error_mentions_status_and_failed_combos(self):
        self.assertEqual(
            alerts(success=False, now=AT_15, failed_combos=400, total_combos=1773),
            ["[주거용] ❌ 오늘 크롤링 최종 실패 (15:20)\n원인: status=failed (실패 조합 400/1773)\n"
             "이전 정상 데이터가 계속 표시됩니다."],
        )

    def test_success_after_earlier_failure_sends_recovery(self):
        self.assertEqual(
            alerts(had_earlier_failure=True),
            ["[주거용] ✅ 크롤링 복구 (10:05) — 앞선 시도 실패 후 성공"],
        )

    def test_success_with_missing_combos_sends_partial_notice(self):
        self.assertEqual(
            alerts(failed_combos=3),
            ["[주거용] ⚠️ 일부 조합 수집 실패 3/1773 — 저장은 완료"],
        )

    def test_recovery_and_partial_are_merged_into_one_message(self):
        self.assertEqual(
            alerts(had_earlier_failure=True, failed_combos=3, label="상업용"),
            ["[상업용] ✅ 크롤링 복구 (10:05) — 앞선 시도 실패 후 성공\n"
             "[상업용] ⚠️ 일부 조합 수집 실패 3/1773 — 저장은 완료"],
        )


class RunTest(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.state_path = Path(tmp.name) / "state.json"
        self.db = mock.Mock()
        self.db.has_successful_live_crawl_on.return_value = False
        self.crawler = mock.Mock()
        self.crawler.crawl_all.return_value = {
            "status": "success", "total": 10, "urgent": 10, "source": "naver",
            "failed_combos": 0, "total_combos": 9,
        }
        self.sent = []

    def _args(self, **overrides):
        values = dict(
            database_url="postgresql://x", skip_if_succeeded_today=True, final_hour=15,
            label="주거용", alert_state=str(self.state_path),
        )
        values.update(overrides)
        return SimpleNamespace(**values)

    def _run(self, now=AT_10, db_factory=None, send_ok=True, deal_alerts=None, **arg_overrides):
        def send(text):
            self.sent.append(text)
            return send_ok

        return run(
            self._args(**arg_overrides),
            now_fn=lambda: now,
            db_factory=db_factory or (lambda url: self.db),
            crawler_factory=lambda db: self.crawler,
            send=send,
            deal_alerts=deal_alerts or (lambda db, send: []),
        )

    def test_run_skips_when_already_succeeded_today(self):
        self.db.has_successful_live_crawl_on.return_value = True
        self.assertEqual(self._run(), 0)
        self.crawler.crawl_all.assert_not_called()
        self.db.has_successful_live_crawl_on.assert_called_once_with(date(2026, 10, 22))
        self.assertEqual(self.sent, [])
        self.db.close.assert_called_once()

    def test_run_without_skip_flag_crawls_even_if_succeeded(self):
        self.db.has_successful_live_crawl_on.return_value = True
        self.assertEqual(self._run(skip_if_succeeded_today=False), 0)
        self.crawler.crawl_all.assert_called_once()

    def test_run_failed_crawl_before_final_slot_exits_1_without_alert(self):
        self.crawler.crawl_all.return_value = {"status": "failed", "total": 0, "urgent": 0,
                                               "source": "naver", "failed_combos": 0, "total_combos": 0}
        self.assertEqual(self._run(), 1)
        self.assertEqual(self.sent, [])
        self.assertTrue(AlertState(self.state_path).had_failure(date(2026, 10, 22)))

    def test_run_db_connect_failure_at_final_hour_alerts_and_exits_1(self):
        def broken(url):
            raise RuntimeError("couldn't get a connection after 10.00 sec")

        self.assertEqual(self._run(now=AT_15, db_factory=broken), 1)
        self.assertEqual(len(self.sent), 1)
        self.assertIn("❌ 오늘 크롤링 최종 실패", self.sent[0])
        self.assertIn("RuntimeError: couldn't get a connection", self.sent[0])

    def test_run_recovery_after_earlier_failure(self):
        AlertState(self.state_path).record_attempt(date(2026, 10, 22), success=False)
        self.assertEqual(self._run(), 0)
        self.assertEqual(self.sent, ["[주거용] ✅ 크롤링 복구 (10:05) — 앞선 시도 실패 후 성공"])

    def test_run_failed_send_is_kept_pending_and_sent_next_run(self):
        def broken(url):
            raise RuntimeError("offline")

        self._run(now=AT_15, db_factory=broken, send_ok=False)
        failure_message = self.sent[-1]
        self.assertEqual(AlertState(self.state_path)._data["pending"], [failure_message])

        self.sent.clear()
        next_morning = datetime(2026, 10, 23, 9, 30)
        self.assertEqual(self._run(now=next_morning), 0)
        self.assertEqual(self.sent, [failure_message])
        self.assertEqual(AlertState(self.state_path).take_pending(), [])

    def test_retry_slot_after_local_success_skips_without_touching_db(self):
        AlertState(self.state_path).record_attempt(date(2026, 10, 22), success=True)

        def broken(url):
            raise OSError("network is unreachable")

        self.assertEqual(self._run(now=AT_15, db_factory=broken), 0)
        self.assertEqual(self.sent, [])
        self.crawler.crawl_all.assert_not_called()

    def test_run_ending_after_midnight_counts_as_final_slot(self):
        times = iter([datetime(2026, 10, 22, 15, 0), datetime(2026, 10, 23, 0, 40)])
        self.crawler.crawl_all.return_value = {"status": "failed", "failed_combos": 0, "total_combos": 0}
        result = run(
            self._args(),
            now_fn=lambda: next(times),
            db_factory=lambda url: self.db,
            crawler_factory=lambda db: self.crawler,
            send=lambda text: self.sent.append(text) or True,
        )
        self.assertEqual(result, 1)
        self.assertEqual(len(self.sent), 1)
        self.assertIn("최종 실패", self.sent[0])

    def test_alert_redacts_database_credentials(self):
        def broken(url):
            raise RuntimeError('invalid dsn: "postgresql://neondb_owner:npg_SECRET@host/db" password=npg_SECRET2 x')

        self._run(now=AT_15, db_factory=broken)
        self.assertNotIn("npg_SECRET", self.sent[0])
        self.assertIn("postgresql://***@host/db", self.sent[0])
        self.assertIn("password=***", self.sent[0])

    def test_unwritable_alert_state_still_crawls_and_alerts(self):
        self.state_path.parent.chmod(0o500)
        self.addCleanup(self.state_path.parent.chmod, 0o700)

        def broken(url):
            raise RuntimeError("down")

        self.assertEqual(self._run(now=AT_15, db_factory=broken), 1)
        self.assertEqual(len(self.sent), 1)

    def test_deal_alerts_dispatched_after_successful_crawl(self):
        calls = []

        def deal_alerts(db, send):
            calls.append(db)
            return ["unsent deal message"]

        self.assertEqual(self._run(deal_alerts=deal_alerts), 0)
        self.assertEqual(calls, [self.db])
        self.assertEqual(AlertState(self.state_path)._data["pending"], ["unsent deal message"])

    def test_deal_alerts_skipped_when_crawl_failed(self):
        self.crawler.crawl_all.return_value = {"status": "failed", "failed_combos": 0, "total_combos": 0}
        calls = []
        self._run(deal_alerts=lambda db, send: calls.append(db) or [])
        self.assertEqual(calls, [])

    def test_deal_alert_crash_does_not_fail_crawl(self):
        def boom(db, send):
            raise RuntimeError("bad rules file")

        self.assertEqual(self._run(deal_alerts=boom), 0)

    def test_pending_that_fails_again_stays_pending_once(self):
        AlertState(self.state_path).add_pending("old")
        self._run(send_ok=False)
        self.assertEqual(AlertState(self.state_path)._data["pending"], ["old"])


class InstallLaunchdCrawlTest(unittest.TestCase):
    def _plist(self, *argv):
        parser = install_launchd_crawl.build_parser()
        args = parser.parse_args(["--database-url", "postgresql://u:p@h/db", "--mode", "agent", *argv])
        return install_launchd_crawl.make_plist(args)

    def test_install_plist_has_three_slots_and_skip_args(self):
        plist = self._plist("--hour", "9", "--retry-hours", "12,15", "--final-hour", "15", "--label-text", "주거용")
        self.assertEqual(
            plist["StartCalendarInterval"],
            [{"Hour": 9, "Minute": 0}, {"Hour": 12, "Minute": 0}, {"Hour": 15, "Minute": 0}],
        )
        self.assertEqual(
            plist["ProgramArguments"][2:],
            ["--database-url", "postgresql://u:p@h/db?sslmode=require",
             "--skip-if-succeeded-today", "--final-hour", "15", "--label", "주거용"],
        )
        plistlib.dumps(plist)  # 직렬화 가능

    def test_install_plist_without_retry_hours_keeps_single_slot(self):
        plist = self._plist("--hour", "10")
        self.assertEqual(plist["StartCalendarInterval"], [{"Hour": 10, "Minute": 0}])
        self.assertIn("--skip-if-succeeded-today", plist["ProgramArguments"])


if __name__ == "__main__":
    unittest.main()
