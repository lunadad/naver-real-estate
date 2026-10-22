"""텔레그램 전송과 로컬 알림 상태 파일."""

import json
import sys
import tempfile
import unittest
from datetime import date, timedelta
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import notifier  # noqa: E402
from notifier import AlertState, load_telegram_config, send_telegram  # noqa: E402


class TelegramConfigTest(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.hermes = Path(tmp.name) / ".env"

    def test_config_prefers_env_over_hermes(self):
        self.hermes.write_text("TELEGRAM_BOT_TOKEN=hermes\nTELEGRAM_HOME_CHANNEL=111\n")
        config = load_telegram_config(
            {"TELEGRAM_BOT_TOKEN": "env-token", "TELEGRAM_CHAT_ID": "222"}, self.hermes
        )
        self.assertEqual(config, {"token": "env-token", "chat_id": "222"})

    def test_config_reads_hermes_home_channel_and_thread(self):
        self.hermes.write_text(
            '# comment\n\nTELEGRAM_BOT_TOKEN="abc"\nTELEGRAM_HOME_CHANNEL=\'-100\'\n'
            "TELEGRAM_HOME_CHANNEL_THREAD_ID=7\n"
        )
        self.assertEqual(
            load_telegram_config({}, self.hermes),
            {"token": "abc", "chat_id": "-100", "thread_id": "7"},
        )

    def test_config_ignores_blank_thread_id(self):
        self.hermes.write_text("TELEGRAM_BOT_TOKEN=abc\nTELEGRAM_HOME_CHANNEL=1\nTELEGRAM_HOME_CHANNEL_THREAD_ID=\n")
        self.assertEqual(load_telegram_config({}, self.hermes), {"token": "abc", "chat_id": "1"})

    def test_config_none_when_missing(self):
        self.assertIsNone(load_telegram_config({}, self.hermes))
        self.hermes.write_text("TELEGRAM_BOT_TOKEN=abc\n")
        self.assertIsNone(load_telegram_config({}, self.hermes))


class SendTelegramTest(unittest.TestCase):
    CONFIG = {"token": "T", "chat_id": "C", "thread_id": "9"}

    def test_send_posts_expected_payload(self):
        post = mock.Mock()
        post.return_value.json.return_value = {"ok": True}
        self.assertTrue(send_telegram("hi", config=self.CONFIG, post=post))
        post.assert_called_once_with(
            "https://api.telegram.org/botT/sendMessage",
            json={"chat_id": "C", "text": "hi", "disable_web_page_preview": True, "message_thread_id": "9"},
            timeout=15,
        )

    def test_send_returns_false_and_never_raises_on_exception(self):
        post = mock.Mock(side_effect=ConnectionError("offline"))
        with self.assertLogs("notifier", level="WARNING"):
            self.assertFalse(send_telegram("hi", config=self.CONFIG, post=post))

    def test_send_returns_false_when_not_ok(self):
        post = mock.Mock()
        post.return_value.json.return_value = {"ok": False, "description": "chat not found"}
        with self.assertLogs("notifier", level="WARNING"):
            self.assertFalse(send_telegram("hi", config=self.CONFIG, post=post))

    def test_send_returns_false_when_not_configured(self):
        post = mock.Mock()
        with mock.patch.object(notifier, "load_telegram_config", return_value=None), \
                self.assertLogs("notifier", level="WARNING"):
            self.assertFalse(send_telegram("hi", post=post))
        post.assert_not_called()


class AlertStateTest(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.path = Path(tmp.name) / "logs" / "state.json"

    def test_alert_state_round_trip_and_take_pending_clears(self):
        today = date(2026, 10, 22)
        state = AlertState(self.path)
        self.assertFalse(state.had_failure(today))
        state.record_attempt(today, success=False)
        state.record_attempt(today, success=True)
        state.add_pending("msg-1")
        state.add_pending("msg-2")

        reloaded = AlertState(self.path)
        self.assertTrue(reloaded.had_failure(today))
        self.assertFalse(reloaded.had_failure(today - timedelta(days=1)))
        self.assertEqual(reloaded.take_pending(), ["msg-1", "msg-2"])
        self.assertEqual(AlertState(self.path).take_pending(), [])

    def test_alert_state_tolerates_corrupt_file(self):
        self.path.parent.mkdir(parents=True)
        self.path.write_text("{not json")
        with self.assertLogs("notifier", level="WARNING"):
            state = AlertState(self.path)
        self.assertEqual(state.take_pending(), [])
        state.record_attempt(date(2026, 10, 22), success=False)
        self.assertTrue(AlertState(self.path).had_failure(date(2026, 10, 22)))

    def test_had_success_is_recorded(self):
        today = date(2026, 10, 22)
        state = AlertState(self.path)
        state.record_attempt(today, success=False)
        self.assertFalse(AlertState(self.path).had_success(today))
        state.record_attempt(today, success=True)
        self.assertTrue(AlertState(self.path).had_success(today))

    def test_unwritable_state_never_raises(self):
        self.path.parent.mkdir(parents=True)
        self.path.parent.chmod(0o500)
        self.addCleanup(self.path.parent.chmod, 0o700)
        state = AlertState(self.path)
        with self.assertLogs("notifier", level="WARNING"):
            state.record_attempt(date(2026, 10, 22), success=False)
            state.add_pending("x")
            self.assertEqual(state.take_pending(), ["x"])
        self.assertTrue(state.had_failure(date(2026, 10, 22)))

    def test_bad_day_keys_are_ignored_when_pruning(self):
        self.path.parent.mkdir(parents=True)
        self.path.write_text('{"days": {"garbage": {"failures": 1}}, "pending": []}')
        state = AlertState(self.path)
        state.record_attempt(date(2026, 10, 22), success=True)
        self.assertTrue(AlertState(self.path).had_success(date(2026, 10, 22)))

    def test_save_is_atomic_and_leaves_no_temp_file(self):
        AlertState(self.path).record_attempt(date(2026, 10, 22), success=True)
        self.assertEqual([p.name for p in self.path.parent.iterdir()], [self.path.name])

    def test_alert_state_prunes_old_days(self):
        today = date(2026, 10, 22)
        state = AlertState(self.path)
        state.record_attempt(today - timedelta(days=31), success=False)
        state.record_attempt(today, success=False)
        days = json.loads(self.path.read_text())["days"]
        self.assertEqual(list(days), [today.isoformat()])


if __name__ == "__main__":
    unittest.main()
