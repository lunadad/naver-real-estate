"""⑥ 텔레그램 질의 봇 명령과 ⑤의 텔레그램 진짜 급매 알림(맥미니 로컬 규칙)."""

import json
import sys
import tempfile
import unittest
from datetime import datetime, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import telegram_commands as cmds  # noqa: E402
from telegram_alerts import TelegramRuleStore, dispatch_telegram_deal_alerts  # noqa: E402

DISTRICTS = [
    {"region": "서울특별시", "district": "송파구"},
    {"region": "서울특별시", "district": "강남구"},
    {"region": "경기도", "district": "성남시 분당구"},
]
NOW = datetime(2026, 10, 23, 9, 40)


def deal(no, discount, price_value, **extra):
    row = {
        "article_no": no, "district": "송파구", "building_name": f"단지{no}", "area": "84",
        "floor": "10/20", "price": f"{price_value // 10000}억", "price_sort_value": price_value,
        "bargain_discount": discount, "trade_type": "매매", "naver_url": f"https://n.test/{no}",
        "confirmed_date": "20261023",
    }
    row.update(extra)
    return row


class RuleStoreTest(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.path = Path(tmp.name) / "logs" / "rules.json"

    def test_add_list_remove_round_trip(self):
        store = TelegramRuleStore(self.path)
        first = store.add(district="송파구", region="서울특별시", max_price=200000, min_discount=0.15, now=NOW)
        second = store.add(district="강남구", region="서울특별시", now=NOW)
        self.assertEqual((first["id"], second["id"]), (1, 2))
        self.assertEqual([r["id"] for r in TelegramRuleStore(self.path).list()], [1, 2])
        self.assertTrue(store.remove(1))
        self.assertFalse(store.remove(1))
        self.assertEqual([r["district"] for r in TelegramRuleStore(self.path).list()], ["강남구"])

    def test_corrupt_file_starts_empty(self):
        self.path.parent.mkdir(parents=True)
        self.path.write_text("{oops")
        with self.assertLogs("telegram_alerts", level="WARNING"):
            self.assertEqual(TelegramRuleStore(self.path).list(), [])

    def test_describe_rule(self):
        store = TelegramRuleStore(self.path)
        rule = store.add(district="송파구", region="서울특별시", max_price=200000, min_discount=0.15, now=NOW)
        self.assertEqual(cmds.describe_rule(rule), "#1 송파구 · 20억 이하 · 15%↓")


class FakeDb:
    def __init__(self, rows):
        self.rows = rows
        self.calls = []

    def get_listings(self, **kwargs):
        self.calls.append(kwargs)
        rows = [r for r in self.rows if not kwargs.get("district") or kwargs["district"] in r["district"]]
        if kwargs.get("max_price"):
            rows = [r for r in rows if r["price_sort_value"] <= kwargs["max_price"]]
        return {"listings": rows, "total": len(rows)}


class DispatchTest(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.store = TelegramRuleStore(Path(tmp.name) / "rules.json")
        self.store.add(district="송파구", region="서울특별시", max_price=200000, min_discount=0.15, now=NOW)
        self.db = FakeDb([
            deal("1", 0.30, 180000), deal("2", 0.12, 150000), deal("3", 0.20, 250000),
            deal("4", 0.18, 170000), deal("5", 0.45, 100000),
        ])

    def test_sends_one_message_with_matching_bargains_once(self):
        sent = []
        unsent = dispatch_telegram_deal_alerts(self.db, self.store, lambda t: sent.append(t) or True, now=NOW)
        self.assertEqual(unsent, [])
        self.assertEqual(len(sent), 1)
        text = sent[0]
        self.assertIn("[진짜 급매 알림] 송파구 · 20억 이하 · 15%↓ — 새 2건", text)
        self.assertLess(text.index("단지1"), text.index("단지4"))  # 할인율 큰 순
        for excluded in ("단지2", "단지3", "단지5"):  # 할인 부족 / 상한 초과 / 이상치
            self.assertNotIn(excluded, text)
        self.assertEqual(self.db.calls[0]["bargain_only"], True)
        self.assertEqual(self.db.calls[0]["trade_type"], "매매")

        sent.clear()
        dispatch_telegram_deal_alerts(self.db, self.store, lambda t: sent.append(t) or True, now=NOW)
        self.assertEqual(sent, [])

    def test_failed_send_is_returned_for_pending_queue_and_not_resent(self):
        # 못 보낸 메시지는 pending 큐가 재전송하므로, 다음 발송에서 "새 급매"로 중복되면 안 된다
        unsent = dispatch_telegram_deal_alerts(self.db, self.store, lambda t: False, now=NOW)
        self.assertEqual(len(unsent), 1)
        sent = []
        dispatch_telegram_deal_alerts(self.db, self.store, lambda t: sent.append(t) or True, now=NOW)
        self.assertEqual(sent, [])

    def test_sent_records_expire_after_30_days(self):
        dispatch_telegram_deal_alerts(self.db, self.store, lambda t: True, now=NOW)
        sent = []
        dispatch_telegram_deal_alerts(
            self.db, self.store, lambda t: sent.append(t) or True, now=NOW + timedelta(days=31)
        )
        self.assertEqual(len(sent), 1)

    def test_no_rules_no_queries(self):
        empty = TelegramRuleStore(self.store.path.with_name("none.json"))
        self.assertEqual(dispatch_telegram_deal_alerts(self.db, empty, lambda t: True, now=NOW), [])
        self.assertEqual(self.db.calls, [])


class CommandsTest(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.rules_path = Path(tmp.name) / "rules.json"
        self.requests = []

    def fetch(self, path, params=None):
        self.requests.append((path, dict(params or {})))
        if path == "/api/listings":
            if params.get("search"):
                return {"listings": [
                    {"district": "송파구", "building_name": "헬리오시티"},
                    {"district": "송파구", "building_name": "헬리오시티"},
                    {"district": "강동구", "building_name": "헬리오시티2"},
                ]}
            return {"listings": [deal("1", 0.30, 180000), deal("2", 0.12, 150000), deal("4", 0.18, 170000)]}
        if path == "/api/complex":
            return {"district": "송파구", "building_name": "헬리오시티",
                    "stats": {"current_count": 41, "week_ago_count": 35, "bargain_count": 5, "price_down_ratio": 0.098}}
        if path == "/api/weekly-report":
            return "📊 주간 급매 리포트\n..."
        raise AssertionError(path)

    def test_deals_formats_top_bargains(self):
        reply = cmds.handle_deals("송파 20억 15%", fetch=self.fetch, districts=DISTRICTS)
        path, params = self.requests[0]
        self.assertEqual(path, "/api/listings")
        self.assertEqual(params["district"], "송파구")
        self.assertEqual(params["max_price"], 200000)
        self.assertEqual(params["bargain_only"], "true")
        self.assertIn("송파구 · 20억 이하 · 15%↓ 진짜 급매 2건", reply)
        self.assertLess(reply.index("단지1"), reply.index("단지4"))
        self.assertNotIn("단지2", reply)

    def test_deals_without_region_shows_usage(self):
        self.assertIn("사용법", cmds.handle_deals("", fetch=self.fetch, districts=DISTRICTS))
        self.assertIn("지역을 찾지 못했습니다", cmds.handle_deals("화성인마을", fetch=self.fetch, districts=DISTRICTS))
        self.assertEqual(self.requests, [])

    def test_deals_network_error_is_friendly(self):
        def broken(path, params=None):
            raise OSError("offline")
        self.assertIn("사이트에 연결하지 못했습니다", cmds.handle_deals("송파", fetch=broken, districts=DISTRICTS))

    def test_complex_finds_most_common_match(self):
        reply = cmds.handle_complex("헬리오시티", fetch=self.fetch)
        self.assertEqual(self.requests[-1], ("/api/complex", {"district": "송파구", "building_name": "헬리오시티"}))
        self.assertIn("헬리오시티 (송파구)", reply)
        self.assertIn("급매 41건 · 지난주 대비 +6", reply)
        self.assertIn("/complex?district=", reply)

    def test_weekly_returns_report_text(self):
        self.assertTrue(cmds.handle_weekly("", fetch=self.fetch).startswith("📊 주간 급매 리포트"))

    def test_dealalert_add_list_delete(self):
        store = TelegramRuleStore(self.rules_path)
        add = cmds.handle_dealalert("add 송파 20억 15%", store=store, districts=DISTRICTS, now=NOW)
        self.assertIn("#1 송파구 · 20억 이하 · 15%↓", add)
        self.assertIn("#1", cmds.handle_dealalert("list", store=store, districts=DISTRICTS, now=NOW))
        self.assertIn("삭제", cmds.handle_dealalert("del 1", store=store, districts=DISTRICTS, now=NOW))
        self.assertIn("등록된 알림이 없습니다", cmds.handle_dealalert("list", store=store, districts=DISTRICTS, now=NOW))
        self.assertIn("사용법", cmds.handle_dealalert("", store=store, districts=DISTRICTS, now=NOW))
        self.assertIn("지역을 찾지 못했습니다", cmds.handle_dealalert("add 없는곳", store=store, districts=DISTRICTS, now=NOW))
        self.assertTrue(json.loads(self.rules_path.read_text()))


if __name__ == "__main__":
    unittest.main()
