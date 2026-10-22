"""⑤ 진짜 급매 맞춤 알림 — 할인율·가격 상한 조건, 매매가 상한 필터, 주간 리포트 API."""

import os
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "tests"))

from database import Database  # noqa: E402
from deal_query import format_manwon, parse_price  # noqa: E402
from test_bargain import SEED  # noqa: E402


class MoneyHelpersTest(unittest.TestCase):
    def test_parse_price(self):
        self.assertEqual(parse_price("20억"), 200000)
        self.assertEqual(parse_price("15억5천"), 155000)
        self.assertEqual(parse_price("15억 5,000"), 155000)
        self.assertEqual(parse_price("9000"), 9000)
        self.assertEqual(parse_price("9,000만"), 9000)
        self.assertIsNone(parse_price(""))
        self.assertIsNone(parse_price("싸게"))

    def test_format_manwon(self):
        self.assertEqual(format_manwon(200000), "20억")
        self.assertEqual(format_manwon(155000), "15억 5,000")
        self.assertEqual(format_manwon(9000), "9,000만")


class BargainAlertRuleTest(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.db = Database(db_path=os.path.join(tmp.name, "a.db"), skip_price_backfill=True)
        self.addCleanup(self.db.close)

    def _matches(self, **rule):
        self.db.create_alert_rule("c1", **rule)
        self.db.insert_listings(SEED, "s1")
        self.db.log_crawl("s1", len(SEED), len(SEED), "success", "naver")
        return sorted(m["article_no"] for m in self.db.get_new_alert_matches("c1"))

    def test_rule_stores_bargain_fields_and_auto_name(self):
        rule = self.db.create_alert_rule(
            "c1", district="강남구", bargain_only=True, min_discount=0.15, max_price=200000
        )
        self.assertEqual((rule["bargain_only"], rule["min_discount"], rule["max_price"]), (1, 0.15, 200000))
        self.assertEqual(rule["name"], "강남구 · 진짜 급매 15%↓ · 20억 이하")

    def test_plain_rule_keeps_old_defaults(self):
        rule = self.db.create_alert_rule("c1", district="강남구")
        self.assertEqual((rule["bargain_only"], rule["min_discount"], rule["max_price"]), (0, None, None))
        self.assertEqual(rule["name"], "강남구")

    def test_bargain_only_matches_only_true_bargains(self):
        # A3 −20% 진짜 급매, A4 −50% 이상치 제외, 나머지는 할인 없음
        self.assertEqual(self._matches(bargain_only=True), ["A3"])

    def test_min_discount_raises_the_bar(self):
        self.assertEqual(self._matches(bargain_only=True, min_discount=0.25), [])

    def test_min_discount_below_ten_percent_is_clamped(self):
        self.assertEqual(self._matches(bargain_only=True, min_discount=0.05), ["A3"])

    def test_max_price_filters_by_sale_price(self):
        self.assertEqual(self._matches(bargain_only=True, max_price=40000), ["A3"])

    def test_max_price_excludes_pricier(self):
        self.assertEqual(self._matches(district="강남구", max_price=30000), ["A4", "R1"])


class BargainAlertApiTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls._tmp = tempfile.TemporaryDirectory()
        db_path = os.path.join(cls._tmp.name, "api.db")
        os.environ["FORCE_LOCAL_SQLITE"] = "1"
        os.environ["DATABASE_URL"] = ""
        os.environ["DB_PATH"] = db_path
        os.environ["ENABLE_SCHEDULER"] = "false"
        os.environ["SEED_DEMO_DATA"] = "false"
        import app as app_module  # noqa: E402

        cls.app_module = app_module
        cls._original_db = app_module.db
        app_module.db = Database(db_path=db_path, skip_price_backfill=True)
        app_module.db.insert_listings(SEED, "s1")
        app_module.db.log_crawl("s1", len(SEED), len(SEED), "success", "naver")
        cls.client = app_module.app.test_client()

    @classmethod
    def tearDownClass(cls):
        cls.app_module.db = cls._original_db

    def test_bargain_only_counts_as_a_filter(self):
        response = self.client.post(
            "/api/alert-rules",
            json={"client_id": "web1", "bargain_only": True, "min_discount": 0.2, "max_price": 300000},
        )
        self.assertEqual(response.status_code, 200)
        rule = response.get_json()["rule"]
        self.assertEqual((rule["bargain_only"], rule["min_discount"], rule["max_price"]), (1, 0.2, 300000))

    def test_invalid_bargain_params_are_rejected(self):
        for payload in (
            {"client_id": "web2", "bargain_only": True, "min_discount": 0.5},
            {"client_id": "web2", "bargain_only": True, "min_discount": "x"},
            {"client_id": "web2", "bargain_only": True, "max_price": -1},
            {"client_id": "web2", "bargain_only": True, "max_price": "비쌈"},
        ):
            response = self.client.post("/api/alert-rules", json=payload)
            self.assertEqual(response.status_code, 400, payload)

    def test_listings_max_price_filter(self):
        body = self.client.get("/api/listings?max_price=40000").get_json()
        self.assertEqual(sorted(l["article_no"] for l in body["listings"]), ["A3", "A4", "R1"])
        body = self.client.get("/api/map-listings?max_price=30000").get_json()
        self.assertEqual(sorted(l["article_no"] for l in body["listings"]), ["A4", "R1"])
        # 숫자가 아니면 무시(기존 동작 유지)
        self.assertEqual(self.client.get("/api/listings?max_price=abc").get_json()["total"], len(SEED))

    def test_weekly_report_api_returns_text(self):
        response = self.client.get("/api/weekly-report")
        self.assertEqual(response.status_code, 200)
        self.assertTrue(response.content_type.startswith("text/plain"))
        self.assertIn("📊 주간 급매 리포트", response.get_data(as_text=True))


if __name__ == "__main__":
    unittest.main()
