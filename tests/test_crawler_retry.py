"""크롤러 페이지 요청 재시도와 실패 조합 집계."""

import sys
import unittest
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import crawler as crawler_module  # noqa: E402


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


ONE_DISTRICT = {"서울특별시": {"districts": {"강남구": {"code": "111", "lat": 37.5, "lng": 127.0}}}}
TWO_DISTRICTS = {
    "서울특별시": {
        "districts": {
            "강남구": {"code": "111", "lat": 37.5, "lng": 127.0},
            "서초구": {"code": "222", "lat": 37.4, "lng": 127.2},
        }
    }
}
COMBOS_PER_DISTRICT = 9  # 3 매물유형 × 3 거래유형


def ok(articles, more=False):
    return {"ok": True, "data": {"articleList": articles, "isMoreData": more}}


def fail(status):
    return {"ok": False, "status": status}


class CrawlerRetryTest(unittest.TestCase):
    def _crawl(self, responses, regions=ONE_DISTRICT):
        page = _FakePage(responses)
        crawler = crawler_module.NaverRealEstateCrawler(db=mock.Mock())
        with mock.patch.object(crawler_module, "PLAYWRIGHT_AVAILABLE", True), \
                mock.patch.object(crawler_module, "sync_playwright", create=True,
                                  return_value=_FakePlaywright(page)), \
                mock.patch.object(crawler_module.time, "sleep") as sleep, \
                mock.patch.object(type(crawler), "REGIONS", regions):
            listings = crawler._crawl_with_playwright()
        delays = [call.args[0] for call in sleep.call_args_list if call.args[0] != 0.1]
        return crawler, listings, delays, page

    def test_429_then_success_retries_with_rate_limit_delay(self):
        crawler, listings, delays, _ = self._crawl(
            {("111", "OBYG", "A1", 1): [fail(429), ok([_article(1)])]}
        )
        self.assertEqual([l["article_no"] for l in listings], ["1"])
        self.assertEqual(delays, [30])
        self.assertEqual(crawler.last_crawl_stats, {"failed_combos": 0, "total_combos": COMBOS_PER_DISTRICT})

    def test_5xx_exhausts_retries_and_marks_combo_failed(self):
        crawler, listings, delays, page = self._crawl(
            {
                ("111", "OBYG", "A1", 1): ok([_article(1)], more=True),
                ("111", "OBYG", "A1", 2): [fail(503)] * 4,
            }
        )
        self.assertEqual([l["article_no"] for l in listings], ["1"])
        self.assertEqual(delays, [2, 5, 15])
        self.assertEqual(crawler.last_crawl_stats["failed_combos"], 1)
        page2_calls = [c for c in page.evaluate_calls if c["cortarNo"] == "111" and c["ptype"] == "OBYG"
                       and c["ttype"] == "A1" and c["page"] == 2]
        self.assertEqual(len(page2_calls), 4)

    def test_401_is_not_retried(self):
        crawler, listings, delays, _ = self._crawl({("111", "OBYG", "A1", 1): fail(401)})
        self.assertEqual(listings, [])
        self.assertEqual(delays, [])
        self.assertEqual(crawler.last_crawl_stats["failed_combos"], 1)

    def test_evaluate_exception_counts_as_failure(self):
        crawler, _, delays, _ = self._crawl(
            {("111", "OBYG", "A1", 1): [RuntimeError("closed")] * 4}
        )
        self.assertEqual(delays, [2, 5, 15])
        self.assertEqual(crawler.last_crawl_stats["failed_combos"], 1)

    def test_ok_with_empty_article_list_is_normal_end(self):
        crawler, listings, delays, _ = self._crawl({("111", "OBYG", "A1", 1): ok([], more=True)})
        self.assertEqual(listings, [])
        self.assertEqual(delays, [])
        self.assertEqual(crawler.last_crawl_stats["failed_combos"], 0)

    def test_token_expiry_mid_crawl(self):
        responses = {("111", "OBYG", "A1", 1): ok([_article(1)])}
        for code in ("111", "222"):
            for ptype in ("OBYG", "SGJT", "TJ"):
                for ttype in ("A1", "B1", "B2"):
                    responses.setdefault((code, ptype, ttype, 1), fail(401))
        crawler, listings, delays, _ = self._crawl(responses, regions=TWO_DISTRICTS)
        total = 2 * COMBOS_PER_DISTRICT
        self.assertEqual(crawler.last_crawl_stats, {"failed_combos": total - 1, "total_combos": total})
        self.assertEqual(delays, [])
        self.assertEqual(len(listings), 1)

    def test_aborts_when_failed_combos_exceed_ratio_of_planned(self):
        responses = {
            (code, ptype, ttype, 1): fail(401)
            for code in ("111", "222") for ptype in ("OBYG", "SGJT", "TJ") for ttype in ("A1", "B1", "B2")
        }
        crawler, _, _, page = self._crawl(responses, regions=TWO_DISTRICTS)
        total = 2 * COMBOS_PER_DISTRICT
        # 18 × 0.2 = 3.6 → 4번째 실패에서 중단, 나머지는 시도하지 않음
        self.assertEqual(len(page.evaluate_calls), 4)
        self.assertEqual(crawler.last_crawl_stats, {"failed_combos": total, "total_combos": total})

    def test_unattempted_combos_count_as_failed_when_crawl_crashes(self):
        real_info = crawler_module.logger.info

        def crash_on_district_log(msg, *args, **kwargs):
            if str(msg).startswith("[1/"):
                raise RuntimeError("browser crashed")
            return real_info(msg, *args, **kwargs)

        with mock.patch.object(crawler_module.logger, "info", side_effect=crash_on_district_log):
            crawler, listings, _, _ = self._crawl(
                {("111", "OBYG", "A1", 1): ok([_article(1)])}, regions=TWO_DISTRICTS
            )
        total = 2 * COMBOS_PER_DISTRICT
        self.assertEqual(len(listings), 1)
        self.assertEqual(
            crawler.last_crawl_stats,
            {"failed_combos": COMBOS_PER_DISTRICT, "total_combos": total},
        )

    def test_token_bootstrap_failure_reports_zero_combos(self):
        crawler = crawler_module.NaverRealEstateCrawler(db=mock.Mock())
        page = _FakePage({})
        page.goto = lambda url, **_: None  # 토큰 요청이 발생하지 않음
        with mock.patch.object(crawler_module, "PLAYWRIGHT_AVAILABLE", True), \
                mock.patch.object(crawler_module, "sync_playwright", create=True,
                                  return_value=_FakePlaywright(page)), \
                mock.patch.object(type(crawler), "TOKEN_BOOTSTRAP_URLS", ["https://x"]):
            self.assertEqual(crawler._crawl_with_playwright(), [])
        self.assertEqual(crawler.last_crawl_stats, {"failed_combos": 0, "total_combos": 0})


class CrawlAllFailedRatioTest(unittest.TestCase):
    def _crawl_all(self, failed, total):
        db = mock.Mock()
        db.get_last_successful_live_crawl.return_value = None
        crawler = crawler_module.NaverRealEstateCrawler(db)
        listings = [{
            "article_no": "9", "region": "서울특별시", "district": "강남구",
            "property_type": "상가", "raw_property_code": "OBYG", "trade_type": "매매",
            "price": "5억", "area": "84", "floor": "3/10", "building_name": "건물9",
            "description": "", "is_urgent": True, "tags": ["급매"], "confirmed_date": "",
            "latitude": 1, "longitude": 2, "naver_url": "",
        }]

        def fake_crawl():
            crawler.last_crawl_stats = {"failed_combos": failed, "total_combos": total}
            return listings

        with mock.patch.object(crawler, "_crawl_with_playwright", side_effect=fake_crawl):
            return db, crawler.crawl_all()

    def test_crawl_all_fails_when_failed_ratio_exceeds_20_percent(self):
        db, result = self._crawl_all(3, 10)
        self.assertEqual(result["status"], "failed")
        self.assertEqual((result["failed_combos"], result["total_combos"]), (3, 10))
        db.insert_listings.assert_not_called()
        self.assertEqual(db.log_crawl.call_args.args[3:], ("failed", "naver"))

    def test_crawl_all_succeeds_at_exactly_20_percent(self):
        db, result = self._crawl_all(2, 10)
        self.assertEqual(result["status"], "success")
        self.assertEqual((result["failed_combos"], result["total_combos"]), (2, 10))
        db.insert_listings.assert_called_once()

    def test_crawl_all_empty_result_still_reports_combo_counts(self):
        db = mock.Mock()
        crawler = crawler_module.NaverRealEstateCrawler(db)

        def fake_crawl():
            crawler.last_crawl_stats = {"failed_combos": 9, "total_combos": 9}
            return []

        with mock.patch.object(crawler, "_crawl_with_playwright", side_effect=fake_crawl), \
                mock.patch.dict("os.environ", {"ALLOW_DEMO_FALLBACK": "false"}):
            result = crawler.crawl_all()
        self.assertEqual(result["status"], "failed")
        self.assertEqual((result["failed_combos"], result["total_combos"]), (9, 9))


if __name__ == "__main__":
    unittest.main()
