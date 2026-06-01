import requests
import json
import time
import random
import uuid
import logging
import os
import re
import threading
from pathlib import Path
from datetime import datetime, timedelta
from typing import List, Dict, Optional

try:
    from playwright.sync_api import TimeoutError as PlaywrightTimeoutError, sync_playwright
    PLAYWRIGHT_AVAILABLE = True
except ImportError:
    PLAYWRIGHT_AVAILABLE = False
    PlaywrightTimeoutError = Exception

logger = logging.getLogger(__name__)

STEALTH_INIT_SCRIPT = """
Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
window.chrome = window.chrome || { runtime: {} };
Object.defineProperty(navigator, 'plugins', { get: () => [1, 2, 3, 4, 5] });
Object.defineProperty(navigator, 'languages', { get: () => ['ko-KR', 'ko', 'en-US', 'en'] });
"""


# ── JavaScript: 단일 페이지 fetch ─────────────────────────────────────────
JS_FETCH_PAGE = """
async (args) => {
    const controller = new AbortController();
    const timeoutId = setTimeout(() => controller.abort(), args.timeoutMs || 8000);
    try {
        const r = await fetch("/api/articles?cortarNo=" + args.cortarNo + "&realEstateType=" + args.ptype + "&tradeType=" + args.ttype + "&tag=" + encodeURIComponent(args.tag) + "&priceType=RETAIL&sameAddressGroup=true&page=" + args.page + "&perPage=20&sortBy=RECENT&showHidden=false", {
            headers: {"Authorization": args.token, "Accept": "application/json"},
            signal: controller.signal
        });
        if (!r.ok) return null;
        return await r.json();
    } catch (e) {
        return null;
    } finally {
        clearTimeout(timeoutId);
    }
}
"""


class NaverRealEstateCrawler:
    REGIONS_FILE = Path(__file__).resolve().parent / "data" / "regions.json"
    API_BASE = "https://new.land.naver.com/api"
    FASTSELL_TAG = ":::::::::FASTSELL"
    FASTSELL_URL_TAG = "FASTSELL"

    PROPERTY_TYPE_MAP = {
        "OBYG": "상가",
        "SGJT": "업무",
        "TJ": "토지",
    }

    TRADE_TYPE_MAP = {
        "A1": "매매",
        "B1": "전세",
        "B2": "월세",
    }

    REGIONS = {
        "서울특별시": {
            "code": "1100000000",
            "lat": 37.5665,
            "lng": 126.9780,
            "districts": {
                "강남구": {"code": "1168000000", "lat": 37.5172, "lng": 127.0473},
                "강동구": {"code": "1174000000", "lat": 37.5300, "lng": 127.1237},
                "강북구": {"code": "1130500000", "lat": 37.6395, "lng": 127.0255},
                "서초구": {"code": "1165000000", "lat": 37.4837, "lng": 127.0324},
                "송파구": {"code": "1171000000", "lat": 37.5145, "lng": 127.1059},
                "마포구": {"code": "1144000000", "lat": 37.5638, "lng": 126.9084},
                "용산구": {"code": "1117000000", "lat": 37.5326, "lng": 126.9905},
                "종로구": {"code": "1111000000", "lat": 37.5807, "lng": 126.9828},
                "중구": {"code": "1114000000", "lat": 37.5637, "lng": 126.9975},
                "광진구": {"code": "1121500000", "lat": 37.5363, "lng": 127.0880},
                "성동구": {"code": "1120000000", "lat": 37.5634, "lng": 127.0369},
                "동대문구": {"code": "1123000000", "lat": 37.5742, "lng": 127.0395},
                "중랑구": {"code": "1126000000", "lat": 37.6063, "lng": 127.0930},
                "성북구": {"code": "1129000000", "lat": 37.5900, "lng": 127.0165},
                "도봉구": {"code": "1132000000", "lat": 37.6686, "lng": 127.0466},
                "노원구": {"code": "1135000000", "lat": 37.6543, "lng": 127.0568},
                "은평구": {"code": "1138000000", "lat": 37.6176, "lng": 126.9227},
                "서대문구": {"code": "1141000000", "lat": 37.5791, "lng": 126.9368},
                "양천구": {"code": "1147000000", "lat": 37.5171, "lng": 126.8663},
                "강서구": {"code": "1150000000", "lat": 37.5509, "lng": 126.8495},
                "구로구": {"code": "1153000000", "lat": 37.4952, "lng": 126.8877},
                "금천구": {"code": "1154500000", "lat": 37.4565, "lng": 126.8954},
                "관악구": {"code": "1162000000", "lat": 37.4784, "lng": 126.9516},
                "동작구": {"code": "1159000000", "lat": 37.5124, "lng": 126.9395},
                "영등포구": {"code": "1156000000", "lat": 37.5264, "lng": 126.8963},
            },
        },
        "경기도": {
            "code": "4100000000",
            "lat": 37.2750,
            "lng": 127.0094,
            "districts": {
                "광명시": {"code": "4121000000", "lat": 37.4785, "lng": 126.8644},
                "광주시": {"code": "4161000000", "lat": 37.4291, "lng": 127.2552},
                "과천시": {"code": "4129000000", "lat": 37.4289, "lng": 126.9882},
                "구리시": {"code": "4131000000", "lat": 37.5936, "lng": 127.1298},
                "군포시": {"code": "4141000000", "lat": 37.3615, "lng": 126.9349},
                "김포시": {"code": "4157000000", "lat": 37.6156, "lng": 126.7158},
                "성남시": {"code": "4113500000", "lat": 37.4386, "lng": 127.1378},
                "수원시": {"code": "4111500000", "lat": 37.2636, "lng": 127.0286},
                "시흥시": {"code": "4139000000", "lat": 37.3799, "lng": 126.8032},
                "안산시 상록구": {"code": "4127100000", "lat": 37.3154, "lng": 126.8597},
                "용인시": {"code": "4117300000", "lat": 37.2411, "lng": 127.1776},
                "안양시": {"code": "4113700000", "lat": 37.3943, "lng": 126.9568},
                "안산시 단원구": {"code": "4127300000", "lat": 37.3217, "lng": 126.8309},
                "안성시": {"code": "4155000000", "lat": 37.0078, "lng": 127.2800},
                "양주시": {"code": "4163000000", "lat": 37.7849, "lng": 127.0458},
                "여주시": {"code": "4167000000", "lat": 37.2983, "lng": 127.6370},
                "오산시": {"code": "4137000000", "lat": 37.1499, "lng": 127.0775},
                "의왕시": {"code": "4143000000", "lat": 37.3449, "lng": 126.9690},
                "이천시": {"code": "4150000000", "lat": 37.2809, "lng": 127.4429},
                "화성시": {"code": "4159000000", "lat": 37.1996, "lng": 126.8314},
                "고양시": {"code": "4128500000", "lat": 37.6584, "lng": 126.8320},
                "부천시": {"code": "4119500000", "lat": 37.5035, "lng": 126.7660},
                "파주시": {"code": "4148000000", "lat": 37.7599, "lng": 126.7802},
                "평택시": {"code": "4122000000", "lat": 36.9925, "lng": 127.1127},
                "포천시": {"code": "4165000000", "lat": 37.8948, "lng": 127.2007},
                "하남시": {"code": "4145000000", "lat": 37.5393, "lng": 127.2149},
                "남양주시": {"code": "4136000000", "lat": 37.6360, "lng": 127.2165},
                "의정부시": {"code": "4115000000", "lat": 37.7381, "lng": 127.0338},
                "동두천시": {"code": "4125000000", "lat": 37.9031, "lng": 127.0605},
            },
        },
        "인천광역시": {
            "code": "2800000000",
            "lat": 37.4563,
            "lng": 126.7052,
            "districts": {
                "연수구": {"code": "2817700000", "lat": 37.4103, "lng": 126.6784},
                "남동구": {"code": "2818500000", "lat": 37.4468, "lng": 126.7314},
                "부평구": {"code": "2823700000", "lat": 37.4927, "lng": 126.7228},
                "서구": {"code": "2826000000", "lat": 37.5452, "lng": 126.6762},
            },
        },
        "부산광역시": {
            "code": "2600000000",
            "lat": 35.1796,
            "lng": 129.0756,
            "districts": {
                "해운대구": {"code": "2635000000", "lat": 35.1631, "lng": 129.1636},
                "수영구": {"code": "2638000000", "lat": 35.1453, "lng": 129.1130},
                "남구": {"code": "2623000000", "lat": 35.1367, "lng": 129.0843},
                "부산진구": {"code": "2614000000", "lat": 35.1629, "lng": 129.0530},
                "동래구": {"code": "2626000000", "lat": 35.1983, "lng": 129.0860},
            },
        },
        "대구광역시": {
            "code": "2700000000",
            "lat": 35.8714,
            "lng": 128.6014,
            "districts": {
                "수성구": {"code": "2720000000", "lat": 35.8579, "lng": 128.6298},
                "달서구": {"code": "2729000000", "lat": 35.8298, "lng": 128.5326},
                "중구": {"code": "2711000000", "lat": 35.8694, "lng": 128.6063},
            },
        },
        "대전광역시": {
            "code": "3000000000",
            "lat": 36.3504,
            "lng": 127.3845,
            "districts": {
                "서구": {"code": "3017000000", "lat": 36.3553, "lng": 127.3834},
                "유성구": {"code": "3020000000", "lat": 36.3625, "lng": 127.3565},
                "중구": {"code": "3011000000", "lat": 36.3253, "lng": 127.4214},
            },
        },
        "광주광역시": {
            "code": "2900000000",
            "lat": 35.1595,
            "lng": 126.8526,
            "districts": {
                "서구": {"code": "2914000000", "lat": 35.1525, "lng": 126.8898},
                "남구": {"code": "2915500000", "lat": 35.1326, "lng": 126.9023},
                "북구": {"code": "2917000000", "lat": 35.1847, "lng": 126.9121},
            },
        },
        "울산광역시": {
            "code": "3100000000",
            "lat": 35.5384,
            "lng": 129.3114,
            "districts": {
                "남구": {"code": "3114000000", "lat": 35.5364, "lng": 129.3343},
                "북구": {"code": "3117000000", "lat": 35.5824, "lng": 129.3612},
            },
        },
        "세종특별자치시": {
            "code": "3600000000",
            "lat": 36.4800,
            "lng": 127.2890,
            "districts": {
                "세종특별자치시": {"code": "3600000000", "lat": 36.4800, "lng": 127.2890},
            },
        },
        "강원특별자치도": {
            "code": "4200000000",
            "lat": 37.8228,
            "lng": 128.1555,
            "districts": {
                "강원특별자치도": {"code": "4200000000", "lat": 37.8228, "lng": 128.1555},
            },
        },
        "충청북도": {
            "code": "4300000000",
            "lat": 36.6357,
            "lng": 127.4917,
            "districts": {
                "충청북도": {"code": "4300000000", "lat": 36.6357, "lng": 127.4917},
            },
        },
        "충청남도": {
            "code": "4400000000",
            "lat": 36.5184,
            "lng": 126.8000,
            "districts": {
                "충청남도": {"code": "4400000000", "lat": 36.5184, "lng": 126.8000},
            },
        },
        "전북특별자치도": {
            "code": "5200000000",
            "lat": 35.7175,
            "lng": 127.1530,
            "districts": {
                "전북특별자치도": {"code": "5200000000", "lat": 35.7175, "lng": 127.1530},
            },
        },
        "전라남도": {
            "code": "4600000000",
            "lat": 34.8161,
            "lng": 126.4630,
            "districts": {
                "전라남도": {"code": "4600000000", "lat": 34.8161, "lng": 126.4630},
            },
        },
        "경상북도": {
            "code": "4700000000",
            "lat": 36.5760,
            "lng": 128.5056,
            "districts": {
                "경상북도": {"code": "4700000000", "lat": 36.5760, "lng": 128.5056},
            },
        },
        "경상남도": {
            "code": "4800000000",
            "lat": 35.4606,
            "lng": 128.2132,
            "districts": {
                "경상남도": {"code": "4800000000", "lat": 35.4606, "lng": 128.2132},
            },
        },
        "제주특별자치도": {
            "code": "5000000000",
            "lat": 33.4996,
            "lng": 126.5312,
            "districts": {
                "제주특별자치도": {"code": "5000000000", "lat": 33.4996, "lng": 126.5312},
            },
        },
    }

    if REGIONS_FILE.exists():
        REGIONS = json.loads(REGIONS_FILE.read_text(encoding="utf-8"))

    URGENT_KEYWORDS = ["급매", "급처", "급급매", "급처분", "긴급매물", "시세이하", "손해보고", "급하게"]
    MIN_LIVE_CRAWL_RATIO = float((os.getenv("MIN_LIVE_CRAWL_RATIO") or "0.5").strip())
    TOKEN_BOOTSTRAP_URLS = [
        "https://new.land.naver.com/complexes/867?ms=37.5209277,126.9710095,17&a=APT:PRE:ABYG:JGC&e=RETAIL&y=FASTSELL&ad=true",
        "https://new.land.naver.com/",
        "https://new.land.naver.com/complexes",
    ]
    BROWSER_USER_AGENT = (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    )

    def __init__(self, db):
        self.db = db
        self._auth_token = None
        self._token_lock = threading.Lock()
        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
                "Accept": "application/json, text/plain, */*",
                "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
                "Accept-Encoding": "gzip, deflate, br",
                "Referer": "https://new.land.naver.com/",
                "sec-ch-ua": '"Chromium";v="122", "Not(A:Brand";v="24", "Google Chrome";v="122"',
                "sec-ch-ua-mobile": "?0",
                "sec-ch-ua-platform": '"macOS"',
            }
        )

    def _is_urgent(self, tags: list, description: str) -> bool:
        desc_lower = (description or "").lower()
        for tag in tags:
            if "급" in str(tag):
                return True
        for kw in self.URGENT_KEYWORDS:
            if kw in desc_lower:
                return True
        return False

    def _format_article_price(self, article: Dict) -> str:
        base_price = article.get("dealOrWarrantPrc") or ""
        rent_price = article.get("rentPrc")
        trade_type = article.get("tradeTypeCode")

        if trade_type == "B2" and rent_price not in (None, ""):
            return f"{base_price}/{rent_price}"

        return base_price

    def _is_price_down_article(self, article: Dict) -> bool:
        state = str(article.get("priceChangeState") or "").upper()
        if state in {"DOWN", "DECREASE"}:
            return True
        return bool(article.get("isPriceModification"))

    def _first_article_value(self, article: Dict, keys: List[str]):
        for key in keys:
            value = article.get(key)
            if value not in (None, ""):
                return value
        return None

    def _build_naver_listing_url(
        self,
        article_no,
        property_code,
        trade_code,
        lat,
        lng,
    ) -> Optional[str]:
        if not article_no:
            return None

        params = [
            f"ms={lat},{lng},16" if lat not in (None, "") and lng not in (None, "") else "",
            f"a={property_code}" if property_code else "",
            f"b={trade_code}" if trade_code else "",
            "e=RETAIL",
            f"y={self.FASTSELL_URL_TAG}",
            "ad=true",
            f"articleNo={article_no}",
        ]
        query = "&".join(param for param in params if param)
        return f"https://new.land.naver.com/search?{query}"

    def _parse_float_value(self, value) -> Optional[float]:
        if value in (None, ""):
            return None
        try:
            return float(str(value).replace("%", "").replace(",", "").strip())
        except (TypeError, ValueError):
            return None

    def _parse_area_m2(self, *values) -> Optional[float]:
        for value in values:
            if value in (None, ""):
                continue
            text = str(value).replace(",", "").strip()
            match = re.search(r"(\d+(?:\.\d+)?)", text)
            if not match:
                continue
            area = float(match.group(1))
            if "평" in text and "㎡" not in text and "m2" not in text.lower():
                area *= 3.305785
            return round(area, 2)
        return None

    def _parse_money_to_manwon(self, raw: Optional[str]) -> Optional[int]:
        text = re.sub(r"\s+", "", str(raw or ""))
        text = text.replace(",", "")
        if not text or not re.search(r"\d", text):
            return None
        if "억" in text:
            eok_part, rest = text.split("억", 1)
            eok_digits = re.sub(r"[^\d]", "", eok_part)
            total = (int(eok_digits) if eok_digits else 0) * 10000
            rest_digits = re.sub(r"[^\d]", "", rest)
            return total + (int(rest_digits) if rest_digits else 0)
        digits = re.sub(r"[^\d]", "", text)
        return int(digits) if digits else None

    def _estimate_yield_rate(self, formatted_price: str) -> Optional[float]:
        if "/" not in str(formatted_price or ""):
            return None
        deposit_raw, monthly_raw = formatted_price.split("/", 1)
        deposit = self._parse_money_to_manwon(deposit_raw)
        monthly = self._parse_money_to_manwon(monthly_raw)
        if not deposit or not monthly:
            return None
        return round((monthly * 12 / deposit) * 100, 2)

    def _extract_premium_info(self, article: Dict, tags: List[str], desc: str) -> Optional[str]:
        value = self._first_article_value(
            article,
            ["premiumPrc", "premiumPrice", "rightPrc", "rightPrice", "premiumInfo"],
        )
        if value not in (None, ""):
            return str(value)
        text = " ".join([*(tags or []), desc or ""])
        if any(token in text for token in ("무권리", "권리금 없음", "권리금없음")):
            return "무권리"
        if "권리금" in text:
            return "권리금 확인"
        return None

    def _extract_road_access(self, article: Dict, tags: List[str], desc: str) -> Optional[str]:
        value = self._first_article_value(
            article,
            ["roadAccess", "roadAccessName", "roadCondition", "contactRoad"],
        )
        if value not in (None, ""):
            return str(value)
        text = " ".join([*(tags or []), desc or ""])
        if any(token in text for token in ("코너", "양면", "삼면")):
            return "코너/다중접면"
        if any(token in text for token in ("대로변", "대로", "왕복", "광로")):
            return "대로변"
        if any(token in text for token in ("도로접", "접도", "도로 접", "진입로")):
            return "도로접면"
        if "맹지" in text:
            return "맹지 유의"
        return None

    def _extract_price_drop_rate(self, article: Dict) -> Optional[float]:
        return self._parse_float_value(
            self._first_article_value(
                article,
                [
                    "priceChangeRate",
                    "priceChangePercent",
                    "priceChangeRateValue",
                    "priceDownRate",
                    "dealPriceChangeRate",
                ],
            )
        )

    def _fetch_combo_all_pages(self, page, token, cortarNo, ptype, ttype, max_pages=100):
        """한 조합(지역+유형+거래)의 전 페이지를 순회, FASTSELL 태그 매물만 반환."""
        all_articles = []

        for pg in range(1, max_pages + 1):
            try:
                data = page.evaluate(JS_FETCH_PAGE, {
                    "cortarNo": cortarNo,
                    "token": token,
                    "ptype": ptype,
                    "ttype": ttype,
                    "page": pg,
                    "tag": self.FASTSELL_TAG,
                    "timeoutMs": 8000,
                })
            except Exception:
                break

            if not data or not data.get("articleList"):
                break

            for art in data["articleList"]:
                tags = art.get("tagList") or []
                desc = art.get("articleFeatureDesc") or ""
                price_down = self._is_price_down_article(art)
                formatted_price = self._format_article_price(art)
                land_use_zone = self._first_article_value(
                    art,
                    [
                        "landUseZoneName",
                        "landUseRegionName",
                        "useRegionName",
                        "useZoneName",
                        "usageArea",
                    ],
                )
                land_category = self._first_article_value(
                    art,
                    ["landCategoryName", "landCategory", "jimokName", "landTypeName"],
                )

                if price_down and "가격인하" not in tags:
                    tags = list(tags) + ["가격인하"]

                all_articles.append({
                    "articleNo": str(art.get("articleNo")),
                    "articleName": art.get("articleName") or "",
                    "tagList": tags,
                    "desc": desc,
                    "price": formatted_price,
                    "area": art.get("areaName") or "",
                    "floor": art.get("floorInfo") or "",
                    "date": art.get("articleConfirmYmd") or "",
                    "lat": art.get("latitude"),
                    "lng": art.get("longitude"),
                    "priceDown": price_down,
                    "areaM2": self._parse_area_m2(
                        art.get("areaName"),
                        art.get("area1"),
                        art.get("area2"),
                        art.get("exclusiveArea"),
                        art.get("landArea"),
                    ),
                    "landUseZone": str(land_use_zone) if land_use_zone not in (None, "") else None,
                    "landCategory": str(land_category) if land_category not in (None, "") else None,
                    "roadAccess": self._extract_road_access(art, tags, desc),
                    "premiumInfo": self._extract_premium_info(art, tags, desc),
                    "estimatedYieldRate": self._estimate_yield_rate(formatted_price),
                    "priceDropRate": self._extract_price_drop_rate(art),
                })

            if not data.get("isMoreData"):
                break

            time.sleep(0.1)

        return all_articles

    def _crawl_with_playwright(self) -> List[Dict]:
        """
        Playwright 기반 급매 전용 크롤링.
        각 지역×유형×거래 조합별로 전 페이지를 5페이지 배치로 순회하며
        급매/가격인하 매물만 수집.
        """
        if not PLAYWRIGHT_AVAILABLE:
            return []

        listings = []
        seen_articles = set()
        prop_codes = ["OBYG", "SGJT", "TJ"]
        trade_codes = ["A1", "B1", "B2"]

        try:
            logger.info("Playwright 급매 전용 크롤링 시작...")
            with sync_playwright() as p:
                browser = p.chromium.launch(
                    headless=True,
                    args=[
                        "--disable-blink-features=AutomationControlled",
                        "--disable-dev-shm-usage",
                        "--disable-gpu",
                    ],
                )
                context = browser.new_context(
                    user_agent=self.BROWSER_USER_AGENT,
                    viewport={"width": 1365, "height": 900},
                    locale="ko-KR",
                    extra_http_headers={
                        "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
                    },
                )
                page = context.new_page()
                page.set_default_timeout(60000)
                page.add_init_script(STEALTH_INIT_SCRIPT)

                # ── 토큰 포착 ──────────────────────────────
                auth_token = [None]

                def on_request(req):
                    if "new.land.naver.com/api/" in req.url:
                        h = req.headers.get("authorization")
                        if h:
                            auth_token[0] = h

                page.on("request", on_request)

                for bootstrap_url in self.TOKEN_BOOTSTRAP_URLS:
                    try:
                        logger.info("토큰 부트스트랩 페이지 진입: %s", bootstrap_url)
                        page.goto(
                            bootstrap_url,
                            wait_until="domcontentloaded",
                            timeout=45000,
                        )
                    except PlaywrightTimeoutError:
                        logger.warning("부트스트랩 페이지 로드 타임아웃: %s", bootstrap_url)
                    except Exception as e:
                        logger.warning("부트스트랩 페이지 진입 실패 (%s): %s", bootstrap_url, e)

                    for _ in range(8):
                        if auth_token[0]:
                            break
                        page.wait_for_timeout(1000)

                    if auth_token[0]:
                        break

                if not auth_token[0]:
                    logger.warning("Authorization 토큰 획득 실패")
                    browser.close()
                    return []

                token = auth_token[0]
                logger.info(f"토큰 획득 성공 ({token[:40]}...)")

                # ── 지역별 × 유형별 급매 크롤링 ────────────
                total_districts = sum(len(r["districts"]) for r in self.REGIONS.values())
                done = 0

                for region_name, region_info in self.REGIONS.items():
                    for district_name, d_info in region_info["districts"].items():
                        done += 1
                        district_count = 0

                        for ptype_code in prop_codes:
                            for ttype_code in trade_codes:
                                try:
                                    articles = self._fetch_combo_all_pages(
                                        page, token,
                                        d_info["code"], ptype_code, ttype_code,
                                        max_pages=20,
                                    )

                                    for art in articles:
                                        ano = art["articleNo"]
                                        if ano in seen_articles:
                                            continue
                                        seen_articles.add(ano)

                                        tags = art.get("tagList", [])
                                        desc = art.get("desc", "")

                                        if art.get("priceDown") and "가격인하" not in tags:
                                            tags = list(tags) + ["가격인하"]

                                        art_lat = art.get("lat") or d_info["lat"]
                                        art_lng = art.get("lng") or d_info["lng"]
                                        naver_url = self._build_naver_listing_url(
                                            ano,
                                            ptype_code,
                                            ttype_code,
                                            art_lat,
                                            art_lng,
                                        )

                                        listings.append({
                                            "article_no": ano,
                                            "region": region_name,
                                            "district": district_name,
                                            "property_type": self.PROPERTY_TYPE_MAP.get(ptype_code, ptype_code),
                                            "raw_property_code": ptype_code,
                                            "trade_type": self.TRADE_TYPE_MAP.get(ttype_code, ttype_code),
                                            "price": art.get("price", ""),
                                            "area": art.get("area", ""),
                                            "floor": art.get("floor", ""),
                                            "building_name": art.get("articleName", ""),
                                            "description": desc,
                                            "is_urgent": True,
                                            "tags": tags,
                                            "confirmed_date": art.get("date", ""),
                                            "latitude": art_lat,
                                            "longitude": art_lng,
                                            "naver_url": naver_url,
                                            "area_m2": art.get("areaM2"),
                                            "land_use_zone": art.get("landUseZone"),
                                            "land_category": art.get("landCategory"),
                                            "road_access": art.get("roadAccess"),
                                            "premium_info": art.get("premiumInfo"),
                                            "estimated_yield_rate": art.get("estimatedYieldRate"),
                                            "price_drop_rate": art.get("priceDropRate"),
                                        })
                                        district_count += 1

                                except Exception as e:
                                    logger.debug(f"API 오류 ({district_name}/{ptype_code}/{ttype_code}): {e}")

                        logger.info(
                            f"[{done}/{total_districts}] {region_name} {district_name}: "
                            f"{district_count}개 급매"
                        )

                browser.close()
        except Exception as e:
            logger.error(f"Playwright 크롤링 실패: {e}")

        logger.info(f"급매 크롤링 완료: {len(listings)}개 급매 수집")
        return listings

    def generate_demo_data(self) -> List[Dict]:
        """급매 전용 데모 데이터 생성."""
        rng = random.Random(42)

        property_types = ["상가", "업무", "토지"]
        raw_codes_by_type = {"상가": "OBYG", "업무": "SGJT", "토지": "TJ"}
        trade_types = ["매매", "전세", "월세"]

        price_cfg = {
            "상가": {"매매": (3, 60), "전세": (2, 30), "월세": (100, 1500)},
            "업무": {"매매": (4, 80), "전세": (3, 50), "월세": (120, 1800)},
            "토지": {"매매": (5, 200), "전세": (0.5, 2), "월세": (30, 300)},
        }

        areas_by_type = {
            "상가": ["33㎡", "49㎡", "66㎡", "99㎡", "132㎡"],
            "업무": ["49㎡", "66㎡", "99㎡", "132㎡", "165㎡"],
            "토지": ["330㎡", "495㎡", "660㎡", "990㎡", "1320㎡"],
        }

        shop_sfx = ["상가", "근린상가", "로드샵", "주상복합상가"]
        office_sfx = ["오피스", "업무타워", "지식산업센터", "오피스동"]
        land_sfx = ["토지", "대지", "잡종지", "계획관리"]

        urgent_descs = [
            "급매!! 시세보다 저렴하게 내놓습니다. 빠른 협의 가능.",
            "급처분 — 급한 사정으로 시세 이하 판매합니다.",
            "이사 일정으로 인한 급매물입니다. 즉시 입주 가능.",
            "투자 목적 급처, 협의 가능합니다.",
            "급매! 손해보고 파는 매물입니다. 연락 주세요.",
            "급처 — 내놓은 지 3일 이내 계약 원합니다.",
            "가격 내렸습니다. 급하게 처분합니다.",
            "시세 대비 급매가, 즉시 입주 가능한 급매물.",
        ]

        all_tags_pool = ["역세권", "신축", "대단지", "주차가능", "남향", "학세권", "숲세권"]
        commercial_tags = ["무권리", "코너", "대로변", "권리금 확인"]
        land_zones = ["계획관리", "자연녹지", "준주거", "일반상업", "제2종일반주거"]
        land_categories = ["대지", "잡종지", "전", "답", "임야"]
        urgent_tag_types = ["급매", "가격인하"]

        def make_building_name(rng, ptype, short):
            if ptype == "상가":
                sfx = rng.choice(shop_sfx)
                n = rng.randint(1, 8)
                return f"{short} {n}번가 {sfx}"
            if ptype == "업무":
                sfx = rng.choice(office_sfx)
                n = rng.randint(1, 6)
                return f"{short} {n}지구 {sfx}"
            sfx = rng.choice(land_sfx)
            n = rng.randint(1, 999)
            return f"{short} {n}번지 {sfx}"

        listings = []
        article_id = 1_000_000

        for region_name, region_info in self.REGIONS.items():
            for district_name, d_info in region_info["districts"].items():
                n = rng.randint(8, 25)
                for _ in range(n):
                    ptype = rng.choice(property_types)
                    ttype = rng.choice(trade_types)

                    lo, hi = price_cfg[ptype][ttype]
                    if ttype == "월세":
                        deposit = rng.randint(500, 5000)
                        monthly = rng.randint(int(lo), int(hi))
                        price = f"{deposit:,}만/{monthly}만"
                    else:
                        val = rng.uniform(lo, hi)
                        bil = int(val)
                        chun = int((val - bil) * 10) * 1000
                        price = f"{bil}억" + (f" {chun:,}만" if chun else "")

                    max_floor = {"상가": 20, "업무": 30, "토지": 1}.get(ptype, 10)
                    floor_n = rng.randint(1, max_floor)
                    total_f = rng.randint(floor_n, max_floor)

                    urgent_tag = rng.choice(urgent_tag_types)
                    tags = [urgent_tag]
                    for t in rng.sample(all_tags_pool, k=rng.randint(0, 3)):
                        tags.append(t)
                    if ptype in {"상가", "업무"} and rng.random() < 0.5:
                        tags.append(rng.choice(commercial_tags))
                    if ptype == "토지":
                        tags.extend([rng.choice(land_zones), rng.choice(land_categories)])

                    desc = rng.choice(urgent_descs)
                    short = district_name.replace("구", "").replace("시", "")
                    name = make_building_name(rng, ptype, short)
                    area_pool = areas_by_type.get(ptype, ["59㎡", "84㎡"])
                    area = rng.choice(area_pool)

                    lat = d_info["lat"] + rng.uniform(-0.025, 0.025)
                    lng = d_info["lng"] + rng.uniform(-0.025, 0.025)

                    article_id += 1
                    days_ago = rng.randint(0, 45)
                    conf_date = (datetime.now() - timedelta(days=days_ago)).strftime("%Y%m%d")

                    listings.append(
                        {
                            "article_no": str(article_id),
                            "region": region_name,
                            "district": district_name,
                            "property_type": ptype,
                            "raw_property_code": raw_codes_by_type.get(ptype),
                            "trade_type": ttype,
                            "price": price,
                            "area": area,
                            "floor": f"{floor_n}/{total_f}",
                            "building_name": name,
                            "description": desc,
                            "is_urgent": True,
                            "tags": tags,
                            "confirmed_date": conf_date,
                            "latitude": lat,
                            "longitude": lng,
                            "naver_url": None,
                            "area_m2": self._parse_area_m2(area),
                            "land_use_zone": rng.choice(land_zones) if ptype == "토지" else None,
                            "land_category": rng.choice(land_categories) if ptype == "토지" else None,
                            "road_access": rng.choice(["대로변", "도로접면", "코너/다중접면"]) if ptype != "업무" or rng.random() < 0.4 else None,
                            "premium_info": rng.choice(["무권리", "권리금 확인", None]) if ptype == "상가" else None,
                            "estimated_yield_rate": round(rng.uniform(4.5, 11.5), 2) if ttype == "월세" else None,
                            "price_drop_rate": round(rng.uniform(1.0, 12.0), 1) if urgent_tag == "가격인하" else None,
                        }
                    )

        return listings

    def crawl_all(self) -> Dict:
        session_id = str(uuid.uuid4())[:8]
        all_listings = []
        source = "naver"
        allow_demo_fallback = os.getenv("ALLOW_DEMO_FALLBACK", "false").strip().lower() in {"1", "true", "yes", "on"}
        logger.info("급매 전용 크롤링 시작 (Playwright)...")
        all_listings = self._crawl_with_playwright()

        if not all_listings:
            if allow_demo_fallback:
                logger.warning("라이브 크롤링 데이터 없음 → 데모 데이터 폴백 사용")
                all_listings = self.generate_demo_data()
                source = "demo"
                urgent_count = len(all_listings)

                def persist_degraded_crawl():
                    self.db.insert_listings(all_listings, session_id)
                    self.db.log_crawl(session_id, len(all_listings), urgent_count, "degraded", source)
                    return {"total": len(all_listings), "urgent": urgent_count, "source": source, "status": "degraded"}

                result = self._with_db_reconnect("degraded crawl persistence", persist_degraded_crawl)
                logger.warning(f"급매 크롤링 폴백 완료: {len(all_listings)}개 [출처: {source}, status=degraded]")
                return result

            logger.error("라이브 크롤링 데이터 없음: 데모 폴백 비활성화 상태. 기존 데이터 유지")

            def persist_failed_crawl():
                self.db.log_crawl(session_id, 0, 0, "failed", source)
                return {"total": 0, "urgent": 0, "source": source, "status": "failed"}

            return self._with_db_reconnect("failed crawl logging", persist_failed_crawl)

        def persist_live_crawl():
            previous_live = self.db.get_last_successful_live_crawl()
            previous_total = int((previous_live or {}).get("total_count") or 0)
            previous_session_id = (previous_live or {}).get("session_id")
            if (
                previous_total
                and previous_session_id
                and hasattr(self.db, "count_commercial_listings_for_session")
            ):
                previous_commercial_total = self.db.count_commercial_listings_for_session(previous_session_id)
                if previous_commercial_total:
                    previous_total = previous_commercial_total
                else:
                    logger.warning(
                        "직전 성공 크롤링 세션에 상업용 매물이 없어 라이브 수량 안전장치 기준에서 제외: previous_session=%s previous_total=%s",
                        previous_session_id,
                        previous_total,
                    )
                    previous_total = 0
            min_allowed = int(previous_total * self.MIN_LIVE_CRAWL_RATIO) if previous_total else 0

            if previous_total and len(all_listings) < min_allowed:
                logger.error(
                    "라이브 크롤링 결과가 비정상적으로 적음: current=%s, previous=%s, min_allowed=%s",
                    len(all_listings),
                    previous_total,
                    min_allowed,
                )
                self.db.log_crawl(session_id, len(all_listings), len(all_listings), "failed", source)
                return {"total": len(all_listings), "urgent": len(all_listings), "source": source, "status": "failed"}

            urgent_count = len(all_listings)
            self.db.insert_listings(all_listings, session_id)
            self.db.log_crawl(session_id, len(all_listings), urgent_count, "success", "naver")
            return {"total": len(all_listings), "urgent": urgent_count, "source": source, "status": "success"}

        result = self._with_db_reconnect("live crawl persistence", persist_live_crawl)

        if result.get("status") == "success":
            logger.info(f"급매 크롤링 완료: {len(all_listings)}개 급매 [출처: {source}]")
        return result

    def _with_db_reconnect(self, label: str, operation):
        for attempt in range(1, 3):
            try:
                return operation()
            except Exception as exc:
                is_transient = (
                    hasattr(self.db, "is_transient_connection_error")
                    and self.db.is_transient_connection_error(exc)
                )
                if not is_transient or attempt >= 2:
                    raise

                logger.warning(
                    "%s failed after a transient database connection error; reconnecting and retrying: %s",
                    label,
                    exc,
                )
                if hasattr(self.db, "reconnect"):
                    self.db.reconnect()
                time.sleep(2)
