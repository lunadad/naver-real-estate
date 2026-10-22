"""텔레그램 슬래시 명령 처리 (/deals, /complex, /weekly, /dealalert).

Hermes 플러그인(integrations/hermes/real-estate)이 이 함수들을 호출한다. DB 비밀번호
없이 공개 사이트 API만 쓴다. 표준 라이브러리만 사용한다.
"""

import json
import urllib.error
import urllib.parse
import urllib.request
from collections import Counter
from datetime import datetime
from typing import Callable, Dict, List, Optional

from deal_query import load_districts, parse_deal_args
from telegram_alerts import (
    SITE_URL,
    TelegramRuleStore,
    format_deal_line,
    rule_label,
    select_bargains,
)

MAX_ITEMS = 5
DEALS_USAGE = (
    "사용법: /deals <지역> [가격상한] [최소할인%]\n"
    "예) /deals 송파 20억 15%  ·  /deals 분당 12억5천"
)
DEALALERT_USAGE = (
    "사용법:\n"
    "/dealalert add <지역> [가격상한] [최소할인%] — 새 진짜 급매가 나오면 크롤링 직후 알림\n"
    "/dealalert list — 등록된 알림\n"
    "/dealalert del <번호> — 삭제"
)
OFFLINE_REPLY = "사이트에 연결하지 못했습니다. 잠시 후 다시 시도해 주세요."


def fetch_site(path: str, params: Optional[Dict] = None):
    """공개 사이트 API GET. JSON이면 dict, 아니면 text."""
    query = f"?{urllib.parse.urlencode(params)}" if params else ""
    request = urllib.request.Request(
        f"{SITE_URL}{path}{query}", headers={"User-Agent": "real-estate-telegram-bot"}
    )
    with urllib.request.urlopen(request, timeout=10) as response:
        body = response.read().decode("utf-8")
        if "json" in (response.headers.get("Content-Type") or ""):
            return json.loads(body)
        return body


def describe_rule(rule: Dict) -> str:
    return f"#{rule['id']} {rule_label(rule)}"


def _ambiguous_reply(parsed: Dict) -> Optional[str]:
    if parsed.get("too_large"):
        return f"가격 상한 '{parsed['too_large'][0]}'이(가) 너무 큽니다. 예) 20억, 15억5천"
    if not parsed.get("ambiguous"):
        return None
    token = parsed["ambiguous"][0]
    return (f"'{token}'가 가격인지 할인율인지 알 수 없어요. "
            f"{token}% 또는 {token}억처럼 단위를 붙여 주세요.")


def _is_not_found(exc: Exception) -> bool:
    return isinstance(exc, urllib.error.HTTPError) and exc.code == 404


def _unknown_region_reply(parsed: Dict) -> str:
    return f"지역을 찾지 못했습니다: {' '.join(parsed['unknown']) or '(없음)'}\n" + DEALS_USAGE


def handle_deals(raw: str, *, fetch: Callable = fetch_site,
                 districts: Optional[List[Dict]] = None) -> str:
    if not (raw or "").strip():
        return DEALS_USAGE
    parsed = parse_deal_args(raw, districts if districts is not None else load_districts())
    problem = _ambiguous_reply(parsed)
    if problem:
        return problem
    if not parsed["district"]:
        return _unknown_region_reply(parsed)
    params = {
        "region": parsed["region"], "district": parsed["district"], "trade_type": "매매",
        "bargain_only": "true", "sort_by": "recent", "per_page": 100,
    }
    if parsed["max_price"]:
        params["max_price"] = parsed["max_price"]
    try:
        result = fetch("/api/listings", params)
    except Exception:
        return OFFLINE_REPLY
    picked = select_bargains(result.get("listings") or [], parsed["min_discount"])
    label = rule_label(parsed)
    if not picked:
        return f"{label} 조건의 진짜 급매가 없습니다 (최신 크롤링 기준)."
    lines = [f"{label} 진짜 급매 {len(picked)}건 (최신 크롤링 기준)"]
    for row in picked[:MAX_ITEMS]:
        lines.extend(format_deal_line(row))
    return "\n".join(lines)


def handle_complex(raw: str, *, fetch: Callable = fetch_site) -> str:
    tokens = (raw or "").split()
    if not tokens:
        return "사용법: /complex <단지명> [구]\n예) /complex 헬리오시티"
    name, district_hint = tokens[0], " ".join(tokens[1:])
    try:
        found = fetch("/api/listings", {"search": name, "per_page": 100}).get("listings") or []
    except Exception:
        return OFFLINE_REPLY
    pairs = [(r.get("district"), r.get("building_name")) for r in found
             if r.get("building_name") and (not district_hint or district_hint in (r.get("district") or ""))]
    exact = [p for p in pairs if p[1] == name]
    candidates = Counter(exact or [p for p in pairs if name in p[1]])
    if not candidates:
        return f"'{name}' 단지의 급매를 찾지 못했습니다."
    district, building = candidates.most_common(1)[0][0]
    try:
        data = fetch("/api/complex", {"district": district, "building_name": building})
    except Exception as exc:
        if _is_not_found(exc):
            return f"'{building}' ({district}) 단지 데이터를 찾지 못했습니다."
        return OFFLINE_REPLY
    stats = data.get("stats") or {}
    current, week_ago = stats.get("current_count"), stats.get("week_ago_count")
    trend = f"지난주 대비 {current - week_ago:+d}" if current is not None and week_ago is not None else "지난주 데이터 없음"
    ratio = stats.get("price_down_ratio")
    # /api/complex는 0~1 비율을 준다
    ratio_text = f"{ratio * 100:.1f}%" if isinstance(ratio, (int, float)) else "—"
    link = f"{SITE_URL}/complex?" + urllib.parse.urlencode({"district": district, "name": building})
    return "\n".join([
        f"{building} ({district})",
        f"급매 {current if current is not None else '—'}건 · {trend}",
        f"진짜 급매 {stats.get('bargain_count', 0)}건 · 가격인하 비율 {ratio_text}",
        link,
    ])


def handle_weekly(raw: str, *, fetch: Callable = fetch_site) -> str:
    try:
        return str(fetch("/api/weekly-report", None))
    except Exception as exc:
        if _is_not_found(exc):
            return "아직 크롤링 데이터가 없습니다. 첫 크롤링이 끝난 뒤 다시 시도해 주세요."
        return OFFLINE_REPLY


def handle_dealalert(raw: str, *, store: Optional[TelegramRuleStore] = None,
                     districts: Optional[List[Dict]] = None, now: Optional[datetime] = None) -> str:
    store = store or TelegramRuleStore()
    action, _, rest = (raw or "").strip().partition(" ")
    if action == "add":
        parsed = parse_deal_args(rest, districts if districts is not None else load_districts())
        problem = _ambiguous_reply(parsed)
        if problem:
            return problem
        if not parsed["district"]:
            return _unknown_region_reply(parsed)
        rule = store.add(district=parsed["district"], region=parsed["region"] or "",
                         max_price=parsed["max_price"], min_discount=parsed["min_discount"], now=now)
        return f"알림 등록: {describe_rule(rule)}\n크롤링이 끝나면 새 진짜 급매만 모아 보내드려요."
    if action == "list":
        rules = store.list()
        if not rules:
            return "등록된 알림이 없습니다.\n" + DEALALERT_USAGE
        return "\n".join(describe_rule(rule) for rule in rules)
    if action == "del":
        try:
            rule_id = int(rest.strip().lstrip("#"))
        except ValueError:
            return DEALALERT_USAGE
        return f"#{rule_id} 알림을 삭제했습니다." if store.remove(rule_id) else f"#{rule_id} 알림이 없습니다."
    return DEALALERT_USAGE
