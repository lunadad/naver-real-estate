"""진짜 급매 점수 — 같은 단지 시세(중앙값) 대비 할인율.

1순위로 같은 (구, 단지, 평형 타입) 매물 가격 중앙값과 비교하고, 그 그룹이
MIN_GROUP건 미만이면 같은 (구, 단지)의 ㎡당 가격 중앙값과 비교한다.
"""

import re
import statistics
from collections import defaultdict
from typing import Dict, Iterable, Mapping, Optional, Tuple

BARGAIN_MIN = 0.10
BARGAIN_MAX = 0.40  # 이보다 크게 싸면 지분 매물·입력 오류로 의심
MIN_GROUP = 3
TARGET_PROPERTY_TYPES = frozenset({"아파트", "오피스텔"})

# 물리적으로 다른 주택이 같은 이름으로 합쳐지는 것을 막기 위해,
# 고유 건물명이 아닌 매물 유형명은 단지 단위 집계에서 제외한다.
GENERIC_BUILDING_NAMES = frozenset(
    {
        "아파트",
        "오피스텔",
        "빌라/연립",
        "빌라",
        "연립",
        "연립주택",
        "다세대",
        "다세대주택",
        "단독/다가구",
        "단독주택",
        "다가구",
        "다가구주택",
        "주택",
        "상가/업무",
        "상가",
        "상가주택",
        "근린상가",
        "원룸",
    }
)


def area_number(area: Optional[str]) -> Optional[float]:
    """평형 타입("84A", "113E")의 앞 숫자를 ㎡ 값으로 읽는다."""
    match = re.match(r"\s*(\d+(?:\.\d+)?)", area or "")
    return float(match.group(1)) if match else None


def is_bargain(discount: Optional[float]) -> bool:
    return discount is not None and BARGAIN_MIN <= discount <= BARGAIN_MAX


def _is_target(listing: Mapping) -> bool:
    building = str(listing.get("building_name") or "").strip()
    value = listing.get("price_sort_value")
    return (
        listing.get("trade_type") == "매매"
        and listing.get("property_type") in TARGET_PROPERTY_TYPES
        and bool(building)
        and building not in GENERIC_BUILDING_NAMES
        and bool(str(listing.get("area") or "").strip())
        and value is not None
        and value > 0
    )


def compute_bargain_scores(listings: Iterable[Mapping]) -> Dict[str, Tuple[float, str]]:
    """article_no → (할인율, 기준). 할인율은 음수(더 비쌈)도 그대로 둔다."""
    targets = [listing for listing in listings if _is_target(listing)]

    by_area_type = defaultdict(list)
    by_complex_m2 = defaultdict(list)
    for listing in targets:
        complex_key = (listing.get("district"), str(listing["building_name"]).strip())
        by_area_type[complex_key + (str(listing["area"]).strip(),)].append(listing["price_sort_value"])
        area = area_number(listing["area"])
        if area:
            by_complex_m2[complex_key].append(listing["price_sort_value"] / area)

    scores = {}
    for listing in targets:
        complex_key = (listing.get("district"), str(listing["building_name"]).strip())
        value = listing["price_sort_value"]
        same_area = by_area_type[complex_key + (str(listing["area"]).strip(),)]
        if len(same_area) >= MIN_GROUP:
            discount = 1 - value / statistics.median(same_area)
            basis = "same_area"
        else:
            area = area_number(listing["area"])
            per_m2 = by_complex_m2[complex_key]
            if not area or len(per_m2) < MIN_GROUP:
                continue
            discount = 1 - (value / area) / statistics.median(per_m2)
            basis = "complex_m2"
        scores[str(listing["article_no"])] = (round(discount, 4), basis)
    return scores
