"""단지 상세 페이지의 순수 계산 — DB 접근 없이 매물 dict 목록만 다룬다."""

import statistics
from typing import Dict, Iterable, List, Optional

from bargain import area_number, is_bargain


def _tidy(value):
    if isinstance(value, float) and value.is_integer():
        return int(value)
    return round(value, 1) if isinstance(value, float) else value


def median_price(values: List[int]):
    return _tidy(statistics.median(values)) if values else None


def has_price_down(listing: Dict) -> bool:
    return "가격인하" in str(listing.get("tags") or "")


def build_areas(listings: Iterable[Dict]) -> List[Dict]:
    """매매·가격>0 매물을 평형 타입별로 묶는다. 호가는 낮은 순, 평형은 ㎡ 순."""
    grouped: Dict[str, List[Dict]] = {}
    for listing in listings:
        value = listing.get("price_sort_value")
        area = str(listing.get("area") or "").strip()
        if listing.get("trade_type") != "매매" or not area or not value or value <= 0:
            continue
        grouped.setdefault(area, []).append(listing)

    areas = []
    for area, items in grouped.items():
        items.sort(key=lambda item: (item["price_sort_value"], str(item.get("article_no"))))
        areas.append(
            {
                "area": area,
                "median": median_price([item["price_sort_value"] for item in items]),
                "prices": [
                    {"value": item["price_sort_value"], "bargain": is_bargain(item.get("bargain_discount"))}
                    for item in items
                ],
            }
        )
    areas.sort(key=lambda a: (area_number(a["area"]) or 0, a["area"]))
    return areas


def order_listings(listings: Iterable[Dict]) -> List[Dict]:
    """진짜 급매 먼저(할인율 큰 순), 나머지는 할인율 큰 순 → 가격 낮은 순."""
    def key(item):
        discount = item.get("bargain_discount")
        return (
            0 if is_bargain(discount) else 1,
            -(discount if discount is not None else float("-inf")),
            item.get("price_sort_value") or 0,
            str(item.get("article_no")),
        )

    result = []
    for item in sorted(listings, key=key):
        result.append({**item, "is_bargain": is_bargain(item.get("bargain_discount"))})
    return result


def build_stats(listings: List[Dict], week_ago_count: Optional[int]) -> Dict:
    total = len(listings)
    down = sum(1 for item in listings if has_price_down(item))
    return {
        "current_count": total,
        "week_ago_count": week_ago_count,
        "bargain_count": sum(1 for item in listings if is_bargain(item.get("bargain_discount"))),
        "price_down_ratio": round(down / total, 4) if total else 0.0,
    }
