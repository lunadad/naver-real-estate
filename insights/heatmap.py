"""지역 히트맵의 순수 계산 — 구별 지표 조합."""

from typing import Dict, Iterable, List, Mapping, Optional

from bargain import is_bargain

DEFAULT_REGION = "서울특별시"

METRIC_DEFS = [
    {"key": "bargain_ratio", "label": "진짜 급매 비율", "unit": "ratio"},
    {"key": "price_down_ratio", "label": "가격인하 비율", "unit": "ratio"},
    {"key": "weekly_change_pct", "label": "주간 급매 증감", "unit": "pct"},
]


def _ratio(part: int, whole: int) -> Optional[float]:
    return round(part / whole, 4) if whole else None


def weekly_change_pct(current: Optional[int], previous: Optional[int]) -> Optional[float]:
    if current is None or not previous:
        return None
    return round((current - previous) / previous * 100, 1)


def build_districts(
    district_names: Iterable[str],
    listing_rows: Iterable[Mapping],
    stats_now: Mapping[str, Mapping],
    stats_prev: Optional[Mapping[str, Mapping]],
) -> List[Dict]:
    """구 목록 순서를 유지하며 구별 지표를 만든다.

    total·가격인하는 crawl_region_stats(없으면 listings 집계)에서, 진짜 급매 수는
    listings의 bargain_discount에서 센다. stats_prev가 None이면 주간 증감은 모두 None.
    """
    counted: Dict[str, Dict[str, int]] = {}
    for row in listing_rows:
        entry = counted.setdefault(row["district"], {"total": 0, "down": 0, "bargain": 0})
        entry["total"] += 1
        if "가격인하" in str(row.get("tags") or ""):
            entry["down"] += 1
        if is_bargain(row.get("bargain_discount")):
            entry["bargain"] += 1

    result = []
    for name in district_names:
        counts = counted.get(name, {"total": 0, "down": 0, "bargain": 0})
        stat = stats_now.get(name)
        total = int(stat["total_count"] or 0) if stat else counts["total"]
        down = int(stat["price_down_count"] or 0) if stat else counts["down"]
        prev = stats_prev.get(name) if stats_prev is not None else None
        result.append(
            {
                "district": name,
                "total": total,
                "bargain_count": counts["bargain"],
                "bargain_ratio": _ratio(counts["bargain"], total),
                "price_down_ratio": _ratio(down, total),
                "weekly_change_pct": weekly_change_pct(
                    total if total else None, int(prev["total_count"] or 0) if prev else None
                ),
            }
        )
    return result
