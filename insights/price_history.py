"""가격 이력 모달의 "협상 단서" 계산 (순수 함수)."""

from datetime import datetime
from typing import Dict, List, Optional

from bargain import is_bargain


def _parse(value) -> Optional[datetime]:
    if isinstance(value, datetime):
        return value.replace(tzinfo=None)
    try:
        return datetime.fromisoformat(str(value)).replace(tzinfo=None)
    except (TypeError, ValueError):
        return None


def _days_between(start, end: datetime) -> Optional[int]:
    start = _parse(start)
    if start is None:
        return None
    return max(0, (end - start).days)


def build_insight(
    tracking: Dict,
    events: List[Dict],
    bargain_discount: Optional[float],
    now: Optional[datetime] = None,
) -> Dict:
    """처음 발견 후 경과일, 인하 횟수, 누적 변화율, 마지막 변경 후 일수, 진짜 급매 할인율."""
    now = now or datetime.now()
    total_change_pct = None
    last_change_at = None
    if events:
        first_value = events[0].get("price_sort_value")
        last_value = events[-1].get("price_sort_value")
        if first_value and last_value is not None:
            total_change_pct = round((last_value - first_value) / first_value * 100, 2)
        # 이벤트가 둘 이상일 때만 "변경"이 있다 (첫 이벤트는 최초 발견).
        if len(events) > 1:
            last_change_at = events[-1].get("observed_at")
    return {
        "days_since_first_seen": _days_between(tracking.get("first_seen_at"), now),
        "days_since_last_change": _days_between(last_change_at, now) if last_change_at else None,
        "cut_count": int(tracking.get("price_cut_count") or 0),
        "total_change_pct": total_change_pct,
        "bargain_discount_pct": round(bargain_discount * 100, 1) if is_bargain(bargain_discount) else None,
    }
