"""DB에 저장된 naive 시각(ISO 문자열 또는 datetime) 공용 헬퍼.

tz 정보는 변환 없이 버린다 — 크롤러가 저장한 벽시계 시각끼리 비교하기 위해서다.
UTC로 정규화가 필요하면 insights.sessions.parse_dt를 쓴다.
"""

from datetime import datetime
from typing import Optional


def parse_naive(value) -> Optional[datetime]:
    """ISO 문자열/datetime → naive datetime. 읽을 수 없으면 None."""
    if isinstance(value, datetime):
        return value.replace(tzinfo=None)
    try:
        return datetime.fromisoformat(str(value)).replace(tzinfo=None)
    except (TypeError, ValueError):
        return None


def days_between(start, end: datetime) -> Optional[int]:
    """start(문자열/datetime)부터 end(datetime)까지 지난 일수. 음수는 0, start를 못 읽으면 None."""
    start = parse_naive(start)
    return max(0, (end - start).days) if start else None


def days_span(start, end) -> Optional[int]:
    """둘 다 저장된 시각 값일 때의 일수. 하나라도 못 읽으면 None."""
    end = parse_naive(end)
    return days_between(start, end) if end else None
