"""쿼리 공용 조각 — SQLite·Postgres 공통(`?` 플레이스홀더)."""

from typing import Sequence

CHUNK = 500  # 큰 IN 목록·executemany를 나누는 크기

# crawl_history에서 "라이브 성공 세션"만 고르는 조건 (source가 NULL인 옛 행도 라이브로 본다)
LIVE_SESSION_SQL = "status = 'success' AND COALESCE(source, 'naver') <> 'demo'"


def chunks(items: Sequence, size: int = CHUNK):
    for start in range(0, len(items), size):
        yield items[start : start + size]


def placeholders(count: int) -> str:
    return ",".join("?" * count)
