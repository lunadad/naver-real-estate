"""쿼리 공용 조각 — SQLite·Postgres 공통(`?` 플레이스홀더)."""

from typing import Sequence

CHUNK = 500  # 큰 IN 목록·executemany를 나누는 크기


def chunks(items: Sequence, size: int = CHUNK):
    for start in range(0, len(items), size):
        yield items[start : start + size]


def placeholders(count: int) -> str:
    return ",".join("?" * count)
