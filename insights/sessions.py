"""크롤 세션 조회 공용 헬퍼 — 최신 세션과 "7일 전" 비교 세션 고르기.

7일 전 세션은 weekly_report와 같은 규칙(라이브 성공 세션만)으로, 최신 세션 시각 -7일을
기준 ±12시간 안에서 가장 가까운 세션을 쓴다. 없으면 None (거짓 비교를 만들지 않는다).
"""

from datetime import datetime, timedelta, timezone
from typing import Dict, Optional, Sequence, Tuple

from insights.sql import LIVE_SESSION_SQL

WEEK_WINDOW = timedelta(hours=12)
COMPARISON_SLACK = timedelta(hours=12)


def parse_dt(value) -> Optional[datetime]:
    """ISO 문자열/datetime → naive datetime (tz가 있으면 UTC 기준으로 정규화)."""
    if value is None:
        return None
    if not isinstance(value, datetime):
        try:
            value = datetime.fromisoformat(str(value).strip().replace("Z", "+00:00"))
        except ValueError:
            return None
    if value.tzinfo is not None:
        value = value.astimezone(timezone.utc).replace(tzinfo=None)
    return value


def pick_week_ago_session(
    sessions: Sequence[Tuple[str, object]], latest_session_id: str, latest_at
) -> Optional[str]:
    """(session_id, crawled_at) 목록에서 latest -7일 ±12h에 가장 가까운 세션 id."""
    latest_dt = parse_dt(latest_at)
    if latest_dt is None:
        return None
    target = latest_dt - timedelta(days=7)
    best_id, best_gap = None, None
    for session_id, crawled_at in sessions:
        if session_id == latest_session_id:
            continue
        dt = parse_dt(crawled_at)
        if dt is None:
            continue
        gap = abs(dt - target)
        if gap <= WEEK_WINDOW and (best_gap is None or gap < best_gap):
            best_id, best_gap = session_id, gap
    return best_id


def get_latest_session(db, conn) -> Tuple[Optional[str], Optional[datetime]]:
    """사이트가 보여주는 최신 세션(listings 기준)과 그 크롤 시각."""
    session_id = db._get_latest_visible_session_id(conn)
    if not session_id:
        return None, None
    row = conn.execute(
        "SELECT crawled_at FROM crawl_history WHERE session_id = ? ORDER BY crawled_at DESC LIMIT 1",
        (session_id,),
    ).fetchone()
    crawled_at = row["crawled_at"] if row else None
    if crawled_at is None:
        row = conn.execute(
            "SELECT MAX(crawled_at) AS crawled_at FROM listings WHERE crawl_session = ?",
            (session_id,),
        ).fetchone()
        crawled_at = row["crawled_at"] if row else None
    return session_id, parse_dt(crawled_at)


def get_comparison_session(
    conn, latest_session_id: str, latest_at: datetime, days: int, columns: str = "session_id, crawled_at"
):
    """latest보다 days일(-12시간 허용) 이전인 가장 최근 라이브 성공 세션 행.

    없으면 latest와 다른 가장 오래된 라이브 성공 세션 행, 그것도 없으면 None.
    주간 리포트와 "빨리 빠지는 단지"가 같은 규칙을 쓴다.
    """
    cutoff = (latest_at - timedelta(days=days) + COMPARISON_SLACK).isoformat()
    row = conn.execute(
        f"SELECT {columns} FROM crawl_history WHERE {LIVE_SESSION_SQL} AND session_id <> ? "
        "AND crawled_at <= ? ORDER BY crawled_at DESC LIMIT 1",
        (latest_session_id, cutoff),
    ).fetchone()
    if row is None:
        row = conn.execute(
            f"SELECT {columns} FROM crawl_history WHERE {LIVE_SESSION_SQL} AND session_id <> ? "
            "ORDER BY crawled_at ASC LIMIT 1",
            (latest_session_id,),
        ).fetchone()
    return row


def get_building_counts(conn, session_id: str) -> Dict[Tuple[str, str], int]:
    """(district, building_name) → 그 세션의 단지별 매물 수."""
    rows = conn.execute(
        "SELECT district, building_name, total_count FROM crawl_building_stats WHERE session_id = ?",
        (session_id,),
    ).fetchall()
    return {(row["district"], row["building_name"]): row["total_count"] or 0 for row in rows}


def get_week_ago_session(conn, latest_session_id: str, latest_at) -> Optional[str]:
    rows = conn.execute(
        f"SELECT session_id, crawled_at FROM crawl_history WHERE {LIVE_SESSION_SQL} "
        "ORDER BY crawled_at DESC LIMIT 500"
    ).fetchall()
    return pick_week_ago_session(
        [(row["session_id"], row["crawled_at"]) for row in rows], latest_session_id, latest_at
    )
