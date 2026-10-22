"""크롤 세션 조회 공용 헬퍼 — 최신 세션과 "7일 전" 비교 세션 고르기.

7일 전 세션은 weekly_report와 같은 규칙(라이브 성공 세션만)으로, 최신 세션 시각 -7일을
기준 ±12시간 안에서 가장 가까운 세션을 쓴다. 없으면 None (거짓 비교를 만들지 않는다).
"""

from datetime import datetime, timedelta, timezone
from typing import Optional, Sequence, Tuple

WEEK_WINDOW = timedelta(hours=12)
LIVE_SESSION_SQL = "status = 'success' AND COALESCE(source, 'naver') <> 'demo'"


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


def get_week_ago_session(conn, latest_session_id: str, latest_at) -> Optional[str]:
    rows = conn.execute(
        f"SELECT session_id, crawled_at FROM crawl_history WHERE {LIVE_SESSION_SQL} "
        "ORDER BY crawled_at DESC LIMIT 500"
    ).fetchall()
    return pick_week_ago_session(
        [(row["session_id"], row["crawled_at"]) for row in rows], latest_session_id, latest_at
    )
