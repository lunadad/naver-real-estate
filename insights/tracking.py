"""매물 가격 이력(listing_tracking/listing_price_events)과 사라진 매물(listing_disappearances).

모든 쓰기 함수는 호출자(Database.insert_listings)의 트랜잭션 커넥션을 받아 같은 트랜잭션에서
실행된다. SQL은 SQLite·Postgres 공통(`?` 플레이스홀더, ISO 문자열 시각)이며 큰 IN 목록은
500개씩 나눈다.
"""

import json
from datetime import datetime, timedelta
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

from bargain import is_bargain

CHUNK = 500
RETENTION_DAYS = 90

TRACKING_TABLES_SQLITE = """
CREATE TABLE IF NOT EXISTS listing_tracking (
    article_no TEXT PRIMARY KEY,
    first_seen_at TEXT,
    last_seen_at TEXT,
    last_price TEXT,
    last_price_sort_value BIGINT,
    price_change_count INTEGER NOT NULL DEFAULT 0,
    price_cut_count INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS listing_price_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    article_no TEXT NOT NULL,
    session_id TEXT NOT NULL,
    observed_at TEXT,
    price TEXT,
    price_sort_value BIGINT,
    prev_price TEXT,
    prev_price_sort_value BIGINT,
    change_pct REAL,
    UNIQUE(article_no, session_id)
);

CREATE TABLE IF NOT EXISTS listing_disappearances (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    article_no TEXT NOT NULL,
    session_id TEXT NOT NULL,
    disappeared_at TEXT,
    region TEXT,
    district TEXT,
    building_name TEXT,
    property_type TEXT,
    trade_type TEXT,
    area TEXT,
    floor TEXT,
    price TEXT,
    naver_url TEXT,
    first_seen_at TEXT,
    was_bargain INTEGER NOT NULL DEFAULT 0,
    had_price_down INTEGER NOT NULL DEFAULT 0,
    reappeared_at TEXT,
    UNIQUE(article_no, session_id)
);
"""

TRACKING_TABLES_POSTGRES = [
    """
    CREATE TABLE IF NOT EXISTS listing_tracking (
        article_no TEXT PRIMARY KEY,
        first_seen_at TEXT,
        last_seen_at TEXT,
        last_price TEXT,
        last_price_sort_value BIGINT,
        price_change_count INTEGER NOT NULL DEFAULT 0,
        price_cut_count INTEGER NOT NULL DEFAULT 0
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS listing_price_events (
        id BIGSERIAL PRIMARY KEY,
        article_no TEXT NOT NULL,
        session_id TEXT NOT NULL,
        observed_at TEXT,
        price TEXT,
        price_sort_value BIGINT,
        prev_price TEXT,
        prev_price_sort_value BIGINT,
        change_pct DOUBLE PRECISION,
        UNIQUE(article_no, session_id)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS listing_disappearances (
        id BIGSERIAL PRIMARY KEY,
        article_no TEXT NOT NULL,
        session_id TEXT NOT NULL,
        disappeared_at TEXT,
        region TEXT,
        district TEXT,
        building_name TEXT,
        property_type TEXT,
        trade_type TEXT,
        area TEXT,
        floor TEXT,
        price TEXT,
        naver_url TEXT,
        first_seen_at TEXT,
        was_bargain INTEGER NOT NULL DEFAULT 0,
        had_price_down INTEGER NOT NULL DEFAULT 0,
        reappeared_at TEXT,
        UNIQUE(article_no, session_id)
    )
    """,
]

TRACKING_INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_tracking_last_seen ON listing_tracking(last_seen_at)",
    "CREATE INDEX IF NOT EXISTS idx_price_events_article ON listing_price_events(article_no)",
    "CREATE INDEX IF NOT EXISTS idx_disappearances_at ON listing_disappearances(disappeared_at)",
    "CREATE INDEX IF NOT EXISTS idx_disappearances_article ON listing_disappearances(article_no)",
]


def _chunks(items: Sequence, size: int = CHUNK):
    for start in range(0, len(items), size):
        yield items[start : start + size]


def _placeholders(count: int) -> str:
    return ",".join("?" * count)


def _iso(value) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.isoformat()
    return str(value)


def record_price_events(
    conn,
    listings: Sequence[Dict],
    price_values: Sequence[Tuple[Optional[int], Optional[int]]],
    session_id: str,
    now: str,
) -> Dict[str, Tuple[str, int]]:
    """처음 본 매물은 tracking+최초 이벤트를 만들고, 가격 문자열이 바뀌면 이벤트를 더한다.

    같은 (article_no, session_id)는 한 번만 기록하므로 같은 세션을 다시 넣어도 중복되지 않는다.
    반환: article_no → (first_seen_at, price_cut_count) — listings 컬럼에 함께 저장한다.
    """
    article_nos = []
    for listing in listings:
        article_no = str(listing.get("article_no"))
        if article_no not in article_nos:
            article_nos.append(article_no)

    state: Dict[str, Dict] = {}
    for chunk in _chunks(article_nos):
        rows = conn.execute(
            "SELECT article_no, first_seen_at, last_price, last_price_sort_value, "
            "price_change_count, price_cut_count FROM listing_tracking "
            f"WHERE article_no IN ({_placeholders(len(chunk))})",
            chunk,
        ).fetchall()
        for row in rows:
            state[row["article_no"]] = {
                "first_seen_at": _iso(row["first_seen_at"]),
                "last_price": row["last_price"],
                "last_value": row["last_price_sort_value"],
                "changes": row["price_change_count"] or 0,
                "cuts": row["price_cut_count"] or 0,
                "exists": True,
            }

    seen_this_session = set()
    for chunk in _chunks(article_nos):
        rows = conn.execute(
            "SELECT article_no FROM listing_price_events "
            f"WHERE session_id = ? AND article_no IN ({_placeholders(len(chunk))})",
            [session_id] + list(chunk),
        ).fetchall()
        seen_this_session.update(row["article_no"] for row in rows)

    events = []
    for listing, (price_value, _rent) in zip(listings, price_values):
        article_no = str(listing.get("article_no"))
        price = str(listing.get("price") or "")
        entry = state.get(article_no)
        if entry is None:
            state[article_no] = {
                "first_seen_at": now,
                "last_price": price,
                "last_value": price_value,
                "changes": 0,
                "cuts": 0,
                "exists": False,
            }
            events.append((article_no, session_id, now, price, price_value, None, None, None))
            seen_this_session.add(article_no)
            continue
        if article_no in seen_this_session or price == (entry["last_price"] or ""):
            continue
        prev_value = entry["last_value"]
        change_pct = None
        if prev_value and price_value is not None:
            change_pct = round((price_value - prev_value) / prev_value * 100, 2)
        entry["changes"] += 1
        if prev_value is not None and price_value is not None and price_value < prev_value:
            entry["cuts"] += 1
        events.append(
            (article_no, session_id, now, price, price_value, entry["last_price"], prev_value, change_pct)
        )
        entry["last_price"] = price
        entry["last_value"] = price_value
        seen_this_session.add(article_no)

    new_rows = []
    update_rows = []
    for article_no in article_nos:
        entry = state[article_no]
        if entry["exists"]:
            update_rows.append(
                (now, entry["last_price"], entry["last_value"], entry["changes"], entry["cuts"], article_no)
            )
        else:
            new_rows.append(
                (article_no, entry["first_seen_at"], now, entry["last_price"], entry["last_value"],
                 entry["changes"], entry["cuts"])
            )
    for chunk in _chunks(new_rows):
        conn.executemany(
            "INSERT INTO listing_tracking (article_no, first_seen_at, last_seen_at, last_price, "
            "last_price_sort_value, price_change_count, price_cut_count) VALUES (?, ?, ?, ?, ?, ?, ?) "
            "ON CONFLICT(article_no) DO NOTHING",
            chunk,
        )
    for chunk in _chunks(update_rows):
        conn.executemany(
            "UPDATE listing_tracking SET last_seen_at = ?, last_price = ?, last_price_sort_value = ?, "
            "price_change_count = ?, price_cut_count = ? WHERE article_no = ?",
            chunk,
        )
    for chunk in _chunks(events):
        conn.executemany(
            "INSERT INTO listing_price_events (article_no, session_id, observed_at, price, "
            "price_sort_value, prev_price, prev_price_sort_value, change_pct) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT(article_no, session_id) DO NOTHING",
            chunk,
        )
    return {no: (state[no]["first_seen_at"], state[no]["cuts"]) for no in article_nos}


def mark_reappeared(conn, article_nos: Iterable[str], session_id: str, now: str) -> None:
    """새 세션에 다시 나타난 매물의 열린 소멸 기록에 reappeared_at을 찍는다."""
    nos = sorted({str(no) for no in article_nos})
    for chunk in _chunks(nos):
        conn.execute(
            "UPDATE listing_disappearances SET reappeared_at = ? "
            f"WHERE reappeared_at IS NULL AND session_id <> ? AND article_no IN ({_placeholders(len(chunk))})",
            [now, session_id] + list(chunk),
        )


def _normalize_scopes(incomplete_scopes) -> set:
    return {tuple(scope) for scope in (incomplete_scopes or [])}


def record_disappearances(conn, session_id: str, now: str, incomplete_scopes=None) -> int:
    """새 세션 upsert 후에도 직전 세션 꼬리표가 남은 행 = 사라진 매물.

    크롤러가 실패한 (구, 매물유형, 거래유형) 범위의 행은 가짜 소멸이므로 기록하지 않는다.
    """
    rows = conn.execute(
        "SELECT article_no, region, district, building_name, property_type, trade_type, area, floor, "
        "price, naver_url, first_seen_at, crawled_at, crawl_session, bargain_discount, tags, price_cut_count "
        "FROM listings WHERE crawl_session IS NOT NULL AND crawl_session <> ?",
        (session_id,),
    ).fetchall()
    if not rows:
        return 0

    latest_other = max(rows, key=lambda r: str(_iso(r["crawled_at"]) or ""))["crawl_session"]
    skip = _normalize_scopes(incomplete_scopes)
    payload = []
    for row in rows:
        if row["crawl_session"] != latest_other:
            continue
        if (row["district"], row["property_type"], row["trade_type"]) in skip:
            continue
        try:
            tags = json.loads(row["tags"] or "[]") or []
        except (TypeError, ValueError):
            tags = []
        if not isinstance(tags, (list, tuple)):
            tags = []
        had_price_down = "가격인하" in tags or (row["price_cut_count"] or 0) > 0
        payload.append(
            (
                str(row["article_no"]), session_id, now, row["region"], row["district"],
                row["building_name"], row["property_type"], row["trade_type"], row["area"], row["floor"],
                row["price"], row["naver_url"], _iso(row["first_seen_at"]) or _iso(row["crawled_at"]),
                1 if is_bargain(row["bargain_discount"]) else 0, 1 if had_price_down else 0,
            )
        )
    for chunk in _chunks(payload):
        conn.executemany(
            "INSERT INTO listing_disappearances (article_no, session_id, disappeared_at, region, district, "
            "building_name, property_type, trade_type, area, floor, price, naver_url, first_seen_at, "
            "was_bargain, had_price_down) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) "
            "ON CONFLICT(article_no, session_id) DO NOTHING",
            chunk,
        )
    return len(payload)


def prune(conn, now: datetime) -> None:
    """90일 이상 안 보인 tracking/이벤트와 90일 지난 소멸 기록을 지운다."""
    cutoff = (now - timedelta(days=RETENTION_DAYS)).isoformat()
    conn.execute("DELETE FROM listing_tracking WHERE last_seen_at < ?", (cutoff,))
    conn.execute(
        "DELETE FROM listing_price_events WHERE NOT EXISTS "
        "(SELECT 1 FROM listing_tracking t WHERE t.article_no = listing_price_events.article_no)"
    )
    conn.execute("DELETE FROM listing_disappearances WHERE disappeared_at < ?", (cutoff,))


# ── 조회 ─────────────────────────────────────────────────────────────────────
def get_history(conn, article_no: str) -> Optional[Dict]:
    tracking = conn.execute(
        "SELECT * FROM listing_tracking WHERE article_no = ?", (str(article_no),)
    ).fetchone()
    if tracking is None:
        return None
    events = conn.execute(
        "SELECT observed_at, session_id, price, price_sort_value, prev_price, prev_price_sort_value, "
        "change_pct FROM listing_price_events WHERE article_no = ? ORDER BY observed_at ASC, id ASC",
        (str(article_no),),
    ).fetchall()
    listing = conn.execute(
        "SELECT article_no, region, district, building_name, property_type, trade_type, area, floor, price, "
        "naver_url, bargain_discount, bargain_basis FROM listings WHERE article_no = ?",
        (str(article_no),),
    ).fetchone()
    return {
        "tracking": dict(tracking),
        "events": [dict(e) for e in events],
        "listing": dict(listing) if listing else None,
    }


def disappearance_tag(row: Dict) -> str:
    if row.get("reappeared_at"):
        return "철회 후 재등록"
    if row.get("was_bargain"):
        return "진짜 급매였음"
    if row.get("had_price_down"):
        return "인하 후 소진"
    return "급매"


def list_disappeared(conn, since: str, limit: int) -> List[Dict]:
    rows = conn.execute(
        "SELECT article_no, disappeared_at, region, district, building_name, property_type, trade_type, "
        "area, floor, price, naver_url, first_seen_at, was_bargain, had_price_down, reappeared_at "
        "FROM listing_disappearances WHERE disappeared_at >= ? "
        "ORDER BY disappeared_at DESC, id DESC LIMIT ?",
        (since, limit),
    ).fetchall()
    return [dict(r) for r in rows]


FAST_COMPLEX_MIN_PREV = 3


def fast_complexes(conn, days: int, limit: int = 5) -> Dict:
    """crawl_building_stats로 days일 전 대비 매물 감소율 상위 단지.

    비교 기준 세션은 주간 리포트와 같은 규칙: 최신 라이브 성공 세션보다 days일(-12시간 허용)
    이전인 가장 최근 라이브 성공 세션, 없으면 최신과 다른 가장 오래된 라이브 성공 세션.
    """
    live = "status = 'success' AND COALESCE(source, 'naver') <> 'demo'"
    latest = conn.execute(
        f"SELECT session_id, crawled_at FROM crawl_history WHERE {live} ORDER BY crawled_at DESC LIMIT 1"
    ).fetchone()
    empty = {"days": days, "items": [], "latest_session": None, "previous_session": None}
    if latest is None:
        return empty
    latest_at = latest["crawled_at"]
    if not isinstance(latest_at, datetime):
        latest_at = datetime.fromisoformat(str(latest_at))
    cutoff = (latest_at - timedelta(days=days) + timedelta(hours=12)).isoformat()
    previous = conn.execute(
        f"SELECT session_id, crawled_at FROM crawl_history WHERE {live} AND session_id <> ? "
        "AND crawled_at <= ? ORDER BY crawled_at DESC LIMIT 1",
        (latest["session_id"], cutoff),
    ).fetchone()
    if previous is None:
        previous = conn.execute(
            f"SELECT session_id, crawled_at FROM crawl_history WHERE {live} AND session_id <> ? "
            "ORDER BY crawled_at ASC LIMIT 1",
            (latest["session_id"],),
        ).fetchone()
    if previous is None:
        return {**empty, "latest_session": latest["session_id"]}

    def counts(session_id):
        rows = conn.execute(
            "SELECT district, building_name, total_count FROM crawl_building_stats WHERE session_id = ?",
            (session_id,),
        ).fetchall()
        return {(r["district"], r["building_name"]): r["total_count"] or 0 for r in rows}

    cur, prev = counts(latest["session_id"]), counts(previous["session_id"])
    items = []
    if cur and prev:
        for key, prev_count in prev.items():
            count = cur.get(key, 0)
            # 2→0 같은 작은 단지가 100%로 상위를 채우지 않도록 직전 3건 이상만 본다
            if prev_count >= FAST_COMPLEX_MIN_PREV and count < prev_count:
                drop = prev_count - count
                items.append(
                    {
                        "district": key[0],
                        "building_name": key[1],
                        "prev_count": prev_count,
                        "count": count,
                        "drop": drop,
                        "drop_ratio": drop / prev_count,
                    }
                )
        items.sort(key=lambda i: (-i["drop_ratio"], -i["drop"], i["district"], i["building_name"]))
    return {
        "days": days,
        "items": items[:limit],
        "latest_session": latest["session_id"],
        "previous_session": previous["session_id"],
    }
