"""관심 매물(찜) 상태 · 매물 비교 · 적정가 판단.

찜 목록은 브라우저(localStorage)에만 있고, 서버는 받은 article_no 목록의 현재 상태만 계산한다.
가격 단위는 listings.price_sort_value와 같은 만원이다.
"""

import json
import statistics
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Sequence

from bargain import area_number, is_bargain
from insights.dates import days_between, days_span, parse_naive
from insights.sql import placeholders

MAX_WATCH_ITEMS = 50
MAX_COMPARE_ITEMS = 3
MAX_WATCH_COMPLEXES = 20
NEW_BARGAIN_LIMIT = 10
MIN_PEERS = 3
SELL_DAYS_LOOKBACK = 90
M2_PER_PYEONG = 3.3058

LISTING_COLUMNS = (
    "article_no, region, district, property_type, trade_type, price, price_sort_value, area, floor, "
    "building_name, tags, naver_url, bargain_discount, first_seen_at, price_cut_count"
)


def _tags(raw) -> List[str]:
    if isinstance(raw, (list, tuple)):
        return list(raw)
    try:
        tags = json.loads(raw or "[]")
    except (TypeError, ValueError):
        return []
    return list(tags) if isinstance(tags, (list, tuple)) else []


def price_per_pyeong(price_value, area) -> Optional[int]:
    """3.3㎡(평)당 가격(만원)."""
    m2 = area_number(area)
    if not price_value or not m2:
        return None
    return round(price_value / (m2 / M2_PER_PYEONG))


def watch_change(item: Dict, saved_price_value) -> Dict:
    """찜할 때 가격 대비 지금 상태."""
    if item.get("status") == "gone":
        return {"kind": "gone"}
    current = item.get("price_sort_value")
    if item.get("status") != "active" or not saved_price_value or current is None or current == saved_price_value:
        return {"kind": "none"}
    diff = current - saved_price_value
    return {
        "kind": "price_down" if diff < 0 else "price_up",
        "diff": diff,
        "pct": round(diff / saved_price_value * 100, 1),
    }


def negotiation(days_listed, complex_avg_days, cut_count, sell_days) -> Dict:
    """인하 이력·노출 기간·비슷한 급매 소진 속도로 협상 여지를 단계로 요약."""
    signals = []
    if cut_count:
        signals.append(f"이미 {cut_count}번 가격을 내렸어요")
    if days_listed is not None and complex_avg_days is not None and days_listed >= complex_avg_days + 7:
        signals.append(f"단지 평균({complex_avg_days}일)보다 오래 걸려 있어요")
    if days_listed is not None and sell_days is not None and days_listed > sell_days:
        signals.append(f"비슷한 급매는 보통 {sell_days}일 안에 사라졌는데 아직 남아 있어요")
    level = "high" if len(signals) >= 2 else "some" if signals else "low"
    return {"level": level, "signals": signals}


def _listing_payload(row: Dict, cut_count, now: datetime) -> Dict:
    discount = row.get("bargain_discount")
    return {
        "article_no": str(row["article_no"]),
        "status": "active",
        "region": row.get("region"),
        "district": row.get("district"),
        "building_name": row.get("building_name"),
        "property_type": row.get("property_type"),
        "trade_type": row.get("trade_type"),
        "area": row.get("area"),
        "floor": row.get("floor"),
        "price": row.get("price"),
        "price_sort_value": row.get("price_sort_value"),
        "naver_url": row.get("naver_url"),
        "tags": _tags(row.get("tags")),
        "is_bargain": is_bargain(discount),
        "median_gap_pct": round(-discount * 100, 1) if discount is not None else None,
        "price_per_pyeong": price_per_pyeong(row.get("price_sort_value"), row.get("area")),
        "first_seen_at": row.get("first_seen_at"),
        "days_listed": days_between(row.get("first_seen_at"), now),
        "cut_count": int(cut_count or 0),
    }


def lookup(conn, session_id: Optional[str], article_nos: Sequence[str], now: datetime) -> Dict[str, Dict]:
    """article_no → 현재 상태. active(최신 세션에 있음) / gone(사라짐 기록) / unknown."""
    nos = list(dict.fromkeys(str(no) for no in article_nos if str(no or "").strip()))
    if not nos:
        return {}
    rows = {}
    if session_id:
        for row in conn.execute(
            f"SELECT {LISTING_COLUMNS} FROM listings WHERE crawl_session = ? "
            f"AND article_no IN ({placeholders(len(nos))})",
            [session_id] + nos,
        ).fetchall():
            rows[str(row["article_no"])] = dict(row)
    cuts = {
        str(row["article_no"]): row["price_cut_count"]
        for row in conn.execute(
            f"SELECT article_no, price_cut_count FROM listing_tracking WHERE article_no IN ({placeholders(len(nos))})",
            nos,
        ).fetchall()
    }
    missing = [no for no in nos if no not in rows]
    gone = {}
    if missing:
        for row in conn.execute(
            "SELECT article_no, disappeared_at, region, district, building_name, property_type, trade_type, "
            "area, floor, price, naver_url, first_seen_at FROM listing_disappearances "
            f"WHERE reappeared_at IS NULL AND article_no IN ({placeholders(len(missing))}) "
            "ORDER BY disappeared_at ASC, id ASC",
            missing,
        ).fetchall():
            gone[str(row["article_no"])] = dict(row)  # 같은 매물은 가장 최근 기록이 남는다

    result = {}
    for no in nos:
        if no in rows:
            result[no] = _listing_payload(rows[no], cuts.get(no, rows[no].get("price_cut_count")), now)
        elif no in gone:
            snap = gone[no]
            result[no] = {
                **{k: snap.get(k) for k in ("region", "district", "building_name", "property_type", "trade_type",
                                            "area", "floor", "price", "naver_url", "disappeared_at")},
                "article_no": no,
                "status": "gone",
                "days_listed": days_span(snap.get("first_seen_at"), snap.get("disappeared_at")),
                "cut_count": int(cuts.get(no) or 0),
            }
        else:
            result[no] = {"article_no": no, "status": "unknown"}
    return result


def new_bargains(conn, session_id: Optional[str], watched: Sequence[Dict], limit: int = NEW_BARGAIN_LIMIT) -> List[Dict]:
    """찜한 매물의 단지에서 찜한 뒤 새로 나온 진짜 급매."""
    if not session_id:
        return []
    since_by_complex: Dict[tuple, str] = {}
    watched_nos = set()
    for item in watched:
        watched_nos.add(str(item.get("article_no")))
        key = (item.get("district"), item.get("building_name"))
        if not key[0] or not key[1]:
            continue
        saved_at = str(item.get("saved_at") or "")
        if key not in since_by_complex or saved_at < since_by_complex[key]:
            since_by_complex[key] = saved_at
    found = []
    for (district, building), since in list(since_by_complex.items())[:MAX_WATCH_COMPLEXES]:
        for row in conn.execute(
            f"SELECT {LISTING_COLUMNS} FROM listings WHERE crawl_session = ? AND district = ? AND building_name = ?",
            (session_id, district, building),
        ).fetchall():
            row = dict(row)
            if str(row["article_no"]) in watched_nos or not is_bargain(row.get("bargain_discount")):
                continue
            first_seen = parse_naive(row.get("first_seen_at"))
            since_dt = parse_naive(since)
            if first_seen is None or (since_dt is not None and first_seen <= since_dt):
                continue
            found.append(_listing_payload(row, row.get("price_cut_count"), datetime.now()))
    found.sort(key=lambda item: item.get("first_seen_at") or "", reverse=True)
    return found[:limit]


def bargain_sell_days(conn, district, building_name, now: datetime) -> Optional[Dict]:
    """같은 단지(부족하면 같은 구)에서 진짜 급매가 처음 발견부터 사라지기까지 걸린 평균 일수."""
    since = (now - timedelta(days=SELL_DAYS_LOOKBACK)).isoformat()
    scopes = (
        ("complex", "district = ? AND building_name = ?", (district, building_name), 2),
        ("district", "district = ?", (district,), 3),
    )
    for scope, where, params, minimum in scopes:
        rows = conn.execute(
            "SELECT first_seen_at, disappeared_at FROM listing_disappearances "
            f"WHERE was_bargain = 1 AND reappeared_at IS NULL AND disappeared_at >= ? AND {where}",
            (since,) + params,
        ).fetchall()
        spans = (days_span(row["first_seen_at"], row["disappeared_at"]) for row in rows)
        days = [span for span in spans if span is not None]
        if len(days) >= minimum:
            return {"days": round(sum(days) / len(days)), "scope": scope, "count": len(days)}
    return None


def fair_price(conn, session_id: Optional[str], article_no: str, now: datetime) -> Optional[Dict]:
    """같은 단지·같은 평형(부족하면 단지 전체) 매매 매물 사이에서 이 매물의 평당가 위치."""
    target = None
    if session_id:
        target = conn.execute(
            f"SELECT {LISTING_COLUMNS} FROM listings WHERE crawl_session = ? AND article_no = ?",
            (session_id, str(article_no)),
        ).fetchone()
    if target is None:
        return None
    target = dict(target)
    target_value = price_per_pyeong(target.get("price_sort_value"), target.get("area"))
    if target.get("trade_type") != "매매" or target_value is None or not target.get("building_name"):
        return {"available": False, "reason": "매매 아파트·오피스텔만 비교할 수 있어요"}

    complex_rows = [
        dict(row)
        for row in conn.execute(
            f"SELECT {LISTING_COLUMNS} FROM listings WHERE crawl_session = ? AND district = ? "
            "AND building_name = ? AND trade_type = '매매' AND price_sort_value > 0",
            (session_id, target.get("district"), target.get("building_name")),
        ).fetchall()
    ]
    same_area = [row for row in complex_rows if str(row.get("area") or "").strip() == str(target.get("area") or "").strip()]
    if len(same_area) >= MIN_PEERS:
        basis, peers = "same_area", same_area
    elif len(complex_rows) >= MIN_PEERS:
        basis, peers = "complex", complex_rows
    else:
        return {"available": False, "reason": "같은 단지에 비교할 매물이 부족해요"}

    points = []
    for row in peers:
        value = price_per_pyeong(row.get("price_sort_value"), row.get("area"))
        if value is not None:
            points.append({"value": value, "is_self": str(row["article_no"]) == str(article_no)})
    points.sort(key=lambda p: (p["value"], not p["is_self"]))
    values = [p["value"] for p in points]
    median = round(statistics.median(values))

    listed_days = [d for d in (days_between(row.get("first_seen_at"), now) for row in complex_rows) if d is not None]
    complex_avg_days = round(sum(listed_days) / len(listed_days)) if listed_days else None
    tracking = conn.execute(
        "SELECT price_cut_count FROM listing_tracking WHERE article_no = ?", (str(article_no),)
    ).fetchone()
    cut_count = int((tracking["price_cut_count"] if tracking else target.get("price_cut_count")) or 0)
    days_listed = days_between(target.get("first_seen_at"), now)
    sell = bargain_sell_days(conn, target.get("district"), target.get("building_name"), now)

    return {
        "available": True,
        "basis": basis,
        "area": target.get("area"),
        "count": len(values),
        "rank": 1 + sum(1 for v in values if v < target_value),
        "value": target_value,
        "median": median,
        "gap_pct": round((target_value - median) / median * 100, 1) if median else None,
        "min": values[0],
        "max": values[-1],
        "points": points,
        "cut_count": cut_count,
        "days_listed": days_listed,
        "complex_avg_days": complex_avg_days,
        "sell_days": sell,
        "negotiation": negotiation(days_listed, complex_avg_days, cut_count, sell["days"] if sell else None),
    }
