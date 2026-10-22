"""가격 이력(②)·사라진 매물(③) 라우트."""

import os
from datetime import datetime, timedelta, timezone

from flask import Blueprint, current_app, jsonify, render_template, request

from insights import tracking as store
from insights.price_history import build_insight

bp = Blueprint("tracking", __name__)

ALLOWED_DAYS = (1, 7, 30)
KST = timezone(timedelta(hours=9))


def _db():
    return current_app.config["GET_DB"]()


def _naive_now() -> datetime:
    """크롤러가 저장하는 naive 시각(datetime.now())과 같은 기준의 현재 시각.

    운영(DATABASE_URL 설정)은 맥미니 크롤러가 KST로 저장하므로 서버 시간대와 무관하게 KST를 쓴다.
    """
    if os.getenv("DATABASE_URL", "").strip():
        return datetime.now(KST).replace(tzinfo=None)
    return datetime.now()


def _normalize_days(raw, default=7) -> int:
    try:
        value = int(raw)
    except (TypeError, ValueError):
        return default
    for allowed in ALLOWED_DAYS:
        if value <= allowed:
            return allowed
    return ALLOWED_DAYS[-1]


def _parse(value):
    try:
        return datetime.fromisoformat(str(value)).replace(tzinfo=None)
    except (TypeError, ValueError):
        return None


@bp.route("/api/listings/<article_no>/history")
def listing_history(article_no):
    with _db().get_connection() as conn:
        history = store.get_history(conn, article_no)
    if history is None:
        return jsonify({"status": "error", "message": "history not found"}), 404
    listing = history["listing"] or {}
    insight = build_insight(
        history["tracking"], history["events"], listing.get("bargain_discount"), now=_naive_now()
    )
    return jsonify(
        {
            "article_no": str(article_no),
            "listing": listing or None,
            "tracking": history["tracking"],
            "events": history["events"],
            "insight": insight,
        }
    )


@bp.route("/disappeared")
def disappeared_page():
    return render_template("disappeared.html")


@bp.route("/api/disappeared")
def disappeared_api():
    days = _normalize_days(request.args.get("days"))
    try:
        limit = min(500, max(1, int(request.args.get("limit", 100))))
    except (TypeError, ValueError):
        limit = 100
    since = (_naive_now() - timedelta(days=days)).isoformat()
    with _db().get_connection() as conn:
        rows = store.list_disappeared(conn, since, limit)
        total = conn.execute(
            "SELECT COUNT(*) AS c FROM listing_disappearances WHERE disappeared_at >= ?", (since,)
        ).fetchone()["c"]
        bargain_total = conn.execute(
            "SELECT COUNT(*) AS c FROM listing_disappearances WHERE disappeared_at >= ? AND was_bargain = 1",
            (since,),
        ).fetchone()["c"]
    items = []
    for row in rows:
        first_seen, gone = _parse(row.get("first_seen_at")), _parse(row.get("disappeared_at"))
        row["days_listed"] = max(0, (gone - first_seen).days) if first_seen and gone else None
        row["tag"] = store.disappearance_tag(row)
        row["was_bargain"] = bool(row["was_bargain"])
        row["had_price_down"] = bool(row["had_price_down"])
        items.append(row)
    return jsonify(
        {"days": days, "count": len(items), "total": total, "bargain_total": bargain_total, "items": items}
    )


@bp.route("/api/disappeared/fast-complexes")
def fast_complexes_api():
    days = _normalize_days(request.args.get("days"))
    with _db().get_connection() as conn:
        return jsonify(store.fast_complexes(conn, days))
