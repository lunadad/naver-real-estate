"""가격 이력(②)·사라진 매물(③) 라우트."""

from datetime import timedelta

from flask import Blueprint, jsonify, render_template, request

from insights import tracking as store
from insights.dates import days_span
from insights.price_history import build_insight
from views import common
from views.common import error_response, get_db, naive_now

bp = Blueprint("tracking", __name__)

ALLOWED_DAYS = (1, 7, 30)
KST = common.KST  # 기존 공개 이름 유지


def _normalize_days(raw, default=7) -> int:
    try:
        value = int(raw)
    except (TypeError, ValueError):
        return default
    for allowed in ALLOWED_DAYS:
        if value <= allowed:
            return allowed
    return ALLOWED_DAYS[-1]


@bp.route("/api/listings/<article_no>/history")
def listing_history(article_no):
    with get_db().get_connection() as conn:
        history = store.get_history(conn, article_no)
    if history is None:
        return error_response("history not found", 404)
    listing = history["listing"] or {}
    insight = build_insight(
        history["tracking"], history["events"], listing.get("bargain_discount"), now=naive_now()
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
    since = (naive_now() - timedelta(days=days)).isoformat()
    with get_db().get_connection() as conn:
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
        row["days_listed"] = days_span(row.get("first_seen_at"), row.get("disappeared_at"))
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
    with get_db().get_connection() as conn:
        return jsonify(store.fast_complexes(conn, days))
