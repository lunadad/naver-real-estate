"""관심 매물(찜) 페이지 · 매물 비교 · 적정가 판단 라우트."""

from flask import Blueprint, current_app, jsonify, render_template, request

from insights import watchlist as store
from views.tracking import _naive_now

bp = Blueprint("watchlist", __name__)


def _db():
    return current_app.config["GET_DB"]()


def _error(message, status_code):
    return jsonify({"status": "error", "message": message}), status_code


def _price_value(raw):
    try:
        value = int(raw)
    except (TypeError, ValueError):
        return None
    return value if value > 0 else None


@bp.route("/watchlist")
def watchlist_page():
    return render_template("watchlist.html")


@bp.route("/api/watchlist/status", methods=["POST"])
def watchlist_status():
    body = request.get_json(silent=True) or {}
    raw_items = body.get("items")
    if not isinstance(raw_items, list):
        return _error("items must be a list", 400)
    saved = []
    for raw in raw_items:
        if isinstance(raw, dict) and str(raw.get("article_no") or "").strip():
            saved.append(raw)
        if len(saved) >= store.MAX_WATCH_ITEMS:
            break

    db = _db()
    now = _naive_now()
    with db.get_connection() as conn:
        session_id = db._get_latest_visible_session_id(conn)
        current = store.lookup(conn, session_id, [item["article_no"] for item in saved], now)
        items, complexes = [], []
        for raw in saved:
            item = dict(current.get(str(raw["article_no"]).strip(), {"article_no": str(raw["article_no"]), "status": "unknown"}))
            item["saved_at"] = raw.get("saved_at")
            item["change"] = store.watch_change(item, _price_value(raw.get("saved_price_value")))
            items.append(item)
            complexes.append(
                {
                    "article_no": item["article_no"],
                    "saved_at": raw.get("saved_at"),
                    "district": item.get("district") or raw.get("district"),
                    "building_name": item.get("building_name") or raw.get("building_name"),
                }
            )
        bargains = store.new_bargains(conn, session_id, complexes)
    return jsonify({"items": items, "new_bargains": bargains})


@bp.route("/api/listings/compare")
def compare_listings():
    ids = [part.strip() for part in (request.args.get("ids") or "").split(",") if part.strip()]
    ids = list(dict.fromkeys(ids))[: store.MAX_COMPARE_ITEMS]
    if not ids:
        return _error("ids required", 400)
    db = _db()
    with db.get_connection() as conn:
        current = store.lookup(conn, db._get_latest_visible_session_id(conn), ids, _naive_now())
    return jsonify({"items": [current[no] for no in ids if no in current]})


@bp.route("/api/listings/<article_no>/fair-price")
def listing_fair_price(article_no):
    db = _db()
    with db.get_connection() as conn:
        result = store.fair_price(conn, db._get_latest_visible_session_id(conn), article_no, _naive_now())
    if result is None:
        return _error("listing not found", 404)
    return jsonify(result)
