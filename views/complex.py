"""단지 상세 페이지: GET /complex (HTML), GET /api/complex (JSON)."""

from flask import Blueprint, jsonify, render_template, request

from insights.complex import build_areas, build_stats, order_listings
from insights.sessions import get_latest_session, get_week_ago_session

bp = Blueprint("complex_view", __name__)
_ctx = None  # app 모듈(런타임 db 교체를 따라가려고 모듈 자체를 들고 있는다)

LISTING_COLUMNS = (
    "article_no, region, district, property_type, trade_type, price, price_sort_value, area, floor, "
    "building_name, tags, confirmed_date, naver_url, bargain_discount, bargain_basis"
)


def register(app, ctx):
    global _ctx
    _ctx = ctx
    app.register_blueprint(bp)


@bp.route("/complex")
def complex_page():
    return render_template("complex.html")


@bp.route("/api/complex")
def complex_api():
    district = (request.args.get("district") or "").strip()
    building_name = (request.args.get("building_name") or "").strip()
    if not district or not building_name:
        return _ctx.error_response("district and building_name required", 400)

    db = _ctx.db
    with db.get_connection() as conn:
        session_id, crawled_at = get_latest_session(db, conn)
        rows = []
        if session_id:
            rows = [
                dict(row)
                for row in conn.execute(
                    f"SELECT {LISTING_COLUMNS} FROM listings "
                    "WHERE crawl_session = ? AND district = ? AND building_name = ?",
                    (session_id, district, building_name),
                ).fetchall()
            ]
        if not rows:
            return _ctx.error_response("complex not found", 404)

        week_ago_count = None
        week_session = get_week_ago_session(conn, session_id, crawled_at)
        if week_session:
            row = conn.execute(
                "SELECT total_count FROM crawl_building_stats "
                "WHERE session_id = ? AND district = ? AND building_name = ?",
                (week_session, district, building_name),
            ).fetchone()
            week_ago_count = int(row["total_count"] or 0) if row else 0

    payload = {
        "region": rows[0].get("region"),
        "district": district,
        "building_name": building_name,
        "stats": build_stats(rows, week_ago_count),
        "areas": build_areas(rows),
        "history": _ctx.build_building_history_series(district, building_name, 14),
        "listings": order_listings(rows),
    }
    return _ctx.cacheable_json(_ctx.serialize_api_value(payload), max_age=300)
