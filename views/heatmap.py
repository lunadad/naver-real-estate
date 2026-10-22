"""지역 히트맵: GET /heatmap (HTML), GET /api/heatmap (JSON)."""

from flask import Blueprint, render_template, request

from insights.heatmap import DEFAULT_REGION, METRIC_DEFS, build_districts
from insights.sessions import get_latest_session, get_week_ago_session

bp = Blueprint("heatmap_view", __name__)
_ctx = None


def register(app, ctx):
    global _ctx
    _ctx = ctx
    app.register_blueprint(bp)


@bp.route("/heatmap")
def heatmap_page():
    return render_template("heatmap.html")


def _stats_by_district(conn, session_id, region):
    rows = conn.execute(
        "SELECT district, total_count, price_down_count FROM crawl_region_stats "
        "WHERE session_id = ? AND region = ?",
        (session_id, region),
    ).fetchall()
    return {row["district"]: dict(row) for row in rows}


@bp.route("/api/heatmap")
def heatmap_api():
    regions = _ctx.load_regions()
    region = (request.args.get("region") or "").strip() or DEFAULT_REGION
    if region not in regions:
        return _ctx.error_response("unknown region", 404)

    db = _ctx.db
    listing_rows, stats_now, stats_prev = [], {}, None
    with db.get_connection() as conn:
        session_id, crawled_at = get_latest_session(db, conn)
        if session_id:
            listing_rows = [
                dict(row)
                for row in conn.execute(
                    "SELECT district, tags, bargain_discount FROM listings "
                    "WHERE crawl_session = ? AND region = ?",
                    (session_id, region),
                ).fetchall()
            ]
            stats_now = _stats_by_district(conn, session_id, region)
            week_session = get_week_ago_session(conn, session_id, crawled_at)
            if week_session:
                stats_prev = _stats_by_district(conn, week_session, region)

    payload = {
        "region": region,
        "regions": list(regions),
        "metric_defs": METRIC_DEFS,
        "districts": build_districts(
            list(regions[region]["districts"]), listing_rows, stats_now, stats_prev
        ),
    }
    return _ctx.cacheable_json(_ctx.serialize_api_value(payload), max_age=300)
