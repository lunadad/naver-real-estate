import logging
import os
import json
from datetime import datetime

from apscheduler.schedulers.background import BackgroundScheduler
from flask import Flask, jsonify, render_template, request, send_from_directory
from flask_cors import CORS

try:
    from pywebpush import WebPushException, webpush
except ImportError:  # pragma: no cover - optional until dependency is installed
    WebPushException = Exception
    webpush = None

from crawler import NaverRealEstateCrawler
from database import Database

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)


def env_flag(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


BASE_DIR = os.path.dirname(__file__)
DEFAULT_DB_PATH = os.path.join(BASE_DIR, "real_estate.db")
DATABASE_URL = os.getenv("DATABASE_URL", "").strip()
DB_PATH = os.getenv("DB_PATH", DEFAULT_DB_PATH)
ENABLE_SCHEDULER = env_flag("ENABLE_SCHEDULER", True)
SEED_DEMO_DATA = env_flag("SEED_DEMO_DATA", True)
VAPID_PUBLIC_KEY = os.getenv("VAPID_PUBLIC_KEY", "").strip()
VAPID_PRIVATE_KEY = os.getenv("VAPID_PRIVATE_KEY", "").strip()
VAPID_SUBJECT = os.getenv("VAPID_SUBJECT", "mailto:alerts@example.com").strip()

app = Flask(__name__)
app.secret_key = os.getenv("SECRET_KEY", os.urandom(24))
CORS(app)

db = Database(db_path=DB_PATH, database_url=DATABASE_URL)
crawler = NaverRealEstateCrawler(db)

# ── Scheduler ───────────────────────────────────────────────────────────────
scheduler = BackgroundScheduler(timezone="Asia/Seoul")
SCHEDULED_HOUR = 9  # default: 9 AM KST


def scheduled_crawl():
    logger.info("⏰ 자동 크롤링 시작...")
    result = crawler.crawl_all()
    dispatch_push_alerts()
    return result


scheduler.add_job(
    scheduled_crawl,
    trigger="cron",
    hour=SCHEDULED_HOUR,
    minute=0,
    id="daily_crawl",
    replace_existing=True,
)
if ENABLE_SCHEDULER:
    scheduler.start()
else:
    logger.info("자동 스케줄러 비활성화됨 (ENABLE_SCHEDULER=false)")


def ensure_initial_data():
    if db.get_last_crawl() or not SEED_DEMO_DATA:
        return

    logger.info("초기 데이터 없음 → 데모 데이터 로드")
    demo = crawler.generate_demo_data()
    import uuid as _uuid

    sid = str(_uuid.uuid4())[:8]
    db.insert_listings(demo, sid)
    db.log_crawl(sid, len(demo), len(demo), "success", "demo")
    logger.info(f"데모 데이터 {len(demo)}개 로드 완료")


ensure_initial_data()


def push_configured() -> bool:
    return bool(webpush and VAPID_PUBLIC_KEY and VAPID_PRIVATE_KEY)


def build_push_payload(matches):
    first = matches[0]
    extra_count = max(0, len(matches) - 1)
    app_name = "부동산 급매 알리미"
    label = ", ".join(first.get("alert_names") or []) or "새 급매"
    location = " ".join(
        part
        for part in [first.get("region", "").strip(), first.get("district", "").strip()]
        if part
    ).strip()
    first_line = f"[{first.get('property_type', '-')}/{first.get('trade_type', '-')}] {first.get('building_name', '매물')} {first.get('price', '')}".strip()
    body_parts = [label, first_line]
    if location:
        body_parts.append(location)
    if extra_count:
        body_parts.append(f"외 {extra_count}건")

    return {
        "title": app_name,
        "body": " · ".join(part for part in body_parts if part),
        "tag": f"alert-batch-{first.get('article_no')}",
        "url": first.get("naver_url") or "/",
        "data": {
            "article_no": first.get("article_no"),
            "count": len(matches),
            "alert_names": first.get("alert_names") or [],
        },
    }


def send_push_notification(subscription, payload):
    return webpush(
        subscription_info=subscription,
        data=json.dumps(payload, ensure_ascii=False),
        vapid_private_key=VAPID_PRIVATE_KEY,
        vapid_claims={"sub": VAPID_SUBJECT},
    )


def dispatch_push_alerts(limit: int = 5):
    if not push_configured():
        logger.info("Web Push 미설정으로 모바일 푸시 전송 생략")
        return {"clients": 0, "sent": 0, "matches": 0}

    sent = 0
    matched_clients = 0
    matched_articles = 0

    for client_id in db.get_push_client_ids():
        matches = db.get_pending_alert_matches(client_id, limit=limit)
        if not matches:
            continue

        subscriptions = [
            item.get("subscription")
            for item in db.get_push_subscriptions(client_id)
            if item.get("subscription")
        ]
        if not subscriptions:
            continue

        payload = build_push_payload(matches)
        delivered = False
        for subscription in subscriptions:
            endpoint = (subscription or {}).get("endpoint", "")
            try:
                send_push_notification(subscription, payload)
                delivered = True
                sent += 1
                if endpoint:
                    db.touch_push_subscription_success(endpoint)
            except WebPushException as exc:
                status_code = getattr(getattr(exc, "response", None), "status_code", None)
                if status_code in {404, 410} and endpoint:
                    db.delete_push_subscription_by_endpoint(endpoint)
                logger.warning("Push delivery failed for client %s: %s", client_id, exc)
            except Exception as exc:  # pragma: no cover - network/runtime failure
                logger.warning("Push delivery failed for client %s: %s", client_id, exc)

        if delivered:
            db.mark_alert_matches_delivered(matches)
            matched_clients += 1
            matched_articles += len(matches)

    logger.info(
        "모바일 푸시 전송 완료: clients=%s sent=%s matches=%s",
        matched_clients,
        sent,
        matched_articles,
    )
    return {"clients": matched_clients, "sent": sent, "matches": matched_articles}


# ── Routes ───────────────────────────────────────────────────────────────────
@app.route("/")
def index():
    return render_template("index.html")


@app.route("/manifest.json")
def manifest():
    response = send_from_directory(
        os.path.join(app.root_path, "static"),
        "manifest.json",
        mimetype="application/manifest+json",
    )
    response.headers["Cache-Control"] = "no-cache"
    return response


@app.route("/sw.js")
def service_worker():
    response = send_from_directory(
        os.path.join(app.root_path, "static"),
        "sw.js",
        mimetype="application/javascript",
    )
    response.headers["Cache-Control"] = "no-cache"
    return response


@app.route("/api/listings")
def get_listings():
    try:
        page = max(1, int(request.args.get("page", 1)))
        per_page = min(100, max(1, int(request.args.get("per_page", 20))))
    except (ValueError, TypeError):
        page, per_page = 1, 20

    result = db.get_listings(
        region=request.args.get("region", ""),
        district=request.args.get("district", ""),
        property_type=request.args.get("property_type", ""),
        trade_type=request.args.get("trade_type", ""),
        urgent_only=request.args.get("urgent_only", "false").lower() == "true",
        search=request.args.get("search", ""),
        page=page,
        per_page=per_page,
        sort_by=request.args.get("sort_by", "urgent"),
        price_down_only=request.args.get("price_down_only", "false").lower() == "true",
    )
    return jsonify(result)


@app.route("/api/alert-rules", methods=["GET"])
def get_alert_rules():
    client_id = (request.args.get("client_id") or "").strip()
    if not client_id:
        return jsonify({"status": "error", "message": "client_id required"}), 400
    return jsonify({"rules": db.get_alert_rules(client_id)})


@app.route("/api/alert-rules", methods=["POST"])
def create_alert_rule():
    data = request.get_json() or {}
    client_id = (data.get("client_id") or "").strip()
    if not client_id:
        return jsonify({"status": "error", "message": "client_id required"}), 400

    if not any(
        [
            (data.get("keyword") or "").strip(),
            (data.get("district") or "").strip(),
            (data.get("property_type") or "").strip(),
            (data.get("trade_type") or "").strip(),
        ]
    ):
        return jsonify({"status": "error", "message": "at least one filter required"}), 400

    rule = db.create_alert_rule(
        client_id=client_id,
        keyword=data.get("keyword", ""),
        district=data.get("district", ""),
        property_type=data.get("property_type", ""),
        trade_type=data.get("trade_type", ""),
        name=data.get("name", ""),
    )
    return jsonify({"status": "success", "rule": rule})


@app.route("/api/alert-rules/<int:alert_id>", methods=["DELETE"])
def delete_alert_rule(alert_id: int):
    client_id = (request.args.get("client_id") or "").strip()
    if not client_id:
        return jsonify({"status": "error", "message": "client_id required"}), 400

    deleted = db.delete_alert_rule(client_id, alert_id)
    if not deleted:
        return jsonify({"status": "error", "message": "alert not found"}), 404

    return jsonify({"status": "success"})


@app.route("/api/alerts/check")
def check_alerts():
    client_id = (request.args.get("client_id") or "").strip()
    if not client_id:
        return jsonify({"status": "error", "message": "client_id required"}), 400

    matches = db.get_new_alert_matches(client_id)
    return jsonify({"status": "success", "matches": matches})


@app.route("/api/push/public-key")
def get_push_public_key():
    return jsonify(
        {
            "configured": push_configured(),
            "public_key": VAPID_PUBLIC_KEY if push_configured() else "",
        }
    )


@app.route("/api/push/subscribe", methods=["POST"])
def subscribe_push():
    data = request.get_json() or {}
    client_id = (data.get("client_id") or "").strip()
    subscription = data.get("subscription") or {}

    if not client_id:
        return jsonify({"status": "error", "message": "client_id required"}), 400
    if not push_configured():
        return jsonify({"status": "error", "message": "push not configured"}), 400

    try:
        db.save_push_subscription(client_id, subscription)
    except ValueError as exc:
        return jsonify({"status": "error", "message": str(exc)}), 400

    return jsonify({"status": "success"})


@app.route("/api/push/subscribe", methods=["DELETE"])
def unsubscribe_push():
    data = request.get_json(silent=True) or {}
    client_id = (data.get("client_id") or request.args.get("client_id") or "").strip()
    endpoint = (data.get("endpoint") or request.args.get("endpoint") or "").strip()

    if not client_id:
        return jsonify({"status": "error", "message": "client_id required"}), 400

    deleted = db.delete_push_subscription(client_id, endpoint)
    return jsonify({"status": "success", "deleted": deleted})


@app.route("/api/region-stats")
def get_region_stats():
    return jsonify(db.get_region_stats())


@app.route("/api/trends")
def get_trends():
    return jsonify(db.get_trends())


@app.route("/api/regions")
def get_regions():
    regions = []
    for name, info in crawler.REGIONS.items():
        entry = {
            "name": name,
            "lat": info["lat"],
            "lng": info["lng"],
            "districts": [
                {"name": dn, "lat": di["lat"], "lng": di["lng"]}
                for dn, di in info["districts"].items()
            ],
        }
        regions.append(entry)
    return jsonify(regions)


@app.route("/api/crawl", methods=["POST"])
def trigger_crawl():
    try:
        result = crawler.crawl_all()
        push_result = dispatch_push_alerts()
        return jsonify(
            {
                "status": "success",
                "total": result["total"],
                "urgent": result["urgent"],
                "source": result["source"],
                "push": push_result,
                "message": f"✅ 급매 {result['total']}개 수집 완료",
                "crawled_at": datetime.now().isoformat(),
            }
        )
    except Exception as e:
        logger.error(f"Crawl error: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route("/api/crawl-status")
def crawl_status():
    last = db.get_last_crawl()
    job = scheduler.get_job("daily_crawl") if ENABLE_SCHEDULER else None
    next_run = job.next_run_time.isoformat() if job and job.next_run_time else None
    return jsonify(
        {
            "last_crawl": last,
            "next_crawl": next_run,
            "scheduled_hour": SCHEDULED_HOUR,
        }
    )


@app.route("/api/update-schedule", methods=["POST"])
def update_schedule():
    global SCHEDULED_HOUR
    if not ENABLE_SCHEDULER:
        return jsonify({"status": "error", "message": "scheduler disabled"}), 400

    data = request.get_json() or {}
    hour = int(data.get("hour", SCHEDULED_HOUR))
    hour = max(0, min(23, hour))
    SCHEDULED_HOUR = hour
    scheduler.reschedule_job("daily_crawl", trigger="cron", hour=hour, minute=0)
    job = scheduler.get_job("daily_crawl")
    next_run = job.next_run_time.isoformat() if job and job.next_run_time else None
    return jsonify(
        {"status": "success", "scheduled_hour": hour, "next_crawl": next_run}
    )


# ── Startup ──────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    port = int(os.getenv("PORT", "5101"))
    app.run(debug=False, port=port, host="0.0.0.0")
