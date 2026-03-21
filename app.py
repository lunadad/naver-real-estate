import logging
import os
import json
import plistlib
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

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
KST = timezone(timedelta(hours=9), name="KST")
UTC = timezone.utc


BASE_DIR = Path(__file__).resolve().parent


def load_env_file(path: Path):
    if not path.exists() or not path.is_file():
        return

    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key:
            os.environ.setdefault(key, value)


def load_local_runtime_env():
    for name in (".env.local", ".env"):
        load_env_file(BASE_DIR / name)

    if os.getenv("DATABASE_URL", "").strip():
        return

    plist_candidates = [
        Path.home() / "Library/LaunchAgents/com.lunadad.naver-real-estate-crawl.plist",
        Path("/Library/LaunchDaemons/com.lunadad.naver-real-estate-crawl.plist"),
    ]

    for plist_path in plist_candidates:
        if not plist_path.exists():
            continue
        try:
            with plist_path.open("rb") as fp:
                payload = plistlib.load(fp)
        except Exception:
            continue

        env_vars = payload.get("EnvironmentVariables") or {}
        for key, value in env_vars.items():
            if isinstance(value, str):
                os.environ.setdefault(key, value)

        schedule = payload.get("StartCalendarInterval") or {}
        if isinstance(schedule, dict):
            if "Hour" in schedule:
                os.environ.setdefault("LOCAL_CRAWL_SCHEDULE_HOUR", str(schedule["Hour"]))
            if "Minute" in schedule:
                os.environ.setdefault("LOCAL_CRAWL_SCHEDULE_MINUTE", str(schedule["Minute"]))

        if os.getenv("DATABASE_URL", "").strip():
            os.environ.setdefault("ENABLE_SCHEDULER", "false")
            return

        args = payload.get("ProgramArguments") or []
        for index, arg in enumerate(args[:-1]):
            if arg == "--database-url" and isinstance(args[index + 1], str):
                os.environ.setdefault("DATABASE_URL", args[index + 1])
                os.environ.setdefault("ENABLE_SCHEDULER", "false")
                return


def get_naive_db_timezone():
    override = os.getenv("NAIVE_DB_TIMEZONE", "").strip().upper()
    if override == "UTC":
        return UTC
    if override == "KST":
        return KST
    return KST if os.getenv("DATABASE_URL", "").strip() else UTC


load_local_runtime_env()
NAIVE_DB_TZ = get_naive_db_timezone()


def env_flag(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def serialize_api_value(value):
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=NAIVE_DB_TZ)
        value = value.astimezone(KST)
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, dict):
        return {key: serialize_api_value(item) for key, item in value.items()}
    if isinstance(value, list):
        return [serialize_api_value(item) for item in value]
    return value

DEFAULT_DB_PATH = str(BASE_DIR / "real_estate.db")
DATABASE_URL = os.getenv("DATABASE_URL", "").strip()
DB_PATH = os.getenv("DB_PATH", DEFAULT_DB_PATH)
ENABLE_SCHEDULER = env_flag("ENABLE_SCHEDULER", True)
SEED_DEMO_DATA = env_flag("SEED_DEMO_DATA", False)
VAPID_PUBLIC_KEY = os.getenv("VAPID_PUBLIC_KEY", "").strip()
VAPID_PRIVATE_KEY = os.getenv("VAPID_PRIVATE_KEY", "").strip()
VAPID_SUBJECT = os.getenv("VAPID_SUBJECT", "mailto:alerts@example.com").strip()
EXTERNAL_CRAWL_HOUR = int((os.getenv("LOCAL_CRAWL_SCHEDULE_HOUR") or "9").strip())
EXTERNAL_CRAWL_MINUTE = int((os.getenv("LOCAL_CRAWL_SCHEDULE_MINUTE") or "0").strip())
EXTERNAL_CRAWL_GRACE_MINUTES = int((os.getenv("LOCAL_CRAWL_GRACE_MINUTES") or "120").strip())

app = Flask(__name__)
app.secret_key = os.getenv("SECRET_KEY", os.urandom(24))
CORS(app)

db = Database(db_path=DB_PATH, database_url=DATABASE_URL)
crawler = NaverRealEstateCrawler(db)

# ── Scheduler ───────────────────────────────────────────────────────────────
scheduler = BackgroundScheduler(timezone=KST)
SCHEDULED_HOUR = 9  # default: 9 AM KST


def scheduled_crawl():
    logger.info("⏰ 자동 크롤링 시작...")
    result = crawler.crawl_all()
    if result.get("status") == "success":
        dispatch_push_alerts()
    else:
        logger.warning("자동 크롤링이 %s 상태로 종료되어 푸시 전송을 생략합니다.", result.get("status"))
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
    if db.get_last_crawl():
        return

    if not SEED_DEMO_DATA:
        logger.info("초기 데이터 없음 (SEED_DEMO_DATA=false): 데모 데이터 시드 생략")
        return

    logger.warning("초기 데이터 없음 → 데모 데이터 로드 (운영환경에서는 비권장)")
    demo = crawler.generate_demo_data()
    import uuid as _uuid

    sid = str(_uuid.uuid4())[:8]
    db.insert_listings(demo, sid)
    db.log_crawl(sid, len(demo), len(demo), "degraded", "demo")
    logger.warning(f"데모 데이터 {len(demo)}개 로드 완료 [status=degraded, source=demo]")


ensure_initial_data()


def push_configured() -> bool:
    return bool(webpush and VAPID_PUBLIC_KEY and VAPID_PRIVATE_KEY)


def coerce_kst_datetime(value):
    if not value:
        return None
    if isinstance(value, datetime):
        dt = value
    elif isinstance(value, str):
        try:
            dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None
    else:
        return None

    if dt.tzinfo is None:
        return dt.replace(tzinfo=NAIVE_DB_TZ).astimezone(KST)
    return dt.astimezone(KST)


def get_next_external_crawl_time(now=None):
    now = now or datetime.now(KST)
    next_run = now.replace(
        hour=max(0, min(23, EXTERNAL_CRAWL_HOUR)),
        minute=max(0, min(59, EXTERNAL_CRAWL_MINUTE)),
        second=0,
        microsecond=0,
    )
    if next_run <= now:
        next_run = next_run + timedelta(days=1)
    return next_run


def get_external_schedule_state(last_attempt):
    now = datetime.now(KST)
    next_run = get_next_external_crawl_time(now)
    expected_last_run = next_run - timedelta(days=1)
    grace_until = expected_last_run + timedelta(
        minutes=max(0, EXTERNAL_CRAWL_GRACE_MINUTES)
    )

    last_attempt_at = coerce_kst_datetime((last_attempt or {}).get("crawled_at"))
    attempted_current_slot = bool(last_attempt_at and last_attempt_at >= expected_last_run)

    if attempted_current_slot:
        if (last_attempt or {}).get("status") == "success":
            status = "healthy"
            stale = False
            message = ""
        else:
            status = "failed"
            stale = True
            message = (
                f"오늘 {expected_last_run.strftime('%m/%d %H:%M')} 크롤링 실패로 "
                "이전 정상 데이터 표시 중"
            )
    elif now <= grace_until:
        status = "pending"
        stale = False
        message = (
            f"오늘 {expected_last_run.strftime('%m/%d %H:%M')} 크롤링 대기/진행 중"
        )
    else:
        status = "missed"
        stale = True
        message = (
            f"오늘 {expected_last_run.strftime('%m/%d %H:%M')} 크롤링 미실행으로 "
            "이전 정상 데이터 표시 중"
        )

    return {
        "mode": "external",
        "status": status,
        "stale": stale,
        "message": message,
        "expected_last_run": expected_last_run,
        "grace_until": grace_until,
        "next_run": next_run,
        "last_attempt_at": last_attempt_at,
    }


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
        sort_by=request.args.get("sort_by", "price-desc"),
        price_down_only=request.args.get("price_down_only", "false").lower() == "true",
    )
    return jsonify(serialize_api_value(result))


@app.route("/api/alert-rules", methods=["GET"])
def get_alert_rules():
    client_id = (request.args.get("client_id") or "").strip()
    if not client_id:
        return jsonify({"status": "error", "message": "client_id required"}), 400
    return jsonify(serialize_api_value({"rules": db.get_alert_rules(client_id)}))


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
    return jsonify(serialize_api_value({"status": "success", "rule": rule}))


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
    return jsonify(serialize_api_value({"status": "success", "matches": matches}))


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
    return jsonify(serialize_api_value(db.get_region_stats()))


@app.route("/api/trends")
def get_trends():
    return jsonify(serialize_api_value(db.get_trends()))


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
        run_status = result.get("status", "success")
        push_result = (
            dispatch_push_alerts()
            if run_status == "success"
            else {"clients": 0, "sent": 0, "matches": 0}
        )
        message_prefix = "✅" if run_status == "success" else ("⚠️" if run_status == "degraded" else "❌")
        return jsonify(
            {
                "status": run_status,
                "total": result["total"],
                "urgent": result["urgent"],
                "source": result["source"],
                "push": push_result,
                "message": f"{message_prefix} 급매 {result['total']}개 수집 ({run_status})",
                "crawled_at": datetime.now(KST).isoformat(),
            }
        )
    except Exception as e:
        logger.error(f"Crawl error: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route("/api/crawl-status")
def crawl_status():
    last = db.get_last_crawl(prefer_visible=True)
    last_attempt = db.get_last_crawl()
    job = scheduler.get_job("daily_crawl") if ENABLE_SCHEDULER else None
    schedule_state = None
    if job and job.next_run_time:
        next_run = job.next_run_time
        schedule_state = {
            "mode": "internal",
            "status": "healthy",
            "stale": False,
            "message": "",
            "expected_last_run": None,
            "grace_until": None,
            "next_run": next_run,
            "last_attempt_at": coerce_kst_datetime((last_attempt or {}).get("crawled_at")),
        }
    else:
        schedule_state = get_external_schedule_state(last_attempt)
        next_run = schedule_state["next_run"]
    return jsonify(
        serialize_api_value(
            {
                "last_crawl": last,
                "last_attempt": last_attempt,
                "next_crawl": next_run,
                "scheduled_hour": SCHEDULED_HOUR if ENABLE_SCHEDULER else EXTERNAL_CRAWL_HOUR,
                "schedule_state": schedule_state,
            }
        )
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
        serialize_api_value({"status": "success", "scheduled_hour": hour, "next_crawl": next_run})
    )


# ── Startup ──────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    port = int(os.getenv("PORT", "5101"))
    app.run(debug=False, port=port, host="0.0.0.0")
