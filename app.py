import logging
import os
from datetime import datetime

from apscheduler.schedulers.background import BackgroundScheduler
from flask import Flask, jsonify, render_template, request
from flask_cors import CORS

from crawler import NaverRealEstateCrawler
from database import Database

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

app = Flask(__name__)
CORS(app)

db = Database(db_path=os.path.join(os.path.dirname(__file__), "real_estate.db"))
crawler = NaverRealEstateCrawler(db)

# ── Scheduler ───────────────────────────────────────────────────────────────
scheduler = BackgroundScheduler(timezone="Asia/Seoul")
SCHEDULED_HOUR = 9  # default: 9 AM KST


def scheduled_crawl():
    logger.info("⏰ 자동 크롤링 시작...")
    crawler.crawl_all()


scheduler.add_job(
    scheduled_crawl,
    trigger="cron",
    hour=SCHEDULED_HOUR,
    minute=0,
    id="daily_crawl",
    replace_existing=True,
)
scheduler.start()


# ── Routes ───────────────────────────────────────────────────────────────────
@app.route("/")
def index():
    return render_template("index.html")


@app.route("/api/listings")
def get_listings():
    result = db.get_listings(
        region=request.args.get("region", ""),
        district=request.args.get("district", ""),
        property_type=request.args.get("property_type", ""),
        trade_type=request.args.get("trade_type", ""),
        urgent_only=request.args.get("urgent_only", "false").lower() == "true",
        search=request.args.get("search", ""),
        page=int(request.args.get("page", 1)),
        per_page=int(request.args.get("per_page", 20)),
        sort_by=request.args.get("sort_by", "urgent"),
        price_down_only=request.args.get("price_down_only", "false").lower() == "true",
    )
    return jsonify(result)


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
        return jsonify(
            {
                "status": "success",
                "total": result["total"],
                "urgent": result["urgent"],
                "source": result["source"],
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
    job = scheduler.get_job("daily_crawl")
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
    if not db.get_last_crawl():
        logger.info("초기 데이터 없음 → 데모 데이터 로드 후 서버 시작")
        demo = crawler.generate_demo_data()
        import uuid as _uuid
        sid = str(_uuid.uuid4())[:8]
        db.insert_listings(demo, sid)
        db.log_crawl(sid, len(demo), len(demo), "success", "demo")
        logger.info(f"데모 데이터 {len(demo)}개 로드 완료")
    app.run(debug=False, port=5101, host="0.0.0.0")
