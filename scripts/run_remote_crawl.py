import argparse
import logging
import os
import sys
import time
from datetime import datetime
from pathlib import Path
from logging.handlers import RotatingFileHandler
from typing import List, Optional
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

ROOT_DIR = Path(__file__).resolve().parents[1]
LOGS_DIR = ROOT_DIR / "logs"
DEFAULT_LOG_PATH = LOGS_DIR / "run_remote_crawl.log"
DEFAULT_ALERT_STATE_PATH = LOGS_DIR / "crawl_alert_state.json"
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from crawler import NaverRealEstateCrawler
from database import Database
from notifier import AlertState, send_telegram

logger = logging.getLogger("run_remote_crawl")


def ensure_postgres_sslmode(database_url: str) -> str:
    if not database_url.startswith(("postgresql://", "postgres://")):
        return database_url

    parts = urlsplit(database_url)
    query = dict(parse_qsl(parts.query, keep_blank_values=True))
    query.setdefault("sslmode", "require")
    return urlunsplit((parts.scheme, parts.netloc, parts.path, urlencode(query), parts.fragment))


def configure_logging(log_path: str):
    LOGS_DIR.mkdir(exist_ok=True)
    target = Path(log_path).expanduser()
    target.parent.mkdir(parents=True, exist_ok=True)

    formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s")
    root = logging.getLogger()
    root.setLevel(logging.INFO)
    root.handlers.clear()

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    root.addHandler(stream_handler)

    file_handler = RotatingFileHandler(
        target,
        maxBytes=5 * 1024 * 1024,
        backupCount=5,
        encoding="utf-8",
    )
    file_handler.setFormatter(formatter)
    root.addHandler(file_handler)


def build_parser():
    parser = argparse.ArgumentParser(
        description="Run a live crawl from this machine and write results to the remote Postgres database."
    )
    parser.add_argument(
        "--database-url",
        default=os.getenv("DATABASE_URL", "").strip(),
        help="Neon or remote Postgres DATABASE_URL",
    )
    parser.add_argument(
        "--min-live-crawl-ratio",
        default=os.getenv("MIN_LIVE_CRAWL_RATIO", "0.5"),
        help="Fail the crawl if live listings drop below this ratio of the last successful live crawl.",
    )
    parser.add_argument(
        "--log-path",
        default=os.getenv("LOCAL_CRAWL_LOG_PATH", str(DEFAULT_LOG_PATH)),
        help="Local file path for crawl execution logs.",
    )
    parser.add_argument(
        "--skip-if-succeeded-today",
        action="store_true",
        help="Exit without crawling when today's live crawl already succeeded (retry slots).",
    )
    parser.add_argument(
        "--final-hour",
        type=int,
        default=16,
        help="Hour of the last daily slot; a failure at or after it sends a Telegram alert.",
    )
    parser.add_argument("--label", default="상업용", help="Prefix for Telegram alerts.")
    parser.add_argument(
        "--alert-state",
        default=str(DEFAULT_ALERT_STATE_PATH),
        help="Local JSON file remembering today's failures and unsent alerts.",
    )
    return parser


def decide_alerts(
    *,
    label: str,
    success: bool,
    now: datetime,
    final_hour: int,
    had_earlier_failure: bool,
    failed_combos: int,
    total_combos: int,
    error: Optional[str],
) -> List[str]:
    """이번 실행 결과로 보낼 텔레그램 메시지 목록을 정한다. 정상 성공이면 빈 목록."""
    clock = now.strftime("%H:%M")
    if not success:
        if now.hour < final_hour:
            return []
        cause = error or "status=failed"
        if not error and failed_combos:
            cause += f" (실패 조합 {failed_combos}/{total_combos})"
        return [
            f"[{label}] ❌ 오늘 크롤링 최종 실패 ({clock})\n원인: {cause}\n"
            "이전 정상 데이터가 계속 표시됩니다."
        ]

    lines = []
    if had_earlier_failure:
        lines.append(f"[{label}] ✅ 크롤링 복구 ({clock}) — 앞선 시도 실패 후 성공")
    if failed_combos:
        lines.append(f"[{label}] ⚠️ 일부 조합 수집 실패 {failed_combos}/{total_combos} — 저장은 완료")
    return ["\n".join(lines)] if lines else []


def _send_or_keep(state: AlertState, send, messages: List[str]):
    for text in messages:
        if not send(text):
            state.add_pending(text)


def run(args, *, now_fn=datetime.now, db_factory=None, crawler_factory=None, send=send_telegram) -> int:
    """크롤링 1회와 알림 처리. 반환값은 프로세스 종료 코드."""
    db_factory = db_factory or (
        lambda url: _connect_with_retry(url, skip_price_backfill=True)
    )
    crawler_factory = crawler_factory or NaverRealEstateCrawler
    state = AlertState(Path(args.alert_state))
    _send_or_keep(state, send, state.take_pending())

    today = now_fn().date()
    db = None
    result = {}
    error = None
    try:
        db = db_factory(args.database_url)
        if args.skip_if_succeeded_today and db.has_successful_live_crawl_on(today):
            logger.info("오늘(%s) 크롤링이 이미 성공해 이번 슬롯은 건너뜀", today.isoformat())
            return 0
        result = crawler_factory(db).crawl_all()
    except Exception as exc:
        error = f"{type(exc).__name__}: {exc}"
        logger.exception("Local remote crawl crashed")
    finally:
        if db is not None:
            db.close()

    status = "error" if error else result.get("status", "success")
    logger.info(
        "Local remote crawl finished: status=%s source=%s total=%s urgent=%s failed_combos=%s/%s",
        status,
        result.get("source"),
        result.get("total"),
        result.get("urgent"),
        result.get("failed_combos", 0),
        result.get("total_combos", 0),
    )

    success = status == "success"
    had_earlier_failure = state.had_failure(today)
    state.record_attempt(today, success)
    messages = decide_alerts(
        label=args.label,
        success=success,
        now=now_fn(),
        final_hour=args.final_hour,
        had_earlier_failure=had_earlier_failure,
        failed_combos=int(result.get("failed_combos") or 0),
        total_combos=int(result.get("total_combos") or 0),
        error=error,
    )
    _send_or_keep(state, send, messages)
    return 0 if success else 1


def _connect_with_retry(
    database_url: str,
    max_attempts: int = 5,
    delay: int = 30,
    skip_price_backfill: bool = False,
) -> "Database":
    last_exc: Exception = RuntimeError("no attempts made")
    for attempt in range(1, max_attempts + 1):
        try:
            return Database(
                database_url=database_url,
                skip_price_backfill=skip_price_backfill,
            )
        except Exception as exc:
            last_exc = exc
            if attempt >= max_attempts:
                break
            logger.warning(
                "DB connection attempt %d/%d failed: %s — retrying in %ds",
                attempt, max_attempts, exc, delay,
            )
            time.sleep(delay)
    raise last_exc


def main():
    parser = build_parser()
    args = parser.parse_args()
    configure_logging(args.log_path)

    database_url = ensure_postgres_sslmode(args.database_url)

    if not database_url:
        raise SystemExit("DATABASE_URL is required")

    os.environ["DATABASE_URL"] = database_url
    os.environ.setdefault("ALLOW_DEMO_FALLBACK", "false")
    os.environ.setdefault("SEED_DEMO_DATA", "false")
    os.environ["MIN_LIVE_CRAWL_RATIO"] = str(args.min_live_crawl_ratio)
    os.environ["LOCAL_CRAWL_LOG_PATH"] = str(args.log_path)

    print(f"[run_remote_crawl] python={sys.executable} pid={os.getpid()}", flush=True)
    logger.info("Local remote crawl starting (pid=%s)", os.getpid())
    logger.info("Local crawl log file: %s", Path(args.log_path).expanduser())

    args.database_url = database_url
    exit_code = run(args)
    if exit_code:
        raise SystemExit(exit_code)


if __name__ == "__main__":
    main()
