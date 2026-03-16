import argparse
import logging
import os
import sys
from pathlib import Path
from logging.handlers import RotatingFileHandler

ROOT_DIR = Path(__file__).resolve().parents[1]
LOGS_DIR = ROOT_DIR / "logs"
DEFAULT_LOG_PATH = LOGS_DIR / "run_remote_crawl.log"
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from crawler import NaverRealEstateCrawler
from database import Database

logger = logging.getLogger("run_remote_crawl")


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
        help="Render Postgres DATABASE_URL",
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
    return parser


def main():
    parser = build_parser()
    args = parser.parse_args()
    configure_logging(args.log_path)

    if not args.database_url:
        raise SystemExit("DATABASE_URL is required")

    os.environ["DATABASE_URL"] = args.database_url
    os.environ.setdefault("ALLOW_DEMO_FALLBACK", "false")
    os.environ.setdefault("SEED_DEMO_DATA", "false")
    os.environ["MIN_LIVE_CRAWL_RATIO"] = str(args.min_live_crawl_ratio)
    os.environ["LOCAL_CRAWL_LOG_PATH"] = str(args.log_path)

    logger.info("Local remote crawl starting (pid=%s)", os.getpid())
    logger.info("Local crawl log file: %s", Path(args.log_path).expanduser())

    db = Database(database_url=args.database_url)
    crawler = NaverRealEstateCrawler(db)
    result = crawler.crawl_all()

    status = result.get("status", "success")
    logger.info(
        "Local remote crawl finished: status=%s source=%s total=%s urgent=%s",
        status,
        result.get("source"),
        result.get("total"),
        result.get("urgent"),
    )

    if status != "success":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
