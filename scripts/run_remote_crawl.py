import argparse
import logging
import os
import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from crawler import NaverRealEstateCrawler
from database import Database


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)


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
    return parser


def main():
    parser = build_parser()
    args = parser.parse_args()

    if not args.database_url:
        raise SystemExit("DATABASE_URL is required")

    os.environ["DATABASE_URL"] = args.database_url
    os.environ.setdefault("ALLOW_DEMO_FALLBACK", "false")
    os.environ.setdefault("SEED_DEMO_DATA", "false")
    os.environ["MIN_LIVE_CRAWL_RATIO"] = str(args.min_live_crawl_ratio)

    db = Database(database_url=args.database_url)
    crawler = NaverRealEstateCrawler(db)
    result = crawler.crawl_all()

    status = result.get("status", "success")
    print(
        f"crawl status={status} source={result.get('source')} total={result.get('total')} urgent={result.get('urgent')}",
        flush=True,
    )

    if status != "success":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
