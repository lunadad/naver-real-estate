import argparse
import logging
import os
import sys
from datetime import datetime
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from database import Database
from notifier import send_telegram
from weekly_report import format_weekly_report

SITE_URL = "https://naver-real-estate-henna.vercel.app"
logger = logging.getLogger("send_weekly_report")


def build_parser():
    parser = argparse.ArgumentParser(description="Send the weekly market report to Telegram.")
    parser.add_argument(
        "--database-url",
        default=os.getenv("DATABASE_URL", "").strip(),
        help="Neon or remote Postgres DATABASE_URL",
    )
    parser.add_argument("--dry-run", action="store_true", help="Print the message without sending it.")
    return parser


def _connect(database_url: str) -> Database:
    return Database(database_url=database_url, skip_price_backfill=True)


def run(args, *, now_fn=datetime.now, db_factory=_connect, send=send_telegram, out=print) -> int:
    """리포트 1회 생성·전송. 반환값은 종료 코드(0 전송/드라이런, 1 데이터 없음·전송 실패)."""
    db = db_factory(args.database_url)
    try:
        data = db.get_weekly_report_data(now_fn())
    finally:
        db.close()
    if data is None:
        logger.error("라이브 크롤링 성공 기록이 없어 리포트를 만들 수 없음")
        return 1

    message = format_weekly_report(data, SITE_URL)
    if args.dry_run:
        out(message)
        return 0
    if not send(message):
        logger.error("주간 리포트 텔레그램 전송 실패")
        return 1
    logger.info("주간 리포트 전송 완료")
    return 0


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
    args = build_parser().parse_args()
    if not args.database_url:
        raise SystemExit("DATABASE_URL is required")
    try:
        code = run(args)
    except Exception:
        logger.exception("주간 리포트 실행 중 오류")
        code = 1
    sys.exit(code)


if __name__ == "__main__":
    main()
