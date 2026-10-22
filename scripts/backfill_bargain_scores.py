"""최신(또는 지정) 세션 매물의 진짜 급매 점수를 다시 계산해 저장한다.

사용:
  python scripts/backfill_bargain_scores.py --database-url "$DATABASE_URL"
  python scripts/backfill_bargain_scores.py --db-path real_estate.db --session-id abc123
"""

import argparse
import os
import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from database import Database  # noqa: E402


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--database-url", default=os.getenv("DATABASE_URL", "").strip())
    parser.add_argument("--db-path", default=str(ROOT_DIR / "real_estate.db"),
                        help="SQLite path used when --database-url is empty")
    parser.add_argument("--session-id", default=None, help="default: latest visible session")
    args = parser.parse_args()

    db = Database(db_path=args.db_path, database_url=args.database_url, skip_price_backfill=True)
    try:
        updated = db.backfill_bargain_scores(args.session_id)
    finally:
        db.close()
    print(f"updated {updated} listings")


if __name__ == "__main__":
    main()
