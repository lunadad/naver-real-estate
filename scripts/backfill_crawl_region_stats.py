import argparse
import os
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Optional

ROOT_DIR = Path(__file__).resolve().parents[1]

import sys

if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from crawler import NaverRealEstateCrawler
from database import Database

KST = timezone(timedelta(hours=9))

LINE_RE = re.compile(
    r"^(?P<ts>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}),\d+\s+\[INFO\]\s+crawler:\s+\[(?P<idx>\d+)/(?P<total>\d+)\]\s+(?P<label>.+):\s+(?P<count>\d+)개 급매$"
)
FINISH_RE = re.compile(
    r"^(?P<ts>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}),\d+\s+\[INFO\]\s+run_remote_crawl:\s+Local remote crawl finished: status=success source=naver total=(?P<total>\d+) urgent=(?P<urgent>\d+)$"
)


def parse_args():
    parser = argparse.ArgumentParser(
        description="Backfill crawl_region_stats from local crawl logs."
    )
    parser.add_argument(
        "--database-url",
        default=os.getenv("DATABASE_URL", "").strip(),
        help="Target Postgres DATABASE_URL",
    )
    parser.add_argument(
        "--log-path",
        default=str(ROOT_DIR / "logs" / "run_remote_crawl.log"),
        help="Path to run_remote_crawl.log",
    )
    return parser.parse_args()


def region_names():
    return sorted(NaverRealEstateCrawler.REGIONS.keys(), key=len, reverse=True)


def split_region_label(label: str):
    for region in region_names():
        prefix = f"{region} "
        if label.startswith(prefix):
            return region, label[len(prefix) :].strip()
    return None, None


def parse_runs(log_path: Path):
    runs: List[Dict] = []
    current: Dict = {"rows": []}
    for raw_line in log_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        match = LINE_RE.match(line)
        if match:
            region, district = split_region_label(match.group("label"))
            if not region or not district:
                continue
            current["rows"].append(
                {
                    "region": region,
                    "district": district,
                    "total_count": int(match.group("count")),
                    "price_down_count": 0,
                }
            )
            continue

        finish = FINISH_RE.match(line)
        if finish:
            if current["rows"]:
                runs.append(
                    {
                        "finished_at": datetime.strptime(
                            finish.group("ts"), "%Y-%m-%d %H:%M:%S"
                        ).replace(tzinfo=KST),
                        "total_count": int(finish.group("total")),
                        "rows": current["rows"],
                    }
                )
            current = {"rows": []}
    return runs


def choose_session(conn, finished_at: datetime, total_count: int) -> Optional[str]:
    rows = conn.execute(
        """
        SELECT session_id, crawled_at, total_count
        FROM crawl_history
        WHERE status = 'success'
          AND COALESCE(source, 'naver') <> 'demo'
          AND DATE(crawled_at) = ?
          AND total_count = ?
        ORDER BY crawled_at DESC
        """,
        (finished_at.date().isoformat(), total_count),
    ).fetchall()
    if not rows:
        return None
    return rows[0]["session_id"]


def main():
    args = parse_args()
    if not args.database_url:
        raise SystemExit("DATABASE_URL is required")

    log_path = Path(args.log_path).expanduser()
    if not log_path.exists():
        raise SystemExit(f"log file not found: {log_path}")

    db = Database(database_url=args.database_url, skip_price_backfill=True)
    runs = parse_runs(log_path)

    restored = 0
    with db.get_connection() as conn:
        for run in runs:
            session_id = choose_session(conn, run["finished_at"], run["total_count"])
            if not session_id:
                continue

            existing = conn.execute(
                "SELECT COUNT(*) AS cnt FROM crawl_region_stats WHERE session_id = ?",
                (session_id,),
            ).fetchone()["cnt"]
            if existing:
                continue

            payload = []
            created_at = run["finished_at"].isoformat()
            for row in run["rows"]:
                payload.append(
                    {
                        "session_id": session_id,
                        "region": row["region"],
                        "district": row["district"],
                        "total_count": row["total_count"],
                        "price_down_count": 0,
                        "created_at": created_at,
                    }
                )
            db.replace_crawl_region_stats(session_id, payload)
            restored += 1
            print(
                f"restored session={session_id} total={run['total_count']} regions={len(payload)}"
            )

    print(f"restored_runs={restored}")


if __name__ == "__main__":
    main()
