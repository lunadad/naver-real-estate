import argparse
import os
import plistlib
import sys
from pathlib import Path
from typing import Optional

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from database import ensure_postgres_sslmode

DEFAULT_LABEL = "com.lunadad.naver-real-estate-weekly-report"
CRAWL_PLIST_PATH = Path.home() / "Library" / "LaunchAgents" / "com.lunadad.naver-real-estate-crawl.plist"
SCRIPT_PATH = ROOT_DIR / "scripts" / "send_weekly_report.py"
LOGS_DIR = ROOT_DIR / "logs"


def default_python_path() -> str:
    current = Path(sys.executable).expanduser()
    if current.exists():
        return str(current)
    for candidate in (ROOT_DIR / ".venv" / "bin" / "python3", ROOT_DIR / ".venv-migrate" / "bin" / "python3"):
        if candidate.exists():
            return str(candidate)
    return "/usr/bin/python3"


def read_database_url_from_plist(path: Path) -> Optional[str]:
    """기존 크롤 plist의 ProgramArguments에서 --database-url 다음 값을 읽는다."""
    try:
        with Path(path).open("rb") as fp:
            arguments = plistlib.load(fp).get("ProgramArguments", [])
    except (OSError, plistlib.InvalidFileException, ValueError):
        return None
    for index, value in enumerate(arguments[:-1]):
        if value == "--database-url":
            return arguments[index + 1]
    return None


def build_parser():
    parser = argparse.ArgumentParser(
        description="Create a macOS LaunchAgent plist that sends the weekly report every Monday 10:30."
    )
    parser.add_argument(
        "--database-url",
        default=os.getenv("DATABASE_URL", "").strip(),
        help="Postgres DATABASE_URL (default: read from the existing crawl LaunchAgent plist)",
    )
    parser.add_argument("--python", default=default_python_path(), help="Python executable to run the script")
    parser.add_argument("--label", default=DEFAULT_LABEL, help="launchd label")
    parser.add_argument(
        "--install",
        action="store_true",
        help="Write the plist into ~/Library/LaunchAgents and print the launchctl commands.",
    )
    return parser


def build_plist_payload(args) -> dict:
    database_url = ensure_postgres_sslmode(args.database_url)
    return {
        "Label": args.label,
        "ProgramArguments": [args.python, str(SCRIPT_PATH), "--database-url", database_url],
        "WorkingDirectory": str(ROOT_DIR),
        "EnvironmentVariables": {"PYTHONUNBUFFERED": "1"},
        "StartCalendarInterval": {"Weekday": 1, "Hour": 10, "Minute": 30},
        "StandardOutPath": str(LOGS_DIR / "launchd-weekly-report.out.log"),
        "StandardErrorPath": str(LOGS_DIR / "launchd-weekly-report.err.log"),
        "RunAtLoad": False,
    }


def main():
    args = build_parser().parse_args()
    if not args.database_url:
        args.database_url = read_database_url_from_plist(CRAWL_PLIST_PATH) or ""
    if not args.database_url:
        raise SystemExit(f"DATABASE_URL is required (--database-url, or a crawl plist at {CRAWL_PLIST_PATH})")

    if args.install:
        target = Path.home() / "Library" / "LaunchAgents" / f"{args.label}.plist"
    else:
        target = ROOT_DIR / "scripts" / f"{args.label}.agent.plist"

    LOGS_DIR.mkdir(exist_ok=True)
    target.parent.mkdir(parents=True, exist_ok=True)
    with target.open("wb") as fp:
        plistlib.dump(build_plist_payload(args), fp, sort_keys=False)
    print(f"wrote {target}")
    if args.install:
        print(f"launchctl bootout gui/$(id -u) {target} 2>/dev/null || true")
        print(f"launchctl bootstrap gui/$(id -u) {target}")
        print(f"\nLogs: {LOGS_DIR}/launchd-weekly-report.*.log")


if __name__ == "__main__":
    main()
