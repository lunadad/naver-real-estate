import argparse
import os
import plistlib
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parents[1]
DEFAULT_LABEL = "com.lunadad.naver-real-estate-crawl"


def build_parser():
    parser = argparse.ArgumentParser(
        description="Create a macOS launchd plist for local daily crawling to Render Postgres."
    )
    parser.add_argument(
        "--database-url",
        default=os.getenv("DATABASE_URL", "").strip(),
        help="Render Postgres DATABASE_URL",
    )
    parser.add_argument(
        "--python",
        default=str(ROOT_DIR / ".venv-migrate" / "bin" / "python3"),
        help="Python executable to use",
    )
    parser.add_argument(
        "--hour",
        type=int,
        default=9,
        help="Hour in local time (0-23)",
    )
    parser.add_argument(
        "--minute",
        type=int,
        default=0,
        help="Minute in local time (0-59)",
    )
    parser.add_argument(
        "--label",
        default=DEFAULT_LABEL,
        help="launchd label",
    )
    parser.add_argument(
        "--install",
        action="store_true",
        help="Write the plist into ~/Library/LaunchAgents and print the launchctl commands.",
    )
    return parser


def make_plist(args):
    logs_dir = ROOT_DIR / "logs"
    logs_dir.mkdir(exist_ok=True)

    return {
        "Label": args.label,
        "ProgramArguments": [
            args.python,
            str(ROOT_DIR / "scripts" / "run_remote_crawl.py"),
            "--database-url",
            args.database_url,
        ],
        "WorkingDirectory": str(ROOT_DIR),
        "EnvironmentVariables": {
            "DATABASE_URL": args.database_url,
            "ALLOW_DEMO_FALLBACK": "false",
            "SEED_DEMO_DATA": "false",
            "MIN_LIVE_CRAWL_RATIO": "0.5",
            "PYTHONUNBUFFERED": "1",
        },
        "StartCalendarInterval": {
            "Hour": max(0, min(23, args.hour)),
            "Minute": max(0, min(59, args.minute)),
        },
        "StandardOutPath": str(logs_dir / "launchd-crawl.out.log"),
        "StandardErrorPath": str(logs_dir / "launchd-crawl.err.log"),
        "RunAtLoad": False,
    }


def main():
    parser = build_parser()
    args = parser.parse_args()

    if not args.database_url:
        raise SystemExit("DATABASE_URL is required")

    plist_data = make_plist(args)

    if args.install:
        target = Path.home() / "Library" / "LaunchAgents" / f"{args.label}.plist"
    else:
        target = ROOT_DIR / "scripts" / f"{args.label}.plist"

    target.parent.mkdir(parents=True, exist_ok=True)
    with target.open("wb") as fp:
        plistlib.dump(plist_data, fp, sort_keys=False)

    print(f"wrote {target}")
    print(f"launchctl unload {target} 2>/dev/null || true")
    print(f"launchctl load {target}")
    print(f"launchctl start {args.label}")


if __name__ == "__main__":
    main()
