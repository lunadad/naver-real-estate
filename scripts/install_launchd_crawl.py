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
        "--mode",
        choices=("daemon", "agent"),
        default="daemon",
        help="Install as a LaunchDaemon (login-independent) or LaunchAgent.",
    )
    parser.add_argument(
        "--user-name",
        default=os.getenv("SUDO_USER") or os.getenv("USER", "").strip(),
        help="User account to run the job under when mode=daemon.",
    )
    parser.add_argument(
        "--group-name",
        default="staff",
        help="Group name to use when mode=daemon.",
    )
    parser.add_argument(
        "--run-at-load",
        dest="run_at_load",
        action="store_true",
        help="Run once when the job is loaded.",
    )
    parser.add_argument(
        "--no-run-at-load",
        dest="run_at_load",
        action="store_false",
        help="Do not run automatically when the job is loaded.",
    )
    parser.set_defaults(run_at_load=None)
    parser.add_argument(
        "--install",
        action="store_true",
        help="Install the plist into LaunchDaemons/LaunchAgents and print the launchctl commands.",
    )
    return parser


def make_plist(args):
    logs_dir = ROOT_DIR / "logs"
    logs_dir.mkdir(exist_ok=True)

    run_at_load = args.run_at_load
    if run_at_load is None:
        run_at_load = args.mode == "daemon"

    plist = {
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
        "RunAtLoad": run_at_load,
    }
    if args.mode == "daemon":
        if args.user_name:
            plist["UserName"] = args.user_name
        if args.group_name:
            plist["GroupName"] = args.group_name
        plist["ProcessType"] = "Background"
    return plist


def install_target(args):
    if args.install and args.mode == "daemon":
        return Path("/Library/LaunchDaemons") / f"{args.label}.plist"
    if args.install:
        return Path.home() / "Library" / "LaunchAgents" / f"{args.label}.plist"
    suffix = "daemon" if args.mode == "daemon" else "agent"
    return ROOT_DIR / "scripts" / f"{args.label}.{suffix}.plist"


def print_install_commands(target, args):
    if args.mode == "daemon":
        print(f"sudo chown root:wheel {target}")
        print(f"sudo chmod 644 {target}")
        print(f"sudo launchctl bootout system/{args.label} 2>/dev/null || true")
        print(f"sudo launchctl bootstrap system {target}")
        print(f"sudo launchctl kickstart -k system/{args.label}")
    else:
        print(f"launchctl bootout gui/$(id -u) {target} 2>/dev/null || true")
        print(f"launchctl bootstrap gui/$(id -u) {target}")
        print(f"launchctl kickstart -k gui/$(id -u)/{args.label}")


def main():
    parser = build_parser()
    args = parser.parse_args()

    if not args.database_url:
        raise SystemExit("DATABASE_URL is required")

    plist_data = make_plist(args)
    target = install_target(args)

    try:
        target.parent.mkdir(parents=True, exist_ok=True)
        with target.open("wb") as fp:
            plistlib.dump(plist_data, fp, sort_keys=False)
    except PermissionError as exc:
        if args.mode == "daemon":
            raise SystemExit(
                f"{exc}. Re-run with sudo for --mode daemon, or use --mode agent."
            ) from exc
        raise

    print(f"wrote {target}")
    print_install_commands(target, args)


if __name__ == "__main__":
    main()
