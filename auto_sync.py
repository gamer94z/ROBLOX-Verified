import argparse
import re
import shutil
import subprocess
import sys
import time
from pathlib import Path

PROJECT_DIR = Path(__file__).resolve().parent
COLLECTOR_DIR = Path(r"C:\Users\alexl\Desktop\BladesNetworkV2")
COLLECTOR_SCRIPT = COLLECTOR_DIR / "blades_network_v2.py"
COLLECTOR_USERS_FILE = COLLECTOR_DIR / "verified_users.txt"
COLLECTOR_IDS_FILE = COLLECTOR_DIR / "verified_ids.txt"

LOCAL_USERS_FILE = PROJECT_DIR / "verified_users.txt"
LOCAL_UPDATE_SCRIPT = PROJECT_DIR / "update_db.py"

LINE_PATTERN = re.compile(r"^.*?\((\d+)\)\s*-\s*.*$")


def parse_ids_from_verified_users(path: Path):
    ids = set()
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            match = LINE_PATTERN.match(line.strip())
            if match:
                ids.add(int(match.group(1)))
    return sorted(ids)


def write_ids_csv(ids, path: Path):
    path.write_text(",".join(str(uid) for uid in ids) + "\n", encoding="utf-8")


def run_collector():
    if not COLLECTOR_SCRIPT.exists():
        raise FileNotFoundError(f"Collector script not found: {COLLECTOR_SCRIPT}")

    # The collector currently pauses for Enter at the end. Send one newline so it can finish unattended.
    subprocess.run(
        [sys.executable, str(COLLECTOR_SCRIPT)],
        cwd=str(COLLECTOR_DIR),
        input="\n",
        text=True,
        check=True,
    )


def sync_outputs_to_website():
    if not COLLECTOR_USERS_FILE.exists():
        raise FileNotFoundError(f"Collector output not found: {COLLECTOR_USERS_FILE}")

    ids = parse_ids_from_verified_users(COLLECTOR_USERS_FILE)
    write_ids_csv(ids, COLLECTOR_IDS_FILE)

    shutil.copy2(COLLECTOR_USERS_FILE, LOCAL_USERS_FILE)

    subprocess.run(
        [sys.executable, str(LOCAL_UPDATE_SCRIPT)],
        cwd=str(PROJECT_DIR),
        check=True,
    )

    return len(ids)


def run_cycle(skip_collector: bool):
    if not skip_collector:
        print("[SYNC] Running collector...")
        run_collector()
    else:
        print("[SYNC] Skipping collector run (--skip-collector).")

    print("[SYNC] Regenerating verified_ids.txt and updating website DB...")
    count = sync_outputs_to_website()
    print(f"[SYNC] Completed. Seed IDs regenerated: {count}")


def main():
    parser = argparse.ArgumentParser(
        description="Run Roblox collector and sync website database automatically."
    )
    parser.add_argument(
        "--interval-minutes",
        type=int,
        default=0,
        help="If set (>0), repeat forever with this delay between runs.",
    )
    parser.add_argument(
        "--skip-collector",
        action="store_true",
        help="Only sync existing collector output into the website DB.",
    )
    args = parser.parse_args()

    if args.interval_minutes and args.interval_minutes < 1:
        raise ValueError("--interval-minutes must be >= 1")

    if args.interval_minutes:
        while True:
            started = time.strftime("%Y-%m-%d %H:%M:%S")
            print(f"\n[SYNC] Cycle started at {started}")
            try:
                run_cycle(skip_collector=args.skip_collector)
            except Exception as exc:
                print(f"[SYNC] Cycle failed: {exc}")

            print(f"[SYNC] Sleeping {args.interval_minutes} minutes...")
            time.sleep(args.interval_minutes * 60)
    else:
        run_cycle(skip_collector=args.skip_collector)


if __name__ == "__main__":
    main()
