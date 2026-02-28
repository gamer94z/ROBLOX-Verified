from flask import Flask, render_template, request, jsonify, session, redirect, url_for
import datetime
import requests
import math
import time
import os
import logging
import threading
from collections import deque
from concurrent.futures import ThreadPoolExecutor, as_completed
from database import (
    init_db,
    get_all_users,
    get_user,
    set_bought_tag,
    get_bought_tags,
    upsert_frontier_candidates,
    pull_frontier_candidates,
    mark_frontier_checked,
    get_frontier_stats,
    save_collector_state,
    load_collector_state,
    get_evidence_for_user,
    add_evidence,
    update_evidence,
    delete_evidence,
    delete_all_evidence_for_user,
    get_evidence_counts,
    add_or_update_manual_user,
    remove_user,
    add_admin_log,
    get_admin_logs,
    DB_NAME,
    IS_POSTGRES,
)
from update_db import parse_verified_users_file, sync_database, TXT_FILE

app = Flask(__name__)
app.secret_key = os.environ.get("FLASK_SECRET_KEY", "change-this-secret")

# ---------------- Database ----------------
init_db()
db = get_all_users()

# ---------------- Constants ----------------
BASE_USERS = "https://users.roblox.com/v1/users"
BASE_FRIENDS = "https://friends.roblox.com/v1/users"
BASE_GROUPS = "https://groups.roblox.com/v1/groups"
VIDEO_STARS_GROUP_ID = 4199740
USERS_PER_PAGE = 30
CACHE_EXPIRY = 3600
DEV_UID = "10006170169"
ADMIN_PASSWORD = os.environ.get("ADMIN_PASSWORD", "changeme")
AUTO_SYNC_ENABLED = os.environ.get("AUTO_SYNC_ENABLED", "1") == "1"
AUTO_SYNC_INTERVAL_SECONDS = max(
    60, int(os.environ.get("AUTO_SYNC_INTERVAL_SECONDS", "600"))
)
AUTO_SYNC_SEED_LIMIT = max(10, int(os.environ.get("AUTO_SYNC_SEED_LIMIT", "120")))
AUTO_SYNC_VERIFY_BATCH_SIZE = max(
    20, min(100, int(os.environ.get("AUTO_SYNC_VERIFY_BATCH_SIZE", "100")))
)
AUTO_SYNC_BATCH_DELAY = float(os.environ.get("AUTO_SYNC_BATCH_DELAY", "0.6"))
AUTO_SYNC_GROUP_DELAY = float(os.environ.get("AUTO_SYNC_GROUP_DELAY", "0.4"))
AUTO_SYNC_FRIEND_DELAY = float(os.environ.get("AUTO_SYNC_FRIEND_DELAY", "0.3"))
AUTO_SYNC_MAX_RETRIES = max(1, int(os.environ.get("AUTO_SYNC_MAX_RETRIES", "3")))
VERIFIED_IDS_FILE = os.environ.get("VERIFIED_IDS_FILE", "verified_ids.txt")
AUTO_SYNC_FRONTIER_BATCH = max(50, int(os.environ.get("AUTO_SYNC_FRONTIER_BATCH", "400")))
AUTO_SYNC_FRONTIER_RECHECK_COOLDOWN = max(
    3600, int(os.environ.get("AUTO_SYNC_FRONTIER_RECHECK_COOLDOWN", str(7 * 24 * 60 * 60)))
)

GROUPS = {
    1200769: "Official Roblox Group",
    4199740: "Roblox Video Stars",
    3514227: "DevForum Community",
}
EXCLUDED_USERNAMES = {"roblox", "builderman"}

user_cache = {}
star_cache = {}
terminated_cache = {}
monitor_events = deque(maxlen=250)
monitor_event_seq = 0
monitor_event_lock = threading.Lock()
last_seen_db_mtime = None
api_limit_log_last = {}
adaptive_backoff_seconds = {
    "get": 0.0,
    "post": 0.0,
}
auto_sync_state = {
    "enabled": AUTO_SYNC_ENABLED,
    "interval_seconds": AUTO_SYNC_INTERVAL_SECONDS,
    "running": False,
    "last_started_ts": None,
    "last_success_ts": None,
    "last_error": None,
    "next_run_ts": None,
    "cycles": 0,
    "current_stage": "Idle",
    "stage_started_ts": None,
    "stage_details": "",
    "last_cycle_seed_verified": 0,
    "last_cycle_frontier_checked": 0,
    "last_cycle_frontier_verified": 0,
    "last_cycle_scanned_candidates": 0,
    "last_cycle_new_added": 0,
    "total_new_verified_found": 0,
    "total_scanned_candidates": 0,
    "stage_progress_done": 0,
    "stage_progress_total": 0,
    "stage_eta_seconds": 0,
    "last_cycle_duration_seconds": 0,
    "avg_cycle_duration_seconds": 0,
    "group_scan_name": "",
    "group_scan_index": 0,
    "group_scan_total_groups": 0,
    "group_member_scanned": 0,
    "group_member_total": 0,
    "cycle_history": [],
    "api_limit_total": 0,
    "api_limit_hit_timestamps": [],
    "api_endpoints": {},
}
auto_sync_thread_started = False
bootstrap_sync_done = False
last_state_persist_ts = 0.0
PERSISTED_SYNC_KEYS = (
    "enabled",
    "interval_seconds",
    "running",
    "last_started_ts",
    "last_success_ts",
    "last_error",
    "next_run_ts",
    "cycles",
    "current_stage",
    "stage_started_ts",
    "stage_details",
    "last_cycle_seed_verified",
    "last_cycle_frontier_checked",
    "last_cycle_frontier_verified",
    "last_cycle_scanned_candidates",
    "last_cycle_new_added",
    "total_new_verified_found",
    "total_scanned_candidates",
    "stage_progress_done",
    "stage_progress_total",
    "stage_eta_seconds",
    "last_cycle_duration_seconds",
    "avg_cycle_duration_seconds",
    "group_scan_name",
    "group_scan_index",
    "group_scan_total_groups",
    "group_member_scanned",
    "group_member_total",
    "cycle_history",
    "api_limit_total",
    "api_limit_hit_timestamps",
    "api_endpoints",
)


def user_sort_key(item):
    uid, info = item
    username = str(info.get("username", "")).lower()
    uid_text = str(uid)
    if uid_text.isdigit():
        return (username, 0, int(uid_text))
    return (username, 1, uid_text.lower())


def next_monitor_event_id():
    global monitor_event_seq
    with monitor_event_lock:
        monitor_event_seq += 1
        return monitor_event_seq


def push_monitor_event(level, message, details=None):
    monitor_events.appendleft(
        {
            "id": next_monitor_event_id(),
            "ts": int(time.time()),
            "level": level,
            "message": message,
            "details": details or {},
        }
    )


def persist_auto_sync_state(force=False):
    global last_state_persist_ts
    now = time.time()
    if not force and (now - last_state_persist_ts) < 1.0:
        return
    snapshot = {k: auto_sync_state.get(k) for k in PERSISTED_SYNC_KEYS}
    save_collector_state(snapshot)
    last_state_persist_ts = now


def hydrate_auto_sync_state_from_db():
    persisted = load_collector_state()
    if not persisted:
        return
    for key in PERSISTED_SYNC_KEYS:
        if key in persisted:
            auto_sync_state[key] = persisted.get(key)


def set_collector_stage(stage, details="", progress_done=0, progress_total=0, eta_seconds=0):
    auto_sync_state["current_stage"] = stage
    auto_sync_state["stage_started_ts"] = int(time.time())
    auto_sync_state["stage_details"] = details
    auto_sync_state["stage_progress_done"] = int(progress_done or 0)
    auto_sync_state["stage_progress_total"] = int(progress_total or 0)
    auto_sync_state["stage_eta_seconds"] = max(0, int(eta_seconds or 0))
    persist_auto_sync_state(force=True)


def update_collector_progress(done, total, eta_seconds=0):
    auto_sync_state["stage_progress_done"] = int(done or 0)
    auto_sync_state["stage_progress_total"] = int(total or 0)
    auto_sync_state["stage_eta_seconds"] = max(0, int(eta_seconds or 0))
    persist_auto_sync_state(force=False)


def update_collector_stage_details(details, eta_seconds=None):
    auto_sync_state["stage_details"] = details
    if eta_seconds is not None:
        auto_sync_state["stage_eta_seconds"] = max(0, int(eta_seconds or 0))
    persist_auto_sync_state(force=False)


def record_api_limit_hit(service, endpoint, wait_seconds=None):
    now_ts = int(time.time())
    key = f"{service} {endpoint}"
    auto_sync_state["api_limit_total"] = int(auto_sync_state.get("api_limit_total") or 0) + 1

    hits = list(auto_sync_state.get("api_limit_hit_timestamps") or [])
    hits.append(now_ts)
    cutoff = now_ts - 3600
    auto_sync_state["api_limit_hit_timestamps"] = [ts for ts in hits if int(ts) >= cutoff][-800:]

    endpoint_map = dict(auto_sync_state.get("api_endpoints") or {})
    row = dict(endpoint_map.get(key) or {})
    row["count"] = int(row.get("count") or 0) + 1
    row["last_ts"] = now_ts
    if wait_seconds is not None:
        row["last_wait_seconds"] = int(max(0, wait_seconds))
    endpoint_map[key] = row
    auto_sync_state["api_endpoints"] = endpoint_map
    persist_auto_sync_state(force=False)


def log_monitor_event(level, message, details=None):
    push_monitor_event(level, message, details)


class MonitorLogHandler(logging.Handler):
    def emit(self, record):
        try:
            push_monitor_event(record.levelname.lower(), record.getMessage(), {})
        except Exception:
            pass


class PathSuppressFilter(logging.Filter):
    def filter(self, record):
        msg = record.getMessage()
        return "/api/collector_monitor" not in msg


app_logger = logging.getLogger("gamers_network")
app_logger.setLevel(logging.INFO)
if not app_logger.handlers:
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(logging.Formatter("[%(asctime)s] %(levelname)s: %(message)s"))
    app_logger.addHandler(stream_handler)
if not any(isinstance(h, MonitorLogHandler) for h in app_logger.handlers):
    app_logger.addHandler(MonitorLogHandler())
for noisy_logger_name in ("werkzeug", "gunicorn.access"):
    noisy_logger = logging.getLogger(noisy_logger_name)
    if not any(isinstance(f, PathSuppressFilter) for f in noisy_logger.filters):
        noisy_logger.addFilter(PathSuppressFilter())


def log_api_limit(service, endpoint, details=None, wait_seconds=None):
    record_api_limit_hit(service, endpoint, wait_seconds=wait_seconds)
    key = f"{service}:{endpoint}"
    now = time.time()
    # Throttle duplicate limit logs so monitor feed stays readable.
    if now - api_limit_log_last.get(key, 0) < 60:
        return
    api_limit_log_last[key] = now
    details_payload = details or {}
    if wait_seconds is not None:
        try:
            details_payload["retry_in_seconds"] = int(max(0, wait_seconds))
        except Exception:
            pass
    message = f"API rate limit hit: {service} {endpoint}"
    if wait_seconds is not None:
        message += f" (retry in {int(max(0, wait_seconds))}s)"
    log_monitor_event(
        "warn",
        message,
        details_payload,
    )
    if wait_seconds is not None:
        app_logger.warning("API rate limit hit: %s %s (retry in %ss)", service, endpoint, int(max(0, wait_seconds)))
    else:
        app_logger.warning("API rate limit hit: %s %s", service, endpoint)


def load_parsed_from_db_snapshot():
    parsed = {}
    for uid, info in get_all_users().items():
        uid_str = str(uid)
        parsed[uid_str] = {
            "username": info.get("username") or uid_str,
            "raw_source": "Seed List" if info.get("source") == "Seed List" else "Newly Added",
        }
    return parsed


def write_verified_users_file(parsed_rows):
    rows = sorted(parsed_rows.items(), key=lambda x: int(x[0]) if str(x[0]).isdigit() else 0)
    lines = [f"{row['username']} ({uid}) - {row['raw_source']}" for uid, row in rows]
    with open(TXT_FILE, "w", encoding="utf-8") as f:
        f.write("\n".join(lines) + ("\n" if lines else ""))


def safe_get(url, params=None, timeout=12):
    for attempt in range(1, AUTO_SYNC_MAX_RETRIES + 1):
        if adaptive_backoff_seconds["get"] > 0:
            time.sleep(adaptive_backoff_seconds["get"])
        try:
            r = requests.get(url, params=params, timeout=timeout)
            if r.status_code == 429:
                adaptive_backoff_seconds["get"] = min(
                    max(1.0, adaptive_backoff_seconds["get"] * 1.4 + 0.8), 12.0
                )
                wait_for = 2 + adaptive_backoff_seconds["get"]
                log_api_limit(url.split("/")[2], url, {"attempt": attempt}, wait_seconds=wait_for)
                time.sleep(wait_for)
                continue
            adaptive_backoff_seconds["get"] = max(0.0, adaptive_backoff_seconds["get"] * 0.85 - 0.05)
            return r
        except requests.exceptions.RequestException:
            app_logger.warning("Auto-sync GET failed (%s/%s): %s", attempt, AUTO_SYNC_MAX_RETRIES, url)
            time.sleep(2)
    return None


def safe_post(url, json_data, timeout=12):
    cooldown = 8
    for attempt in range(1, AUTO_SYNC_MAX_RETRIES + 1):
        if adaptive_backoff_seconds["post"] > 0:
            time.sleep(adaptive_backoff_seconds["post"])
        try:
            r = requests.post(url, json=json_data, timeout=timeout)
            if r.status_code == 429:
                adaptive_backoff_seconds["post"] = min(
                    max(1.0, adaptive_backoff_seconds["post"] * 1.4 + 0.8), 12.0
                )
                wait_for = max(cooldown, adaptive_backoff_seconds["post"])
                log_api_limit(url.split("/")[2], url, {"attempt": attempt}, wait_seconds=wait_for)
                time.sleep(cooldown)
                cooldown = min(cooldown * 2, 30)
                continue
            adaptive_backoff_seconds["post"] = max(
                0.0, adaptive_backoff_seconds["post"] * 0.85 - 0.05
            )
            return r
        except requests.exceptions.RequestException:
            app_logger.warning("Auto-sync POST failed (%s/%s): %s", attempt, AUTO_SYNC_MAX_RETRIES, url)
            time.sleep(2)
    return None


def load_seed_ids():
    if not os.path.exists(VERIFIED_IDS_FILE):
        return []

    with open(VERIFIED_IDS_FILE, "r", encoding="utf-8") as f:
        raw = f.read()

    raw = raw.replace("\n", ",").replace("\r", ",")
    ids = set()
    for part in raw.split(","):
        part = part.strip()
        if part.isdigit():
            ids.add(int(part))
    return list(ids)


def verify_badges_batch(user_ids, progress_stage=None, progress_detail=None):
    verified = {}
    total = len(user_ids)
    if not total:
        return verified

    if progress_stage:
        set_collector_stage(
            progress_stage,
            progress_detail or "",
            progress_done=0,
            progress_total=total,
            eta_seconds=0,
        )

    batch_start_ts = time.time()
    for i in range(0, total, AUTO_SYNC_VERIFY_BATCH_SIZE):
        batch = user_ids[i:i + AUTO_SYNC_VERIFY_BATCH_SIZE]
        r = safe_post(BASE_USERS, {"userIds": batch})
        if not r or r.status_code != 200:
            continue

        for user in r.json().get("data", []):
            name = (user.get("name") or "").lower()
            if bool(user.get("hasVerifiedBadge")) and name not in EXCLUDED_USERNAMES:
                verified[int(user["id"])] = user.get("name") or str(user["id"])

        processed = min(i + AUTO_SYNC_VERIFY_BATCH_SIZE, total)
        remaining = max(0, total - processed)
        elapsed = max(0.001, time.time() - batch_start_ts)
        avg_per_user = elapsed / max(1, processed)
        eta_seconds = int(remaining * avg_per_user)
        update_collector_progress(processed, total, eta_seconds=eta_seconds)

        app_logger.info("Collector batch verified %s/%s", processed, total)
        time.sleep(AUTO_SYNC_BATCH_DELAY)

    return verified


def scan_group(group_id, group_name, verified_users, frontier_candidates, group_index=None, group_total=None):
    app_logger.info("Collector scanning group: %s (%s)", group_name, group_id)
    cursor = None
    pages = 0
    seen_candidates = set()
    started = time.time()
    scanned_members = 0
    group_member_total = 0

    try:
        meta = safe_get(f"{BASE_GROUPS}/{group_id}")
        if meta and meta.status_code == 200:
            group_member_total = int(meta.json().get("memberCount") or 0)
    except Exception:
        group_member_total = 0
    auto_sync_state["group_scan_name"] = group_name
    auto_sync_state["group_scan_index"] = int(group_index or 0)
    auto_sync_state["group_scan_total_groups"] = int(group_total or 0)
    auto_sync_state["group_member_scanned"] = 0
    auto_sync_state["group_member_total"] = int(group_member_total or 0)
    persist_auto_sync_state(force=True)

    while True:
        r = safe_get(f"{BASE_GROUPS}/{group_id}/users", params={"limit": 100, "cursor": cursor})
        if not r or r.status_code != 200:
            app_logger.warning("Collector group request failed: %s", group_name)
            break

        data = r.json()
        users = data.get("data", [])
        scanned_members += len(users)
        cursor = data.get("nextPageCursor")
        pages += 1

        ids_to_check = []
        for entry in users:
            user = entry.get("user", {})
            uid = user.get("userId")
            uname = (user.get("username") or "").lower()
            if not uid or uid in verified_users or uname in EXCLUDED_USERNAMES or uid in seen_candidates:
                continue
            seen_candidates.add(uid)
            frontier_candidates.add(int(uid))
            ids_to_check.append(uid)

        verified_batch = verify_badges_batch(ids_to_check)
        for uid, name in verified_batch.items():
            verified_users[uid] = {"username": name, "source": f"Group: {group_name}"}

        if group_index and group_total:
            remaining_groups = max(0, group_total - group_index)
            elapsed = max(0.001, time.time() - started)
            eta_seconds = int(elapsed * remaining_groups)
            detail = (
                f"Group {group_index}/{group_total}: {group_name} | pages {pages} | candidates {len(seen_candidates)}"
            )
            if group_member_total > 0:
                done = min(scanned_members, group_member_total)
                rate = done / elapsed if elapsed > 0 else 0.0
                remaining_members = max(0, group_member_total - done)
                member_eta = int(remaining_members / rate) if rate > 0 else 0
                auto_sync_state["group_member_scanned"] = int(done)
                auto_sync_state["group_member_total"] = int(group_member_total)
                update_collector_progress(done, group_member_total, eta_seconds=member_eta)
                detail = (
                    f"Group {group_index}/{group_total}: {group_name} | members {done}/{group_member_total} scanned | "
                    f"candidates {len(seen_candidates)}"
                )
                eta_seconds = member_eta
            else:
                auto_sync_state["group_member_scanned"] = int(scanned_members)
                auto_sync_state["group_member_total"] = 0
                update_collector_progress(pages, max(1, pages + 1), eta_seconds=eta_seconds)
            update_collector_stage_details(
                detail,
                eta_seconds=eta_seconds,
            )

        if not cursor:
            break
        time.sleep(AUTO_SYNC_GROUP_DELAY)

    app_logger.info("Collector finished group: %s (pages=%s)", group_name, pages)
    return len(seen_candidates)


def expand_friends(verified_users, frontier_candidates):
    app_logger.info("Collector expanding verified friend network...")
    queue = list(verified_users.keys())
    seen = set(queue)
    scanned = 0
    roots_done = 0
    started = time.time()

    update_collector_progress(0, len(queue), eta_seconds=0)
    update_collector_stage_details(
        f"Branch roots queued: {len(queue)} | scanned candidates: {scanned}",
        eta_seconds=0,
    )

    while queue:
        uid = queue.pop(0)
        roots_done += 1
        cursor = None
        while True:
            r = safe_get(f"{BASE_FRIENDS}/{uid}/friends", params={"limit": 100, "cursor": cursor})
            if not r or r.status_code != 200:
                break

            data = r.json()
            friends = data.get("data", [])
            cursor = data.get("nextPageCursor")

            ids = []
            for f in friends:
                fid = f.get("id")
                fname = (f.get("name") or "").lower()
                if not fid or fid in seen or fname in EXCLUDED_USERNAMES:
                    continue
                ids.append(fid)
                seen.add(fid)
                frontier_candidates.add(int(fid))

            scanned += len(ids)
            verified_batch = verify_badges_batch(ids)
            for fid, name in verified_batch.items():
                if fid not in verified_users:
                    verified_users[fid] = {"username": name, "source": "Verified Friend"}
                    queue.append(fid)

            if not cursor:
                break
            time.sleep(AUTO_SYNC_FRIEND_DELAY)

        pending = len(queue)
        elapsed = max(0.001, time.time() - started)
        avg_per_root = elapsed / max(1, roots_done)
        eta_seconds = int(avg_per_root * pending)
        update_collector_progress(roots_done, roots_done + pending, eta_seconds=eta_seconds)
        update_collector_stage_details(
            f"Roots processed: {roots_done} | roots left: {pending} | scanned candidates: {scanned}",
            eta_seconds=eta_seconds,
        )

    return scanned


def run_auto_sync_cycle():
    set_collector_stage("Preparing Snapshot", "Loading existing verified snapshot")
    if os.path.exists(TXT_FILE):
        parsed = parse_verified_users_file(TXT_FILE)
    else:
        parsed = {}
    if not parsed:
        parsed = load_parsed_from_db_snapshot()

    existing_db = get_all_users()
    fallback_seed_ids = [
        int(uid)
        for uid, info in existing_db.items()
        if str(uid).isdigit() and info.get("source") == "Seed List"
    ]
    if not fallback_seed_ids:
        fallback_seed_ids = [int(uid) for uid in existing_db.keys() if str(uid).isdigit()]

    seed_ids = load_seed_ids() or fallback_seed_ids
    seed_ids = seed_ids[:AUTO_SYNC_SEED_LIMIT]
    app_logger.info("Collector loaded %s seed users", len(seed_ids))

    set_collector_stage(
        "Stage 1: Seed Verification",
        f"Checking {len(seed_ids)} seed candidates",
        progress_done=0,
        progress_total=len(seed_ids),
        eta_seconds=0,
    )
    frontier_candidates = set(int(uid) for uid in seed_ids if str(uid).isdigit())
    seed_verified = verify_badges_batch(
        seed_ids,
        progress_stage="Stage 1: Seed Verification",
        progress_detail=f"Checking {len(seed_ids)} seed candidates",
    )
    verified_users = {
        uid: {"username": name, "source": "Seed List"}
        for uid, name in seed_verified.items()
    }
    frontier_candidates.update(int(uid) for uid in seed_verified.keys())
    app_logger.info("Collector seed verification complete: %s", len(verified_users))

    scanned_count = 0
    group_total = len(GROUPS)
    completed_group_seconds = 0.0
    for group_index, (gid, gname) in enumerate(GROUPS.items(), start=1):
        remaining_groups = max(0, group_total - group_index)
        avg_group_seconds = (
            completed_group_seconds / (group_index - 1)
            if group_index > 1
            else 0.0
        )
        set_collector_stage(
            "Stage 2: Group Discovery",
            f"Scanning group {group_index}/{group_total}: {gname}",
            progress_done=group_index - 1,
            progress_total=group_total,
            eta_seconds=int(avg_group_seconds * remaining_groups),
        )
        group_started = time.time()
        scanned_count += scan_group(
            gid,
            gname,
            verified_users,
            frontier_candidates,
            group_index=group_index,
            group_total=group_total,
        )
        completed_group_seconds += max(0.0, time.time() - group_started)
        avg_group_seconds = completed_group_seconds / group_index

    set_collector_stage(
        "Stage 3: Friend Expansion",
        "Walking friend graph from verified users",
        progress_done=0,
        progress_total=max(1, len(verified_users)),
        eta_seconds=0,
    )
    scanned_count += expand_friends(verified_users, frontier_candidates)

    # Persist discovered candidates and re-check highest-priority entries from frontier.
    set_collector_stage(
        "Stage 4: Frontier Recheck",
        f"Rechecking up to {AUTO_SYNC_FRONTIER_BATCH} high-priority candidates",
    )
    upsert_frontier_candidates(frontier_candidates, source="collector_discovery", score_boost=1)
    frontier_batch = pull_frontier_candidates(
        limit=AUTO_SYNC_FRONTIER_BATCH,
        non_verified_cooldown_seconds=AUTO_SYNC_FRONTIER_RECHECK_COOLDOWN,
    )
    frontier_batch_ids = [int(uid) for uid in frontier_batch if str(uid).isdigit()]
    frontier_verified = verify_badges_batch(
        frontier_batch_ids,
        progress_stage="Stage 4: Frontier Recheck",
        progress_detail=f"Rechecking {len(frontier_batch_ids)} high-priority candidates",
    )
    frontier_results = {uid: False for uid in frontier_batch}
    for uid, name in frontier_verified.items():
        uid_str = str(uid)
        frontier_results[uid_str] = True
        if uid not in verified_users:
            verified_users[uid] = {"username": name, "source": "Frontier Discovery"}
    mark_frontier_checked(frontier_results)

    set_collector_stage(
        "Stage 5: Database Sync",
        "Writing verified snapshot to DB and TXT",
        progress_done=0,
        progress_total=2,
        eta_seconds=4,
    )
    added = 0
    for uid, info in verified_users.items():
        uid_str = str(uid)
        if uid_str not in parsed:
            parsed[uid_str] = {"username": info["username"], "raw_source": info["source"]}
            added += 1

    write_verified_users_file(parsed)
    update_collector_progress(1, 2, eta_seconds=2)
    sync_database(parsed)
    update_collector_progress(2, 2, eta_seconds=0)
    auto_sync_state["last_cycle_seed_verified"] = len(seed_verified)
    auto_sync_state["last_cycle_frontier_checked"] = len(frontier_batch_ids)
    auto_sync_state["last_cycle_frontier_verified"] = len(frontier_verified)
    auto_sync_state["last_cycle_scanned_candidates"] = scanned_count
    auto_sync_state["last_cycle_new_added"] = added
    auto_sync_state["total_new_verified_found"] = int(
        auto_sync_state.get("total_new_verified_found") or 0
    ) + int(added)
    auto_sync_state["total_scanned_candidates"] = int(
        auto_sync_state.get("total_scanned_candidates") or 0
    ) + int(scanned_count)
    log_monitor_event(
        "ok",
        "Collector cycle finished",
        {
            "seed_verified": len(seed_verified),
            "frontier_checked": len(frontier_batch_ids),
            "frontier_verified": len(frontier_verified),
            "verified_total": len(verified_users),
            "scanned_candidates": scanned_count,
            "new_added": added,
            "total_snapshot": len(parsed),
        },
    )
    return len(parsed), added, scanned_count


def bootstrap_database_once():
    global bootstrap_sync_done
    if bootstrap_sync_done:
        return
    if not os.path.exists(TXT_FILE):
        app_logger.warning("Bootstrap sync skipped: %s not found", TXT_FILE)
        bootstrap_sync_done = True
        return
    try:
        parsed = parse_verified_users_file(TXT_FILE)
        if parsed:
            sync_database(parsed)
            app_logger.info("Bootstrap sync loaded %s users from %s", len(parsed), TXT_FILE)
            log_monitor_event("ok", "Bootstrap database sync complete", {"users": len(parsed)})
        else:
            app_logger.warning("Bootstrap sync found no parsable rows in %s", TXT_FILE)
    except Exception:
        app_logger.exception("Bootstrap sync failed")
    finally:
        bootstrap_sync_done = True


def auto_sync_loop():
    app_logger.info(
        "Auto-sync worker started (interval=%ss, enabled=%s)",
        AUTO_SYNC_INTERVAL_SECONDS,
        "yes" if AUTO_SYNC_ENABLED else "no",
    )
    while True:
        started_ts = int(time.time())
        cycle_start_monotonic = time.time()
        auto_sync_state["running"] = True
        auto_sync_state["last_started_ts"] = started_ts
        auto_sync_state["next_run_ts"] = None
        auto_sync_state["last_error"] = None
        auto_sync_state["cycles"] = int(auto_sync_state["cycles"] or 0) + 1
        set_collector_stage("Cycle Starting", f"Loop #{auto_sync_state['cycles']}")

        try:
            parsed_count, added_count, scanned_count = run_auto_sync_cycle()
            auto_sync_state["last_success_ts"] = int(time.time())
            cycle_duration = max(0, int(time.time() - cycle_start_monotonic))
            auto_sync_state["last_cycle_duration_seconds"] = cycle_duration
            prev_avg = int(auto_sync_state.get("avg_cycle_duration_seconds") or 0)
            if prev_avg <= 0:
                auto_sync_state["avg_cycle_duration_seconds"] = cycle_duration
            else:
                auto_sync_state["avg_cycle_duration_seconds"] = int(prev_avg * 0.75 + cycle_duration * 0.25)
            app_logger.info(
                "Auto-sync completed successfully (%s parsed users, %s added, %s scanned)",
                parsed_count,
                added_count,
                scanned_count,
            )
            history = list(auto_sync_state.get("cycle_history") or [])
            history.append(
                {
                    "cycle": int(auto_sync_state.get("cycles") or 0),
                    "ts": int(time.time()),
                    "status": "ok",
                    "duration_seconds": cycle_duration,
                    "parsed_users": int(parsed_count),
                    "new_added": int(added_count),
                    "scanned_candidates": int(scanned_count),
                    "error": "",
                }
            )
            auto_sync_state["cycle_history"] = history[-20:]
            persist_auto_sync_state()
        except Exception as exc:
            auto_sync_state["last_error"] = str(exc)
            app_logger.exception("Auto-sync cycle failed")
            cycle_duration = max(0, int(time.time() - cycle_start_monotonic))
            history = list(auto_sync_state.get("cycle_history") or [])
            history.append(
                {
                    "cycle": int(auto_sync_state.get("cycles") or 0),
                    "ts": int(time.time()),
                    "status": "error",
                    "duration_seconds": cycle_duration,
                    "parsed_users": 0,
                    "new_added": 0,
                    "scanned_candidates": 0,
                    "error": str(exc),
                }
            )
            auto_sync_state["cycle_history"] = history[-20:]
            persist_auto_sync_state()
        finally:
            auto_sync_state["running"] = False
            auto_sync_state["next_run_ts"] = int(time.time()) + AUTO_SYNC_INTERVAL_SECONDS
            set_collector_stage(
                "Sleeping",
                f"Next cycle in {AUTO_SYNC_INTERVAL_SECONDS}s",
                progress_done=0,
                progress_total=1,
                eta_seconds=AUTO_SYNC_INTERVAL_SECONDS,
            )
            persist_auto_sync_state()

        time.sleep(AUTO_SYNC_INTERVAL_SECONDS)


def start_auto_sync_worker():
    global auto_sync_thread_started
    hydrate_auto_sync_state_from_db()
    if auto_sync_thread_started or not AUTO_SYNC_ENABLED:
        return

    # Avoid duplicate workers under Flask debug reloader parent process.
    if app.debug and os.environ.get("WERKZEUG_RUN_MAIN") != "true":
        return

    # Fast startup population so deploys do not appear empty before full cycle finishes.
    bootstrap_database_once()

    worker = threading.Thread(target=auto_sync_loop, name="auto-sync-worker", daemon=True)
    worker.start()
    auto_sync_thread_started = True


def is_admin_authenticated():
    return bool(session.get("admin_auth"))


# ---------------- Helper: fetch live user data ----------------
def fetch_user_data(uid):
    now = time.time()

    if uid in user_cache:
        data, timestamp = user_cache[uid]
        if now - timestamp < CACHE_EXPIRY:
            return data

    live = None
    stats = {"friends": 0, "followers": 0, "following": 0}
    avatar_url = ""
    star = False
    basic_ok = False
    stats_ok = False
    avatar_ok = False

    # --- Basic user info ---
    try:
        r = requests.get(f"{BASE_USERS}/{uid}", timeout=5)
        if r.status_code == 200:
            d = r.json()
            join_date = d.get("created")
            if join_date:
                join_date = datetime.datetime.fromisoformat(
                    join_date.replace("Z", "")
                ).strftime("%Y-%m-%d")

            live = {
                "username": d.get("name"),
                "displayName": d.get("displayName"),
                "joined": join_date or "Unknown",
            }
            basic_ok = True
        elif r.status_code == 429:
            log_api_limit("users.roblox.com", f"/v1/users/{uid}")
    except:
        pass

    # --- Stats ---
    try:
        endpoints = {
            "friends": "friends/count",
            "followers": "followers/count",
            "following": "followings/count",
        }
        for key, endpoint in endpoints.items():
            r = requests.get(f"{BASE_FRIENDS}/{uid}/{endpoint}", timeout=5)
            if r.status_code == 200:
                stats[key] = r.json().get("count", 0)
                stats_ok = True
            elif r.status_code == 429:
                log_api_limit("friends.roblox.com", f"/v1/users/{uid}/{endpoint}")
    except:
        pass

    # --- Avatar ---
    try:
        url = f"https://thumbnails.roblox.com/v1/users/avatar-headshot?userIds={uid}&size=150x150&format=Png&isCircular=true"
        r = requests.get(url, timeout=5)
        if r.status_code == 200:
            avatar_url = r.json()["data"][0]["imageUrl"]
            avatar_ok = bool(avatar_url)
        elif r.status_code == 429:
            log_api_limit("thumbnails.roblox.com", "/v1/users/avatar-headshot")
    except:
        pass

    # --- Star creator check ---
    try:
        r = requests.get(f"https://groups.roblox.com/v1/users/{uid}/groups/roles", timeout=5)
        if r.status_code == 200:
            for group in r.json().get("data", []):
                if group.get("group") and group["group"].get("id") == VIDEO_STARS_GROUP_ID:
                    star = True
                    break
        elif r.status_code == 429:
            log_api_limit("groups.roblox.com", f"/v1/users/{uid}/groups/roles")
    except:
        pass

    profile_url = f"https://www.roblox.com/users/{uid}/profile"

    data = {
        "live": live,
        "stats": stats,
        "avatar_url": avatar_url,
        "is_star_creator": star,
        "profile_url": profile_url,
        "_partial": not (basic_ok and stats_ok and avatar_ok),
    }

    # Cache complete data normally; cache partial data briefly so transient API
    # failures recover quickly instead of showing stale N/A/0 for long periods.
    if data["_partial"]:
        user_cache[uid] = (data, now - (CACHE_EXPIRY - 20))
    else:
        user_cache[uid] = (data, now)
    return data


def check_star_creator(uid):
    now = time.time()
    key = str(uid)
    cached = star_cache.get(key)
    if cached and now - cached[1] < CACHE_EXPIRY:
        return cached[0]

    star = False
    fetched_ok = False
    try:
        r = requests.get(f"https://groups.roblox.com/v1/users/{uid}/groups/roles", timeout=5)
        if r.status_code == 200:
            fetched_ok = True
            for group in r.json().get("data", []):
                if group.get("group") and group["group"].get("id") == VIDEO_STARS_GROUP_ID:
                    star = True
                    break
        elif cached:
            return cached[0]
        elif r.status_code == 429:
            log_api_limit("groups.roblox.com", f"/v1/users/{uid}/groups/roles")
    except:
        if cached:
            return cached[0]
        return False

    # Only refresh cache when we actually got a valid API response.
    if fetched_ok:
        star_cache[key] = (star, now)
    elif cached:
        return cached[0]
    return star


def check_terminated(uid, force_refresh=False):
    now = time.time()
    key = str(uid)
    cached = terminated_cache.get(key)
    if cached and not force_refresh and now - cached[1] < CACHE_EXPIRY:
        return cached[0]

    terminated = False

    # Primary lookup via users batch endpoint for this single uid.
    # Do not early-return on `False` because this endpoint can omit
    # termination state for some edge cases; fallback GET is more reliable.
    primary_known = False
    try:
        r = requests.post(BASE_USERS, json={"userIds": [int(uid)]}, timeout=6)
        if r.status_code == 200:
            data = r.json().get("data", [])
            if data:
                d = data[0]
                terminated = bool(d.get("terminated", False) or d.get("isBanned", False))
                primary_known = True
                if terminated:
                    terminated_cache[key] = (True, now)
                    return True
        elif r.status_code == 429:
            log_api_limit("users.roblox.com", "/v1/users (POST)")
    except:
        pass

    # Fallback direct profile lookup; 404 implies removed/terminated profile.
    try:
        r = requests.get(f"{BASE_USERS}/{uid}", timeout=6)
        if r.status_code == 404:
            terminated = True
        elif r.status_code == 200:
            d = r.json()
            terminated = bool(d.get("terminated", False) or d.get("isBanned", False))
        elif r.status_code == 429:
            log_api_limit("users.roblox.com", f"/v1/users/{uid}")
        else:
            # On non-success responses, preserve previous cache if present.
            if cached:
                return cached[0]
            # If primary succeeded and said not terminated, keep that as best signal.
            if primary_known:
                terminated = False
    except:
        # Preserve previous cache result on transient failures.
        if cached:
            return cached[0]
        # If primary succeeded and said not terminated, keep that as best signal.
        terminated = False if primary_known else False

    terminated_cache[key] = (terminated, now)
    return terminated

# ---------------- Routes ----------------

@app.route("/")
@app.route("/home")
def home():
    last_updated = datetime.datetime.now().strftime("%Y-%m-%d %H:%M")
    return render_template("home.html", last_updated=last_updated, DEV_UID=DEV_UID)


@app.route("/collector-monitor")
def collector_monitor():
    return render_template("collector_monitor.html", DEV_UID=DEV_UID)


@app.route("/admin", methods=["GET", "POST"])
def admin():
    if not is_admin_authenticated():
        error = None
        if request.method == "POST":
            password = request.form.get("password", "")
            if password == ADMIN_PASSWORD:
                session["admin_auth"] = True
                return redirect(url_for("admin"))
            error = "Invalid password"
        return render_template("admin_login.html", error=error)

    query = request.args.get("query", "").strip().lower()
    bought_filter = request.args.get("bought_filter", "all").strip().lower()
    if bought_filter not in {"all", "blocked", "clean"}:
        bought_filter = "all"
    try:
        page = max(1, int(request.args.get("page", 1)))
    except:
        page = 1

    users = get_all_users()
    rows = sorted(
        users.items(),
        key=lambda x: x[1].get("username", "").lower(),
    )
    if query:
        rows = [
            item
            for item in rows
            if query in str(item[0]).lower() or query in item[1].get("username", "").lower()
        ]
    if bought_filter == "blocked":
        rows = [item for item in rows if bool(item[1].get("bought_tag"))]
    elif bought_filter == "clean":
        rows = [item for item in rows if not bool(item[1].get("bought_tag"))]

    per_page = 50
    total = len(rows)
    total_pages = max(1, math.ceil(total / per_page))
    start = (page - 1) * per_page
    page_items = rows[start:start + per_page]

    return render_template(
        "admin_panel.html",
        users=page_items,
        query=query,
        bought_filter=bought_filter,
        page=page,
        total=total,
        total_pages=total_pages,
        admin_logs=get_admin_logs(120),
    )


@app.route("/admin/logout", methods=["POST"])
def admin_logout():
    session.pop("admin_auth", None)
    return redirect(url_for("admin"))


@app.route("/api/admin/bought_tag", methods=["POST"])
def api_admin_bought_tag():
    if not is_admin_authenticated():
        return jsonify({"error": "Unauthorized"}), 401

    payload = request.get_json(silent=True) or {}
    uid = str(payload.get("uid", "")).strip()
    enabled = bool(payload.get("bought_tag", False))
    if not uid.isdigit():
        return jsonify({"error": "Invalid uid"}), 400

    if not set_bought_tag(uid, enabled):
        return jsonify({"error": "User not found"}), 404
    # Bump shared freshness marker so all connected clients reload data across workers.
    auto_sync_state["last_success_ts"] = int(time.time())
    persist_auto_sync_state(force=True)
    log_monitor_event(
        "warn" if enabled else "info",
        "Bought tag updated",
        {"uid": uid, "bought_tag": bool(enabled)},
    )
    add_admin_log(
        "bought_tag_set" if enabled else "bought_tag_removed",
        target_uid=uid,
        detail=f"Bought tag {'enabled' if enabled else 'disabled'}",
    )

    return jsonify({"ok": True, "uid": uid, "bought_tag": enabled})


@app.route("/api/admin/evidence/<uid>", methods=["GET"])
def api_admin_get_evidence(uid):
    if not is_admin_authenticated():
        return jsonify({"error": "Unauthorized"}), 401
    if not str(uid).isdigit():
        return jsonify({"error": "Invalid uid"}), 400
    return jsonify({"uid": str(uid), "items": get_evidence_for_user(uid)})


@app.route("/api/admin/evidence", methods=["POST"])
def api_admin_add_evidence():
    if not is_admin_authenticated():
        return jsonify({"error": "Unauthorized"}), 401
    payload = request.get_json(silent=True) or {}
    uid = str(payload.get("uid", "")).strip()
    source_type = str(payload.get("source_type", "other")).strip().lower()[:32]
    title = str(payload.get("title", "")).strip()[:160]
    url = str(payload.get("url", "")).strip()[:700]
    note = str(payload.get("note", "")).strip()[:4000]
    if not uid.isdigit():
        return jsonify({"error": "Invalid uid"}), 400
    if not title and not url and not note:
        return jsonify({"error": "Provide at least one of title, link, or note"}), 400

    evidence_id = add_evidence(uid, source_type, title, url, note)
    auto_sync_state["last_success_ts"] = int(time.time())
    persist_auto_sync_state(force=True)
    add_admin_log(
        "evidence_add",
        target_uid=uid,
        detail=f"id={int(evidence_id)}; type={source_type}; title={title[:80]}",
    )
    log_monitor_event("info", "Evidence added", {"uid": uid, "evidence_id": evidence_id})
    return jsonify({"ok": True, "id": int(evidence_id), "uid": uid})


@app.route("/api/admin/evidence/<int:evidence_id>", methods=["PATCH"])
def api_admin_update_evidence(evidence_id):
    if not is_admin_authenticated():
        return jsonify({"error": "Unauthorized"}), 401
    payload = request.get_json(silent=True) or {}
    source_type = str(payload.get("source_type", "other")).strip().lower()[:32]
    title = str(payload.get("title", "")).strip()[:160]
    url = str(payload.get("url", "")).strip()[:700]
    note = str(payload.get("note", "")).strip()[:4000]
    if not title and not url and not note:
        return jsonify({"error": "Provide at least one of title, link, or note"}), 400
    if not update_evidence(evidence_id, source_type, title, url, note):
        return jsonify({"error": "Evidence not found"}), 404
    auto_sync_state["last_success_ts"] = int(time.time())
    persist_auto_sync_state(force=True)
    add_admin_log(
        "evidence_update",
        target_uid="",
        detail=f"id={int(evidence_id)}; type={source_type}; title={title[:80]}",
    )
    log_monitor_event("info", "Evidence updated", {"evidence_id": int(evidence_id)})
    return jsonify({"ok": True})


@app.route("/api/admin/evidence/<int:evidence_id>", methods=["DELETE"])
def api_admin_delete_evidence(evidence_id):
    if not is_admin_authenticated():
        return jsonify({"error": "Unauthorized"}), 401
    if not delete_evidence(evidence_id):
        return jsonify({"error": "Evidence not found"}), 404
    auto_sync_state["last_success_ts"] = int(time.time())
    persist_auto_sync_state(force=True)
    add_admin_log("evidence_delete", target_uid="", detail=f"id={int(evidence_id)}")
    log_monitor_event("warn", "Evidence deleted", {"evidence_id": int(evidence_id)})
    return jsonify({"ok": True})


@app.route("/api/admin/evidence/user/<uid>", methods=["DELETE"])
def api_admin_delete_all_evidence(uid):
    if not is_admin_authenticated():
        return jsonify({"error": "Unauthorized"}), 401
    if not str(uid).isdigit():
        return jsonify({"error": "Invalid uid"}), 400
    deleted = delete_all_evidence_for_user(uid)
    auto_sync_state["last_success_ts"] = int(time.time())
    persist_auto_sync_state(force=True)
    add_admin_log("evidence_delete_all", target_uid=str(uid), detail=f"deleted={int(deleted)}")
    log_monitor_event("warn", "All evidence deleted for user", {"uid": str(uid), "deleted": int(deleted)})
    return jsonify({"ok": True, "deleted": int(deleted)})


@app.route("/api/evidence/<uid>")
def api_public_evidence(uid):
    if not str(uid).isdigit():
        return jsonify({"error": "Invalid uid"}), 400
    user = get_user(uid)
    if not user:
        return jsonify({"error": "User not found"}), 404
    items = get_evidence_for_user(uid)
    return jsonify(
        {
            "uid": str(uid),
            "username": user.get("username", ""),
            "bought_tag": bool(user.get("bought_tag")),
            "count": len(items),
            "items": items,
        }
    )


@app.route("/api/admin/manual_user_add", methods=["POST"])
def api_admin_manual_user_add():
    if not is_admin_authenticated():
        return jsonify({"error": "Unauthorized"}), 401
    payload = request.get_json(silent=True) or {}
    uid = str(payload.get("uid", "")).strip()
    status = str(payload.get("status", "Newly Added")).strip()
    bought = bool(payload.get("bought_tag", False))
    if not uid.isdigit():
        return jsonify({"error": "Invalid uid"}), 400

    username = payload.get("username")
    username = str(username).strip() if username else ""
    if not username:
        try:
            r = requests.get(f"{BASE_USERS}/{uid}", timeout=8)
            if r.status_code != 200:
                return jsonify({"error": f"Unable to fetch Roblox user ({r.status_code})"}), 400
            username = str(r.json().get("name") or "").strip()
        except Exception:
            return jsonify({"error": "Failed to fetch Roblox user"}), 400
    if not username:
        return jsonify({"error": "Username not resolved"}), 400

    add_or_update_manual_user(uid, username, status=status, bought_tag=bought)
    auto_sync_state["last_success_ts"] = int(time.time())
    persist_auto_sync_state(force=True)
    add_admin_log(
        "manual_user_add",
        target_uid=uid,
        detail=f"username={username}; status={status}; bought_tag={int(bought)}",
    )
    log_monitor_event("ok", "Manual user added", {"uid": uid, "username": username, "status": status})
    return jsonify({"ok": True, "uid": uid, "username": username})


@app.route("/api/admin/manual_user_remove", methods=["POST"])
def api_admin_manual_user_remove():
    if not is_admin_authenticated():
        return jsonify({"error": "Unauthorized"}), 401
    payload = request.get_json(silent=True) or {}
    uid = str(payload.get("uid", "")).strip()
    if not uid.isdigit():
        return jsonify({"error": "Invalid uid"}), 400
    if not remove_user(uid):
        return jsonify({"error": "User not found"}), 404
    auto_sync_state["last_success_ts"] = int(time.time())
    persist_auto_sync_state(force=True)
    add_admin_log("manual_user_remove", target_uid=uid, detail="User removed from database")
    log_monitor_event("warn", "Manual user removed", {"uid": uid})
    return jsonify({"ok": True, "uid": uid})


@app.route("/api/admin/logs")
def api_admin_logs():
    if not is_admin_authenticated():
        return jsonify({"error": "Unauthorized"}), 401
    try:
        limit = int(request.args.get("limit", 120))
    except Exception:
        limit = 120
    return jsonify({"items": get_admin_logs(limit)})

@app.route("/index")
def index():
    search_type = request.args.get("search_type", "new")
    query = request.args.get("query", "").strip().lower()

    try:
        page = max(1, int(request.args.get("page", 1)))
    except:
        page = 1

    db = get_all_users()  # refresh live DB

    def parse_date_to_ts(date_text, end_of_day=False):
        if not date_text:
            return None
        try:
            dt = datetime.datetime.strptime(str(date_text), "%Y-%m-%d")
            if end_of_day:
                dt = dt + datetime.timedelta(days=1, seconds=-1)
            return int(dt.timestamp())
        except Exception:
            return None

    def chunked_evidence_counts(uid_iterable, chunk_size=700):
        uid_list = [str(uid) for uid in uid_iterable if str(uid).isdigit()]
        if not uid_list:
            return {}
        merged = {}
        for i in range(0, len(uid_list), chunk_size):
            part = uid_list[i:i + chunk_size]
            merged.update(get_evidence_counts(part))
        return merged

    # ---------------- Filter users ----------------
    if search_type == "new":
        filtered = {uid: info for uid, info in db.items() if info["source"] != "Seed List"}
        total_label = f"Total New Users: {len(filtered)}"

    elif search_type == "seed":
        filtered = {uid: info for uid, info in db.items() if info["source"] == "Seed List"}
        total_label = f"Total Seed Users: {len(filtered)}"

    elif search_type == "database":
        filtered = db

        # --- Database filters ---
        length3 = request.args.get("length3")
        length4 = request.args.get("length4")
        length5 = request.args.get("length5")
        letters_only = request.args.get("letters_only")
        status_filter = request.args.get("status_filter", "all").strip().lower()
        bought_filter = request.args.get("bought_filter", "all").strip().lower()
        evidence_filter = request.args.get("evidence_filter", "all").strip().lower()
        added_window = request.args.get("added_window", "all").strip().lower()
        added_from = request.args.get("added_from", "").strip()
        added_to = request.args.get("added_to", "").strip()
        contains_numbers = request.args.get("contains_numbers", "all").strip().lower()
        starts_with = request.args.get("starts_with", "").strip().lower()
        ends_with = request.args.get("ends_with", "").strip().lower()
        min_len_text = request.args.get("min_len", "").strip()
        max_len_text = request.args.get("max_len", "").strip()
        sort_by = request.args.get("sort_by", "username_asc").strip().lower()

        lengths = []
        if length3: lengths.append(3)
        if length4: lengths.append(4)
        if length5: lengths.append(5)

        # status/source filtering
        if status_filter == "seed":
            filtered = {uid: info for uid, info in filtered.items() if info.get("source") == "Seed List"}
        elif status_filter == "new":
            filtered = {uid: info for uid, info in filtered.items() if info.get("source") != "Seed List"}
        elif status_filter == "manual":
            filtered = {uid: info for uid, info in filtered.items() if bool(info.get("manual_add"))}

        if lengths:
            filtered = {uid: info for uid, info in filtered.items()
                        if len(info["username"]) in lengths}

        if letters_only:
            filtered = {uid: info for uid, info in filtered.items()
                        if info["username"].isalpha()}

        if starts_with:
            filtered = {uid: info for uid, info in filtered.items()
                        if str(info.get("username", "")).lower().startswith(starts_with)}
        if ends_with:
            filtered = {uid: info for uid, info in filtered.items()
                        if str(info.get("username", "")).lower().endswith(ends_with)}

        if contains_numbers in {"yes", "no"}:
            want_numbers = contains_numbers == "yes"
            filtered = {
                uid: info
                for uid, info in filtered.items()
                if any(ch.isdigit() for ch in str(info.get("username", ""))) == want_numbers
            }

        try:
            min_len = max(1, int(min_len_text)) if min_len_text else None
        except Exception:
            min_len = None
        try:
            max_len = max(1, int(max_len_text)) if max_len_text else None
        except Exception:
            max_len = None
        if min_len is not None:
            filtered = {uid: info for uid, info in filtered.items() if len(str(info.get("username", ""))) >= min_len}
        if max_len is not None:
            filtered = {uid: info for uid, info in filtered.items() if len(str(info.get("username", ""))) <= max_len}

        now_ts = int(time.time())
        if added_window in {"24h", "7d", "14d", "30d"}:
            days = {"24h": 1, "7d": 7, "14d": 14, "30d": 30}[added_window]
            cutoff = now_ts - (days * 24 * 60 * 60)
            filtered = {
                uid: info for uid, info in filtered.items()
                if int(info.get("first_seen_ts") or 0) >= cutoff
            }

        from_ts = parse_date_to_ts(added_from, end_of_day=False)
        to_ts = parse_date_to_ts(added_to, end_of_day=True)
        if from_ts is not None:
            filtered = {
                uid: info for uid, info in filtered.items()
                if int(info.get("first_seen_ts") or 0) >= from_ts
            }
        if to_ts is not None:
            filtered = {
                uid: info for uid, info in filtered.items()
                if int(info.get("first_seen_ts") or 0) <= to_ts
            }

        if bought_filter == "bought":
            filtered = {uid: info for uid, info in filtered.items() if bool(info.get("bought_tag"))}
        elif bought_filter == "not_bought":
            filtered = {uid: info for uid, info in filtered.items() if not bool(info.get("bought_tag"))}

        evidence_counts_map = {}
        if evidence_filter in {"has_evidence", "no_evidence", "bought_no_evidence"}:
            evidence_counts_map = chunked_evidence_counts(filtered.keys())
            if evidence_filter == "has_evidence":
                filtered = {
                    uid: info for uid, info in filtered.items()
                    if int(evidence_counts_map.get(str(uid), 0)) > 0
                }
            elif evidence_filter == "no_evidence":
                filtered = {
                    uid: info for uid, info in filtered.items()
                    if int(evidence_counts_map.get(str(uid), 0)) == 0
                }
            elif evidence_filter == "bought_no_evidence":
                filtered = {
                    uid: info for uid, info in filtered.items()
                    if bool(info.get("bought_tag")) and int(evidence_counts_map.get(str(uid), 0)) == 0
                }

        total_label = f"Total Users: {len(filtered)}"

        # Sorting
        if sort_by == "username_desc":
            sorted_items = sorted(
                filtered.items(),
                key=lambda it: str(it[1].get("username", "")).lower(),
                reverse=True,
            )
        elif sort_by == "added_newest":
            sorted_items = sorted(
                filtered.items(),
                key=lambda it: int(it[1].get("first_seen_ts") or 0),
                reverse=True,
            )
        elif sort_by == "added_oldest":
            sorted_items = sorted(
                filtered.items(),
                key=lambda it: int(it[1].get("first_seen_ts") or 0),
            )
        elif sort_by == "uid_desc":
            sorted_items = sorted(
                filtered.items(),
                key=lambda it: int(it[0]) if str(it[0]).isdigit() else -1,
                reverse=True,
            )
        elif sort_by == "uid_asc":
            sorted_items = sorted(
                filtered.items(),
                key=lambda it: int(it[0]) if str(it[0]).isdigit() else (10**18),
            )
        else:
            sorted_items = sorted(filtered.items(), key=user_sort_key)

    elif search_type == "individual":
        if query:
            filtered = {uid: info for uid, info in db.items() if query in info["username"].lower()}
        else:
            filtered = {}  # No cards shown by default
        total_label = f"Total Results: {len(filtered)}"

    else:
        filtered = {}
        total_label = ""

    if search_type != "database":
        sorted_items = sorted(filtered.items(), key=user_sort_key)
    total_pages = math.ceil(len(sorted_items) / USERS_PER_PAGE)
    start = (page - 1) * USERS_PER_PAGE
    end = start + USERS_PER_PAGE
    page_items = dict(sorted_items[start:end])

    return render_template(
        "index.html",
        results=page_items,
        search_type=search_type,
        query=query,
        page=page,
        total_pages=total_pages,
        total_label=total_label
    )

@app.route("/database")
def database_page():
    db = get_all_users()

    # Get filters from query string
    length = request.args.get("length", "").strip()
    letters_only = request.args.get("letters_only", "") == "1"

    # Apply filters
    filtered = {}
    for uid, info in db.items():
        username = info["username"]
        if length and len(username) != int(length):
            continue
        if letters_only and not username.isalpha():
            continue
        filtered[uid] = info

    # Pagination
    try:
        page = max(1, int(request.args.get("page", 1)))
    except:
        page = 1

    sorted_items = sorted(filtered.items(), key=user_sort_key)
    total_users = len(sorted_items)
    total_pages = math.ceil(total_users / USERS_PER_PAGE)
    start = (page - 1) * USERS_PER_PAGE
    end = start + USERS_PER_PAGE
    page_items = sorted_items[start:end]

    # Pass filter values back to template to keep selections
    filters = {
        "length": length,
        "letters_only": letters_only
    }

    return render_template(
        "database.html",
        users=page_items,
        total_users=total_users,
        total_pages=total_pages,
        page=page,
        filters=filters
    )

@app.route("/user/<int:uid>")
def user_info(uid):
    stored = get_user(uid)

    if not stored:
        return jsonify({"error": "User not found"}), 404

    live_data = fetch_user_data(uid)
    return jsonify({"stored": stored, **live_data})

@app.route("/users_batch")
def users_batch():
    uids = request.args.get("uids", "").split(",")
    valid_uids = [uid for uid in uids if uid.isdigit()]
    if not valid_uids:
        return jsonify({})

    url = f"https://thumbnails.roblox.com/v1/users/avatar-headshot?userIds={','.join(valid_uids)}&size=150x150&format=Png&isCircular=true"
    avatar_data = {}

    try:
        r = requests.get(url, timeout=5)
        if r.status_code == 200:
            for entry in r.json().get("data", []):
                avatar_data[str(entry["targetId"])] = entry.get("imageUrl", "")
        elif r.status_code == 429:
            log_api_limit("thumbnails.roblox.com", "/v1/users/avatar-headshot")
    except:
        pass

    response = {}
    bought_map = get_bought_tags(valid_uids)
    evidence_counts = get_evidence_counts(valid_uids)
    now = time.time()
    for uid in valid_uids:
        # Fast path: return cached star status only, do not block this endpoint on
        # slow per-user group-role checks. Frontend requests /stars_batch separately.
        cached_star = star_cache.get(str(uid))
        star = bool(cached_star[0]) if cached_star else False
        cached_term = terminated_cache.get(str(uid))
        terminated = (
            bool(cached_term[0])
            if cached_term and now - cached_term[1] < CACHE_EXPIRY
            else False
        )
        response[uid] = {
            "avatar_url": avatar_data.get(uid, ""),
            "is_star_creator": star,
            "is_terminated": terminated,
            "is_bought": bought_map.get(uid, False),
            "evidence_count": int(evidence_counts.get(uid, 0)),
            "has_evidence": int(evidence_counts.get(uid, 0)) > 0,
        }

    app_logger.info("Batch checked user cards: %s users", len(valid_uids))
    return jsonify(response)


@app.route("/terminated_batch")
def terminated_batch():
    uids = request.args.get("uids", "").split(",")
    valid_uids = [uid for uid in uids if uid.isdigit()]
    if not valid_uids:
        return jsonify({})
    force_refresh = request.args.get("force", "0") == "1"

    result = {}
    with ThreadPoolExecutor(max_workers=8) as pool:
        futures = {pool.submit(check_terminated, uid, force_refresh): uid for uid in valid_uids}
        for fut in as_completed(futures):
            uid = futures[fut]
            try:
                result[uid] = bool(fut.result())
            except:
                result[uid] = False

    app_logger.info(
        "Batch checked terminated flags: %s users (force=%s)",
        len(valid_uids),
        "yes" if force_refresh else "no",
    )
    return jsonify(result)


@app.route("/stars_batch")
def stars_batch():
    uids = request.args.get("uids", "").split(",")
    valid_uids = [uid for uid in uids if uid.isdigit()]
    if not valid_uids:
        return jsonify({})

    result = {}

    # Resolve in parallel so star badges populate quickly without blocking page load.
    with ThreadPoolExecutor(max_workers=8) as pool:
        future_map = {pool.submit(check_star_creator, uid): uid for uid in valid_uids}
        for fut in as_completed(future_map):
            uid = future_map[fut]
            try:
                result[uid] = bool(fut.result())
            except:
                result[uid] = False

    app_logger.info("Batch checked star creator roles: %s users", len(valid_uids))
    return jsonify(result)

@app.route("/api/recent_activity")
def recent_activity():
    db = get_all_users()
    new_users = [(uid, user) for uid, user in db.items() if user.get("source") != "Seed List"]
    # Show truly newest additions based on first-seen timestamp.
    new_users_sorted = sorted(
        new_users,
        key=lambda x: (int(x[1].get("first_seen_ts") or 0), int(x[0]) if str(x[0]).isdigit() else 0),
        reverse=True,
    )
    recent = new_users_sorted[:8]

    result = []
    for uid, u in recent:
        data = fetch_user_data(uid)
        result.append({
            "uid": uid,
            "username": u.get("username"),
            "avatar_url": data.get("avatar_url", ""),
            "is_star_creator": data.get("is_star_creator", False),
            "first_seen_ts": int(u.get("first_seen_ts") or 0),
            "is_bought": bool(u.get("bought_tag")),
        })

    return jsonify(result)


@app.route("/api/recent_bought")
def recent_bought():
    db = get_all_users()
    bought_users = [(uid, user) for uid, user in db.items() if bool(user.get("bought_tag"))]
    bought_sorted = sorted(
        bought_users,
        key=lambda x: (int(x[1].get("first_seen_ts") or 0), int(x[0]) if str(x[0]).isdigit() else 0),
        reverse=True,
    )
    recent = bought_sorted[:8]

    result = []
    for uid, u in recent:
        data = fetch_user_data(uid)
        result.append(
            {
                "uid": uid,
                "username": u.get("username"),
                "avatar_url": data.get("avatar_url", ""),
                "is_star_creator": data.get("is_star_creator", False),
                "source": u.get("source"),
                "first_seen_ts": int(u.get("first_seen_ts") or 0),
                "is_bought": True,
            }
        )

    return jsonify(result)


@app.route("/api/live_status")
def live_status():
    global last_seen_db_mtime
    hydrate_auto_sync_state_from_db()
    db = get_all_users()
    total = len(db)
    seed_total = sum(1 for _, u in db.items() if u.get("source") == "Seed List")
    new_total = total - seed_total

    if IS_POSTGRES:
        db_mtime = int(auto_sync_state.get("last_success_ts") or 0)
        if db_mtime <= 0:
            db_mtime = int(time.time())
    else:
        try:
            db_mtime = int(os.path.getmtime(DB_NAME))
        except OSError:
            db_mtime = int(time.time())

    if last_seen_db_mtime is None:
        last_seen_db_mtime = db_mtime
    elif db_mtime != last_seen_db_mtime:
        log_monitor_event(
            "ok",
            "Database file updated",
            {
                "db_mtime": db_mtime,
                "db_updated_at": datetime.datetime.fromtimestamp(db_mtime).strftime("%Y-%m-%d %H:%M:%S"),
            },
        )
        last_seen_db_mtime = db_mtime

    resp = jsonify(
        {
            "database_mode": "Auto Collecting",
            "total_users": total,
            "seed_users": seed_total,
            "new_users": new_total,
            "db_mtime": db_mtime,
            "db_updated_at": datetime.datetime.fromtimestamp(db_mtime).strftime("%Y-%m-%d %H:%M:%S"),
        }
    )
    # Prevent intermediary/browser caching so clients always see fresh cross-user updates.
    resp.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    resp.headers["Pragma"] = "no-cache"
    resp.headers["Expires"] = "0"
    return resp


@app.route("/api/collector_monitor")
def collector_monitor_data():
    hydrate_auto_sync_state_from_db()
    now_ts = int(time.time())
    db = get_all_users()
    total = len(db)
    seed_total = sum(1 for _, u in db.items() if u.get("source") == "Seed List")
    new_total = total - seed_total
    bought_total = sum(1 for _, u in db.items() if bool(u.get("bought_tag")))
    frontier = get_frontier_stats()

    recent_rows = sorted(
        db.items(),
        key=lambda x: (int(x[1].get("first_seen_ts") or 0), int(x[0]) if str(x[0]).isdigit() else 0),
        reverse=True,
    )[:12]
    recent = [
        {
            "uid": str(uid),
            "username": info.get("username", "Unknown"),
            "source": info.get("source", "Unknown"),
            "first_seen_ts": int(info.get("first_seen_ts") or 0),
            "bought_tag": bool(info.get("bought_tag")),
        }
        for uid, info in recent_rows
    ]
    # 24h additions trend (hourly buckets).
    hour = 3600
    aligned_now = now_ts - (now_ts % hour)
    buckets = [0] * 24
    for _, info in db.items():
        ts = int(info.get("first_seen_ts") or 0)
        age = aligned_now - ts
        if 0 <= age < 24 * hour:
            idx = 23 - int(age // hour)
            if 0 <= idx < 24:
                buckets[idx] += 1
    trend_24h = []
    for i in range(24):
        bucket_start = aligned_now - (23 - i) * hour
        trend_24h.append(
            {
                "t": bucket_start,
                "label": datetime.datetime.fromtimestamp(bucket_start).strftime("%H:%M"),
                "count": buckets[i],
            }
        )

    api_hits = [int(ts) for ts in list(auto_sync_state.get("api_limit_hit_timestamps") or []) if str(ts).isdigit()]
    recent_cutoff = now_ts - 600
    api_recent_10m = sum(1 for ts in api_hits if ts >= recent_cutoff)
    endpoint_rows = []
    for name, row in dict(auto_sync_state.get("api_endpoints") or {}).items():
        endpoint_rows.append(
            {
                "name": name,
                "count": int(row.get("count") or 0),
                "last_ts": int(row.get("last_ts") or 0),
                "last_wait_seconds": int(row.get("last_wait_seconds") or 0),
            }
        )
    endpoint_rows.sort(key=lambda r: (r["count"], r["last_ts"]), reverse=True)
    endpoint_rows = endpoint_rows[:8]

    return jsonify(
        {
            "database_mode": "Auto Collecting",
            "auto_sync": auto_sync_state,
            "collector_progress": {
                "current_stage": auto_sync_state.get("current_stage") or "Idle",
                "stage_details": auto_sync_state.get("stage_details") or "",
                "stage_elapsed_seconds": max(
                    0, now_ts - int(auto_sync_state.get("stage_started_ts") or now_ts)
                ),
                "stage_progress_done": int(auto_sync_state.get("stage_progress_done") or 0),
                "stage_progress_total": int(auto_sync_state.get("stage_progress_total") or 0),
                "stage_eta_seconds": int(auto_sync_state.get("stage_eta_seconds") or 0),
                "cycles": int(auto_sync_state.get("cycles") or 0),
                "last_cycle_seed_verified": int(auto_sync_state.get("last_cycle_seed_verified") or 0),
                "last_cycle_frontier_checked": int(auto_sync_state.get("last_cycle_frontier_checked") or 0),
                "last_cycle_frontier_verified": int(auto_sync_state.get("last_cycle_frontier_verified") or 0),
                "last_cycle_scanned_candidates": int(auto_sync_state.get("last_cycle_scanned_candidates") or 0),
                "last_cycle_new_added": int(auto_sync_state.get("last_cycle_new_added") or 0),
                "last_cycle_duration_seconds": int(auto_sync_state.get("last_cycle_duration_seconds") or 0),
                "avg_cycle_duration_seconds": int(auto_sync_state.get("avg_cycle_duration_seconds") or 0),
                "group_scan_name": auto_sync_state.get("group_scan_name") or "",
                "group_scan_index": int(auto_sync_state.get("group_scan_index") or 0),
                "group_scan_total_groups": int(auto_sync_state.get("group_scan_total_groups") or 0),
                "group_member_scanned": int(auto_sync_state.get("group_member_scanned") or 0),
                "group_member_total": int(auto_sync_state.get("group_member_total") or 0),
                "total_new_verified_found": int(auto_sync_state.get("total_new_verified_found") or 0),
                "total_scanned_candidates": int(auto_sync_state.get("total_scanned_candidates") or 0),
                "next_run_in_seconds": max(
                    0, int(auto_sync_state.get("next_run_ts") or now_ts) - now_ts
                ) if not auto_sync_state.get("running") else 0,
            },
            "totals": {
                "total_users": total,
                "seed_users": seed_total,
                "new_users": new_total,
                "bought_users": bought_total,
            },
            "frontier": frontier,
            "cache": {
                "user_cache": len(user_cache),
                "star_cache": len(star_cache),
                "terminated_cache": len(terminated_cache),
            },
            "events": list(monitor_events)[:60],
            "recent": recent,
            "trend_24h": trend_24h,
            "api_health": {
                "total_429": int(auto_sync_state.get("api_limit_total") or 0),
                "recent_10m_429": int(api_recent_10m),
                "endpoints": endpoint_rows,
            },
            "cycle_history": list(auto_sync_state.get("cycle_history") or [])[-20:],
            "server_time": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }
    )


@app.route("/api/collector_events")
def collector_events():
    try:
        since_id = max(0, int(request.args.get("since_id", 0)))
    except Exception:
        since_id = 0
    try:
        limit = max(1, min(120, int(request.args.get("limit", 40))))
    except Exception:
        limit = 40

    all_events = list(monitor_events)
    newer = [e for e in all_events if int(e.get("id", 0)) > since_id]
    newer_sorted = sorted(newer, key=lambda e: int(e.get("id", 0)))
    if len(newer_sorted) > limit:
        newer_sorted = newer_sorted[-limit:]

    latest_id = int(all_events[0].get("id", 0)) if all_events else since_id
    return jsonify({"events": newer_sorted, "latest_id": latest_id})

# Start background worker as soon as app is imported/launched.
start_auto_sync_worker()

# ---------------- Run ----------------
if __name__ == "__main__":
    log_monitor_event("info", "Collector monitor initialized")
    start_auto_sync_worker()
    app.run(debug=True)
