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
from database import init_db, get_all_users, get_user, set_bought_tag, get_bought_tags
from update_db import parse_verified_users_file, sync_database, TXT_FILE

app = Flask(__name__)
app.secret_key = os.environ.get("FLASK_SECRET_KEY", "change-this-secret")

# ---------------- Database ----------------
init_db()
db = get_all_users()

# ---------------- Constants ----------------
BASE_USERS = "https://users.roblox.com/v1/users"
BASE_FRIENDS = "https://friends.roblox.com/v1/users"
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

user_cache = {}
star_cache = {}
terminated_cache = {}
monitor_events = deque(maxlen=250)
last_seen_db_mtime = None
api_limit_log_last = {}
auto_sync_state = {
    "enabled": AUTO_SYNC_ENABLED,
    "interval_seconds": AUTO_SYNC_INTERVAL_SECONDS,
    "running": False,
    "last_started_ts": None,
    "last_success_ts": None,
    "last_error": None,
    "next_run_ts": None,
    "cycles": 0,
}
auto_sync_thread_started = False


def log_monitor_event(level, message, details=None):
    monitor_events.appendleft(
        {
            "ts": int(time.time()),
            "level": level,
            "message": message,
            "details": details or {},
        }
    )


class MonitorLogHandler(logging.Handler):
    def emit(self, record):
        try:
            monitor_events.appendleft(
                {
                    "ts": int(time.time()),
                    "level": record.levelname.lower(),
                    "message": record.getMessage(),
                    "details": {},
                }
            )
        except Exception:
            pass


app_logger = logging.getLogger("gamers_network")
app_logger.setLevel(logging.INFO)
if not app_logger.handlers:
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(logging.Formatter("[%(asctime)s] %(levelname)s: %(message)s"))
    app_logger.addHandler(stream_handler)
if not any(isinstance(h, MonitorLogHandler) for h in app_logger.handlers):
    app_logger.addHandler(MonitorLogHandler())


def log_api_limit(service, endpoint, details=None):
    key = f"{service}:{endpoint}"
    now = time.time()
    # Throttle duplicate limit logs so monitor feed stays readable.
    if now - api_limit_log_last.get(key, 0) < 60:
        return
    api_limit_log_last[key] = now
    log_monitor_event(
        "warn",
        f"API rate limit hit: {service} {endpoint}",
        details or {},
    )
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


def fetch_friend_candidates(uid):
    try:
        r = requests.get(f"{BASE_FRIENDS}/{uid}/friends", timeout=8)
        if r.status_code == 429:
            log_api_limit("friends.roblox.com", f"/v1/users/{uid}/friends")
            return []
        if r.status_code != 200:
            return []
        return r.json().get("data", []) or []
    except Exception:
        return []


def verify_badges_batch(user_ids):
    verified = {}
    if not user_ids:
        return verified
    for i in range(0, len(user_ids), AUTO_SYNC_VERIFY_BATCH_SIZE):
        chunk = user_ids[i:i + AUTO_SYNC_VERIFY_BATCH_SIZE]
        try:
            r = requests.post(BASE_USERS, json={"userIds": chunk}, timeout=10)
            if r.status_code == 429:
                log_api_limit("users.roblox.com", "/v1/users (POST verify)")
                continue
            if r.status_code != 200:
                continue
            for row in r.json().get("data", []):
                uid = str(row.get("id"))
                has_badge = bool(row.get("hasVerifiedBadge", False) or row.get("isVerified", False))
                if has_badge:
                    verified[uid] = row.get("name") or uid
        except Exception:
            continue
    return verified


def discover_verified_from_network(seed_uids):
    discovered = {}
    scanned_candidates = {}
    seen_candidate_ids = set()
    seed_cap = min(len(seed_uids), AUTO_SYNC_SEED_LIMIT)

    for idx, uid in enumerate(seed_uids[:seed_cap], start=1):
        friends = fetch_friend_candidates(uid)
        app_logger.info("Collector scanned %s/%s seeds", idx, seed_cap)
        for f in friends:
            cid = str(f.get("id") or "")
            if not cid.isdigit() or cid in seen_candidate_ids:
                continue
            seen_candidate_ids.add(cid)
            scanned_candidates[cid] = f.get("name") or cid
            if bool(f.get("hasVerifiedBadge", False) or f.get("isVerified", False)):
                discovered[cid] = f.get("name") or cid

    unresolved = [int(uid) for uid in scanned_candidates.keys() if uid not in discovered]
    discovered.update(verify_badges_batch(unresolved))
    return discovered, len(scanned_candidates)


def run_auto_sync_cycle():
    if os.path.exists(TXT_FILE):
        parsed = parse_verified_users_file(TXT_FILE)
    else:
        parsed = {}

    # Never run destructive sync with an empty snapshot.
    if not parsed:
        parsed = load_parsed_from_db_snapshot()

    existing_db = get_all_users()
    seed_uids = [
        int(uid)
        for uid, info in existing_db.items()
        if str(uid).isdigit() and info.get("source") == "Seed List"
    ]
    if not seed_uids:
        seed_uids = [int(uid) for uid in existing_db.keys() if str(uid).isdigit()]

    discovered, scanned_count = discover_verified_from_network(seed_uids)
    added = 0
    for uid, username in discovered.items():
        if uid not in parsed:
            parsed[uid] = {"username": username, "raw_source": "Newly Added"}
            added += 1

    write_verified_users_file(parsed)
    sync_database(parsed)
    log_monitor_event(
        "ok",
        "Collector cycle finished",
        {
            "scanned_candidates": scanned_count,
            "discovered_verified": len(discovered),
            "new_added": added,
            "total_snapshot": len(parsed),
        },
    )
    return len(parsed), added, scanned_count


def auto_sync_loop():
    app_logger.info(
        "Auto-sync worker started (interval=%ss, enabled=%s)",
        AUTO_SYNC_INTERVAL_SECONDS,
        "yes" if AUTO_SYNC_ENABLED else "no",
    )
    while True:
        started_ts = int(time.time())
        auto_sync_state["running"] = True
        auto_sync_state["last_started_ts"] = started_ts
        auto_sync_state["next_run_ts"] = None
        auto_sync_state["last_error"] = None
        auto_sync_state["cycles"] = int(auto_sync_state["cycles"] or 0) + 1

        try:
            parsed_count, added_count, scanned_count = run_auto_sync_cycle()
            auto_sync_state["last_success_ts"] = int(time.time())
            app_logger.info(
                "Auto-sync completed successfully (%s parsed users, %s added, %s scanned)",
                parsed_count,
                added_count,
                scanned_count,
            )
        except Exception as exc:
            auto_sync_state["last_error"] = str(exc)
            app_logger.exception("Auto-sync cycle failed")
        finally:
            auto_sync_state["running"] = False
            auto_sync_state["next_run_ts"] = int(time.time()) + AUTO_SYNC_INTERVAL_SECONDS

        time.sleep(AUTO_SYNC_INTERVAL_SECONDS)


def start_auto_sync_worker():
    global auto_sync_thread_started
    if auto_sync_thread_started or not AUTO_SYNC_ENABLED:
        return

    # Avoid duplicate workers under Flask debug reloader parent process.
    if app.debug and os.environ.get("WERKZEUG_RUN_MAIN") != "true":
        return

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
    }

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
    return render_template("collector_monitor.html")


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
    log_monitor_event(
        "warn" if enabled else "info",
        "Bought tag updated",
        {"uid": uid, "bought_tag": bool(enabled)},
    )

    return jsonify({"ok": True, "uid": uid, "bought_tag": enabled})

@app.route("/index")
def index():
    search_type = request.args.get("search_type", "new")
    query = request.args.get("query", "").strip().lower()

    try:
        page = max(1, int(request.args.get("page", 1)))
    except:
        page = 1

    db = get_all_users()  # refresh live DB

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

        lengths = []
        if length3: lengths.append(3)
        if length4: lengths.append(4)
        if length5: lengths.append(5)

        if lengths:
            filtered = {uid: info for uid, info in filtered.items()
                        if len(info["username"]) in lengths}

        if letters_only:
            filtered = {uid: info for uid, info in filtered.items()
                        if info["username"].isalpha()}

        total_label = f"Total Users: {len(filtered)}"

    elif search_type == "individual":
        if query:
            filtered = {uid: info for uid, info in db.items() if query in info["username"].lower()}
        else:
            filtered = {}  # No cards shown by default
        total_label = f"Total Results: {len(filtered)}"

    else:
        filtered = {}
        total_label = ""

    total_pages = math.ceil(len(filtered) / USERS_PER_PAGE)
    start = (page - 1) * USERS_PER_PAGE
    end = start + USERS_PER_PAGE
    page_items = dict(list(filtered.items())[start:end])

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

    total_users = len(filtered)
    total_pages = math.ceil(total_users / USERS_PER_PAGE)
    start = (page - 1) * USERS_PER_PAGE
    end = start + USERS_PER_PAGE
    page_items = list(filtered.items())[start:end]

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
    db = get_all_users()
    total = len(db)
    seed_total = sum(1 for _, u in db.items() if u.get("source") == "Seed List")
    new_total = total - seed_total

    try:
        db_mtime = int(os.path.getmtime("verified_users.db"))
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

    return jsonify(
        {
            "database_mode": "Auto Collecting",
            "total_users": total,
            "seed_users": seed_total,
            "new_users": new_total,
            "db_mtime": db_mtime,
            "db_updated_at": datetime.datetime.fromtimestamp(db_mtime).strftime("%Y-%m-%d %H:%M:%S"),
        }
    )


@app.route("/api/collector_monitor")
def collector_monitor_data():
    db = get_all_users()
    total = len(db)
    seed_total = sum(1 for _, u in db.items() if u.get("source") == "Seed List")
    new_total = total - seed_total
    bought_total = sum(1 for _, u in db.items() if bool(u.get("bought_tag")))

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
    now_ts = int(time.time())
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

    return jsonify(
        {
            "database_mode": "Auto Collecting",
            "auto_sync": auto_sync_state,
            "totals": {
                "total_users": total,
                "seed_users": seed_total,
                "new_users": new_total,
                "bought_users": bought_total,
            },
            "cache": {
                "user_cache": len(user_cache),
                "star_cache": len(star_cache),
                "terminated_cache": len(terminated_cache),
            },
            "events": list(monitor_events)[:60],
            "recent": recent,
            "trend_24h": trend_24h,
            "server_time": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }
    )

# Start background worker as soon as app is imported/launched.
start_auto_sync_worker()

# ---------------- Run ----------------
if __name__ == "__main__":
    log_monitor_event("info", "Collector monitor initialized")
    start_auto_sync_worker()
    app.run(debug=True)
