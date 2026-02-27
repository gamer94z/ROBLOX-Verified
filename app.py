from flask import Flask, render_template, request, jsonify, session, redirect, url_for
import datetime
import requests
import math
import time
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from database import init_db, get_all_users, get_user, set_bought_tag, get_bought_tags

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

user_cache = {}
star_cache = {}
terminated_cache = {}


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
    except:
        pass

    # --- Avatar ---
    try:
        url = f"https://thumbnails.roblox.com/v1/users/avatar-headshot?userIds={uid}&size=150x150&format=Png&isCircular=true"
        r = requests.get(url, timeout=5)
        if r.status_code == 200:
            avatar_url = r.json()["data"][0]["imageUrl"]
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
    cached = star_cache.get(str(uid))
    if cached and now - cached[1] < CACHE_EXPIRY:
        return cached[0]

    star = False
    try:
        r = requests.get(f"https://groups.roblox.com/v1/users/{uid}/groups/roles", timeout=5)
        if r.status_code == 200:
            for group in r.json().get("data", []):
                if group.get("group") and group["group"].get("id") == VIDEO_STARS_GROUP_ID:
                    star = True
                    break
    except:
        pass

    star_cache[str(uid)] = (star, now)
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
    terminated_data = {}

    try:
        r = requests.get(url, timeout=5)
        if r.status_code == 200:
            for entry in r.json().get("data", []):
                avatar_data[str(entry["targetId"])] = entry.get("imageUrl", "")
    except:
        pass

    returned_ids = set()
    unresolved_false = []
    try:
        user_ids = [int(uid) for uid in valid_uids]
        r = requests.post(BASE_USERS, json={"userIds": user_ids}, timeout=5)
        if r.status_code == 200:
            for entry in r.json().get("data", []):
                entry_uid = str(entry.get("id"))
                returned_ids.add(entry_uid)
                is_term = bool(entry.get("terminated", False) or entry.get("isBanned", False))
                terminated_data[entry_uid] = is_term
                if not is_term:
                    unresolved_false.append(entry_uid)
    except:
        pass

    # Fallback: resolve missing users in parallel to reduce timeout misses.
    missing_uids = [uid for uid in valid_uids if uid not in returned_ids]
    if missing_uids:
        with ThreadPoolExecutor(max_workers=8) as pool:
            futures = {pool.submit(check_terminated, uid): uid for uid in missing_uids}
            for fut in as_completed(futures):
                uid = futures[fut]
                try:
                    terminated_data[uid] = bool(fut.result())
                except:
                    terminated_data[uid] = False

    # Verification pass for entries that came back as non-terminated in batch.
    # This catches occasional false negatives from the bulk endpoint.
    if unresolved_false:
        with ThreadPoolExecutor(max_workers=8) as pool:
            futures = {pool.submit(check_terminated, uid, True): uid for uid in unresolved_false}
            for fut in as_completed(futures):
                uid = futures[fut]
                try:
                    terminated_data[uid] = bool(fut.result())
                except:
                    pass

    response = {}
    bought_map = get_bought_tags(valid_uids)
    for uid in valid_uids:
        # Fast path: return cached star status only, do not block this endpoint on
        # slow per-user group-role checks. Frontend requests /stars_batch separately.
        cached_star = star_cache.get(str(uid))
        star = bool(cached_star[0]) if cached_star else False
        response[uid] = {
            "avatar_url": avatar_data.get(uid, ""),
            "is_star_creator": star,
            "is_terminated": terminated_data.get(uid, False),
            "is_bought": bought_map.get(uid, False),
        }

    return jsonify(response)


@app.route("/terminated_batch")
def terminated_batch():
    uids = request.args.get("uids", "").split(",")
    valid_uids = [uid for uid in uids if uid.isdigit()]
    if not valid_uids:
        return jsonify({})

    result = {}
    with ThreadPoolExecutor(max_workers=8) as pool:
        futures = {pool.submit(check_terminated, uid, True): uid for uid in valid_uids}
        for fut in as_completed(futures):
            uid = futures[fut]
            try:
                result[uid] = bool(fut.result())
            except:
                result[uid] = False

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
    db = get_all_users()
    total = len(db)
    seed_total = sum(1 for _, u in db.items() if u.get("source") == "Seed List")
    new_total = total - seed_total

    try:
        db_mtime = int(os.path.getmtime("verified_users.db"))
    except OSError:
        db_mtime = int(time.time())

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

# ---------------- Run ----------------
if __name__ == "__main__":
    app.run(debug=True)
