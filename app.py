from flask import Flask, render_template, request, jsonify
import datetime
import requests
import math
import time
from database import init_db, get_all_users, get_user  # your existing DB functions

app = Flask(__name__)

# ---------------- Database ----------------
init_db()
db = get_all_users()

# ---------------- Constants ----------------
BASE_USERS = "https://users.roblox.com/v1/users"
BASE_FRIENDS = "https://friends.roblox.com/v1/users"
VIDEO_STARS_GROUP_ID = 4199740
USERS_PER_PAGE = 30
CACHE_EXPIRY = 3600  # 1 hour cache
DEV_UID = "10006170169"

user_cache = {}  # store live Roblox data to avoid repeated requests

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
                join_date = datetime.datetime.fromisoformat(join_date.replace("Z", "")).strftime("%Y-%m-%d")
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

# ---------------- Routes ----------------

# Home / landing page
@app.route("/")
@app.route("/home")
def home():
    last_updated = datetime.datetime.now().strftime("%Y-%m-%d %H:%M")
    return render_template("home.html", last_updated=last_updated, DEV_UID=DEV_UID)

# User listing / index page
@app.route("/index")
def index():
    search_type = request.args.get("search_type", "new")
    query = request.args.get("query", "").strip().lower()

    try:
        page = max(1, int(request.args.get("page", 1)))
    except:
        page = 1

    filtered = {}

    if search_type == "new":
        filtered = {uid: info for uid, info in db.items() if info["source"] != "Seed List"}
    elif search_type == "seed":
        filtered = {uid: info for uid, info in db.items() if info["source"] == "Seed List"}
    elif search_type == "individual" and query:
        filtered = {uid: info for uid, info in db.items() if query in info["username"].lower()}

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
    )

# ---------------- Full user info for modal ----------------
@app.route("/user/<int:uid>")
def user_info(uid):
    stored = get_user(uid)

    # Developer fallback
    if not stored and str(uid) == DEV_UID:
        try:
            r = requests.get(f"{BASE_USERS}/{uid}", timeout=5).json()
            join_date = r.get("created")
            if join_date:
                join_date = datetime.datetime.fromisoformat(join_date.replace("Z","")).strftime("%Y-%m-%d")
            stored = {"username": r.get("name"), "source": "Developer"}

            avatar_req = requests.get(
                f"https://thumbnails.roblox.com/v1/users/avatar-headshot?userIds={uid}&size=150x150&format=Png&isCircular=true",
                timeout=5
            ).json()
            avatar_url = avatar_req["data"][0]["imageUrl"]

            live_data = {
                "live": {"displayName": r.get("displayName"), "joined": join_date},
                "stats": {"friends":0, "followers":0, "following":0},
                "avatar_url": avatar_url,
                "is_star_creator": False,
                "profile_url": f"https://www.roblox.com/users/{uid}/profile"
            }
            return jsonify({"stored": stored, **live_data})
        except Exception as e:
            print("Developer fetch error:", e)
            return jsonify({"error": "Developer info fetch failed"}), 500

    if not stored:
        return jsonify({"error": "User not found"}), 404

    live_data = fetch_user_data(uid)
    return jsonify({"stored": stored, **live_data})

# ---------------- Batch avatar fetch ----------------
@app.route("/users_batch")
def users_batch():
    uids = request.args.get("uids", "").split(",")
    valid_uids = [uid for uid in uids if uid.isdigit()]
    if not valid_uids: return jsonify({})

    url = f"https://thumbnails.roblox.com/v1/users/avatar-headshot?userIds={','.join(valid_uids)}&size=150x150&format=Png&isCircular=true"
    avatar_data = {}
    try:
        r = requests.get(url, timeout=5)
        if r.status_code == 200:
            for entry in r.json().get("data", []):
                avatar_data[str(entry["targetId"])] = entry.get("imageUrl", "")
    except: pass

    response = {}
    for uid in valid_uids:
        response[uid] = {"avatar_url": avatar_data.get(uid, ""), "is_star_creator": False}
    return jsonify(response)

# ---------------- Recent Activity API ----------------
@app.route("/api/recent_activity")
def recent_activity():
    # Only include users marked as "Newly Added"
    new_users = [(uid, user) for uid, user in db.items() if user["source"] != "Seed List"]

    # Sort by the timestamp they were added, newest first
    new_users_sorted = sorted(new_users, key=lambda x: x[1].get("added_timestamp", 0), reverse=True)

    # Limit to 5 users
    recent = new_users_sorted[:5]

    # Include avatar URL and star creator status from cache/live data
    result = []
    for uid, u in recent:
        data = fetch_user_data(uid)  # ensures avatar_url and is_star_creator
        result.append({
            "uid": uid,
            "username": u["username"],
            "avatar_url": data.get("avatar_url", ""),
            "is_star_creator": data.get("is_star_creator", False)
        })

    return jsonify(result)

# ---------------- Run ----------------
if __name__ == "__main__":
    app.run(debug=True)