import sqlite3
import os

DB_NAME = os.environ.get("DB_PATH", "verified_users.db")


def _ensure_parent_dir(path):
    parent = os.path.dirname(os.path.abspath(path))
    if parent:
        os.makedirs(parent, exist_ok=True)


def _get_table_columns(conn, table_name):
    cur = conn.cursor()
    cur.execute(f"PRAGMA table_info({table_name})")
    return [row[1] for row in cur.fetchall()]


def _create_users_table(conn):
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS users (
            user_id TEXT PRIMARY KEY,
            username TEXT NOT NULL,
            status TEXT NOT NULL,
            first_seen_ts INTEGER NOT NULL,
            bought_tag INTEGER NOT NULL DEFAULT 0
        )
        """
    )
    conn.commit()


def _migrate_users_table_if_needed(conn):
    cur = conn.cursor()
    cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='users'")
    exists = cur.fetchone() is not None

    if not exists:
        _create_users_table(conn)
        return

    cols = _get_table_columns(conn, "users")
    expected = {"user_id", "username", "status", "first_seen_ts", "bought_tag"}

    if set(cols) == expected:
        return

    if {"user_id", "username", "status"}.issubset(set(cols)) and "first_seen_ts" not in cols:
        cur.execute("ALTER TABLE users ADD COLUMN first_seen_ts INTEGER")
        cur.execute(
            "UPDATE users SET first_seen_ts = strftime('%s','now') WHERE first_seen_ts IS NULL"
        )
        if "bought_tag" not in cols:
            cur.execute("ALTER TABLE users ADD COLUMN bought_tag INTEGER NOT NULL DEFAULT 0")
        conn.commit()
        return

    if {"user_id", "username", "status", "first_seen_ts"}.issubset(set(cols)) and "bought_tag" not in cols:
        cur.execute("ALTER TABLE users ADD COLUMN bought_tag INTEGER NOT NULL DEFAULT 0")
        conn.commit()
        return

    cur.execute("ALTER TABLE users RENAME TO users_legacy")
    _create_users_table(conn)

    legacy_cols = _get_table_columns(conn, "users_legacy")

    if {"user_id", "username", "status"}.issubset(set(legacy_cols)):
        bought_expr = "COALESCE(bought_tag, 0)" if "bought_tag" in legacy_cols else "0"
        cur.execute(
            """
            INSERT OR IGNORE INTO users (user_id, username, status, first_seen_ts, bought_tag)
            SELECT
                CAST(user_id AS TEXT),
                username,
                status,
                COALESCE(first_seen_ts, strftime('%s','now')),
                """
            + bought_expr
            + """
            FROM users_legacy
            """
        )
    elif {"id", "username", "source"}.issubset(set(legacy_cols)):
        cur.execute(
            """
            INSERT OR IGNORE INTO users (user_id, username, status, first_seen_ts, bought_tag)
            SELECT
                CAST(id AS TEXT),
                username,
                source,
                strftime('%s','now'),
                0
            FROM users_legacy
            """
        )

    cur.execute("DROP TABLE users_legacy")
    conn.commit()


def init_db():
    _ensure_parent_dir(DB_NAME)
    conn = sqlite3.connect(DB_NAME)
    try:
        _migrate_users_table_if_needed(conn)
    finally:
        conn.close()


def get_all_users():
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute("SELECT user_id, username, status, first_seen_ts, bought_tag FROM users")
    rows = c.fetchall()
    conn.close()

    users = {}
    for user_id, username, status, first_seen_ts, bought_tag in rows:
        uid = int(user_id) if str(user_id).isdigit() else user_id
        users[uid] = {
            "username": username,
            "source": status,
            "first_seen_ts": first_seen_ts,
            "bought_tag": bool(bought_tag),
        }

    return users


def get_user(uid):
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute(
        "SELECT username, status, first_seen_ts, bought_tag FROM users WHERE user_id=?",
        (str(uid),),
    )
    row = c.fetchone()
    conn.close()

    if row:
        return {
            "username": row[0],
            "source": row[1],
            "first_seen_ts": row[2],
            "bought_tag": bool(row[3]),
        }
    return None


def set_bought_tag(uid, enabled):
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute(
        "UPDATE users SET bought_tag=? WHERE user_id=?",
        (1 if enabled else 0, str(uid)),
    )
    changed = c.rowcount > 0
    conn.commit()
    conn.close()
    return changed


def get_bought_tags(uids):
    valid = [str(uid) for uid in uids if str(uid).isdigit()]
    if not valid:
        return {}

    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    placeholders = ",".join("?" for _ in valid)
    c.execute(
        f"SELECT user_id, bought_tag FROM users WHERE user_id IN ({placeholders})",
        valid,
    )
    rows = c.fetchall()
    conn.close()
    return {str(uid): bool(flag) for uid, flag in rows}
