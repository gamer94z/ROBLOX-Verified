import os
import sqlite3

DB_NAME = os.environ.get("DB_PATH", "verified_users.db")
DATABASE_URL = os.environ.get("DATABASE_URL", "").strip()
IS_POSTGRES = DATABASE_URL.startswith("postgres://") or DATABASE_URL.startswith("postgresql://")

if IS_POSTGRES:
    import psycopg2


def _pg_dsn():
    if DATABASE_URL.startswith("postgres://"):
        return "postgresql://" + DATABASE_URL[len("postgres://"):]
    return DATABASE_URL


def get_connection():
    if IS_POSTGRES:
        return psycopg2.connect(_pg_dsn())
    parent = os.path.dirname(os.path.abspath(DB_NAME))
    if parent:
        os.makedirs(parent, exist_ok=True)
    return sqlite3.connect(DB_NAME)


def _placeholder():
    return "%s" if IS_POSTGRES else "?"


def _get_table_columns(conn, table_name):
    cur = conn.cursor()
    if IS_POSTGRES:
        cur.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = %s
            """,
            (table_name,),
        )
        return [row[0] for row in cur.fetchall()]
    cur.execute(f"PRAGMA table_info({table_name})")
    return [row[1] for row in cur.fetchall()]


def _create_users_table(conn):
    cur = conn.cursor()
    if IS_POSTGRES:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS users (
                user_id TEXT PRIMARY KEY,
                username TEXT NOT NULL,
                status TEXT NOT NULL,
                first_seen_ts BIGINT NOT NULL,
                bought_tag INTEGER NOT NULL DEFAULT 0
            )
            """
        )
    else:
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
    if IS_POSTGRES:
        _create_users_table(conn)
        cols = set(_get_table_columns(conn, "users"))
        if "first_seen_ts" not in cols:
            cur.execute("ALTER TABLE users ADD COLUMN first_seen_ts BIGINT")
            cur.execute(
                "UPDATE users SET first_seen_ts = EXTRACT(EPOCH FROM NOW())::BIGINT WHERE first_seen_ts IS NULL"
            )
        if "bought_tag" not in cols:
            cur.execute("ALTER TABLE users ADD COLUMN bought_tag INTEGER NOT NULL DEFAULT 0")
        conn.commit()
        return

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
    conn = get_connection()
    try:
        _migrate_users_table_if_needed(conn)
    finally:
        conn.close()


def get_all_users():
    conn = get_connection()
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
            "first_seen_ts": int(first_seen_ts),
            "bought_tag": bool(bought_tag),
        }

    return users


def get_user(uid):
    conn = get_connection()
    c = conn.cursor()
    p = _placeholder()
    c.execute(
        f"SELECT username, status, first_seen_ts, bought_tag FROM users WHERE user_id={p}",
        (str(uid),),
    )
    row = c.fetchone()
    conn.close()

    if row:
        return {
            "username": row[0],
            "source": row[1],
            "first_seen_ts": int(row[2]),
            "bought_tag": bool(row[3]),
        }
    return None


def set_bought_tag(uid, enabled):
    conn = get_connection()
    c = conn.cursor()
    p = _placeholder()
    c.execute(
        f"UPDATE users SET bought_tag={p} WHERE user_id={p}",
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

    conn = get_connection()
    c = conn.cursor()
    p = _placeholder()
    placeholders = ",".join(p for _ in valid)
    c.execute(
        f"SELECT user_id, bought_tag FROM users WHERE user_id IN ({placeholders})",
        valid,
    )
    rows = c.fetchall()
    conn.close()
    return {str(uid): bool(flag) for uid, flag in rows}
