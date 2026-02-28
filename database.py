import os
import sqlite3
import time
import json

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
                bought_tag INTEGER NOT NULL DEFAULT 0,
                manual_add INTEGER NOT NULL DEFAULT 0
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
                bought_tag INTEGER NOT NULL DEFAULT 0,
                manual_add INTEGER NOT NULL DEFAULT 0
            )
            """
        )
    conn.commit()


def _create_candidate_frontier_table(conn):
    cur = conn.cursor()
    if IS_POSTGRES:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS candidate_frontier (
                user_id TEXT PRIMARY KEY,
                score INTEGER NOT NULL DEFAULT 0,
                discovered_ts BIGINT NOT NULL,
                last_checked_ts BIGINT,
                check_count INTEGER NOT NULL DEFAULT 0,
                last_result TEXT NOT NULL DEFAULT 'unknown',
                source TEXT
            )
            """
        )
    else:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS candidate_frontier (
                user_id TEXT PRIMARY KEY,
                score INTEGER NOT NULL DEFAULT 0,
                discovered_ts INTEGER NOT NULL,
                last_checked_ts INTEGER,
                check_count INTEGER NOT NULL DEFAULT 0,
                last_result TEXT NOT NULL DEFAULT 'unknown',
                source TEXT
            )
            """
        )
    conn.commit()


def _migrate_candidate_frontier_if_needed(conn):
    _create_candidate_frontier_table(conn)
    cols = set(_get_table_columns(conn, "candidate_frontier"))
    cur = conn.cursor()
    if "source" not in cols:
        cur.execute("ALTER TABLE candidate_frontier ADD COLUMN source TEXT")
    conn.commit()


def _create_collector_state_table(conn):
    cur = conn.cursor()
    if IS_POSTGRES:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS collector_state (
                id INTEGER PRIMARY KEY,
                state_json TEXT NOT NULL,
                updated_ts BIGINT NOT NULL
            )
            """
        )
    else:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS collector_state (
                id INTEGER PRIMARY KEY,
                state_json TEXT NOT NULL,
                updated_ts INTEGER NOT NULL
            )
            """
        )
    conn.commit()


def _migrate_collector_state_if_needed(conn):
    _create_collector_state_table(conn)


def _create_admin_logs_table(conn):
    cur = conn.cursor()
    if IS_POSTGRES:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS admin_logs (
                id BIGSERIAL PRIMARY KEY,
                action TEXT NOT NULL,
                target_uid TEXT NOT NULL DEFAULT '',
                detail TEXT NOT NULL DEFAULT '',
                created_ts BIGINT NOT NULL
            )
            """
        )
    else:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS admin_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                action TEXT NOT NULL,
                target_uid TEXT NOT NULL DEFAULT '',
                detail TEXT NOT NULL DEFAULT '',
                created_ts INTEGER NOT NULL
            )
            """
        )
    conn.commit()


def _migrate_admin_logs_if_needed(conn):
    _create_admin_logs_table(conn)


def _create_evidence_table(conn):
    cur = conn.cursor()
    if IS_POSTGRES:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS evidence (
                id BIGSERIAL PRIMARY KEY,
                user_id TEXT NOT NULL,
                source_type TEXT NOT NULL DEFAULT 'other',
                title TEXT NOT NULL DEFAULT '',
                url TEXT NOT NULL DEFAULT '',
                note TEXT NOT NULL DEFAULT '',
                created_ts BIGINT NOT NULL,
                updated_ts BIGINT NOT NULL
            )
            """
        )
        cur.execute("CREATE INDEX IF NOT EXISTS evidence_user_idx ON evidence(user_id)")
    else:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS evidence (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id TEXT NOT NULL,
                source_type TEXT NOT NULL DEFAULT 'other',
                title TEXT NOT NULL DEFAULT '',
                url TEXT NOT NULL DEFAULT '',
                note TEXT NOT NULL DEFAULT '',
                created_ts INTEGER NOT NULL,
                updated_ts INTEGER NOT NULL
            )
            """
        )
        cur.execute("CREATE INDEX IF NOT EXISTS evidence_user_idx ON evidence(user_id)")
    conn.commit()


def _migrate_evidence_if_needed(conn):
    _create_evidence_table(conn)


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
        if "manual_add" not in cols:
            cur.execute("ALTER TABLE users ADD COLUMN manual_add INTEGER NOT NULL DEFAULT 0")
        conn.commit()
        return

    cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='users'")
    exists = cur.fetchone() is not None

    if not exists:
        _create_users_table(conn)
        return

    cols = _get_table_columns(conn, "users")
    expected = {"user_id", "username", "status", "first_seen_ts", "bought_tag", "manual_add"}

    if set(cols) == expected:
        return

    if {"user_id", "username", "status"}.issubset(set(cols)) and "first_seen_ts" not in cols:
        cur.execute("ALTER TABLE users ADD COLUMN first_seen_ts INTEGER")
        cur.execute(
            "UPDATE users SET first_seen_ts = strftime('%s','now') WHERE first_seen_ts IS NULL"
        )
        if "bought_tag" not in cols:
            cur.execute("ALTER TABLE users ADD COLUMN bought_tag INTEGER NOT NULL DEFAULT 0")
        if "manual_add" not in cols:
            cur.execute("ALTER TABLE users ADD COLUMN manual_add INTEGER NOT NULL DEFAULT 0")
        conn.commit()
        return

    if {"user_id", "username", "status", "first_seen_ts"}.issubset(set(cols)) and "bought_tag" not in cols:
        cur.execute("ALTER TABLE users ADD COLUMN bought_tag INTEGER NOT NULL DEFAULT 0")
        conn.commit()
        return

    if {"user_id", "username", "status", "first_seen_ts", "bought_tag"}.issubset(set(cols)) and "manual_add" not in cols:
        cur.execute("ALTER TABLE users ADD COLUMN manual_add INTEGER NOT NULL DEFAULT 0")
        conn.commit()
        return

    cur.execute("ALTER TABLE users RENAME TO users_legacy")
    _create_users_table(conn)

    legacy_cols = _get_table_columns(conn, "users_legacy")

    if {"user_id", "username", "status"}.issubset(set(legacy_cols)):
        bought_expr = "COALESCE(bought_tag, 0)" if "bought_tag" in legacy_cols else "0"
        manual_expr = "COALESCE(manual_add, 0)" if "manual_add" in legacy_cols else "0"
        cur.execute(
            """
            INSERT OR IGNORE INTO users (user_id, username, status, first_seen_ts, bought_tag, manual_add)
            SELECT
                CAST(user_id AS TEXT),
                username,
                status,
                COALESCE(first_seen_ts, strftime('%s','now')),
                """
            + bought_expr
            + """,
                """
            + manual_expr
            + """
            FROM users_legacy
            """
        )
    elif {"id", "username", "source"}.issubset(set(legacy_cols)):
        cur.execute(
            """
            INSERT OR IGNORE INTO users (user_id, username, status, first_seen_ts, bought_tag, manual_add)
            SELECT
                CAST(id AS TEXT),
                username,
                source,
                strftime('%s','now'),
                0,
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
        _migrate_candidate_frontier_if_needed(conn)
        _migrate_collector_state_if_needed(conn)
        _migrate_admin_logs_if_needed(conn)
        _migrate_evidence_if_needed(conn)
    finally:
        conn.close()


def get_all_users():
    conn = get_connection()
    c = conn.cursor()
    c.execute("SELECT user_id, username, status, first_seen_ts, bought_tag, manual_add FROM users")
    rows = c.fetchall()
    conn.close()

    users = {}
    for user_id, username, status, first_seen_ts, bought_tag, manual_add in rows:
        uid = int(user_id) if str(user_id).isdigit() else user_id
        users[uid] = {
            "username": username,
            "source": status,
            "first_seen_ts": int(first_seen_ts),
            "bought_tag": bool(bought_tag),
            "manual_add": bool(manual_add),
        }

    return users


def get_user(uid):
    conn = get_connection()
    c = conn.cursor()
    p = _placeholder()
    c.execute(
        f"SELECT username, status, first_seen_ts, bought_tag, manual_add FROM users WHERE user_id={p}",
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
            "manual_add": bool(row[4]),
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


def upsert_frontier_candidates(candidates, source, score_boost=1, now_ts=None):
    if not candidates:
        return
    now_ts = int(now_ts or time.time())
    rows = [(str(uid), int(score_boost), int(now_ts), source) for uid in candidates if str(uid).isdigit()]
    if not rows:
        return

    conn = get_connection()
    cur = conn.cursor()
    p = _placeholder()
    if IS_POSTGRES:
        cur.executemany(
            f"""
            INSERT INTO candidate_frontier (user_id, score, discovered_ts, source)
            VALUES ({p}, {p}, {p}, {p})
            ON CONFLICT(user_id) DO UPDATE SET
                score = candidate_frontier.score + EXCLUDED.score,
                source = COALESCE(EXCLUDED.source, candidate_frontier.source)
            """,
            rows,
        )
    else:
        cur.executemany(
            f"""
            INSERT INTO candidate_frontier (user_id, score, discovered_ts, source)
            VALUES ({p}, {p}, {p}, {p})
            ON CONFLICT(user_id) DO UPDATE SET
                score = score + excluded.score,
                source = COALESCE(excluded.source, candidate_frontier.source)
            """,
            rows,
        )
    conn.commit()
    conn.close()


def pull_frontier_candidates(limit=400, non_verified_cooldown_seconds=7 * 24 * 60 * 60, now_ts=None):
    now_ts = int(now_ts or time.time())
    conn = get_connection()
    cur = conn.cursor()
    p = _placeholder()
    cur.execute(
        f"""
        SELECT user_id
        FROM candidate_frontier
        WHERE (
            last_checked_ts IS NULL
            OR (
                (last_result IS NULL OR last_result <> 'verified')
                AND last_checked_ts <= {p}
            )
        )
        ORDER BY score DESC, COALESCE(last_checked_ts, 0) ASC, discovered_ts ASC
        LIMIT {p}
        """,
        (int(now_ts - int(non_verified_cooldown_seconds)), int(limit)),
    )
    rows = [str(r[0]) for r in cur.fetchall()]
    conn.close()
    return rows


def mark_frontier_checked(results_map, now_ts=None):
    if not results_map:
        return
    now_ts = int(now_ts or time.time())
    conn = get_connection()
    cur = conn.cursor()
    p = _placeholder()
    sql = (
        f"""
        UPDATE candidate_frontier
        SET
            last_checked_ts = {p},
            check_count = check_count + 1,
            last_result = {p},
            score = CASE
                WHEN {p} = 'verified' THEN score + 5
                ELSE score - 1
            END
        WHERE user_id = {p}
        """
    )
    rows = []
    for uid, is_verified in results_map.items():
        uid_str = str(uid)
        if not uid_str.isdigit():
            continue
        result = "verified" if bool(is_verified) else "not_verified"
        rows.append((int(now_ts), result, result, uid_str))
    if rows:
        cur.executemany(sql, rows)
    conn.commit()
    conn.close()


def get_frontier_stats():
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM candidate_frontier")
    total = int(cur.fetchone()[0])
    cur.execute("SELECT COUNT(*) FROM candidate_frontier WHERE last_result='verified'")
    verified = int(cur.fetchone()[0])
    cur.execute("SELECT COUNT(*) FROM candidate_frontier WHERE last_result='not_verified'")
    not_verified = int(cur.fetchone()[0])
    conn.close()
    return {
        "total": total,
        "verified": verified,
        "not_verified": not_verified,
    }


def save_collector_state(state):
    payload = json.dumps(state or {}, separators=(",", ":"), ensure_ascii=True)
    now_ts = int(time.time())
    conn = get_connection()
    cur = conn.cursor()
    p = _placeholder()
    if IS_POSTGRES:
        cur.execute(
            f"""
            INSERT INTO collector_state (id, state_json, updated_ts)
            VALUES (1, {p}, {p})
            ON CONFLICT (id) DO UPDATE SET
                state_json = EXCLUDED.state_json,
                updated_ts = EXCLUDED.updated_ts
            """,
            (payload, now_ts),
        )
    else:
        cur.execute(
            f"""
            INSERT INTO collector_state (id, state_json, updated_ts)
            VALUES (1, {p}, {p})
            ON CONFLICT(id) DO UPDATE SET
                state_json = excluded.state_json,
                updated_ts = excluded.updated_ts
            """,
            (payload, now_ts),
        )
    conn.commit()
    conn.close()


def load_collector_state():
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("SELECT state_json FROM collector_state WHERE id = 1")
    row = cur.fetchone()
    conn.close()
    if not row or not row[0]:
        return {}
    try:
        data = json.loads(row[0])
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def get_evidence_for_user(uid):
    uid_text = str(uid)
    conn = get_connection()
    cur = conn.cursor()
    p = _placeholder()
    cur.execute(
        f"""
        SELECT id, user_id, source_type, title, url, note, created_ts, updated_ts
        FROM evidence
        WHERE user_id={p}
        ORDER BY updated_ts DESC, id DESC
        """,
        (uid_text,),
    )
    rows = cur.fetchall()
    conn.close()
    result = []
    for row in rows:
        result.append(
            {
                "id": int(row[0]),
                "user_id": str(row[1]),
                "source_type": row[2] or "other",
                "title": row[3] or "",
                "url": row[4] or "",
                "note": row[5] or "",
                "created_ts": int(row[6] or 0),
                "updated_ts": int(row[7] or 0),
            }
        )
    return result


def add_evidence(uid, source_type, title, url, note):
    uid_text = str(uid)
    now_ts = int(time.time())
    source = (source_type or "other").strip().lower()[:32]
    conn = get_connection()
    cur = conn.cursor()
    p = _placeholder()
    if IS_POSTGRES:
        cur.execute(
            f"""
            INSERT INTO evidence (user_id, source_type, title, url, note, created_ts, updated_ts)
            VALUES ({p}, {p}, {p}, {p}, {p}, {p}, {p})
            RETURNING id
            """,
            (uid_text, source or "other", title or "", url or "", note or "", now_ts, now_ts),
        )
        row = cur.fetchone()
        evidence_id = int(row[0]) if row else 0
    else:
        cur.execute(
            f"""
            INSERT INTO evidence (user_id, source_type, title, url, note, created_ts, updated_ts)
            VALUES ({p}, {p}, {p}, {p}, {p}, {p}, {p})
            """,
            (uid_text, source or "other", title or "", url or "", note or "", now_ts, now_ts),
        )
        evidence_id = int(cur.lastrowid or 0)
    conn.commit()
    conn.close()
    return evidence_id


def update_evidence(evidence_id, source_type, title, url, note):
    now_ts = int(time.time())
    source = (source_type or "other").strip().lower()[:32]
    conn = get_connection()
    cur = conn.cursor()
    p = _placeholder()
    cur.execute(
        f"""
        UPDATE evidence
        SET source_type={p}, title={p}, url={p}, note={p}, updated_ts={p}
        WHERE id={p}
        """,
        (source or "other", title or "", url or "", note or "", now_ts, int(evidence_id)),
    )
    changed = cur.rowcount > 0
    conn.commit()
    conn.close()
    return changed


def delete_evidence(evidence_id):
    conn = get_connection()
    cur = conn.cursor()
    p = _placeholder()
    cur.execute(f"DELETE FROM evidence WHERE id={p}", (int(evidence_id),))
    changed = cur.rowcount > 0
    conn.commit()
    conn.close()
    return changed


def delete_all_evidence_for_user(uid):
    conn = get_connection()
    cur = conn.cursor()
    p = _placeholder()
    cur.execute(f"DELETE FROM evidence WHERE user_id={p}", (str(uid),))
    deleted = int(cur.rowcount or 0)
    conn.commit()
    conn.close()
    return deleted


def get_evidence_counts(uids):
    valid = [str(uid) for uid in uids if str(uid).isdigit()]
    if not valid:
        return {}
    conn = get_connection()
    cur = conn.cursor()
    p = _placeholder()
    placeholders = ",".join(p for _ in valid)
    cur.execute(
        f"""
        SELECT user_id, COUNT(*)
        FROM evidence
        WHERE user_id IN ({placeholders})
        GROUP BY user_id
        """,
        valid,
    )
    rows = cur.fetchall()
    conn.close()
    return {str(uid): int(count or 0) for uid, count in rows}


def add_or_update_manual_user(uid, username, status="Newly Added", bought_tag=False):
    uid_text = str(uid)
    now_ts = int(time.time())
    status_text = "Seed List" if str(status).lower().startswith("seed") else "Newly Added"
    bought = 1 if bought_tag else 0
    conn = get_connection()
    cur = conn.cursor()
    p = _placeholder()
    if IS_POSTGRES:
        cur.execute(
            f"""
            INSERT INTO users (user_id, username, status, first_seen_ts, bought_tag, manual_add)
            VALUES ({p}, {p}, {p}, {p}, {p}, 1)
            ON CONFLICT(user_id) DO UPDATE SET
                username = EXCLUDED.username,
                status = EXCLUDED.status,
                bought_tag = EXCLUDED.bought_tag,
                manual_add = 1
            """,
            (uid_text, username, status_text, now_ts, bought),
        )
    else:
        cur.execute(
            f"""
            INSERT INTO users (user_id, username, status, first_seen_ts, bought_tag, manual_add)
            VALUES ({p}, {p}, {p}, {p}, {p}, 1)
            ON CONFLICT(user_id) DO UPDATE SET
                username = excluded.username,
                status = excluded.status,
                bought_tag = excluded.bought_tag,
                manual_add = 1
            """,
            (uid_text, username, status_text, now_ts, bought),
        )
    conn.commit()
    conn.close()
    return True


def remove_user(uid):
    uid_text = str(uid)
    conn = get_connection()
    cur = conn.cursor()
    p = _placeholder()
    cur.execute(f"DELETE FROM users WHERE user_id={p}", (uid_text,))
    deleted_users = int(cur.rowcount or 0)
    cur.execute(f"DELETE FROM evidence WHERE user_id={p}", (uid_text,))
    cur.execute(f"DELETE FROM candidate_frontier WHERE user_id={p}", (uid_text,))
    conn.commit()
    conn.close()
    return deleted_users > 0


def add_admin_log(action, target_uid="", detail=""):
    now_ts = int(time.time())
    conn = get_connection()
    cur = conn.cursor()
    p = _placeholder()
    cur.execute(
        f"""
        INSERT INTO admin_logs (action, target_uid, detail, created_ts)
        VALUES ({p}, {p}, {p}, {p})
        """,
        (str(action or "")[:120], str(target_uid or "")[:40], str(detail or "")[:3000], now_ts),
    )
    conn.commit()
    conn.close()


def get_admin_logs(limit=120):
    lim = max(1, min(500, int(limit)))
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        f"""
        SELECT id, action, target_uid, detail, created_ts
        FROM admin_logs
        ORDER BY id DESC
        LIMIT {lim}
        """
    )
    rows = cur.fetchall()
    conn.close()
    result = []
    for row in rows:
        result.append(
            {
                "id": int(row[0]),
                "action": row[1] or "",
                "target_uid": row[2] or "",
                "detail": row[3] or "",
                "created_ts": int(row[4] or 0),
            }
        )
    return result
