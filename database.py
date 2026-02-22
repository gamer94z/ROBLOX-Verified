import sqlite3

DB_NAME = "verified_users.db"

def init_db():
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute("""
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY,
            username TEXT NOT NULL,
            source TEXT NOT NULL
        )
    """)
    conn.commit()
    conn.close()

def insert_user(uid, username, source):
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute(
        "INSERT OR IGNORE INTO users (id, username, source) VALUES (?, ?, ?)",
        (uid, username, source)
    )
    conn.commit()
    conn.close()

def get_all_users():
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute("SELECT id, username, source FROM users")
    rows = c.fetchall()
    conn.close()
    return {row[0]: {"username": row[1], "source": row[2]} for row in rows}

def get_user(uid):
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute("SELECT username, source FROM users WHERE id=?", (uid,))
    row = c.fetchone()
    conn.close()
    if row:
        return {"username": row[0], "source": row[1]}
    return None