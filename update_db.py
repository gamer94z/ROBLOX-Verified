import sqlite3
import re

TXT_FILE = "verified_users.txt"
DB_FILE = "verified_users.db"

conn = sqlite3.connect(DB_FILE)
c = conn.cursor()

# 🔥 Always drop the table first (fixes your error permanently)
c.execute("DROP TABLE IF EXISTS users")

# Create fresh table
c.execute("""
CREATE TABLE users (
    user_id TEXT PRIMARY KEY,
    username TEXT,
    status TEXT
)
""")

pattern = re.compile(r"^(.*?) \((\d+)\) - (.*)$")

with open(TXT_FILE, "r", encoding="utf-8") as file:
    for line in file:
        line = line.strip()

        if not line or "BLADES VERIFIED" in line or "Version:" in line or "Generated:" in line or "-----" in line:
            continue

        match = pattern.match(line)
        if match:
            username = match.group(1)
            user_id = match.group(2)
            status = match.group(3)

            c.execute(
                "INSERT INTO users (user_id, username, status) VALUES (?, ?, ?)",
                (user_id, username, status)
            )

conn.commit()
conn.close()

print("Database completely rebuilt successfully.")