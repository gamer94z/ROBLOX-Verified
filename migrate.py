import re
from database import init_db, insert_user

TXT_FILE = "verified_users.txt"
ENTRY_REGEX = re.compile(r"^(?P<username>.+?)\s+\((?P<id>\d+)\)\s+-\s+(?P<source>.+)$")

def migrate():
    init_db()
    with open(TXT_FILE, "r", encoding="utf-8") as f:
        for line in f:
            match = ENTRY_REGEX.match(line.strip())
            if match:
                uid = int(match.group("id"))
                username = match.group("username")
                source = match.group("source")
                insert_user(uid, username, source)
    print("Migration complete.")

if __name__ == "__main__":
    migrate()