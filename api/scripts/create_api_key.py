import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from app.auth import generate_api_key, hash_api_key
from app.db import pool

def create_key(name: str, scopes: list[str]):
    raw_key = generate_api_key()
    key_hash = hash_api_key(raw_key)

    with pool.connection() as conn:
        conn.execute(
            "INSERT INTO api_keys (key_hash, name, scopes) VALUES (%s, %s, %s)",
            (key_hash, name, scopes),
        )

    print(f"Created key '{name}' with scopes {scopes}")
    print(f"RAW KEY (save this now, it will not be shown again): {raw_key}")

if __name__ == "__main__":
    name = sys.argv[1]
    scopes = sys.argv[2].split(",")
    create_key(name, scopes)