import hashlib
import secrets
from fastapi import Header, HTTPException, Depends
from app.db import pool


def generate_api_key() -> str:
    return f"jq_{secrets.token_urlsafe(32)}"

def hash_api_key(api_key:str) -> str:
    return hashlib.sha256(api_key.encode()).hexdigest()

def get_current_key_scopes(authorization: str | None = Header(default=None)) -> list[str]:
    if authorization is None or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Missing or malformed Authorization header. Expected: Bearer <api_key>")

    raw_key = authorization.removeprefix("Bearer ")
    key_hash = hash_api_key(raw_key)

    with pool.connection() as conn:
        row = conn.execute(
            "SELECT scopes, revoked FROM api_keys WHERE key_hash = %s",
            (key_hash,),
        ).fetchone()

    if row is None:
        raise HTTPException(status_code=401, detail="Invalid API key")

    scopes, revoked = row
    if revoked:
        raise HTTPException(status_code=401, detail="API key has been revoked")

    return scopes


def require_scope(required_scope: str):
    def checker(scopes: list[str] = Depends(get_current_key_scopes)) -> list[str]:
        if required_scope not in scopes:
            raise HTTPException(status_code=403, detail=f"This operation requires the '{required_scope}' scope")
        return scopes
    return checker