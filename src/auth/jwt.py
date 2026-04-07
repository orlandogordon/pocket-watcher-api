"""JWT encode/decode helpers.

Uses HS256 with the symmetric secret from `src.auth.config`. Payload includes
`sub` (user id as string), `exp`, `iat`, and `jti` (unique token id).

`iat` is paired with `UserDB.jwt_valid_after` in the auth dependency to enable
per-user "log out everywhere" without a revocation table.

`jti` is unused today but cheap to add now; it enables future per-token
revocation (e.g., kill one device's session) without a schema migration.
"""
from datetime import datetime, timedelta, timezone
from uuid import uuid4

import jwt as pyjwt

from src.auth.config import JWT_ALGORITHM, JWT_EXPIRY_DAYS, JWT_SECRET


def create_access_token(user_id: int) -> tuple[str, datetime]:
    """Mint a JWT for the given user id.

    Returns (token, expires_at) so callers can echo the expiry to clients.
    """
    now = datetime.now(timezone.utc)
    expires_at = now + timedelta(days=JWT_EXPIRY_DAYS)
    payload = {
        "sub": str(user_id),
        "iat": int(now.timestamp()),
        "exp": int(expires_at.timestamp()),
        "jti": str(uuid4()),
    }
    token = pyjwt.encode(payload, JWT_SECRET, algorithm=JWT_ALGORITHM)
    return token, expires_at


def decode_access_token(token: str) -> dict:
    """Decode and verify a JWT. Raises pyjwt exceptions on failure."""
    return pyjwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGORITHM])
