"""Authentication middleware.

Runs before every request, extracts the Bearer token, validates it, and
populates the per-request user id contextvar. Routes that then call
`current_user_id()` (plain) or `Depends(get_current_user_id)` both read
from the same contextvar.

Public paths (login, user registration, docs, health) are allowed through
without a token; their routes do not call `current_user_id()`.
"""
from datetime import datetime, timezone

import jwt as pyjwt
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request

from src.auth.context import set_current_user_id
from src.auth.jwt import decode_access_token
from src.db.core import session_local, UserDB

# Paths that are reachable without a valid token.
PUBLIC_PATHS: set[str] = {
    "/",
    "/auth/login",
    "/users/login",
    "/openapi.json",
    "/docs",
    "/docs/oauth2-redirect",
    "/redoc",
    "/favicon.ico",
}

# Path prefixes that are reachable without a token.
PUBLIC_PREFIXES: tuple[str, ...] = ()


def _is_public(path: str) -> bool:
    if path in PUBLIC_PATHS:
        return True
    return any(path.startswith(p) for p in PUBLIC_PREFIXES)


class AuthMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        # Always start with a clean context for this request.
        set_current_user_id(None)

        if _is_public(request.url.path):
            return await call_next(request)

        auth_header = request.headers.get("Authorization", "")
        if not auth_header.lower().startswith("bearer "):
            # Leave contextvar unset; route handlers will raise 401 when
            # they try to resolve the user.
            return await call_next(request)

        token = auth_header.split(" ", 1)[1].strip()
        try:
            payload = decode_access_token(token)
        except pyjwt.PyJWTError:
            return await call_next(request)

        sub = payload.get("sub")
        iat_ts = payload.get("iat")
        if sub is None or iat_ts is None:
            return await call_next(request)

        try:
            user_id = int(sub)
        except (TypeError, ValueError):
            return await call_next(request)

        # Look up user + run revocation check. One DB roundtrip per
        # authenticated request — fine for this app size.
        db = session_local()
        try:
            user = db.query(UserDB).filter(UserDB.db_id == user_id).first()
            if user is None:
                return await call_next(request)
            if user.jwt_valid_after is not None:
                iat = datetime.fromtimestamp(int(iat_ts), tz=timezone.utc)
                cutoff = user.jwt_valid_after
                if cutoff.tzinfo is None:
                    cutoff = cutoff.replace(tzinfo=timezone.utc)
                if iat < cutoff:
                    return await call_next(request)
        finally:
            db.close()

        set_current_user_id(user_id)
        return await call_next(request)
