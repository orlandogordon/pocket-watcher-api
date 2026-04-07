"""Per-request auth context.

The auth middleware validates the JWT and populates `_current_user_id` for
the duration of the request. Both the FastAPI dependencies and the bare
`current_user_id()` helper read from this contextvar, so there's a single
source of truth per request.
"""
from contextvars import ContextVar
from typing import Optional

from fastapi import HTTPException, status

_current_user_id: ContextVar[Optional[int]] = ContextVar(
    "pocket_watcher_current_user_id", default=None
)


def set_current_user_id(user_id: Optional[int]) -> None:
    _current_user_id.set(user_id)


def current_user_id() -> int:
    """Return the authenticated user id for the current request.

    Raises 401 if the middleware did not authenticate this request. Use
    this from inside route handlers that don't want to add a FastAPI
    dependency parameter.
    """
    uid = _current_user_id.get()
    if uid is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Not authenticated",
            headers={"WWW-Authenticate": "Bearer"},
        )
    return uid
