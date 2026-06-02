"""Per-request observability context.

Holds the ``request_id`` for the current request so it can be stamped onto
every log record (see ``logging_config.ContextFilter``) and echoed back in
responses. Mirrors the auth ``user_id`` contextvar in ``src/auth/context.py``;
kept separate because it is not an auth concern.
"""
from contextvars import ContextVar
from typing import Optional

_request_id: ContextVar[Optional[str]] = ContextVar(
    "pocket_watcher_request_id", default=None
)


def set_request_id(request_id: Optional[str]) -> None:
    _request_id.set(request_id)


def get_request_id() -> Optional[str]:
    return _request_id.get()
