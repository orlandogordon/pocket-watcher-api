"""Request/response access logging.

Generates a ``request_id`` for every request (honoring an inbound
``X-Request-ID`` if a proxy supplies one), logs an entry line on arrival and a
completion line with status + duration on the way out, and echoes the
``X-Request-ID`` back on the response.

Ordering matters: this middleware must be registered as the **innermost** one
(added before ``AuthMiddleware`` in ``main.py``). ``BaseHTTPMiddleware``
contextvars only propagate downward, so being innermost lets the completion log
inherit the ``user_id`` that the outer ``AuthMiddleware`` set for the request.

Request *bodies* are intentionally not read or logged — doing so would consume
the stream (breaking uploads) and risk leaking secrets (login, password
change). Only the declared body size (Content-Length) is recorded.
"""
import time
import uuid

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request

from src.logging_config import get_logger
from src.request_context import set_request_id

logger = get_logger("request")


def _completion_level(status_code: int) -> int:
    if status_code >= 500:
        return 40  # ERROR
    if status_code >= 400:
        return 30  # WARNING
    return 20  # INFO


class RequestLoggingMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        request_id = request.headers.get("X-Request-ID") or uuid.uuid4().hex
        set_request_id(request_id)

        content_length = request.headers.get("content-length")
        logger.info(
            "request.start",
            extra={
                "method": request.method,
                "path": request.url.path,
                "query": request.url.query or None,
                "request_bytes": int(content_length) if content_length else None,
            },
        )

        start = time.perf_counter()
        try:
            response = await call_next(request)
        except Exception:
            duration_ms = round((time.perf_counter() - start) * 1000, 2)
            # The exception will be turned into a 500 by the registered
            # handler; log it here with the request context attached.
            logger.error(
                "request.failed",
                extra={
                    "method": request.method,
                    "path": request.url.path,
                    "duration_ms": duration_ms,
                },
                exc_info=True,
            )
            raise

        duration_ms = round((time.perf_counter() - start) * 1000, 2)
        logger.log(
            _completion_level(response.status_code),
            "request.complete",
            extra={
                "method": request.method,
                "path": request.url.path,
                "status_code": response.status_code,
                "duration_ms": duration_ms,
            },
        )
        response.headers["X-Request-ID"] = request_id
        return response
