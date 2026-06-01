"""LLM service health probe (#60).

Proactive online/offline signal for the upload/onboarding UI so it can warn,
before an import, that rows will land un-enriched (no merchant/category
suggestions) while the local LLM backend is unreachable. Authed like the rest
of the API; always returns 200 — "offline" is a normal answer, never a 5xx.
"""
from __future__ import annotations

import threading
import time
from datetime import datetime, timezone
from typing import Dict, Optional

from fastapi import APIRouter, Depends

from src.auth.dependencies import get_current_user_id
from src.services.llm_client import get_llm_client

router = APIRouter(prefix="/health", tags=["health"])

# A single real probe is reused for this many seconds, so page polling (or
# refresh-spam) can't each hit the model — and a health check can never contend
# with an in-flight bulk import. Decouples the FE poll interval from the real
# probe rate (#60, Approach A).
_CACHE_TTL_S = 60.0

_lock = threading.Lock()
_cached: Optional[Dict] = None
_cached_monotonic = 0.0


def reset_llm_health_cache() -> None:
    """Clear the cached probe result — for tests and env-var changes."""
    global _cached, _cached_monotonic
    with _lock:
        _cached = None
        _cached_monotonic = 0.0


@router.get("/llm")
def llm_health(_user_id: int = Depends(get_current_user_id)) -> Dict:
    """Report whether the LLM enrichment backend is reachable right now.

    Cached for a short TTL. ``checked_at`` reflects the real probe time, so a
    cache hit reports when the result was actually measured."""
    global _cached, _cached_monotonic
    with _lock:
        now = time.monotonic()
        if _cached is not None and (now - _cached_monotonic) < _CACHE_TTL_S:
            return _cached
        online, model = get_llm_client().health_check()
        _cached = {
            "online": online,
            "model": model,
            "checked_at": datetime.now(timezone.utc).isoformat(),
        }
        _cached_monotonic = now
        return _cached
