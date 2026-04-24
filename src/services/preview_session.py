import json
from typing import Optional, Dict, Any, List, Tuple
from datetime import datetime, timedelta
from uuid import uuid4
import redis
from src.logging_config import get_logger

logger = get_logger(__name__)

SESSION_PREFIX = "preview:session:"
DEFAULT_EXPIRY_SECONDS = 43200  # 12 hours


def create_preview_session(
    r: redis.Redis,
    user_id: int,
    institution: str,
    account_id: Optional[int],
    filename: str,
    source_type: str,
    rejected: Dict[str, list],
    ready_to_import: Dict[str, list],
    summary: Dict[str, Any],
    account_info: Optional[Dict] = None,
    llm_summary: Optional[Dict[str, Any]] = None,
    expiry: int = DEFAULT_EXPIRY_SECONDS,
) -> Tuple[str, str]:
    """
    Create a preview session in Redis.

    Returns:
        (session_id, expires_at_iso)
    """
    session_id = str(uuid4())
    expires_at = datetime.utcnow() + timedelta(seconds=expiry)

    session_data = {
        "user_id": user_id,
        "institution": institution,
        "account_id": account_id,
        "filename": filename,
        "source_type": source_type,
        "account_info": account_info,
        "rejected": rejected,
        "ready_to_import": ready_to_import,
        "summary": summary,
        "llm_summary": llm_summary,
        "created_at": datetime.utcnow().isoformat(),
        "expires_at": expires_at.isoformat(),
    }

    key = f"{SESSION_PREFIX}{session_id}"
    r.setex(key, expiry, json.dumps(session_data, default=str))
    logger.info(f"Created preview session {session_id} for user {user_id} ({expiry}s TTL)")
    return session_id, expires_at.isoformat()


def get_preview_session(
    r: redis.Redis,
    session_id: str,
    user_id: int,
) -> Optional[Dict]:
    """
    Retrieve a preview session. Returns None if not found, expired, or wrong user.
    """
    key = f"{SESSION_PREFIX}{session_id}"
    data = r.get(key)
    if not data:
        return None
    session = json.loads(data)
    if session["user_id"] != user_id:
        logger.warning(f"User {user_id} tried to access session {session_id} owned by {session['user_id']}")
        return None
    return session


def save_preview_session(
    r: redis.Redis,
    session_id: str,
    session: Dict,
) -> bool:
    """
    Save updated session data back to Redis, preserving remaining TTL.
    """
    key = f"{SESSION_PREFIX}{session_id}"
    ttl = r.ttl(key)
    if ttl <= 0:
        return False
    r.setex(key, ttl, json.dumps(session, default=str))
    return True


def delete_preview_session(
    r: redis.Redis,
    session_id: str,
    user_id: int,
) -> bool:
    """Delete a preview session after ownership check."""
    session = get_preview_session(r, session_id, user_id)
    if not session:
        return False
    key = f"{SESSION_PREFIX}{session_id}"
    r.delete(key)
    logger.info(f"Deleted preview session {session_id}")
    return True


def list_user_sessions(r: redis.Redis, user_id: int) -> List[Dict]:
    """List all active preview sessions for a user via Redis SCAN."""
    sessions = []
    for key in r.scan_iter(match=f"{SESSION_PREFIX}*"):
        data = r.get(key)
        if not data:
            continue
        session = json.loads(data)
        if session["user_id"] != user_id:
            continue
        # key may be bytes or str depending on Redis client config
        key_str = key.decode() if isinstance(key, bytes) else key
        session_id = key_str.removeprefix(SESSION_PREFIX)
        sessions.append({
            "preview_session_id": session_id,
            "institution": session["institution"],
            "filename": session["filename"],
            "created_at": session["created_at"],
            "expires_at": session["expires_at"],
            "summary": session["summary"],
        })
    return sessions


def extend_session_expiry(
    r: redis.Redis,
    session_id: str,
    user_id: int,
    additional_seconds: int = DEFAULT_EXPIRY_SECONDS,
) -> Optional[str]:
    """
    Extend session TTL. Returns new expires_at ISO string, or None if session not found.
    """
    session = get_preview_session(r, session_id, user_id)
    if not session:
        return None
    key = f"{SESSION_PREFIX}{session_id}"
    new_expires_at = datetime.utcnow() + timedelta(seconds=additional_seconds)
    session["expires_at"] = new_expires_at.isoformat()
    r.setex(key, additional_seconds, json.dumps(session, default=str))
    return new_expires_at.isoformat()
