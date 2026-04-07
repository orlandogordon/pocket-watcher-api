"""Auth dependencies for FastAPI routers.

The auth middleware (`src.auth.middleware.AuthMiddleware`) validates the
Bearer token and populates a contextvar for the request. These
dependencies read from that contextvar, so all auth paths agree on the
current user.
"""
from fastapi import Depends, HTTPException, status
from sqlalchemy.orm import Session

from src.auth.context import current_user_id
from src.db.core import UserDB, get_db


def get_current_user_id() -> int:
    """FastAPI dependency — returns the authenticated user id.

    Raises 401 via `current_user_id()` if the middleware did not
    authenticate this request.
    """
    return current_user_id()


def get_current_user(
    user_id: int = Depends(get_current_user_id),
    db: Session = Depends(get_db),
) -> UserDB:
    """FastAPI dependency — returns the full `UserDB` row."""
    user = db.query(UserDB).filter(UserDB.db_id == user_id).first()
    if user is None:
        # Middleware already validated + revocation-checked, so this is
        # very unlikely, but keep the defensive check.
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="User not found",
            headers={"WWW-Authenticate": "Bearer"},
        )
    return user


def require_self_or_admin(target_user_id: int, current_user: UserDB) -> None:
    """Authorize a request as either the target user themselves or an admin.

    Raises 403 otherwise. Not a FastAPI dependency — call from inside the
    route after resolving the target user id (the target may come from
    a path int, or be resolved from a UUID first).
    """
    if current_user.db_id != target_user_id and not current_user.is_admin:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Not authorized",
        )


def get_current_admin_user_id(
    user: UserDB = Depends(get_current_user),
) -> int:
    """FastAPI dependency — requires the authenticated user to be an admin.

    Returns the user's db_id. Raises 403 if the user is authenticated
    but not an admin. (401 is raised upstream by `get_current_user_id`
    if there is no authenticated user.)
    """
    if not user.is_admin:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Admin privileges required",
        )
    return user.db_id
