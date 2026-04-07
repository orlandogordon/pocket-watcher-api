"""Auth dependencies for FastAPI routers.

The auth middleware (`src.auth.middleware.AuthMiddleware`) validates the
Bearer token and populates a contextvar for the request. These
dependencies read from that contextvar, so all auth paths agree on the
current user.
"""
from fastapi import Depends
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
        from fastapi import HTTPException, status
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="User not found",
            headers={"WWW-Authenticate": "Bearer"},
        )
    return user
