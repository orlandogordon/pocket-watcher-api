"""Auth endpoints: login and current-user lookup."""
from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session

from src.auth.dependencies import get_current_user
from src.auth.jwt import create_access_token
from src.crud.crud_user import authenticate_user
from src.db.core import UserDB, get_db
from src.logging_config import get_logger
from src.models.user import TokenResponse, UserLogin, UserResponse

logger = get_logger(__name__)

router = APIRouter(prefix="/auth", tags=["auth"])


@router.post("/login", response_model=TokenResponse)
def login(credentials: UserLogin, db: Session = Depends(get_db)) -> TokenResponse:
    user = authenticate_user(db, email=credentials.email, password=credentials.password)
    if user is None:
        logger.info("Failed login attempt for email=%s", credentials.email)
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid email or password",
            headers={"WWW-Authenticate": "Bearer"},
        )

    token, expires_at = create_access_token(user.db_id)
    logger.info("Login success user_id=%s", user.db_id)
    return TokenResponse(access_token=token, expires_at=expires_at)


@router.get("/me", response_model=UserResponse)
def read_current_user(user: UserDB = Depends(get_current_user)) -> UserDB:
    return user
