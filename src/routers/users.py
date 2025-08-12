from fastapi import APIRouter, HTTPException, Request
from fastapi.params import Depends
from sqlalchemy.orm import Session
from src.db.core import NotFoundError, get_db
from src.models.user import UserCreate, UserUpdate, UserResponse
from src.crud.crud_user import create_db_user, read_db_user, update_db_user, delete_db_user

# from .limiter import limiter


router = APIRouter(
    prefix="/users",
)


# @limiter.limit("1/second")
@router.post("/")
def create_user(request: Request, user: UserCreate, db: Session = Depends(get_db)) -> UserResponse:
    db_user = create_db_user(db, user)
    return UserResponse(**db_user.__dict__)


@router.get("/{user_id}")
def read_user(request: Request, user_id: str, db: Session = Depends(get_db)) -> UserResponse:
    try:
        db_user = read_db_user(db, user_id=int(user_id))
    except NotFoundError as e:
        raise HTTPException(status_code=404) from e
    return UserResponse(**db_user.__dict__)


@router.get("/{user_id}/automations")
def read_user_automations(
    request: Request, user_id: int, db: Session = Depends(get_db)
) -> list[UserResponse]:
    # try:
    #     transactions = read_db_transactions_for_user(user_id, db)
    # except NotFoundError as e:
    #     raise HTTPException(status_code=404) from e
    # return [Automation(**automation.__dict__) for automation in automations]
    return []


@router.put("/{user_id}")
def update_user(request: Request, user_id: str, user: UserUpdate, db: Session = Depends(get_db)) -> UserResponse:
    try:
        db_user = update_db_user(db, user_id=int(user_id), user_updates=user)
    except NotFoundError as e:
        raise HTTPException(status_code=404) from e
    return UserResponse(**db_user.__dict__)


@router.delete("/{user_id}")
def delete_user(request: Request, user_id: str, db: Session = Depends(get_db)) -> UserResponse:
    try:
        db_user = delete_db_user(db, user_id=int(user_id))
    except NotFoundError as e:
        raise HTTPException(status_code=404) from e
    return UserResponse(**db_user.__dict__)
