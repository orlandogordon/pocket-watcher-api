from fastapi import APIRouter, HTTPException, Request
from fastapi.params import Depends
from sqlalchemy.orm import Session
from ..db.core import NotFoundError, get_db
from ..db.users import (
    User,
    UserCreate,
    UserUpdate,
    read_db_user,
    create_db_user,
    update_db_user,
    delete_db_user,
)
# from .limiter import limiter


router = APIRouter(
    prefix="/users",
)


# @limiter.limit("1/second")
@router.post("/")
def create_user(request: Request, user: UserCreate, db: Session = Depends(get_db)) -> User:
    db_user = create_db_user(user, db)
    return User(**db_user.__dict__)


@router.get("/{user_id}")
def read_user(request: Request, user_id: int, db: Session = Depends(get_db)) -> User:
    try:
        db_user = read_db_user(user_id, db)
    except NotFoundError as e:
        raise HTTPException(status_code=404) from e
    return User(**db_user.__dict__)


@router.get("/{user_id}/automations")
def read_user_automations(
    request: Request, user_id: int, db: Session = Depends(get_db)
) -> list[User]:
    # try:
    #     transactions = read_db_transactions_for_user(user_id, db)
    # except NotFoundError as e:
    #     raise HTTPException(status_code=404) from e
    # return [Automation(**automation.__dict__) for automation in automations]
    return []


@router.put("/{user_id}")
def update_user(request: Request, user_id: int, user: UserUpdate, db: Session = Depends(get_db)) -> User:
    try:
        db_user = update_db_user(user_id, user, db)
    except NotFoundError as e:
        raise HTTPException(status_code=404) from e
    return User(**db_user.__dict__)


@router.delete("/{user_id}")
def delete_user(request: Request, user_id: int, db: Session = Depends(get_db)) -> User:
    try:
        db_user = delete_db_user(user_id, db)
    except NotFoundError as e:
        raise HTTPException(status_code=404) from e
    return User(**db_user.__dict__)