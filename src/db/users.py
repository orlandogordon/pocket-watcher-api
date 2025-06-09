from typing import Optional
from pydantic import BaseModel
from sqlalchemy.orm import Session
from .core import UserDB, NotFoundError


class User(BaseModel):
    id: int
    name: str
    description: Optional[str] = None


class UserCreate(BaseModel):
    name: str
    description: Optional[str] = None


class UserUpdate(BaseModel):
    name: Optional[str] = None
    description: Optional[str] = None


def read_db_user(user_id: int, session: Session) -> UserDB:
    db_user = session.query(UserDB).filter(UserDB.id == user_id).first()
    if db_user is None:
        raise NotFoundError(f"User with id {user_id} not found.")
    return db_user


def create_db_user(user: UserCreate, session: Session) -> UserDB:
    db_user = UserDB(**user.model_dump(exclude_none=True))
    session.add(db_user)
    session.commit()
    session.refresh(db_user)

    return db_user


def update_db_user(user_id: int, user: UserUpdate, session: Session) -> UserDB:
    db_user = read_db_user(user_id, session)
    for key, value in user.model_dump(exclude_none=True).items():
        setattr(db_user, key, value)
    session.commit()
    session.refresh(db_user)

    return db_user


def delete_db_user(user_id: int, session: Session) -> UserDB:
    db_user = read_db_user(user_id, session)
    session.delete(db_user)
    session.commit()
    return db_user