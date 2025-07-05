from typing import Optional
from typing_extensions import Self
from pydantic import BaseModel, model_validator, Field
from sqlalchemy.orm import Session
from .core import UserDB, NotFoundError
from datetime import date
from uuid import uuid4, UUID


class User(BaseModel):
    public_id: UUID
    first_name: str
    last_name: str
    email: str
    date_of_birth: date


class UserInput(BaseModel):
    first_name: str
    last_name: str
    email: str
    password: str
    confirm_password: str
    date_of_birth: date

    @model_validator(mode="after")
    def check_passwords_match(self) -> Self:
        if self.password != self.confirm_password:
            raise ValueError("Passwords do not match.")
        return self

class UserCreate(BaseModel):
    public_id: UUID = Field(default_factory=uuid4)
    first_name: str
    last_name: str
    email: str
    password: str
    date_of_birth: date


class UserUpdate(BaseModel):
    first_name: Optional[str]
    last_name: Optional[str]
    email: Optional[str]
    password: Optional[str]
    confirm_password: Optional[str]
    date_of_birth: Optional[date]


def read_db_user(user_id: int, session: Session) -> UserDB:
    db_user = session.query(UserDB).filter(UserDB.id == user_id).first()
    if db_user is None:
        raise NotFoundError(f"User with id {user_id} not found.")
    return db_user


def create_db_user(user: UserCreate, session: Session) -> UserDB:
    db_user = UserDB(**user.model_dump())
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