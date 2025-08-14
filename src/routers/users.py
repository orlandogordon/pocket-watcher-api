from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session
from typing import List
from uuid import UUID

from src.crud import crud_user
from src.models import user as user_models
from src.db.core import get_db, NotFoundError

router = APIRouter(
    prefix="/users",
    tags=["users"],
)

@router.post("/", response_model=user_models.UserResponse, status_code=status.HTTP_201_CREATED)
def create_user(user: user_models.UserCreate, db: Session = Depends(get_db)):
    """
    Create a new user.
    """
    try:
        db_user = crud_user.create_db_user(db=db, user_data=user)
    except ValueError as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))
    return db_user

@router.post("/login")
def login_for_access_token(user_login: user_models.UserLogin, db: Session = Depends(get_db)):
    """
    Authenticate user and return a token.
    (Note: Token implementation is a placeholder).
    """
    user = crud_user.authenticate_user(db, email=user_login.email, password=user_login.password)
    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect email or password",
            headers={"WWW-Authenticate": "Bearer"},
        )
    # In a real application, you would create and return a JWT token here.
    return {"access_token": user.username, "token_type": "bearer"}

@router.get("/", response_model=List[user_models.UserResponse])
def read_users(skip: int = 0, limit: int = 100, db: Session = Depends(get_db)):
    """
    Retrieve a list of users.
    """
    users = crud_user.read_db_users(db, skip=skip, limit=limit)
    return users

@router.get("/{user_id}", response_model=user_models.UserResponse)
def read_user(user_id: int, db: Session = Depends(get_db)):
    """
    Retrieve a single user by their integer ID.
    """
    db_user = crud_user.read_db_user(db, user_id=user_id)
    if db_user is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="User not found")
    return db_user

@router.get("/uuid/{user_uuid}", response_model=user_models.UserResponse)
def read_user_by_uuid(user_uuid: UUID, db: Session = Depends(get_db)):
    """
    Retrieve a single user by their UUID.
    """
    db_user = crud_user.read_db_user(db, user_uuid=user_uuid)
    if db_user is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="User not found")
    return db_user

@router.put("/{user_id}", response_model=user_models.UserResponse)
def update_user(user_id: int, user: user_models.UserUpdate, db: Session = Depends(get_db)):
    """
    Update a user's profile.
    """
    try:
        updated_user = crud_user.update_db_user(db=db, user_id=user_id, user_updates=user)
    except NotFoundError as e:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(e))
    except ValueError as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))
    return updated_user

@router.delete("/{user_id}", response_model=user_models.UserResponse)
def delete_user(user_id: int, db: Session = Depends(get_db)):
    """
    Delete a user.
    """
    db_user = crud_user.read_db_user(db, user_id=user_id)
    if db_user is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="User not found")
    
    try:
        crud_user.delete_db_user(db=db, user_id=user_id)
    except ValueError as e:
        # This might happen if there are constraints preventing deletion.
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(e))
    return db_user

@router.post("/{user_id}/change-password", status_code=status.HTTP_200_OK)
def change_password(user_id: int, password_change: user_models.PasswordChange, db: Session = Depends(get_db)):
    """
    Change a user's password.
    """
    try:
        crud_user.change_user_password(db=db, user_id=user_id, password_change=password_change)
    except NotFoundError as e:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(e))
    except ValueError as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))
    
    return {"message": "Password changed successfully"}