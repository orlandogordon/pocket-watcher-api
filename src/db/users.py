from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError
from typing import Optional, List
from uuid import uuid4
from datetime import datetime
import bcrypt
import hashlib

# Import your database models and Pydantic models
from .core import UserDB, NotFoundError
from pydantic import BaseModel, Field, model_validator, field_validator
from typing import Optional
from datetime import date, datetime
from uuid import UUID
from typing_extensions import Self
import re


# ===== USER PYDANTIC MODELS =====

class UserCreate(BaseModel):
    email: str = Field(..., description="User's email address")
    username: str = Field(..., min_length=3, max_length=100, description="Username (3-100 characters)")
    password: str = Field(..., min_length=8, description="Password (minimum 8 characters)")
    confirm_password: str = Field(..., description="Password confirmation")
    first_name: Optional[str] = Field(None, max_length=100)
    last_name: Optional[str] = Field(None, max_length=100)
    date_of_birth: Optional[date] = Field(None, description="Date of birth")

    @field_validator('email')
    @classmethod
    def validate_email(cls, v: str) -> str:
        email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        if not re.match(email_pattern, v):
            raise ValueError('Invalid email format')
        return v.lower().strip()

    @field_validator('username')
    @classmethod
    def validate_username(cls, v: str) -> str:
        if not re.match(r'^[a-zA-Z0-9_-]+$', v):
            raise ValueError('Username can only contain letters, numbers, hyphens, and underscores')
        return v.lower().strip()

    @field_validator('password')
    @classmethod
    def validate_password(cls, v: str) -> str:
        if len(v) < 8:
            raise ValueError('Password must be at least 8 characters long')
        if not re.search(r'[A-Z]', v):
            raise ValueError('Password must contain at least one uppercase letter')
        if not re.search(r'[a-z]', v):
            raise ValueError('Password must contain at least one lowercase letter')
        if not re.search(r'\d', v):
            raise ValueError('Password must contain at least one number')
        return v

    @model_validator(mode="after")
    def check_passwords_match(self) -> Self:
        if self.password != self.confirm_password:
            raise ValueError("Passwords do not match")
        return self


class UserUpdate(BaseModel):
    """Update user profile - all fields optional"""
    email: Optional[str] = None
    username: Optional[str] = Field(None, min_length=3, max_length=100)
    first_name: Optional[str] = Field(None, max_length=100)
    last_name: Optional[str] = Field(None, max_length=100)
    date_of_birth: Optional[date] = None

    @field_validator('email')
    @classmethod
    def validate_email(cls, v: Optional[str]) -> Optional[str]:
        if v is None:
            return v
        email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        if not re.match(email_pattern, v):
            raise ValueError('Invalid email format')
        return v.lower().strip()

    @field_validator('username')
    @classmethod
    def validate_username(cls, v: Optional[str]) -> Optional[str]:
        if v is None:
            return v
        if not re.match(r'^[a-zA-Z0-9_-]+$', v):
            raise ValueError('Username can only contain letters, numbers, hyphens, and underscores')
        return v.lower().strip()


class UserResponse(BaseModel):
    """User data returned to client - no sensitive info"""
    id: UUID
    email: str
    username: str
    first_name: Optional[str]
    last_name: Optional[str]
    date_of_birth: Optional[date]
    last_login_at: Optional[datetime]
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True


class UserLogin(BaseModel):
    email: str = Field(..., description="User's email or username")
    password: str = Field(..., description="User's password")

    @field_validator('email')
    @classmethod
    def validate_login_identifier(cls, v: str) -> str:
        return v.lower().strip()


class PasswordChange(BaseModel):
    current_password: str = Field(..., description="Current password")
    new_password: str = Field(..., min_length=8, description="New password (minimum 8 characters)")
    confirm_new_password: str = Field(..., description="Confirm new password")

    @field_validator('new_password')
    @classmethod
    def validate_new_password(cls, v: str) -> str:
        if len(v) < 8:
            raise ValueError('Password must be at least 8 characters long')
        if not re.search(r'[A-Z]', v):
            raise ValueError('Password must contain at least one uppercase letter')
        if not re.search(r'[a-z]', v):
            raise ValueError('Password must contain at least one lowercase letter')
        if not re.search(r'\d', v):
            raise ValueError('Password must contain at least one number')
        return v

    @model_validator(mode="after")
    def check_passwords_match(self) -> Self:
        if self.new_password != self.confirm_new_password:
            raise ValueError("New passwords do not match")
        return self


# ===== PASSWORD HASHING UTILITIES =====

def hash_password(password: str) -> str:
    """Hash a password using a secure method (you should use bcrypt in production)"""
    # In production, use bcrypt or similar
    return bcrypt.hashpw(password.encode('utf-8'), bcrypt.gensalt()).decode('utf-8')
    
    # Simple hash for development (NOT for production!)
    # return hashlib.sha256(password.encode()).hexdigest()


def verify_password(plain_password: str, hashed_password: str) -> bool:
    """Verify a password against its hash"""
    # In production with bcrypt:
    return bcrypt.checkpw(plain_password.encode('utf-8'), hashed_password.encode('utf-8'))
    
    # Simple verification for development
    # return hash_password(plain_password) == hashed_password


# ===== DATABASE OPERATIONS =====

def create_db_user(db: Session, user_data: UserCreate) -> UserDB:
    """Create a new user in the database"""
    
    # Check if email already exists
    existing_user = db.query(UserDB).filter(UserDB.email == user_data.email).first()
    if existing_user:
        raise ValueError("Email already registered")
    
    # Check if username already exists
    existing_username = db.query(UserDB).filter(UserDB.username == user_data.username).first()
    if existing_username:
        raise ValueError("Username already taken")
    
    # Create new user
    db_user = UserDB(
        id=uuid4(),
        email=user_data.email,
        username=user_data.username,
        password_hash=hash_password(user_data.password),
        first_name=user_data.first_name,
        last_name=user_data.last_name,
        date_of_birth=user_data.date_of_birth,
        created_at=datetime.utcnow(),
        updated_at=datetime.utcnow()
    )
    
    try:
        db.add(db_user)
        db.commit()
        db.refresh(db_user)
        return db_user
    except IntegrityError as e:
        db.rollback()
        raise ValueError("User creation failed due to database constraint")


def read_db_user(db: Session, user_id: int = None, user_uuid: UUID = None, 
                 email: str = None, username: str = None) -> Optional[UserDB]:
    """Read a user from the database by various identifiers"""
    
    query = db.query(UserDB)
    
    if user_id:
        return query.filter(UserDB.db_id == user_id).first()
    elif user_uuid:
        return query.filter(UserDB.id == user_uuid).first()
    elif email:
        return query.filter(UserDB.email == email.lower()).first()
    elif username:
        return query.filter(UserDB.username == username.lower()).first()
    else:
        raise ValueError("Must provide at least one identifier (user_id, user_uuid, email, or username)")


def read_db_users(db: Session, skip: int = 0, limit: int = 100) -> List[UserDB]:
    """Read multiple users from the database (for admin purposes)"""
    return db.query(UserDB).offset(skip).limit(limit).all()


def update_db_user(db: Session, user_id: int, user_updates: UserUpdate) -> UserDB:
    """Update an existing user in the database"""
    
    # Get the existing user
    db_user = db.query(UserDB).filter(UserDB.db_id == user_id).first()
    if not db_user:
        raise NotFoundError(f"User with id {user_id} not found")
    
    # Check for email uniqueness if email is being updated
    if user_updates.email and user_updates.email != db_user.email:
        existing_email = db.query(UserDB).filter(
            UserDB.email == user_updates.email,
            UserDB.db_id != user_id
        ).first()
        if existing_email:
            raise ValueError("Email already registered")
    
    # Check for username uniqueness if username is being updated
    if user_updates.username and user_updates.username != db_user.username:
        existing_username = db.query(UserDB).filter(
            UserDB.username == user_updates.username,
            UserDB.db_id != user_id
        ).first()
        if existing_username:
            raise ValueError("Username already taken")
    
    # Update only the fields that are provided
    update_data = user_updates.model_dump(exclude_unset=True)
    for field, value in update_data.items():
        setattr(db_user, field, value)
    
    # Always update the updated_at timestamp
    db_user.updated_at = datetime.utcnow()
    
    try:
        db.commit()
        db.refresh(db_user)
        return db_user
    except IntegrityError:
        db.rollback()
        raise ValueError("User update failed due to database constraint")


def delete_db_user(db: Session, user_id: int) -> bool:
    """Delete a user from the database"""
    
    db_user = db.query(UserDB).filter(UserDB.db_id == user_id).first()
    if not db_user:
        raise NotFoundError(f"User with id {user_id} not found")
    
    try:
        # Note: This will cascade delete related records based on your foreign key constraints
        db.delete(db_user)
        db.commit()
        return True
    except Exception as e:
        db.rollback()
        raise ValueError(f"Failed to delete user: {str(e)}")


def authenticate_user(db: Session, email: str, password: str) -> Optional[UserDB]:
    """Authenticate a user by email and password"""
    
    user = read_db_user(db, email=email)
    if not user:
        return None
    
    if not verify_password(password, user.password_hash):
        return None
    
    # Update last login time
    user.last_login_at = datetime.utcnow()
    user.updated_at = datetime.utcnow()
    db.commit()
    db.refresh(user)
    
    return user


def change_user_password(db: Session, user_id: int, password_change: PasswordChange) -> UserDB:
    """Change a user's password"""
    
    db_user = db.query(UserDB).filter(UserDB.db_id == user_id).first()
    if not db_user:
        raise NotFoundError(f"User with id {user_id} not found")
    
    # Verify current password
    if not verify_password(password_change.current_password, db_user.password_hash):
        raise ValueError("Current password is incorrect")
    
    # Update password
    db_user.password_hash = hash_password(password_change.new_password)
    db_user.updated_at = datetime.utcnow()
    
    try:
        db.commit()
        db.refresh(db_user)
        return db_user
    except Exception as e:
        db.rollback()
        raise ValueError(f"Failed to change password: {str(e)}")


def get_user_count(db: Session) -> int:
    """Get total count of users (for pagination)"""
    return db.query(UserDB).count()