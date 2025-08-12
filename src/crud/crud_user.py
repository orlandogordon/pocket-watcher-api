from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError
from typing import Optional, List
from uuid import uuid4, UUID
from datetime import datetime
import bcrypt

# Import your database models and Pydantic models
from src.db.core import UserDB, NotFoundError
from src.models.user import UserCreate, UserUpdate, PasswordChange


# ===== PASSWORD HASHING UTILITIES =====

def hash_password(password: str) -> str:
    """Hash a password using a secure method (you should use bcrypt in production)"""
    # In production, use bcrypt or similar
    return bcrypt.hashpw(password.encode('utf-8'), bcrypt.gensalt()).decode('utf-8')
    

def verify_password(plain_password: str, hashed_password: str) -> bool:
    """Verify a password against its hash"""
    # In production with bcrypt:
    return bcrypt.checkpw(plain_password.encode('utf-8'), hashed_password.encode('utf-8'))


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
