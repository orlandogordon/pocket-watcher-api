from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError
from sqlalchemy import and_, or_, desc, asc
from typing import Optional, List, Dict, Any
from datetime import datetime, date
from uuid import uuid4

# Import your database models
from .core import TagDB, UserDB, TransactionTagDB, TransactionDB, NotFoundError
from pydantic import BaseModel, Field, field_validator
from typing import Optional, List
from datetime import datetime


# ===== TAG PYDANTIC MODELS =====

class TagCreate(BaseModel):
    tag_name: str = Field(..., min_length=1, max_length=100, description="Tag name")
    color: Optional[str] = Field(None, pattern=r'^#[0-9A-Fa-f]{6}$', description="Hex color code")

    @field_validator('tag_name')
    @classmethod
    def validate_tag_name(cls, v: str) -> str:
        return v.strip()

    @field_validator('color')
    @classmethod
    def validate_color(cls, v: Optional[str]) -> Optional[str]:
        if v and not v.startswith('#'):
            v = f"#{v}"
        return v.upper() if v else v


class TagUpdate(BaseModel):
    """Update tag - all fields optional"""
    tag_name: Optional[str] = Field(None, min_length=1, max_length=100)
    color: Optional[str] = Field(None, pattern=r'^#[0-9A-Fa-f]{6}$')

    @field_validator('tag_name')
    @classmethod
    def validate_tag_name(cls, v: Optional[str]) -> Optional[str]:
        return v.strip() if v else v

    @field_validator('color')
    @classmethod
    def validate_color(cls, v: Optional[str]) -> Optional[str]:
        if v and not v.startswith('#'):
            v = f"#{v}"
        return v.upper() if v else v


class TagResponse(BaseModel):
    """Tag data returned to client"""
    tag_id: int
    tag_name: str
    color: Optional[str]
    created_at: datetime
    transaction_count: Optional[int] = None  # Number of transactions with this tag

    class Config:
        from_attributes = True


class TransactionTagCreate(BaseModel):
    """Add tag to transaction"""
    transaction_id: int = Field(..., description="Transaction DB ID")
    tag_id: int = Field(..., description="Tag ID")


class TransactionTagResponse(BaseModel):
    """Transaction-Tag relationship response"""
    transaction_id: int
    tag_id: int
    created_at: datetime

    class Config:
        from_attributes = True


class TagStats(BaseModel):
    """Tag usage statistics"""
    tag_id: int
    tag_name: str
    color: Optional[str]
    transaction_count: int
    total_amount: float
    average_amount: float
    most_recent_use: Optional[datetime]


# ===== DATABASE OPERATIONS =====

def create_db_tag(db: Session, user_id: int, tag_data: TagCreate) -> TagDB:
    """Create a new tag"""
    
    # Verify user exists
    user = db.query(UserDB).filter(UserDB.db_id == user_id).first()
    if not user:
        raise NotFoundError(f"User with id {user_id} not found")
    
    # Check for duplicate tag name for this user
    existing_tag = db.query(TagDB).filter(
        TagDB.user_id == user_id,
        TagDB.tag_name.ilike(tag_data.tag_name.strip())
    ).first()
    if existing_tag:
        raise ValueError(f"Tag with name '{tag_data.tag_name}' already exists")
    
    # Create new tag
    db_tag = TagDB(
        user_id=user_id,
        tag_name=tag_data.tag_name.strip(),
        color=tag_data.color,
        created_at=datetime.utcnow()
    )
    
    try:
        db.add(db_tag)
        db.commit()
        db.refresh(db_tag)
        return db_tag
    except IntegrityError:
        db.rollback()
        raise ValueError("Tag creation failed due to database constraint")


def read_db_tag(db: Session, tag_id: int, user_id: Optional[int] = None) -> Optional[TagDB]:
    """Read a tag by ID"""
    
    query = db.query(TagDB).filter(TagDB.tag_id == tag_id)
    
    if user_id:
        query = query.filter(TagDB.user_id == user_id)
    
    return query.first()


def read_db_tags(db: Session, user_id: int, skip: int = 0, limit: int = 100, 
                include_transaction_count: bool = False) -> List[TagDB]:
    """Read all tags for a user"""
    
    query = db.query(TagDB).filter(TagDB.user_id == user_id)
    
    if include_transaction_count:
        # This would require a more complex query with joins and counts
        # For now, we'll fetch tags and add counts separately if needed
        pass
    
    query = query.order_by(TagDB.tag_name)
    return query.offset(skip).limit(limit).all()


def update_db_tag(db: Session, tag_id: int, user_id: int, tag_updates: TagUpdate) -> TagDB:
    """Update an existing tag"""
    
    # Get the existing tag
    db_tag = db.query(TagDB).filter(
        TagDB.tag_id == tag_id,
        TagDB.user_id == user_id
    ).first()
    
    if not db_tag:
        raise NotFoundError(f"Tag with id {tag_id} not found")
    
    # Check for duplicate tag name if name is being updated
    update_data = tag_updates.model_dump(exclude_unset=True)
    if 'tag_name' in update_data:
        existing_tag = db.query(TagDB).filter(
            TagDB.user_id == user_id,
            TagDB.tag_name.ilike(update_data['tag_name'].strip()),
            TagDB.tag_id != tag_id
        ).first()
        if existing_tag:
            raise ValueError(f"Tag with name '{update_data['tag_name']}' already exists")
    
    # Update the tag
    for field, value in update_data.items():
        setattr(db_tag, field, value)
    
    try:
        db.commit()
        db.refresh(db_tag)
        return db_tag
    except IntegrityError:
        db.rollback()
        raise ValueError("Tag update failed due to database constraint")


def delete_db_tag(db: Session, tag_id: int, user_id: int) -> bool:
    """Delete a tag and all its transaction associations"""
    
    db_tag = db.query(TagDB).filter(
        TagDB.tag_id == tag_id,
        TagDB.user_id == user_id
    ).first()
    
    if not db_tag:
        raise NotFoundError(f"Tag with id {tag_id} not found")
    
    try:
        # First delete all transaction-tag relationships
        db.query(TransactionTagDB).filter(TransactionTagDB.tag_id == tag_id).delete()
        
        # Then delete the tag
        db.delete(db_tag)
        db.commit()
        return True
    except Exception as e:
        db.rollback()
        raise ValueError(f"Failed to delete tag: {str(e)}")


def add_tag_to_transaction(db: Session, user_id: int, transaction_id: int, tag_id: int) -> TransactionTagDB:
    """Add a tag to a transaction"""
    
    # Verify transaction belongs to user
    transaction = db.query(TransactionDB).filter(
        TransactionDB.db_id == transaction_id,
        TransactionDB.user_id == user_id
    ).first()
    if not transaction:
        raise NotFoundError(f"Transaction with id {transaction_id} not found")
    
    # Verify tag belongs to user
    tag = db.query(TagDB).filter(
        TagDB.tag_id == tag_id,
        TagDB.user_id == user_id
    ).first()
    if not tag:
        raise NotFoundError(f"Tag with id {tag_id} not found")
    
    # Check if relationship already exists
    existing_relationship = db.query(TransactionTagDB).filter(
        TransactionTagDB.transaction_id == transaction_id,
        TransactionTagDB.tag_id == tag_id
    ).first()
    if existing_relationship:
        raise ValueError("Transaction is already tagged with this tag")
    
    # Create the relationship
    db_transaction_tag = TransactionTagDB(
        transaction_id=transaction_id,
        tag_id=tag_id,
        created_at=datetime.utcnow()
    )
    
    try:
        db.add(db_transaction_tag)
        db.commit()
        db.refresh(db_transaction_tag)
        return db_transaction_tag
    except IntegrityError:
        db.rollback()
        raise ValueError("Failed to add tag to transaction")


def remove_tag_from_transaction(db: Session, user_id: int, transaction_id: int, tag_id: int) -> bool:
    """Remove a tag from a transaction"""
    
    # Verify transaction belongs to user
    transaction = db.query(TransactionDB).filter(
        TransactionDB.db_id == transaction_id,
        TransactionDB.user_id == user_id
    ).first()
    if not transaction:
        raise NotFoundError(f"Transaction with id {transaction_id} not found")
    
    # Verify tag belongs to user
    tag = db.query(TagDB).filter(
        TagDB.tag_id == tag_id,
        TagDB.user_id == user_id
    ).first()
    if not tag:
        raise NotFoundError(f"Tag with id {tag_id} not found")
    
    # Find and delete the relationship
    transaction_tag = db.query(TransactionTagDB).filter(
        TransactionTagDB.transaction_id == transaction_id,
        TransactionTagDB.tag_id == tag_id
    ).first()
    
    if not transaction_tag:
        raise NotFoundError("Transaction tag relationship not found")
    
    try:
        db.delete(transaction_tag)
        db.commit()
        return True
    except Exception as e:
        db.rollback()
        raise ValueError(f"Failed to remove tag from transaction: {str(e)}")


def get_tags_for_transaction(db: Session, transaction_id: int, user_id: int) -> List[TagDB]:
    """Get all tags for a specific transaction"""
    
    # Verify transaction belongs to user
    transaction = db.query(TransactionDB).filter(
        TransactionDB.db_id == transaction_id,
        TransactionDB.user_id == user_id
    ).first()
    if not transaction:
        raise NotFoundError(f"Transaction with id {transaction_id} not found")
    
    tags = db.query(TagDB).join(TransactionTagDB).filter(
        TransactionTagDB.transaction_id == transaction_id,
        TagDB.user_id == user_id
    ).order_by(TagDB.tag_name).all()
    
    return tags


def get_transactions_for_tag(db: Session, tag_id: int, user_id: int, skip: int = 0, limit: int = 100) -> List[TransactionDB]:
    """Get all transactions for a specific tag"""
    
    # Verify tag belongs to user
    tag = db.query(TagDB).filter(
        TagDB.tag_id == tag_id,
        TagDB.user_id == user_id
    ).first()
    if not tag:
        raise NotFoundError(f"Tag with id {tag_id} not found")
    
    transactions = db.query(TransactionDB).join(TransactionTagDB).filter(
        TransactionTagDB.tag_id == tag_id,
        TransactionDB.user_id == user_id
    ).order_by(desc(TransactionDB.transaction_date)).offset(skip).limit(limit).all()
    
    return transactions


def get_tag_stats(db: Session, tag_id: int, user_id: int) -> TagStats:
    """Get statistics for a specific tag"""
    
    # Verify tag belongs to user
    tag = db.query(TagDB).filter(
        TagDB.tag_id == tag_id,
        TagDB.user_id == user_id
    ).first()
    if not tag:
        raise NotFoundError(f"Tag with id {tag_id} not found")
    
    # Get all transactions for this tag
    transactions = db.query(TransactionDB).join(TransactionTagDB).filter(
        TransactionTagDB.tag_id == tag_id,
        TransactionDB.user_id == user_id
    ).all()
    
    if not transactions:
        return TagStats(
            tag_id=tag.tag_id,
            tag_name=tag.tag_name,
            color=tag.color,
            transaction_count=0,
            total_amount=0.0,
            average_amount=0.0,
            most_recent_use=None
        )
    
    total_amount = sum(float(t.amount) for t in transactions)
    average_amount = total_amount / len(transactions)
    most_recent_use = max(t.transaction_date for t in transactions)
    
    return TagStats(
        tag_id=tag.tag_id,
        tag_name=tag.tag_name,
        color=tag.color,
        transaction_count=len(transactions),
        total_amount=total_amount,
        average_amount=average_amount,
        most_recent_use=datetime.combine(most_recent_use, datetime.min.time())
    )


def get_all_tag_stats(db: Session, user_id: int) -> List[TagStats]:
    """Get statistics for all user tags"""
    
    tags = db.query(TagDB).filter(TagDB.user_id == user_id).all()
    
    stats = []
    for tag in tags:
        try:
            tag_stats = get_tag_stats(db, tag.tag_id, user_id)
            stats.append(tag_stats)
        except Exception:
            # If we can't get stats for a tag, include it with zero stats
            stats.append(TagStats(
                tag_id=tag.tag_id,
                tag_name=tag.tag_name,
                color=tag.color,
                transaction_count=0,
                total_amount=0.0,
                average_amount=0.0,
                most_recent_use=None
            ))
    
    return stats


def search_tags(db: Session, user_id: int, search_term: str) -> List[TagDB]:
    """Search tags by name"""
    
    return db.query(TagDB).filter(
        TagDB.user_id == user_id,
        TagDB.tag_name.ilike(f"%{search_term}%")
    ).order_by(TagDB.tag_name).all()


def bulk_tag_transactions(db: Session, user_id: int, transaction_ids: List[int], tag_id: int) -> List[TransactionTagDB]:
    """Add the same tag to multiple transactions"""
    
    # Verify tag belongs to user
    tag = db.query(TagDB).filter(
        TagDB.tag_id == tag_id,
        TagDB.user_id == user_id
    ).first()
    if not tag:
        raise NotFoundError(f"Tag with id {tag_id} not found")
    
    # Verify all transactions belong to user
    transactions = db.query(TransactionDB).filter(
        TransactionDB.db_id.in_(transaction_ids),
        TransactionDB.user_id == user_id
    ).all()
    
    if len(transactions) != len(transaction_ids):
        raise ValueError("One or more transactions not found or don't belong to user")
    
    created_relationships = []
    errors = []
    
    for transaction_id in transaction_ids:
        try:
            # Check if relationship already exists
            existing = db.query(TransactionTagDB).filter(
                TransactionTagDB.transaction_id == transaction_id,
                TransactionTagDB.tag_id == tag_id
            ).first()
            
            if existing:
                continue  # Skip if already tagged
            
            # Create relationship
            db_transaction_tag = TransactionTagDB(
                transaction_id=transaction_id,
                tag_id=tag_id,
                created_at=datetime.utcnow()
            )
            
            db.add(db_transaction_tag)
            created_relationships.append(db_transaction_tag)
            
        except Exception as e:
            errors.append({
                'transaction_id': transaction_id,
                'error': str(e)
            })
    
    try:
        if created_relationships:
            db.commit()
            for relationship in created_relationships:
                db.refresh(relationship)
        
        return created_relationships
        
    except Exception as e:
        db.rollback()
        raise ValueError(f"Bulk tag operation failed: {str(e)}")