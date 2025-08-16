from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError
from typing import List, Optional

from src.db.core import CategoryDB, NotFoundError
from src.models.category import CategoryCreate, CategoryUpdate

def create_db_category(db: Session, category_data: CategoryCreate) -> CategoryDB:
    """Create a new global category"""
    
    # Check for duplicate category name
    existing_category = db.query(CategoryDB).filter(CategoryDB.name.ilike(category_data.name.strip())).first()
    if existing_category:
        raise ValueError(f"Category with name '{category_data.name}' already exists")

    db_category = CategoryDB(
        name=category_data.name.strip(),
        parent_category_id=category_data.parent_category_id
    )
    
    try:
        db.add(db_category)
        db.commit()
        db.refresh(db_category)
        return db_category
    except IntegrityError:
        db.rollback()
        raise ValueError("Category creation failed due to a database constraint.")

def read_db_categories(db: Session, skip: int = 0, limit: int = 100) -> List[CategoryDB]:
    """Read all global categories"""
    return db.query(CategoryDB).order_by(CategoryDB.name).offset(skip).limit(limit).all()

def read_db_category(db: Session, category_id: int) -> Optional[CategoryDB]:
    """Read a single category by its ID"""
    return db.query(CategoryDB).filter(CategoryDB.id == category_id).first()

def update_db_category(db: Session, category_id: int, category_updates: CategoryUpdate) -> CategoryDB:
    """Update a category's details"""
    db_category = read_db_category(db, category_id)
    if not db_category:
        raise NotFoundError(f"Category with id {category_id} not found")

    update_data = category_updates.model_dump(exclude_unset=True)
    
    # Check for duplicate name if name is being updated
    if 'name' in update_data:
        new_name = update_data['name'].strip()
        existing = db.query(CategoryDB).filter(CategoryDB.name.ilike(new_name), CategoryDB.id != category_id).first()
        if existing:
            raise ValueError(f"Category with name '{new_name}' already exists")
        db_category.name = new_name

    if 'parent_category_id' in update_data:
        db_category.parent_category_id = update_data['parent_category_id']

    try:
        db.commit()
        db.refresh(db_category)
        return db_category
    except IntegrityError:
        db.rollback()
        raise ValueError("Category update failed due to a database constraint.")

def delete_db_category(db: Session, category_id: int) -> bool:
    """Delete a category"""
    db_category = read_db_category(db, category_id)
    if not db_category:
        raise NotFoundError(f"Category with id {category_id} not found")
    
    # Note: Add logic here to check for existing transactions or budget categories
    # before allowing deletion to prevent orphaned records. For now, we'll allow it.

    try:
        db.delete(db_category)
        db.commit()
        return True
    except IntegrityError:
        db.rollback()
        # This will likely fire if transactions or budgets are still using this category
        raise ValueError("Cannot delete category as it is currently in use.")
