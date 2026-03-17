from sqlalchemy.orm import Session, joinedload
from sqlalchemy.exc import IntegrityError
from typing import List, Optional
from uuid import UUID, uuid4

from src.db.core import CategoryDB, BudgetTemplateCategoryDB, TransactionDB, FinancialPlanExpenseDB, NotFoundError
from src.models.category import CategoryCreate, CategoryUpdate
from src.logging_config import get_logger

logger = get_logger(__name__)

_UNSET = object()

def create_db_category(db: Session, category_data: CategoryCreate, *, parent_category_id: Optional[int] = None) -> CategoryDB:
    """Create a new global category"""

    # Check for duplicate category name
    existing_category = db.query(CategoryDB).filter(CategoryDB.name.ilike(category_data.name.strip())).first()
    if existing_category:
        raise ValueError(f"Category with name '{category_data.name}' already exists")

    db_category = CategoryDB(
        uuid=uuid4(),
        name=category_data.name.strip(),
        parent_category_id=parent_category_id
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
    return db.query(CategoryDB).options(joinedload(CategoryDB.parent)).order_by(CategoryDB.name).offset(skip).limit(limit).all()

def read_db_category(db: Session, category_id: int) -> Optional[CategoryDB]:
    """Read a single category by its ID"""
    return db.query(CategoryDB).options(joinedload(CategoryDB.parent)).filter(CategoryDB.id == category_id).first()

def update_db_category(db: Session, category_id: int, category_updates: CategoryUpdate, *, parent_category_id=_UNSET) -> CategoryDB:
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

    if parent_category_id is not _UNSET:
        db_category.parent_category_id = parent_category_id
    # Remove UUID field from update_data so it's not set on the DB model
    update_data.pop('parent_category_uuid', None)

    try:
        db.commit()
        db.refresh(db_category)
        return db_category
    except IntegrityError:
        db.rollback()
        raise ValueError("Category update failed due to a database constraint.")

def delete_db_category(db: Session, category_id: int, force: bool = False) -> bool:
    """Delete a category. If force=True, removes all references first."""
    db_category = read_db_category(db, category_id)
    if not db_category:
        raise NotFoundError(f"Category with id {category_id} not found")

    # Check for references
    children_count = db.query(CategoryDB).filter(CategoryDB.parent_category_id == category_id).count()
    budget_count = db.query(BudgetTemplateCategoryDB).filter(
        (BudgetTemplateCategoryDB.category_id == category_id) | (BudgetTemplateCategoryDB.subcategory_id == category_id)
    ).count()
    txn_primary_count = db.query(TransactionDB).filter(TransactionDB.category_id == category_id).count()
    txn_sub_count = db.query(TransactionDB).filter(TransactionDB.subcategory_id == category_id).count()
    expense_count = db.query(FinancialPlanExpenseDB).filter(FinancialPlanExpenseDB.category_id == category_id).count()

    total_refs = children_count + budget_count + txn_primary_count + txn_sub_count + expense_count

    if total_refs > 0 and not force:
        parts = []
        if children_count:
            parts.append(f"{children_count} child {'category' if children_count == 1 else 'categories'}")
        if budget_count:
            parts.append(f"{budget_count} template {'allocation' if budget_count == 1 else 'allocations'}")
        if txn_primary_count:
            parts.append(f"{txn_primary_count} {'transaction' if txn_primary_count == 1 else 'transactions'} (primary)")
        if txn_sub_count:
            parts.append(f"{txn_sub_count} {'transaction' if txn_sub_count == 1 else 'transactions'} (sub-category)")
        if expense_count:
            parts.append(f"{expense_count} financial plan {'expense' if expense_count == 1 else 'expenses'}")
        detail = ", ".join(parts)
        raise ValueError(f"Cannot delete category: used by {detail}. Use force=true to remove all references and delete.")

    if total_refs > 0 and force:
        logger.info(f"Force-deleting category {category_id} ({db_category.name}), removing {total_refs} references")
        # Remove budget template category allocations
        if budget_count:
            db.query(BudgetTemplateCategoryDB).filter(
                (BudgetTemplateCategoryDB.category_id == category_id) | (BudgetTemplateCategoryDB.subcategory_id == category_id)
            ).delete()
        # Null out transaction references
        if txn_primary_count:
            db.query(TransactionDB).filter(TransactionDB.category_id == category_id).update({TransactionDB.category_id: None})
        if txn_sub_count:
            db.query(TransactionDB).filter(TransactionDB.subcategory_id == category_id).update({TransactionDB.subcategory_id: None})
        # Remove financial plan expenses
        if expense_count:
            db.query(FinancialPlanExpenseDB).filter(FinancialPlanExpenseDB.category_id == category_id).delete()
        # Reassign child categories to this category's parent (or None)
        if children_count:
            db.query(CategoryDB).filter(CategoryDB.parent_category_id == category_id).update(
                {CategoryDB.parent_category_id: db_category.parent_category_id}
            )

    db.delete(db_category)
    db.commit()
    return True


# ===== UUID-BASED OPERATIONS =====

def read_db_category_by_uuid(db: Session, category_uuid: UUID) -> Optional[CategoryDB]:
    """Read a single category by its UUID"""
    return db.query(CategoryDB).options(joinedload(CategoryDB.parent)).filter(CategoryDB.uuid == category_uuid).first()

def read_db_categories_by_uuids(db: Session, uuids: List[UUID]) -> List[CategoryDB]:
    """Read multiple categories by their UUIDs in a single query"""
    return db.query(CategoryDB).filter(CategoryDB.uuid.in_(uuids)).all()

def update_db_category_by_uuid(db: Session, category_uuid: UUID, category_updates: CategoryUpdate, *, parent_category_id=_UNSET) -> CategoryDB:
    """Update a category by UUID"""
    db_category = read_db_category_by_uuid(db, category_uuid)
    if not db_category:
        raise NotFoundError(f"Category not found")
    return update_db_category(db, db_category.id, category_updates, parent_category_id=parent_category_id)

def delete_db_category_by_uuid(db: Session, category_uuid: UUID, force: bool = False) -> bool:
    """Delete a category by UUID"""
    db_category = read_db_category_by_uuid(db, category_uuid)
    if not db_category:
        raise NotFoundError(f"Category not found")
    return delete_db_category(db, db_category.id, force=force)
