from sqlalchemy.orm import Session, joinedload
from sqlalchemy.exc import IntegrityError
from sqlalchemy import and_, or_, desc, asc, func
from typing import Optional, List, Dict, Any
from datetime import datetime, date
from decimal import Decimal
from uuid import uuid4

# Import your database models
from src.db.core import BudgetDB, BudgetCategoryDB, UserDB, TransactionDB, NotFoundError, TransactionType, CategoryDB
from src.models.budget import BudgetCreate, BudgetUpdate, BudgetCategoryCreate, BudgetCategoryUpdate, BudgetStats, BudgetPerformance


# ===== DATABASE OPERATIONS =====

def create_db_budget(db: Session, user_id: int, budget_data: BudgetCreate) -> BudgetDB:
    """Create a new budget with categories"""
    
    # Verify user exists
    user = db.query(UserDB).filter(UserDB.db_id == user_id).first()
    if not user:
        raise NotFoundError(f"User with id {user_id} not found")
    
    # Check for duplicate budget name for this user
    existing_budget = db.query(BudgetDB).filter(
        BudgetDB.user_id == user_id,
        BudgetDB.budget_name.ilike(budget_data.budget_name.strip())
    ).first()
    if existing_budget:
        raise ValueError(f"Budget with name '{budget_data.budget_name}' already exists")
    
    # Create new budget
    db_budget = BudgetDB(
        user_id=user_id,
        budget_name=budget_data.budget_name.strip(),
        start_date=budget_data.start_date,
        end_date=budget_data.end_date,
        created_at=datetime.utcnow(),
        updated_at=datetime.utcnow()
    )
    
    try:
        db.add(db_budget)
        db.flush()  # Get the budget_id without committing
        
        # Create budget categories
        for category_data in budget_data.categories:
            # Verify category exists
            category = db.query(CategoryDB).filter(CategoryDB.id == category_data.category_id).first()
            if not category:
                raise NotFoundError(f"Category with id {category_data.category_id} not found")

            db_category = BudgetCategoryDB(
                budget_id=db_budget.budget_id,
                category_id=category_data.category_id,
                allocated_amount=category_data.allocated_amount,
                created_at=datetime.utcnow()
            )
            db.add(db_category)
        
        db.commit()
        db.refresh(db_budget)
        return db_budget
    except IntegrityError:
        db.rollback()
        raise ValueError("Budget creation failed due to database constraint")


def read_db_budget(db: Session, budget_id: int, user_id: Optional[int] = None, 
                   include_categories: bool = True, include_spending: bool = True) -> Optional[BudgetDB]:
    """Read a budget by ID with optional spending calculations"""
    
    query = db.query(BudgetDB).filter(BudgetDB.budget_id == budget_id)
    
    if user_id:
        query = query.filter(BudgetDB.user_id == user_id)

    # Eager load categories and their nested category details
    query = query.options(joinedload(BudgetDB.budget_categories).joinedload(BudgetCategoryDB.category))
    
    budget = query.first()
    
    if budget and include_spending:
        # Calculate spending for each category
        for category_item in budget.budget_categories:
            spent = calculate_category_spending(db, budget, category_item.category_id)
            category_item.spent_amount = spent
            category_item.remaining_amount = category_item.allocated_amount - spent
            if category_item.allocated_amount > 0:
                category_item.percentage_used = float(spent / category_item.allocated_amount * 100)
            else:
                category_item.percentage_used = 0.0
    
    return budget


def read_db_budgets(db: Session, user_id: int, skip: int = 0, limit: int = 100, 
                   include_spending: bool = False, active_only: bool = False) -> List[BudgetDB]:
    """Read all budgets for a user"""
    
    query = db.query(BudgetDB).filter(BudgetDB.user_id == user_id)
    
    if active_only:
        current_date = date.today()
        query = query.filter(
            BudgetDB.start_date <= current_date,
            BudgetDB.end_date >= current_date
        )
    
    query = query.order_by(desc(BudgetDB.start_date))
    query = query.options(joinedload(BudgetDB.budget_categories).joinedload(BudgetCategoryDB.category))
    budgets = query.offset(skip).limit(limit).all()
    
    if include_spending:
        for budget in budgets:
            budget.total_allocated = sum(cat.allocated_amount for cat in budget.budget_categories)
            budget.total_spent = sum(calculate_category_spending(db, budget, cat.category_id) 
                                   for cat in budget.budget_categories)
            budget.total_remaining = budget.total_allocated - budget.total_spent
            if budget.total_allocated > 0:
                budget.percentage_used = float(budget.total_spent / budget.total_allocated * 100)
            else:
                budget.percentage_used = 0.0
            
            # Check if budget is currently active
            current_date = date.today()
            budget.is_active = budget.start_date <= current_date <= budget.end_date
    
    return budgets


def update_db_budget(db: Session, budget_id: int, user_id: int, budget_updates: BudgetUpdate) -> BudgetDB:
    """Update an existing budget"""
    
    # Get the existing budget
    db_budget = db.query(BudgetDB).filter(
        BudgetDB.budget_id == budget_id,
        BudgetDB.user_id == user_id
    ).first()
    
    if not db_budget:
        raise NotFoundError(f"Budget with id {budget_id} not found")
    
    # Check for duplicate budget name if name is being updated
    update_data = budget_updates.model_dump(exclude_unset=True)
    if 'budget_name' in update_data:
        existing_budget = db.query(BudgetDB).filter(
            BudgetDB.user_id == user_id,
            BudgetDB.budget_name.ilike(update_data['budget_name'].strip()),
            BudgetDB.budget_id != budget_id
        ).first()
        if existing_budget:
            raise ValueError(f"Budget with name '{update_data['budget_name']}' already exists")
    
    # Validate date range if both dates are being updated
    if 'start_date' in update_data and 'end_date' in update_data:
        if update_data['end_date'] <= update_data['start_date']:
            raise ValueError("end_date must be after start_date")
    elif 'start_date' in update_data:
        if update_data['start_date'] >= db_budget.end_date:
            raise ValueError("start_date must be before current end_date")
    elif 'end_date' in update_data:
        if update_data['end_date'] <= db_budget.start_date:
            raise ValueError("end_date must be after current start_date")
    
    # Update the budget
    for field, value in update_data.items():
        setattr(db_budget, field, value)
    
    db_budget.updated_at = datetime.utcnow()
    
    try:
        db.commit()
        db.refresh(db_budget)
        return db_budget
    except IntegrityError:
        db.rollback()
        raise ValueError("Budget update failed due to database constraint")


def delete_db_budget(db: Session, budget_id: int, user_id: int) -> bool:
    """Delete a budget and all its categories"""
    
    db_budget = db.query(BudgetDB).filter(
        BudgetDB.budget_id == budget_id,
        BudgetDB.user_id == user_id
    ).first()
    
    if not db_budget:
        raise NotFoundError(f"Budget with id {budget_id} not found")
    
    try:
        # First delete all budget categories
        db.query(BudgetCategoryDB).filter(BudgetCategoryDB.budget_id == budget_id).delete()
        
        # Then delete the budget
        db.delete(db_budget)
        db.commit()
        return True
    except Exception as e:
        db.rollback()
        raise ValueError(f"Failed to delete budget: {str(e)}")


def add_budget_category(db: Session, budget_id: int, user_id: int, category_data: BudgetCategoryCreate) -> BudgetCategoryDB:
    """Add a category to an existing budget"""
    
    # Verify budget belongs to user
    budget = db.query(BudgetDB).filter(
        BudgetDB.budget_id == budget_id,
        BudgetDB.user_id == user_id
    ).first()
    if not budget:
        raise NotFoundError(f"Budget with id {budget_id} not found")
    
    # Check for duplicate category in this budget
    existing_category = db.query(BudgetCategoryDB).filter(
        BudgetCategoryDB.budget_id == budget_id,
        BudgetCategoryDB.category_id == category_data.category_id
    ).first()
    if existing_category:
        raise ValueError(f"Category ID '{category_data.category_id}' already exists in this budget")
    
    # Create new category
    db_category = BudgetCategoryDB(
        budget_id=budget_id,
        category_id=category_data.category_id,
        allocated_amount=category_data.allocated_amount,
        created_at=datetime.utcnow()
    )
    
    try:
        db.add(db_category)
        db.commit()
        db.refresh(db_category)
        return db_category
    except IntegrityError:
        db.rollback()
        raise ValueError("Budget category creation failed due to database constraint")


def update_budget_category(db: Session, budget_category_id: int, user_id: int, 
                          category_updates: BudgetCategoryUpdate) -> BudgetCategoryDB:
    """Update a budget category"""
    
    # Get the existing category and verify user owns the budget
    db_category = db.query(BudgetCategoryDB).join(BudgetDB).filter(
        BudgetCategoryDB.budget_category_id == budget_category_id,
        BudgetDB.user_id == user_id
    ).first()
    
    if not db_category:
        raise NotFoundError(f"Budget category with id {budget_category_id} not found")
    
    # Update the category
    update_data = category_updates.model_dump(exclude_unset=True)
    for field, value in update_data.items():
        setattr(db_category, field, value)
    
    try:
        db.commit()
        db.refresh(db_category)
        return db_category
    except IntegrityError:
        db.rollback()
        raise ValueError("Budget category update failed due to database constraint")


def delete_budget_category(db: Session, budget_category_id: int, user_id: int) -> bool:
    """Delete a budget category"""
    
    # Get the existing category and verify user owns the budget
    db_category = db.query(BudgetCategoryDB).join(BudgetDB).filter(
        BudgetCategoryDB.budget_category_id == budget_category_id,
        BudgetDB.user_id == user_id
    ).first()
    
    if not db_category:
        raise NotFoundError(f"Budget category with id {budget_category_id} not found")
    
    try:
        db.delete(db_category)
        db.commit()
        return True
    except Exception as e:
        db.rollback()
        raise ValueError(f"Failed to delete budget category: {str(e)}")


def calculate_category_spending(db: Session, budget: BudgetDB, category_id: int) -> Decimal:
    """Calculate total spending for a category within budget period"""
    
    result = db.query(func.coalesce(func.sum(TransactionDB.amount), 0)).filter(
        TransactionDB.user_id == budget.user_id,
        TransactionDB.category_id == category_id,
        TransactionDB.transaction_date >= budget.start_date,
        TransactionDB.transaction_date <= budget.end_date,
        TransactionDB.transaction_type.in_([
            TransactionType.DEBIT,
            TransactionType.WITHDRAWAL,
            TransactionType.FEE
        ])
    ).scalar()
    
    return abs(Decimal(str(result))) if result else Decimal('0.00')


def get_budget_stats(db: Session, budget_id: int, user_id: int) -> BudgetStats:
    """Get detailed budget statistics"""
    
    budget = read_db_budget(db, budget_id, user_id)
    
    if not budget:
        raise NotFoundError(f"Budget with id {budget_id} not found")
    
    current_date = date.today()
    period_days = (budget.end_date - budget.start_date).days + 1
    
    if current_date > budget.end_date:
        days_remaining = 0
    elif current_date < budget.start_date:
        days_remaining = period_days
    else:
        days_remaining = (budget.end_date - current_date).days + 1
    
    categories_over_budget = 0
    categories_on_track = 0
    categories_under_budget = 0
    biggest_overspend_amount = Decimal('0.00')
    biggest_overspend_category = None
    most_efficient_category = None
    most_efficient_ratio = 0.0
    
    total_allocated = Decimal('0.00')
    total_spent = Decimal('0.00')
    
    for category_item in budget.budget_categories:
        spent = category_item.spent_amount # Already calculated in read_db_budget
        total_allocated += category_item.allocated_amount
        total_spent += spent
        
        overspend = spent - category_item.allocated_amount
        if overspend > 0:
            categories_over_budget += 1
            if overspend > biggest_overspend_amount:
                biggest_overspend_amount = overspend
                biggest_overspend_category = category_item.category.name
        elif spent == Decimal('0.00'):
            categories_under_budget += 1
        elif category_item.allocated_amount > 0:
            usage_ratio = float(spent / category_item.allocated_amount)
            if 0.8 <= usage_ratio <= 1.0:  # 80-100% usage considered "on track"
                categories_on_track += 1
                if usage_ratio > most_efficient_ratio:
                    most_efficient_ratio = usage_ratio
                    most_efficient_category = category_item.category.name
            else:
                categories_under_budget += 1
    
    # Calculate daily burn rate and projections
    days_elapsed = max(1, (min(current_date, budget.end_date) - budget.start_date).days + 1)
    daily_burn_rate = total_spent / days_elapsed if days_elapsed > 0 else Decimal('0.00')
    projected_total_spend = daily_burn_rate * period_days
    
    return BudgetStats(
        budget_id=budget.budget_id,
        budget_name=budget.budget_name,
        period_days=period_days,
        days_remaining=days_remaining,
        categories_count=len(budget.budget_categories),
        categories_over_budget=categories_over_budget,
        categories_on_track=categories_on_track,
        categories_under_budget=categories_under_budget,
        biggest_overspend_category=biggest_overspend_category,
        biggest_overspend_amount=biggest_overspend_amount if biggest_overspend_category else None,
        most_efficient_category=most_efficient_category,
        daily_burn_rate=daily_burn_rate,
        projected_total_spend=projected_total_spend
    )


def get_budget_performance(db: Session, budget_id: int, user_id: int) -> List[BudgetPerformance]:
    """Get performance analysis for all categories in a budget"""
    
    budget = read_db_budget(db, budget_id, user_id)
    
    if not budget:
        raise NotFoundError(f"Budget with id {budget_id} not found")
    
    current_date = date.today()
    period_days = (budget.end_date - budget.start_date).days + 1
    days_elapsed = max(1, (min(current_date, budget.end_date) - budget.start_date).days + 1)
    
    performance_list = []
    
    for category_item in budget.budget_categories:
        spent = category_item.spent_amount # Already calculated
        remaining = category_item.remaining_amount # Already calculated
        percentage_used = category_item.percentage_used # Already calculated
        
        # Determine status
        if spent > category_item.allocated_amount:
            status = "over_budget"
        elif percentage_used >= 80:
            status = "on_track"
        else:
            status = "under_budget"
        
        daily_average = spent / days_elapsed if days_elapsed > 0 else Decimal('0.00')
        projected_spend = daily_average * period_days
        
        performance_list.append(BudgetPerformance(
            budget_id=budget.budget_id,
            category_id=category_item.category_id,
            category_name=category_item.category.name,
            allocated_amount=category_item.allocated_amount,
            spent_amount=spent,
            remaining_amount=remaining,
            percentage_used=percentage_used,
            status=status,
            daily_average=daily_average,
            projected_spend=projected_spend
        ))
    
    return performance_list


def get_active_budgets(db: Session, user_id: int) -> List[BudgetDB]:
    """Get all currently active budgets for a user"""
    
    current_date = date.today()
    
    return db.query(BudgetDB).filter(
        BudgetDB.user_id == user_id,
        BudgetDB.start_date <= current_date,
        BudgetDB.end_date >= current_date
    ).order_by(BudgetDB.budget_name).all()


def get_budget_by_category_id(db: Session, user_id: int, category_id: int, 
                               target_date: Optional[date] = None) -> Optional[BudgetDB]:
    """Find the active budget that contains a specific category ID"""
    
    if target_date is None:
        target_date = date.today()
    
    budget = db.query(BudgetDB).join(BudgetCategoryDB).filter(
        BudgetDB.user_id == user_id,
        BudgetDB.start_date <= target_date,
        BudgetDB.end_date >= target_date,
        BudgetCategoryDB.category_id == category_id
    ).first()
    
    return budget


def copy_budget(db: Session, budget_id: int, user_id: int, new_budget_name: str,
                new_start_date: date, new_end_date: date) -> BudgetDB:
    """Copy an existing budget with new dates and name"""
    
    # Get the source budget
    source_budget = db.query(BudgetDB).filter(
        BudgetDB.budget_id == budget_id,
        BudgetDB.user_id == user_id
    ).first()
    
    if not source_budget:
        raise NotFoundError(f"Budget with id {budget_id} not found")
    
    # Validate dates
    if new_end_date <= new_start_date:
        raise ValueError("end_date must be after start_date")
    
    # Check for duplicate budget name
    existing_budget = db.query(BudgetDB).filter(
        BudgetDB.user_id == user_id,
        BudgetDB.budget_name.ilike(new_budget_name.strip())
    ).first()
    if existing_budget:
        raise ValueError(f"Budget with name '{new_budget_name}' already exists")
    
    try:
        # Create new budget
        new_budget = BudgetDB(
            user_id=user_id,
            budget_name=new_budget_name.strip(),
            start_date=new_start_date,
            end_date=new_end_date,
            created_at=datetime.utcnow(),
            updated_at=datetime.utcnow()
        )
        
        db.add(new_budget)
        db.flush()  # Get the budget_id
        
        # Copy categories
        for category in source_budget.budget_categories:
            new_category = BudgetCategoryDB(
                budget_id=new_budget.budget_id,
                category_id=category.category_id,
                allocated_amount=category.allocated_amount,
                created_at=datetime.utcnow()
            )
            db.add(new_category)
        
        db.commit()
        db.refresh(new_budget)
        return new_budget
        
    except IntegrityError:
        db.rollback()
        raise ValueError("Budget copy failed due to database constraint")


def get_budget_variance_report(db: Session, user_id: int, 
                              start_date: Optional[date] = None,
                              end_date: Optional[date] = None) -> List[Dict[str, Any]]:
    """Get budget variance report comparing planned vs actual spending"""
    
    query = db.query(BudgetDB).filter(BudgetDB.user_id == user_id)
    
    if start_date:
        query = query.filter(BudgetDB.end_date >= start_date)
    if end_date:
        query = query.filter(BudgetDB.start_date <= end_date)
    
    budgets = query.options(joinedload(BudgetDB.budget_categories).joinedload(BudgetCategoryDB.category)).order_by(BudgetDB.start_date).all()
    
    variance_report = []
    
    for budget in budgets:
        budget_total_allocated = sum(cat.allocated_amount for cat in budget.budget_categories)
        budget_total_spent = Decimal('0.00')
        
        category_variances = []
        
        for category_item in budget.budget_categories:
            spent = calculate_category_spending(db, budget, category_item.category_id)
            budget_total_spent += spent
            
            variance_amount = spent - category_item.allocated_amount
            variance_percentage = float(variance_amount / category_item.allocated_amount * 100) if category_item.allocated_amount > 0 else 0.0
            
            category_variances.append({
                'category': category_item.category.name,
                'allocated': float(category_item.allocated_amount),
                'spent': float(spent),
                'variance_amount': float(variance_amount),
                'variance_percentage': variance_percentage
            })
        
        budget_variance_amount = budget_total_spent - budget_total_allocated
        budget_variance_percentage = float(budget_variance_amount / budget_total_allocated * 100) if budget_total_allocated > 0 else 0.0
        
        variance_report.append({
            'budget_id': budget.budget_id,
            'budget_name': budget.budget_name,
            'start_date': budget.start_date.isoformat(),
            'end_date': budget.end_date.isoformat(),
            'total_allocated': float(budget_total_allocated),
            'total_spent': float(budget_total_spent),
            'variance_amount': float(budget_variance_amount),
            'variance_percentage': budget_variance_percentage,
            'categories': category_variances
        })
    
    return variance_report
