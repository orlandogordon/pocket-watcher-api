from sqlalchemy.orm import Session, joinedload
from sqlalchemy.exc import IntegrityError
from sqlalchemy import and_, or_
from typing import Optional, List, Dict, Tuple
from datetime import datetime, date
from decimal import Decimal
from uuid import uuid4, UUID
from calendar import monthrange

from src.db.core import (
    BudgetTemplateDB, BudgetTemplateCategoryDB, BudgetMonthDB,
    UserDB, TransactionDB, NotFoundError, TransactionType, CategoryDB,
    TransactionSplitAllocationDB, TransactionAmortizationScheduleDB,
)
from src.models.budget import (
    TemplateCreate, TemplateUpdate, TemplateCategoryCreate, TemplateCategoryUpdate,
    BudgetMonthUpdate,
)
from src.crud.crud_transaction import get_refund_adjustments
from src.logging_config import get_logger

logger = get_logger(__name__)


# ===== TEMPLATE OPERATIONS =====

def create_template(db: Session, user_id: int, data: TemplateCreate,
                    *, resolved_category_ids: Optional[Dict[UUID, int]] = None) -> BudgetTemplateDB:
    user = db.query(UserDB).filter(UserDB.db_id == user_id).first()
    if not user:
        raise NotFoundError(f"User with id {user_id} not found")

    existing = db.query(BudgetTemplateDB).filter(
        BudgetTemplateDB.user_id == user_id,
        BudgetTemplateDB.template_name.ilike(data.template_name.strip())
    ).first()
    if existing:
        raise ValueError(f"Template with name '{data.template_name}' already exists")

    template = BudgetTemplateDB(
        id=uuid4(),
        user_id=user_id,
        template_name=data.template_name.strip(),
        is_default=data.is_default,
        created_at=datetime.utcnow(),
        updated_at=datetime.utcnow(),
    )

    try:
        # If marking as default, clear any existing default
        if data.is_default:
            _clear_default_template(db, user_id)

        db.add(template)
        db.flush()

        # Add categories
        if data.categories and resolved_category_ids:
            _add_template_categories(db, template, data.categories, resolved_category_ids)

        db.commit()
        db.refresh(template)
        return template
    except IntegrityError:
        db.rollback()
        raise ValueError("Template creation failed due to database constraint")


def read_template(db: Session, template_uuid: UUID, user_id: int) -> Optional[BudgetTemplateDB]:
    return (
        db.query(BudgetTemplateDB)
        .filter(BudgetTemplateDB.id == template_uuid, BudgetTemplateDB.user_id == user_id)
        .options(
            joinedload(BudgetTemplateDB.categories).joinedload(BudgetTemplateCategoryDB.category),
            joinedload(BudgetTemplateDB.categories).joinedload(BudgetTemplateCategoryDB.subcategory),
        )
        .first()
    )


def read_templates(db: Session, user_id: int, skip: int = 0, limit: int = 100) -> List[BudgetTemplateDB]:
    return (
        db.query(BudgetTemplateDB)
        .filter(BudgetTemplateDB.user_id == user_id)
        .options(
            joinedload(BudgetTemplateDB.categories).joinedload(BudgetTemplateCategoryDB.category),
            joinedload(BudgetTemplateDB.categories).joinedload(BudgetTemplateCategoryDB.subcategory),
        )
        .order_by(BudgetTemplateDB.template_name)
        .offset(skip).limit(limit)
        .all()
    )


def update_template(db: Session, template_uuid: UUID, user_id: int,
                     updates: TemplateUpdate) -> BudgetTemplateDB:
    template = db.query(BudgetTemplateDB).filter(
        BudgetTemplateDB.id == template_uuid, BudgetTemplateDB.user_id == user_id
    ).first()
    if not template:
        raise NotFoundError("Template not found")

    update_data = updates.model_dump(exclude_unset=True)

    if 'template_name' in update_data:
        existing = db.query(BudgetTemplateDB).filter(
            BudgetTemplateDB.user_id == user_id,
            BudgetTemplateDB.template_name.ilike(update_data['template_name'].strip()),
            BudgetTemplateDB.template_id != template.template_id,
        ).first()
        if existing:
            raise ValueError(f"Template with name '{update_data['template_name']}' already exists")

    if update_data.get('is_default'):
        _clear_default_template(db, user_id, exclude_id=template.template_id)

    for field, value in update_data.items():
        setattr(template, field, value)
    template.updated_at = datetime.utcnow()

    try:
        db.commit()
        db.refresh(template)
        return template
    except IntegrityError:
        db.rollback()
        raise ValueError("Template update failed due to database constraint")


def delete_template(db: Session, template_uuid: UUID, user_id: int) -> bool:
    template = db.query(BudgetTemplateDB).filter(
        BudgetTemplateDB.id == template_uuid, BudgetTemplateDB.user_id == user_id
    ).first()
    if not template:
        raise NotFoundError("Template not found")

    # Unassign from any months using this template
    db.query(BudgetMonthDB).filter(
        BudgetMonthDB.template_id == template.template_id
    ).update({BudgetMonthDB.template_id: None})

    db.delete(template)  # cascade deletes categories
    db.commit()
    return True


# ===== TEMPLATE CATEGORY OPERATIONS =====

def add_template_category(db: Session, template_uuid: UUID, user_id: int,
                           data: TemplateCategoryCreate,
                           *, category_id: int,
                           subcategory_id: Optional[int] = None) -> BudgetTemplateCategoryDB:
    template = db.query(BudgetTemplateDB).filter(
        BudgetTemplateDB.id == template_uuid, BudgetTemplateDB.user_id == user_id
    ).first()
    if not template:
        raise NotFoundError("Template not found")

    # Validate subcategory belongs to parent
    if subcategory_id:
        _validate_subcategory(db, category_id, subcategory_id)

    # Check for duplicate
    existing = db.query(BudgetTemplateCategoryDB).filter(
        BudgetTemplateCategoryDB.template_id == template.template_id,
        BudgetTemplateCategoryDB.category_id == category_id,
        BudgetTemplateCategoryDB.subcategory_id == subcategory_id,
    ).first()
    if existing:
        raise ValueError("This category allocation already exists in the template")

    # Envelope validation: subcategory allocations must not exceed parent
    if subcategory_id:
        _validate_envelope(db, template.template_id, category_id, data.allocated_amount)

    alloc = BudgetTemplateCategoryDB(
        id=uuid4(),
        template_id=template.template_id,
        category_id=category_id,
        subcategory_id=subcategory_id,
        allocated_amount=data.allocated_amount,
        created_at=datetime.utcnow(),
    )

    try:
        db.add(alloc)
        db.commit()
        db.refresh(alloc)
        return alloc
    except IntegrityError:
        db.rollback()
        raise ValueError("Template category creation failed due to database constraint")


def update_template_category(db: Session, allocation_uuid: UUID, user_id: int,
                              updates: TemplateCategoryUpdate) -> BudgetTemplateCategoryDB:
    alloc = (
        db.query(BudgetTemplateCategoryDB)
        .join(BudgetTemplateDB)
        .filter(BudgetTemplateCategoryDB.id == allocation_uuid, BudgetTemplateDB.user_id == user_id)
        .first()
    )
    if not alloc:
        raise NotFoundError("Template category not found")

    # Envelope validation
    if alloc.subcategory_id:
        _validate_envelope(db, alloc.template_id, alloc.category_id,
                          updates.allocated_amount, exclude_allocation_id=alloc.allocation_id)
    else:
        # Updating a parent allocation — ensure it's still >= sum of subcategory allocations
        sub_sum = _subcategory_sum(db, alloc.template_id, alloc.category_id)
        if updates.allocated_amount < sub_sum:
            raise ValueError(
                f"Parent allocation ({updates.allocated_amount}) cannot be less than "
                f"sum of subcategory allocations ({sub_sum})"
            )

    alloc.allocated_amount = updates.allocated_amount

    try:
        db.commit()
        db.refresh(alloc)
        return alloc
    except IntegrityError:
        db.rollback()
        raise ValueError("Template category update failed due to database constraint")


def delete_template_category(db: Session, allocation_uuid: UUID, user_id: int) -> bool:
    alloc = (
        db.query(BudgetTemplateCategoryDB)
        .join(BudgetTemplateDB)
        .filter(BudgetTemplateCategoryDB.id == allocation_uuid, BudgetTemplateDB.user_id == user_id)
        .first()
    )
    if not alloc:
        raise NotFoundError("Template category not found")

    # If deleting a parent allocation, also delete its subcategory allocations
    if alloc.subcategory_id is None:
        db.query(BudgetTemplateCategoryDB).filter(
            BudgetTemplateCategoryDB.template_id == alloc.template_id,
            BudgetTemplateCategoryDB.category_id == alloc.category_id,
            BudgetTemplateCategoryDB.subcategory_id.isnot(None),
        ).delete()

    db.delete(alloc)
    db.commit()
    return True


# ===== BUDGET MONTH OPERATIONS =====

def get_or_create_budget_month(db: Session, user_id: int, year: int, month: int) -> BudgetMonthDB:
    """Get-or-create a budget month entry. Auto-assigns the default template if one exists."""
    if month < 1 or month > 12:
        raise ValueError("Month must be between 1 and 12")

    budget_month = db.query(BudgetMonthDB).filter(
        BudgetMonthDB.user_id == user_id,
        BudgetMonthDB.year == year,
        BudgetMonthDB.month == month,
    ).first()

    if budget_month:
        return budget_month

    # Find user's default template
    default_template = db.query(BudgetTemplateDB).filter(
        BudgetTemplateDB.user_id == user_id,
        BudgetTemplateDB.is_default == True,
    ).first()

    budget_month = BudgetMonthDB(
        id=uuid4(),
        user_id=user_id,
        template_id=default_template.template_id if default_template else None,
        year=year,
        month=month,
        created_at=datetime.utcnow(),
        updated_at=datetime.utcnow(),
    )
    db.add(budget_month)
    db.commit()
    db.refresh(budget_month)
    return budget_month


def update_budget_month(db: Session, user_id: int, year: int, month: int,
                         updates: BudgetMonthUpdate,
                         *, resolved_template_id: Optional[int] = None) -> BudgetMonthDB:
    """Update a budget month's template assignment."""
    budget_month = get_or_create_budget_month(db, user_id, year, month)

    if updates.template_uuid is None:
        budget_month.template_id = None
    else:
        budget_month.template_id = resolved_template_id

    budget_month.updated_at = datetime.utcnow()
    db.commit()
    db.refresh(budget_month)
    return budget_month


def list_budget_months(db: Session, user_id: int,
                        start_year: Optional[int] = None, start_month: Optional[int] = None,
                        end_year: Optional[int] = None, end_month: Optional[int] = None) -> List[BudgetMonthDB]:
    """List existing budget months for a user (does NOT auto-create)."""
    query = db.query(BudgetMonthDB).filter(BudgetMonthDB.user_id == user_id)

    if start_year and start_month:
        query = query.filter(
            (BudgetMonthDB.year > start_year) |
            ((BudgetMonthDB.year == start_year) & (BudgetMonthDB.month >= start_month))
        )
    if end_year and end_month:
        query = query.filter(
            (BudgetMonthDB.year < end_year) |
            ((BudgetMonthDB.year == end_year) & (BudgetMonthDB.month <= end_month))
        )

    return query.order_by(BudgetMonthDB.year.desc(), BudgetMonthDB.month.desc()).all()


# ===== SPENDING CALCULATION =====

def _month_date_range(year: int, month: int) -> Tuple[date, date]:
    """Get the first and last day of a given month."""
    start = date(year, month, 1)
    _, last_day = monthrange(year, month)
    end = date(year, month, last_day)
    return start, end


def calculate_category_spending(db: Session, user_id: int, year: int, month: int,
                                 category_id: int,
                                 subcategory_id: Optional[int] = None) -> Decimal:
    """Calculate total spending for a category (optionally subcategory) within a calendar month.
    Accounts for refund/offset/reversal relationships by adjusting effective amounts."""

    start_date, end_date = _month_date_range(year, month)

    # Build category filter for transactions
    def _txn_category_filter(query):
        if subcategory_id is not None:
            return query.filter(
                TransactionDB.category_id == category_id,
                TransactionDB.subcategory_id == subcategory_id,
            )
        return query.filter(TransactionDB.category_id == category_id)

    expense_txns = _txn_category_filter(
        db.query(TransactionDB.db_id, TransactionDB.amount)
        .filter(
            TransactionDB.user_id == user_id,
            TransactionDB.transaction_date >= start_date,
            TransactionDB.transaction_date <= end_date,
            TransactionDB.transaction_type.in_([
                TransactionType.PURCHASE,
                TransactionType.WITHDRAWAL,
                TransactionType.FEE,
            ]),
        )
    ).all()

    if not expense_txns:
        # Still check amortization allocations from transactions outside this period
        amort_query = (
            db.query(TransactionAmortizationScheduleDB)
            .join(TransactionDB, TransactionAmortizationScheduleDB.transaction_id == TransactionDB.db_id)
            .filter(
                TransactionDB.user_id == user_id,
                TransactionAmortizationScheduleDB.month_date >= start_date,
                TransactionAmortizationScheduleDB.month_date <= end_date,
            )
        )
        amort_query = _txn_category_filter(amort_query)
        amort_allocs_only = amort_query.all()
        return sum(a.amount for a in amort_allocs_only)

    txn_ids = [t.db_id for t in expense_txns]
    adjustments, absorbed_ids = get_refund_adjustments(db, user_id, txn_ids)

    # Find amortized transactions
    amortized_txn_ids = set(
        row[0] for row in
        db.query(TransactionAmortizationScheduleDB.transaction_id)
        .filter(TransactionAmortizationScheduleDB.transaction_id.in_(txn_ids))
        .distinct()
        .all()
    )

    total = Decimal('0.00')
    for txn in expense_txns:
        if txn.db_id in absorbed_ids:
            continue
        if txn.db_id in amortized_txn_ids:
            continue
        effective = abs(txn.amount) - adjustments.get(txn.db_id, Decimal('0.00'))
        total += max(effective, Decimal('0.00'))

    # Amortization allocations for this category within the month
    amort_query = (
        db.query(
            TransactionAmortizationScheduleDB.amount,
            TransactionAmortizationScheduleDB.transaction_id,
            TransactionDB.amount.label("txn_amount"),
        )
        .join(TransactionDB, TransactionAmortizationScheduleDB.transaction_id == TransactionDB.db_id)
        .filter(
            TransactionDB.user_id == user_id,
            TransactionAmortizationScheduleDB.month_date >= start_date,
            TransactionAmortizationScheduleDB.month_date <= end_date,
        )
    )
    amort_query = _txn_category_filter(amort_query)
    amort_allocs = amort_query.all()

    if amort_allocs:
        amort_txn_ids_list = list(set(a[1] for a in amort_allocs))
        amort_adjustments, amort_absorbed = get_refund_adjustments(db, user_id, amort_txn_ids_list)

        for alloc_amount, txn_db_id, txn_amount in amort_allocs:
            if txn_db_id in amort_absorbed:
                continue
            adj = amort_adjustments.get(txn_db_id, Decimal('0.00'))
            if adj and txn_amount:
                ratio = 1 - adj / abs(txn_amount)
                effective = max(alloc_amount * ratio, Decimal('0.00'))
            else:
                effective = alloc_amount
            total += effective

    # Split allocations
    split_query = (
        db.query(
            TransactionSplitAllocationDB.amount,
            TransactionDB.db_id,
            TransactionDB.amount.label("txn_amount"),
        )
        .join(TransactionDB, TransactionSplitAllocationDB.transaction_id == TransactionDB.db_id)
        .filter(
            TransactionDB.user_id == user_id,
            TransactionSplitAllocationDB.category_id == category_id,
            TransactionDB.transaction_date >= start_date,
            TransactionDB.transaction_date <= end_date,
            TransactionDB.transaction_type.in_([
                TransactionType.PURCHASE,
                TransactionType.WITHDRAWAL,
                TransactionType.FEE,
            ]),
        )
    )
    if subcategory_id is not None:
        split_query = split_query.filter(
            TransactionSplitAllocationDB.subcategory_id == subcategory_id
        )
    split_allocs = split_query.all()

    if split_allocs:
        split_txn_ids = list(set(sa[1] for sa in split_allocs))
        split_adjustments, split_absorbed = get_refund_adjustments(db, user_id, split_txn_ids)

        for alloc_amount, txn_db_id, txn_amount in split_allocs:
            if txn_db_id in split_absorbed:
                continue
            adj = split_adjustments.get(txn_db_id, Decimal('0.00'))
            if adj and txn_amount:
                ratio = 1 - adj / txn_amount
                effective = max(alloc_amount * ratio, Decimal('0.00'))
            else:
                effective = alloc_amount
            total += effective

    return total


def get_budget_month_with_spending(db: Session, user_id: int, year: int, month: int) -> dict:
    """Get a budget month with its template allocations and calculated spending."""
    budget_month = get_or_create_budget_month(db, user_id, year, month)

    # Load template with categories if assigned
    template = None
    categories_spending = []
    total_allocated = Decimal('0.00')
    total_spent = Decimal('0.00')

    if budget_month.template_id:
        template = (
            db.query(BudgetTemplateDB)
            .filter(BudgetTemplateDB.template_id == budget_month.template_id)
            .options(
                joinedload(BudgetTemplateDB.categories).joinedload(BudgetTemplateCategoryDB.category),
                joinedload(BudgetTemplateDB.categories).joinedload(BudgetTemplateCategoryDB.subcategory),
            )
            .first()
        )

        if template:
            for alloc in template.categories:
                spent = calculate_category_spending(
                    db, user_id, year, month,
                    alloc.category_id, alloc.subcategory_id
                )
                remaining = alloc.allocated_amount - spent
                pct = float(spent / alloc.allocated_amount * 100) if alloc.allocated_amount > 0 else 0.0

                categories_spending.append({
                    "category": alloc.category,
                    "subcategory": alloc.subcategory,
                    "allocated_amount": alloc.allocated_amount,
                    "spent_amount": spent,
                    "remaining_amount": remaining,
                    "percentage_used": pct,
                })
                total_allocated += alloc.allocated_amount
                total_spent += spent

    return {
        "id": budget_month.id,
        "year": budget_month.year,
        "month": budget_month.month,
        "template": template,
        "categories": categories_spending,
        "total_allocated": total_allocated,
        "total_spent": total_spent,
        "total_remaining": total_allocated - total_spent,
        "percentage_used": float(total_spent / total_allocated * 100) if total_allocated > 0 else 0.0,
        "created_at": budget_month.created_at,
        "updated_at": budget_month.updated_at,
    }


def get_budget_month_stats(db: Session, user_id: int, year: int, month: int) -> dict:
    """Get detailed stats for a budget month."""
    data = get_budget_month_with_spending(db, user_id, year, month)

    start_date, end_date = _month_date_range(year, month)
    period_days = (end_date - start_date).days + 1
    current_date = date.today()

    if current_date > end_date:
        days_remaining = 0
    elif current_date < start_date:
        days_remaining = period_days
    else:
        days_remaining = (end_date - current_date).days + 1

    categories_over = 0
    categories_on_track = 0
    categories_under = 0
    biggest_overspend_amount = Decimal('0.00')
    biggest_overspend_category = None
    most_efficient_category = None
    most_efficient_ratio = 0.0

    for cat in data["categories"]:
        spent = cat["spent_amount"]
        allocated = cat["allocated_amount"]
        overspend = spent - allocated
        if overspend > 0:
            categories_over += 1
            if overspend > biggest_overspend_amount:
                biggest_overspend_amount = overspend
                biggest_overspend_category = cat["category"].name
        elif spent == Decimal('0.00'):
            categories_under += 1
        elif allocated > 0:
            usage_ratio = float(spent / allocated)
            if 0.8 <= usage_ratio <= 1.0:
                categories_on_track += 1
                if usage_ratio > most_efficient_ratio:
                    most_efficient_ratio = usage_ratio
                    most_efficient_category = cat["category"].name
            else:
                categories_under += 1

    days_elapsed = max(1, (min(current_date, end_date) - start_date).days + 1)
    total_spent = data["total_spent"]
    daily_burn_rate = total_spent / days_elapsed if days_elapsed > 0 else Decimal('0.00')
    projected_total_spend = daily_burn_rate * period_days

    return {
        "id": data["id"],
        "year": year,
        "month": month,
        "template_name": data["template"].template_name if data["template"] else None,
        "period_days": period_days,
        "days_remaining": days_remaining,
        "categories_count": len(data["categories"]),
        "categories_over_budget": categories_over,
        "categories_on_track": categories_on_track,
        "categories_under_budget": categories_under,
        "biggest_overspend_category": biggest_overspend_category,
        "biggest_overspend_amount": biggest_overspend_amount if biggest_overspend_category else None,
        "most_efficient_category": most_efficient_category,
        "daily_burn_rate": daily_burn_rate,
        "projected_total_spend": projected_total_spend,
    }


def get_budget_month_performance(db: Session, user_id: int, year: int, month: int) -> List[dict]:
    """Get performance breakdown for each allocation in a budget month."""
    data = get_budget_month_with_spending(db, user_id, year, month)

    start_date, end_date = _month_date_range(year, month)
    period_days = (end_date - start_date).days + 1
    current_date = date.today()
    days_elapsed = max(1, (min(current_date, end_date) - start_date).days + 1)

    results = []
    for cat in data["categories"]:
        spent = cat["spent_amount"]
        allocated = cat["allocated_amount"]
        remaining = cat["remaining_amount"]
        pct = cat["percentage_used"]

        if spent > allocated:
            status = "over_budget"
        elif pct >= 80:
            status = "on_track"
        else:
            status = "under_budget"

        daily_average = spent / days_elapsed if days_elapsed > 0 else Decimal('0.00')
        projected_spend = daily_average * period_days

        result = {
            "category_uuid": cat["category"].uuid,
            "category_name": cat["category"].name,
            "allocated_amount": allocated,
            "spent_amount": spent,
            "remaining_amount": remaining,
            "percentage_used": pct,
            "status": status,
            "daily_average": daily_average,
            "projected_spend": projected_spend,
        }
        if cat["subcategory"]:
            result["subcategory_uuid"] = cat["subcategory"].uuid
            result["subcategory_name"] = cat["subcategory"].name

        results.append(result)

    return results


# ===== HELPERS =====

def _clear_default_template(db: Session, user_id: int, exclude_id: Optional[int] = None):
    """Clear the is_default flag on all templates for a user."""
    query = db.query(BudgetTemplateDB).filter(
        BudgetTemplateDB.user_id == user_id,
        BudgetTemplateDB.is_default == True,
    )
    if exclude_id:
        query = query.filter(BudgetTemplateDB.template_id != exclude_id)
    query.update({BudgetTemplateDB.is_default: False})


def _validate_subcategory(db: Session, parent_category_id: int, subcategory_id: int):
    """Validate that a subcategory belongs to the given parent category."""
    sub = db.query(CategoryDB).filter(CategoryDB.id == subcategory_id).first()
    if not sub:
        raise NotFoundError("Subcategory not found")
    if sub.parent_category_id != parent_category_id:
        raise ValueError("Subcategory does not belong to the specified parent category")


def _subcategory_sum(db: Session, template_id: int, category_id: int,
                     exclude_allocation_id: Optional[int] = None) -> Decimal:
    """Sum of all subcategory allocations for a given parent category in a template."""
    query = db.query(BudgetTemplateCategoryDB).filter(
        BudgetTemplateCategoryDB.template_id == template_id,
        BudgetTemplateCategoryDB.category_id == category_id,
        BudgetTemplateCategoryDB.subcategory_id.isnot(None),
    )
    if exclude_allocation_id:
        query = query.filter(BudgetTemplateCategoryDB.allocation_id != exclude_allocation_id)
    return sum(a.allocated_amount for a in query.all())


def _validate_envelope(db: Session, template_id: int, category_id: int,
                        new_amount: Decimal, exclude_allocation_id: Optional[int] = None):
    """Validate that subcategory allocations don't exceed the parent envelope."""
    parent_alloc = db.query(BudgetTemplateCategoryDB).filter(
        BudgetTemplateCategoryDB.template_id == template_id,
        BudgetTemplateCategoryDB.category_id == category_id,
        BudgetTemplateCategoryDB.subcategory_id.is_(None),
    ).first()

    if not parent_alloc:
        raise ValueError("Must create a parent category allocation before adding subcategory allocations")

    existing_sub_sum = _subcategory_sum(db, template_id, category_id, exclude_allocation_id)
    if existing_sub_sum + new_amount > parent_alloc.allocated_amount:
        raise ValueError(
            f"Subcategory allocations ({existing_sub_sum + new_amount}) would exceed "
            f"parent envelope ({parent_alloc.allocated_amount})"
        )


def _add_template_categories(db: Session, template: BudgetTemplateDB,
                              categories: List[TemplateCategoryCreate],
                              resolved_ids: Dict[UUID, int]):
    """Add categories to a template during creation."""
    for cat_data in categories:
        cat_id = resolved_ids.get(cat_data.category_uuid)
        if cat_id is None:
            raise ValueError(f"Category UUID {cat_data.category_uuid} was not resolved")

        sub_id = None
        if cat_data.subcategory_uuid:
            sub_id = resolved_ids.get(cat_data.subcategory_uuid)
            if sub_id is None:
                raise ValueError(f"Subcategory UUID {cat_data.subcategory_uuid} was not resolved")
            _validate_subcategory(db, cat_id, sub_id)

        alloc = BudgetTemplateCategoryDB(
            id=uuid4(),
            template_id=template.template_id,
            category_id=cat_id,
            subcategory_id=sub_id,
            allocated_amount=cat_data.allocated_amount,
            created_at=datetime.utcnow(),
        )
        db.add(alloc)

    # Validate envelopes after all categories are added
    db.flush()
    _validate_envelopes_bulk(db, template.template_id)


def _validate_envelopes_bulk(db: Session, template_id: int):
    """Validate all envelope constraints for a template."""
    allocs = db.query(BudgetTemplateCategoryDB).filter(
        BudgetTemplateCategoryDB.template_id == template_id
    ).all()

    # Group by category_id
    parent_amounts: Dict[int, Decimal] = {}
    sub_sums: Dict[int, Decimal] = {}

    for alloc in allocs:
        if alloc.subcategory_id is None:
            parent_amounts[alloc.category_id] = alloc.allocated_amount
        else:
            sub_sums[alloc.category_id] = sub_sums.get(alloc.category_id, Decimal('0.00')) + alloc.allocated_amount

    for cat_id, sub_sum in sub_sums.items():
        parent = parent_amounts.get(cat_id)
        if parent is None:
            raise ValueError("Must create a parent category allocation before adding subcategory allocations")
        if sub_sum > parent:
            raise ValueError(
                f"Subcategory allocations ({sub_sum}) exceed parent envelope ({parent})"
            )
