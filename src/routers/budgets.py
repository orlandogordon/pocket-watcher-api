from fastapi import APIRouter, Depends, HTTPException, status, Query
from sqlalchemy.orm import Session
from typing import List, Optional
from uuid import UUID

from src.crud import crud_budget
from src.models import budget as budget_models
from src.db.core import get_db, NotFoundError
from src.auth.dependencies import get_current_user_id

router = APIRouter(
    prefix="/budgets",
    tags=["budgets"],
)

def _parse_uuid(value: str) -> UUID:
    try:
        return UUID(value)
    except ValueError:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid UUID format")


# ===== TEMPLATE ENDPOINTS =====

@router.post("/templates/", response_model=budget_models.TemplateResponse, status_code=status.HTTP_201_CREATED)
def create_template(
    data: budget_models.TemplateCreate,
    db: Session = Depends(get_db),
    user_id: int = Depends(get_current_user_id),
):
    """Create a new budget template with optional category allocations."""
    from src.crud import crud_category

    resolved_ids = {}
    for cat in data.categories:
        db_cat = crud_category.read_db_category_by_uuid(db, cat.category_uuid)
        if not db_cat:
            raise HTTPException(status_code=404, detail=f"Category not found: {cat.category_uuid}")
        resolved_ids[cat.category_uuid] = db_cat.id
        if cat.subcategory_uuid:
            db_sub = crud_category.read_db_category_by_uuid(db, cat.subcategory_uuid)
            if not db_sub:
                raise HTTPException(status_code=404, detail=f"Subcategory not found: {cat.subcategory_uuid}")
            resolved_ids[cat.subcategory_uuid] = db_sub.id

    try:
        return crud_budget.create_template(db, user_id, data, resolved_category_ids=resolved_ids)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/templates/", response_model=List[budget_models.TemplateResponse])
def list_templates(
    skip: int = 0,
    limit: int = 100,
    db: Session = Depends(get_db),
    user_id: int = Depends(get_current_user_id),
):
    """List all budget templates for the current user."""
    return crud_budget.read_templates(db, user_id, skip, limit)


@router.get("/templates/{template_uuid}", response_model=budget_models.TemplateResponse)
def get_template(
    template_uuid: str,
    db: Session = Depends(get_db),
    user_id: int = Depends(get_current_user_id),
):
    """Get a specific budget template by UUID."""
    parsed = _parse_uuid(template_uuid)
    template = crud_budget.read_template(db, parsed, user_id)
    if not template:
        raise HTTPException(status_code=404, detail="Template not found")
    return template


@router.put("/templates/{template_uuid}", response_model=budget_models.TemplateResponse)
def update_template(
    template_uuid: str,
    data: budget_models.TemplateUpdate,
    db: Session = Depends(get_db),
    user_id: int = Depends(get_current_user_id),
):
    """Update a budget template's name or default status."""
    parsed = _parse_uuid(template_uuid)
    try:
        return crud_budget.update_template(db, parsed, user_id, data)
    except NotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.delete("/templates/{template_uuid}", status_code=status.HTTP_204_NO_CONTENT)
def delete_template(
    template_uuid: str,
    db: Session = Depends(get_db),
    user_id: int = Depends(get_current_user_id),
):
    """Delete a budget template. Months using it will be unassigned."""
    parsed = _parse_uuid(template_uuid)
    try:
        crud_budget.delete_template(db, parsed, user_id)
    except NotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))


# ===== TEMPLATE CATEGORY ENDPOINTS =====

@router.post("/templates/{template_uuid}/categories/",
             response_model=budget_models.TemplateCategoryResponse,
             status_code=status.HTTP_201_CREATED)
def add_template_category(
    template_uuid: str,
    data: budget_models.TemplateCategoryCreate,
    db: Session = Depends(get_db),
    user_id: int = Depends(get_current_user_id),
):
    """Add a category allocation to a template."""
    _parse_uuid(template_uuid)
    from src.crud import crud_category

    db_cat = crud_category.read_db_category_by_uuid(db, data.category_uuid)
    if not db_cat:
        raise HTTPException(status_code=404, detail="Category not found")

    sub_id = None
    if data.subcategory_uuid:
        db_sub = crud_category.read_db_category_by_uuid(db, data.subcategory_uuid)
        if not db_sub:
            raise HTTPException(status_code=404, detail="Subcategory not found")
        sub_id = db_sub.id

    parsed = _parse_uuid(template_uuid)
    try:
        return crud_budget.add_template_category(
            db, parsed, user_id, data,
            category_id=db_cat.id, subcategory_id=sub_id,
        )
    except (NotFoundError, ValueError) as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.put("/templates/categories/{allocation_uuid}",
            response_model=budget_models.TemplateCategoryResponse)
def update_template_category(
    allocation_uuid: str,
    data: budget_models.TemplateCategoryUpdate,
    db: Session = Depends(get_db),
    user_id: int = Depends(get_current_user_id),
):
    """Update a template category allocation amount."""
    parsed = _parse_uuid(allocation_uuid)
    try:
        return crud_budget.update_template_category(db, parsed, user_id, data)
    except NotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.delete("/templates/categories/{allocation_uuid}", status_code=status.HTTP_204_NO_CONTENT)
def delete_template_category(
    allocation_uuid: str,
    db: Session = Depends(get_db),
    user_id: int = Depends(get_current_user_id),
):
    """Delete a category allocation from a template."""
    parsed = _parse_uuid(allocation_uuid)
    try:
        crud_budget.delete_template_category(db, parsed, user_id)
    except NotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))


# ===== BUDGET MONTH ENDPOINTS =====

@router.get("/months/{year}/{month}", response_model=budget_models.BudgetMonthResponse)
def get_budget_month(
    year: int,
    month: int,
    db: Session = Depends(get_db),
    user_id: int = Depends(get_current_user_id),
):
    """Get budget for a specific month (auto-creates with default template if needed)."""
    try:
        return crud_budget.get_budget_month_with_spending(db, user_id, year, month)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.put("/months/{year}/{month}", response_model=budget_models.BudgetMonthResponse)
def update_budget_month(
    year: int,
    month: int,
    data: budget_models.BudgetMonthUpdate,
    db: Session = Depends(get_db),
    user_id: int = Depends(get_current_user_id),
):
    """Assign or unassign a template for a specific month."""
    resolved_template_id = None
    if data.template_uuid is not None:
        template = crud_budget.read_template(db, data.template_uuid, user_id)
        if not template:
            raise HTTPException(status_code=404, detail="Template not found")
        resolved_template_id = template.template_id

    try:
        crud_budget.update_budget_month(
            db, user_id, year, month, data,
            resolved_template_id=resolved_template_id,
        )
        # Return full spending data
        return crud_budget.get_budget_month_with_spending(db, user_id, year, month)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/months/", response_model=List[budget_models.BudgetMonthResponse])
def list_budget_months(
    start_year: Optional[int] = None,
    start_month: Optional[int] = None,
    end_year: Optional[int] = None,
    end_month: Optional[int] = None,
    db: Session = Depends(get_db),
    user_id: int = Depends(get_current_user_id),
):
    """List existing budget months (does not auto-create). Returns lightweight list without spending."""
    months = crud_budget.list_budget_months(db, user_id, start_year, start_month, end_year, end_month)
    results = []
    for m in months:
        template = None
        if m.template_id:
            from sqlalchemy.orm import joinedload
            template = db.query(crud_budget.BudgetTemplateDB).filter(
                crud_budget.BudgetTemplateDB.template_id == m.template_id
            ).first()
        results.append({
            "id": m.id,
            "year": m.year,
            "month": m.month,
            "template": template,
            "created_at": m.created_at,
            "updated_at": m.updated_at,
        })
    return results


# ===== BUDGET MONTH STATS / PERFORMANCE =====

@router.get("/months/{year}/{month}/stats", response_model=budget_models.BudgetMonthStats)
def get_budget_month_stats(
    year: int,
    month: int,
    db: Session = Depends(get_db),
    user_id: int = Depends(get_current_user_id),
):
    """Get detailed statistics for a budget month."""
    try:
        return crud_budget.get_budget_month_stats(db, user_id, year, month)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/months/{year}/{month}/performance", response_model=List[budget_models.BudgetMonthPerformance])
def get_budget_month_performance(
    year: int,
    month: int,
    db: Session = Depends(get_db),
    user_id: int = Depends(get_current_user_id),
):
    """Get performance breakdown for each category in a budget month."""
    try:
        return crud_budget.get_budget_month_performance(db, user_id, year, month)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
