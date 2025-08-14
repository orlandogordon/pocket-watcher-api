from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session
from typing import List, Optional
from datetime import date

from src.crud import crud_budget
from src.models import budget as budget_models
from src.db.core import get_db, NotFoundError

router = APIRouter(
    prefix="/budgets",
    tags=["budgets"],
)

# This is a placeholder for a proper authentication dependency.
def get_current_user_id() -> int:
    return 1

@router.post("/", response_model=budget_models.BudgetResponse, status_code=status.HTTP_201_CREATED)
def create_budget(
    budget: budget_models.BudgetCreate,
    db: Session = Depends(get_db),
    user_id: int = Depends(get_current_user_id)
):
    """
    Create a new budget with associated categories.
    """
    try:
        return crud_budget.create_db_budget(db=db, user_id=user_id, budget_data=budget)
    except ValueError as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))

@router.get("/", response_model=List[budget_models.BudgetResponse])
def read_budgets(
    skip: int = 0,
    limit: int = 100,
    include_spending: bool = True,
    active_only: bool = False,
    db: Session = Depends(get_db),
    user_id: int = Depends(get_current_user_id)
):
    """
    Retrieve all budgets for the current user.
    """
    return crud_budget.read_db_budgets(
        db=db, user_id=user_id, skip=skip, limit=limit, 
        include_spending=include_spending, active_only=active_only
    )

@router.get("/{budget_id}", response_model=budget_models.BudgetResponse)
def read_budget(
    budget_id: int,
    db: Session = Depends(get_db),
    user_id: int = Depends(get_current_user_id)
):
    """
    Retrieve a specific budget by its ID, including category and spending details.
    """
    db_budget = crud_budget.read_db_budget(db=db, budget_id=budget_id, user_id=user_id)
    if db_budget is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Budget not found")
    return db_budget

@router.put("/{budget_id}", response_model=budget_models.BudgetResponse)
def update_budget(
    budget_id: int,
    budget: budget_models.BudgetUpdate,
    db: Session = Depends(get_db),
    user_id: int = Depends(get_current_user_id)
):
    """
    Update a budget's name or date range.
    """
    try:
        return crud_budget.update_db_budget(db=db, budget_id=budget_id, user_id=user_id, budget_updates=budget)
    except NotFoundError as e:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(e))
    except ValueError as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))

@router.delete("/{budget_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_budget(
    budget_id: int,
    db: Session = Depends(get_db),
    user_id: int = Depends(get_current_user_id)
):
    """
    Delete a budget and all of its associated categories.
    """
    try:
        if not crud_budget.delete_db_budget(db=db, budget_id=budget_id, user_id=user_id):
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Budget not found")
    except (NotFoundError, ValueError) as e:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(e))

@router.post("/{budget_id}/categories/", response_model=budget_models.BudgetCategoryResponse, status_code=status.HTTP_201_CREATED)
def add_category_to_budget(
    budget_id: int,
    category: budget_models.BudgetCategoryCreate,
    db: Session = Depends(get_db),
    user_id: int = Depends(get_current_user_id)
):
    """
    Add a new category to an existing budget.
    """
    try:
        return crud_budget.add_budget_category(db=db, budget_id=budget_id, user_id=user_id, category_data=category)
    except (NotFoundError, ValueError) as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))

@router.put("/categories/{budget_category_id}", response_model=budget_models.BudgetCategoryResponse)
def update_budget_category(
    budget_category_id: int,
    category: budget_models.BudgetCategoryUpdate,
    db: Session = Depends(get_db),
    user_id: int = Depends(get_current_user_id)
):
    """
    Update a budget category's allocated amount.
    """
    try:
        return crud_budget.update_budget_category(db=db, budget_category_id=budget_category_id, user_id=user_id, category_updates=category)
    except (NotFoundError, ValueError) as e:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(e))

@router.delete("/categories/{budget_category_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_budget_category(
    budget_category_id: int,
    db: Session = Depends(get_db),
    user_id: int = Depends(get_current_user_id)
):
    """
    Delete a category from a budget.
    """
    try:
        if not crud_budget.delete_budget_category(db=db, budget_category_id=budget_category_id, user_id=user_id):
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Category not found")
    except (NotFoundError, ValueError) as e:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(e))

@router.get("/{budget_id}/stats", response_model=budget_models.BudgetStats)
def get_budget_stats(
    budget_id: int,
    db: Session = Depends(get_db),
    user_id: int = Depends(get_current_user_id)
):
    """
    Get detailed statistics and insights for a specific budget.
    """
    try:
        return crud_budget.get_budget_stats(db=db, budget_id=budget_id, user_id=user_id)
    except NotFoundError as e:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(e))

@router.get("/{budget_id}/performance", response_model=List[budget_models.BudgetPerformance])
def get_budget_performance(
    budget_id: int,
    db: Session = Depends(get_db),
    user_id: int = Depends(get_current_user_id)
):
    """
    Get a performance breakdown for each category in a budget.
    """
    try:
        return crud_budget.get_budget_performance(db=db, budget_id=budget_id, user_id=user_id)
    except NotFoundError as e:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(e))

@router.post("/{budget_id}/copy", response_model=budget_models.BudgetResponse)
def copy_budget(
    budget_id: int,
    new_budget_name: str,
    new_start_date: date,
    new_end_date: date,
    db: Session = Depends(get_db),
    user_id: int = Depends(get_current_user_id)
):
    """
    Create a copy of an existing budget with a new name and date range.
    """
    try:
        return crud_budget.copy_budget(
            db=db, budget_id=budget_id, user_id=user_id, 
            new_budget_name=new_budget_name, 
            new_start_date=new_start_date, 
            new_end_date=new_end_date
        )
    except (NotFoundError, ValueError) as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))
