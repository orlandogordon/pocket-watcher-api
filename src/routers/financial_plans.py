from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from typing import List

from src.db.core import get_db, NotFoundError
from src.crud import crud_financial_plan
from src.models import financial_plan as financial_plan_models

# This is a placeholder for your actual authentication logic
def get_current_user_id():
    return 1

router = APIRouter(
    prefix="/financial_plans",
    tags=["financial_plans"],
    responses={404: {"description": "Not found"}},
)

@router.post("/", response_model=financial_plan_models.FinancialPlan)
def create_financial_plan(plan: financial_plan_models.FinancialPlanCreate, db: Session = Depends(get_db), user_id: int = Depends(get_current_user_id)):
    return crud_financial_plan.create_financial_plan(db=db, user_id=user_id, plan=plan)

@router.get("/", response_model=List[financial_plan_models.FinancialPlan])
def read_financial_plans(skip: int = 0, limit: int = 100, db: Session = Depends(get_db), user_id: int = Depends(get_current_user_id)):
    plans = crud_financial_plan.get_financial_plans(db, user_id=user_id, skip=skip, limit=limit)
    return plans

@router.get("/{plan_id}", response_model=financial_plan_models.FinancialPlan)
def read_financial_plan(plan_id: int, db: Session = Depends(get_db), user_id: int = Depends(get_current_user_id)):
    db_plan = crud_financial_plan.get_financial_plan(db, user_id=user_id, plan_id=plan_id)
    if db_plan is None:
        raise HTTPException(status_code=404, detail="Financial plan not found")
    return db_plan

@router.put("/{plan_id}", response_model=financial_plan_models.FinancialPlan)
def update_financial_plan(plan_id: int, plan: financial_plan_models.FinancialPlanUpdate, db: Session = Depends(get_db), user_id: int = Depends(get_current_user_id)):
    db_plan = crud_financial_plan.get_financial_plan(db, user_id=user_id, plan_id=plan_id)
    if db_plan is None:
        raise HTTPException(status_code=404, detail="Financial plan not found")
    return crud_financial_plan.update_financial_plan(db=db, db_plan=db_plan, plan_in=plan)

@router.delete("/{plan_id}", status_code=204)
def delete_financial_plan(plan_id: int, db: Session = Depends(get_db), user_id: int = Depends(get_current_user_id)):
    db_plan = crud_financial_plan.get_financial_plan(db, user_id=user_id, plan_id=plan_id)
    if db_plan is None:
        raise HTTPException(status_code=404, detail="Financial plan not found")
    crud_financial_plan.delete_financial_plan(db=db, db_plan=db_plan)
    return

@router.get("/{plan_id}/summary", response_model=financial_plan_models.FinancialPlanSummary)
def get_financial_plan_summary(plan_id: int, db: Session = Depends(get_db), user_id: int = Depends(get_current_user_id)):
    db_plan = crud_financial_plan.get_financial_plan(db, user_id=user_id, plan_id=plan_id)
    if db_plan is None:
        raise HTTPException(status_code=404, detail="Financial plan not found")
    return crud_financial_plan.get_financial_plan_summary(db_plan)

@router.post("/{plan_id}/entries", response_model=financial_plan_models.FinancialPlanEntry)
def create_financial_plan_entry(plan_id: int, entry: financial_plan_models.FinancialPlanEntryCreate, db: Session = Depends(get_db), user_id: int = Depends(get_current_user_id)):
    db_plan = crud_financial_plan.get_financial_plan(db, user_id=user_id, plan_id=plan_id)
    if db_plan is None:
        raise HTTPException(status_code=404, detail="Financial plan not found")
    return crud_financial_plan.create_financial_plan_entry(db=db, plan_id=plan_id, entry=entry)

@router.post("/{plan_id}/entries/bulk-upload", response_model=List[financial_plan_models.FinancialPlanEntry])
def create_bulk_financial_plan_entries(plan_id: int, bulk_data: financial_plan_models.FinancialPlanEntryBulkCreate, db: Session = Depends(get_db), user_id: int = Depends(get_current_user_id)):
    try:
        return crud_financial_plan.bulk_create_financial_plan_entries(db=db, user_id=user_id, plan_id=plan_id, bulk_data=bulk_data)
    except NotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))

@router.put("/entries/{entry_id}", response_model=financial_plan_models.FinancialPlanEntry)
def update_financial_plan_entry(entry_id: int, entry: financial_plan_models.FinancialPlanEntryUpdate, db: Session = Depends(get_db)):
    db_entry = crud_financial_plan.get_financial_plan_entry(db, entry_id=entry_id)
    if db_entry is None:
        raise HTTPException(status_code=404, detail="Financial plan entry not found")
    # TODO: Add user ownership check here
    return crud_financial_plan.update_financial_plan_entry(db=db, db_entry=db_entry, entry_in=entry)

@router.delete("/entries/{entry_id}", status_code=204)
def delete_financial_plan_entry(entry_id: int, db: Session = Depends(get_db)):
    db_entry = crud_financial_plan.get_financial_plan_entry(db, entry_id=entry_id)
    if db_entry is None:
        raise HTTPException(status_code=404, detail="Financial plan entry not found")
    # TODO: Add user ownership check here
    crud_financial_plan.delete_financial_plan_entry(db=db, db_entry=db_entry)
    return
