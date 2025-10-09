from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session
from typing import List

from src.crud import crud_debt
from src.models import debt as debt_models
from src.db.core import get_db, NotFoundError

router = APIRouter(
    prefix="/debt",
    tags=["debt"],
)

# This is a placeholder for a proper authentication dependency.
def get_current_user_id() -> int:
    return 1

# ===== DEBT REPAYMENT PLANS =====

@router.post("/plans/", response_model=debt_models.DebtRepaymentPlanResponse, status_code=status.HTTP_201_CREATED)
def create_debt_repayment_plan(
    plan_data: debt_models.DebtRepaymentPlanCreate,
    db: Session = Depends(get_db),
    user_id: int = Depends(get_current_user_id)
):
    """
    Create a new debt repayment plan.
    """
    return crud_debt.create_debt_repayment_plan(db=db, user_id=user_id, plan_data=plan_data)

@router.get("/plans/", response_model=List[debt_models.DebtRepaymentPlanResponse])
def read_debt_repayment_plans(
    db: Session = Depends(get_db),
    user_id: int = Depends(get_current_user_id)
):
    """
    Retrieve all debt repayment plans for the current user.
    """
    return crud_debt.read_all_debt_repayment_plans_for_user(db=db, user_id=user_id)

@router.get("/plans/{plan_id}", response_model=debt_models.DebtRepaymentPlanResponse)
def read_debt_repayment_plan(
    plan_id: int,
    db: Session = Depends(get_db),
    user_id: int = Depends(get_current_user_id)
):
    """
    Retrieve a specific debt repayment plan.
    """
    db_plan = crud_debt.read_debt_repayment_plan(db=db, plan_id=plan_id, user_id=user_id)
    if db_plan is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Plan not found")
    return db_plan

@router.post("/plans/accounts/", status_code=status.HTTP_201_CREATED)
def add_account_to_debt_plan(
    link_data: debt_models.DebtPlanAccountLinkCreate,
    db: Session = Depends(get_db),
    user_id: int = Depends(get_current_user_id)
):
    """
    Link a loan account to a debt repayment plan.
    """
    try:
        crud_debt.add_account_to_plan(db=db, user_id=user_id, link_data=link_data)
        return {"message": "Account successfully added to plan."}
    except NotFoundError as e:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(e))

@router.delete("/plans/{plan_id}/accounts/{account_id}", status_code=status.HTTP_204_NO_CONTENT)
def remove_account_from_debt_plan(
    plan_id: int,
    account_id: int,
    db: Session = Depends(get_db),
    user_id: int = Depends(get_current_user_id)
):
    """
    Remove a loan account from a debt repayment plan.
    """
    try:
        if not crud_debt.remove_account_from_plan(db=db, user_id=user_id, plan_id=plan_id, account_id=account_id):
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Account link to plan not found.")
    except NotFoundError as e:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(e))

# ===== DEBT REPAYMENT SCHEDULES =====

@router.post("/schedules/", status_code=status.HTTP_201_CREATED)
def create_or_update_payment_schedule(
    schedule_data: debt_models.DebtRepaymentScheduleBulkCreate,
    db: Session = Depends(get_db),
    user_id: int = Depends(get_current_user_id)
):
    """
    Create or replace the monthly payment schedule for a specific loan account.
    """
    try:
        count = crud_debt.bulk_create_or_update_schedule(db=db, user_id=user_id, schedule_data=schedule_data)
        return {"message": f"{count} schedule entries created/updated for account {schedule_data.account_id}."}
    except NotFoundError as e:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(e))

@router.get("/schedules/{account_id}", response_model=List[debt_models.DebtRepaymentScheduleResponse])
def read_payment_schedule_for_account(
    account_id: int,
    db: Session = Depends(get_db),
    user_id: int = Depends(get_current_user_id)
):
    """
    Retrieve the monthly payment schedule for a specific loan account.
    """
    try:
        return crud_debt.read_schedule_for_account(db=db, user_id=user_id, account_id=account_id)
    except NotFoundError as e:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(e))

# ===== DEBT PAYMENTS =====

@router.post("/payments/", response_model=debt_models.DebtPaymentResponse, status_code=status.HTTP_201_CREATED)
def create_debt_payment(
    payment_data: debt_models.DebtPaymentCreate,
    db: Session = Depends(get_db),
    user_id: int = Depends(get_current_user_id)
):
    """
    Record a new debt payment against a loan account.
    """
    try:
        return crud_debt.create_debt_payment(db=db, user_id=user_id, payment_data=payment_data)
    except NotFoundError as e:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(e))
    except ValueError as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))

@router.post("/payments/bulk-upload", response_model=List[debt_models.DebtPaymentResponse])
def create_bulk_debt_payments(bulk_data: debt_models.DebtPaymentBulkCreate, db: Session = Depends(get_db), user_id: int = Depends(get_current_user_id)):
    try:
        return crud_debt.bulk_create_debt_payments(db=db, user_id=user_id, bulk_data=bulk_data)
    except NotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except ValueError as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))

@router.get("/accounts/{account_id}/payments/", response_model=List[debt_models.DebtPaymentResponse])
def read_debt_payments_for_account(
    account_id: int,
    db: Session = Depends(get_db),
    user_id: int = Depends(get_current_user_id)
):
    """
    Retrieve all debt payments for a specific loan account.
    """
    try:
        return crud_debt.read_all_debt_payments_for_account(db=db, user_id=user_id, account_id=account_id)
    except NotFoundError as e:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(e))

@router.get("/payments/{payment_id}/", response_model=debt_models.DebtPaymentResponse)
def read_debt_payment(
    payment_id: int,
    db: Session = Depends(get_db),
    user_id: int = Depends(get_current_user_id)
):
    """
    Retrieve a specific debt payment by its ID.
    """
    db_payment = crud_debt.read_debt_payment(db=db, payment_id=payment_id, user_id=user_id)
    if db_payment is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Payment not found")
    return db_payment

@router.put("/payments/{payment_id}/", response_model=debt_models.DebtPaymentResponse)
def update_debt_payment(
    payment_id: int,
    payment_data: debt_models.DebtPaymentUpdate,
    db: Session = Depends(get_db),
    user_id: int = Depends(get_current_user_id)
):
    """
    Update a debt payment.
    """
    updated_payment = crud_debt.update_debt_payment(db=db, payment_id=payment_id, user_id=user_id, payment_data=payment_data)
    if updated_payment is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Payment not found")
    return updated_payment

@router.delete("/payments/{payment_id}/", status_code=status.HTTP_204_NO_CONTENT)
def delete_debt_payment(
    payment_id: int,
    db: Session = Depends(get_db),
    user_id: int = Depends(get_current_user_id)
):
    """
    Delete a debt payment.
    """
    if not crud_debt.delete_debt_payment(db=db, payment_id=payment_id, user_id=user_id):
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Payment not found")
