from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError
from typing import List
from uuid import UUID

from src.crud import crud_debt, crud_account
from src.models import debt as debt_models
from src.db.core import get_db, NotFoundError

router = APIRouter(
    prefix="/debt",
    tags=["debt"],
)

# This is a placeholder for a proper authentication dependency.
def get_current_user_id() -> int:
    return 1

def _parse_uuid(value: str) -> UUID:
    try:
        return UUID(value)
    except ValueError:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid UUID format")

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
    try:
        return crud_debt.create_debt_repayment_plan(db=db, user_id=user_id, plan_data=plan_data)
    except IntegrityError:
        db.rollback()
        raise HTTPException(status_code=409, detail="A debt repayment plan with this name already exists")

@router.get("/plans/", response_model=List[debt_models.DebtRepaymentPlanResponse])
def read_debt_repayment_plans(
    db: Session = Depends(get_db),
    user_id: int = Depends(get_current_user_id)
):
    """
    Retrieve all debt repayment plans for the current user.
    """
    return crud_debt.read_all_debt_repayment_plans_for_user(db=db, user_id=user_id)

@router.get("/plans/{plan_uuid}", response_model=debt_models.DebtRepaymentPlanResponse)
def read_debt_repayment_plan(
    plan_uuid: str,
    db: Session = Depends(get_db),
    user_id: int = Depends(get_current_user_id)
):
    """
    Retrieve a specific debt repayment plan.
    """
    parsed_uuid = _parse_uuid(plan_uuid)
    db_plan = crud_debt.read_debt_repayment_plan_by_uuid(db=db, plan_uuid=parsed_uuid, user_id=user_id)
    if db_plan is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Plan not found")
    return db_plan

@router.put("/plans/{plan_uuid}", response_model=debt_models.DebtRepaymentPlanResponse)
def update_debt_repayment_plan(
    plan_uuid: str,
    plan_data: debt_models.DebtRepaymentPlanUpdate,
    db: Session = Depends(get_db),
    user_id: int = Depends(get_current_user_id)
):
    """
    Update a debt repayment plan.
    """
    parsed_uuid = _parse_uuid(plan_uuid)
    updated_plan = crud_debt.update_debt_repayment_plan_by_uuid(db=db, plan_uuid=parsed_uuid, user_id=user_id, plan_data=plan_data)
    if updated_plan is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Plan not found")
    return updated_plan

@router.delete("/plans/{plan_uuid}", status_code=status.HTTP_204_NO_CONTENT)
def delete_debt_repayment_plan(
    plan_uuid: str,
    db: Session = Depends(get_db),
    user_id: int = Depends(get_current_user_id)
):
    """
    Delete a debt repayment plan.
    """
    parsed_uuid = _parse_uuid(plan_uuid)
    if not crud_debt.delete_debt_repayment_plan_by_uuid(db=db, plan_uuid=parsed_uuid, user_id=user_id):
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Plan not found")

@router.post("/plans/accounts/", status_code=status.HTTP_201_CREATED)
def add_account_to_debt_plan(
    link_data: debt_models.DebtPlanAccountLinkCreate,
    db: Session = Depends(get_db),
    user_id: int = Depends(get_current_user_id)
):
    """
    Link a loan account to a debt repayment plan.
    """
    # Resolve UUIDs
    db_plan = crud_debt.read_debt_repayment_plan_by_uuid(db, link_data.plan_uuid, user_id)
    if not db_plan:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Plan not found")
    db_account = crud_account.read_db_account_by_uuid(db, link_data.account_uuid, user_id)
    if not db_account:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Account not found")
    try:
        crud_debt.add_account_to_plan(db=db, user_id=user_id, link_data=link_data, plan_id=db_plan.plan_id, account_id=db_account.id)
        return {"message": "Account successfully added to plan."}
    except NotFoundError as e:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(e))

@router.get("/plans/{plan_uuid}/accounts/", response_model=List[debt_models.DebtPlanAccountLinkResponse])
def read_accounts_for_plan(
    plan_uuid: str,
    db: Session = Depends(get_db),
    user_id: int = Depends(get_current_user_id)
):
    """
    Retrieve all accounts linked to a debt repayment plan.
    """
    parsed_uuid = _parse_uuid(plan_uuid)
    db_plan = crud_debt.read_debt_repayment_plan_by_uuid(db=db, plan_uuid=parsed_uuid, user_id=user_id)
    if db_plan is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Plan not found")
    accounts = crud_debt.read_accounts_for_plan(db=db, plan_id=db_plan.plan_id, user_id=user_id)
    return [{"account_uuid": a.uuid} for a in accounts]

@router.delete("/plans/{plan_uuid}/accounts/{account_uuid}", status_code=status.HTTP_204_NO_CONTENT)
def remove_account_from_debt_plan(
    plan_uuid: str,
    account_uuid: str,
    db: Session = Depends(get_db),
    user_id: int = Depends(get_current_user_id)
):
    """
    Remove a loan account from a debt repayment plan.
    """
    parsed_plan_uuid = _parse_uuid(plan_uuid)
    parsed_account_uuid = _parse_uuid(account_uuid)
    db_plan = crud_debt.read_debt_repayment_plan_by_uuid(db=db, plan_uuid=parsed_plan_uuid, user_id=user_id)
    if db_plan is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Plan not found")
    db_account = crud_account.read_db_account_by_uuid(db=db, account_uuid=parsed_account_uuid, user_id=user_id)
    if db_account is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Account not found")
    try:
        if not crud_debt.remove_account_from_plan(db=db, user_id=user_id, plan_id=db_plan.plan_id, account_id=db_account.id):
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
    # Resolve account UUID
    db_account = crud_account.read_db_account_by_uuid(db, schedule_data.account_uuid, user_id)
    if not db_account:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Account not found")
    try:
        count = crud_debt.bulk_create_or_update_schedule(db=db, user_id=user_id, schedule_data=schedule_data, account_id=db_account.id)
        return {"message": f"{count} schedule entries created/updated for account."}
    except NotFoundError as e:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(e))

@router.get("/schedules/{account_uuid}", response_model=List[debt_models.DebtRepaymentScheduleResponse])
def read_payment_schedule_for_account(
    account_uuid: str,
    db: Session = Depends(get_db),
    user_id: int = Depends(get_current_user_id)
):
    """
    Retrieve the monthly payment schedule for a specific loan account.
    """
    parsed_uuid = _parse_uuid(account_uuid)
    db_account = crud_account.read_db_account_by_uuid(db=db, account_uuid=parsed_uuid, user_id=user_id)
    if db_account is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Account not found")
    try:
        return crud_debt.read_schedule_for_account(db=db, user_id=user_id, account_id=db_account.id)
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
    # Resolve UUIDs
    loan_account = crud_account.read_db_account_by_uuid(db, payment_data.loan_account_uuid, user_id)
    if not loan_account:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Loan account not found")

    payment_source_account_id = None
    if payment_data.payment_source_account_uuid:
        source = crud_account.read_db_account_by_uuid(db, payment_data.payment_source_account_uuid, user_id)
        if not source:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Payment source account not found")
        payment_source_account_id = source.id

    transaction_id = None
    if payment_data.transaction_uuid:
        from src.crud.crud_transaction import read_db_transaction_by_uuid
        txn = read_db_transaction_by_uuid(db, payment_data.transaction_uuid, user_id)
        if not txn:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Transaction not found")
        transaction_id = txn.db_id

    try:
        return crud_debt.create_debt_payment(
            db=db, user_id=user_id, payment_data=payment_data,
            loan_account_id=loan_account.id,
            payment_source_account_id=payment_source_account_id,
            transaction_id=transaction_id,
        )
    except NotFoundError as e:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(e))
    except ValueError as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))

@router.post("/payments/bulk-upload", response_model=List[debt_models.DebtPaymentResponse], status_code=201)
def create_bulk_debt_payments(bulk_data: debt_models.DebtPaymentBulkCreate, db: Session = Depends(get_db), user_id: int = Depends(get_current_user_id)):
    # Resolve UUIDs for each payment
    resolved_ids = []
    for payment_data in bulk_data.payments:
        loan_account = crud_account.read_db_account_by_uuid(db, payment_data.loan_account_uuid, user_id)
        if not loan_account:
            raise HTTPException(status_code=404, detail="Loan account not found")

        ids = {'loan_account_id': loan_account.id}

        if payment_data.payment_source_account_uuid:
            source = crud_account.read_db_account_by_uuid(db, payment_data.payment_source_account_uuid, user_id)
            if not source:
                raise HTTPException(status_code=404, detail="Payment source account not found")
            ids['payment_source_account_id'] = source.id

        if payment_data.transaction_uuid:
            from src.crud.crud_transaction import read_db_transaction_by_uuid
            txn = read_db_transaction_by_uuid(db, payment_data.transaction_uuid, user_id)
            if not txn:
                raise HTTPException(status_code=404, detail="Transaction not found")
            ids['transaction_id'] = txn.db_id

        resolved_ids.append(ids)

    try:
        return crud_debt.bulk_create_debt_payments(db=db, user_id=user_id, bulk_data=bulk_data, resolved_ids=resolved_ids)
    except NotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except ValueError as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))

@router.get("/accounts/{account_uuid}/payments/", response_model=List[debt_models.DebtPaymentResponse])
def read_debt_payments_for_account(
    account_uuid: str,
    db: Session = Depends(get_db),
    user_id: int = Depends(get_current_user_id)
):
    """
    Retrieve all debt payments for a specific loan account.
    """
    parsed_uuid = _parse_uuid(account_uuid)
    db_account = crud_account.read_db_account_by_uuid(db=db, account_uuid=parsed_uuid, user_id=user_id)
    if db_account is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Account not found")
    try:
        return crud_debt.read_all_debt_payments_for_account(db=db, user_id=user_id, account_id=db_account.id)
    except NotFoundError as e:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(e))

@router.get("/payments/{payment_uuid}/", response_model=debt_models.DebtPaymentResponse)
def read_debt_payment(
    payment_uuid: str,
    db: Session = Depends(get_db),
    user_id: int = Depends(get_current_user_id)
):
    """
    Retrieve a specific debt payment by its UUID.
    """
    parsed_uuid = _parse_uuid(payment_uuid)
    db_payment = crud_debt.read_debt_payment_by_uuid(db=db, payment_uuid=parsed_uuid, user_id=user_id)
    if db_payment is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Payment not found")
    return db_payment

@router.put("/payments/{payment_uuid}/", response_model=debt_models.DebtPaymentResponse)
def update_debt_payment(
    payment_uuid: str,
    payment_data: debt_models.DebtPaymentUpdate,
    db: Session = Depends(get_db),
    user_id: int = Depends(get_current_user_id)
):
    """
    Update a debt payment.
    """
    parsed_uuid = _parse_uuid(payment_uuid)

    # Resolve optional UUID fields
    resolved_updates = {}
    update_fields = payment_data.model_dump(exclude_unset=True)

    if 'payment_source_account_uuid' in update_fields:
        if payment_data.payment_source_account_uuid:
            source = crud_account.read_db_account_by_uuid(db, payment_data.payment_source_account_uuid, user_id)
            if not source:
                raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Payment source account not found")
            resolved_updates['payment_source_account_id'] = source.id
        else:
            resolved_updates['payment_source_account_id'] = None

    if 'transaction_uuid' in update_fields:
        if payment_data.transaction_uuid:
            from src.crud.crud_transaction import read_db_transaction_by_uuid
            txn = read_db_transaction_by_uuid(db, payment_data.transaction_uuid, user_id)
            if not txn:
                raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Transaction not found")
            resolved_updates['transaction_id'] = txn.db_id
        else:
            resolved_updates['transaction_id'] = None

    updated_payment = crud_debt.update_debt_payment_by_uuid(
        db=db, payment_uuid=parsed_uuid, user_id=user_id, payment_data=payment_data,
        resolved_updates=resolved_updates if resolved_updates else None
    )
    if updated_payment is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Payment not found")
    return updated_payment

@router.delete("/payments/{payment_uuid}/", status_code=status.HTTP_204_NO_CONTENT)
def delete_debt_payment(
    payment_uuid: str,
    db: Session = Depends(get_db),
    user_id: int = Depends(get_current_user_id)
):
    """
    Delete a debt payment.
    """
    parsed_uuid = _parse_uuid(payment_uuid)
    if not crud_debt.delete_debt_payment_by_uuid(db=db, payment_uuid=parsed_uuid, user_id=user_id):
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Payment not found")
