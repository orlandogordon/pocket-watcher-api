from sqlalchemy.orm import Session
from typing import Optional, List

from src.db.core import (
    AccountDB,
    UserDB,
    DebtRepaymentPlanDB,
    DebtPlanAccountLinkDB,
    DebtRepaymentScheduleDB,
    DebtPaymentDB,
    DebtStrategy,
    NotFoundError
)
from src.models.debt import (
    DebtRepaymentPlanCreate,
    DebtPlanAccountLinkCreate,
    DebtRepaymentScheduleBulkCreate,
    DebtPaymentCreate,
    DebtPaymentUpdate
)

# ===== DATABASE OPERATIONS - PLANS =====

def create_debt_repayment_plan(db: Session, user_id: int, plan_data: DebtRepaymentPlanCreate) -> DebtRepaymentPlanDB:
    db_plan = DebtRepaymentPlanDB(
        **plan_data.model_dump(exclude={"strategy"}), 
        strategy=DebtStrategy(plan_data.strategy.value),
        user_id=user_id
    )
    db.add(db_plan)
    db.commit()
    db.refresh(db_plan)
    return db_plan

def read_debt_repayment_plan(db: Session, plan_id: int, user_id: int) -> Optional[DebtRepaymentPlanDB]:
    return db.query(DebtRepaymentPlanDB).filter(
        DebtRepaymentPlanDB.plan_id == plan_id, 
        DebtRepaymentPlanDB.user_id == user_id
    ).first()

def read_all_debt_repayment_plans_for_user(db: Session, user_id: int) -> List[DebtRepaymentPlanDB]:
    return db.query(DebtRepaymentPlanDB).filter(DebtRepaymentPlanDB.user_id == user_id).all()

def add_account_to_plan(db: Session, user_id: int, link_data: DebtPlanAccountLinkCreate) -> DebtPlanAccountLinkDB:
    plan = read_debt_repayment_plan(db, link_data.plan_id, user_id)
    if not plan:
        raise NotFoundError("Repayment plan not found.")

    account = db.query(AccountDB).filter(AccountDB.id == link_data.account_id, AccountDB.user_id == user_id).first()
    if not account:
        raise NotFoundError("Account not found.")

    db_link = DebtPlanAccountLinkDB(**link_data.model_dump())
    db.add(db_link)
    db.commit()
    db.refresh(db_link)
    return db_link

def remove_account_from_plan(db: Session, user_id: int, plan_id: int, account_id: int) -> bool:
    link = db.query(DebtPlanAccountLinkDB).join(DebtRepaymentPlanDB).filter(
        DebtRepaymentPlanDB.user_id == user_id,
        DebtPlanAccountLinkDB.plan_id == plan_id,
        DebtPlanAccountLinkDB.account_id == account_id
    ).first()

    if not link:
        raise NotFoundError("Account link to plan not found.")

    db.delete(link)
    db.commit()
    return True

# ===== DATABASE OPERATIONS - SCHEDULES =====

def bulk_create_or_update_schedule(db: Session, user_id: int, schedule_data: DebtRepaymentScheduleBulkCreate):
    account = db.query(AccountDB).filter(AccountDB.id == schedule_data.account_id, AccountDB.user_id == user_id).first()
    if not account:
        raise NotFoundError("Account not found.")

    # Delete existing schedule for this account to replace it
    db.query(DebtRepaymentScheduleDB).filter(
        DebtRepaymentScheduleDB.account_id == schedule_data.account_id,
        DebtRepaymentScheduleDB.user_id == user_id
    ).delete()

    new_schedules = []
    for schedule in schedule_data.schedules:
        new_schedules.append(
            DebtRepaymentScheduleDB(
                user_id=user_id,
                account_id=schedule_data.account_id,
                payment_month=schedule.payment_month,
                scheduled_payment_amount=schedule.scheduled_payment_amount
            )
        )
    
    db.bulk_save_objects(new_schedules)
    db.commit()
    return len(new_schedules)

def read_schedule_for_account(db: Session, user_id: int, account_id: int) -> List[DebtRepaymentScheduleDB]:
    account = db.query(AccountDB).filter(AccountDB.id == account_id, AccountDB.user_id == user_id).first()
    if not account:
        raise NotFoundError("Account not found.")

    return db.query(DebtRepaymentScheduleDB).filter(
        DebtRepaymentScheduleDB.account_id == account_id
    ).order_by(DebtRepaymentScheduleDB.payment_month).all()

# ===== DATABASE OPERATIONS - PAYMENTS =====

def create_debt_payment(db: Session, user_id: int, payment_data: DebtPaymentCreate) -> DebtPaymentDB:
    # Verify that the loan account exists and belongs to the user
    loan_account = db.query(AccountDB).filter(
        AccountDB.id == payment_data.loan_account_id,
        AccountDB.user_id == user_id
    ).first()
    if not loan_account:
        raise NotFoundError("Loan account not found.")
    
    # Optionally, verify the source account if provided
    if payment_data.payment_source_account_id:
        source_account = db.query(AccountDB).filter(
            AccountDB.id == payment_data.payment_source_account_id,
            AccountDB.user_id == user_id
        ).first()
        if not source_account:
            raise NotFoundError("Payment source account not found.")

    db_payment = DebtPaymentDB(**payment_data.model_dump())
    db.add(db_payment)
    db.commit()
    db.refresh(db_payment)
    return db_payment

def read_debt_payment(db: Session, payment_id: int, user_id: int) -> Optional[DebtPaymentDB]:
    return db.query(DebtPaymentDB).join(AccountDB, DebtPaymentDB.loan_account_id == AccountDB.id).filter(
        DebtPaymentDB.payment_id == payment_id,
        AccountDB.user_id == user_id
    ).first()

def read_all_debt_payments_for_account(db: Session, account_id: int, user_id: int) -> List[DebtPaymentDB]:
    # Verify account ownership first
    account = db.query(AccountDB).filter(
        AccountDB.id == account_id,
        AccountDB.user_id == user_id
    ).first()
    if not account:
        raise NotFoundError("Account not found.")
    
    return db.query(DebtPaymentDB).filter(DebtPaymentDB.loan_account_id == account_id).all()

def update_debt_payment(db: Session, payment_id: int, user_id: int, payment_data: DebtPaymentUpdate) -> Optional[DebtPaymentDB]:
    db_payment = read_debt_payment(db, payment_id, user_id)
    if not db_payment:
        return None

    for key, value in payment_data.model_dump(exclude_unset=True).items():
        setattr(db_payment, key, value)
    
    db.commit()
    db.refresh(db_payment)
    return db_payment

def delete_debt_payment(db: Session, payment_id: int, user_id: int) -> bool:
    db_payment = read_debt_payment(db, payment_id, user_id)
    if not db_payment:
        return False
    
    db.delete(db_payment)
    db.commit()
    return True
