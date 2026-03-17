from sqlalchemy.orm import Session, joinedload
from typing import Optional, List, Dict
from uuid import UUID, uuid4

from src.db.core import (
    AccountDB,
    AccountType,
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
    DebtPaymentUpdate,
    DebtPaymentBulkCreate,
    DebtRepaymentPlanUpdate
)

# ===== DATABASE OPERATIONS - PLANS =====

def create_debt_repayment_plan(db: Session, user_id: int, plan_data: DebtRepaymentPlanCreate) -> DebtRepaymentPlanDB:
    db_plan = DebtRepaymentPlanDB(
        id=uuid4(),
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

def add_account_to_plan(db: Session, user_id: int, link_data: DebtPlanAccountLinkCreate, *, plan_id: int, account_id: int) -> DebtPlanAccountLinkDB:
    plan = read_debt_repayment_plan(db, plan_id, user_id)
    if not plan:
        raise NotFoundError("Repayment plan not found.")

    account = db.query(AccountDB).filter(AccountDB.id == account_id, AccountDB.user_id == user_id).first()
    if not account:
        raise NotFoundError("Account not found.")

    db_link = DebtPlanAccountLinkDB(
        plan_id=plan_id,
        account_id=account_id,
        priority=link_data.priority,
    )
    db.add(db_link)
    db.commit()
    db.refresh(db_link)
    return db_link

def read_accounts_for_plan(db: Session, plan_id: int, user_id: int) -> List[AccountDB]:
    """Return all accounts linked to a plan."""
    links = db.query(DebtPlanAccountLinkDB).filter(
        DebtPlanAccountLinkDB.plan_id == plan_id
    ).order_by(DebtPlanAccountLinkDB.priority).all()
    account_ids = [link.account_id for link in links]
    if not account_ids:
        return []
    return db.query(AccountDB).filter(
        AccountDB.id.in_(account_ids),
        AccountDB.user_id == user_id
    ).all()

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

def bulk_create_or_update_schedule(db: Session, user_id: int, schedule_data: DebtRepaymentScheduleBulkCreate, *, account_id: int):
    account = db.query(AccountDB).filter(AccountDB.id == account_id, AccountDB.user_id == user_id).first()
    if not account:
        raise NotFoundError("Account not found.")

    # Delete existing schedule for this account to replace it
    db.query(DebtRepaymentScheduleDB).filter(
        DebtRepaymentScheduleDB.account_id == account_id,
        DebtRepaymentScheduleDB.user_id == user_id
    ).delete()

    new_schedules = []
    for schedule in schedule_data.schedules:
        new_schedules.append(
            DebtRepaymentScheduleDB(
                id=uuid4(),
                user_id=user_id,
                account_id=account_id,
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

    return db.query(DebtRepaymentScheduleDB).options(
        joinedload(DebtRepaymentScheduleDB.account),
    ).filter(
        DebtRepaymentScheduleDB.account_id == account_id
    ).order_by(DebtRepaymentScheduleDB.payment_month).all()

# ===== DATABASE OPERATIONS - PAYMENTS =====

def create_debt_payment(db: Session, user_id: int, payment_data: DebtPaymentCreate, *, loan_account_id: int, payment_source_account_id: Optional[int] = None, transaction_id: Optional[int] = None) -> DebtPaymentDB:
    # Verify that the loan account exists and belongs to the user
    loan_account = db.query(AccountDB).filter(
        AccountDB.id == loan_account_id,
        AccountDB.user_id == user_id
    ).first()
    if not loan_account:
        raise NotFoundError("Loan account not found.")

    # Validate that the account is a LOAN or CREDIT_CARD account
    if loan_account.account_type not in [AccountType.LOAN, AccountType.CREDIT_CARD]:
        raise ValueError(f"Account must be of type LOAN or CREDIT_CARD. Found: {loan_account.account_type.value}")

    # Optionally, verify the source account if provided
    if payment_source_account_id:
        source_account = db.query(AccountDB).filter(
            AccountDB.id == payment_source_account_id,
            AccountDB.user_id == user_id
        ).first()
        if not source_account:
            raise NotFoundError("Payment source account not found.")

    # Calculate principal and interest amounts if not provided
    principal_amount = payment_data.principal_amount
    interest_amount = payment_data.interest_amount

    if principal_amount is None or interest_amount is None:
        # If interest rate is available, calculate the interest portion
        if loan_account.interest_rate is not None and loan_account.balance is not None:
            # Calculate monthly interest: (balance * annual_rate) / 12
            calculated_interest = (loan_account.balance * loan_account.interest_rate) / 12
            calculated_interest = calculated_interest.quantize(payment_data.payment_amount.as_tuple().exponent)

            if interest_amount is None:
                interest_amount = calculated_interest

            if principal_amount is None:
                principal_amount = payment_data.payment_amount - interest_amount
        else:
            # If we can't calculate interest, assume entire payment is principal
            if principal_amount is None:
                principal_amount = payment_data.payment_amount
            if interest_amount is None:
                interest_amount = payment_data.payment_amount - (principal_amount or 0)

    # Calculate remaining balance after payment
    remaining_balance = payment_data.remaining_balance_after_payment
    if remaining_balance is None and loan_account.balance is not None:
        # For LOAN accounts, reduce the balance by the principal amount
        # For CREDIT_CARD accounts, reduce the balance by the payment amount
        if loan_account.account_type == AccountType.LOAN:
            remaining_balance = loan_account.balance - principal_amount
        else:  # CREDIT_CARD
            remaining_balance = loan_account.balance - payment_data.payment_amount

    # Create the payment record
    db_payment = DebtPaymentDB(
        id=uuid4(),
        loan_account_id=loan_account_id,
        payment_source_account_id=payment_source_account_id,
        transaction_id=transaction_id,
        payment_amount=payment_data.payment_amount,
        principal_amount=principal_amount,
        interest_amount=interest_amount,
        remaining_balance_after_payment=remaining_balance,
        payment_date=payment_data.payment_date,
        description=payment_data.description
    )
    db.add(db_payment)

    # Update the loan account balance
    if remaining_balance is not None:
        loan_account.balance = remaining_balance
        loan_account.balance_last_updated = db_payment.created_at

    db.commit()
    db.refresh(db_payment)
    return db_payment

def bulk_create_debt_payments(db: Session, user_id: int, bulk_data: DebtPaymentBulkCreate, *, resolved_ids: List[Dict]) -> List[DebtPaymentDB]:
    db_payments = []
    for i, payment_data in enumerate(bulk_data.payments):
        ids = resolved_ids[i]
        db_payments.append(create_debt_payment(
            db, user_id, payment_data,
            loan_account_id=ids['loan_account_id'],
            payment_source_account_id=ids.get('payment_source_account_id'),
            transaction_id=ids.get('transaction_id'),
        ))
    return db_payments

def read_debt_payment(db: Session, payment_id: int, user_id: int) -> Optional[DebtPaymentDB]:
    return db.query(DebtPaymentDB).join(AccountDB, DebtPaymentDB.loan_account_id == AccountDB.id).options(
        joinedload(DebtPaymentDB.loan_account),
        joinedload(DebtPaymentDB.payment_source_account),
        joinedload(DebtPaymentDB.transaction),
    ).filter(
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
    
    return db.query(DebtPaymentDB).options(
        joinedload(DebtPaymentDB.loan_account),
        joinedload(DebtPaymentDB.payment_source_account),
        joinedload(DebtPaymentDB.transaction),
    ).filter(DebtPaymentDB.loan_account_id == account_id).all()

def update_debt_payment(db: Session, payment_id: int, user_id: int, payment_data: DebtPaymentUpdate, *, resolved_updates: Optional[Dict] = None) -> Optional[DebtPaymentDB]:
    from datetime import datetime
    from decimal import Decimal

    db_payment = read_debt_payment(db, payment_id, user_id)
    if not db_payment:
        return None

    loan_account = db_payment.loan_account

    # 1. Reverse the original balance effect
    if loan_account and loan_account.balance is not None:
        if loan_account.account_type == AccountType.LOAN:
            loan_account.balance += (db_payment.principal_amount or Decimal('0'))
        else:  # CREDIT_CARD
            loan_account.balance += (db_payment.payment_amount or Decimal('0'))

    # 2. Build update dict from payload
    update_data = payment_data.model_dump(exclude_unset=True)
    uuid_fields = ['payment_source_account_uuid', 'transaction_uuid']
    for f in uuid_fields:
        update_data.pop(f, None)
    if resolved_updates:
        update_data.update(resolved_updates)

    # 3. Determine new payment_amount
    new_payment_amount = update_data.get('payment_amount', db_payment.payment_amount)

    # 4. Recalculate principal/interest if not explicitly provided
    new_principal = update_data.get('principal_amount')
    new_interest = update_data.get('interest_amount')

    if new_principal is None or new_interest is None:
        if loan_account and loan_account.interest_rate is not None and loan_account.balance is not None:
            calculated_interest = (loan_account.balance * loan_account.interest_rate) / 12
            calculated_interest = calculated_interest.quantize(new_payment_amount.as_tuple().exponent)
            if new_interest is None:
                new_interest = calculated_interest
            if new_principal is None:
                new_principal = new_payment_amount - new_interest
        else:
            if new_principal is None:
                new_principal = new_payment_amount
            if new_interest is None:
                new_interest = new_payment_amount - (new_principal or Decimal('0'))

    update_data['principal_amount'] = new_principal
    update_data['interest_amount'] = new_interest

    # 5. Compute remaining balance and apply to account
    remaining_balance = update_data.get('remaining_balance_after_payment')
    if remaining_balance is None and loan_account and loan_account.balance is not None:
        if loan_account.account_type == AccountType.LOAN:
            remaining_balance = loan_account.balance - new_principal
        else:  # CREDIT_CARD
            remaining_balance = loan_account.balance - new_payment_amount
    update_data['remaining_balance_after_payment'] = remaining_balance

    if loan_account and remaining_balance is not None:
        loan_account.balance = remaining_balance
        loan_account.balance_last_updated = datetime.utcnow()

    # 6. Apply all fields to the payment record
    for key, value in update_data.items():
        setattr(db_payment, key, value)

    db.commit()
    db.refresh(db_payment)
    return db_payment

def delete_debt_payment(db: Session, payment_id: int, user_id: int) -> bool:
    from datetime import datetime
    from decimal import Decimal

    db_payment = read_debt_payment(db, payment_id, user_id)
    if not db_payment:
        return False

    # Reverse balance effect on the loan account
    loan_account = db_payment.loan_account
    if loan_account and loan_account.balance is not None:
        if loan_account.account_type == AccountType.LOAN:
            loan_account.balance += (db_payment.principal_amount or Decimal('0'))
        else:  # CREDIT_CARD
            loan_account.balance += (db_payment.payment_amount or Decimal('0'))
        loan_account.balance_last_updated = datetime.utcnow()

    db.delete(db_payment)
    db.commit()
    return True


# ===== UUID-BASED OPERATIONS - PLANS =====

def read_debt_repayment_plan_by_uuid(db: Session, plan_uuid: UUID, user_id: int) -> Optional[DebtRepaymentPlanDB]:
    return db.query(DebtRepaymentPlanDB).filter(
        DebtRepaymentPlanDB.id == plan_uuid,
        DebtRepaymentPlanDB.user_id == user_id
    ).first()

def update_debt_repayment_plan_by_uuid(db: Session, plan_uuid: UUID, user_id: int, plan_data: DebtRepaymentPlanUpdate) -> Optional[DebtRepaymentPlanDB]:
    db_plan = read_debt_repayment_plan_by_uuid(db, plan_uuid, user_id)
    if not db_plan:
        return None

    update_dict = plan_data.model_dump(exclude_unset=True)
    if "strategy" in update_dict and update_dict["strategy"] is not None:
        update_dict["strategy"] = DebtStrategy(update_dict["strategy"].value)

    for key, value in update_dict.items():
        setattr(db_plan, key, value)

    db.commit()
    db.refresh(db_plan)
    return db_plan

def delete_debt_repayment_plan_by_uuid(db: Session, plan_uuid: UUID, user_id: int) -> bool:
    db_plan = read_debt_repayment_plan_by_uuid(db, plan_uuid, user_id)
    if not db_plan:
        return False

    db.delete(db_plan)
    db.commit()
    return True


# ===== UUID-BASED OPERATIONS - PAYMENTS =====

def read_debt_payment_by_uuid(db: Session, payment_uuid: UUID, user_id: int) -> Optional[DebtPaymentDB]:
    return db.query(DebtPaymentDB).join(AccountDB, DebtPaymentDB.loan_account_id == AccountDB.id).options(
        joinedload(DebtPaymentDB.loan_account),
        joinedload(DebtPaymentDB.payment_source_account),
        joinedload(DebtPaymentDB.transaction),
    ).filter(
        DebtPaymentDB.id == payment_uuid,
        AccountDB.user_id == user_id
    ).first()

def update_debt_payment_by_uuid(db: Session, payment_uuid: UUID, user_id: int, payment_data: DebtPaymentUpdate, *, resolved_updates: Optional[Dict] = None) -> Optional[DebtPaymentDB]:
    db_payment = read_debt_payment_by_uuid(db, payment_uuid, user_id)
    if not db_payment:
        return None
    return update_debt_payment(db, db_payment.payment_id, user_id, payment_data, resolved_updates=resolved_updates)

def delete_debt_payment_by_uuid(db: Session, payment_uuid: UUID, user_id: int) -> bool:
    db_payment = read_debt_payment_by_uuid(db, payment_uuid, user_id)
    if not db_payment:
        return False
    return delete_debt_payment(db, db_payment.payment_id, user_id)
