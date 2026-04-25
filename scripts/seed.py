import sys
import os
import random
from sqlalchemy.orm import Session
from datetime import date, datetime, timedelta
from decimal import Decimal
from uuid import uuid4
from faker import Faker

# Add the project root to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.db.core import (
    session_local,
    UserDB,
    AccountDB,
    TransactionDB,
    TagDB,
    TransactionTagDB,
    BudgetTemplateDB,
    BudgetTemplateCategoryDB,
    BudgetMonthDB,
    DebtRepaymentPlanDB,
    DebtPlanAccountLinkDB,
    DebtPaymentDB,
    InvestmentTransactionDB,
    DebtRepaymentScheduleDB,
    TransactionRelationshipDB,
    CategoryDB,
    FinancialPlanDB,
    FinancialPlanMonthDB,
    FinancialPlanExpenseDB,
    AccountType,
    TransactionType,
    DebtStrategy,
    InvestmentTransactionType,
    SourceType,
    RelationshipType,
)
from src.crud.crud_user import hash_password
from src.crud.crud_investment import rebuild_holdings_from_transactions

fake = Faker()

# Bootstrap admin user — predictable creds for dev login.
ADMIN_EMAIL = "dev@pocketwatcher.local"
ADMIN_USERNAME = "dev"
ADMIN_PASSWORD = "Password123!"

DEFAULT_PASSWORD = "Password123!"
NUM_USERS = 10  # 1 admin + 9 fake users


def _money(low: float, high: float) -> Decimal:
    """Helper: round Decimal to 2 places to avoid float-precision noise."""
    return Decimal(str(round(random.uniform(low, high), 2)))


def seed_database():
    """Fills the database with sample data for development."""
    db: Session = session_local()

    try:
        if db.query(UserDB).count() > 0:
            print("Database appears to be already seeded. Exiting.")
            return

        print("Seeding database...")

        # 1. Categories come from the predefined-category Alembic migration
        # (src/constants/categories.py). Seed reads them — never creates them.
        categories_map: dict[int, list[int]] = {}
        parents = db.query(CategoryDB).filter(CategoryDB.parent_category_id.is_(None)).all()
        if not parents:
            raise RuntimeError(
                "No categories found — run `alembic upgrade head` before seeding. "
                "See src/constants/categories.py."
            )
        for parent in parents:
            children = db.query(CategoryDB).filter(CategoryDB.parent_category_id == parent.id).all()
            categories_map[parent.id] = [c.id for c in children]
        print(f"Loaded {len(categories_map)} parent categories from migration.")

        # 2. Users
        for i in range(NUM_USERS):
            is_admin_user = i == 0
            print(f"--- Seeding user {i+1}/{NUM_USERS} {'(ADMIN)' if is_admin_user else ''} ---")

            if is_admin_user:
                user = UserDB(
                    id=uuid4(),
                    email=ADMIN_EMAIL,
                    username=ADMIN_USERNAME,
                    password_hash=hash_password(ADMIN_PASSWORD),
                    first_name="Dev",
                    last_name="Admin",
                    is_admin=True,
                )
            else:
                user = UserDB(
                    id=uuid4(),
                    email=fake.unique.email(),
                    username=fake.unique.user_name(),
                    password_hash=hash_password(DEFAULT_PASSWORD),
                    first_name=fake.first_name(),
                    last_name=fake.last_name(),
                    date_of_birth=fake.date_of_birth(minimum_age=18, maximum_age=70),
                    is_admin=False,
                )
            db.add(user)
            db.flush()

            # 3. Accounts
            account_specs = [
                (AccountType.CHECKING, "Main Checking", _money(5000, 10000)),
                (AccountType.SAVINGS, "Emergency Fund", _money(20000, 30000)),
                (AccountType.CREDIT_CARD, "Rewards Card", _money(-2000, -500)),
                (AccountType.LOAN, "Car Loan", _money(-15000, -8000)),
                (AccountType.INVESTMENT, "Brokerage", _money(40000, 60000)),
            ]

            user_accounts = []
            for acc_type, acc_name, balance in account_specs:
                account = AccountDB(
                    uuid=uuid4(),
                    user_id=user.db_id,
                    account_name=f"{acc_name}",
                    account_type=acc_type,
                    institution_name=fake.company(),
                    balance=balance,
                    interest_rate=Decimal("0.0525") if acc_type == AccountType.LOAN else None,
                    minimum_payment=_money(200, 400) if acc_type == AccountType.LOAN else None,
                    original_principal=_money(15000, 25000) if acc_type == AccountType.LOAN else None,
                )
                db.add(account)
                user_accounts.append(account)
            db.flush()

            loan_accounts = [a for a in user_accounts if a.account_type == AccountType.LOAN]
            investment_accounts = [a for a in user_accounts if a.account_type == AccountType.INVESTMENT]
            checking_account = next((a for a in user_accounts if a.account_type == AccountType.CHECKING), None)

            # 4. Regular transactions
            all_transactions = []
            purchases = []
            for account in user_accounts:
                if account.account_type in (AccountType.LOAN, AccountType.INVESTMENT):
                    continue

                for _ in range(random.randint(40, 80)):
                    trans_date = fake.date_between(start_date="-2y", end_date="today")
                    parent_cat_id = random.choice(list(categories_map.keys()))
                    sub_cat_id = random.choice(categories_map[parent_cat_id]) if categories_map[parent_cat_id] else None
                    is_purchase = random.choice([True, False])
                    amount = _money(5, 500)

                    transaction = TransactionDB(
                        id=uuid4(),
                        user_id=user.db_id,
                        account_id=account.id,
                        transaction_date=trans_date,
                        amount=amount,
                        transaction_type=TransactionType.PURCHASE if is_purchase else TransactionType.CREDIT,
                        category_id=parent_cat_id,
                        subcategory_id=sub_cat_id,
                        description=fake.catch_phrase(),
                        merchant_name=fake.company(),
                        transaction_hash=fake.sha256(raw_output=False),
                        source_type=SourceType.MANUAL,
                    )
                    db.add(transaction)
                    all_transactions.append(transaction)
                    if is_purchase:
                        purchases.append(transaction)
            db.flush()

            # 5. Tags + transaction tags
            user_tags = []
            for _ in range(random.randint(3, 8)):
                tag = TagDB(
                    id=uuid4(),
                    user_id=user.db_id,
                    tag_name=fake.unique.word() + str(random.randint(0, 999)),
                    color=fake.hex_color(),
                )
                db.add(tag)
                user_tags.append(tag)
            db.flush()

            if all_transactions and user_tags:
                for transaction in random.sample(all_transactions, min(len(all_transactions), 25)):
                    tag = random.choice(user_tags)
                    db.add(TransactionTagDB(transaction_id=transaction.db_id, tag_id=tag.tag_id))

            # 6. Investment transactions (holdings rebuilt at end)
            if investment_accounts:
                inv_acc = investment_accounts[0]
                seeds = [
                    {"symbol": "AAPL", "qty": Decimal("50"), "price": Decimal("175.00")},
                    {"symbol": "GOOGL", "qty": Decimal("20"), "price": Decimal("140.00")},
                    {"symbol": "VTSAX", "qty": Decimal("100"), "price": Decimal("105.50")},
                ]
                for s in seeds:
                    db.add(InvestmentTransactionDB(
                        id=uuid4(),
                        user_id=user.db_id,
                        account_id=inv_acc.id,
                        transaction_type=InvestmentTransactionType.BUY,
                        symbol=s["symbol"],
                        quantity=s["qty"],
                        price_per_share=s["price"],
                        total_amount=s["qty"] * s["price"],
                        transaction_date=fake.date_between(start_date="-2y", end_date="-1y"),
                        transaction_hash=str(uuid4()),
                    ))
                    # A few follow-up buys
                    for _ in range(random.randint(2, 4)):
                        qty = _money(1, 5)
                        price = s["price"] * Decimal(str(round(random.uniform(0.95, 1.05), 4)))
                        db.add(InvestmentTransactionDB(
                            id=uuid4(),
                            user_id=user.db_id,
                            account_id=inv_acc.id,
                            transaction_type=InvestmentTransactionType.BUY,
                            symbol=s["symbol"],
                            quantity=qty,
                            price_per_share=price,
                            total_amount=qty * price,
                            transaction_date=fake.date_between(start_date="-1y", end_date="today"),
                            transaction_hash=str(uuid4()),
                        ))
                    # Dividend
                    db.add(InvestmentTransactionDB(
                        id=uuid4(),
                        user_id=user.db_id,
                        account_id=inv_acc.id,
                        transaction_type=InvestmentTransactionType.DIVIDEND,
                        symbol=s["symbol"],
                        total_amount=_money(20, 200),
                        transaction_date=fake.date_between(start_date="-1y", end_date="today"),
                        transaction_hash=str(uuid4()),
                    ))
                db.flush()
                rebuild_holdings_from_transactions(db, inv_acc.id)

            # 7. Debt repayment plan + payments
            if loan_accounts:
                debt_plan = DebtRepaymentPlanDB(
                    id=uuid4(),
                    user_id=user.db_id,
                    plan_name=f"Debt Annihilator {i}",
                    strategy=random.choice(list(DebtStrategy)),
                    status="ACTIVE",
                )
                db.add(debt_plan)
                db.flush()

                for priority, loan_acc in enumerate(loan_accounts):
                    db.add(DebtPlanAccountLinkDB(
                        plan_id=debt_plan.plan_id,
                        account_id=loan_acc.id,
                        priority=priority + 1,
                    ))

                if checking_account:
                    for loan_acc in loan_accounts:
                        for _ in range(random.randint(6, 18)):
                            payment_amount = _money(250, 600)
                            interest_amount = payment_amount * Decimal("0.25")
                            principal_amount = payment_amount - interest_amount
                            db.add(DebtPaymentDB(
                                id=uuid4(),
                                loan_account_id=loan_acc.id,
                                payment_source_account_id=checking_account.id,
                                payment_amount=payment_amount,
                                principal_amount=principal_amount,
                                interest_amount=interest_amount,
                                payment_date=fake.date_between(start_date="-18m", end_date="today"),
                            ))

            # 8. Refund relationships
            if len(purchases) > 5:
                for _ in range(5):
                    purchase = random.choice(purchases)
                    refund = TransactionDB(
                        id=uuid4(),
                        user_id=user.db_id,
                        account_id=purchase.account_id,
                        transaction_date=purchase.transaction_date + timedelta(days=random.randint(2, 10)),
                        amount=abs(purchase.amount) * Decimal("0.5"),
                        transaction_type=TransactionType.CREDIT,
                        category_id=purchase.category_id,
                        description=f"Refund for: {purchase.description}",
                        merchant_name=purchase.merchant_name,
                        transaction_hash=fake.sha256(raw_output=False),
                        source_type=SourceType.MANUAL,
                    )
                    db.add(refund)
                    db.flush()
                    db.add(TransactionRelationshipDB(
                        id=uuid4(),
                        from_transaction_id=refund.db_id,
                        to_transaction_id=purchase.db_id,
                        relationship_type=RelationshipType.REFUNDS,
                        amount_allocated=refund.amount,
                    ))

            # 9. Debt repayment schedules
            for loan_acc in loan_accounts:
                for j in range(12):
                    payment_month = (date.today().replace(day=1) + timedelta(days=31 * j)).replace(day=1)
                    db.add(DebtRepaymentScheduleDB(
                        id=uuid4(),
                        user_id=user.db_id,
                        account_id=loan_acc.id,
                        payment_month=payment_month,
                        scheduled_payment_amount=_money(200, 500),
                    ))

            # 10. Budget template + month assignment
            today = date.today()
            template = BudgetTemplateDB(
                id=uuid4(),
                user_id=user.db_id,
                template_name=f"Standard Budget {i}",
                is_default=True,
            )
            db.add(template)
            db.flush()

            chosen_parents = random.sample(list(categories_map.keys()), k=min(5, len(categories_map)))
            for cat_id in chosen_parents:
                db.add(BudgetTemplateCategoryDB(
                    id=uuid4(),
                    template_id=template.template_id,
                    category_id=cat_id,
                    allocated_amount=_money(200, 1500),
                ))

            db.add(BudgetMonthDB(
                id=uuid4(),
                user_id=user.db_id,
                template_id=template.template_id,
                year=today.year,
                month=today.month,
            ))

            # 11. Financial plan with months + expenses
            financial_plan = FinancialPlanDB(
                id=uuid4(),
                user_id=user.db_id,
                plan_name=f"What-if Scenario {i}",
                start_date=date(today.year, 1, 1),
                end_date=date(today.year, 12, 31),
            )
            db.add(financial_plan)
            db.flush()

            for month_num in range(1, 4):  # Jan, Feb, Mar planning months
                fp_month = FinancialPlanMonthDB(
                    id=uuid4(),
                    plan_id=financial_plan.plan_id,
                    year=today.year,
                    month=month_num,
                    planned_income=_money(6000, 10000),
                )
                db.add(fp_month)
                db.flush()

                for cat_id in random.sample(list(categories_map.keys()), k=random.randint(3, 6)):
                    db.add(FinancialPlanExpenseDB(
                        id=uuid4(),
                        month_id=fp_month.month_id,
                        category_id=cat_id,
                        description=fake.bs().title(),
                        amount=_money(100, 800),
                        expense_type=random.choice(["recurring", "one_time"]),
                    ))

            db.commit()
            print(f"  done.")

        print("\nSeed complete.")
        print(f"  Admin login: {ADMIN_EMAIL} / {ADMIN_PASSWORD}")
        print(f"  Other users: <fake-email> / {DEFAULT_PASSWORD}")

    except Exception as e:
        print(f"Seed failed: {e}")
        import traceback
        traceback.print_exc()
        db.rollback()
    finally:
        db.close()


if __name__ == "__main__":
    seed_database()
