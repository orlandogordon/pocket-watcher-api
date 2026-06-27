#!/usr/bin/env python
"""Wipe-and-reseed the public demo's shared account (#82).

The portfolio demo (the ``DEMO_MODE`` droplet) is a single shared, anonymous,
non-admin user whose data resets daily so every visitor lands on a clean,
fully-categorized dataset. This job is the reset engine:

- **Idempotent wipe-and-seed.** Deletes the existing demo user — ORM
  ``cascade="all, delete-orphan"`` on ``UserDB`` removes every owned account,
  transaction, tag, budget, debt, investment, etc. — then recreates it with a
  realistic, already-categorized dataset. Safe to run on first boot (the DB is
  never left empty) and on every daily tick.
- **Resets IN PLACE** against the running DB — never ``docker compose down/up``
  — so there is no downtime window if a visitor hits the app mid-reset.
- **Leaves every OTHER user untouched** (e.g. the bootstrapped admin): it only
  ever deletes the one row matching ``DEMO_USER_EMAIL``.

    DEMO_USER_EMAIL=demo@example.com DEMO_USER_PASSWORD='Password123!' \\
        python -m src.jobs.demo_reset

Env:
    DEMO_USER_EMAIL     (default demo@pocketwatcher.local)
    DEMO_USER_USERNAME  (default demo)
    DEMO_USER_PASSWORD  (default Password123!) — must satisfy the password policy
                        (min 8, upper + lower + digit); the FE auto-login uses it.
"""
from __future__ import annotations

import os
import random
from datetime import date
from decimal import Decimal
from uuid import uuid4

from faker import Faker

from src.crud.crud_investment import rebuild_holdings_from_transactions
from src.crud.crud_user import create_db_user, read_db_user
from src.db.core import (
    AccountDB,
    AccountType,
    BudgetMonthDB,
    BudgetTemplateCategoryDB,
    BudgetTemplateDB,
    CategoryDB,
    DebtPaymentDB,
    DebtPlanAccountLinkDB,
    DebtRepaymentPlanDB,
    DebtStrategy,
    InvestmentTransactionDB,
    InvestmentTransactionType,
    session_local,
    TagDB,
    TransactionDB,
    TransactionTagDB,
    TransactionType,
    SourceType,
)
from src.logging_config import get_logger, setup_logging
from src.models.user import UserCreate

logger = get_logger(__name__)
fake = Faker()

# Realistic merchant names so the seeded dashboards read like real spending
# (recruiters see "Whole Foods", not a Faker catch-phrase). Categories are
# assigned from the live set below — pairing isn't guaranteed coherent, which is
# fine: the LLM categorization showcase happens on the upload path, the seed just
# needs every page to look populated and already-categorized.
_MERCHANTS = [
    "Whole Foods Market", "Trader Joe's", "Costco Wholesale", "Target",
    "Starbucks", "Chipotle", "Shake Shack", "Sweetgreen", "DoorDash",
    "Amazon", "Apple", "Netflix", "Spotify", "Uber", "Lyft", "Shell",
    "Exxon", "Delta Air Lines", "Marriott", "CVS Pharmacy", "Walgreens",
    "Home Depot", "IKEA", "Nike", "REI", "Verizon", "Comcast Xfinity",
    "PSE&G", "Planet Fitness", "AMC Theatres",
]

_INVESTMENTS = [
    ("AAPL", Decimal("185.00")),
    ("VOO", Decimal("465.00")),
    ("MSFT", Decimal("420.00")),
    ("VTI", Decimal("255.00")),
]


def _money(low: float, high: float) -> Decimal:
    return Decimal(str(round(random.uniform(low, high), 2)))


def _load_categories(db) -> dict[int, list[int]]:
    """Map each parent category db_id -> its subcategory db_ids.

    Categories come from the predefined-category Alembic migration; this job
    reads them and never creates them.
    """
    parents = db.query(CategoryDB).filter(CategoryDB.parent_category_id.is_(None)).all()
    if not parents:
        raise RuntimeError(
            "No categories found — run `alembic upgrade head` before seeding "
            "(the category-seed migration, src/constants/categories.py)."
        )
    return {
        p.db_id: [
            c.db_id
            for c in db.query(CategoryDB).filter(CategoryDB.parent_category_id == p.db_id)
        ]
        for p in parents
    }


def _seed_user(db, user) -> None:
    categories = _load_categories(db)
    parent_ids = list(categories.keys())

    # 1. Accounts — one of each type so every account view is populated.
    specs = [
        (AccountType.CHECKING, "Everyday Checking", _money(4000, 9000), {}),
        (AccountType.SAVINGS, "Emergency Fund", _money(15000, 30000), {}),
        (AccountType.CREDIT_CARD, "Rewards Card", _money(-2200, -400), {}),
        (AccountType.LOAN, "Auto Loan", _money(-14000, -7000), {
            "interest_rate": Decimal("0.0599"),
            "minimum_payment": _money(280, 420),
            "original_principal": _money(18000, 26000),
        }),
        (AccountType.INVESTMENT, "Brokerage", _money(35000, 60000), {}),
    ]
    accounts: dict[AccountType, AccountDB] = {}
    for acc_type, name, balance, extra in specs:
        acc = AccountDB(
            uuid=uuid4(),
            user_id=user.db_id,
            account_name=name,
            account_type=acc_type,
            institution_name="Pocket Watcher Demo Bank",
            balance=balance,
            **extra,
        )
        db.add(acc)
        accounts[acc_type] = acc
    db.flush()

    # 2. Categorized transactions on the spending accounts (checking + credit).
    purchases: list[TransactionDB] = []
    for acc in (accounts[AccountType.CHECKING], accounts[AccountType.CREDIT_CARD]):
        for _ in range(random.randint(50, 70)):
            parent_id = random.choice(parent_ids)
            subs = categories[parent_id]
            is_purchase = random.random() < 0.85
            txn = TransactionDB(
                uuid=uuid4(),
                user_id=user.db_id,
                account_id=acc.db_id,
                transaction_date=fake.date_between(start_date="-6M", end_date="today"),
                amount=_money(6, 280),
                transaction_type=TransactionType.PURCHASE if is_purchase else TransactionType.CREDIT,
                category_id=parent_id,
                subcategory_id=random.choice(subs) if subs else None,
                merchant_name=random.choice(_MERCHANTS),
                description=random.choice(_MERCHANTS),
                transaction_hash=uuid4().hex,
                source_type=SourceType.MANUAL,
            )
            db.add(txn)
            if is_purchase:
                purchases.append(txn)
    db.flush()

    # 3. A couple of user tags applied to a sample of transactions.
    tags = []
    for tag_name, color in [("Recurring", "#4f46e5"), ("Tax Deductible", "#16a34a")]:
        tag = TagDB(uuid=uuid4(), user_id=user.db_id, tag_name=tag_name, color=color)
        db.add(tag)
        tags.append(tag)
    db.flush()
    for txn in random.sample(purchases, min(len(purchases), 20)):
        db.add(TransactionTagDB(transaction_id=txn.db_id, tag_id=random.choice(tags).db_id))

    # 4. Investments (holdings rebuilt from the transaction ledger).
    inv = accounts[AccountType.INVESTMENT]
    for symbol, price in _INVESTMENTS:
        db.add(InvestmentTransactionDB(
            uuid=uuid4(),
            user_id=user.db_id,
            account_id=inv.db_id,
            transaction_type=InvestmentTransactionType.BUY,
            symbol=symbol,
            quantity=Decimal(str(random.randint(10, 60))),
            price_per_share=price,
            total_amount=_money(2000, 8000),
            transaction_date=fake.date_between(start_date="-2y", end_date="-6M"),
            transaction_hash=uuid4().hex,
        ))
    db.flush()
    rebuild_holdings_from_transactions(db, inv.db_id)

    # 5. Debt repayment plan + payment history on the loan.
    loan = accounts[AccountType.LOAN]
    checking = accounts[AccountType.CHECKING]
    plan = DebtRepaymentPlanDB(
        uuid=uuid4(),
        user_id=user.db_id,
        plan_name="Auto Loan Payoff",
        strategy=DebtStrategy.AVALANCHE,
        status="ACTIVE",
    )
    db.add(plan)
    db.flush()
    db.add(DebtPlanAccountLinkDB(plan_id=plan.db_id, account_id=loan.db_id, priority=1))
    for _ in range(random.randint(8, 14)):
        payment = _money(300, 500)
        interest = (payment * Decimal("0.2")).quantize(Decimal("0.01"))
        db.add(DebtPaymentDB(
            uuid=uuid4(),
            loan_account_id=loan.db_id,
            payment_source_account_id=checking.db_id,
            payment_amount=payment,
            principal_amount=payment - interest,
            interest_amount=interest,
            payment_date=fake.date_between(start_date="-12M", end_date="today"),
        ))

    # 6. Default budget template + current month so the budget view is live.
    template = BudgetTemplateDB(
        uuid=uuid4(), user_id=user.db_id, template_name="Monthly Budget", is_default=True,
    )
    db.add(template)
    db.flush()
    for parent_id in random.sample(parent_ids, min(6, len(parent_ids))):
        db.add(BudgetTemplateCategoryDB(
            uuid=uuid4(),
            template_id=template.db_id,
            category_id=parent_id,
            allocated_amount=_money(150, 1200),
        ))
    today = date.today()
    db.add(BudgetMonthDB(
        uuid=uuid4(),
        user_id=user.db_id,
        template_id=template.db_id,
        year=today.year,
        month=today.month,
    ))


def reset_demo_data(db, *, email: str, username: str, password: str) -> None:
    """Delete the demo user (cascade) and reseed it. Idempotent."""
    existing = read_db_user(db, email=email)
    if existing is not None:
        db.delete(existing)  # cascade wipes all owned data
        db.flush()

    user = create_db_user(
        db,
        UserCreate(
            email=email,
            username=username,
            password=password,
            confirm_password=password,
        ),
    )
    _seed_user(db, user)
    db.commit()


def main() -> int:
    setup_logging()

    email = os.getenv("DEMO_USER_EMAIL", "demo@pocketwatcher.local")
    username = os.getenv("DEMO_USER_USERNAME", "demo")
    password = os.getenv("DEMO_USER_PASSWORD", "Password123!")

    db = session_local()
    try:
        reset_demo_data(db, email=email, username=username, password=password)
    except Exception:
        db.rollback()
        logger.error("demo reset failed", exc_info=True)
        return 1
    finally:
        db.close()

    logger.info("demo reset complete (user=%s)", email)
    print(f"demo reset complete: {email}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
