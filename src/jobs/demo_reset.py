#!/usr/bin/env python
"""Wipe-and-reseed the public demo's shared account (#82).

The portfolio demo (the ``DEMO_MODE`` droplet) is a single shared, anonymous,
non-admin user whose data resets daily so every visitor lands on a clean,
fully-categorized dataset. This job is the reset engine:

- **Idempotent wipe-and-seed.** Deletes the existing demo user — ORM
  ``cascade="all, delete-orphan"`` on ``UserDB`` removes every owned account,
  transaction, tag, budget, debt, investment, etc. — then recreates it with a
  realistic dataset: a year-plus of mostly-categorized history across accounts
  at real institutions (Chase, Ally, Amex, Fidelity, …), a populated attention
  inbox (a few 'Needs Review' rows + unlinked transfer pairs awaiting
  matching), and weekly value-history snapshots so the net-worth page reads as
  a real, lived-in account. Safe to run on first boot (the DB is never left
  empty) and on every daily tick.
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
from datetime import date, timedelta
from decimal import Decimal
from uuid import uuid4

from faker import Faker

from src.crud.crud_investment import (
    _update_investment_account_balance,
    rebuild_holdings_from_transactions,
)
from src.crud.crud_user import create_db_user, read_db_user
from src.db.core import (
    AccountDB,
    AccountType,
    AccountValueHistoryDB,
    BudgetMonthDB,
    BudgetTemplateCategoryDB,
    BudgetTemplateDB,
    CategoryDB,
    DebtPaymentDB,
    DebtPlanAccountLinkDB,
    DebtRepaymentPlanDB,
    DebtStrategy,
    InvestmentHoldingDB,
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
from src.services.system_tags import append_review_note, get_system_tag
from src.utils.time import utcnow

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

# Raw, un-cleaned statement descriptions for the "Needs Review" inbox rows —
# deliberately uncategorized + no merchant so they look like a fresh import the
# user still has to triage (mirrors what the bulk-import flow auto-flags).
_RAW_DESCRIPTIONS = [
    "POS DEBIT 0041 PURCHASE",
    "ACH WEB PMT 8829301",
    "SQ *VENDOR 4471",
    "CHECKCARD 1182 TST* MERCHANT",
    "EXTERNAL WITHDRAWAL REF 99281",
    "PAYPAL *INST XFER 77310",
]

# How far back transaction history runs. > 1 year so the spending trends, budget
# history, and net-worth chart all read as a real, lived-in account (#82).
_HISTORY_MONTHS = "-14M"
_HISTORY_DAYS = 420  # ~14 months, for the biweekly paycheck cadence

# Biweekly direct-deposit paychecks land on the checking account. Total payroll
# is sized to modestly exceed seeded expenses so the dashboard shows a realistic
# positive savings rate instead of income perpetually trailing spend.
_PAYCHECK_INTERVAL_DAYS = 14
_INCOME_OVER_EXPENSE = (1.08, 1.18)  # target payroll / total expenses
_EMPLOYER = "ACME CORP DIRECT DEP"

# Snapshot history span (days) and cadence. Weekly points over ~14 months give
# the net-worth chart a populated, trending series; the history endpoints
# downsample to monthly past a year anyway.
_SNAPSHOT_DAYS = 425
_SNAPSHOT_STEP_DAYS = 7

_TWO_PLACES = Decimal("0.01")


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


def _seed_needs_review(db, user, account) -> None:
    """Seed a handful of uncategorized, un-merchanted transactions tagged
    'Needs Review' so the attention inbox (``project_needs_review``) has real
    work in it. The 'Needs Review' system tag is created by ``create_db_user``;
    we look it up and apply it here.
    """
    tag = get_system_tag(user.db_id, db, "Needs Review")
    if tag is None:  # defensive — ensure_system_tags runs in create_db_user
        return
    for raw in random.sample(_RAW_DESCRIPTIONS, 5):
        txn = TransactionDB(
            uuid=uuid4(),
            user_id=user.db_id,
            account_id=account.db_id,
            transaction_date=fake.date_between(start_date="-21d", end_date="today"),
            amount=_money(12, 240),
            transaction_type=TransactionType.PURCHASE,
            category_id=None,
            subcategory_id=None,
            merchant_name=None,
            description=raw,
            comments=append_review_note(None, missing_category=True, missing_merchant=True),
            transaction_hash=uuid4().hex,
            source_type=SourceType.PDF,
        )
        db.add(txn)
        db.flush()
        db.add(TransactionTagDB(transaction_id=txn.db_id, tag_id=tag.db_id))


def _seed_paychecks(db, user, checking, total_expenses) -> None:
    """Seed a biweekly salary DEPOSIT stream on the checking account.

    Total payroll is sized to ``total_expenses × _INCOME_OVER_EXPENSE`` so the
    dashboard's monthly income realistically clears monthly spend (a positive
    savings rate) instead of always trailing it. DEPOSIT counts as income on an
    asset account (``get_transaction_stats`` / ``get_monthly_averages``).
    """
    today = date.today()
    pay_dates = [
        today - timedelta(days=offset)
        for offset in range(0, _HISTORY_DAYS + 1, _PAYCHECK_INTERVAL_DAYS)
    ]
    if not pay_dates:
        return

    target = total_expenses * Decimal(str(round(random.uniform(*_INCOME_OVER_EXPENSE), 3)))
    base = target / Decimal(len(pay_dates))
    for pay_date in pay_dates:
        # Small per-check variance (overtime / bonuses) around the steady base.
        amount = (base * Decimal(str(round(random.uniform(0.97, 1.03), 4)))).quantize(_TWO_PLACES)
        db.add(TransactionDB(
            uuid=uuid4(),
            user_id=user.db_id,
            account_id=checking.db_id,
            transaction_date=pay_date,
            amount=amount,
            transaction_type=TransactionType.DEPOSIT,
            merchant_name=_EMPLOYER,
            description=_EMPLOYER,
            transaction_hash=uuid4().hex,
            source_type=SourceType.MANUAL,
        ))
    db.flush()


def _seed_transfer_pairs(db, user, accounts) -> None:
    """Seed unlinked TRANSFER_OUT / TRANSFER_IN pairs across accounts so the
    inbox surfaces transfer-pair suggestions (``find_pair_suggestions``).

    A pair is suggested when amount matches and the IN date falls inside the
    [out-5d, out+1d] window with no OFFSETS link. Each pair uses a distinct
    amount (well above the 6–280 purchase range) so they don't cross-match each
    other or the regular spend. We do NOT create the OFFSETS relationship —
    pairing them is exactly the demo action a visitor performs from the inbox.
    """
    checking = accounts[AccountType.CHECKING]
    savings = accounts[AccountType.SAVINGS]
    credit = accounts[AccountType.CREDIT_CARD]

    # (out_account, in_account, amount, days_ago, in_offset_days, out_description)
    # in_offset_days is subtracted from the out date (IN posts on/before OUT).
    pairs = [
        (checking, savings, Decimal("500.00"), 8, 0,
         "Transfer to Ally Online Savings"),          # HIGH — names partner acct
        (checking, credit, Decimal("875.00"), 12, 2,
         "AUTOPAY American Express Gold"),             # HIGH — names partner acct
        (checking, savings, Decimal("1250.00"), 19, 1,
         "Online Transfer ref #44192"),               # MEDIUM — amount+date only
    ]
    for out_acc, in_acc, amount, days_ago, in_offset, out_desc in pairs:
        out_date = date.today() - timedelta(days=days_ago)
        in_date = out_date - timedelta(days=in_offset)
        db.add(TransactionDB(
            uuid=uuid4(),
            user_id=user.db_id,
            account_id=out_acc.db_id,
            transaction_date=out_date,
            amount=amount,
            transaction_type=TransactionType.TRANSFER_OUT,
            description=out_desc,
            transaction_hash=uuid4().hex,
            source_type=SourceType.MANUAL,
        ))
        db.add(TransactionDB(
            uuid=uuid4(),
            user_id=user.db_id,
            account_id=in_acc.db_id,
            transaction_date=in_date,
            amount=amount,
            transaction_type=TransactionType.TRANSFER_IN,
            description=f"Transfer from {checking.account_name}",
            transaction_hash=uuid4().hex,
            source_type=SourceType.MANUAL,
        ))
    db.flush()


def _seed_snapshots(db, accounts) -> None:
    """Write weekly value-history snapshots for every account over ~14 months,
    trending from a plausible starting balance to each account's current
    balance, so the net-worth chart and per-account history read as real.

    Synthesized directly (no price fetch) — the daily EOD job pulls live prices,
    but the demo must seed offline and deterministically on a small droplet.
    """
    today = date.today()
    start = today - timedelta(days=_SNAPSHOT_DAYS)

    dates: list[date] = []
    d = start
    while d < today:
        dates.append(d)
        d += timedelta(days=_SNAPSHOT_STEP_DAYS)
    dates.append(today)
    last_idx = len(dates) - 1

    for acc in accounts.values():
        end_balance = acc.balance
        is_liability = acc.account_type in (AccountType.CREDIT_CARD, AccountType.LOAN)
        # Liabilities carried more debt earlier (end balance is negative, so a
        # larger magnitude start pays down toward it); assets started lower.
        start_balance = end_balance * (Decimal("1.5") if is_liability else Decimal("0.65"))

        # For investment accounts, derive the securities/cash/cost-basis split
        # from the live holdings so the per-account history agrees with the
        # holdings view. Ratios are taken against the current balance and held
        # constant across the trend (positions are roughly stable over the
        # window — all buys predate it).
        inv_ratios = None
        if acc.account_type == AccountType.INVESTMENT and end_balance:
            holdings = db.query(InvestmentHoldingDB).filter(
                InvestmentHoldingDB.account_id == acc.db_id
            ).all()
            market_value = sum(
                (h.quantity * h.current_price for h in holdings if h.current_price),
                Decimal("0"),
            )
            total_cost = sum(
                (h.quantity * h.average_cost_basis for h in holdings if h.average_cost_basis),
                Decimal("0"),
            )
            cash = end_balance - market_value
            inv_ratios = (
                market_value / end_balance,                      # securities share
                cash / end_balance,                              # cash share
                (total_cost / market_value) if market_value else Decimal("0"),  # cost per $ of value
            )

        for i, value_date in enumerate(dates):
            if i == last_idx:
                balance = end_balance  # land exactly on the live balance
            else:
                frac = Decimal(i) / Decimal(last_idx)
                trend = start_balance + (end_balance - start_balance) * frac
                noise = Decimal(str(round(random.uniform(-0.015, 0.015), 4)))
                balance = trend * (Decimal("1") + noise)
            balance = balance.quantize(_TWO_PLACES)

            snap = AccountValueHistoryDB(
                uuid=uuid4(),
                account_id=acc.db_id,
                value_date=value_date,
                balance=balance,
                snapshot_source="BACKFILL",
            )
            if inv_ratios is not None:
                sec_ratio, cash_ratio, basis_ratio = inv_ratios
                securities = (balance * sec_ratio).quantize(_TWO_PLACES)
                snap.securities_value = securities
                snap.cash_balance = (balance * cash_ratio).quantize(_TWO_PLACES)
                snap.total_cost_basis = (securities * basis_ratio).quantize(_TWO_PLACES)
                snap.unrealized_gain_loss = (securities - snap.total_cost_basis).quantize(_TWO_PLACES)
            db.add(snap)
    db.flush()


def _seed_user(db, user) -> None:
    categories = _load_categories(db)
    parent_ids = list(categories.keys())

    # 1. Accounts — one of each type, with real institution names so the demo
    # reads like a genuine multi-bank setup (recruiters see "Chase", "Fidelity").
    specs = [
        (AccountType.CHECKING, "TD Bank Convenience Checking", "TD Bank", "4821",
         _money(4000, 9000), {}),
        (AccountType.SAVINGS, "Ally Online Savings", "Ally Bank", "7390",
         _money(15000, 30000), {}),
        (AccountType.CREDIT_CARD, "Amex Gold Card", "American Express", "1005",
         _money(-2200, -400), {}),
        (AccountType.LOAN, "Toyota Auto Loan", "Toyota Financial Services", "6634",
         _money(-14000, -7000), {
            "interest_rate": Decimal("0.0599"),
            "minimum_payment": _money(280, 420),
            "original_principal": _money(18000, 26000),
        }),
        (AccountType.INVESTMENT, "Schwab Brokerage", "Charles Schwab", "2287",
         _money(35000, 60000), {}),
    ]
    accounts: dict[AccountType, AccountDB] = {}
    for acc_type, name, institution, last4, balance, extra in specs:
        acc = AccountDB(
            uuid=uuid4(),
            user_id=user.db_id,
            account_name=name,
            account_type=acc_type,
            institution_name=institution,
            account_number_last4=last4,
            balance=balance,
            **extra,
        )
        db.add(acc)
        accounts[acc_type] = acc
    db.flush()

    # 2. Categorized spend on the spending accounts (checking + credit). Most
    # rows are purchases (expense); the rest are small refunds (CREDIT). Real
    # income is the paycheck stream below, not refund noise.
    purchases: list[TransactionDB] = []
    total_expenses = Decimal("0")
    for acc in (accounts[AccountType.CHECKING], accounts[AccountType.CREDIT_CARD]):
        for _ in range(random.randint(130, 170)):
            parent_id = random.choice(parent_ids)
            subs = categories[parent_id]
            is_purchase = random.random() < 0.90
            amount = _money(6, 280) if is_purchase else _money(5, 75)
            txn = TransactionDB(
                uuid=uuid4(),
                user_id=user.db_id,
                account_id=acc.db_id,
                transaction_date=fake.date_between(start_date=_HISTORY_MONTHS, end_date="today"),
                amount=amount,
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
                total_expenses += amount
    db.flush()

    _seed_paychecks(db, user, accounts[AccountType.CHECKING], total_expenses)

    # 3. A couple of user tags applied to a sample of transactions.
    tags = []
    for tag_name, color in [("Recurring", "#4f46e5"), ("Tax Deductible", "#16a34a")]:
        tag = TagDB(uuid=uuid4(), user_id=user.db_id, tag_name=tag_name, color=color)
        db.add(tag)
        tags.append(tag)
    db.flush()
    for txn in random.sample(purchases, min(len(purchases), 20)):
        db.add(TransactionTagDB(transaction_id=txn.db_id, tag_id=random.choice(tags).db_id))

    _seed_needs_review(db, user, accounts[AccountType.CHECKING])
    _seed_transfer_pairs(db, user, accounts)

    # 4. Investments — buy a dollar-sized lot of each position, rebuild holdings
    # from the ledger, then mark each holding to a current price above cost so
    # the portfolio shows market value AND unrealized P&L. In prod the EOD job
    # fetches live prices; the seed sets them offline so the holdings view isn't
    # blank. Account balance is then recomputed = market value + leftover cash.
    inv = accounts[AccountType.INVESTMENT]
    inv.initial_cash_balance = _money(55000, 70000)
    for symbol, price in _INVESTMENTS:
        quantity = (_money(8000, 14000) / price).quantize(Decimal("0.0001"))
        db.add(InvestmentTransactionDB(
            uuid=uuid4(),
            user_id=user.db_id,
            account_id=inv.db_id,
            transaction_type=InvestmentTransactionType.BUY,
            symbol=symbol,
            quantity=quantity,
            price_per_share=price,
            total_amount=(quantity * price).quantize(_TWO_PLACES),
            transaction_date=fake.date_between(start_date="-2y", end_date="-6M"),
            transaction_hash=uuid4().hex,
        ))
    db.flush()
    rebuild_holdings_from_transactions(db, inv.db_id)
    for holding in db.query(InvestmentHoldingDB).filter(
        InvestmentHoldingDB.account_id == inv.db_id
    ):
        gain_factor = Decimal(str(round(random.uniform(0.92, 1.35), 4)))
        holding.current_price = (holding.average_cost_basis * gain_factor).quantize(Decimal("0.0001"))
        holding.last_price_update = utcnow()
    db.flush()
    _update_investment_account_balance(db, inv.db_id)

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

    # 7. Daily value-history snapshots so the net-worth page is populated.
    _seed_snapshots(db, accounts)


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
