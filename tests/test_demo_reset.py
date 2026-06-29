"""demo_reset.reset_demo_data (#82).

The daily demo reset is an idempotent wipe-and-seed of one shared non-admin
user. Categories aren't seeded into the test DB by the schema-only create_all,
so each test seeds them from the predefined constants first (the same set the
Alembic category migration installs in prod).
"""
from src.constants.categories import PREDEFINED_CATEGORIES
from src.crud.crud_user import read_db_user
from src.db.core import (
    AccountDB,
    AccountType,
    AccountValueHistoryDB,
    CategoryDB,
    InvestmentHoldingDB,
    TransactionDB,
    UserDB,
)
from src.jobs.demo_reset import reset_demo_data
from src.services.system_tags import get_system_tag
from src.services.transfer_pairing import find_pair_suggestions
from tests.factories import make_account, make_user

DEMO = dict(email="demo@pocketwatcher.local", username="demo", password="Password123!")


def _seed_categories(db):
    for pname, puuid, subs in PREDEFINED_CATEGORIES:
        parent = CategoryDB(uuid=puuid, name=pname)
        db.add(parent)
        db.flush()
        for sname, suuid in subs:
            db.add(CategoryDB(uuid=suuid, name=sname, parent_category_id=parent.db_id))
    db.flush()


def test_reset_seeds_demo_user_with_categorized_data(db):
    _seed_categories(db)
    reset_demo_data(db, **DEMO)

    user = read_db_user(db, email=DEMO["email"])
    assert user is not None
    assert user.is_admin is False

    accounts = db.query(AccountDB).filter(AccountDB.user_id == user.db_id).all()
    assert len(accounts) == 5  # one of each account type

    txns = db.query(TransactionDB).filter(TransactionDB.user_id == user.db_id).all()
    assert txns, "expected seeded transactions"
    # Most spend is pre-categorized; only the deliberate review/transfer rows
    # are left uncategorized, so the bulk of the ledger reads as triaged.
    categorized = [t for t in txns if t.category_id is not None]
    assert len(categorized) > len(txns) * 0.7


def test_reset_seeds_real_institution_names(db):
    _seed_categories(db)
    reset_demo_data(db, **DEMO)
    user = read_db_user(db, email=DEMO["email"])

    institutions = {
        a.institution_name
        for a in db.query(AccountDB).filter(AccountDB.user_id == user.db_id)
    }
    assert "Pocket Watcher Demo Bank" not in institutions
    assert {"TD Bank", "Charles Schwab"} <= institutions


def test_reset_seeds_year_plus_of_history(db):
    from datetime import date, timedelta

    _seed_categories(db)
    reset_demo_data(db, **DEMO)
    user = read_db_user(db, email=DEMO["email"])

    earliest = min(
        t.transaction_date
        for t in db.query(TransactionDB).filter(TransactionDB.user_id == user.db_id)
    )
    assert earliest <= date.today() - timedelta(days=365)


def test_reset_seeds_inbox_items(db):
    _seed_categories(db)
    reset_demo_data(db, **DEMO)
    user = read_db_user(db, email=DEMO["email"])

    # Needs-review rows: tagged with the system tag, uncategorized.
    tag = get_system_tag(user.db_id, db, "Needs Review")
    assert tag is not None
    tagged = (
        db.query(TransactionDB)
        .join(TransactionDB.transaction_tags)
        .filter(TransactionDB.user_id == user.db_id)
        .all()
    )
    flagged = [t for t in tagged if any(tt.tag_id == tag.db_id for tt in t.transaction_tags)]
    assert flagged, "expected Needs Review transactions in the inbox"
    assert all(t.category_id is None for t in flagged)

    # Transfer-pair suggestions surface from the unlinked transfer rows.
    suggestions = find_pair_suggestions(db, user.db_id)
    assert len(suggestions) >= 3


def test_reset_seeds_income_above_expenses(db):
    from src.crud.crud_transaction import get_transaction_stats

    _seed_categories(db)
    reset_demo_data(db, **DEMO)
    user = read_db_user(db, email=DEMO["email"])

    stats = get_transaction_stats(db, user.db_id)
    # A realistic, positive savings rate — income clears spend but not absurdly.
    assert stats.total_income > stats.total_expenses
    assert stats.total_income < stats.total_expenses * 2


def test_reset_seeds_priced_investment_holdings(db):
    _seed_categories(db)
    reset_demo_data(db, **DEMO)
    user = read_db_user(db, email=DEMO["email"])

    inv = (
        db.query(AccountDB)
        .filter(
            AccountDB.user_id == user.db_id,
            AccountDB.account_type == AccountType.INVESTMENT,
        )
        .one()
    )
    holdings = (
        db.query(InvestmentHoldingDB)
        .filter(InvestmentHoldingDB.account_id == inv.db_id)
        .all()
    )
    assert holdings, "expected seeded holdings"
    # Every holding has a market price and a cost basis -> market value + P&L.
    for h in holdings:
        assert h.current_price is not None and h.current_price > 0
        assert h.average_cost_basis is not None and h.average_cost_basis > 0


def test_reset_seeds_snapshots_for_all_accounts(db):
    _seed_categories(db)
    reset_demo_data(db, **DEMO)
    user = read_db_user(db, email=DEMO["email"])

    account_ids = [
        a.db_id for a in db.query(AccountDB).filter(AccountDB.user_id == user.db_id)
    ]
    for account_id in account_ids:
        snaps = (
            db.query(AccountValueHistoryDB)
            .filter(AccountValueHistoryDB.account_id == account_id)
            .all()
        )
        assert len(snaps) > 50, "expected a year+ of weekly snapshots per account"
        # The latest snapshot lands exactly on the account's live balance.
        account = next(a for a in db.query(AccountDB) if a.db_id == account_id)
        latest = max(snaps, key=lambda s: s.value_date)
        assert latest.balance == account.balance


def test_reset_is_idempotent(db):
    _seed_categories(db)
    reset_demo_data(db, **DEMO)
    reset_demo_data(db, **DEMO)

    # Exactly one demo user, exactly one account set — prior data was wiped, not
    # duplicated or leaked.
    assert db.query(UserDB).filter(UserDB.email == DEMO["email"]).count() == 1
    user = read_db_user(db, email=DEMO["email"])
    assert db.query(AccountDB).filter(AccountDB.user_id == user.db_id).count() == 5


def test_reset_leaves_other_users_untouched(db):
    _seed_categories(db)
    other = make_user(db, email="real@example.com", username="realuser")
    other_account = make_account(db, other)

    reset_demo_data(db, **DEMO)

    assert read_db_user(db, email="real@example.com") is not None
    assert db.query(AccountDB).filter(AccountDB.db_id == other_account.db_id).count() == 1
