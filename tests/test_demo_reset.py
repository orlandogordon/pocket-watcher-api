"""demo_reset.reset_demo_data (#82).

The daily demo reset is an idempotent wipe-and-seed of one shared non-admin
user. Categories aren't seeded into the test DB by the schema-only create_all,
so each test seeds them from the predefined constants first (the same set the
Alembic category migration installs in prod).
"""
from src.constants.categories import PREDEFINED_CATEGORIES
from src.crud.crud_user import read_db_user
from src.db.core import AccountDB, CategoryDB, TransactionDB, UserDB
from src.jobs.demo_reset import reset_demo_data
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
    # Every seeded transaction is already categorized — nothing lands in review.
    assert all(t.category_id is not None for t in txns)


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
