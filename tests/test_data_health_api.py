"""Integration tests for /data-health endpoints.

Invokes handlers directly with an in-memory SQLite session, bypassing
HTTP/auth — same pattern as test_transfers_api.
"""
import unittest
from datetime import date, datetime, timedelta
from decimal import Decimal
from uuid import uuid4

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.db.core import (
    AccountDB,
    AccountType,
    AccountValueHistoryDB,
    Base,
    SourceType,
    TransactionDB,
    TransactionTagDB,
    TransactionType,
    UserDB,
)
from src.routers.data_health import count_attention_items, list_attention_items
from src.services.system_tags import ensure_system_tags, get_system_tag


def _seed_user_with_accounts(session, email="t@x.com"):
    user = UserDB(id=uuid4(), email=email, username=email, password_hash="x")
    session.add(user)
    session.flush()
    checking = AccountDB(
        uuid=uuid4(), user_id=user.db_id, account_name="TD Main Checking",
        account_type=AccountType.CHECKING, institution_name="TD Bank",
        balance=Decimal("0"),
    )
    amex = AccountDB(
        uuid=uuid4(), user_id=user.db_id, account_name="Amex Gold",
        account_type=AccountType.CREDIT_CARD, institution_name="American Express",
        balance=Decimal("0"),
    )
    session.add_all([checking, amex])
    session.flush()
    session.commit()
    return user, checking, amex


def _seed_one_of_each_kind(session, user, checking, amex):
    """Create exactly one row per signal kind. Returns the seeded objects
    so tests can introspect IDs."""
    ensure_system_tags(user.db_id, session)
    nr_tag = get_system_tag(user.db_id, session, "Needs Review")

    base_dt = datetime(2026, 1, 1, 12, 0, 0)

    # 1. Needs Review on a regular transaction.
    nr_txn = TransactionDB(
        id=uuid4(), user_id=user.db_id, account_id=checking.id,
        transaction_hash=str(uuid4()), source_type=SourceType.MANUAL,
        transaction_date=date(2026, 4, 1), amount=Decimal("12.50"),
        transaction_type=TransactionType.PURCHASE, description="Starbucks",
        created_at=base_dt + timedelta(hours=1),
    )
    session.add(nr_txn)
    session.flush()
    session.add(TransactionTagDB(
        transaction_id=nr_txn.db_id, tag_id=nr_tag.tag_id,
        created_at=base_dt + timedelta(hours=1),
    ))

    # 2. Transfer pair (TRANSFER_OUT on checking + TRANSFER_IN on amex).
    out = TransactionDB(
        id=uuid4(), user_id=user.db_id, account_id=checking.id,
        transaction_hash=str(uuid4()), source_type=SourceType.MANUAL,
        transaction_date=date(2026, 2, 5), amount=Decimal("100"),
        transaction_type=TransactionType.TRANSFER_OUT,
        description="ELECTRONICPMT AMEXEPAYMENT",
        created_at=base_dt + timedelta(hours=2),
    )
    in_ = TransactionDB(
        id=uuid4(), user_id=user.db_id, account_id=amex.id,
        transaction_hash=str(uuid4()), source_type=SourceType.MANUAL,
        transaction_date=date(2026, 2, 4), amount=Decimal("100"),
        transaction_type=TransactionType.TRANSFER_IN, description="AUTOPAY",
        created_at=base_dt + timedelta(hours=2),
    )
    session.add_all([out, in_])

    # 3. Transfer orphan (TRANSFER_OUT with no matching partner).
    orphan = TransactionDB(
        id=uuid4(), user_id=user.db_id, account_id=checking.id,
        transaction_hash=str(uuid4()), source_type=SourceType.MANUAL,
        transaction_date=date(2025, 11, 1), amount=Decimal("250"),
        transaction_type=TransactionType.TRANSFER_OUT,
        description="LONELY TRANSFER",
        created_at=base_dt + timedelta(hours=3),
    )
    session.add(orphan)

    # 4. Snapshot flagged for review.
    snap = AccountValueHistoryDB(
        uuid=uuid4(), account_id=checking.id,
        value_date=date(2024, 1, 1), balance=Decimal("500"),
        needs_review=True, review_reason="before earliest transaction",
        created_at=base_dt + timedelta(hours=4),
    )
    session.add(snap)

    session.commit()
    return {
        "needs_review_txn": nr_txn,
        "pair_out": out,
        "pair_in": in_,
        "orphan": orphan,
        "snapshot": snap,
    }


class DataHealthAPIBase(unittest.TestCase):
    def setUp(self):
        self.engine = create_engine("sqlite:///:memory:")
        Base.metadata.create_all(self.engine)
        self.Session = sessionmaker(bind=self.engine)
        self.session = self.Session()
        self.user, self.checking, self.amex = _seed_user_with_accounts(self.session)

    def tearDown(self):
        self.session.close()
        Base.metadata.drop_all(self.engine)
        self.engine.dispose()


class TestListAttentionItems(DataHealthAPIBase):
    def test_empty_user(self):
        items = list_attention_items(user_id=self.user.db_id, db=self.session)
        self.assertEqual(items, [])

    def test_all_four_kinds_surface_sorted_desc(self):
        _seed_one_of_each_kind(self.session, self.user, self.checking, self.amex)
        items = list_attention_items(user_id=self.user.db_id, db=self.session)
        kinds = [i.kind for i in items]
        self.assertEqual(set(kinds), {"needs_review", "transfer_pair", "transfer_orphan", "snapshot_review"})

        # Snapshot (h=4) > orphan (h=3) > pair (h=2) > needs_review (h=1).
        timestamps = [i.created_at for i in items]
        self.assertEqual(timestamps, sorted(timestamps, reverse=True))
        self.assertEqual(items[0].kind, "snapshot_review")
        self.assertEqual(items[-1].kind, "needs_review")

    def test_isolation_between_users(self):
        _seed_one_of_each_kind(self.session, self.user, self.checking, self.amex)
        other, other_chk, other_amex = _seed_user_with_accounts(self.session, email="other@x.com")
        items = list_attention_items(user_id=other.db_id, db=self.session)
        self.assertEqual(items, [])


class TestCountAttentionItems(DataHealthAPIBase):
    def test_empty_user(self):
        resp = count_attention_items(user_id=self.user.db_id, db=self.session)
        self.assertEqual(resp.total, 0)
        self.assertEqual(resp.by_kind["needs_review"], 0)
        self.assertEqual(resp.by_kind["transfer_pair"], 0)
        self.assertEqual(resp.by_kind["transfer_orphan"], 0)
        self.assertEqual(resp.by_kind["snapshot_review"], 0)

    def test_count_matches_seeded_kinds(self):
        _seed_one_of_each_kind(self.session, self.user, self.checking, self.amex)
        resp = count_attention_items(user_id=self.user.db_id, db=self.session)
        self.assertEqual(resp.by_kind["needs_review"], 1)
        self.assertEqual(resp.by_kind["transfer_pair"], 1)
        self.assertEqual(resp.by_kind["transfer_orphan"], 1)
        self.assertEqual(resp.by_kind["snapshot_review"], 1)
        self.assertEqual(resp.total, 4)


if __name__ == "__main__":
    unittest.main()
