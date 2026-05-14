"""Guard tests: regular TransactionDB writes must be rejected when the
target account is an INVESTMENT account.

Direct-handler tests with an in-memory SQLite session, same pattern as
test_transfers_api.py / test_data_health_api.py. The upload-confirm path
in routers/uploads.py shares the same guard but is exercised separately
since it depends on a Redis session.
"""
import unittest
from datetime import date, datetime
from decimal import Decimal
from uuid import uuid4

from fastapi import HTTPException
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.auth.context import set_current_user_id
from src.db.core import (
    AccountDB,
    AccountType,
    Base,
    SourceType,
    TransactionDB,
    TransactionType,
    UserDB,
)
from src.models.transaction import (
    TransactionBulkUpdate,
    TransactionCreate,
    TransactionImport,
    TransactionTypeEnum,
    TransactionUpdate,
    SourceTypeEnum,
)
from src.routers.transactions import (
    bulk_update_transactions,
    create_transaction,
    create_transactions,
    update_transaction,
)


class FakeRequest:
    """Stand-in for FastAPI Request — handlers accept but don't use it."""
    pass


def _seed(session):
    user = UserDB(id=uuid4(), email="t@x.com", username="t", password_hash="x")
    session.add(user)
    session.flush()

    checking = AccountDB(
        uuid=uuid4(), user_id=user.db_id, account_name="TD Checking",
        account_type=AccountType.CHECKING, institution_name="TD",
        balance=Decimal("0"),
    )
    invest = AccountDB(
        uuid=uuid4(), user_id=user.db_id, account_name="Schwab Brokerage",
        account_type=AccountType.INVESTMENT, institution_name="Schwab",
        balance=Decimal("0"),
    )
    session.add_all([checking, invest])
    session.flush()
    session.commit()
    return user, checking, invest


def _seed_txn(session, user_id, account_id, description="x"):
    """Insert a regular transaction on the given account; return it."""
    txn = TransactionDB(
        id=uuid4(), user_id=user_id, account_id=account_id,
        transaction_hash=str(uuid4()), source_type=SourceType.MANUAL,
        transaction_date=date(2026, 4, 1), amount=Decimal("10"),
        transaction_type=TransactionType.PURCHASE, description=description,
        created_at=datetime(2026, 4, 1, 12),
    )
    session.add(txn)
    session.flush()
    session.commit()
    return txn


class GuardBase(unittest.TestCase):
    def setUp(self):
        self.engine = create_engine("sqlite:///:memory:")
        Base.metadata.create_all(self.engine)
        self.Session = sessionmaker(bind=self.engine)
        self.session = self.Session()
        self.user, self.checking, self.invest = _seed(self.session)
        set_current_user_id(self.user.db_id)

    def tearDown(self):
        self.session.close()
        Base.metadata.drop_all(self.engine)
        self.engine.dispose()
        set_current_user_id(None)


class TestCreateTransactionGuard(GuardBase):
    def _payload(self, account_uuid):
        return TransactionCreate(
            account_uuid=account_uuid,
            transaction_date=date(2026, 4, 1),
            amount=Decimal("10"),
            transaction_type=TransactionTypeEnum.PURCHASE,
            description="x",
        )

    def test_rejects_investment_account(self):
        with self.assertRaises(HTTPException) as ctx:
            create_transaction(FakeRequest(), self._payload(self.invest.uuid), db=self.session)
        self.assertEqual(ctx.exception.status_code, 400)
        self.assertIn("investment", ctx.exception.detail.lower())

    def test_allows_non_investment_account(self):
        resp = create_transaction(FakeRequest(), self._payload(self.checking.uuid), db=self.session)
        self.assertIsNotNone(resp.id)


class TestUpdateTransactionGuard(GuardBase):
    def test_rejects_reassignment_to_investment(self):
        txn = _seed_txn(self.session, self.user.db_id, self.checking.id)
        with self.assertRaises(HTTPException) as ctx:
            update_transaction(
                FakeRequest(), str(txn.id),
                TransactionUpdate(account_uuid=self.invest.uuid),
                db=self.session,
            )
        self.assertEqual(ctx.exception.status_code, 400)

    def test_allows_reassignment_to_non_investment(self):
        # Create a second non-investment account to reassign to.
        savings = AccountDB(
            uuid=uuid4(), user_id=self.user.db_id, account_name="Savings",
            account_type=AccountType.SAVINGS, institution_name="TD",
            balance=Decimal("0"),
        )
        self.session.add(savings); self.session.commit()
        txn = _seed_txn(self.session, self.user.db_id, self.checking.id)
        resp = update_transaction(
            FakeRequest(), str(txn.id),
            TransactionUpdate(account_uuid=savings.uuid),
            db=self.session,
        )
        self.assertIsNotNone(resp.id)

    def test_allows_field_edit_without_account_change(self):
        """Description-only edit must not trigger the guard, even though
        the row's existing account could (in some pathological case) be
        INVESTMENT — the guard only fires on reassignment."""
        txn = _seed_txn(self.session, self.user.db_id, self.checking.id)
        resp = update_transaction(
            FakeRequest(), str(txn.id),
            TransactionUpdate(description="edited"),
            db=self.session,
        )
        self.assertEqual(resp.description, "edited")


class TestBulkUpdateGuard(GuardBase):
    def test_rejects_investment_target(self):
        t1 = _seed_txn(self.session, self.user.db_id, self.checking.id, description="t1")
        t2 = _seed_txn(self.session, self.user.db_id, self.checking.id, description="t2")
        with self.assertRaises(HTTPException) as ctx:
            bulk_update_transactions(
                FakeRequest(),
                TransactionBulkUpdate(
                    transaction_uuids=[t1.id, t2.id],
                    account_uuid=self.invest.uuid,
                ),
                db=self.session,
            )
        self.assertEqual(ctx.exception.status_code, 400)
        # Verify no partial write — both rows still attached to checking.
        self.session.refresh(t1); self.session.refresh(t2)
        self.assertEqual(t1.account_id, self.checking.id)
        self.assertEqual(t2.account_id, self.checking.id)

    def test_allows_other_bulk_fields_with_no_account_change(self):
        """Bulk-updating comments with no account_uuid should pass."""
        t1 = _seed_txn(self.session, self.user.db_id, self.checking.id, description="t1")
        result = bulk_update_transactions(
            FakeRequest(),
            TransactionBulkUpdate(
                transaction_uuids=[t1.id],
                comments="bulk note",
            ),
            db=self.session,
        )
        self.assertIn("updated", result["message"])


class TestBulkUploadGuard(GuardBase):
    def _import(self, account_uuid):
        return TransactionImport(
            account_uuid=account_uuid,
            transactions=[
                TransactionCreate(
                    account_uuid=account_uuid,
                    transaction_date=date(2026, 4, 1),
                    amount=Decimal("10"),
                    transaction_type=TransactionTypeEnum.PURCHASE,
                    description="bulk",
                ),
            ],
            source_type=SourceTypeEnum.CSV,
        )

    def test_rejects_investment_target(self):
        with self.assertRaises(HTTPException) as ctx:
            create_transactions(FakeRequest(), self._import(self.invest.uuid), db=self.session)
        self.assertEqual(ctx.exception.status_code, 400)

    def test_allows_non_investment_target(self):
        resp = create_transactions(FakeRequest(), self._import(self.checking.uuid), db=self.session)
        self.assertEqual(len(resp), 1)


if __name__ == "__main__":
    unittest.main()
