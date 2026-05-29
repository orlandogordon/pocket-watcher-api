"""Regression tests for transaction/investment-transaction delete paths
under SQLite FK enforcement.

Background: backend #39 enabled `PRAGMA foreign_keys = ON` globally for
SQLite. Before that, FK constraints (including ORM-cascade and SET NULL)
were silently unenforced on SQLite. The existing test suite never
exercised delete paths against the variety of child tables a transaction
can have, so these regressions guard against any FK violation surfacing
in production deletes now that enforcement is live.
"""
import unittest
from datetime import date, datetime
from decimal import Decimal
from uuid import uuid4

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.db.core import (
    AccountDB,
    AccountType,
    Base,
    CategoryDB,
    DismissedTransferPairDB,
    InvestmentTransactionDB,
    InvestmentTransactionType,
    ParsedImportDB,
    RelationshipType,
    SourceType,
    SkippedTransactionDB,
    TagDB,
    TransactionAmortizationScheduleDB,
    TransactionDB,
    TransactionRelationshipDB,
    TransactionSplitAllocationDB,
    TransactionTagDB,
    TransactionType,
    UploadJobDB,
    UserDB,
)


class DeleteCascadeBase(unittest.TestCase):
    def setUp(self):
        self.engine = create_engine("sqlite:///:memory:")
        Base.metadata.create_all(self.engine)
        self.Session = sessionmaker(bind=self.engine)
        self.session = self.Session()

        self.user = UserDB(uuid=uuid4(), email="t@x", username="t", password_hash="x")
        self.session.add(self.user)
        self.session.flush()

        self.account = AccountDB(
            uuid=uuid4(), user_id=self.user.db_id, account_name="A",
            account_type=AccountType.CHECKING, institution_name="T",
            balance=Decimal("0"),
        )
        self.session.add(self.account)
        self.session.flush()

        self.category = CategoryDB(uuid=uuid4(), name="Food")
        self.session.add(self.category)
        self.session.flush()

        self.session.commit()

    def tearDown(self):
        self.session.close()
        Base.metadata.drop_all(self.engine)
        self.engine.dispose()

    def _make_transaction(self, amount=100, txn_type=TransactionType.PURCHASE) -> TransactionDB:
        t = TransactionDB(
            uuid=uuid4(), user_id=self.user.db_id, account_id=self.account.db_id,
            transaction_hash=str(uuid4()), source_type=SourceType.MANUAL,
            transaction_date=date(2026, 1, 1), amount=Decimal(str(amount)),
            transaction_type=txn_type, description="test",
        )
        self.session.add(t)
        self.session.flush()
        return t

    def _make_investment_transaction(self) -> InvestmentTransactionDB:
        t = InvestmentTransactionDB(
            uuid=uuid4(), user_id=self.user.db_id, account_id=self.account.db_id,
            transaction_hash=str(uuid4()),
            transaction_type=InvestmentTransactionType.BUY,
            total_amount=Decimal("100"), transaction_date=date(2026, 1, 1),
        )
        self.session.add(t)
        self.session.flush()
        return t


class TestRegularTransactionDelete(DeleteCascadeBase):
    """Each test attaches a different kind of child row to a transaction
    and verifies delete succeeds without FK violation."""

    def test_delete_with_split_allocations(self):
        t = self._make_transaction()
        split = TransactionSplitAllocationDB(
            uuid=uuid4(), transaction_id=t.db_id, category_id=self.category.db_id,
            amount=Decimal("100"),
        )
        self.session.add(split)
        self.session.commit()

        self.session.delete(t)
        self.session.commit()  # would raise FK violation if cascade broken
        self.assertEqual(self.session.query(TransactionSplitAllocationDB).count(), 0)

    def test_delete_with_amortization_schedule(self):
        t = self._make_transaction()
        sched = TransactionAmortizationScheduleDB(
            uuid=uuid4(), transaction_id=t.db_id,
            month_date=date(2026, 1, 1), amount=Decimal("100"),
        )
        self.session.add(sched)
        self.session.commit()

        self.session.delete(t)
        self.session.commit()
        self.assertEqual(self.session.query(TransactionAmortizationScheduleDB).count(), 0)

    def test_delete_with_tag_m2m(self):
        t = self._make_transaction()
        tag = TagDB(uuid=uuid4(), user_id=self.user.db_id, tag_name="recurring")
        self.session.add(tag)
        self.session.flush()
        link = TransactionTagDB(transaction_id=t.db_id, tag_id=tag.db_id)
        self.session.add(link)
        self.session.commit()

        self.session.delete(t)
        self.session.commit()
        self.assertEqual(self.session.query(TransactionTagDB).count(), 0)
        # Tag itself survives
        self.assertEqual(self.session.query(TagDB).count(), 1)

    def test_delete_with_relationship(self):
        t1 = self._make_transaction(amount=100, txn_type=TransactionType.TRANSFER_OUT)
        t2 = self._make_transaction(amount=100, txn_type=TransactionType.TRANSFER_IN)
        rel = TransactionRelationshipDB(
            uuid=uuid4(), relationship_type=RelationshipType.OFFSETS,
            from_transaction_id=t1.db_id, to_transaction_id=t2.db_id,
        )
        self.session.add(rel)
        self.session.commit()

        self.session.delete(t1)
        self.session.commit()
        self.assertEqual(self.session.query(TransactionRelationshipDB).count(), 0)
        # Partner survives
        self.assertEqual(self.session.query(TransactionDB).count(), 1)

    def test_delete_with_parsed_import_sets_null(self):
        """Audit row survives; transaction_id FK gets nulled."""
        t = self._make_transaction()
        job = UploadJobDB(
            uuid=uuid4(), user_id=self.user.db_id, account_id=self.account.db_id,
            institution="test", status="COMPLETED",
        )
        self.session.add(job)
        self.session.flush()
        pi = ParsedImportDB(
            upload_job_id=job.db_id, transaction_id=t.uuid,
            raw_parsed_data={"foo": "bar"},
        )
        self.session.add(pi)
        self.session.commit()

        self.session.delete(t)
        self.session.commit()

        surviving = self.session.query(ParsedImportDB).one()
        self.assertIsNone(surviving.transaction_id,
                          "FK should have been SET NULL, not violated")

    def test_delete_with_skipped_transaction_sets_null(self):
        t = self._make_transaction()
        job = UploadJobDB(
            uuid=uuid4(), user_id=self.user.db_id, account_id=self.account.db_id,
            institution="test", status="COMPLETED",
        )
        self.session.add(job)
        self.session.flush()
        sk = SkippedTransactionDB(
            upload_job_id=job.db_id,
            transaction_type="REGULAR",
            parsed_date=date(2026, 1, 1),
            parsed_amount=Decimal("100"),
            parsed_description="dup",
            parsed_transaction_type="PURCHASE",
            existing_transaction_id=t.uuid,
        )
        self.session.add(sk)
        self.session.commit()

        self.session.delete(t)
        self.session.commit()

        surviving = self.session.query(SkippedTransactionDB).one()
        self.assertIsNone(surviving.existing_transaction_id)


class TestInvestmentTransactionDelete(DeleteCascadeBase):
    """Investment transactions have fewer child types, but the OFFSETS
    relationship is the one that historically had no cascade on the
    investment side. #39 fixed it; this test guards the fix."""

    def test_delete_with_relationship_investment_side(self):
        regular = self._make_transaction(
            amount=500, txn_type=TransactionType.TRANSFER_OUT,
        )
        inv = self._make_investment_transaction()
        # Change inv type to TRANSFER_IN for a realistic cross-table pair
        inv.transaction_type = InvestmentTransactionType.TRANSFER_IN
        inv.total_amount = Decimal("500")
        self.session.flush()

        rel = TransactionRelationshipDB(
            uuid=uuid4(), relationship_type=RelationshipType.OFFSETS,
            from_transaction_id=regular.db_id,
            to_investment_transaction_id=inv.db_id,
        )
        self.session.add(rel)
        self.session.commit()

        # Delete the investment side — DB cascade must fire (no ORM cascade
        # exists on InvestmentTransactionDB for relationships).
        self.session.delete(inv)
        self.session.commit()
        self.assertEqual(self.session.query(TransactionRelationshipDB).count(), 0)

    def test_delete_with_dismissed_pair(self):
        regular = self._make_transaction(
            amount=500, txn_type=TransactionType.TRANSFER_OUT,
        )
        inv = self._make_investment_transaction()
        dismissal = DismissedTransferPairDB(
            user_id=self.user.db_id,
            from_transaction_id=regular.db_id,
            to_investment_transaction_id=inv.db_id,
            created_at=datetime.utcnow(),
        )
        self.session.add(dismissal)
        self.session.commit()

        self.session.delete(inv)
        self.session.commit()
        self.assertEqual(self.session.query(DismissedTransferPairDB).count(), 0)


if __name__ == "__main__":
    unittest.main()
