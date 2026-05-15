"""Round-trip duplicate-detection test for the preview/confirm upload flow.

Backend todo #52: after switching the transaction hash to be account_id-based,
re-uploading the same parsed statement must flag every row as a duplicate.

This test bypasses the PDF/CSV parser layer and exercises the unit of behavior
#52 actually changes: the hash function, the writer in confirm_statement_import,
and the analyzer's pre-flight collision detection. It seeds an account, manually
constructs ParsedTransaction objects, persists them once via the same
generate_transaction_hash() call confirm uses, then runs analyze_regular_transactions
on the same parsed list and asserts every row is flagged as a database duplicate.
"""
import unittest
from datetime import date
from decimal import Decimal
from uuid import uuid4

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.crud.crud_transaction import generate_transaction_hash
from src.db.core import (
    AccountDB,
    AccountType,
    Base,
    SourceType,
    TransactionDB,
    TransactionType,
    UserDB,
)
from src.parser.models import ParsedTransaction
from src.services.duplicate_analyzer import analyze_regular_transactions


def _make_parsed(amount: str, desc: str, day: int, txn_type: str = "PURCHASE") -> ParsedTransaction:
    return ParsedTransaction(
        transaction_date=date(2026, 1, day),
        amount=Decimal(amount),
        transaction_type=txn_type,
        description=desc,
    )


class TestUploadDedupRoundtrip(unittest.TestCase):
    def setUp(self):
        self.engine = create_engine("sqlite:///:memory:")
        Base.metadata.create_all(self.engine)
        self.Session = sessionmaker(bind=self.engine)
        self.session = self.Session()

        self.user = UserDB(id=uuid4(), email="t@x.com", username="t", password_hash="x")
        self.session.add(self.user)
        self.session.flush()

        self.account = AccountDB(
            uuid=uuid4(), user_id=self.user.db_id,
            account_name="TD Main Checking",
            account_type=AccountType.CHECKING,
            institution_name="TD Bank",
            balance=Decimal("0"),
        )
        self.session.add(self.account)
        self.session.commit()

    def tearDown(self):
        self.session.close()
        Base.metadata.drop_all(self.engine)
        self.engine.dispose()

    def _persist_one(self, parsed: ParsedTransaction) -> TransactionDB:
        """Mirror the hash + insert that confirm_statement_import does."""
        txn_type_value = TransactionType[parsed.transaction_type.upper()].value
        txn_hash = generate_transaction_hash(
            user_id=self.user.db_id,
            account_id=self.account.id,
            transaction_date=parsed.transaction_date,
            transaction_type_value=txn_type_value,
            amount=parsed.amount,
            description=parsed.description,
        )
        t = TransactionDB(
            id=uuid4(),
            user_id=self.user.db_id,
            account_id=self.account.id,
            transaction_hash=txn_hash,
            source_type=SourceType.PDF,
            transaction_date=parsed.transaction_date,
            amount=parsed.amount,
            transaction_type=TransactionType[parsed.transaction_type.upper()],
            description=parsed.description,
        )
        self.session.add(t)
        return t

    def test_reupload_flags_every_row_as_duplicate(self):
        parsed = [
            _make_parsed("12.50", "Coffee shop", 5),
            _make_parsed("89.99", "Gas station", 7),
            _make_parsed("3.00", "Subway tap", 8),
        ]
        for p in parsed:
            self._persist_one(p)
        self.session.commit()

        rejected, ready = analyze_regular_transactions(
            parsed, self.user.db_id, self.account.id, self.session,
        )

        self.assertEqual(len(rejected), 3, "every parsed row should be rejected as a DB duplicate")
        self.assertEqual(len(ready), 0, "no parsed row should land in ready_to_import")
        for item in rejected:
            self.assertTrue(item["is_duplicate"])
            self.assertEqual(item["duplicate_type"], "database")

    def test_different_account_same_data_does_not_flag(self):
        """Two accounts at the same institution must not collide on dedup."""
        other = AccountDB(
            uuid=uuid4(), user_id=self.user.db_id,
            account_name="TD Savings",
            account_type=AccountType.SAVINGS,
            institution_name="TD Bank",
            balance=Decimal("0"),
        )
        self.session.add(other)
        self.session.commit()

        parsed = [_make_parsed("12.50", "Coffee shop", 5)]
        self._persist_one(parsed[0])
        self.session.commit()

        rejected, ready = analyze_regular_transactions(
            parsed, self.user.db_id, other.id, self.session,
        )
        self.assertEqual(len(rejected), 0)
        self.assertEqual(len(ready), 1)

    def test_analyzer_rejects_none_account_id(self):
        with self.assertRaises(ValueError):
            analyze_regular_transactions(
                [_make_parsed("12.50", "Coffee", 5)],
                self.user.db_id, None, self.session,
            )


if __name__ == "__main__":
    unittest.main()
