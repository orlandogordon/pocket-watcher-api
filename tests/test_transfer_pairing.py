"""Tier B transfer pairing unit tests."""
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
    DismissedTransferPairDB,
    InvestmentTransactionDB,
    InvestmentTransactionType,
    RelationshipType,
    SourceType,
    TransactionDB,
    TransactionRelationshipDB,
    TransactionType,
    UserDB,
)
from src.services.transfer_pairing import (
    PairConfidence,
    TxnSide,
    create_offsets_relationship,
    find_auto_pair_for_outflow,
    find_orphans,
    find_pair_suggestions,
)


def _user(session) -> UserDB:
    u = UserDB(id=uuid4(), email="t@x.com", username="t", password_hash="x")
    session.add(u)
    session.flush()
    return u


def _account(session, user_id: int, name: str, account_type: AccountType, institution: str = "TestBank") -> AccountDB:
    a = AccountDB(
        uuid=uuid4(), user_id=user_id, account_name=name,
        account_type=account_type, institution_name=institution,
        balance=Decimal("0"),
    )
    session.add(a)
    session.flush()
    return a


def _txn(session, user_id: int, account_id: int, txn_type: TransactionType,
         amount, txn_date: date, description: str = "") -> TransactionDB:
    t = TransactionDB(
        id=uuid4(), user_id=user_id, account_id=account_id,
        transaction_hash=str(uuid4()),
        source_type=SourceType.MANUAL,
        transaction_date=txn_date,
        amount=Decimal(str(amount)),
        transaction_type=txn_type,
        description=description,
    )
    session.add(t)
    session.flush()
    return t


def _inv_txn(session, user_id: int, account_id: int, txn_type: InvestmentTransactionType,
             amount, txn_date: date, description: str = "") -> InvestmentTransactionDB:
    t = InvestmentTransactionDB(
        id=uuid4(), user_id=user_id, account_id=account_id,
        transaction_hash=str(uuid4()),
        transaction_type=txn_type,
        total_amount=Decimal(str(amount)),
        transaction_date=txn_date,
        description=description,
    )
    session.add(t)
    session.flush()
    return t


class PairingBase(unittest.TestCase):
    def setUp(self):
        self.engine = create_engine("sqlite:///:memory:")
        Base.metadata.create_all(self.engine)
        self.Session = sessionmaker(bind=self.engine)
        self.session = self.Session()
        self.user = _user(self.session)
        self.checking = _account(self.session, self.user.db_id, "TD Main Checking", AccountType.CHECKING, "TD Bank")
        self.amex = _account(self.session, self.user.db_id, "Amex Gold", AccountType.CREDIT_CARD, "American Express")
        self.schwab = _account(self.session, self.user.db_id, "Schwab Brokerage", AccountType.INVESTMENT, "Charles Schwab")
        self.session.commit()

    def tearDown(self):
        self.session.close()
        Base.metadata.drop_all(self.engine)
        self.engine.dispose()


class TestFindPairSuggestions(PairingBase):
    def test_exact_pair_high_confidence(self):
        _txn(self.session, self.user.db_id, self.checking.id,
             TransactionType.TRANSFER_OUT, 100, date(2026, 2, 5),
             "ELECTRONICPMT AMEXEPAYMENT")
        _txn(self.session, self.user.db_id, self.amex.id,
             TransactionType.TRANSFER_IN, 100, date(2026, 2, 4),
             "AUTOPAY PAYMENT THANK YOU")
        self.session.commit()

        candidates = find_pair_suggestions(self.session, self.user.db_id)
        self.assertEqual(len(candidates), 1)
        c = candidates[0]
        # OUT.date - IN.date = -1 (CC posts 1 day before checking)
        self.assertEqual(c.date_offset_days, -1)
        self.assertEqual(c.confidence, PairConfidence.HIGH)

    def test_out_of_window_no_suggestion(self):
        _txn(self.session, self.user.db_id, self.checking.id,
             TransactionType.TRANSFER_OUT, 100, date(2026, 2, 15),
             "AMEXEPAYMENT")
        _txn(self.session, self.user.db_id, self.amex.id,
             TransactionType.TRANSFER_IN, 100, date(2026, 2, 5))
        self.session.commit()
        self.assertEqual(find_pair_suggestions(self.session, self.user.db_id), [])

    def test_amount_mismatch_no_suggestion(self):
        _txn(self.session, self.user.db_id, self.checking.id,
             TransactionType.TRANSFER_OUT, 100.00, date(2026, 2, 5), "AMEX")
        _txn(self.session, self.user.db_id, self.amex.id,
             TransactionType.TRANSFER_IN, 100.01, date(2026, 2, 4))
        self.session.commit()
        self.assertEqual(find_pair_suggestions(self.session, self.user.db_id), [])

    def test_already_paired_excluded(self):
        out = _txn(self.session, self.user.db_id, self.checking.id,
                   TransactionType.TRANSFER_OUT, 100, date(2026, 2, 5), "AMEX")
        in_ = _txn(self.session, self.user.db_id, self.amex.id,
                   TransactionType.TRANSFER_IN, 100, date(2026, 2, 4))
        rel = TransactionRelationshipDB(
            id=uuid4(), relationship_type=RelationshipType.OFFSETS,
            from_transaction_id=out.db_id, to_transaction_id=in_.db_id,
        )
        self.session.add(rel)
        self.session.commit()
        self.assertEqual(find_pair_suggestions(self.session, self.user.db_id), [])

    def test_dismissed_pair_excluded(self):
        out = _txn(self.session, self.user.db_id, self.checking.id,
                   TransactionType.TRANSFER_OUT, 100, date(2026, 2, 5), "AMEX")
        in_ = _txn(self.session, self.user.db_id, self.amex.id,
                   TransactionType.TRANSFER_IN, 100, date(2026, 2, 4))
        self.session.add(DismissedTransferPairDB(
            user_id=self.user.db_id,
            from_transaction_id=out.db_id,
            to_transaction_id=in_.db_id,
            created_at=datetime.utcnow(),
        ))
        self.session.commit()
        self.assertEqual(find_pair_suggestions(self.session, self.user.db_id), [])

    def test_cross_table_pair_with_investment(self):
        # Checking TRANSFER_OUT pairs with Schwab investment TRANSFER_IN.
        _txn(self.session, self.user.db_id, self.checking.id,
             TransactionType.TRANSFER_OUT, 500, date(2026, 3, 10),
             "SCHWAB BROKERAGE MONEYLINK")
        _inv_txn(self.session, self.user.db_id, self.schwab.id,
                 InvestmentTransactionType.TRANSFER_IN, 500, date(2026, 3, 9))
        self.session.commit()

        candidates = find_pair_suggestions(self.session, self.user.db_id)
        self.assertEqual(len(candidates), 1)
        c = candidates[0]
        self.assertFalse(c.out_side.is_investment)
        self.assertTrue(c.in_side.is_investment)
        self.assertEqual(c.confidence, PairConfidence.HIGH)


class TestFindAutoPair(PairingBase):
    def test_unique_closest_date_auto_pairs(self):
        out = _txn(self.session, self.user.db_id, self.checking.id,
                   TransactionType.TRANSFER_OUT, 100, date(2026, 2, 5),
                   "AMEXEPAYMENT")
        in_ = _txn(self.session, self.user.db_id, self.amex.id,
                   TransactionType.TRANSFER_IN, 100, date(2026, 2, 4))
        self.session.commit()

        out_side = TxnSide(
            is_investment=False, txn_id=out.db_id, user_id=self.user.db_id,
            account_id=self.checking.id, transaction_date=out.transaction_date,
            amount=out.amount, description=out.description,
        )
        match = find_auto_pair_for_outflow(self.session, out_side, self.amex.id)
        self.assertIsNotNone(match)
        self.assertEqual(match.txn_id, in_.db_id)

    def test_tied_closest_date_returns_none(self):
        out = _txn(self.session, self.user.db_id, self.checking.id,
                   TransactionType.TRANSFER_OUT, 100, date(2026, 2, 5),
                   "AMEXEPAYMENT")
        # Two candidates at identical offset (-1 day from OUT).
        _txn(self.session, self.user.db_id, self.amex.id,
             TransactionType.TRANSFER_IN, 100, date(2026, 2, 4))
        _txn(self.session, self.user.db_id, self.amex.id,
             TransactionType.TRANSFER_IN, 100, date(2026, 2, 6))  # +1 offset
        self.session.commit()

        out_side = TxnSide(
            is_investment=False, txn_id=out.db_id, user_id=self.user.db_id,
            account_id=self.checking.id, transaction_date=out.transaction_date,
            amount=out.amount, description=out.description,
        )
        # Date 2026-02-04 is at |−1| = 1; 2026-02-06 is at |+1| = 1. Tied.
        self.assertIsNone(find_auto_pair_for_outflow(self.session, out_side, self.amex.id))

    def test_wrong_account_no_pair(self):
        out = _txn(self.session, self.user.db_id, self.checking.id,
                   TransactionType.TRANSFER_OUT, 100, date(2026, 2, 5),
                   "AMEXEPAYMENT")
        # TRANSFER_IN exists but on Schwab, not the suggested Amex partner.
        _inv_txn(self.session, self.user.db_id, self.schwab.id,
                 InvestmentTransactionType.TRANSFER_IN, 100, date(2026, 2, 4))
        self.session.commit()

        out_side = TxnSide(
            is_investment=False, txn_id=out.db_id, user_id=self.user.db_id,
            account_id=self.checking.id, transaction_date=out.transaction_date,
            amount=out.amount, description=out.description,
        )
        self.assertIsNone(find_auto_pair_for_outflow(self.session, out_side, self.amex.id))


class TestCreateOffsetsRelationship(PairingBase):
    def test_regular_pair(self):
        out = _txn(self.session, self.user.db_id, self.checking.id,
                   TransactionType.TRANSFER_OUT, 100, date(2026, 2, 5))
        in_ = _txn(self.session, self.user.db_id, self.amex.id,
                   TransactionType.TRANSFER_IN, 100, date(2026, 2, 4))
        out_side = TxnSide(False, out.db_id, self.user.db_id, self.checking.id,
                           out.transaction_date, out.amount, None)
        in_side = TxnSide(False, in_.db_id, self.user.db_id, self.amex.id,
                          in_.transaction_date, in_.amount, None)
        rel = create_offsets_relationship(self.session, out_side, in_side)
        self.session.commit()
        self.assertEqual(rel.from_transaction_id, out.db_id)
        self.assertEqual(rel.to_transaction_id, in_.db_id)
        self.assertIsNone(rel.from_investment_transaction_id)
        self.assertIsNone(rel.to_investment_transaction_id)

    def test_cross_table_pair(self):
        out = _txn(self.session, self.user.db_id, self.checking.id,
                   TransactionType.TRANSFER_OUT, 500, date(2026, 3, 10))
        in_ = _inv_txn(self.session, self.user.db_id, self.schwab.id,
                       InvestmentTransactionType.TRANSFER_IN, 500, date(2026, 3, 9))
        out_side = TxnSide(False, out.db_id, self.user.db_id, self.checking.id,
                           out.transaction_date, out.amount, None)
        in_side = TxnSide(True, in_.investment_transaction_id, self.user.db_id, self.schwab.id,
                          in_.transaction_date, in_.total_amount, None)
        rel = create_offsets_relationship(self.session, out_side, in_side)
        self.session.commit()
        self.assertEqual(rel.from_transaction_id, out.db_id)
        self.assertIsNone(rel.to_transaction_id)
        self.assertIsNone(rel.from_investment_transaction_id)
        self.assertEqual(rel.to_investment_transaction_id, in_.investment_transaction_id)


class TestFindOrphans(PairingBase):
    def test_orphan_when_no_partner(self):
        # A single TRANSFER_OUT with no matching TRANSFER_IN anywhere.
        _txn(self.session, self.user.db_id, self.checking.id,
             TransactionType.TRANSFER_OUT, 100, date(2026, 2, 5), "AMEX")
        self.session.commit()
        orphans = find_orphans(self.session, self.user.db_id)
        self.assertEqual(len(orphans), 1)

    def test_not_orphan_when_suggestion_exists(self):
        _txn(self.session, self.user.db_id, self.checking.id,
             TransactionType.TRANSFER_OUT, 100, date(2026, 2, 5), "AMEX")
        _txn(self.session, self.user.db_id, self.amex.id,
             TransactionType.TRANSFER_IN, 100, date(2026, 2, 4))
        self.session.commit()
        self.assertEqual(find_orphans(self.session, self.user.db_id), [])


class TestUpdateTypeWithHash(PairingBase):
    def test_hash_changes_with_type(self):
        from src.crud.crud_transaction import update_transaction_type_with_hash, generate_transaction_hash

        txn = _txn(self.session, self.user.db_id, self.checking.id,
                   TransactionType.PURCHASE, 100, date(2026, 2, 5), "AMEXEPAYMENT")
        # Set the hash to the proper PURCHASE hash so we can verify the change.
        txn.transaction_hash = generate_transaction_hash(
            user_id=self.user.db_id,
            institution_name=self.checking.institution_name,
            transaction_date=txn.transaction_date,
            transaction_type_value="PURCHASE",
            amount=txn.amount,
            description=txn.description,
        )
        self.session.commit()
        old_hash = txn.transaction_hash

        update_transaction_type_with_hash(self.session, txn, TransactionType.TRANSFER_OUT)
        self.session.commit()

        self.assertEqual(txn.transaction_type, TransactionType.TRANSFER_OUT)
        self.assertNotEqual(txn.transaction_hash, old_hash)

        expected = generate_transaction_hash(
            user_id=self.user.db_id,
            institution_name=self.checking.institution_name,
            transaction_date=txn.transaction_date,
            transaction_type_value="TRANSFER_OUT",
            amount=txn.amount,
            description=txn.description,
        )
        self.assertEqual(txn.transaction_hash, expected)


if __name__ == "__main__":
    unittest.main()
