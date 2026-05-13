"""Endpoint tests for /transfers/* router.

Invokes handlers directly with an in-memory SQLite session, bypassing
HTTP/auth — same shape as test_transfer_pairing.
"""
import unittest
from datetime import date, datetime
from decimal import Decimal
from uuid import UUID, uuid4

from fastapi import HTTPException
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
    TagDB,
    TransactionDB,
    TransactionRelationshipDB,
    TransactionTagDB,
    TransactionType,
    UserDB,
)
from src.services.system_tags import ensure_system_tags, get_system_tag
from src.routers.transfers import (
    ConfirmRequest,
    DismissRequest,
    confirm_suggestion,
    dismiss_suggestion,
    get_orphans,
    get_suggestions,
)


def _seed_user_with_accounts(session):
    user = UserDB(id=uuid4(), email="t@x.com", username="t", password_hash="x")
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


def _make_pair(session, user_id, checking_id, amex_id, amount=100, out_date=date(2026, 2, 5), in_date=date(2026, 2, 4)):
    out = TransactionDB(
        id=uuid4(), user_id=user_id, account_id=checking_id,
        transaction_hash=str(uuid4()), source_type=SourceType.MANUAL,
        transaction_date=out_date, amount=Decimal(str(amount)),
        transaction_type=TransactionType.TRANSFER_OUT,
        description="ELECTRONICPMT AMEXEPAYMENT",
    )
    in_ = TransactionDB(
        id=uuid4(), user_id=user_id, account_id=amex_id,
        transaction_hash=str(uuid4()), source_type=SourceType.MANUAL,
        transaction_date=in_date, amount=Decimal(str(amount)),
        transaction_type=TransactionType.TRANSFER_IN,
        description="AUTOPAY PAYMENT",
    )
    session.add_all([out, in_])
    session.flush()
    session.commit()
    return out, in_


class TransfersAPIBase(unittest.TestCase):
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


class TestSuggestionsEndpoint(TransfersAPIBase):
    def test_lists_pair(self):
        out, in_ = _make_pair(self.session, self.user.db_id, self.checking.id, self.amex.id)
        result = get_suggestions(user_id=self.user.db_id, db=self.session)
        self.assertEqual(len(result), 1)
        s = result[0]
        self.assertEqual(s.out_side.id, out.id)
        self.assertEqual(s.in_side.id, in_.id)

    def test_empty_when_no_unpaired(self):
        # No transactions at all.
        self.assertEqual(get_suggestions(user_id=self.user.db_id, db=self.session), [])


class TestConfirmEndpoint(TransfersAPIBase):
    def test_creates_offsets_row(self):
        out, in_ = _make_pair(self.session, self.user.db_id, self.checking.id, self.amex.id)
        result = confirm_suggestion(
            ConfirmRequest(from_transaction_uuid=out.id, to_transaction_uuid=in_.id),
            user_id=self.user.db_id, db=self.session,
        )
        self.assertIn("relationship_id", result)
        rel = self.session.query(TransactionRelationshipDB).filter(
            TransactionRelationshipDB.relationship_type == RelationshipType.OFFSETS
        ).first()
        self.assertIsNotNone(rel)
        self.assertEqual(rel.from_transaction_id, out.db_id)
        self.assertEqual(rel.to_transaction_id, in_.db_id)

    def test_reclassify_from_updates_type_and_hash(self):
        # OUT side is PURCHASE; reclassify_from=True flips it to TRANSFER_OUT.
        from src.crud.crud_transaction import generate_transaction_hash

        out = TransactionDB(
            id=uuid4(), user_id=self.user.db_id, account_id=self.checking.id,
            transaction_hash=generate_transaction_hash(
                user_id=self.user.db_id, institution_name=self.checking.institution_name,
                transaction_date=date(2026, 2, 5), transaction_type_value="PURCHASE",
                amount=Decimal("100"), description="AMEXEPAYMENT",
            ),
            source_type=SourceType.MANUAL,
            transaction_date=date(2026, 2, 5), amount=Decimal("100"),
            transaction_type=TransactionType.PURCHASE, description="AMEXEPAYMENT",
        )
        in_ = TransactionDB(
            id=uuid4(), user_id=self.user.db_id, account_id=self.amex.id,
            transaction_hash=str(uuid4()), source_type=SourceType.MANUAL,
            transaction_date=date(2026, 2, 4), amount=Decimal("100"),
            transaction_type=TransactionType.TRANSFER_IN, description="AUTOPAY",
        )
        self.session.add_all([out, in_])
        self.session.commit()
        old_hash = out.transaction_hash

        confirm_suggestion(
            ConfirmRequest(
                from_transaction_uuid=out.id, to_transaction_uuid=in_.id,
                reclassify_from=True,
            ),
            user_id=self.user.db_id, db=self.session,
        )
        self.session.refresh(out)
        self.assertEqual(out.transaction_type, TransactionType.TRANSFER_OUT)
        self.assertNotEqual(out.transaction_hash, old_hash)

    def test_404_on_unknown_uuid(self):
        out, _ = _make_pair(self.session, self.user.db_id, self.checking.id, self.amex.id)
        with self.assertRaises(HTTPException) as cm:
            confirm_suggestion(
                ConfirmRequest(from_transaction_uuid=out.id, to_transaction_uuid=uuid4()),
                user_id=self.user.db_id, db=self.session,
            )
        self.assertEqual(cm.exception.status_code, 404)

    def test_reclassify_strips_needs_review_tag(self):
        from src.crud.crud_transaction import generate_transaction_hash

        ensure_system_tags(self.user.db_id, self.session)
        nr_tag = get_system_tag(self.user.db_id, self.session, "Needs Review")

        out = TransactionDB(
            id=uuid4(), user_id=self.user.db_id, account_id=self.checking.id,
            transaction_hash=generate_transaction_hash(
                user_id=self.user.db_id, institution_name=self.checking.institution_name,
                transaction_date=date(2026, 2, 5), transaction_type_value="PURCHASE",
                amount=Decimal("100"), description="AMEXEPAYMENT",
            ),
            source_type=SourceType.MANUAL,
            transaction_date=date(2026, 2, 5), amount=Decimal("100"),
            transaction_type=TransactionType.PURCHASE, description="AMEXEPAYMENT",
        )
        in_ = TransactionDB(
            id=uuid4(), user_id=self.user.db_id, account_id=self.amex.id,
            transaction_hash=str(uuid4()), source_type=SourceType.MANUAL,
            transaction_date=date(2026, 2, 4), amount=Decimal("100"),
            transaction_type=TransactionType.TRANSFER_IN, description="AUTOPAY",
        )
        self.session.add_all([out, in_])
        self.session.flush()
        self.session.add(TransactionTagDB(transaction_id=out.db_id, tag_id=nr_tag.tag_id))
        self.session.commit()

        confirm_suggestion(
            ConfirmRequest(
                from_transaction_uuid=out.id, to_transaction_uuid=in_.id,
                reclassify_from=True,
            ),
            user_id=self.user.db_id, db=self.session,
        )

        remaining = self.session.query(TransactionTagDB).filter(
            TransactionTagDB.transaction_id == out.db_id,
            TransactionTagDB.tag_id == nr_tag.tag_id,
        ).count()
        self.assertEqual(remaining, 0, "Needs Review tag should be stripped after reclassify")

    def test_confirm_without_reclassify_preserves_needs_review_tag(self):
        ensure_system_tags(self.user.db_id, self.session)
        nr_tag = get_system_tag(self.user.db_id, self.session, "Needs Review")

        out, in_ = _make_pair(self.session, self.user.db_id, self.checking.id, self.amex.id)
        self.session.add(TransactionTagDB(transaction_id=out.db_id, tag_id=nr_tag.tag_id))
        self.session.commit()

        confirm_suggestion(
            ConfirmRequest(from_transaction_uuid=out.id, to_transaction_uuid=in_.id),
            user_id=self.user.db_id, db=self.session,
        )

        remaining = self.session.query(TransactionTagDB).filter(
            TransactionTagDB.transaction_id == out.db_id,
            TransactionTagDB.tag_id == nr_tag.tag_id,
        ).count()
        self.assertEqual(remaining, 1, "Tag should be preserved when no reclassify happened")

    def test_investment_side_confirm_does_not_crash(self):
        """Investment transactions don't carry tags. The auto-strip path must
        skip them cleanly (regular-side guard) rather than fault."""
        schwab = AccountDB(
            uuid=uuid4(), user_id=self.user.db_id, account_name="Schwab Brokerage",
            account_type=AccountType.INVESTMENT, institution_name="Charles Schwab",
            balance=Decimal("0"),
        )
        self.session.add(schwab)
        self.session.flush()

        out = TransactionDB(
            id=uuid4(), user_id=self.user.db_id, account_id=self.checking.id,
            transaction_hash=str(uuid4()), source_type=SourceType.MANUAL,
            transaction_date=date(2026, 3, 10), amount=Decimal("500"),
            transaction_type=TransactionType.TRANSFER_OUT,
            description="SCHWAB BROKERAGE MONEYLINK",
        )
        inv_in = InvestmentTransactionDB(
            id=uuid4(), user_id=self.user.db_id, account_id=schwab.id,
            transaction_hash=str(uuid4()),
            transaction_type=InvestmentTransactionType.TRANSFER_IN,
            total_amount=Decimal("500"),
            transaction_date=date(2026, 3, 9),
            description="MoneyLink deposit",
        )
        self.session.add_all([out, inv_in])
        self.session.commit()

        result = confirm_suggestion(
            ConfirmRequest(from_transaction_uuid=out.id, to_transaction_uuid=inv_in.id),
            user_id=self.user.db_id, db=self.session,
        )
        self.assertIn("relationship_id", result)

    def test_409_when_already_linked(self):
        out, in_ = _make_pair(self.session, self.user.db_id, self.checking.id, self.amex.id)
        rel = TransactionRelationshipDB(
            id=uuid4(), relationship_type=RelationshipType.OFFSETS,
            from_transaction_id=out.db_id, to_transaction_id=in_.db_id,
        )
        self.session.add(rel)
        self.session.commit()

        with self.assertRaises(HTTPException) as cm:
            confirm_suggestion(
                ConfirmRequest(from_transaction_uuid=out.id, to_transaction_uuid=in_.id),
                user_id=self.user.db_id, db=self.session,
            )
        self.assertEqual(cm.exception.status_code, 409)


class TestDismissEndpoint(TransfersAPIBase):
    def test_persists_dismissal(self):
        out, in_ = _make_pair(self.session, self.user.db_id, self.checking.id, self.amex.id)
        dismiss_suggestion(
            DismissRequest(from_transaction_uuid=out.id, to_transaction_uuid=in_.id),
            user_id=self.user.db_id, db=self.session,
        )
        row = self.session.query(DismissedTransferPairDB).first()
        self.assertIsNotNone(row)
        self.assertEqual(row.from_transaction_id, out.db_id)
        self.assertEqual(row.to_transaction_id, in_.db_id)

    def test_dismissed_pair_excluded_from_suggestions(self):
        out, in_ = _make_pair(self.session, self.user.db_id, self.checking.id, self.amex.id)
        dismiss_suggestion(
            DismissRequest(from_transaction_uuid=out.id, to_transaction_uuid=in_.id),
            user_id=self.user.db_id, db=self.session,
        )
        result = get_suggestions(user_id=self.user.db_id, db=self.session)
        self.assertEqual(result, [])


class TestDeleteCascade(TransfersAPIBase):
    """Regression: dismissals and OFFSETS rows must not survive transaction
    deletion. Without cascade, SQLite's int-ID reuse (db_id is not
    AUTOINCREMENT) lets a dismissal from a deleted transaction silently
    suppress a freshly re-imported one."""

    def test_dismissal_cascades_on_transaction_delete(self):
        out, in_ = _make_pair(self.session, self.user.db_id, self.checking.id, self.amex.id)
        dismiss_suggestion(
            DismissRequest(from_transaction_uuid=out.id, to_transaction_uuid=in_.id),
            user_id=self.user.db_id, db=self.session,
        )
        self.assertEqual(self.session.query(DismissedTransferPairDB).count(), 1)

        self.session.delete(out)
        self.session.commit()
        self.assertEqual(
            self.session.query(DismissedTransferPairDB).count(),
            0,
            "Dismissal row should have been cascade-deleted with the transaction",
        )

    def test_offsets_cascades_on_transaction_delete(self):
        out, in_ = _make_pair(self.session, self.user.db_id, self.checking.id, self.amex.id)
        rel = TransactionRelationshipDB(
            id=uuid4(), relationship_type=RelationshipType.OFFSETS,
            from_transaction_id=out.db_id, to_transaction_id=in_.db_id,
        )
        self.session.add(rel)
        self.session.commit()
        self.assertEqual(
            self.session.query(TransactionRelationshipDB)
            .filter(TransactionRelationshipDB.relationship_type == RelationshipType.OFFSETS)
            .count(),
            1,
        )

        self.session.delete(out)
        self.session.commit()
        self.assertEqual(
            self.session.query(TransactionRelationshipDB)
            .filter(TransactionRelationshipDB.relationship_type == RelationshipType.OFFSETS)
            .count(),
            0,
            "OFFSETS row should have been cascade-deleted with the transaction",
        )

    def test_full_dismiss_delete_reimport_resurfaces(self):
        """The bug the frontend agent reported: dismiss pair, delete one
        side, recreate the same content (possibly reusing the same int
        db_id via SQLite ROWID reuse), then assert /transfers/suggestions
        re-surfaces the pair instead of leaving it silently dismissed."""
        out, in_ = _make_pair(self.session, self.user.db_id, self.checking.id, self.amex.id)
        original_out_db_id = out.db_id

        dismiss_suggestion(
            DismissRequest(from_transaction_uuid=out.id, to_transaction_uuid=in_.id),
            user_id=self.user.db_id, db=self.session,
        )
        # Confirm the dismissal hid the pair.
        self.assertEqual(get_suggestions(user_id=self.user.db_id, db=self.session), [])

        # Delete the OUT side and re-create with identical content. Use the
        # same description/date/amount so it would match the same pairing
        # heuristic. The new row gets a fresh UUID but the int db_id may be
        # reused depending on SQLite ROWID behavior.
        out_date = out.transaction_date
        out_amount = out.amount
        out_desc = out.description
        self.session.delete(out)
        self.session.commit()

        new_out = TransactionDB(
            id=uuid4(), user_id=self.user.db_id, account_id=self.checking.id,
            transaction_hash=str(uuid4()), source_type=SourceType.MANUAL,
            transaction_date=out_date, amount=out_amount,
            transaction_type=TransactionType.TRANSFER_OUT, description=out_desc,
        )
        self.session.add(new_out)
        self.session.commit()

        # SANITY: the bug only manifests when db_id is reused; but the FIX is
        # correct regardless. We assert correctness of the fix (no stale
        # dismissal) without depending on whether reuse actually happened.
        suggestions = get_suggestions(user_id=self.user.db_id, db=self.session)
        self.assertEqual(
            len(suggestions),
            1,
            "Re-created transaction should re-surface as a suggestion; "
            "stale dismissal must not have survived the delete.",
        )
        self.assertEqual(suggestions[0].out_side.id, new_out.id)


class TestOrphansEndpoint(TransfersAPIBase):
    def test_orphan_with_no_partner(self):
        out = TransactionDB(
            id=uuid4(), user_id=self.user.db_id, account_id=self.checking.id,
            transaction_hash=str(uuid4()), source_type=SourceType.MANUAL,
            transaction_date=date(2026, 1, 1), amount=Decimal("250"),
            transaction_type=TransactionType.TRANSFER_OUT,
            description="LONELY TRANSFER",
        )
        self.session.add(out)
        self.session.commit()

        result = get_orphans(user_id=self.user.db_id, db=self.session)
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].id, out.id)


if __name__ == "__main__":
    unittest.main()
