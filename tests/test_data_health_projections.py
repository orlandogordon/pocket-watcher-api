"""Unit tests for the data-health projection helpers."""
import unittest
from datetime import date, datetime
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
from src.services.data_health import (
    project_needs_review,
    project_snapshot_review,
    project_transfer_orphans,
    project_transfer_pairs,
)
from src.services.system_tags import ensure_system_tags, get_system_tag


def _seed(session):
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


class ProjectionBase(unittest.TestCase):
    def setUp(self):
        self.engine = create_engine("sqlite:///:memory:")
        Base.metadata.create_all(self.engine)
        self.Session = sessionmaker(bind=self.engine)
        self.session = self.Session()
        self.user, self.checking, self.amex = _seed(self.session)

    def tearDown(self):
        self.session.close()
        Base.metadata.drop_all(self.engine)
        self.engine.dispose()


class TestProjectNeedsReview(ProjectionBase):
    def test_empty_when_no_tag(self):
        self.assertEqual(project_needs_review(self.session, self.user.db_id), [])

    def test_single_row_projection(self):
        ensure_system_tags(self.user.db_id, self.session)
        tag = get_system_tag(self.user.db_id, self.session, "Needs Review")
        txn = TransactionDB(
            id=uuid4(), user_id=self.user.db_id, account_id=self.checking.id,
            transaction_hash=str(uuid4()), source_type=SourceType.MANUAL,
            transaction_date=date(2026, 4, 1), amount=Decimal("12.50"),
            transaction_type=TransactionType.PURCHASE, description="Starbucks",
            merchant_name="Starbucks Coffee",
        )
        self.session.add(txn)
        self.session.flush()
        self.session.add(TransactionTagDB(transaction_id=txn.db_id, tag_id=tag.tag_id))
        self.session.commit()

        items = project_needs_review(self.session, self.user.db_id)
        self.assertEqual(len(items), 1)
        item = items[0]
        self.assertEqual(item.id, f"needs_review:{txn.id}")
        self.assertEqual(item.kind, "needs_review")
        self.assertEqual(item.severity, "action_required")
        self.assertEqual(item.subject.type, "transaction")
        self.assertEqual(item.subject.primary_uuid, txn.id)
        self.assertIsNone(item.confidence)
        self.assertEqual(len(item.actions), 1)
        self.assertEqual(item.actions[0].method, "DELETE")
        self.assertIn(str(txn.id), item.actions[0].href)
        self.assertIn(str(tag.id), item.actions[0].href)
        # Detail-enrichment fields the frontend inbox table depends on.
        self.assertEqual(item.details["merchant_name"], "Starbucks Coffee")
        self.assertEqual(item.details["account_uuid"], str(self.checking.uuid))
        self.assertEqual(item.details["account_name"], "TD Main Checking")

    def test_merchant_name_null_is_preserved(self):
        """merchant_name being null is often *why* the row is flagged —
        the frontend reads null to mean 'no merchant suggested yet'."""
        ensure_system_tags(self.user.db_id, self.session)
        tag = get_system_tag(self.user.db_id, self.session, "Needs Review")
        txn = TransactionDB(
            id=uuid4(), user_id=self.user.db_id, account_id=self.checking.id,
            transaction_hash=str(uuid4()), source_type=SourceType.MANUAL,
            transaction_date=date(2026, 4, 1), amount=Decimal("12.50"),
            transaction_type=TransactionType.PURCHASE, description="POS DEBIT 8472",
        )
        self.session.add(txn)
        self.session.flush()
        self.session.add(TransactionTagDB(transaction_id=txn.db_id, tag_id=tag.tag_id))
        self.session.commit()

        items = project_needs_review(self.session, self.user.db_id)
        self.assertIsNone(items[0].details["merchant_name"])


class TestProjectTransferPairs(ProjectionBase):
    def test_empty(self):
        self.assertEqual(project_transfer_pairs(self.session, self.user.db_id), [])

    def test_pair_surfaces_with_confidence(self):
        out = TransactionDB(
            id=uuid4(), user_id=self.user.db_id, account_id=self.checking.id,
            transaction_hash=str(uuid4()), source_type=SourceType.MANUAL,
            transaction_date=date(2026, 2, 5), amount=Decimal("100"),
            transaction_type=TransactionType.TRANSFER_OUT,
            description="ELECTRONICPMT AMEXEPAYMENT",
        )
        in_ = TransactionDB(
            id=uuid4(), user_id=self.user.db_id, account_id=self.amex.id,
            transaction_hash=str(uuid4()), source_type=SourceType.MANUAL,
            transaction_date=date(2026, 2, 4), amount=Decimal("100"),
            transaction_type=TransactionType.TRANSFER_IN, description="AUTOPAY",
        )
        self.session.add_all([out, in_])
        self.session.commit()

        items = project_transfer_pairs(self.session, self.user.db_id)
        self.assertEqual(len(items), 1)
        item = items[0]
        self.assertEqual(item.kind, "transfer_pair")
        self.assertEqual(item.severity, "suggested")
        self.assertEqual(item.subject.type, "transfer_pair")
        self.assertEqual(item.subject.primary_uuid, out.id)
        self.assertEqual(item.subject.partner_uuid, in_.id)
        self.assertIn(item.confidence, ("HIGH", "MEDIUM"))
        labels = {a.label for a in item.actions}
        self.assertEqual(labels, {"Confirm pair", "Dismiss"})
        # Detail-enrichment fields the frontend inbox table depends on.
        self.assertEqual(item.details["out_description"], "ELECTRONICPMT AMEXEPAYMENT")
        self.assertEqual(item.details["in_description"], "AUTOPAY")
        self.assertEqual(item.details["out_account_uuid"], str(self.checking.uuid))
        self.assertEqual(item.details["out_account_name"], "TD Main Checking")
        self.assertEqual(item.details["in_account_uuid"], str(self.amex.uuid))
        self.assertEqual(item.details["in_account_name"], "Amex Gold")

    def test_confirm_body_includes_reclassify_flags(self):
        """Both sides already typed correctly → both flags are False
        (confirm is a pure OFFSETS link, no type change)."""
        out = TransactionDB(
            id=uuid4(), user_id=self.user.db_id, account_id=self.checking.id,
            transaction_hash=str(uuid4()), source_type=SourceType.MANUAL,
            transaction_date=date(2026, 2, 5), amount=Decimal("100"),
            transaction_type=TransactionType.TRANSFER_OUT, description="AMEX",
        )
        in_ = TransactionDB(
            id=uuid4(), user_id=self.user.db_id, account_id=self.amex.id,
            transaction_hash=str(uuid4()), source_type=SourceType.MANUAL,
            transaction_date=date(2026, 2, 4), amount=Decimal("100"),
            transaction_type=TransactionType.TRANSFER_IN, description="AUTOPAY",
        )
        self.session.add_all([out, in_])
        self.session.commit()

        items = project_transfer_pairs(self.session, self.user.db_id)
        confirm = next(a for a in items[0].actions if a.label == "Confirm pair")
        self.assertEqual(confirm.body["reclassify_from"], False)
        self.assertEqual(confirm.body["reclassify_to"], False)
        # Dismiss action does not carry reclassify flags.
        dismiss = next(a for a in items[0].actions if a.label == "Dismiss")
        self.assertNotIn("reclassify_from", dismiss.body)
        self.assertNotIn("reclassify_to", dismiss.body)


class TestProjectTransferOrphans(ProjectionBase):
    def test_empty(self):
        self.assertEqual(project_transfer_orphans(self.session, self.user.db_id), [])

    def test_orphan_has_no_actions(self):
        out = TransactionDB(
            id=uuid4(), user_id=self.user.db_id, account_id=self.checking.id,
            transaction_hash=str(uuid4()), source_type=SourceType.MANUAL,
            transaction_date=date(2026, 1, 1), amount=Decimal("250"),
            transaction_type=TransactionType.TRANSFER_OUT,
            description="LONELY TRANSFER",
        )
        self.session.add(out)
        self.session.commit()

        items = project_transfer_orphans(self.session, self.user.db_id)
        self.assertEqual(len(items), 1)
        item = items[0]
        self.assertEqual(item.id, f"transfer_orphan:{out.id}")
        self.assertEqual(item.kind, "transfer_orphan")
        self.assertEqual(item.severity, "informational")
        self.assertEqual(item.subject.primary_uuid, out.id)
        self.assertEqual(item.actions, [])
        # Detail-enrichment fields the frontend inbox table depends on.
        self.assertEqual(item.details["transaction_type"], "TRANSFER_OUT")
        self.assertEqual(item.details["account_uuid"], str(self.checking.uuid))
        self.assertEqual(item.details["account_name"], "TD Main Checking")


class TestProjectSnapshotReview(ProjectionBase):
    def test_empty(self):
        self.assertEqual(project_snapshot_review(self.session, self.user.db_id), [])

    def test_flagged_snapshot_projects(self):
        snap = AccountValueHistoryDB(
            uuid=uuid4(), account_id=self.checking.id,
            value_date=date(2024, 1, 1), balance=Decimal("500"),
            needs_review=True, review_reason="before earliest transaction",
            created_at=datetime.utcnow(),
        )
        self.session.add(snap)
        self.session.commit()

        items = project_snapshot_review(self.session, self.user.db_id)
        self.assertEqual(len(items), 1)
        item = items[0]
        self.assertEqual(item.id, f"snapshot_review:{snap.uuid}")
        self.assertEqual(item.kind, "snapshot_review")
        self.assertEqual(item.severity, "informational")
        self.assertEqual(item.subject.type, "snapshot")
        self.assertEqual(item.subject.primary_uuid, snap.uuid)
        self.assertEqual(item.details["review_reason"], "before earliest transaction")
        self.assertEqual(len(item.actions), 1)
        self.assertEqual(item.actions[0].method, "POST")
        self.assertIn(str(self.checking.uuid), item.actions[0].href)

    def test_other_users_snapshot_not_returned(self):
        other = UserDB(id=uuid4(), email="o@x.com", username="o", password_hash="x")
        self.session.add(other)
        self.session.flush()
        other_acct = AccountDB(
            uuid=uuid4(), user_id=other.db_id, account_name="Other Checking",
            account_type=AccountType.CHECKING, institution_name="X",
            balance=Decimal("0"),
        )
        self.session.add(other_acct)
        self.session.flush()
        snap = AccountValueHistoryDB(
            uuid=uuid4(), account_id=other_acct.id,
            value_date=date(2024, 1, 1), balance=Decimal("500"),
            needs_review=True, created_at=datetime.utcnow(),
        )
        self.session.add(snap)
        self.session.commit()

        self.assertEqual(project_snapshot_review(self.session, self.user.db_id), [])


if __name__ == "__main__":
    unittest.main()
