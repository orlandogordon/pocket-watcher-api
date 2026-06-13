"""Direct tests for the shared import pipeline (src.services.bulk_import).

Drives process_file with the synthetic Amex CSV fixture (no real data) against
the in-memory DB, with the LLM faked to its default null suggestions. This is
the auto-accept path that the bulk-upload job (#59) and the local seed scripts
share, so the per-file counts + dedup + Needs-Review tagging are pinned here
independent of the HTTP layer.
"""
from datetime import date
from decimal import Decimal
from pathlib import Path

from src.crud.crud_transaction import bulk_create_transactions_from_parsed_data
from src.db.core import AccountType, TransactionDB, TransactionTagDB, TransactionType
from src.parser.models import ParsedTransaction
from src.services import bulk_import
from src.services.description_cleanup import CleanedResult
from src.services.system_tags import get_system_tag, ensure_system_tags
from tests.factories import make_account, make_transaction

_FIXTURE = Path(__file__).parent / "parsers" / "fixtures" / "amex_sample.csv"
_CSV_BYTES = _FIXTURE.read_bytes()


def _amex_account(db, user):
    return make_account(
        db, user,
        account_name="Amex Platinum",
        account_type=AccountType.CREDIT_CARD,
        institution_name="Amex",
    )


def test_process_file_imports_amex_csv(db, test_user, fake_llm):
    account = _amex_account(db, test_user)

    result = bulk_import.process_file(
        db,
        file_bytes=_CSV_BYTES,
        filename="amex_sample.csv",
        institution="amex",
        account_id=account.db_id,
        user_id=test_user.db_id,
    )

    assert result.ok
    # 3 purchases + 1 AUTOPAY credit in the fixture.
    assert result.transactions_created == 4
    assert result.transactions_skipped == 0
    assert result.investments_created == 0

    rows = (
        db.query(TransactionDB)
        .filter(TransactionDB.account_id == account.db_id)
        .all()
    )
    assert len(rows) == 4

    # fake_llm returns a null category for every row, so all four are flagged
    # Needs Review (the trigger is null category OR null merchant).
    assert result.needs_review == 4
    review_tag = get_system_tag(test_user.db_id, db, "Needs Review")
    tagged = (
        db.query(TransactionTagDB)
        .filter(TransactionTagDB.tag_id == review_tag.db_id)
        .count()
    )
    assert tagged == 4
    # Each flagged row records WHY in its comments (#68) — same note the
    # preview/confirm path writes, now shared via system_tags.append_review_note.
    assert all("Auto-flagged for review:" in (r.comments or "") for r in rows)


def test_bulk_flagging_exempts_transfers_and_writes_reason(db, test_user):
    """#68: the bulk path mirrors preview/confirm — transfers are exempt from the
    category-null Needs-Review heuristic, and flagged rows get a reason in
    ``comments``. Drives the internal apply step directly with two null-category
    rows so the transfer vs non-transfer split is isolated."""
    account = _amex_account(db, test_user)
    ensure_system_tags(test_user.db_id, db)

    purchase = make_transaction(
        db, test_user, account, transaction_type=TransactionType.PURCHASE,
        description="DEBIT THING", transaction_date=date(2026, 2, 1),
        amount=Decimal("10.00"), category_id=None, merchant_name=None, comments=None,
    )
    transfer = make_transaction(
        db, test_user, account, transaction_type=TransactionType.TRANSFER_IN,
        description="TRANSFER MONEY", transaction_date=date(2026, 2, 2),
        amount=Decimal("20.00"), category_id=None, merchant_name=None, comments=None,
    )
    parsed = [
        ParsedTransaction(transaction_date=date(2026, 2, 1), description="DEBIT THING",
                          amount=Decimal("10.00"), transaction_type="PURCHASE"),
        ParsedTransaction(transaction_date=date(2026, 2, 2), description="TRANSFER MONEY",
                          amount=Decimal("20.00"), transaction_type="TRANSFER_IN"),
    ]
    # LLM declined / fell through: null suggestion + null merchant for both.
    results = [
        CleanedResult(raw="DEBIT THING", cleaned="DEBIT THING",
                      source="raw_fallthrough", llm_suggestion=None, merchant_name=None),
        CleanedResult(raw="TRANSFER MONEY", cleaned="TRANSFER MONEY",
                      source="raw_fallthrough", llm_suggestion=None, merchant_name=None),
    ]

    bulk_import._apply_cleanup_to_created(
        db, test_user.db_id, [purchase, transfer], parsed, results, {}, True
    )
    db.flush()

    review_tag = get_system_tag(test_user.db_id, db, "Needs Review")
    tagged_ids = {
        r.transaction_id for r in db.query(TransactionTagDB)
        .filter(TransactionTagDB.tag_id == review_tag.db_id).all()
    }
    # PURCHASE flagged + reason recorded; TRANSFER exempt + comments untouched.
    assert purchase.db_id in tagged_ids
    assert "Auto-flagged for review:" in (purchase.comments or "")
    assert transfer.db_id not in tagged_ids
    assert transfer.comments is None


def test_process_file_dedups_on_reupload(db, test_user, fake_llm):
    account = _amex_account(db, test_user)
    kwargs = dict(
        file_bytes=_CSV_BYTES,
        filename="amex_sample.csv",
        institution="amex",
        account_id=account.db_id,
        user_id=test_user.db_id,
    )

    first = bulk_import.process_file(db, **kwargs)
    assert first.transactions_created == 4

    second = bulk_import.process_file(db, **kwargs)
    assert second.ok
    assert second.transactions_created == 0
    assert second.transactions_skipped == 4
    assert db.query(TransactionDB).filter(TransactionDB.account_id == account.db_id).count() == 4


def test_process_file_unknown_institution_returns_error(db, test_user, fake_llm):
    account = _amex_account(db, test_user)

    result = bulk_import.process_file(
        db,
        file_bytes=_CSV_BYTES,
        filename="amex_sample.csv",
        institution="not-a-real-bank",
        account_id=account.db_id,
        user_id=test_user.db_id,
    )

    assert not result.ok
    assert "parser" in result.error.lower()
    assert db.query(TransactionDB).filter(TransactionDB.account_id == account.db_id).count() == 0


def test_process_file_marks_degraded_when_llm_unavailable(db, test_user, monkeypatch):
    """When the LLM backend is unreachable, rows still import (un-enriched) but
    the per-file result is flagged degraded (#60)."""
    from src.services import description_cleanup
    from src.services.llm_client import LLMUnavailableError

    class _DownLLM:
        model_name = "down"

        def process_transaction_batch(self, parsed):
            raise LLMUnavailableError("offline")

        def health_check(self):
            return (False, None)

    monkeypatch.setattr(description_cleanup, "get_llm_client", lambda: _DownLLM())

    account = _amex_account(db, test_user)
    result = bulk_import.process_file(
        db,
        file_bytes=_CSV_BYTES,
        filename="amex_sample.csv",
        institution="amex",
        account_id=account.db_id,
        user_id=test_user.db_id,
    )

    assert result.ok
    assert result.transactions_created == 4
    assert result.degraded is True


def test_process_file_flags_reconciliation_warning(db, test_user, fake_llm, monkeypatch):
    """A statement whose rows don't reconcile to its control totals still imports
    (non-blocking), but the per-file result carries the off-by delta/detail (#78)."""
    from src.parser.models import ParsedData, ParsedTransaction, ReconciliationResult

    class _FakeParser:
        def parse(self, file_obj, is_csv=False):
            return ParsedData(
                transactions=[
                    ParsedTransaction(
                        transaction_date=date(2026, 5, 1), description="COFFEE",
                        amount=Decimal("10.00"), transaction_type="PURCHASE",
                    ),
                ],
                reconciliation=ReconciliationResult(
                    reconciled=False,
                    expected_net_change=Decimal("-100.00"),
                    parsed_net=Decimal("10.00"),
                    delta=Decimal("110.00"),
                    detail="off by +110.00",
                ),
            )

    monkeypatch.setitem(bulk_import.PARSER_MAPPING, "fakebank", _FakeParser())
    account = _amex_account(db, test_user)
    result = bulk_import.process_file(
        db, file_bytes=b"ignored", filename="stmt.pdf", institution="fakebank",
        account_id=account.db_id, user_id=test_user.db_id,
    )

    assert result.ok
    assert result.transactions_created == 1  # rows still import despite the mismatch
    assert result.reconciliation_warning is True
    assert result.reconciliation_delta == Decimal("110.00")
    assert "off by" in result.reconciliation_detail


def _mta_pair():
    """Two genuinely identical same-day rows (e.g. two MTA pay-go taps)."""
    return [
        ParsedTransaction(transaction_date=date(2026, 3, 1), description="MTA PAYGO",
                          amount=Decimal("2.90"), transaction_type="PURCHASE"),
        ParsedTransaction(transaction_date=date(2026, 3, 1), description="MTA PAYGO",
                          amount=Decimal("2.90"), transaction_type="PURCHASE"),
    ]


def test_within_statement_duplicate_pair_gets_distinct_hashes(db, test_user):
    """#73: two genuinely identical rows in one file are both kept (auto-accept)
    but stored with DISTINCT transaction_hash, mirroring the preview path — so
    the bulk path no longer emits colliding hashes for within-statement repeats."""
    account = _amex_account(db, test_user)

    created, skipped = bulk_create_transactions_from_parsed_data(
        db, test_user.db_id, _mta_pair(), account.db_id,
    )

    assert len(created) == 2
    assert len(skipped) == 0
    hashes = {t.transaction_hash for t in created}
    assert len(hashes) == 2, "within-statement duplicates must get distinct hashes"


def test_process_file_within_statement_pair_distinct_hashes(db, test_user, fake_llm):
    """#73 end-to-end: a statement whose parse yields a genuine same-day identical
    pair imports BOTH rows through the real process_file path (parser doesn't
    collapse them) with DISTINCT transaction hashes."""
    account = _amex_account(db, test_user)
    csv_bytes = (
        b"Date,Description,Amount\n"
        b"05/01/2026,MTA PAYGO NEW YORK NY,2.90\n"
        b"05/01/2026,MTA PAYGO NEW YORK NY,2.90\n"
    )

    result = bulk_import.process_file(
        db,
        file_bytes=csv_bytes,
        filename="dup_pair.csv",
        institution="amex",
        account_id=account.db_id,
        user_id=test_user.db_id,
    )

    assert result.ok
    assert result.transactions_created == 2
    assert result.transactions_skipped == 0

    rows = (
        db.query(TransactionDB)
        .filter(TransactionDB.account_id == account.db_id)
        .all()
    )
    assert len(rows) == 2
    assert len({r.transaction_hash for r in rows}) == 2, \
        "within-statement duplicates must get distinct hashes"


def test_within_statement_pair_reupload_is_net_zero(db, test_user):
    """#73: re-importing the same statement (including its within-statement pair)
    skips every row — the duplicate's base hash is now in the DB snapshot, so no
    new rows and none restored."""
    account = _amex_account(db, test_user)

    first_created, _ = bulk_create_transactions_from_parsed_data(
        db, test_user.db_id, _mta_pair(), account.db_id,
    )
    assert len(first_created) == 2

    second_created, second_skipped = bulk_create_transactions_from_parsed_data(
        db, test_user.db_id, _mta_pair(), account.db_id,
    )
    assert len(second_created) == 0
    assert len(second_skipped) == 2
    assert db.query(TransactionDB).filter(
        TransactionDB.account_id == account.db_id
    ).count() == 2
