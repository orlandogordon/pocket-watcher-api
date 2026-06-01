"""Direct tests for the shared import pipeline (src.services.bulk_import).

Drives process_file with the synthetic Amex CSV fixture (no real data) against
the in-memory DB, with the LLM faked to its default null suggestions. This is
the auto-accept path that the bulk-upload job (#59) and the local seed scripts
share, so the per-file counts + dedup + Needs-Review tagging are pinned here
independent of the HTTP layer.
"""
from pathlib import Path

from src.db.core import AccountType, TransactionDB, TransactionTagDB
from src.services import bulk_import
from src.services.system_tags import get_system_tag
from tests.factories import make_account

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
