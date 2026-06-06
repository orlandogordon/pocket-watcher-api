"""Tests for the scheduled EOD snapshot job (#63).

Exercises the testable core `run_eod_snapshots(db, ...)` and `main()`'s exit
code. Uses checking-only users so `update_investment_prices` short-circuits and
no yfinance/network call is made.
"""
from datetime import date
from decimal import Decimal

import pytest

from src.db.core import AccountType, AccountValueHistoryDB
from src.jobs import eod_snapshot
from tests.factories import make_account, make_user

MONDAY = date(2026, 1, 5)
SATURDAY = date(2026, 1, 3)


def _snapshot_count(db, account_id) -> int:
    return db.query(AccountValueHistoryDB).filter(
        AccountValueHistoryDB.account_id == account_id
    ).count()


def test_creates_snapshots_for_user_accounts(db, test_user):
    acct = make_account(db, test_user, balance=Decimal("250.00"))

    summary = eod_snapshot.run_eod_snapshots(db, MONDAY, user_id=test_user.db_id)

    assert summary["errors"] == 0
    assert summary["snapshots"] == 1
    assert _snapshot_count(db, acct.db_id) == 1


def test_rerun_is_idempotent(db, test_user):
    acct = make_account(db, test_user, balance=Decimal("100.00"))

    eod_snapshot.run_eod_snapshots(db, MONDAY, user_id=test_user.db_id)
    eod_snapshot.run_eod_snapshots(db, MONDAY, user_id=test_user.db_id)

    assert _snapshot_count(db, acct.db_id) == 1


def test_weekend_skip_is_a_noop(db, test_user):
    acct = make_account(db, test_user, balance=Decimal("100.00"))

    summary = eod_snapshot.run_eod_snapshots(
        db, SATURDAY, user_id=test_user.db_id, skip_weekends=True
    )

    assert summary == {"users": 0, "snapshots": 0, "errors": 0}
    assert _snapshot_count(db, acct.db_id) == 0


def test_per_user_failure_is_isolated_and_counted(db, test_user, monkeypatch):
    make_account(db, test_user, balance=Decimal("100.00"))

    def boom(**kwargs):
        raise RuntimeError("price feed exploded")

    monkeypatch.setattr(eod_snapshot, "create_all_account_snapshots", boom)

    summary = eod_snapshot.run_eod_snapshots(db, MONDAY, user_id=test_user.db_id)

    assert summary["errors"] == 1
    assert summary["snapshots"] == 0


def test_unknown_user_is_noop(db):
    summary = eod_snapshot.run_eod_snapshots(db, MONDAY, user_id=999999)
    assert summary == {"users": 0, "snapshots": 0, "errors": 0}


def test_main_exits_nonzero_on_user_error(db, test_user, monkeypatch):
    make_account(db, test_user, balance=Decimal("100.00"))

    monkeypatch.setattr(eod_snapshot, "session_local", lambda: db)
    monkeypatch.setattr(db, "close", lambda: None)
    monkeypatch.setattr(eod_snapshot, "create_all_account_snapshots",
                        lambda **kw: (_ for _ in ()).throw(RuntimeError("boom")))
    monkeypatch.setattr("sys.argv", ["eod_snapshot", "--user-id", str(test_user.db_id)])

    assert eod_snapshot.main() == 1


def test_main_exits_zero_on_success(db, test_user, monkeypatch):
    make_account(db, test_user, balance=Decimal("100.00"))

    monkeypatch.setattr(eod_snapshot, "session_local", lambda: db)
    monkeypatch.setattr(db, "close", lambda: None)
    monkeypatch.setattr("sys.argv", ["eod_snapshot", "--user-id", str(test_user.db_id)])

    assert eod_snapshot.main() == 0
