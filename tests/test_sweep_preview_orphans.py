"""Tests for the preview-orphan storage sweep (#59 follow-up).

Uses the autouse `_isolated_storage` conftest fixture (per-test tmp LocalStorage).
File ages are controlled with os.utime on the resolved path.
"""
import os
import time
from uuid import uuid4

from src.db.core import AccountType, UploadJobDB
from src.services import file_storage
from src.jobs.sweep_preview_orphans import sweep_orphans
from tests.factories import make_account


def _backdate(store, key, hours):
    old = time.time() - hours * 3600
    os.utime(store.root / key, (old, old))


def test_sweep_deletes_old_orphan_keeps_referenced_and_recent(db, test_user):
    store = file_storage.get_storage()
    acct = make_account(db, test_user, account_type=AccountType.CREDIT_CARD)

    # Referenced by an UploadJobDB (even though old) — must be kept.
    ref_key = file_storage.build_key(test_user.db_id, uuid4(), "ref.pdf")
    store.save(b"ref", ref_key)
    _backdate(store, ref_key, 48)
    db.add(UploadJobDB(
        uuid=uuid4(), user_id=test_user.db_id, account_id=acct.id,
        institution="amex", status="COMPLETED", storage_key=ref_key,
    ))
    db.commit()

    # Old orphan (no DB row) — must be deleted.
    old_orphan = file_storage.build_key(test_user.db_id, uuid4(), "old.pdf")
    store.save(b"old", old_orphan)
    _backdate(store, old_orphan, 48)

    # Recent orphan (no DB row but fresh) — an in-flight preview, must be kept.
    recent_orphan = file_storage.build_key(test_user.db_id, uuid4(), "new.pdf")
    store.save(b"new", recent_orphan)

    summary = sweep_orphans(db, min_age_hours=13, dry_run=False)

    assert store.exists(ref_key)
    assert not store.exists(old_orphan)
    assert store.exists(recent_orphan)
    assert summary.referenced == 1
    assert summary.orphaned == 1
    assert summary.deleted == 1


def test_sweep_dry_run_reports_but_deletes_nothing(db, test_user):
    store = file_storage.get_storage()
    key = file_storage.build_key(test_user.db_id, uuid4(), "o.pdf")
    store.save(b"x", key)
    _backdate(store, key, 48)

    summary = sweep_orphans(db, min_age_hours=13, dry_run=True)

    assert summary.orphaned == 1
    assert summary.deleted == 0
    assert store.exists(key)
