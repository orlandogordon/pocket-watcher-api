"""Background worker for bulk statement imports (#59).

``process_batch(db, batch_id)`` is the pure logic — tests call it directly on the
test session. ``submit_bulk_import(batch_id)`` is the threading wrapper that opens
its own ``SessionLocal`` (the request that enqueued the batch has already
returned), mirroring ``job_runner``'s ThreadJobRunner.
"""
from __future__ import annotations

import threading
from datetime import datetime

from sqlalchemy import func

from src.db.core import (
    session_local as SessionLocal,
    BulkImportBatchDB,
    UploadJobDB,
    TransactionDB,
    InvestmentTransactionDB,
)
from src.services import bulk_import
from src.services.file_storage import get_storage
from src.services.account_snapshot import trigger_backfill_if_needed
from src.logging_config import get_logger

logger = get_logger(__name__)


def _earliest_transaction_date(db, account_id: int):
    regular = db.query(func.min(TransactionDB.transaction_date)).filter(
        TransactionDB.account_id == account_id
    ).scalar()
    investment = db.query(func.min(InvestmentTransactionDB.transaction_date)).filter(
        InvestmentTransactionDB.account_id == account_id
    ).scalar()
    candidates = [d for d in (regular, investment) if d is not None]
    return min(candidates) if candidates else None


def process_batch(db, batch_id: int) -> None:
    """Import every file in the batch, updating progress per file, then backfill
    snapshots for each affected account. Per-file errors are recorded on the
    child row and do not abort the batch."""
    batch = db.get(BulkImportBatchDB, batch_id)
    if batch is None:
        logger.error("bulk batch %s not found", batch_id)
        return

    batch.status = "IN_PROGRESS"
    db.commit()

    children = (
        db.query(UploadJobDB)
        .filter(UploadJobDB.batch_id == batch_id)
        .order_by(UploadJobDB.id)
        .all()
    )
    storage = get_storage()
    affected_accounts: set[int] = set()
    cancelled = False

    for job in children:
        db.refresh(batch)
        if batch.status == "CANCELLED":
            cancelled = True
            break

        job.status = "PROCESSING"
        job.started_at = datetime.utcnow()
        db.commit()

        try:
            with storage.open(job.storage_key) as fh:
                file_bytes = fh.read()
            result = bulk_import.process_file(
                db,
                file_bytes=file_bytes,
                filename=job.file_path or "",
                institution=job.institution,
                account_id=job.account_id,
                user_id=job.user_id,
                upload_job_id=job.id,
            )
            job.transactions_created = result.transactions_created
            job.transactions_skipped = result.transactions_skipped
            job.investment_transactions_created = result.investments_created
            job.investment_transactions_skipped = result.investments_skipped
            job.needs_review = result.needs_review
            job.status = "COMPLETED" if result.ok else "FAILED"
            job.error_message = result.error
            if result.ok and job.account_id is not None:
                affected_accounts.add(job.account_id)
        except Exception as e:  # storage/read failure — isolate to this file
            db.rollback()
            job.status = "FAILED"
            job.error_message = str(e)
            logger.error("bulk file %s failed: %s", job.uuid, e, exc_info=True)

        job.completed_at = datetime.utcnow()
        batch.processed_files += 1
        db.commit()

    for account_id in affected_accounts:
        earliest = _earliest_transaction_date(db, account_id)
        if earliest is not None:
            # user_id is the batch owner; all children share it
            trigger_backfill_if_needed(db, batch.user_id, account_id, earliest)

    batch.status = "CANCELLED" if cancelled else "COMPLETED"
    batch.completed_at = datetime.utcnow()
    db.commit()
    logger.info("bulk batch %s %s (%d files)", batch.uuid, batch.status, batch.processed_files)


def _run(batch_id: int) -> None:
    db = SessionLocal()
    try:
        process_batch(db, batch_id)
    finally:
        db.close()


def submit_bulk_import(batch_id: int) -> None:
    """Spawn a daemon thread to import the batch (its own DB session)."""
    thread = threading.Thread(target=_run, args=(batch_id,), daemon=True)
    thread.start()
    logger.info("submitted bulk import batch %s", batch_id)
