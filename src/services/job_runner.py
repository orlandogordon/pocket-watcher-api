"""
Async Job System for Snapshot Backfill

Provides threading-based job execution with concurrency limiting.
Abstraction layer allows future upgrade to Celery without code changes.
"""
import threading
import os
from abc import ABC, abstractmethod
from typing import Optional
from datetime import date, datetime, timedelta
from sqlalchemy.orm import Session

from src.db.core import session_local as SessionLocal, SnapshotBackfillJobDB
from src.services.account_snapshot import recalculate_account_snapshots
from src.logging_config import get_logger

logger = get_logger(__name__)

# Global semaphore - max 10 concurrent backfill jobs
MAX_CONCURRENT_BACKFILLS = 10
backfill_semaphore = threading.Semaphore(MAX_CONCURRENT_BACKFILLS)


class JobRunner(ABC):
    """Abstract interface for running background jobs"""

    @abstractmethod
    def submit_job(self, job_id: int, account_id: int, start_date: date, end_date: date):
        """Submit a job for execution"""
        raise NotImplementedError


class ThreadJobRunner(JobRunner):
    """Simple threading implementation"""

    def submit_job(self, job_id: int, account_id: int, start_date: date, end_date: date):
        thread = threading.Thread(
            target=run_backfill_worker,
            args=(job_id, account_id, start_date, end_date)
        )
        thread.daemon = True
        thread.start()
        logger.info(f"Submitted backfill job {job_id} to thread pool")


class CeleryJobRunner(JobRunner):
    """Celery implementation (future upgrade)"""

    def submit_job(self, job_id: int, account_id: int, start_date: date, end_date: date):
        # Placeholder for Celery integration
        # When implemented, this will call:
        # from src.tasks import backfill_task
        # backfill_task.delay(job_id, account_id, start_date, end_date)
        raise NotImplementedError("Celery job runner not yet implemented. Use 'thread' mode.")


# Configuration
JOB_RUNNER_TYPE = os.getenv('JOB_RUNNER', 'thread')  # 'thread' or 'celery'


def get_job_runner() -> JobRunner:
    """Get configured job runner"""
    if JOB_RUNNER_TYPE == 'celery':
        return CeleryJobRunner()
    return ThreadJobRunner()


def run_backfill_worker(job_id: int, account_id: int, start_date: date, end_date: date):
    """
    Background worker function that executes the backfill.

    Runs with global concurrency limit (semaphore).
    Updates job status throughout execution.
    """
    # Acquire semaphore (blocks if 10 jobs already running)
    with backfill_semaphore:
        db = SessionLocal()

        try:
            # Update job status to IN_PROGRESS
            job = db.query(SnapshotBackfillJobDB).filter(
                SnapshotBackfillJobDB.id == job_id
            ).first()

            if not job:
                logger.error(f"Job {job_id} not found")
                return

            job.status = 'IN_PROGRESS'
            job.started_at = datetime.utcnow()
            db.commit()

            logger.info(f"Starting backfill job {job_id} for account {account_id} ({start_date} to {end_date})")

            # Execute recalculation
            result = recalculate_account_snapshots(
                db=db,
                account_id=account_id,
                start_date=start_date,
                end_date=end_date,
                reason=f"Backfill job {job_id}"
            )

            # Update job with results
            job.status = 'COMPLETED'
            job.completed_at = datetime.utcnow()
            job.snapshots_created = result['created']
            job.snapshots_updated = result['updated']
            job.snapshots_failed = result['failed']
            job.snapshots_skipped = result['skipped']
            db.commit()

            logger.info(f"Completed backfill job {job_id}: {result}")

        except Exception as e:
            logger.error(f"Backfill job {job_id} failed: {str(e)}", exc_info=True)

            try:
                job = db.query(SnapshotBackfillJobDB).filter(
                    SnapshotBackfillJobDB.id == job_id
                ).first()

                if job:
                    job.status = 'FAILED'
                    job.completed_at = datetime.utcnow()
                    job.error_message = str(e)[:500]  # Limit error message length
                    db.commit()
            except Exception as commit_error:
                logger.error(f"Failed to update job {job_id} status: {str(commit_error)}")

        finally:
            db.close()


def recover_interrupted_jobs():
    """
    Mark interrupted jobs as FAILED on server startup.

    Threading jobs don't survive server restarts, so any
    PENDING or IN_PROGRESS jobs are stale.
    """
    db = SessionLocal()

    try:
        interrupted = db.query(SnapshotBackfillJobDB).filter(
            SnapshotBackfillJobDB.status.in_(['PENDING', 'IN_PROGRESS'])
        ).all()

        for job in interrupted:
            job.status = 'FAILED'
            job.error_message = 'Server restarted during execution'
            job.completed_at = datetime.utcnow()

        db.commit()
        logger.info(f"Marked {len(interrupted)} interrupted jobs as FAILED")

    finally:
        db.close()
