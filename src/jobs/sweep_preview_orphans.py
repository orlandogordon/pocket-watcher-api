#!/usr/bin/env python
"""Sweep orphaned preview-upload files (#59 follow-up).

The single-file preview flow saves the uploaded file at preview time and only
"adopts" it (links it to an ``UploadJobDB``) at confirm. A preview that is never
confirmed *or* cancelled leaves its file on disk once the 12h Redis session TTL
expires — unreferenced by any DB row, unreachable by any endpoint, just dead
bytes. This job reclaims them.

A file is deleted only when BOTH hold:
  (a) no ``UploadJobDB.storage_key`` references it, and
  (b) it is older than ``--min-age-hours`` (default 13 = the 12h session TTL +
      a 1h margin), so an in-flight preview is never collected.

Run (schedule via cron/systemd in the C5 deploy):
    python -m src.jobs.sweep_preview_orphans --dry-run
    python -m src.jobs.sweep_preview_orphans --apply
"""
from __future__ import annotations

import argparse
import time
from dataclasses import dataclass

from src.db.core import session_local, UploadJobDB
from src.services.file_storage import get_storage
from src.logging_config import get_logger, setup_logging

logger = get_logger(__name__)

DEFAULT_MIN_AGE_HOURS = 13.0


@dataclass
class SweepSummary:
    scanned: int = 0
    referenced: int = 0
    orphaned: int = 0
    deleted: int = 0


def sweep_orphans(db, *, min_age_hours: float = DEFAULT_MIN_AGE_HOURS,
                  dry_run: bool = True) -> SweepSummary:
    """Delete stored files that no UploadJobDB references and that are older than
    ``min_age_hours``. Returns a tally."""
    storage = get_storage()
    live_keys = {
        key for (key,) in db.query(UploadJobDB.storage_key).filter(
            UploadJobDB.storage_key.isnot(None)
        )
    }
    cutoff = time.time() - min_age_hours * 3600
    summary = SweepSummary()

    for key in storage.iter_keys():
        summary.scanned += 1
        if key in live_keys:
            summary.referenced += 1
            continue
        if storage.modified_time(key) >= cutoff:
            continue  # too new — could be an in-flight (unconfirmed) preview
        summary.orphaned += 1
        if not dry_run:
            storage.delete(key)
            summary.deleted += 1

    return summary


def main() -> int:
    setup_logging()
    parser = argparse.ArgumentParser(description="Sweep orphaned preview-upload files (#59)")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--dry-run", action="store_true", help="report only")
    group.add_argument("--apply", action="store_true", help="delete orphaned files")
    parser.add_argument("--min-age-hours", type=float, default=DEFAULT_MIN_AGE_HOURS,
                        help="only sweep files older than this (default 13)")
    args = parser.parse_args()

    db = session_local()
    try:
        summary = sweep_orphans(db, min_age_hours=args.min_age_hours, dry_run=args.dry_run)
    finally:
        db.close()

    print(f"scanned={summary.scanned} referenced={summary.referenced} "
          f"orphaned={summary.orphaned} deleted={summary.deleted}"
          + (" (dry-run)" if args.dry_run else ""))
    logger.info("preview-orphan sweep: %s", summary)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
