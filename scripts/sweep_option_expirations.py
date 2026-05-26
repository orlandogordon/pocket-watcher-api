#!/usr/bin/env python
"""Sweep for option contracts whose OCC expiration has passed but which
still show open positions (no SELL/EXPIRATION recorded).

See backend todo #57.

Usage:
    python -m scripts.sweep_option_expirations --dry-run
    python -m scripts.sweep_option_expirations --apply

Behavior:
- OTM at expiration: synthesizes an EXPIRATION transaction with $0 proceeds.
  Hash is deterministic so re-runs don't duplicate.
- ITM at expiration: flagged in output; left alone (probably auto-exercised
  into shares — user must reconcile).
- No underlying close available: flagged; left alone.

`--apply` backs up `test.db` first.
"""
from __future__ import annotations

import argparse
import shutil
import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.db.core import session_local  # noqa: E402
from src.logging_config import get_logger, setup_logging  # noqa: E402
from src.services.option_expirations import sweep  # noqa: E402

DB_PATH = Path(__file__).resolve().parents[1] / "test.db"


def snapshot_db() -> Path:
    if not DB_PATH.exists():
        raise FileNotFoundError(f"Expected DB at {DB_PATH}")
    ts = datetime.now().strftime("%Y%m%d-%H%M%S")
    backup = DB_PATH.with_name(f"test.db.bak.expsweep57.{ts}")
    shutil.copy2(DB_PATH, backup)
    return backup


def main() -> int:
    setup_logging()
    logger = get_logger(__name__)

    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--dry-run", action="store_true",
                       help="report what would change without writing")
    group.add_argument("--apply", action="store_true",
                       help="write synthetic EXPIRATION rows (backs up test.db first)")
    args = parser.parse_args()

    if args.apply and DB_PATH.exists():
        backup = snapshot_db()
        logger.info("Backed up DB to %s", backup)

    db = session_local()
    try:
        summary = sweep(db, dry_run=args.dry_run)
    finally:
        db.close()

    print(f"\nOrphan expirations found: {len(summary.orphans)}")
    print(f"  OTM auto-{'would-create' if args.dry_run else 'created'}: "
          f"{summary.created}")
    print(f"  OTM already had synth EXPIRATION (skipped): {summary.skipped_existing}")
    print(f"  ITM flagged for manual review: {summary.flagged_itm}")
    print(f"  UNKNOWN (no underlying price): {summary.flagged_unknown}")

    if summary.flagged_itm or summary.flagged_unknown:
        print("\nThe following contracts need manual reconciliation:")
        for o in summary.orphans:
            if o.status in ("ITM", "UNKNOWN"):
                print(f"  account_id={o.account_id} {o.api_symbol} "
                      f"qty={o.quantity} status={o.status} "
                      f"underlying_close={o.underlying_close}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
