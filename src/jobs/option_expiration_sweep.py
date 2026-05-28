#!/usr/bin/env python
"""Sweep for option contracts whose OCC expiration has passed but which still
show open positions (no SELL/EXPIRATION recorded). See backend todo #57.

Run:
    python -m src.jobs.option_expiration_sweep --dry-run
    python -m src.jobs.option_expiration_sweep --apply

Behavior:
- OTM at expiration: synthesizes an EXPIRATION transaction with $0 proceeds
  (deterministic hash, so re-runs don't duplicate).
- ITM / no-underlying-close: flagged for manual reconciliation, left alone.

Relocated from scripts/sweep_option_expirations.py into tracked source (#59).
The dev-only SQLite backup was dropped — back up via the deployment's DB dump
(C5) before applying in production.
"""
import argparse
import sys

from src.db.core import session_local
from src.logging_config import get_logger, setup_logging
from src.services.option_expirations import sweep


def main() -> int:
    setup_logging()
    logger = get_logger(__name__)

    parser = argparse.ArgumentParser(description="Sweep orphaned option expirations (#57)")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--dry-run", action="store_true",
                       help="report what would change without writing")
    group.add_argument("--apply", action="store_true",
                       help="write synthetic EXPIRATION rows")
    args = parser.parse_args()

    db = session_local()
    try:
        summary = sweep(db, dry_run=args.dry_run)
    finally:
        db.close()

    logger.info("Option expiration sweep complete (dry_run=%s)", args.dry_run)
    print(f"\nOrphan expirations found: {len(summary.orphans)}")
    print(f"  OTM auto-{'would-create' if args.dry_run else 'created'}: {summary.created}")
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
