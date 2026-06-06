#!/usr/bin/env python
"""End-of-Day Snapshot Job.

Runs at market close to fetch latest prices, create daily account snapshots, and
update net-worth history for all users (or one).

Run:
    python -m src.jobs.eod_snapshot [--date YYYY-MM-DD] [--user-id ID] [--skip-weekends]

Scheduled nightly in production (#63) via host cron that runs it inside the api
container. Logs through the app logger (structured JSON → promtail/Loki) and
exits non-zero when any user fails, so cron/monitoring surface failures.

Relocated from scripts/eod_snapshot_job.py into tracked source (#59).
"""
from argparse import ArgumentParser
from datetime import date, datetime
import sys

from src.db.core import session_local, UserDB
from src.logging_config import get_logger, setup_logging
from src.services.account_snapshot import create_all_account_snapshots

logger = get_logger(__name__)


def is_market_open_day(check_date: date) -> bool:
    """Weekday check (does not account for market holidays). 0=Mon..6=Sun.

    Holidays are intentionally not guarded (#63): on a market holiday the run is
    a harmless no-op duplicate — yfinance returns the prior close and net-worth
    history carries the last observation forward — so a calendar dependency isn't
    worth the upkeep.
    """
    return check_date.weekday() < 5


def run_eod_snapshots(
    db, snapshot_date: date, user_id: int = None, skip_weekends: bool = False
) -> dict:
    """Run end-of-day snapshots for all users (or a specific user).

    Returns a summary dict: ``{users, snapshots, errors}``. ``errors`` counts
    users whose snapshot creation raised — the caller exits non-zero on any.
    """
    if skip_weekends and not is_market_open_day(snapshot_date):
        logger.info("Skipping EOD snapshot: %s is a weekend", snapshot_date)
        return {"users": 0, "snapshots": 0, "errors": 0}

    logger.info("Starting EOD snapshot job for %s", snapshot_date)

    if user_id:
        users = db.query(UserDB).filter(UserDB.db_id == user_id).all()
        if not users:
            logger.warning("User %s not found", user_id)
            return {"users": 0, "snapshots": 0, "errors": 0}
    else:
        users = db.query(UserDB).all()

    logger.info("Processing %d user(s)", len(users))
    total_snapshots = 0
    total_errors = 0

    for user in users:
        try:
            snapshots = create_all_account_snapshots(
                db=db,
                user_id=user.db_id,
                snapshot_date=snapshot_date,
                snapshot_source="SCHEDULED",
                update_prices=True,
            )
            logger.info(
                "Created %d snapshots for user %s (id=%s)",
                len(snapshots), user.username, user.db_id,
            )
            total_snapshots += len(snapshots)
        except Exception:
            logger.error(
                "Error processing user %s (id=%s)",
                user.username, user.db_id, exc_info=True,
            )
            total_errors += 1
            continue

    logger.info(
        "EOD snapshot job complete: %d snapshots, %d error(s)",
        total_snapshots, total_errors,
    )
    return {"users": len(users), "snapshots": total_snapshots, "errors": total_errors}


def main() -> int:
    setup_logging()

    parser = ArgumentParser(description="Run end-of-day account snapshots")
    parser.add_argument("--date", type=str, help="Snapshot date (YYYY-MM-DD), defaults to today")
    parser.add_argument("--user-id", type=int, help="Process only a specific user ID")
    parser.add_argument("--skip-weekends", action="store_true", help="Skip execution on weekends")
    args = parser.parse_args()

    if args.date:
        try:
            snapshot_date = datetime.strptime(args.date, "%Y-%m-%d").date()
        except ValueError:
            logger.error("Invalid date format: %s. Use YYYY-MM-DD", args.date)
            return 1
    else:
        snapshot_date = date.today()

    db = session_local()
    try:
        summary = run_eod_snapshots(
            db=db,
            snapshot_date=snapshot_date,
            user_id=args.user_id,
            skip_weekends=args.skip_weekends,
        )
    except Exception:
        logger.error("EOD snapshot job failed", exc_info=True)
        return 1
    finally:
        db.close()

    return 1 if summary["errors"] else 0


if __name__ == "__main__":
    sys.exit(main())
