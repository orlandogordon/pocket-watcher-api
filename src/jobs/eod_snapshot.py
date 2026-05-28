#!/usr/bin/env python
"""End-of-Day Snapshot Job.

Runs at market close to fetch latest prices, create daily account snapshots, and
update net-worth history for all users (or one).

Run:
    python -m src.jobs.eod_snapshot [--date YYYY-MM-DD] [--user-id ID] [--skip-weekends]

Relocated from scripts/eod_snapshot_job.py into tracked source (#59) so the C5
deploy can schedule it via cron/systemd.
"""
from argparse import ArgumentParser
from datetime import date, datetime
import sys

from src.db.core import get_db, UserDB
from src.services.account_snapshot import create_all_account_snapshots


def is_market_open_day(check_date: date) -> bool:
    """Weekday check (does not account for market holidays). 0=Mon..6=Sun."""
    return check_date.weekday() < 5


def run_eod_snapshots(snapshot_date: date, user_id: int = None, skip_weekends: bool = False):
    """Run end-of-day snapshots for all users (or a specific user)."""
    if skip_weekends and not is_market_open_day(snapshot_date):
        print(f"Skipping: {snapshot_date} is a weekend")
        return

    print("=" * 60)
    print(f"Running EOD Snapshot Job - {snapshot_date}")
    print("=" * 60)

    db = next(get_db())
    try:
        if user_id:
            users = db.query(UserDB).filter(UserDB.db_id == user_id).all()
            if not users:
                print(f"User {user_id} not found")
                return
        else:
            users = db.query(UserDB).all()

        print(f"Processing {len(users)} user(s)...")
        total_snapshots = 0
        total_errors = 0

        for user in users:
            print(f"\n--- Processing user: {user.username} (ID: {user.db_id}) ---")
            try:
                snapshots = create_all_account_snapshots(
                    db=db,
                    user_id=user.db_id,
                    snapshot_date=snapshot_date,
                    snapshot_source="SCHEDULED",
                    update_prices=True,
                )
                print(f"Created {len(snapshots)} snapshots for user {user.username}")
                total_snapshots += len(snapshots)
            except Exception as e:
                print(f"ERROR processing user {user.username}: {str(e)}")
                total_errors += 1
                continue

        print("\n" + "=" * 60)
        print("Job Complete!")
        print(f"  Total snapshots created: {total_snapshots}")
        print(f"  Errors: {total_errors}")
        print("=" * 60)
    except Exception as e:
        print(f"FATAL ERROR: {str(e)}")
        raise
    finally:
        db.close()


def main():
    parser = ArgumentParser(description="Run end-of-day account snapshots")
    parser.add_argument("--date", type=str, help="Snapshot date (YYYY-MM-DD), defaults to today")
    parser.add_argument("--user-id", type=int, help="Process only a specific user ID")
    parser.add_argument("--skip-weekends", action="store_true", help="Skip execution on weekends")
    args = parser.parse_args()

    if args.date:
        try:
            snapshot_date = datetime.strptime(args.date, "%Y-%m-%d").date()
        except ValueError:
            print(f"Invalid date format: {args.date}. Use YYYY-MM-DD")
            sys.exit(1)
    else:
        snapshot_date = date.today()

    run_eod_snapshots(
        snapshot_date=snapshot_date,
        user_id=args.user_id,
        skip_weekends=args.skip_weekends,
    )


if __name__ == "__main__":
    main()
