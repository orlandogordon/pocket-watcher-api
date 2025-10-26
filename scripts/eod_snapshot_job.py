#!/usr/bin/env python
"""
End-of-Day Snapshot Job

Runs at market close to:
1. Fetch latest market prices for all investment holdings
2. Create daily snapshots for all user accounts
3. Update net worth history

Usage:
    python scripts/eod_snapshot_job.py [--date YYYY-MM-DD] [--user-id ID]

Options:
    --date: Specific date for snapshot (default: today)
    --user-id: Process only specific user (default: all users)
    --skip-weekends: Skip if run on weekend (default: false)
    --delay: Delay between API calls in seconds (default: 0.5)
"""
import sys
import os
from pathlib import Path
from datetime import date, datetime
from argparse import ArgumentParser

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.db.core import get_db, UserDB
from src.services.account_snapshot import create_all_account_snapshots


def is_market_open_day(check_date: date) -> bool:
    """
    Check if given date is a market trading day (weekday).
    Note: This doesn't account for market holidays.
    """
    # 0 = Monday, 6 = Sunday
    return check_date.weekday() < 5


def run_eod_snapshots(
    snapshot_date: date,
    user_id: int = None,
    skip_weekends: bool = False
):
    """
    Run end-of-day snapshots for all users (or specific user).
    """
    # Check if market is open
    if skip_weekends and not is_market_open_day(snapshot_date):
        print(f"Skipping: {snapshot_date} is a weekend")
        return

    print("=" * 60)
    print(f"Running EOD Snapshot Job - {snapshot_date}")
    print("=" * 60)

    db = next(get_db())

    try:
        # Get users to process
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
                    snapshot_source="EOD_JOB",
                    update_prices=True  # Fetch latest market prices
                )

                print(f"Created {len(snapshots)} snapshots for user {user.username}")
                total_snapshots += len(snapshots)

            except Exception as e:
                print(f"ERROR processing user {user.username}: {str(e)}")
                total_errors += 1
                continue

        print("\n" + "=" * 60)
        print(f"Job Complete!")
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

    parser.add_argument(
        '--date',
        type=str,
        help='Snapshot date (YYYY-MM-DD), defaults to today'
    )

    parser.add_argument(
        '--user-id',
        type=int,
        help='Process only specific user ID'
    )

    parser.add_argument(
        '--skip-weekends',
        action='store_true',
        help='Skip execution on weekends'
    )

    parser.add_argument(
        '--delay',
        type=float,
        default=0.5,
        help='Delay between API calls in seconds (default: 0.5)'
    )

    args = parser.parse_args()

    # Parse date
    if args.date:
        try:
            snapshot_date = datetime.strptime(args.date, '%Y-%m-%d').date()
        except ValueError:
            print(f"Invalid date format: {args.date}. Use YYYY-MM-DD")
            sys.exit(1)
    else:
        snapshot_date = date.today()

    # Run job
    run_eod_snapshots(
        snapshot_date=snapshot_date,
        user_id=args.user_id,
        skip_weekends=args.skip_weekends
    )


if __name__ == "__main__":
    main()
