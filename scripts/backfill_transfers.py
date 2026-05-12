"""One-shot: backfill TRANSFER_OUT classification + OFFSETS pairing
across all users.

What it does, in order:

1. Backup test.db -> test.db.bak.<timestamp>.
2. For each user, load all non-source accounts (currently excludes LOAN
   per todo #39 — flipped on once todo #40 lands).
3. Iterate every PURCHASE/WITHDRAWAL row on the user's CHECKING/SAVINGS
   accounts; for each, run the Tier A classifier. On a match, flip the
   type to TRANSFER_OUT and recompute `transaction_hash` atomically.
4. Run the Tier B pairing pass; for unique-closest-date Tier-A-confirmed
   pairs, auto-create an OFFSETS relationship.
5. For each checking/savings account that had any row reclassified, call
   `recalculate_non_investment_snapshots` over the affected date range so
   snapshot history matches the new types.
6. --dry-run prints the candidate set without writing.

Usage:
    python scripts/backfill_transfers.py --dry-run
    python scripts/backfill_transfers.py
"""
import argparse
import os
import shutil
import sys
from datetime import datetime
from decimal import Decimal

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.join(SCRIPT_DIR, "..")
sys.path.append(PROJECT_ROOT)

from dotenv import load_dotenv
load_dotenv(os.path.join(PROJECT_ROOT, ".env"))

from sqlalchemy.orm import joinedload

from src.db.core import (
    AccountDB,
    AccountType,
    DATABASE_URL,
    RelationshipType,
    TransactionDB,
    TransactionRelationshipDB,
    TransactionType,
    UserDB,
    session_local,
)
from src.crud.crud_transaction import update_transaction_type_with_hash
from src.services.account_snapshot import recalculate_non_investment_snapshots
from src.services.transfer_classifier import classify_outflow
from src.services.transfer_pairing import (
    PairConfidence,
    create_offsets_relationship,
    find_pair_suggestions,
)


SOURCE_TYPES = (AccountType.CHECKING, AccountType.SAVINGS)
CANDIDATE_TYPES = (TransactionType.PURCHASE, TransactionType.WITHDRAWAL)


def backup_db():
    if not DATABASE_URL.startswith("sqlite:///"):
        print(f"DATABASE_URL is not SQLite ({DATABASE_URL}) — skipping backup.")
        return None
    db_filename = DATABASE_URL.replace("sqlite:///", "")
    db_path = db_filename if os.path.isabs(db_filename) else os.path.join(PROJECT_ROOT, db_filename)
    if not os.path.isfile(db_path):
        print(f"DB file not found at {db_path} — skipping backup.")
        return None
    stamp = datetime.now().strftime("%Y-%m-%d-%H%M%S")
    backup_path = f"{db_path}.bak.{stamp}"
    shutil.copy2(db_path, backup_path)
    size_mb = os.path.getsize(backup_path) / (1024 * 1024)
    print(f"Backed up DB -> {backup_path} ({size_mb:.1f} MB)")
    return backup_path


def reclassify_user(db, user: UserDB, dry_run: bool) -> dict:
    """Returns dict of stats and the set of (account_id, min_date, max_date)
    tuples that need snapshot recalculation."""
    user_accounts = db.query(AccountDB).filter(AccountDB.user_id == user.db_id).all()
    source_accounts = [a for a in user_accounts if a.account_type in SOURCE_TYPES]

    if not source_accounts:
        return {"reclassified": 0, "affected_accounts": {}}

    affected: dict[int, list] = {}

    candidate_rows = (
        db.query(TransactionDB)
        .options(joinedload(TransactionDB.account))
        .filter(
            TransactionDB.user_id == user.db_id,
            TransactionDB.account_id.in_([a.id for a in source_accounts]),
            TransactionDB.transaction_type.in_(CANDIDATE_TYPES),
        )
        .all()
    )

    reclassified = 0
    for txn in candidate_rows:
        source = txn.account
        if source is None:
            continue
        result = classify_outflow(
            description=txn.description or "",
            source_account_id=source.id,
            user_accounts=user_accounts,
        )
        if result.transaction_type != TransactionType.TRANSFER_OUT:
            continue

        partner_name = next(
            (a.account_name for a in user_accounts if a.id == result.suggested_partner_account_id),
            "?",
        )
        print(
            f"  [{user.email}] {txn.transaction_date} ${txn.amount} "
            f"'{(txn.description or '')[:60]}' -> TRANSFER_OUT "
            f"(partner: {partner_name}, token: {result.matched_token})"
        )
        if dry_run:
            reclassified += 1
            continue
        try:
            update_transaction_type_with_hash(db, txn, TransactionType.TRANSFER_OUT)
        except ValueError as e:
            print(f"    SKIP (hash collision): {e}")
            continue
        reclassified += 1
        affected.setdefault(source.id, []).append(txn.transaction_date)

    if not dry_run and reclassified:
        db.commit()

    return {"reclassified": reclassified, "affected_accounts": affected}


def pair_user(db, user: UserDB, dry_run: bool) -> int:
    """Run pairing pass; auto-create OFFSETS for high-confidence
    unique-closest-date pairs."""
    candidates = find_pair_suggestions(db, user.db_id)
    # Group by out-side id to enforce unique-closest-date.
    by_out: dict[tuple, list] = {}
    for c in candidates:
        key = (c.out_side.is_investment, c.out_side.txn_id)
        by_out.setdefault(key, []).append(c)

    created = 0
    for _, cands in by_out.items():
        cands.sort(key=lambda c: abs(c.date_offset_days))
        high = [c for c in cands if c.confidence == PairConfidence.HIGH]
        if not high:
            continue
        high.sort(key=lambda c: abs(c.date_offset_days))
        if len(high) >= 2 and abs(high[0].date_offset_days) == abs(high[1].date_offset_days):
            continue
        chosen = high[0]
        print(
            f"  [{user.email}] OFFSETS: "
            f"out_id={chosen.out_side.txn_id}({'inv' if chosen.out_side.is_investment else 'reg'}) "
            f"<-> in_id={chosen.in_side.txn_id}({'inv' if chosen.in_side.is_investment else 'reg'}) "
            f"amt=${chosen.out_side.amount} offset={chosen.date_offset_days}d"
        )
        if dry_run:
            created += 1
            continue
        create_offsets_relationship(db, chosen.out_side, chosen.in_side)
        created += 1

    if not dry_run and created:
        db.commit()

    return created


def recalc_snapshots(db, affected_accounts: dict[int, list], dry_run: bool):
    """Re-run snapshot derivation for affected accounts over the date range
    spanning their reclassified rows."""
    for account_id, dates in affected_accounts.items():
        if not dates:
            continue
        start = min(dates)
        end = datetime.utcnow().date()
        print(f"  Recalculating snapshots for account_id={account_id} from {start} to {end}")
        if not dry_run:
            recalculate_non_investment_snapshots(
                db, account_id, start, end,
                reason="Transfer reclassification backfill (todo #39)",
            )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true",
                        help="Show what would change without writing.")
    args = parser.parse_args()

    if not args.dry_run:
        backup_db()

    db = session_local()
    try:
        users = db.query(UserDB).all()
        print(f"Found {len(users)} user(s).")

        total_reclassified = 0
        total_offsets = 0
        for user in users:
            print(f"\n[user {user.email}]")
            stats = reclassify_user(db, user, args.dry_run)
            total_reclassified += stats["reclassified"]
            paired = pair_user(db, user, args.dry_run)
            total_offsets += paired
            if not args.dry_run and stats["affected_accounts"]:
                recalc_snapshots(db, stats["affected_accounts"], args.dry_run)

        print(
            f"\n{'[DRY-RUN] Would reclassify' if args.dry_run else 'Reclassified'} "
            f"{total_reclassified} row(s). "
            f"{'Would create' if args.dry_run else 'Created'} {total_offsets} OFFSETS."
        )
    finally:
        db.close()


if __name__ == "__main__":
    main()
