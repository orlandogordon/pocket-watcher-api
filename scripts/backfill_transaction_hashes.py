"""Backfill transaction_hash on transactions and investment_transactions
after switching the hash formula from institution_name to account_id.

See backend todo #52.

Usage:
    python -m scripts.backfill_transaction_hashes --dry-run
    python -m scripts.backfill_transaction_hashes --apply

Behavior:
- Snapshots the SQLite DB to test.db.bak.<timestamp> before --apply.
- Recomputes every transaction_hash using the current generate_*_hash
  functions (which now take account_id).
- Groups rows whose recomputed hash collides; for each colliding group,
  the lowest-db_id row keeps the deterministic hash and all others get
  a make_unique=True hash (UUID-suffixed).
- Logs every auto-applied make_unique decision so collisions are
  audit-traceable.
"""
from __future__ import annotations

import argparse
import shutil
import sys
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Tuple

# Make src/ importable when running as a script
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from sqlalchemy.orm import Session  # noqa: E402

from src.db.core import (  # noqa: E402
    session_local,
    TransactionDB,
    InvestmentTransactionDB,
    AccountDB,
)
from src.crud.crud_transaction import generate_transaction_hash  # noqa: E402
from src.crud.crud_investment import generate_investment_transaction_hash  # noqa: E402
from src.parser.models import ParsedInvestmentTransaction  # noqa: E402


DB_PATH = Path(__file__).resolve().parents[1] / "test.db"


def snapshot_db() -> Path:
    """Copy test.db to a timestamped backup. Returns the backup path."""
    if not DB_PATH.exists():
        raise FileNotFoundError(f"Expected DB at {DB_PATH} — adjust DB_PATH in script")
    ts = datetime.now().strftime("%Y%m%d-%H%M%S")
    backup = DB_PATH.with_name(f"test.db.bak.backfill52.{ts}")
    shutil.copy2(DB_PATH, backup)
    return backup


def _recompute_regular(db: Session) -> Dict[str, List[Tuple[int, TransactionDB]]]:
    """Return {new_hash: [(db_id, txn), ...]} grouped for collision detection."""
    groups: Dict[str, List[Tuple[int, TransactionDB]]] = defaultdict(list)
    rows = db.query(TransactionDB).order_by(TransactionDB.db_id.asc()).all()
    skipped = 0
    for txn in rows:
        if txn.account_id is None:
            skipped += 1
            continue
        new_hash = generate_transaction_hash(
            user_id=txn.user_id,
            account_id=txn.account_id,
            transaction_date=txn.transaction_date,
            transaction_type_value=txn.transaction_type.value,
            amount=txn.amount,
            description=txn.description,
        )
        groups[new_hash].append((txn.db_id, txn))
    if skipped:
        print(f"  WARN: skipped {skipped} regular transactions with NULL account_id")
    return groups


def _recompute_investment(db: Session) -> Dict[str, List[Tuple[int, InvestmentTransactionDB]]]:
    groups: Dict[str, List[Tuple[int, InvestmentTransactionDB]]] = defaultdict(list)
    rows = db.query(InvestmentTransactionDB).order_by(
        InvestmentTransactionDB.investment_transaction_id.asc()
    ).all()
    skipped = 0
    for txn in rows:
        if txn.account_id is None:
            skipped += 1
            continue
        parsed = ParsedInvestmentTransaction(
            transaction_date=txn.transaction_date,
            transaction_type=txn.transaction_type.value,
            symbol=txn.symbol,
            api_symbol=txn.api_symbol,
            description=txn.description or "",
            quantity=txn.quantity,
            price_per_share=txn.price_per_share,
            total_amount=txn.total_amount,
        )
        new_hash = generate_investment_transaction_hash(parsed, txn.user_id, txn.account_id)
        groups[new_hash].append((txn.investment_transaction_id, txn))
    if skipped:
        print(f"  WARN: skipped {skipped} investment transactions with NULL account_id")
    return groups


def _institution_for(db: Session, txn) -> str:
    acct = db.query(AccountDB).filter(AccountDB.id == txn.account_id).first()
    return acct.institution_name if acct else "?"


def _print_collision_report(label: str, groups: dict, db: Session) -> int:
    collisions = {h: rows for h, rows in groups.items() if len(rows) > 1}
    print(f"\n=== {label}: {len(collisions)} colliding group(s) ===")
    for h, rows in collisions.items():
        print(f"  Hash {h[:12]}... ({len(rows)} rows):")
        for db_id, txn in rows:
            inst = _institution_for(db, txn)
            desc = (txn.description or "")[:40]
            amt = getattr(txn, "amount", None) or getattr(txn, "total_amount", None)
            print(
                f"    db_id={db_id} acct={txn.account_id} ({inst}) "
                f"date={txn.transaction_date} amt={amt} desc='{desc}'"
            )
    return sum(len(rows) - 1 for rows in collisions.values())


def _apply_regular(db: Session, groups: dict) -> int:
    """Write new hashes. Within a colliding group, lowest db_id keeps the
    deterministic hash; others get make_unique=True. Returns count of rows
    that received make_unique hashes."""
    make_unique_count = 0
    for new_hash, rows in groups.items():
        rows_sorted = sorted(rows, key=lambda r: r[0])
        keeper_db_id, keeper = rows_sorted[0]
        keeper.transaction_hash = new_hash
        for db_id, txn in rows_sorted[1:]:
            unique_hash = generate_transaction_hash(
                user_id=txn.user_id,
                account_id=txn.account_id,
                transaction_date=txn.transaction_date,
                transaction_type_value=txn.transaction_type.value,
                amount=txn.amount,
                description=txn.description,
                make_unique=True,
            )
            txn.transaction_hash = unique_hash
            make_unique_count += 1
            print(
                f"    [regular] db_id={db_id} got make_unique hash "
                f"(collided with keeper db_id={keeper_db_id} on hash {new_hash[:12]}...)"
            )
    return make_unique_count


def _apply_investment(db: Session, groups: dict) -> int:
    make_unique_count = 0
    for new_hash, rows in groups.items():
        rows_sorted = sorted(rows, key=lambda r: r[0])
        keeper_id, keeper = rows_sorted[0]
        keeper.transaction_hash = new_hash
        for inv_id, txn in rows_sorted[1:]:
            parsed = ParsedInvestmentTransaction(
                transaction_date=txn.transaction_date,
                transaction_type=txn.transaction_type.value,
                symbol=txn.symbol,
                api_symbol=txn.api_symbol,
                description=txn.description or "",
                quantity=txn.quantity,
                price_per_share=txn.price_per_share,
                total_amount=txn.total_amount,
            )
            unique_hash = generate_investment_transaction_hash(
                parsed, txn.user_id, txn.account_id, make_unique=True
            )
            txn.transaction_hash = unique_hash
            make_unique_count += 1
            print(
                f"    [investment] inv_id={inv_id} got make_unique hash "
                f"(collided with keeper inv_id={keeper_id} on hash {new_hash[:12]}...)"
            )
    return make_unique_count


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    g = parser.add_mutually_exclusive_group(required=True)
    g.add_argument("--dry-run", action="store_true", help="Print collision report only")
    g.add_argument("--apply", action="store_true", help="Take snapshot and write new hashes")
    args = parser.parse_args()

    db = session_local()
    try:
        print("Recomputing regular transaction hashes...")
        reg_groups = _recompute_regular(db)
        print(f"  {sum(len(v) for v in reg_groups.values())} rows, "
              f"{len(reg_groups)} unique hashes")

        print("Recomputing investment transaction hashes...")
        inv_groups = _recompute_investment(db)
        print(f"  {sum(len(v) for v in inv_groups.values())} rows, "
              f"{len(inv_groups)} unique hashes")

        reg_collide_extra = _print_collision_report("Regular", reg_groups, db)
        inv_collide_extra = _print_collision_report("Investment", inv_groups, db)

        print(f"\nSummary: {reg_collide_extra} regular + {inv_collide_extra} investment "
              f"rows would receive make_unique hashes.")

        if args.dry_run:
            print("\n--dry-run: no writes performed.")
            return 0

        # --apply path
        backup = snapshot_db()
        print(f"\nDB snapshot saved to: {backup}")

        print("\nApplying regular transaction hashes...")
        reg_unique = _apply_regular(db, reg_groups)
        print("Applying investment transaction hashes...")
        inv_unique = _apply_investment(db, inv_groups)

        db.commit()
        print(f"\nApplied. {reg_unique} regular + {inv_unique} investment rows "
              f"received make_unique hashes.")
        return 0
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()


if __name__ == "__main__":
    sys.exit(main())
