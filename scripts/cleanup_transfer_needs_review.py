"""One-shot: detach the 'Needs Review' system tag from existing
TRANSFER_IN / TRANSFER_OUT transactions.

Backend todo #48 (Phase 1, sub-item 1). The needs_review trigger in
uploads.confirm previously attached 'Needs Review' to any transaction
whose final state had a null category or null merchant. Transfers
intentionally have neither (they're balance-neutral movements between
user-owned accounts), so every TRANSFER_IN/OUT row got mis-flagged.

The trigger has been updated in src/routers/uploads.py to skip
transfers; this script cleans up the rows imported before the fix
landed.

Usage:
    python scripts/cleanup_transfer_needs_review.py --dry-run
    python scripts/cleanup_transfer_needs_review.py
"""
import argparse
import os
import shutil
import sys
from datetime import datetime

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.join(SCRIPT_DIR, "..")
sys.path.append(PROJECT_ROOT)

from dotenv import load_dotenv
load_dotenv(os.path.join(PROJECT_ROOT, ".env"))

from src.db.core import (
    DATABASE_URL,
    TagDB,
    TransactionDB,
    TransactionTagDB,
    TransactionType,
    session_local,
)


TRANSFER_TYPES = (TransactionType.TRANSFER_IN, TransactionType.TRANSFER_OUT)


def _backup_sqlite(db_url: str) -> str | None:
    if not db_url.startswith("sqlite:///"):
        return None
    db_path = db_url.replace("sqlite:///", "")
    if not os.path.exists(db_path):
        return None
    ts = datetime.utcnow().strftime("%Y%m%d-%H%M%S")
    backup_path = f"{db_path}.bak.cleanup-transfer-needs-review.{ts}"
    shutil.copy2(db_path, backup_path)
    return backup_path


def run(dry_run: bool) -> int:
    session = session_local()
    try:
        candidates = (
            session.query(TransactionTagDB, TransactionDB, TagDB)
            .join(TransactionDB, TransactionTagDB.transaction_id == TransactionDB.db_id)
            .join(TagDB, TransactionTagDB.tag_id == TagDB.tag_id)
            .filter(
                TagDB.tag_name == "Needs Review",
                TagDB.is_system == True,
                TransactionDB.transaction_type.in_(TRANSFER_TYPES),
            )
            .all()
        )

        print(f"Found {len(candidates)} TransactionTagDB rows attaching "
              f"'Needs Review' to TRANSFER_IN/OUT transactions.")

        if not candidates:
            return 0

        by_type: dict[TransactionType, int] = {}
        by_user: dict[int, int] = {}
        for _, txn, _ in candidates:
            by_type[txn.transaction_type] = by_type.get(txn.transaction_type, 0) + 1
            by_user[txn.user_id] = by_user.get(txn.user_id, 0) + 1

        print("  By transaction_type:")
        for t, n in sorted(by_type.items(), key=lambda kv: -kv[1]):
            print(f"    {t.value}: {n}")
        print(f"  Affected users: {len(by_user)} ({sorted(by_user.keys())[:5]}{'…' if len(by_user) > 5 else ''})")

        if dry_run:
            print("\n--dry-run: no changes written.")
            return 0

        for assoc, _, _ in candidates:
            session.delete(assoc)
        session.commit()
        print(f"\nDetached {len(candidates)} 'Needs Review' tags. Done.")
        return 0
    finally:
        session.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true",
                        help="Report counts without writing.")
    args = parser.parse_args()

    if not args.dry_run:
        backup = _backup_sqlite(DATABASE_URL)
        if backup:
            print(f"Backed up DB to: {backup}\n")

    sys.exit(run(args.dry_run))


if __name__ == "__main__":
    main()
