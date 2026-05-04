"""
Personalised seed: bootstrap an empty DB with admin + personal user + accounts,
then walk a statements directory and import every PDF/CSV under each account.

Personal data is read from a JSON config file that is NOT committed (lives under
``input/`` which is gitignored). The script itself is safe to commit. See
``scripts/seed_personal.example.json`` for the expected config shape.

Layout:
    scripts/seed_personal.py                     (committed)
    scripts/seed_personal.example.json           (committed, fake values)
    input/personal_seed/config.json              (gitignored — your real data)
    input/personal_seed/statements/<slug>/*.pdf  (gitignored — your statements)

Where ``<slug>`` matches an entry in ``config.json["accounts"][].slug``.

Run on an empty DB after ``alembic upgrade head``:
    python scripts/seed_personal.py
"""
import os
import sys
import json
from datetime import date
from decimal import Decimal
from uuid import uuid4

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.join(SCRIPT_DIR, "..")
sys.path.append(PROJECT_ROOT)
sys.path.append(SCRIPT_DIR)  # so we can import sibling scripts directly

from src.db.core import session_local, UserDB, AccountDB, AccountType
from src.crud.crud_user import hash_password

# Reuse the parser → DB pipeline already used by bulk_upload.py. That module
# also auto-accepts merchant/category suggestions, which is what we want for
# a one-shot seed.
from bulk_upload import process_local_file  # noqa: E402


# Bootstrap admin — predictable creds for dev login. Matches seed.py.
ADMIN_EMAIL = "dev@pocketwatcher.local"
ADMIN_USERNAME = "dev"
ADMIN_PASSWORD = "Password123!"

DEFAULT_CONFIG_PATH = os.path.join(
    PROJECT_ROOT, "input", "personal_seed", "config.json"
)
DEFAULT_STATEMENTS_DIR = os.path.join(
    PROJECT_ROOT, "input", "personal_seed", "statements"
)

# Optional account fields and how to coerce them from JSON strings.
_DECIMAL_FIELDS = (
    "balance",
    "original_principal",
    "minimum_payment",
    "interest_rate",
    "initial_cash_balance",
)


def _to_decimal(value):
    return Decimal(str(value)) if value is not None else None


def _build_account(user_db_id: int, spec: dict) -> AccountDB:
    kwargs = {
        "uuid": uuid4(),
        "user_id": user_db_id,
        "account_name": spec["account_name"],
        "account_type": AccountType[spec["account_type"]],
        "institution_name": spec["institution_name"],
        "account_number_last4": spec.get("last4"),
        "interest_rate_type": spec.get("interest_rate_type"),
        "comments": spec.get("comments"),
    }
    for f in _DECIMAL_FIELDS:
        if spec.get(f) is not None:
            kwargs[f] = _to_decimal(spec[f])
    return AccountDB(**kwargs)


def _create_admin(db) -> UserDB:
    admin = UserDB(
        id=uuid4(),
        email=ADMIN_EMAIL,
        username=ADMIN_USERNAME,
        password_hash=hash_password(ADMIN_PASSWORD),
        first_name="Dev",
        last_name="Admin",
        is_admin=True,
    )
    db.add(admin)
    db.flush()
    return admin


def _create_personal_user(db, user_spec: dict) -> UserDB:
    dob = user_spec.get("date_of_birth")
    user = UserDB(
        id=uuid4(),
        email=user_spec["email"],
        username=user_spec["username"],
        password_hash=hash_password(user_spec["password"]),
        first_name=user_spec.get("first_name"),
        last_name=user_spec.get("last_name"),
        date_of_birth=date.fromisoformat(dob) if dob else None,
        is_admin=False,
    )
    db.add(user)
    db.flush()
    return user


def _import_statements_for_account(db, statements_dir: str, account_spec: dict,
                                   account_id: int, user_db_id: int):
    slug = account_spec["slug"]
    institution = account_spec["institution"]
    folder = os.path.join(statements_dir, slug)
    if not os.path.isdir(folder):
        print(f"  [{slug}] no statements folder at {folder} — skipping")
        return

    files = sorted(
        os.path.join(folder, f)
        for f in os.listdir(folder)
        if f.lower().endswith((".pdf", ".csv"))
    )
    if not files:
        print(f"  [{slug}] folder is empty — skipping")
        return

    print(f"  [{slug}] importing {len(files)} file(s) via parser '{institution}'")
    for path in files:
        process_local_file(db, path, institution, account_id, user_db_id)


def seed_personal(config_path: str = DEFAULT_CONFIG_PATH,
                  statements_dir: str = DEFAULT_STATEMENTS_DIR):
    if not os.path.isfile(config_path):
        raise SystemExit(
            f"Config not found at {config_path}.\n"
            f"Copy scripts/seed_personal.example.json there and fill in your details."
        )
    with open(config_path, "r", encoding="utf-8") as f:
        config = json.load(f)

    db = session_local()
    try:
        if db.query(UserDB).count() > 0:
            print("Database already has users — aborting to avoid clobbering existing data.")
            return

        print("Creating admin user...")
        _create_admin(db)

        print(f"Creating personal user '{config['user']['username']}'...")
        user = _create_personal_user(db, config["user"])

        print(f"Creating {len(config['accounts'])} accounts...")
        slug_to_account_id: dict[str, int] = {}
        for spec in config["accounts"]:
            account = _build_account(user.db_id, spec)
            db.add(account)
            db.flush()
            slug_to_account_id[spec["slug"]] = account.id
            print(f"  + {spec['slug']} → id={account.id} ({spec['account_type']})")

        db.commit()
        print("Users + accounts committed.")

        print(f"\nImporting statements from {statements_dir}...")
        for spec in config["accounts"]:
            account_id = slug_to_account_id[spec["slug"]]
            _import_statements_for_account(
                db, statements_dir, spec, account_id, user.db_id,
            )

        print("\nDone.")
    finally:
        db.close()


if __name__ == "__main__":
    seed_personal()
