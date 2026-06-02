#!/usr/bin/env python
"""Bootstrap the first admin user (#61).

`is_admin` is a DB column with no API path to set it — registration is
admin-gated, so a fresh deploy has no admin and no way to make one through the
API. This job mints that first admin from env vars, idempotently, so it's safe
to run on every deploy:

    ADMIN_EMAIL=you@example.com ADMIN_PASSWORD='S3cret!!' \
        python -m src.jobs.bootstrap_admin

If a user with ADMIN_EMAIL already exists it's a no-op (the existing admin flag
is left untouched). Otherwise the user is created via the normal `create_db_user`
path (bcrypt hash, system tags seeded) and then promoted to admin.

Env:
    ADMIN_EMAIL     (required) — email for the admin account
    ADMIN_PASSWORD  (required) — must satisfy the password policy
                                 (min 8, upper + lower + digit)
    ADMIN_USERNAME  (optional) — defaults to "admin"
"""
from __future__ import annotations

import os

from src.crud.crud_user import create_db_user, read_db_user
from src.db.core import session_local
from src.logging_config import get_logger, setup_logging
from src.models.user import UserCreate

logger = get_logger(__name__)


def bootstrap_admin(db, *, email: str, password: str, username: str = "admin") -> bool:
    """Create an admin user if one with ``email`` doesn't already exist.

    Returns True if a new admin was created, False if it already existed.
    """
    if read_db_user(db, email=email) is not None:
        logger.info("Admin bootstrap: user email=%s already exists, no-op", email)
        return False

    user = create_db_user(
        db,
        UserCreate(
            email=email,
            username=username,
            password=password,
            confirm_password=password,
        ),
    )
    user.is_admin = True
    db.commit()
    logger.info("Admin bootstrap: created admin email=%s username=%s", email, username)
    return True


def main() -> int:
    setup_logging()

    email = os.getenv("ADMIN_EMAIL")
    password = os.getenv("ADMIN_PASSWORD")
    username = os.getenv("ADMIN_USERNAME", "admin")
    if not email or not password:
        print("ERROR: ADMIN_EMAIL and ADMIN_PASSWORD must be set")
        return 1

    db = session_local()
    try:
        created = bootstrap_admin(db, email=email, password=password, username=username)
    finally:
        db.close()

    print(f"admin {'created' if created else 'already existed'}: {email}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
