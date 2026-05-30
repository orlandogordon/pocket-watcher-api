"""add public uuid to snapshot_backfill_jobs (#58 phase 3)

Snapshot-backfill jobs were the last public resource addressed by their integer
PK in the URL. Add a ``uuid`` column (the public id, matching every other
table) so ``/accounts/{account_uuid}/snapshot-jobs/{job_uuid}`` is UUID-keyed.
Existing rows are backfilled with fresh UUIDs before the NOT NULL + UNIQUE
constraints are applied.

Revision ID: a7b8c9d0e1f2
Revises: f1a2b3c4d5e6
Create Date: 2026-05-29
"""
from uuid import uuid4

import sqlalchemy as sa
from alembic import op

revision = "a7b8c9d0e1f2"
down_revision = "f1a2b3c4d5e6"
branch_labels = None
depends_on = None

jobs_tbl = sa.table(
    "snapshot_backfill_jobs",
    sa.column("db_id", sa.Integer),
    sa.column("uuid", sa.Uuid),
)


def upgrade() -> None:
    op.add_column("snapshot_backfill_jobs", sa.Column("uuid", sa.Uuid(), nullable=True))

    conn = op.get_bind()
    for (db_id,) in conn.execute(sa.select(jobs_tbl.c.db_id)).fetchall():
        conn.execute(
            jobs_tbl.update().where(jobs_tbl.c.db_id == db_id).values(uuid=uuid4())
        )

    with op.batch_alter_table("snapshot_backfill_jobs") as batch:
        batch.alter_column("uuid", existing_type=sa.Uuid(), nullable=False)
        batch.create_unique_constraint("uq_snapshot_backfill_jobs_uuid", ["uuid"])


def downgrade() -> None:
    with op.batch_alter_table("snapshot_backfill_jobs") as batch:
        batch.drop_constraint("uq_snapshot_backfill_jobs_uuid", type_="unique")
    op.drop_column("snapshot_backfill_jobs", "uuid")
