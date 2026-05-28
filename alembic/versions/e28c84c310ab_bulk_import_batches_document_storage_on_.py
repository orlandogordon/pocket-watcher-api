"""bulk import batches + document storage on upload_jobs (#59)

Revision ID: e28c84c310ab
Revises: e1b7c89a4f02
Create Date: 2026-05-27 16:02:41.358436

"""
from typing import Sequence, Union
from uuid import uuid4

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'e28c84c310ab'
down_revision: Union[str, Sequence[str], None] = 'e1b7c89a4f02'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    # Batch grouping for bulk imports (#59).
    op.create_table(
        "bulk_import_batches",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("uuid", sa.Uuid(), nullable=False),
        sa.Column("user_id", sa.Integer(), sa.ForeignKey("users.db_id"), nullable=False),
        sa.Column("status", sa.String(length=20), nullable=False, server_default="PENDING"),
        sa.Column("total_files", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("processed_files", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("error_message", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("completed_at", sa.DateTime(), nullable=True),
        sa.UniqueConstraint("uuid", name="uq_bulk_batches_uuid"),
    )
    op.create_index("idx_bulk_batches_user", "bulk_import_batches", ["user_id"])
    op.create_index("idx_bulk_batches_status", "bulk_import_batches", ["status"])
    op.create_index("idx_bulk_batches_created", "bulk_import_batches", ["created_at"])

    # Document-storage columns on upload_jobs. uuid is added nullable so existing
    # rows can be backfilled, then a unique index is created.
    op.add_column("upload_jobs", sa.Column("uuid", sa.Uuid(), nullable=True))
    # batch_id added without a DB-level FK: SQLite can't ALTER-add a constraint
    # outside batch mode. The model keeps the ForeignKey (fresh create_all + the
    # ORM relationship still have it); the C1 migration squash will formalize it.
    op.add_column("upload_jobs", sa.Column("batch_id", sa.Integer(), nullable=True))
    op.add_column("upload_jobs", sa.Column("storage_key", sa.String(length=500), nullable=True))
    op.add_column("upload_jobs", sa.Column("file_size", sa.Integer(), nullable=True))
    op.add_column("upload_jobs", sa.Column("content_type", sa.String(length=100), nullable=True))
    op.add_column("upload_jobs", sa.Column("needs_review", sa.Integer(), nullable=False, server_default="0"))

    conn = op.get_bind()
    rows = conn.execute(sa.text("SELECT id FROM upload_jobs WHERE uuid IS NULL")).fetchall()
    for (job_id,) in rows:
        conn.execute(
            sa.text("UPDATE upload_jobs SET uuid = :u WHERE id = :id"),
            {"u": uuid4().hex, "id": job_id},
        )

    op.create_index("uq_upload_jobs_uuid", "upload_jobs", ["uuid"], unique=True)
    op.create_index("idx_upload_jobs_batch", "upload_jobs", ["batch_id"])

    # Link imported transactions back to their source document (#59) so a
    # document delete can cascade to the rows it created. Plain columns (no
    # DB-level FK) for the same SQLite reason as batch_id above.
    op.add_column("transactions", sa.Column("upload_job_id", sa.Integer(), nullable=True))
    op.add_column("investment_transactions", sa.Column("upload_job_id", sa.Integer(), nullable=True))
    op.create_index("idx_transactions_upload_job", "transactions", ["upload_job_id"])
    op.create_index("idx_investment_transactions_upload_job", "investment_transactions", ["upload_job_id"])


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_index("idx_investment_transactions_upload_job", table_name="investment_transactions")
    op.drop_index("idx_transactions_upload_job", table_name="transactions")
    op.drop_column("investment_transactions", "upload_job_id")
    op.drop_column("transactions", "upload_job_id")

    op.drop_column("upload_jobs", "needs_review")
    op.drop_index("idx_upload_jobs_batch", table_name="upload_jobs")
    op.drop_index("uq_upload_jobs_uuid", table_name="upload_jobs")
    op.drop_column("upload_jobs", "content_type")
    op.drop_column("upload_jobs", "file_size")
    op.drop_column("upload_jobs", "storage_key")
    op.drop_column("upload_jobs", "batch_id")
    op.drop_column("upload_jobs", "uuid")

    op.drop_index("idx_bulk_batches_created", table_name="bulk_import_batches")
    op.drop_index("idx_bulk_batches_status", table_name="bulk_import_batches")
    op.drop_index("idx_bulk_batches_user", table_name="bulk_import_batches")
    op.drop_table("bulk_import_batches")
