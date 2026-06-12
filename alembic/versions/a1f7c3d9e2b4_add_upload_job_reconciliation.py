"""add upload_job statement-reconciliation warning columns

Revision ID: a1f7c3d9e2b4
Revises: 997278985314
Create Date: 2026-06-12 16:20:00.000000

Statement-level reconciliation (#78): when a parser's rows don't sum to the
statement's own begin->end balance move, the import still completes but is
flagged. Mirrors llm_degraded as a non-blocking signal; delta/detail carry the
"off by $X" numbers for the UI badge tooltip. Adding columns is portable, so no
dialect guard is needed; reconciliation_warning is NOT NULL with a server
default so existing rows backfill to False.
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'a1f7c3d9e2b4'
down_revision: Union[str, Sequence[str], None] = '997278985314'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "upload_jobs",
        sa.Column(
            "reconciliation_warning",
            sa.Boolean(),
            nullable=False,
            server_default=sa.false(),
        ),
    )
    op.add_column(
        "upload_jobs",
        sa.Column("reconciliation_delta", sa.DECIMAL(precision=15, scale=2), nullable=True),
    )
    op.add_column(
        "upload_jobs",
        sa.Column("reconciliation_detail", sa.Text(), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("upload_jobs", "reconciliation_detail")
    op.drop_column("upload_jobs", "reconciliation_delta")
    op.drop_column("upload_jobs", "reconciliation_warning")
