"""widen snapshot review_reason and recalculation_reason to text

Revision ID: 997278985314
Revises: b1c2d3e4f5a6
Create Date: 2026-06-10 16:00:53.528317

review_reason enumerates every stale holding ("[stale-options] A, B, C, ..."),
which overflows VARCHAR(255) on options-heavy accounts. On Postgres that raises
StringDataRightTruncation, the snapshot INSERT fails, and the day is silently
dropped from the backfill. Widen both reason columns to TEXT so reasons of any
length persist. SQLite stores String(255) and Text identically (TEXT affinity,
no length enforced), so the ALTER is Postgres-only.
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '997278985314'
down_revision: Union[str, Sequence[str], None] = 'b1c2d3e4f5a6'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Widen review_reason / recalculation_reason from VARCHAR(255) to TEXT."""
    if op.get_bind().dialect.name != "postgresql":
        return
    op.alter_column(
        "account_value_history", "review_reason",
        existing_type=sa.String(length=255), type_=sa.Text(), existing_nullable=True,
    )
    op.alter_column(
        "account_value_history", "recalculation_reason",
        existing_type=sa.String(length=255), type_=sa.Text(), existing_nullable=True,
    )


def downgrade() -> None:
    """Narrow back to VARCHAR(255), truncating any over-length values."""
    if op.get_bind().dialect.name != "postgresql":
        return
    op.alter_column(
        "account_value_history", "recalculation_reason",
        existing_type=sa.Text(), type_=sa.String(length=255), existing_nullable=True,
        postgresql_using="left(recalculation_reason, 255)",
    )
    op.alter_column(
        "account_value_history", "review_reason",
        existing_type=sa.Text(), type_=sa.String(length=255), existing_nullable=True,
        postgresql_using="left(review_reason, 255)",
    )
