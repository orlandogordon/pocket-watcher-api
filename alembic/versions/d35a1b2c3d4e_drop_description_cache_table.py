"""Drop description_cache table

Revision ID: d35a1b2c3d4e
Revises: c1a2b3d4e5f6
Create Date: 2026-05-01 00:00:00.000000

Backend todo #35 removed LLM description cleanup in favor of preserving raw
parser descriptions verbatim. The DescriptionCacheDB table cached cleaned
strings; with no cleaned strings to cache, the table has no semantics. Per
#35's analysis: regex extractor + per-batch LLM dedup cover the head and
near-duplicate cases without persistent caching.
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'd35a1b2c3d4e'
down_revision: Union[str, Sequence[str], None] = 'c1a2b3d4e5f6'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.drop_index("idx_description_cache_user_hash", table_name="description_cache")
    op.drop_table("description_cache")


def downgrade() -> None:
    """Downgrade schema — recreate the table empty (cached data is not
    recoverable)."""
    op.create_table(
        "description_cache",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("user_id", sa.Integer(), nullable=False),
        sa.Column("description_hash", sa.String(length=64), nullable=False),
        sa.Column("raw_description", sa.String(length=500), nullable=False),
        sa.Column("cleaned_description", sa.String(length=500), nullable=False),
        sa.Column("source", sa.String(length=20), nullable=False),
        sa.Column("llm_model", sa.String(length=100), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.ForeignKeyConstraint(["user_id"], ["users.db_id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("user_id", "description_hash", name="uq_description_cache_user_hash"),
    )
    op.create_index(
        "idx_description_cache_user_hash",
        "description_cache",
        ["user_id", "description_hash"],
    )
