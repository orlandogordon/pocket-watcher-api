"""add_historical_snapshot_backfill_schema

Revision ID: b893f8c1bcbb
Revises: 6eef45153a9d
Create Date: 2025-11-22 00:48:56.691483

NOTE: Folded into initial migration. This is now a no-op.
"""
from typing import Sequence, Union

# revision identifiers, used by Alembic.
revision: str = 'b893f8c1bcbb'
down_revision: Union[str, Sequence[str], None] = '6eef45153a9d'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    pass


def downgrade() -> None:
    pass
