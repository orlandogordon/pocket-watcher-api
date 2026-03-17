"""Remove duplicate transaction hash constraints

Revision ID: 6eef45153a9d
Revises: 0d6ca885ece1
Create Date: 2025-11-15 00:26:21.123931

NOTE: Folded into initial migration. This is now a no-op.
"""
from typing import Sequence, Union

# revision identifiers, used by Alembic.
revision: str = '6eef45153a9d'
down_revision: Union[str, Sequence[str], None] = '0d6ca885ece1'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    pass


def downgrade() -> None:
    pass
