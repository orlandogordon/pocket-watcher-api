"""add_uuid_to_all_models

Revision ID: a1b2c3d4e5f6
Revises: 755b84c702fe
Create Date: 2026-02-16

NOTE: Folded into initial migration. This is now a no-op.
"""
from typing import Sequence, Union

# revision identifiers, used by Alembic.
revision: str = 'a1b2c3d4e5f6'
down_revision: Union[str, Sequence[str], None] = '755b84c702fe'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    pass


def downgrade() -> None:
    pass
