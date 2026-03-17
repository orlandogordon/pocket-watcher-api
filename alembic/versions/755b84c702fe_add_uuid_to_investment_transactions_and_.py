"""add_uuid_to_investment_transactions_and_upload_job_tracking

Revision ID: 755b84c702fe
Revises: b893f8c1bcbb
Create Date: 2025-11-23 02:44:22.234457

NOTE: Folded into initial migration. This is now a no-op.
"""
from typing import Sequence, Union

# revision identifiers, used by Alembic.
revision: str = '755b84c702fe'
down_revision: Union[str, Sequence[str], None] = 'b893f8c1bcbb'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    pass


def downgrade() -> None:
    pass
