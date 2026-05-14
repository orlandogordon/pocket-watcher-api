"""add match_aliases JSON column to accounts

Revision ID: a7c3b91e5012
Revises: c4d8e3f10287
Create Date: 2026-05-14 22:30:00.000000

User-supplied alternative match strings for the transfer classifier.
Lets a user tell the classifier that 'AMZ_STORECRD' on a TD statement
means their 'Amazon Store Card' account, without relying on generic
single-word tokens like 'STORE' that false-positive on unrelated
merchants (e.g. DERMSTORECOM, WALGREENSSTORE).
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = 'a7c3b91e5012'
down_revision: Union[str, Sequence[str], None] = 'c4d8e3f10287'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Add match_aliases JSON column to accounts (defaults to empty list)."""
    op.add_column(
        'accounts',
        sa.Column('match_aliases', sa.JSON(), nullable=True),
    )


def downgrade() -> None:
    op.drop_column('accounts', 'match_aliases')
