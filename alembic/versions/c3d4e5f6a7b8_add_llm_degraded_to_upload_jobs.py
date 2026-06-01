"""Add llm_degraded flag to upload_jobs (#60).

Distinct from needs_review: marks a file imported while the LLM backend was
unreachable, so its rows landed un-enriched.

Revision ID: c3d4e5f6a7b8
Revises: a7b8c9d0e1f2
Create Date: 2026-05-31
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = 'c3d4e5f6a7b8'
down_revision: Union[str, None] = 'a7b8c9d0e1f2'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    with op.batch_alter_table('upload_jobs') as batch_op:
        batch_op.add_column(
            sa.Column('llm_degraded', sa.Boolean(), nullable=False, server_default=sa.false())
        )


def downgrade() -> None:
    with op.batch_alter_table('upload_jobs') as batch_op:
        batch_op.drop_column('llm_degraded')
