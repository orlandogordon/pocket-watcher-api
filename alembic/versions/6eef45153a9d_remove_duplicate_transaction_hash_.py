"""Remove duplicate transaction hash constraints

Revision ID: 6eef45153a9d
Revises: 0d6ca885ece1
Create Date: 2025-11-15 00:26:21.123931

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '6eef45153a9d'
down_revision: Union[str, Sequence[str], None] = '0d6ca885ece1'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    # SQLite requires batch mode for constraint modifications
    with op.batch_alter_table('transactions', schema=None) as batch_op:
        batch_op.drop_constraint('uq_user_transaction_hash', type_='unique')

    with op.batch_alter_table('investment_transactions', schema=None) as batch_op:
        batch_op.drop_constraint('uq_user_investment_transaction_hash', type_='unique')


def downgrade() -> None:
    """Downgrade schema."""
    # SQLite requires batch mode for constraint modifications
    with op.batch_alter_table('investment_transactions', schema=None) as batch_op:
        batch_op.create_unique_constraint('uq_user_investment_transaction_hash', ['user_id', 'transaction_hash'])

    with op.batch_alter_table('transactions', schema=None) as batch_op:
        batch_op.create_unique_constraint('uq_user_transaction_hash', ['user_id', 'transaction_hash'])
