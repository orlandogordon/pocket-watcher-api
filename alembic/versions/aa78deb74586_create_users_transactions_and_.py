"""create users, transactions, and investments tables

Revision ID: aa78deb74586
Revises: 
Create Date: 2025-07-05 13:59:18.023282

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'aa78deb74586'
down_revision: Union[str, Sequence[str], None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        'users',
        sa.Column('id', sa.Integer, primary_key=True, autoincrement=True),
        sa.Column('name', sa.String, nullable=False),
        sa.Column('updated_at', sa.DateTime, nullable=True, default=None),
        sa.Column('created_at', sa.DateTime, nullable=False, default=sa.func.now()),
    )
    op.create_table(
        'transactions',
        sa.Column('id', sa.Integer, primary_key=True, autoincrement=True),
        sa.Column('user_id', sa.Integer, sa.ForeignKey('users.id'), nullable=False),
        sa.Column('date', sa.DateTime, nullable=False),
        sa.Column('description', sa.String, nullable=False),
        sa.Column('category', sa.String, nullable=False),
        sa.Column('amount', sa.Float, nullable=False),
        sa.Column('transaction_type', sa.String, nullable=False),  # e.g., "income", "expense"
        sa.Column('bank_name', sa.String, nullable=False),
        sa.Column('account_holder', sa.String, nullable=False),
        sa.Column('account_number', sa.Integer, nullable=False),
        sa.Column('created_at', sa.DateTime, nullable=False, default=sa.func.now()),
        sa.Column('updated_at', sa.DateTime, nullable=True, default=None),
    )
    op.create_table(
        'investments',
        sa.Column('id', sa.Integer, primary_key=True, autoincrement=True),
        sa.Column('user_id', sa.Integer, sa.ForeignKey('users.id'), nullable=False),
        sa.Column('date', sa.DateTime, nullable=False),
        sa.Column('transaction_type', sa.String, nullable=False),  # e.g., "buy", "sell"
        sa.Column('symbol', sa.String, nullable=False),
        sa.Column('description', sa.String, nullable=False),
        sa.Column('quantity', sa.Float, nullable=False),
        sa.Column('price_per_unit', sa.Float, nullable=False),
        sa.Column('total_value', sa.Float, nullable=False),
        sa.Column('brokerage_name', sa.String, nullable=False),
        sa.Column('account_holder', sa.String, nullable=True, default=None),
        sa.Column('account_number', sa.Integer, nullable=False),
    )


def downgrade() -> None:
    op.drop_table('users')
    op.drop_table('transactions')
    op.drop_table('investments')
