"""transfer pairing: cross-table FKs and dismissals table

Revision ID: f2b9c1e4a701
Revises: d35a1b2c3d4e
Create Date: 2026-05-11 00:00:00.000000

Backend todo #39 — transfer classification & pairing.

Schema changes:
1. transaction_relationships gains nullable from_investment_transaction_id /
   to_investment_transaction_id FKs into investment_transactions.db_id. The
   existing from_transaction_id / to_transaction_id columns become nullable.
   CHECK constraints enforce exactly-one-per-side
   ({regular, investment} for each of from/to). This lets a single OFFSETS
   row pair a regular checking TRANSFER_OUT with an investment-account
   DEPOSIT (Schwab/TDA funding case).

   The old uq_transaction_relationship UNIQUE(from_txn, to_txn, rel_type)
   doesn't work with the new nullable shape (SQL treats NULLs as distinct so
   it wouldn't catch cross-table dups anyway). Dedup becomes app-level in
   the pairing pass; partial indexes cover query patterns.

2. New dismissed_transfer_pairs table with the same nullable-FK shape, so
   the suggestion inbox can persist "user said these aren't a pair"
   decisions across cross-table pairs as well as regular-only pairs.
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'f2b9c1e4a701'
down_revision: Union[str, Sequence[str], None] = 'd35a1b2c3d4e'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    with op.batch_alter_table('transaction_relationships', schema=None) as batch_op:
        batch_op.alter_column(
            'from_transaction_id',
            existing_type=sa.Integer(),
            nullable=True,
        )
        batch_op.alter_column(
            'to_transaction_id',
            existing_type=sa.Integer(),
            nullable=True,
        )
        batch_op.add_column(sa.Column(
            'from_investment_transaction_id',
            sa.Integer(),
            sa.ForeignKey(
                'investment_transactions.investment_transaction_id',
                name='fk_rel_from_investment_txn',
            ),
            nullable=True,
        ))
        batch_op.add_column(sa.Column(
            'to_investment_transaction_id',
            sa.Integer(),
            sa.ForeignKey(
                'investment_transactions.investment_transaction_id',
                name='fk_rel_to_investment_txn',
            ),
            nullable=True,
        ))
        batch_op.drop_constraint('uq_transaction_relationship', type_='unique')
        batch_op.create_check_constraint(
            'ck_rel_from_exactly_one',
            '((from_transaction_id IS NOT NULL) + '
            '(from_investment_transaction_id IS NOT NULL)) = 1',
        )
        batch_op.create_check_constraint(
            'ck_rel_to_exactly_one',
            '((to_transaction_id IS NOT NULL) + '
            '(to_investment_transaction_id IS NOT NULL)) = 1',
        )

    op.create_index(
        'idx_rel_from_investment_transaction',
        'transaction_relationships',
        ['from_investment_transaction_id'],
    )
    op.create_index(
        'idx_rel_to_investment_transaction',
        'transaction_relationships',
        ['to_investment_transaction_id'],
    )

    op.create_table(
        'dismissed_transfer_pairs',
        sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
        sa.Column('user_id', sa.Integer(), nullable=False),
        sa.Column('from_transaction_id', sa.Integer(), nullable=True),
        sa.Column('from_investment_transaction_id', sa.Integer(), nullable=True),
        sa.Column('to_transaction_id', sa.Integer(), nullable=True),
        sa.Column('to_investment_transaction_id', sa.Integer(), nullable=True),
        sa.Column('created_at', sa.DateTime(), nullable=False),
        sa.ForeignKeyConstraint(['user_id'], ['users.db_id'], ondelete='CASCADE'),
        sa.ForeignKeyConstraint(['from_transaction_id'], ['transactions.db_id']),
        sa.ForeignKeyConstraint(['from_investment_transaction_id'], ['investment_transactions.investment_transaction_id']),
        sa.ForeignKeyConstraint(['to_transaction_id'], ['transactions.db_id']),
        sa.ForeignKeyConstraint(['to_investment_transaction_id'], ['investment_transactions.investment_transaction_id']),
        sa.PrimaryKeyConstraint('id'),
        sa.CheckConstraint(
            '((from_transaction_id IS NOT NULL) + '
            '(from_investment_transaction_id IS NOT NULL)) = 1',
            name='ck_dismissed_from_exactly_one',
        ),
        sa.CheckConstraint(
            '((to_transaction_id IS NOT NULL) + '
            '(to_investment_transaction_id IS NOT NULL)) = 1',
            name='ck_dismissed_to_exactly_one',
        ),
    )
    op.create_index(
        'idx_dismissed_pairs_user',
        'dismissed_transfer_pairs',
        ['user_id'],
    )
    op.create_index(
        'idx_dismissed_pairs_lookup',
        'dismissed_transfer_pairs',
        [
            'user_id',
            'from_transaction_id',
            'from_investment_transaction_id',
            'to_transaction_id',
            'to_investment_transaction_id',
        ],
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_index('idx_dismissed_pairs_lookup', table_name='dismissed_transfer_pairs')
    op.drop_index('idx_dismissed_pairs_user', table_name='dismissed_transfer_pairs')
    op.drop_table('dismissed_transfer_pairs')

    op.drop_index('idx_rel_to_investment_transaction', table_name='transaction_relationships')
    op.drop_index('idx_rel_from_investment_transaction', table_name='transaction_relationships')

    with op.batch_alter_table('transaction_relationships', schema=None) as batch_op:
        batch_op.drop_constraint('ck_rel_to_exactly_one', type_='check')
        batch_op.drop_constraint('ck_rel_from_exactly_one', type_='check')
        batch_op.drop_column('to_investment_transaction_id')
        batch_op.drop_column('from_investment_transaction_id')
        batch_op.create_unique_constraint(
            'uq_transaction_relationship',
            ['from_transaction_id', 'to_transaction_id', 'relationship_type'],
        )
        batch_op.alter_column(
            'to_transaction_id',
            existing_type=sa.Integer(),
            nullable=False,
        )
        batch_op.alter_column(
            'from_transaction_id',
            existing_type=sa.Integer(),
            nullable=False,
        )
