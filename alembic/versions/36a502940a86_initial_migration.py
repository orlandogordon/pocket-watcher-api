"""Initial migration

Revision ID: 36a502940a86
Revises:
Create Date: 2025-10-11 04:20:43.809126

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '36a502940a86'
down_revision: Union[str, Sequence[str], None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema — consolidated initial migration matching current models."""

    # ===== Independent tables (no FKs) =====

    op.create_table('categories',
        sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
        sa.Column('uuid', sa.Uuid(), nullable=False),
        sa.Column('name', sa.String(length=100), nullable=False),
        sa.Column('parent_category_id', sa.Integer(), nullable=True),
        sa.ForeignKeyConstraint(['parent_category_id'], ['categories.id']),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('uuid'),
        sa.UniqueConstraint('name', name='uq_category_name'),
    )
    op.create_index('idx_category_name', 'categories', ['name'], unique=False)

    op.create_table('users',
        sa.Column('db_id', sa.Integer(), autoincrement=True, nullable=False),
        sa.Column('id', sa.Uuid(), nullable=False),
        sa.Column('email', sa.String(length=255), nullable=False),
        sa.Column('username', sa.String(length=100), nullable=False),
        sa.Column('password_hash', sa.String(length=255), nullable=False),
        sa.Column('first_name', sa.String(length=100), nullable=True),
        sa.Column('last_name', sa.String(length=100), nullable=True),
        sa.Column('date_of_birth', sa.Date(), nullable=True),
        sa.Column('last_login_at', sa.DateTime(), nullable=True),
        sa.Column('created_at', sa.DateTime(), nullable=False),
        sa.Column('updated_at', sa.DateTime(), nullable=False),
        sa.PrimaryKeyConstraint('db_id'),
        sa.UniqueConstraint('id'),
        sa.UniqueConstraint('email', name='uq_user_email'),
        sa.UniqueConstraint('username', name='uq_user_username'),
    )
    op.create_index('idx_users_email', 'users', ['email'], unique=False)

    # ===== Tables depending on users =====

    op.create_table('accounts',
        sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
        sa.Column('uuid', sa.Uuid(), nullable=False),
        sa.Column('user_id', sa.Integer(), nullable=False),
        sa.Column('account_name', sa.String(length=255), nullable=False),
        sa.Column('account_type', sa.Enum('CHECKING', 'SAVINGS', 'CREDIT_CARD', 'INVESTMENT', 'LOAN', 'OTHER', name='accounttype'), nullable=False),
        sa.Column('institution_name', sa.String(length=255), nullable=False),
        sa.Column('account_number_last4', sa.String(length=4), nullable=True),
        sa.Column('original_principal', sa.DECIMAL(precision=15, scale=2), nullable=True),
        sa.Column('minimum_payment', sa.DECIMAL(precision=15, scale=2), nullable=True),
        sa.Column('interest_rate', sa.DECIMAL(precision=5, scale=4), nullable=True),
        sa.Column('interest_rate_type', sa.String(length=20), nullable=True),
        sa.Column('initial_cash_balance', sa.DECIMAL(precision=15, scale=2), nullable=False, server_default='0.00'),
        sa.Column('balance', sa.DECIMAL(precision=15, scale=2), nullable=False),
        sa.Column('balance_last_updated', sa.DateTime(), nullable=True),
        sa.Column('comments', sa.Text(), nullable=True),
        sa.Column('created_at', sa.DateTime(), nullable=False),
        sa.Column('updated_at', sa.DateTime(), nullable=False),
        sa.ForeignKeyConstraint(['user_id'], ['users.db_id']),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('uuid'),
        sa.UniqueConstraint('user_id', 'account_name', name='uq_user_account_name'),
    )

    op.create_table('budget_templates',
        sa.Column('template_id', sa.Integer(), autoincrement=True, nullable=False),
        sa.Column('id', sa.Uuid(), nullable=False),
        sa.Column('user_id', sa.Integer(), nullable=False),
        sa.Column('template_name', sa.String(length=255), nullable=False),
        sa.Column('is_default', sa.Boolean(), nullable=False, server_default='0'),
        sa.Column('created_at', sa.DateTime(), nullable=False),
        sa.Column('updated_at', sa.DateTime(), nullable=False),
        sa.ForeignKeyConstraint(['user_id'], ['users.db_id']),
        sa.PrimaryKeyConstraint('template_id'),
        sa.UniqueConstraint('id'),
        sa.UniqueConstraint('user_id', 'template_name', name='uq_user_template_name'),
    )

    op.create_table('debt_repayment_plans',
        sa.Column('plan_id', sa.Integer(), autoincrement=True, nullable=False),
        sa.Column('id', sa.Uuid(), nullable=False),
        sa.Column('user_id', sa.Integer(), nullable=False),
        sa.Column('plan_name', sa.String(length=255), nullable=False),
        sa.Column('strategy', sa.Enum('AVALANCHE', 'SNOWBALL', 'CUSTOM', name='debtstrategy'), nullable=False),
        sa.Column('target_payoff_date', sa.Date(), nullable=True),
        sa.Column('status', sa.String(length=50), nullable=False),
        sa.Column('created_at', sa.DateTime(), nullable=False),
        sa.Column('updated_at', sa.DateTime(), nullable=False),
        sa.ForeignKeyConstraint(['user_id'], ['users.db_id']),
        sa.PrimaryKeyConstraint('plan_id'),
        sa.UniqueConstraint('id'),
        sa.UniqueConstraint('user_id', 'plan_name', name='uq_user_debt_plan_name'),
    )

    op.create_table('financial_plans',
        sa.Column('plan_id', sa.Integer(), autoincrement=True, nullable=False),
        sa.Column('id', sa.Uuid(), nullable=False),
        sa.Column('user_id', sa.Integer(), nullable=False),
        sa.Column('plan_name', sa.String(length=255), nullable=False),
        sa.Column('start_date', sa.Date(), nullable=False),
        sa.Column('end_date', sa.Date(), nullable=False),
        sa.Column('created_at', sa.DateTime(), nullable=False),
        sa.Column('updated_at', sa.DateTime(), nullable=False),
        sa.ForeignKeyConstraint(['user_id'], ['users.db_id']),
        sa.PrimaryKeyConstraint('plan_id'),
        sa.UniqueConstraint('id'),
        sa.UniqueConstraint('user_id', 'plan_name', name='uq_user_financial_plan_name'),
    )

    op.create_table('tags',
        sa.Column('tag_id', sa.Integer(), autoincrement=True, nullable=False),
        sa.Column('id', sa.Uuid(), nullable=False),
        sa.Column('user_id', sa.Integer(), nullable=False),
        sa.Column('tag_name', sa.String(length=100), nullable=False),
        sa.Column('color', sa.String(length=7), nullable=True),
        sa.Column('is_system', sa.Boolean(), nullable=False, server_default='0'),
        sa.Column('created_at', sa.DateTime(), nullable=False),
        sa.ForeignKeyConstraint(['user_id'], ['users.db_id']),
        sa.PrimaryKeyConstraint('tag_id'),
        sa.UniqueConstraint('id'),
        sa.UniqueConstraint('user_id', 'tag_name', name='uq_user_tag_name'),
    )

    # ===== Tables depending on accounts =====

    op.create_table('account_value_history',
        sa.Column('snapshot_id', sa.Integer(), autoincrement=True, nullable=False),
        sa.Column('uuid', sa.Uuid(), nullable=False),
        sa.Column('account_id', sa.Integer(), nullable=False),
        sa.Column('value_date', sa.Date(), nullable=False),
        sa.Column('balance', sa.DECIMAL(precision=15, scale=2), nullable=False),
        sa.Column('securities_value', sa.DECIMAL(precision=15, scale=2), nullable=True),
        sa.Column('cash_balance', sa.DECIMAL(precision=15, scale=2), nullable=True),
        sa.Column('total_cost_basis', sa.DECIMAL(precision=15, scale=2), nullable=True),
        sa.Column('unrealized_gain_loss', sa.DECIMAL(precision=15, scale=2), nullable=True),
        sa.Column('realized_gain_loss', sa.DECIMAL(precision=15, scale=2), nullable=True),
        sa.Column('principal_paid_ytd', sa.DECIMAL(precision=15, scale=2), nullable=True),
        sa.Column('interest_paid_ytd', sa.DECIMAL(precision=15, scale=2), nullable=True),
        sa.Column('snapshot_source', sa.String(length=50), nullable=False),
        sa.Column('last_recalculated_at', sa.DateTime(), nullable=True),
        sa.Column('recalculation_count', sa.Integer(), nullable=False, server_default='0'),
        sa.Column('recalculation_reason', sa.String(length=255), nullable=True),
        sa.Column('needs_review', sa.Boolean(), nullable=False, server_default='0'),
        sa.Column('review_reason', sa.String(length=255), nullable=True),
        sa.Column('created_at', sa.DateTime(), nullable=False),
        sa.ForeignKeyConstraint(['account_id'], ['accounts.id']),
        sa.PrimaryKeyConstraint('snapshot_id'),
        sa.UniqueConstraint('account_id', 'value_date', name='uq_account_value_date'),
        sa.UniqueConstraint('uuid', name='uq_account_value_history_uuid'),
    )
    op.create_index('idx_account_value_account', 'account_value_history', ['account_id'], unique=False)
    op.create_index('idx_account_value_account_date', 'account_value_history', ['account_id', 'value_date'], unique=False)
    op.create_index('idx_account_value_date', 'account_value_history', ['value_date'], unique=False)

    op.create_table('snapshot_backfill_jobs',
        sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
        sa.Column('user_id', sa.Integer(), nullable=False),
        sa.Column('account_id', sa.Integer(), nullable=False),
        sa.Column('start_date', sa.Date(), nullable=False),
        sa.Column('end_date', sa.Date(), nullable=False),
        sa.Column('status', sa.String(length=20), nullable=False),
        sa.Column('error_message', sa.Text(), nullable=True),
        sa.Column('created_at', sa.DateTime(), nullable=False, server_default=sa.text('CURRENT_TIMESTAMP')),
        sa.Column('started_at', sa.DateTime(), nullable=True),
        sa.Column('completed_at', sa.DateTime(), nullable=True),
        sa.Column('snapshots_created', sa.Integer(), nullable=False, server_default='0'),
        sa.Column('snapshots_updated', sa.Integer(), nullable=False, server_default='0'),
        sa.Column('snapshots_failed', sa.Integer(), nullable=False, server_default='0'),
        sa.Column('snapshots_skipped', sa.Integer(), nullable=False, server_default='0'),
        sa.ForeignKeyConstraint(['user_id'], ['users.db_id']),
        sa.ForeignKeyConstraint(['account_id'], ['accounts.id']),
        sa.PrimaryKeyConstraint('id'),
    )
    op.create_index('idx_backfill_jobs_account', 'snapshot_backfill_jobs', ['account_id'], unique=False)
    op.create_index('idx_backfill_jobs_status', 'snapshot_backfill_jobs', ['status'], unique=False)
    op.create_index('idx_backfill_jobs_created', 'snapshot_backfill_jobs', ['created_at'], unique=False)

    op.create_table('budget_template_categories',
        sa.Column('allocation_id', sa.Integer(), autoincrement=True, nullable=False),
        sa.Column('id', sa.Uuid(), nullable=False),
        sa.Column('template_id', sa.Integer(), nullable=False),
        sa.Column('category_id', sa.Integer(), nullable=False),
        sa.Column('subcategory_id', sa.Integer(), nullable=True),
        sa.Column('allocated_amount', sa.DECIMAL(precision=15, scale=2), nullable=False),
        sa.Column('created_at', sa.DateTime(), nullable=False),
        sa.ForeignKeyConstraint(['template_id'], ['budget_templates.template_id']),
        sa.ForeignKeyConstraint(['category_id'], ['categories.id']),
        sa.ForeignKeyConstraint(['subcategory_id'], ['categories.id']),
        sa.PrimaryKeyConstraint('allocation_id'),
        sa.UniqueConstraint('id'),
        sa.UniqueConstraint('template_id', 'category_id', 'subcategory_id', name='uq_template_category_sub'),
    )

    op.create_table('budget_months',
        sa.Column('month_id', sa.Integer(), autoincrement=True, nullable=False),
        sa.Column('id', sa.Uuid(), nullable=False),
        sa.Column('user_id', sa.Integer(), nullable=False),
        sa.Column('template_id', sa.Integer(), nullable=True),
        sa.Column('year', sa.Integer(), nullable=False),
        sa.Column('month', sa.Integer(), nullable=False),
        sa.Column('created_at', sa.DateTime(), nullable=False),
        sa.Column('updated_at', sa.DateTime(), nullable=False),
        sa.ForeignKeyConstraint(['user_id'], ['users.db_id']),
        sa.ForeignKeyConstraint(['template_id'], ['budget_templates.template_id']),
        sa.PrimaryKeyConstraint('month_id'),
        sa.UniqueConstraint('id'),
        sa.UniqueConstraint('user_id', 'year', 'month', name='uq_user_year_month'),
    )
    op.create_index('idx_budget_months_user', 'budget_months', ['user_id'], unique=False)
    op.create_index('idx_budget_months_template', 'budget_months', ['template_id'], unique=False)

    op.create_table('debt_plan_account_links',
        sa.Column('plan_id', sa.Integer(), nullable=False),
        sa.Column('account_id', sa.Integer(), nullable=False),
        sa.Column('priority', sa.Integer(), nullable=False),
        sa.ForeignKeyConstraint(['account_id'], ['accounts.id']),
        sa.ForeignKeyConstraint(['plan_id'], ['debt_repayment_plans.plan_id']),
        sa.PrimaryKeyConstraint('plan_id', 'account_id'),
    )

    op.create_table('debt_repayment_schedules',
        sa.Column('schedule_id', sa.Integer(), autoincrement=True, nullable=False),
        sa.Column('id', sa.Uuid(), nullable=False),
        sa.Column('user_id', sa.Integer(), nullable=False),
        sa.Column('account_id', sa.Integer(), nullable=False),
        sa.Column('payment_month', sa.Date(), nullable=False),
        sa.Column('scheduled_payment_amount', sa.DECIMAL(precision=15, scale=2), nullable=False),
        sa.ForeignKeyConstraint(['account_id'], ['accounts.id']),
        sa.ForeignKeyConstraint(['user_id'], ['users.db_id']),
        sa.PrimaryKeyConstraint('schedule_id'),
        sa.UniqueConstraint('id'),
        sa.UniqueConstraint('user_id', 'account_id', 'payment_month', name='uq_user_account_month_payment'),
    )

    op.create_table('financial_plan_months',
        sa.Column('month_id', sa.Integer(), autoincrement=True, nullable=False),
        sa.Column('id', sa.Uuid(), nullable=False),
        sa.Column('plan_id', sa.Integer(), nullable=False),
        sa.Column('year', sa.Integer(), nullable=False),
        sa.Column('month', sa.Integer(), nullable=False),
        sa.Column('planned_income', sa.DECIMAL(precision=15, scale=2), nullable=False),
        sa.Column('created_at', sa.DateTime(), nullable=False),
        sa.ForeignKeyConstraint(['plan_id'], ['financial_plans.plan_id']),
        sa.PrimaryKeyConstraint('month_id'),
        sa.UniqueConstraint('id'),
        sa.UniqueConstraint('plan_id', 'year', 'month', name='uq_plan_year_month'),
    )

    op.create_table('investment_holdings',
        sa.Column('holding_id', sa.Integer(), autoincrement=True, nullable=False),
        sa.Column('id', sa.Uuid(), nullable=False),
        sa.Column('account_id', sa.Integer(), nullable=False),
        sa.Column('symbol', sa.String(length=20), nullable=False),
        sa.Column('quantity', sa.DECIMAL(precision=15, scale=6), nullable=False),
        sa.Column('average_cost_basis', sa.DECIMAL(precision=15, scale=4), nullable=True),
        sa.Column('current_price', sa.DECIMAL(precision=15, scale=4), nullable=True),
        sa.Column('last_price_update', sa.DateTime(), nullable=True),
        sa.Column('security_type', sa.String(length=20), nullable=True),
        sa.Column('underlying_symbol', sa.String(length=10), nullable=True),
        sa.Column('option_type', sa.String(length=4), nullable=True),
        sa.Column('strike_price', sa.DECIMAL(precision=15, scale=2), nullable=True),
        sa.Column('expiration_date', sa.Date(), nullable=True),
        sa.Column('created_at', sa.DateTime(), nullable=False),
        sa.Column('updated_at', sa.DateTime(), nullable=False),
        sa.ForeignKeyConstraint(['account_id'], ['accounts.id']),
        sa.PrimaryKeyConstraint('holding_id'),
        sa.UniqueConstraint('id'),
        sa.UniqueConstraint('account_id', 'symbol', name='uq_account_symbol'),
    )
    op.create_index('idx_holdings_account', 'investment_holdings', ['account_id'], unique=False)
    op.create_index('idx_holdings_symbol', 'investment_holdings', ['symbol'], unique=False)

    op.create_table('transactions',
        sa.Column('db_id', sa.Integer(), autoincrement=True, nullable=False),
        sa.Column('id', sa.Uuid(), nullable=False),
        sa.Column('user_id', sa.Integer(), nullable=False),
        sa.Column('account_id', sa.Integer(), nullable=True),
        sa.Column('category_id', sa.Integer(), nullable=True),
        sa.Column('subcategory_id', sa.Integer(), nullable=True),
        sa.Column('transaction_hash', sa.String(length=64), nullable=False),
        sa.Column('source_type', sa.Enum('CSV', 'PDF', 'MANUAL', 'API', name='sourcetype'), nullable=False),
        sa.Column('transaction_date', sa.Date(), nullable=False),
        sa.Column('amount', sa.DECIMAL(precision=15, scale=2), nullable=False),
        sa.Column('transaction_type', sa.Enum('PURCHASE', 'CREDIT', 'TRANSFER_IN', 'TRANSFER_OUT', 'DEPOSIT', 'WITHDRAWAL', 'FEE', 'INTEREST', name='transactiontype'), nullable=False),
        sa.Column('description', sa.String(length=500), nullable=True),
        sa.Column('merchant_name', sa.String(length=255), nullable=True),
        sa.Column('comments', sa.Text(), nullable=True),
        sa.Column('created_at', sa.DateTime(), nullable=False),
        sa.Column('updated_at', sa.DateTime(), nullable=False),
        sa.ForeignKeyConstraint(['account_id'], ['accounts.id']),
        sa.ForeignKeyConstraint(['category_id'], ['categories.id']),
        sa.ForeignKeyConstraint(['subcategory_id'], ['categories.id']),
        sa.ForeignKeyConstraint(['user_id'], ['users.db_id']),
        sa.PrimaryKeyConstraint('db_id'),
        sa.UniqueConstraint('id'),
    )
    op.create_index('idx_transactions_date', 'transactions', ['transaction_date'], unique=False)
    op.create_index('idx_transactions_user_account', 'transactions', ['user_id', 'account_id'], unique=False)
    op.create_index('idx_transactions_user_date', 'transactions', ['user_id', 'transaction_date'], unique=False)

    # ===== Tables depending on transactions / accounts / holdings =====

    op.create_table('debt_payments',
        sa.Column('payment_id', sa.Integer(), autoincrement=True, nullable=False),
        sa.Column('id', sa.Uuid(), nullable=False),
        sa.Column('loan_account_id', sa.Integer(), nullable=False),
        sa.Column('payment_source_account_id', sa.Integer(), nullable=True),
        sa.Column('transaction_id', sa.Integer(), nullable=True),
        sa.Column('payment_amount', sa.DECIMAL(precision=15, scale=2), nullable=False),
        sa.Column('principal_amount', sa.DECIMAL(precision=15, scale=2), nullable=True),
        sa.Column('interest_amount', sa.DECIMAL(precision=15, scale=2), nullable=True),
        sa.Column('remaining_balance_after_payment', sa.DECIMAL(precision=15, scale=2), nullable=True),
        sa.Column('payment_date', sa.Date(), nullable=False),
        sa.Column('description', sa.String(length=500), nullable=True),
        sa.Column('created_at', sa.DateTime(), nullable=False),
        sa.ForeignKeyConstraint(['loan_account_id'], ['accounts.id']),
        sa.ForeignKeyConstraint(['payment_source_account_id'], ['accounts.id']),
        sa.ForeignKeyConstraint(['transaction_id'], ['transactions.db_id']),
        sa.PrimaryKeyConstraint('payment_id'),
        sa.UniqueConstraint('id'),
    )
    op.create_index('idx_debt_payments_date', 'debt_payments', ['payment_date'], unique=False)
    op.create_index('idx_debt_payments_loan_account', 'debt_payments', ['loan_account_id'], unique=False)
    op.create_index('idx_debt_payments_source_account', 'debt_payments', ['payment_source_account_id'], unique=False)
    op.create_index('idx_debt_payments_transaction', 'debt_payments', ['transaction_id'], unique=False)

    op.create_table('financial_plan_expenses',
        sa.Column('expense_id', sa.Integer(), autoincrement=True, nullable=False),
        sa.Column('id', sa.Uuid(), nullable=False),
        sa.Column('month_id', sa.Integer(), nullable=False),
        sa.Column('category_id', sa.Integer(), nullable=False),
        sa.Column('description', sa.String(length=255), nullable=False),
        sa.Column('amount', sa.DECIMAL(precision=15, scale=2), nullable=False),
        sa.Column('expense_type', sa.String(length=20), nullable=False),
        sa.Column('created_at', sa.DateTime(), nullable=False),
        sa.ForeignKeyConstraint(['category_id'], ['categories.id']),
        sa.ForeignKeyConstraint(['month_id'], ['financial_plan_months.month_id']),
        sa.PrimaryKeyConstraint('expense_id'),
        sa.UniqueConstraint('id'),
    )

    op.create_table('investment_transactions',
        sa.Column('investment_transaction_id', sa.Integer(), autoincrement=True, nullable=False),
        sa.Column('id', sa.Uuid(), nullable=False),
        sa.Column('user_id', sa.Integer(), nullable=False),
        sa.Column('account_id', sa.Integer(), nullable=True),
        sa.Column('holding_id', sa.Integer(), nullable=True),
        sa.Column('transaction_hash', sa.String(length=64), nullable=False),
        sa.Column('transaction_type', sa.Enum('BUY', 'SELL', 'DIVIDEND', 'INTEREST', 'FEE', 'TRANSFER_IN', 'TRANSFER_OUT', 'SPLIT', 'MERGER', 'SPINOFF', 'REINVESTMENT', name='investmenttransactiontype'), nullable=False),
        sa.Column('symbol', sa.String(length=20), nullable=True),
        sa.Column('api_symbol', sa.String(length=50), nullable=True),
        sa.Column('quantity', sa.DECIMAL(precision=15, scale=6), nullable=True),
        sa.Column('price_per_share', sa.DECIMAL(precision=15, scale=4), nullable=True),
        sa.Column('total_amount', sa.DECIMAL(precision=15, scale=2), nullable=False),
        sa.Column('fees', sa.DECIMAL(precision=15, scale=2), nullable=True),
        sa.Column('transaction_date', sa.Date(), nullable=False),
        sa.Column('description', sa.String(length=500), nullable=True),
        sa.Column('security_type', sa.String(length=20), nullable=True),
        sa.Column('cost_basis_at_sale', sa.DECIMAL(precision=15, scale=4), nullable=True),
        sa.Column('created_at', sa.DateTime(), nullable=False),
        sa.Column('updated_at', sa.DateTime(), nullable=False),
        sa.ForeignKeyConstraint(['account_id'], ['accounts.id']),
        sa.ForeignKeyConstraint(['holding_id'], ['investment_holdings.holding_id']),
        sa.ForeignKeyConstraint(['user_id'], ['users.db_id']),
        sa.PrimaryKeyConstraint('investment_transaction_id'),
        sa.UniqueConstraint('id'),
    )
    op.create_index('idx_investment_transactions_account_date', 'investment_transactions', ['account_id', 'transaction_date'], unique=False)
    op.create_index('idx_investment_transactions_date', 'investment_transactions', ['transaction_date'], unique=False)
    op.create_index('idx_investment_transactions_holding', 'investment_transactions', ['holding_id'], unique=False)
    op.create_index('idx_investment_transactions_type', 'investment_transactions', ['transaction_type'], unique=False)
    op.create_index('idx_investment_transactions_user_date', 'investment_transactions', ['user_id', 'transaction_date'], unique=False)

    op.create_table('transaction_relationships',
        sa.Column('relationship_id', sa.Integer(), autoincrement=True, nullable=False),
        sa.Column('id', sa.Uuid(), nullable=False),
        sa.Column('from_transaction_id', sa.Integer(), nullable=False),
        sa.Column('to_transaction_id', sa.Integer(), nullable=False),
        sa.Column('relationship_type', sa.Enum('OFFSETS', 'REFUNDS', 'FEES_FOR', 'REVERSES', name='relationshiptype'), nullable=False),
        sa.Column('amount_allocated', sa.DECIMAL(precision=15, scale=2), nullable=True),
        sa.Column('notes', sa.Text(), nullable=True),
        sa.Column('created_at', sa.DateTime(), nullable=False),
        sa.ForeignKeyConstraint(['from_transaction_id'], ['transactions.db_id']),
        sa.ForeignKeyConstraint(['to_transaction_id'], ['transactions.db_id']),
        sa.PrimaryKeyConstraint('relationship_id'),
        sa.UniqueConstraint('id'),
        sa.UniqueConstraint('from_transaction_id', 'to_transaction_id', 'relationship_type', name='uq_transaction_relationship'),
    )
    op.create_index('idx_rel_from_transaction', 'transaction_relationships', ['from_transaction_id'], unique=False)
    op.create_index('idx_rel_to_transaction', 'transaction_relationships', ['to_transaction_id'], unique=False)
    op.create_index('idx_rel_type', 'transaction_relationships', ['relationship_type'], unique=False)

    op.create_table('transaction_split_allocations',
        sa.Column('allocation_id', sa.Integer(), autoincrement=True, nullable=False),
        sa.Column('id', sa.Uuid(), nullable=False),
        sa.Column('transaction_id', sa.Integer(), nullable=False),
        sa.Column('category_id', sa.Integer(), nullable=False),
        sa.Column('subcategory_id', sa.Integer(), nullable=True),
        sa.Column('amount', sa.DECIMAL(precision=15, scale=2), nullable=False),
        sa.Column('created_at', sa.DateTime(), nullable=False),
        sa.Column('updated_at', sa.DateTime(), nullable=False),
        sa.ForeignKeyConstraint(['transaction_id'], ['transactions.db_id']),
        sa.ForeignKeyConstraint(['category_id'], ['categories.id']),
        sa.ForeignKeyConstraint(['subcategory_id'], ['categories.id']),
        sa.PrimaryKeyConstraint('allocation_id'),
        sa.UniqueConstraint('id'),
        sa.UniqueConstraint('transaction_id', 'category_id', 'subcategory_id', name='uq_split_allocation_txn_cat_sub'),
    )
    op.create_index('idx_split_alloc_transaction', 'transaction_split_allocations', ['transaction_id'], unique=False)
    op.create_index('idx_split_alloc_category', 'transaction_split_allocations', ['category_id'], unique=False)

    op.create_table('transaction_amortization_schedules',
        sa.Column('schedule_id', sa.Integer(), autoincrement=True, nullable=False),
        sa.Column('id', sa.Uuid(), nullable=False),
        sa.Column('transaction_id', sa.Integer(), nullable=False),
        sa.Column('month_date', sa.Date(), nullable=False),
        sa.Column('amount', sa.DECIMAL(precision=15, scale=2), nullable=False),
        sa.Column('created_at', sa.DateTime(), nullable=False),
        sa.ForeignKeyConstraint(['transaction_id'], ['transactions.db_id']),
        sa.PrimaryKeyConstraint('schedule_id'),
        sa.UniqueConstraint('id'),
        sa.UniqueConstraint('transaction_id', 'month_date', name='uq_amortization_txn_month'),
    )
    op.create_index('idx_amortization_transaction', 'transaction_amortization_schedules', ['transaction_id'], unique=False)
    op.create_index('idx_amortization_month', 'transaction_amortization_schedules', ['month_date'], unique=False)

    op.create_table('transaction_tags',
        sa.Column('transaction_id', sa.Integer(), nullable=False),
        sa.Column('tag_id', sa.Integer(), nullable=False),
        sa.Column('created_at', sa.DateTime(), nullable=False),
        sa.ForeignKeyConstraint(['tag_id'], ['tags.tag_id']),
        sa.ForeignKeyConstraint(['transaction_id'], ['transactions.db_id']),
        sa.PrimaryKeyConstraint('transaction_id', 'tag_id'),
    )

    # ===== Upload tracking tables =====

    op.create_table('upload_jobs',
        sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
        sa.Column('user_id', sa.Integer(), nullable=False),
        sa.Column('account_id', sa.Integer(), nullable=True),
        sa.Column('file_path', sa.String(length=500), nullable=True),
        sa.Column('institution', sa.String(length=100), nullable=False),
        sa.Column('skip_duplicates', sa.Boolean(), nullable=False),
        sa.Column('status', sa.String(length=20), nullable=False),
        sa.Column('error_message', sa.Text(), nullable=True),
        sa.Column('transactions_created', sa.Integer(), nullable=False),
        sa.Column('transactions_skipped', sa.Integer(), nullable=False),
        sa.Column('investment_transactions_created', sa.Integer(), nullable=False),
        sa.Column('investment_transactions_skipped', sa.Integer(), nullable=False),
        sa.Column('created_at', sa.DateTime(), nullable=False),
        sa.Column('started_at', sa.DateTime(), nullable=True),
        sa.Column('completed_at', sa.DateTime(), nullable=True),
        sa.ForeignKeyConstraint(['account_id'], ['accounts.id']),
        sa.ForeignKeyConstraint(['user_id'], ['users.db_id']),
        sa.PrimaryKeyConstraint('id'),
    )
    op.create_index('idx_upload_jobs_account', 'upload_jobs', ['account_id'], unique=False)
    op.create_index('idx_upload_jobs_created', 'upload_jobs', ['created_at'], unique=False)
    op.create_index('idx_upload_jobs_status', 'upload_jobs', ['status'], unique=False)
    op.create_index('idx_upload_jobs_user', 'upload_jobs', ['user_id'], unique=False)

    op.create_table('skipped_transactions',
        sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
        sa.Column('upload_job_id', sa.Integer(), nullable=False),
        sa.Column('transaction_type', sa.String(length=20), nullable=False),
        sa.Column('parsed_date', sa.Date(), nullable=False),
        sa.Column('parsed_amount', sa.DECIMAL(precision=15, scale=2), nullable=False),
        sa.Column('parsed_description', sa.Text(), nullable=False),
        sa.Column('parsed_transaction_type', sa.String(length=50), nullable=False),
        sa.Column('parsed_symbol', sa.String(length=20), nullable=True),
        sa.Column('parsed_quantity', sa.DECIMAL(precision=15, scale=6), nullable=True),
        sa.Column('parsed_data_json', sa.JSON(), nullable=True),
        sa.Column('existing_transaction_id', sa.Uuid(), nullable=True),
        sa.Column('existing_investment_transaction_id', sa.Uuid(), nullable=True),
        sa.Column('created_at', sa.DateTime(), nullable=False),
        sa.ForeignKeyConstraint(['existing_investment_transaction_id'], ['investment_transactions.id'], ondelete='SET NULL'),
        sa.ForeignKeyConstraint(['existing_transaction_id'], ['transactions.id'], ondelete='SET NULL'),
        sa.ForeignKeyConstraint(['upload_job_id'], ['upload_jobs.id']),
        sa.PrimaryKeyConstraint('id'),
    )
    op.create_index('idx_skipped_transactions_date', 'skipped_transactions', ['parsed_date'], unique=False)
    op.create_index('idx_skipped_transactions_job', 'skipped_transactions', ['upload_job_id'], unique=False)

    op.create_table('parsed_imports',
        sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
        sa.Column('upload_job_id', sa.Integer(), nullable=True),
        sa.Column('transaction_id', sa.Uuid(), nullable=True),
        sa.Column('investment_transaction_id', sa.Uuid(), nullable=True),
        sa.Column('raw_parsed_data', sa.JSON(), nullable=False),
        sa.Column('user_edits', sa.JSON(), nullable=True),
        sa.Column('llm_model', sa.String(length=100), nullable=True),
        sa.Column('llm_processed_at', sa.DateTime(), nullable=True),
        sa.Column('created_at', sa.DateTime(), nullable=False),
        sa.ForeignKeyConstraint(['upload_job_id'], ['upload_jobs.id'], ondelete='SET NULL'),
        sa.ForeignKeyConstraint(['transaction_id'], ['transactions.id'], ondelete='SET NULL'),
        sa.ForeignKeyConstraint(['investment_transaction_id'], ['investment_transactions.id'], ondelete='SET NULL'),
        sa.PrimaryKeyConstraint('id'),
    )
    op.create_index('idx_parsed_imports_job', 'parsed_imports', ['upload_job_id'], unique=False)
    op.create_index('idx_parsed_imports_txn', 'parsed_imports', ['transaction_id'], unique=False)
    op.create_index('idx_parsed_imports_inv_txn', 'parsed_imports', ['investment_transaction_id'], unique=False)

    op.create_table('description_cache',
        sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
        sa.Column('user_id', sa.Integer(), nullable=False),
        sa.Column('description_hash', sa.String(length=64), nullable=False),
        sa.Column('raw_description', sa.String(length=500), nullable=False),
        sa.Column('cleaned_description', sa.String(length=500), nullable=False),
        sa.Column('source', sa.String(length=20), nullable=False),
        sa.Column('llm_model', sa.String(length=100), nullable=True),
        sa.Column('created_at', sa.DateTime(), nullable=False),
        sa.Column('updated_at', sa.DateTime(), nullable=False),
        sa.ForeignKeyConstraint(['user_id'], ['users.db_id'], ondelete='CASCADE'),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('user_id', 'description_hash', name='uq_description_cache_user_hash'),
    )
    op.create_index('idx_description_cache_user_hash', 'description_cache', ['user_id', 'description_hash'], unique=False)


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_index('idx_description_cache_user_hash', table_name='description_cache')
    op.drop_table('description_cache')
    op.drop_index('idx_parsed_imports_inv_txn', table_name='parsed_imports')
    op.drop_index('idx_parsed_imports_txn', table_name='parsed_imports')
    op.drop_index('idx_parsed_imports_job', table_name='parsed_imports')
    op.drop_table('parsed_imports')
    op.drop_index('idx_skipped_transactions_job', table_name='skipped_transactions')
    op.drop_index('idx_skipped_transactions_date', table_name='skipped_transactions')
    op.drop_table('skipped_transactions')
    op.drop_index('idx_upload_jobs_user', table_name='upload_jobs')
    op.drop_index('idx_upload_jobs_status', table_name='upload_jobs')
    op.drop_index('idx_upload_jobs_created', table_name='upload_jobs')
    op.drop_index('idx_upload_jobs_account', table_name='upload_jobs')
    op.drop_table('upload_jobs')
    op.drop_table('transaction_tags')
    op.drop_index('idx_amortization_month', table_name='transaction_amortization_schedules')
    op.drop_index('idx_amortization_transaction', table_name='transaction_amortization_schedules')
    op.drop_table('transaction_amortization_schedules')
    op.drop_index('idx_split_alloc_category', table_name='transaction_split_allocations')
    op.drop_index('idx_split_alloc_transaction', table_name='transaction_split_allocations')
    op.drop_table('transaction_split_allocations')
    op.drop_index('idx_rel_type', table_name='transaction_relationships')
    op.drop_index('idx_rel_to_transaction', table_name='transaction_relationships')
    op.drop_index('idx_rel_from_transaction', table_name='transaction_relationships')
    op.drop_table('transaction_relationships')
    op.drop_index('idx_investment_transactions_user_date', table_name='investment_transactions')
    op.drop_index('idx_investment_transactions_type', table_name='investment_transactions')
    op.drop_index('idx_investment_transactions_holding', table_name='investment_transactions')
    op.drop_index('idx_investment_transactions_date', table_name='investment_transactions')
    op.drop_index('idx_investment_transactions_account_date', table_name='investment_transactions')
    op.drop_table('investment_transactions')
    op.drop_table('financial_plan_expenses')
    op.drop_index('idx_debt_payments_transaction', table_name='debt_payments')
    op.drop_index('idx_debt_payments_source_account', table_name='debt_payments')
    op.drop_index('idx_debt_payments_loan_account', table_name='debt_payments')
    op.drop_index('idx_debt_payments_date', table_name='debt_payments')
    op.drop_table('debt_payments')
    op.drop_index('idx_transactions_user_date', table_name='transactions')
    op.drop_index('idx_transactions_user_account', table_name='transactions')
    op.drop_index('idx_transactions_date', table_name='transactions')
    op.drop_table('transactions')
    op.drop_index('idx_holdings_symbol', table_name='investment_holdings')
    op.drop_index('idx_holdings_account', table_name='investment_holdings')
    op.drop_table('investment_holdings')
    op.drop_table('financial_plan_months')
    op.drop_table('debt_repayment_schedules')
    op.drop_table('debt_plan_account_links')
    op.drop_index('idx_budget_months_template', table_name='budget_months')
    op.drop_index('idx_budget_months_user', table_name='budget_months')
    op.drop_table('budget_months')
    op.drop_table('budget_template_categories')
    op.drop_index('idx_backfill_jobs_created', table_name='snapshot_backfill_jobs')
    op.drop_index('idx_backfill_jobs_status', table_name='snapshot_backfill_jobs')
    op.drop_index('idx_backfill_jobs_account', table_name='snapshot_backfill_jobs')
    op.drop_table('snapshot_backfill_jobs')
    op.drop_index('idx_account_value_date', table_name='account_value_history')
    op.drop_index('idx_account_value_account_date', table_name='account_value_history')
    op.drop_index('idx_account_value_account', table_name='account_value_history')
    op.drop_table('account_value_history')
    op.drop_table('tags')
    op.drop_table('financial_plans')
    op.drop_table('debt_repayment_plans')
    op.drop_table('budget_templates')
    op.drop_table('accounts')
    op.drop_index('idx_users_email', table_name='users')
    op.drop_table('users')
    op.drop_index('idx_category_name', table_name='categories')
    op.drop_table('categories')
