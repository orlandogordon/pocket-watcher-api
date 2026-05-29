"""normalize int PK -> db_id and UUID -> uuid across all models (#58)

Renames every model's integer primary key to ``db_id`` and every external
UUID column to ``uuid``, retiring the overloaded ``id`` name (which meant an
int on some tables and a UUID on others). Pure column renames — no data
moves, FK references follow the renamed target column.

Revision ID: f1a2b3c4d5e6
Revises: e28c84c310ab
Create Date: 2026-05-29
"""
from alembic import op

revision = "f1a2b3c4d5e6"
down_revision = "e28c84c310ab"
branch_labels = None
depends_on = None


# (table, old_name, new_name)
INT_PK_RENAMES = [
    ("categories", "id", "db_id"),
    ("budget_templates", "template_id", "db_id"),
    ("budget_template_categories", "allocation_id", "db_id"),
    ("budget_months", "month_id", "db_id"),
    ("investment_holdings", "holding_id", "db_id"),
    ("investment_transactions", "investment_transaction_id", "db_id"),
    ("debt_payments", "payment_id", "db_id"),
    ("debt_repayment_plans", "plan_id", "db_id"),
    ("debt_repayment_schedules", "schedule_id", "db_id"),
    ("transaction_relationships", "relationship_id", "db_id"),
    ("dismissed_transfer_pairs", "id", "db_id"),
    ("transaction_split_allocations", "allocation_id", "db_id"),
    ("transaction_amortization_schedules", "schedule_id", "db_id"),
    ("tags", "tag_id", "db_id"),
    ("accounts", "id", "db_id"),
    ("account_value_history", "snapshot_id", "db_id"),
    ("financial_plans", "plan_id", "db_id"),
    ("financial_plan_months", "month_id", "db_id"),
    ("financial_plan_expenses", "expense_id", "db_id"),
    ("snapshot_backfill_jobs", "id", "db_id"),
    ("bulk_import_batches", "id", "db_id"),
    ("upload_jobs", "id", "db_id"),
    ("skipped_transactions", "id", "db_id"),
    ("parsed_imports", "id", "db_id"),
]

# UUID column rename: id -> uuid (tables where the UUID was named ``id``)
UUID_RENAMES = [
    "users",
    "budget_templates",
    "budget_template_categories",
    "budget_months",
    "investment_holdings",
    "investment_transactions",
    "debt_payments",
    "debt_repayment_plans",
    "debt_repayment_schedules",
    "transactions",
    "transaction_relationships",
    "transaction_split_allocations",
    "transaction_amortization_schedules",
    "tags",
    "financial_plans",
    "financial_plan_months",
    "financial_plan_expenses",
]


def upgrade() -> None:
    for table, old, new in INT_PK_RENAMES:
        op.alter_column(table, old, new_column_name=new)
    for table in UUID_RENAMES:
        op.alter_column(table, "id", new_column_name="uuid")


def downgrade() -> None:
    for table in UUID_RENAMES:
        op.alter_column(table, "uuid", new_column_name="id")
    for table, old, new in INT_PK_RENAMES:
        op.alter_column(table, new, new_column_name=old)
