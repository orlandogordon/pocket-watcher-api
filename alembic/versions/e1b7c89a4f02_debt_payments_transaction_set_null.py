"""debt_payments.transaction_id ON DELETE SET NULL

Revision ID: e1b7c89a4f02
Revises: 4f89e1af2a35
Create Date: 2026-05-17 00:00:00.000000

Before any debt_payments rows exist (current count: 0), set ON DELETE
SET NULL on the FK from debt_payments.transaction_id to transactions.db_id.

With PRAGMA foreign_keys = ON (enabled in #39), deleting a bank-side
transaction that has a linked debt_payments row would otherwise fail
with a constraint violation. SET NULL matches the audit-style pattern
used by parsed_imports / skipped_transactions — the payment record's
principal/interest history survives the delete, just disconnected from
the bank-side txn (a future linker pass can re-attach if the txn is
re-imported).

SQLite can't ALTER an FK in place, so rebuild the table.
"""
from typing import Sequence, Union

from alembic import op


# revision identifiers, used by Alembic.
revision: str = 'e1b7c89a4f02'
down_revision: Union[str, Sequence[str], None] = '4f89e1af2a35'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def _rebuild_debt_payments(*, set_null: bool):
    txn_fk_action = "ON DELETE SET NULL" if set_null else ""
    op.execute("DROP TABLE IF EXISTS _new_debt_payments")
    op.execute(f"""
        CREATE TABLE _new_debt_payments (
            payment_id INTEGER NOT NULL,
            id CHAR(32) NOT NULL,
            loan_account_id INTEGER NOT NULL,
            payment_source_account_id INTEGER,
            transaction_id INTEGER,
            payment_amount DECIMAL(15, 2) NOT NULL,
            principal_amount DECIMAL(15, 2),
            interest_amount DECIMAL(15, 2),
            remaining_balance_after_payment DECIMAL(15, 2),
            payment_date DATE NOT NULL,
            description VARCHAR(500),
            created_at DATETIME NOT NULL,
            PRIMARY KEY (payment_id),
            UNIQUE (id),
            FOREIGN KEY (loan_account_id) REFERENCES accounts (id),
            FOREIGN KEY (payment_source_account_id) REFERENCES accounts (id),
            CONSTRAINT fk_debt_payments_transaction
                FOREIGN KEY (transaction_id)
                REFERENCES transactions (db_id) {txn_fk_action}
        )
    """)
    op.execute("""
        INSERT INTO _new_debt_payments
        SELECT payment_id, id, loan_account_id, payment_source_account_id,
               transaction_id, payment_amount, principal_amount,
               interest_amount, remaining_balance_after_payment,
               payment_date, description, created_at
          FROM debt_payments
    """)
    op.execute("DROP TABLE debt_payments")
    op.execute("ALTER TABLE _new_debt_payments RENAME TO debt_payments")
    op.execute("CREATE INDEX idx_debt_payments_date ON debt_payments (payment_date)")
    op.execute("CREATE INDEX idx_debt_payments_loan_account ON debt_payments (loan_account_id)")
    op.execute("CREATE INDEX idx_debt_payments_source_account ON debt_payments (payment_source_account_id)")
    op.execute("CREATE INDEX idx_debt_payments_transaction ON debt_payments (transaction_id)")


def upgrade() -> None:
    _rebuild_debt_payments(set_null=True)


def downgrade() -> None:
    _rebuild_debt_payments(set_null=False)
