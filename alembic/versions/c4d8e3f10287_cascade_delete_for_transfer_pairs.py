"""cascade delete for transfer-pair FKs

Revision ID: c4d8e3f10287
Revises: f2b9c1e4a701
Create Date: 2026-05-12 00:00:00.000000

Bug fix on top of #39.

`transactions.db_id` is `INTEGER PRIMARY KEY` *without* AUTOINCREMENT, so
SQLite reuses int IDs after delete-then-insert (new row gets
max(existing)+1, which is the freed ID if you deleted the previous max).
Combined with the lack of ON DELETE CASCADE on FKs into the new
`dismissed_transfer_pairs` and the investment-side columns of
`transaction_relationships`, this lets a dismissal "stick" to a freshly
re-imported transaction.

Fix: ON DELETE CASCADE on all four FK columns of `dismissed_transfer_pairs`
and on all four FK columns of `transaction_relationships`. For symmetry
the regular-side FKs of `transaction_relationships` also get DB-level
CASCADE (the ORM already cascades those via `cascade="all, delete-orphan"`,
but DB-level enforcement covers bulk SQL paths too).

`transactions.db_id`'s lack of AUTOINCREMENT is a separate architectural
question — out of scope here. CASCADE makes the lack of AUTOINCREMENT
safe for these two tables.

SQLite can't ALTER an FK in place; the existing FK constraints on these
tables are also a mix of named (the investment-side ones added in
f2b9c1e4a701) and unnamed (the regular-side ones from the initial
migration). Cleanest path: rebuild each table from scratch — create a new
table with the correct shape, copy data, drop the old, rename.
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'c4d8e3f10287'
down_revision: Union[str, Sequence[str], None] = 'f2b9c1e4a701'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def _rebuild_transaction_relationships(*, cascade: bool):
    fk_action = "ON DELETE CASCADE" if cascade else ""
    op.execute("DROP TABLE IF EXISTS _new_transaction_relationships")
    op.execute(f"""
        CREATE TABLE _new_transaction_relationships (
            relationship_id INTEGER NOT NULL,
            id CHAR(32) NOT NULL,
            from_transaction_id INTEGER,
            to_transaction_id INTEGER,
            relationship_type VARCHAR(8) NOT NULL,
            amount_allocated DECIMAL(15, 2),
            notes TEXT,
            created_at DATETIME NOT NULL,
            from_investment_transaction_id INTEGER,
            to_investment_transaction_id INTEGER,
            PRIMARY KEY (relationship_id),
            UNIQUE (id),
            CONSTRAINT ck_rel_from_exactly_one CHECK (
                ((from_transaction_id IS NOT NULL) +
                 (from_investment_transaction_id IS NOT NULL)) = 1
            ),
            CONSTRAINT ck_rel_to_exactly_one CHECK (
                ((to_transaction_id IS NOT NULL) +
                 (to_investment_transaction_id IS NOT NULL)) = 1
            ),
            CONSTRAINT fk_rel_from_transaction
                FOREIGN KEY (from_transaction_id)
                REFERENCES transactions (db_id) {fk_action},
            CONSTRAINT fk_rel_to_transaction
                FOREIGN KEY (to_transaction_id)
                REFERENCES transactions (db_id) {fk_action},
            CONSTRAINT fk_rel_from_investment_txn
                FOREIGN KEY (from_investment_transaction_id)
                REFERENCES investment_transactions (investment_transaction_id) {fk_action},
            CONSTRAINT fk_rel_to_investment_txn
                FOREIGN KEY (to_investment_transaction_id)
                REFERENCES investment_transactions (investment_transaction_id) {fk_action}
        )
    """)
    op.execute("""
        INSERT INTO _new_transaction_relationships
        SELECT relationship_id, id, from_transaction_id, to_transaction_id,
               relationship_type, amount_allocated, notes, created_at,
               from_investment_transaction_id, to_investment_transaction_id
          FROM transaction_relationships
    """)
    op.execute("DROP TABLE transaction_relationships")
    op.execute("ALTER TABLE _new_transaction_relationships RENAME TO transaction_relationships")
    op.execute("CREATE INDEX idx_rel_from_transaction ON transaction_relationships (from_transaction_id)")
    op.execute("CREATE INDEX idx_rel_to_transaction ON transaction_relationships (to_transaction_id)")
    op.execute("CREATE INDEX idx_rel_from_investment_transaction ON transaction_relationships (from_investment_transaction_id)")
    op.execute("CREATE INDEX idx_rel_to_investment_transaction ON transaction_relationships (to_investment_transaction_id)")
    op.execute("CREATE INDEX idx_rel_type ON transaction_relationships (relationship_type)")


def _rebuild_dismissed_transfer_pairs(*, cascade: bool):
    fk_action = "ON DELETE CASCADE" if cascade else ""
    op.execute("DROP TABLE IF EXISTS _new_dismissed_transfer_pairs")
    op.execute(f"""
        CREATE TABLE _new_dismissed_transfer_pairs (
            id INTEGER NOT NULL,
            user_id INTEGER NOT NULL,
            from_transaction_id INTEGER,
            from_investment_transaction_id INTEGER,
            to_transaction_id INTEGER,
            to_investment_transaction_id INTEGER,
            created_at DATETIME NOT NULL,
            PRIMARY KEY (id),
            CONSTRAINT ck_dismissed_from_exactly_one CHECK (
                ((from_transaction_id IS NOT NULL) +
                 (from_investment_transaction_id IS NOT NULL)) = 1
            ),
            CONSTRAINT ck_dismissed_to_exactly_one CHECK (
                ((to_transaction_id IS NOT NULL) +
                 (to_investment_transaction_id IS NOT NULL)) = 1
            ),
            FOREIGN KEY (user_id) REFERENCES users (db_id) ON DELETE CASCADE,
            CONSTRAINT fk_dismissed_from_transaction
                FOREIGN KEY (from_transaction_id)
                REFERENCES transactions (db_id) {fk_action},
            CONSTRAINT fk_dismissed_to_transaction
                FOREIGN KEY (to_transaction_id)
                REFERENCES transactions (db_id) {fk_action},
            CONSTRAINT fk_dismissed_from_investment_txn
                FOREIGN KEY (from_investment_transaction_id)
                REFERENCES investment_transactions (investment_transaction_id) {fk_action},
            CONSTRAINT fk_dismissed_to_investment_txn
                FOREIGN KEY (to_investment_transaction_id)
                REFERENCES investment_transactions (investment_transaction_id) {fk_action}
        )
    """)
    op.execute("""
        INSERT INTO _new_dismissed_transfer_pairs
        SELECT id, user_id, from_transaction_id, from_investment_transaction_id,
               to_transaction_id, to_investment_transaction_id, created_at
          FROM dismissed_transfer_pairs
    """)
    op.execute("DROP TABLE dismissed_transfer_pairs")
    op.execute("ALTER TABLE _new_dismissed_transfer_pairs RENAME TO dismissed_transfer_pairs")
    op.execute("CREATE INDEX idx_dismissed_pairs_user ON dismissed_transfer_pairs (user_id)")
    op.execute("""CREATE INDEX idx_dismissed_pairs_lookup ON dismissed_transfer_pairs
        (user_id, from_transaction_id, from_investment_transaction_id,
         to_transaction_id, to_investment_transaction_id)""")


def upgrade() -> None:
    """Upgrade schema."""
    _rebuild_transaction_relationships(cascade=True)
    _rebuild_dismissed_transfer_pairs(cascade=True)


def downgrade() -> None:
    """Downgrade schema."""
    _rebuild_dismissed_transfer_pairs(cascade=False)
    _rebuild_transaction_relationships(cascade=False)
