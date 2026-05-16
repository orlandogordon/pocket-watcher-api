"""taxonomy redesign (todo #53)

Revision ID: 4f89e1af2a35
Revises: a7c3b91e5012
Create Date: 2026-05-16

Applies the May 2026 taxonomy redesign — see Obsidian todo #53 for the audit
that motivated it. Combines three kinds of change in one atomic step:

  1. Re-upserts the predefined-category tree from src.constants.categories
     (same logic as c1a2b3d4e5f6). On an existing DB this inserts the new
     parents (Subscriptions, Health, Travel) and ~22 new subcategories, and
     applies in-place renames + reparents — same UUIDs, new name and/or
     new parent_category_id, so all FK referrers keep working:
        - Haircut → Hair (same parent: Personal Care)
        - Concerts → Events (same parent: Entertainment)
        - Pharmacy → Prescriptions (reparent: Personal Care → Health)
        - Toiletries (reparent: Personal Care → Shopping)
        - Streaming Services → Streaming (reparent: Entertainment → Subscriptions)
        - General Merchandise → General (reparent: Miscellaneous → Shopping)

  2. Removes the Investments top-level + its 3 subcategories. Per todo
     decision #1 these never represented real spending — real investment
     activity lives in investment_holdings, and outflows to brokerages are
     transfers (#39/#49/#51), not purchases.

  3. Cleans up the four FK referrers of the removed categories before the
     DELETE. transactions.category_id and subcategory_id are nullable so
     we NULL them; budget_template_categories, financial_plan_expenses,
     and transaction_split_allocations have a NOT NULL category_id so we
     delete those rows outright. The subcategory_id columns on the budget
     and split-allocation tables are nullable, so we NULL those.

Idempotent: re-running is a no-op because every step either upserts on UUID
or skips when the source rows are already gone.
"""
from typing import Sequence, Union
from uuid import UUID

from alembic import op
import sqlalchemy as sa
from sqlalchemy import text

from src.constants.categories import PREDEFINED_CATEGORIES


revision: str = '4f89e1af2a35'
down_revision: Union[str, Sequence[str], None] = 'a7c3b91e5012'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


# Categories removed by this migration. Keep these UUIDs hard-coded — once
# they're gone from src.constants.categories there's no way to recover them
# from code, but the migration must remain replayable.
_REMOVED_CATEGORY_UUIDS = {
    "investments_parent":          "1601d6e1-e0d7-44f7-8f47-207ca11538be",
    "stock_purchase":              "a762c7e9-7a3d-4ab5-97e4-814b14d81e0b",
    "retirement_contribution":     "ff08b4f8-e6b2-4cb6-a7d9-0e1f3b346800",
    "crypto":                      "ee100d61-d2ec-430a-8fea-3222e9dfe0ee",
}


def upgrade() -> None:
    bind = op.get_bind()
    categories_tbl = sa.table(
        "categories",
        sa.column("id", sa.Integer),
        sa.column("uuid", sa.Uuid),
        sa.column("name", sa.String),
        sa.column("parent_category_id", sa.Integer),
    )

    # --------------------------------------------------------------------
    # Pass 1: upsert all parents from the new taxonomy.
    # --------------------------------------------------------------------
    for parent_name, parent_uuid, _subs in PREDEFINED_CATEGORIES:
        row = bind.execute(
            sa.select(categories_tbl.c.id).where(categories_tbl.c.uuid == parent_uuid)
        ).first()
        if row is None:
            bind.execute(categories_tbl.insert().values(
                uuid=parent_uuid,
                name=parent_name,
                parent_category_id=None,
            ))
        else:
            bind.execute(categories_tbl.update()
                .where(categories_tbl.c.uuid == parent_uuid)
                .values(name=parent_name, parent_category_id=None))

    # --------------------------------------------------------------------
    # Pass 2: upsert subcategories — handles new rows, renames, and
    # reparents in a single pass keyed on UUID.
    # --------------------------------------------------------------------
    for parent_name, parent_uuid, subs in PREDEFINED_CATEGORIES:
        parent_row = bind.execute(
            sa.select(categories_tbl.c.id).where(categories_tbl.c.uuid == parent_uuid)
        ).first()
        parent_id = parent_row[0]

        for sub_name, sub_uuid in subs:
            row = bind.execute(
                sa.select(categories_tbl.c.id).where(categories_tbl.c.uuid == sub_uuid)
            ).first()
            if row is None:
                bind.execute(categories_tbl.insert().values(
                    uuid=sub_uuid,
                    name=sub_name,
                    parent_category_id=parent_id,
                ))
            else:
                bind.execute(categories_tbl.update()
                    .where(categories_tbl.c.uuid == sub_uuid)
                    .values(name=sub_name, parent_category_id=parent_id))

    # --------------------------------------------------------------------
    # Pass 3: resolve the removed-category UUIDs to integer ids. Routed
    # through the typed `categories_tbl.c.uuid` column so the sa.Uuid type
    # adapter handles SQLite's hex-without-dashes storage format (raw
    # text() with hyphenated strings would silently match nothing).
    # Anything already gone from a prior partial run produces None.
    # --------------------------------------------------------------------
    removed_ids: list[int] = []
    for uuid_str in _REMOVED_CATEGORY_UUIDS.values():
        row = bind.execute(
            sa.select(categories_tbl.c.id)
            .where(categories_tbl.c.uuid == UUID(uuid_str))
        ).first()
        if row is not None:
            removed_ids.append(row[0])

    if not removed_ids:
        # Nothing to remove — re-run on an already-migrated DB.
        return

    # --------------------------------------------------------------------
    # Pass 4: clean up FK referrers BEFORE deleting the rows. RESTRICT FKs
    # would block the DELETE otherwise.
    # --------------------------------------------------------------------
    placeholders = ",".join(f":id_{i}" for i in range(len(removed_ids)))
    bind_params = {f"id_{i}": rid for i, rid in enumerate(removed_ids)}

    # transactions: nullable on both columns — NULL them.
    bind.execute(text(
        f"UPDATE transactions SET category_id = NULL "
        f"WHERE category_id IN ({placeholders})"
    ), bind_params)
    bind.execute(text(
        f"UPDATE transactions SET subcategory_id = NULL "
        f"WHERE subcategory_id IN ({placeholders})"
    ), bind_params)

    # budget_template_categories: category_id NOT NULL → delete the row;
    # subcategory_id nullable → NULL the sub.
    bind.execute(text(
        f"DELETE FROM budget_template_categories "
        f"WHERE category_id IN ({placeholders})"
    ), bind_params)
    bind.execute(text(
        f"UPDATE budget_template_categories SET subcategory_id = NULL "
        f"WHERE subcategory_id IN ({placeholders})"
    ), bind_params)

    # financial_plan_expenses: only has category_id (NOT NULL) → delete.
    bind.execute(text(
        f"DELETE FROM financial_plan_expenses "
        f"WHERE category_id IN ({placeholders})"
    ), bind_params)

    # transaction_split_allocations: category_id NOT NULL → delete;
    # subcategory_id nullable → NULL the sub.
    bind.execute(text(
        f"DELETE FROM transaction_split_allocations "
        f"WHERE category_id IN ({placeholders})"
    ), bind_params)
    bind.execute(text(
        f"UPDATE transaction_split_allocations SET subcategory_id = NULL "
        f"WHERE subcategory_id IN ({placeholders})"
    ), bind_params)

    # --------------------------------------------------------------------
    # Pass 5: drop subcategories first (they FK to the Investments parent
    # via categories.parent_category_id, which is RESTRICT), then the
    # parent. UUID lookups again go through the typed column so SQLite's
    # hex-without-dashes storage format is handled correctly.
    # --------------------------------------------------------------------
    for key in ("stock_purchase", "retirement_contribution", "crypto",
                "investments_parent"):
        bind.execute(
            categories_tbl.delete().where(
                categories_tbl.c.uuid == UUID(_REMOVED_CATEGORY_UUIDS[key])
            )
        )


def downgrade() -> None:
    """Intentional no-op.

    This migration removes 4 categories and re-tags an unknown number of
    transactions to NULL — restoring the prior state would require knowing
    which rows used to point at Investments / *, which we don't preserve.
    Restore from backup if you need to revert.

    Re-adding the removed categories on downgrade would leave the DB in a
    half-state (categories back, but transaction tags still NULL), which is
    worse than just refusing to revert.
    """
