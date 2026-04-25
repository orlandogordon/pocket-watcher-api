"""seed predefined categories (todo #29)

Revision ID: c1a2b3d4e5f6
Revises: 3a4fbf053151
Create Date: 2026-04-24

Idempotent upsert of the locked category tree from src.constants.categories.
Categories are no longer user-editable; scripts/seed.py no longer creates them.
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

from src.constants.categories import PREDEFINED_CATEGORIES


revision: str = 'c1a2b3d4e5f6'
down_revision: Union[str, Sequence[str], None] = '3a4fbf053151'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upsert the predefined category tree.

    Keyed on UUID so re-running the migration never creates duplicates. If a row
    with the same UUID already exists, its name + parent are refreshed to match
    the current definition in code.
    """
    bind = op.get_bind()
    categories_tbl = sa.table(
        "categories",
        sa.column("id", sa.Integer),
        sa.column("uuid", sa.Uuid),
        sa.column("name", sa.String),
        sa.column("parent_category_id", sa.Integer),
    )

    # Pass 1: parents
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

    # Pass 2: subcategories (need parents' integer ids)
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


def downgrade() -> None:
    """Remove only the rows owned by this migration (matched on UUID).

    Deletes subcategories before parents to satisfy FK ordering.
    """
    bind = op.get_bind()
    categories_tbl = sa.table(
        "categories",
        sa.column("uuid", sa.Uuid),
    )

    for _parent_name, _parent_uuid, subs in PREDEFINED_CATEGORIES:
        for _sub_name, sub_uuid in subs:
            bind.execute(categories_tbl.delete().where(categories_tbl.c.uuid == sub_uuid))

    for _parent_name, parent_uuid, _subs in PREDEFINED_CATEGORIES:
        bind.execute(categories_tbl.delete().where(categories_tbl.c.uuid == parent_uuid))
