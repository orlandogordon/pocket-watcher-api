"""
Single source of truth for the system's category tree.

Categories are locked in code. UUIDs were generated once with uuid4() and must
never change — they're referenced by name in the LLM prompt (see #29) and by
UUID in existing data (transactions, budgets, financial plans). Changing a UUID
would orphan rows. Runtime edits via the categories router are disabled
(POST/PUT/DELETE → 405).

--------------------------------------------------------------------------
Adding a new category or subcategory (existing DB — dev or prod):
--------------------------------------------------------------------------

  1. Generate a fresh UUID. Never reuse one, never hand-write one.
         python -c "import uuid; print(uuid.uuid4())"

  2. Add the entry to PREDEFINED_CATEGORIES below.
       - New parent:       append a new top-level tuple.
       - New subcategory:  append to the `[...]` list inside the parent.

  3. Write a new Alembic data migration that upserts the tree onto the
     existing DB. The simplest pattern is to copy
     `alembic/versions/c1a2b3d4e5f6_seed_predefined_categories.py`:
         - set a fresh `revision` ID
         - set `down_revision` to the current head (`alembic heads`)
         - leave the upgrade/downgrade bodies as-is — they already iterate
           PREDEFINED_CATEGORIES and are idempotent on UUID.
     Editing this file WITHOUT a new migration does nothing on a DB that
     already ran c1a2b3d4e5f6 — Alembic skips migrations it's already
     applied, so the new entry never gets inserted.

  4. Apply:
         alembic upgrade head

--------------------------------------------------------------------------
Fresh DB (dev wipe-and-recreate):
--------------------------------------------------------------------------

  Editing this file is enough. `alembic upgrade head` from empty will run
  c1a2b3d4e5f6 for the first time and pick up whatever's in
  PREDEFINED_CATEGORIES. No new migration needed for the first-time case.

--------------------------------------------------------------------------
Renaming a category:
--------------------------------------------------------------------------

  Same as adding — edit the name in this file, then add a new migration
  that re-upserts the tree. The existing c1a2b3d4e5f6 upgrade path does an
  UPDATE when the UUID already exists, so a copy of it is the right pattern.
  The UUID must NOT change.

--------------------------------------------------------------------------
Removing a category:
--------------------------------------------------------------------------

  Use `scripts/remove_category.py <OLD_UUID> <REPLACEMENT_UUID>` to generate
  a migration that repoints every FK referrer, then deletes the row. Edit
  this file afterwards to drop the entry, then `alembic upgrade head`.
"""

from __future__ import annotations

from uuid import UUID


# (category_name, uuid, [(subcategory_name, uuid), ...])
#
# Reused UUIDs after the #53 taxonomy redesign (in-place rename + reparent —
# referring rows keep working):
#   - 7e4d7e16 Haircut → Hair (same parent: Personal Care)
#   - f938e357 Concerts → Events (same parent: Entertainment)
#   - 84f178ed Pharmacy → Prescriptions (reparented: Personal Care → Health)
#   - 1a1b3dd2 Toiletries → Toiletries (reparented: Personal Care → Shopping)
#   - d6762e10 Streaming Services → Streaming (reparented: Entertainment → Subscriptions)
#   - 5247aeec General Merchandise → General (reparented: Miscellaneous → Shopping)
# Removed in #53: Investments parent (1601d6e1) + Stock Purchase (a762c7e9)
# + Retirement Contribution (ff08b4f8) + Crypto (ee100d61).
PREDEFINED_CATEGORIES: list[tuple[str, UUID, list[tuple[str, UUID]]]] = [
    ("Income", UUID("17ac387d-1817-48d5-85c6-84bd2af576e9"), [
        ("Paycheck", UUID("42e344f9-55f1-4f46-9c12-d548658409fb")),
        ("Bonus", UUID("e826a6fe-1426-4c1a-acc6-8fe9ded29c32")),
        ("Investment Income", UUID("fe41dac0-0a3b-4e33-a731-9aecc6217d42")),
        ("Other Income", UUID("df973fc4-a395-467a-8d56-70e53132016b")),
        # Bidirectional: captures both refunds (inflow) and tax payments (outflow).
        ("Taxes", UUID("cf6fb4fe-cb19-4ca0-8cff-a7de70b1645f")),
    ]),
    ("Housing", UUID("f8ee90f0-2d76-4547-b9b4-71fbb2c506d6"), [
        ("Rent", UUID("74cdfe01-d452-4d9f-b619-7290b106b0e8")),
        ("Mortgage", UUID("8c86ff04-3f6c-467c-a5cb-e9295521ae3a")),
        ("Utilities", UUID("8b4be050-62fa-4520-b5af-012e0eb048f5")),
        ("Home Repair", UUID("17e8d1a2-3965-49ea-8bfd-5645657172da")),
        ("Insurance", UUID("537cd764-e90e-4b7c-871a-ab5f6fb65616")),
    ]),
    ("Transportation", UUID("d0032366-ed8b-484b-9564-7f5e9721aa7e"), [
        ("Gas", UUID("936a458b-82eb-4278-b64f-4fba8f7ae8da")),
        ("Public Transit", UUID("d07371fc-fbf5-4388-86de-a6b43c6be316")),
        ("Car Maintenance", UUID("85c97dee-fea6-4c15-b594-285cc9daf747")),
        ("Ride Share", UUID("ed765086-5eae-4ca0-ab6c-9701739e29c2")),
        ("Car Insurance", UUID("2a4476f0-7541-47d1-89b6-d3868c0c6a55")),
        ("Parking", UUID("e2c18ac3-6d9e-4e34-a4d4-59f3ccb4116d")),
    ]),
    ("Food", UUID("9bf074af-479f-4d55-853c-e807a4bbbe9e"), [
        ("Groceries", UUID("0b66599a-0919-46cb-8d86-ea0517a66f12")),
        ("Restaurants", UUID("dd2d9c68-4c00-444e-80ed-775a72087bea")),
        ("Coffee Shops", UUID("88accd63-6963-417a-b334-970d28a91cf5")),
    ]),
    # Services-only after #53. Toiletries + Pharmacy moved out.
    ("Personal Care", UUID("ee02d7ee-7f8f-4983-8693-694dc0a1faae"), [
        ("Hair", UUID("7e4d7e16-f4fb-48ac-9c53-d618db66968a")),
        ("Nails", UUID("5a2cd932-79e7-4c35-b4e2-1e621b6fdb18")),
        ("Spa & Massage", UUID("2fe5c7fd-7e0a-4fb3-9fe6-ce41bea9e352")),
    ]),
    ("Entertainment", UUID("78bd0a07-5447-4cb6-b2d6-315d3d4cb4a0"), [
        ("Movies", UUID("54ad040d-c1e5-487c-99e2-75767e838f45")),
        ("Events", UUID("f938e357-29e6-4f16-9ac7-d139df4ee68b")),
        ("Hobbies", UUID("1831cdfa-bc8a-45e7-a552-404ee54b3464")),
        ("Bars & Clubs", UUID("65d418af-2b29-44a3-af3c-2093655e14c2")),
    ]),
    ("Debt Payment", UUID("54812989-bc35-4acb-aa11-a93aaa7b6b65"), [
        ("Credit Card", UUID("b9328f2f-88f5-4128-90af-87130c967280")),
        ("Student Loan", UUID("3280dd39-0173-4754-bdba-17b1a3981e1e")),
        ("Car Loan", UUID("ab2bc70e-bfac-47ad-b75e-ee0abe7d0017")),
    ]),
    ("Shopping", UUID("134bbe34-09df-4462-9d50-5dab2b03c089"), [
        ("Clothing", UUID("998b29dc-5c13-4a24-95d9-2aa19358df83")),
        ("Electronics", UUID("1e5dad12-0a65-47c6-b224-333fd32b7237")),
        ("Home Goods", UUID("01d99d4f-35a3-4702-817b-494e8078df49")),
        ("General", UUID("5247aeec-a479-4801-9f5e-07af3122f6f9")),
        ("Toiletries", UUID("1a1b3dd2-e0e7-42dc-beed-1cdd88a5441b")),
        ("Pet Supplies", UUID("69f61d67-75dc-43d3-ace4-81190bbd9dda")),
    ]),
    ("Miscellaneous", UUID("0284c65f-1af6-48d2-9133-3d3ac3393ede"), [
        ("Bank Fee", UUID("d7a3041e-5253-492c-82ca-ca24fb25df26")),
        ("Education", UUID("50da18e7-39e2-4b87-83ca-09a6d7f338f3")),
        ("Gifts & Charity", UUID("63e4c43b-a02e-4ac4-b820-46425a20d954")),
    ]),
    ("Health", UUID("829218b9-9519-4ddf-991a-a7e88d22f4d0"), [
        ("Doctor Visits", UUID("7952306d-12ee-4f56-88f4-56988ea8ada6")),
        ("Prescriptions", UUID("84f178ed-34cf-42c5-bc6d-c19cd7a40bf5")),
        ("Health Insurance", UUID("b5e6dcee-3c3c-42e2-930b-fa809099ad9b")),
        ("Pet Vet", UUID("35e709a5-9eb6-40fe-8feb-b1c018c5d19d")),
        ("Pet Insurance", UUID("8635fd38-ee4e-44ee-a6e1-4f4fcba534d3")),
    ]),
    ("Subscriptions", UUID("978bf5d7-68a7-49ce-9f6e-f05ff01f4e07"), [
        ("Software", UUID("d8a8d1c4-1ce3-4316-afc2-84e516652845")),
        ("Streaming", UUID("d6762e10-a608-417a-a7a6-87a2977e59e1")),
        ("News & Media", UUID("de239a7a-b984-44d4-908b-ea5c0e5cdbe7")),
        ("Memberships", UUID("2eaf0bb4-12ef-4049-a905-bcdb9de0142b")),
    ]),
    ("Travel", UUID("9a37b80c-35d2-4ad1-9f7e-9a14f65f2840"), [
        ("Lodging", UUID("2fb24049-46dc-4243-9e50-f08d8e9a5483")),
        ("Flights", UUID("4159192b-54ef-4aaa-8a02-a6be2e73e3c1")),
        ("Rental Car", UUID("98cf65dd-1eda-4318-b173-191ff20822fd")),
        ("Travel Insurance", UUID("bf902872-33c4-4e69-b030-45e9b53d1322")),
    ]),
]


def all_category_uuids() -> list[str]:
    """Flat list of every parent + subcategory UUID as strings, for prompt/JSON-schema enum."""
    out: list[str] = []
    for _, parent_uuid, subs in PREDEFINED_CATEGORIES:
        out.append(str(parent_uuid))
        for _, sub_uuid in subs:
            out.append(str(sub_uuid))
    return out


def all_subcategory_uuids() -> list[str]:
    out: list[str] = []
    for _, _, subs in PREDEFINED_CATEGORIES:
        for _, sub_uuid in subs:
            out.append(str(sub_uuid))
    return out


def all_parent_uuids() -> list[str]:
    return [str(parent_uuid) for _, parent_uuid, _ in PREDEFINED_CATEGORIES]


def subcategory_to_parent() -> dict[str, str]:
    """Map subcategory UUID -> its parent category UUID.

    Used by the LLM pipeline to derive the parent from the (more specific)
    subcategory the model chose, rather than trusting the model to pick a
    consistent (parent, child) pair. See #29 retrospective."""
    out: dict[str, str] = {}
    for _, parent_uuid, subs in PREDEFINED_CATEGORIES:
        for _, sub_uuid in subs:
            out[str(sub_uuid)] = str(parent_uuid)
    return out


def name_by_uuid() -> dict[str, str]:
    out: dict[str, str] = {}
    for parent_name, parent_uuid, subs in PREDEFINED_CATEGORIES:
        out[str(parent_uuid)] = parent_name
        for sub_name, sub_uuid in subs:
            out[str(sub_uuid)] = sub_name
    return out


def render_for_prompt() -> str:
    """Plaintext rendering of the category tree for the LLM system prompt."""
    lines = ["Predefined categories (must use these UUIDs exactly):"]
    for parent_name, parent_uuid, subs in PREDEFINED_CATEGORIES:
        lines.append(f"- {parent_name} [{parent_uuid}]")
        for sub_name, sub_uuid in subs:
            lines.append(f"    - {sub_name} [{sub_uuid}]")
    return "\n".join(lines)
