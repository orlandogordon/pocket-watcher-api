"""
System tag management.

Ensures each user has the required set of protected system tags
(e.g. "Needs Review", "Approved Duplicate").
"""

from uuid import uuid5, UUID
from datetime import datetime
from sqlalchemy.orm import Session

from src.db.core import TagDB
from src.logging_config import get_logger

logger = get_logger(__name__)

# Deterministic namespace for generating system tag UUIDs
_SYSTEM_TAG_NAMESPACE = UUID("a1b2c3d4-e5f6-7890-abcd-ef1234567890")

SYSTEM_TAG_DEFINITIONS = [
    {"tag_name": "Needs Review",       "color": "#F59E0B"},
    {"tag_name": "Approved Duplicate", "color": "#8B5CF6"},
]


def _make_system_tag_uuid(user_id: int, tag_name: str) -> UUID:
    """Generate a deterministic UUID for a system tag based on user_id and tag name."""
    return uuid5(_SYSTEM_TAG_NAMESPACE, f"{user_id}:{tag_name}")


def ensure_system_tags(user_id: int, db: Session) -> list[TagDB]:
    """
    Ensure all system tags exist for the given user. Creates any that are missing.
    Returns the full list of system tags for the user.
    """
    existing = db.query(TagDB).filter(
        TagDB.user_id == user_id,
        TagDB.is_system == True,
    ).all()
    existing_names = {t.tag_name for t in existing}

    created = []
    for defn in SYSTEM_TAG_DEFINITIONS:
        if defn["tag_name"] not in existing_names:
            tag = TagDB(
                id=_make_system_tag_uuid(user_id, defn["tag_name"]),
                user_id=user_id,
                tag_name=defn["tag_name"],
                color=defn["color"],
                is_system=True,
                created_at=datetime.utcnow(),
            )
            db.add(tag)
            created.append(tag)
            logger.info(f"Created system tag '{defn['tag_name']}' for user {user_id}")

    if created:
        db.commit()
        for tag in created:
            db.refresh(tag)

    return existing + created


def get_system_tag(user_id: int, db: Session, tag_name: str) -> TagDB | None:
    """Look up a specific system tag by name for a user."""
    return db.query(TagDB).filter(
        TagDB.user_id == user_id,
        TagDB.is_system == True,
        TagDB.tag_name == tag_name,
    ).first()
