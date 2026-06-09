from datetime import datetime, timezone
from typing import Annotated

from pydantic import PlainSerializer


def utcnow() -> datetime:
    """Naive UTC now — drop-in for the deprecated ``datetime.utcnow``.

    Columns are ``TIMESTAMP WITHOUT TIME ZONE``; we store naive UTC and stamp the
    offset at the serialization edge (see ``UTCDateTime``). Returning naive avoids
    the Postgres session-timezone conversion that an aware value would trigger.
    """
    return datetime.now(timezone.utc).replace(tzinfo=None)


def to_utc_iso(dt: datetime) -> str:
    """Serialize a datetime as ISO-8601 UTC with a ``Z`` suffix.

    A naive value is assumed to already be UTC; an aware value is converted.
    """
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)
    return dt.isoformat().replace("+00:00", "Z")


UTCDateTime = Annotated[
    datetime,
    PlainSerializer(to_utc_iso, return_type=str, when_used="json"),
]
