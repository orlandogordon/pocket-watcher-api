"""Shared router dependencies/helpers.

`parse_uuid` is the single UUID path-param parser used across routers,
replacing the per-router copies. It raises 400 on a malformed UUID;
standardizing that status (400 vs FastAPI-native 422) is tracked as #58.
"""
from uuid import UUID

from fastapi import HTTPException, status


def parse_uuid(value: str) -> UUID:
    try:
        return UUID(value)
    except ValueError:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid UUID format",
        )
