from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session
from typing import List
from uuid import UUID

from src.crud import crud_category
from src.models import category as category_models
from src.db.core import get_db
from src.auth.dependencies import get_current_user_id

router = APIRouter(
    prefix="/categories",
    tags=["categories"],
)


_LOCKED_DETAIL = "Categories are managed via code + migration (src/constants/categories.py)."


@router.post("/", status_code=status.HTTP_405_METHOD_NOT_ALLOWED)
def create_category():
    """Disabled — categories are locked to the code-defined tree (see #29)."""
    raise HTTPException(status_code=status.HTTP_405_METHOD_NOT_ALLOWED, detail=_LOCKED_DETAIL)


@router.get("/", response_model=List[category_models.CategoryResponse])
def read_categories(
    skip: int = 0,
    limit: int = 100,
    db: Session = Depends(get_db),
    user_id: int = Depends(get_current_user_id),
):
    """Retrieve all global categories."""
    return crud_category.read_db_categories(db=db, skip=skip, limit=limit)


@router.get("/{category_uuid}", response_model=category_models.CategoryResponse)
def read_category(
    category_uuid: str,
    db: Session = Depends(get_db),
    user_id: int = Depends(get_current_user_id),
):
    """Retrieve a specific category by its UUID."""
    try:
        parsed_uuid = UUID(category_uuid)
    except ValueError:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid UUID format")
    db_category = crud_category.read_db_category_by_uuid(db=db, category_uuid=parsed_uuid)
    if db_category is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Category not found")
    return db_category


@router.put("/{category_uuid}", status_code=status.HTTP_405_METHOD_NOT_ALLOWED)
def update_category(category_uuid: str):
    """Disabled — categories are locked to the code-defined tree (see #29)."""
    raise HTTPException(status_code=status.HTTP_405_METHOD_NOT_ALLOWED, detail=_LOCKED_DETAIL)


@router.delete("/{category_uuid}", status_code=status.HTTP_405_METHOD_NOT_ALLOWED)
def delete_category(category_uuid: str):
    """Disabled — categories are locked to the code-defined tree (see #29)."""
    raise HTTPException(status_code=status.HTTP_405_METHOD_NOT_ALLOWED, detail=_LOCKED_DETAIL)
