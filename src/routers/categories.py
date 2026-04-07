from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session
from typing import List
from uuid import UUID

from src.crud import crud_category
from src.models import category as category_models
from src.db.core import get_db, NotFoundError
from src.auth.dependencies import get_current_user_id, get_current_admin_user_id

router = APIRouter(
    prefix="/categories",
    tags=["categories"],
)

@router.post("/", response_model=category_models.CategoryResponse, status_code=status.HTTP_201_CREATED)
def create_category(
    category: category_models.CategoryCreate,
    db: Session = Depends(get_db),
    admin_id: int = Depends(get_current_admin_user_id),
):
    """
    Create a new global category. (Admin only)
    """
    # Resolve parent_category_uuid to int ID
    parent_category_id = None
    if category.parent_category_uuid is not None:
        parent = crud_category.read_db_category_by_uuid(db, category.parent_category_uuid)
        if not parent:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Parent category not found")
        parent_category_id = parent.id
    try:
        return crud_category.create_db_category(db=db, category_data=category, parent_category_id=parent_category_id)
    except ValueError as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))

@router.get("/", response_model=List[category_models.CategoryResponse])
def read_categories(
    skip: int = 0,
    limit: int = 100,
    db: Session = Depends(get_db),
    user_id: int = Depends(get_current_user_id),
):
    """
    Retrieve all global categories.
    """
    return crud_category.read_db_categories(db=db, skip=skip, limit=limit)

@router.get("/{category_uuid}", response_model=category_models.CategoryResponse)
def read_category(
    category_uuid: str,
    db: Session = Depends(get_db),
    user_id: int = Depends(get_current_user_id),
):
    """
    Retrieve a specific category by its UUID.
    """
    try:
        parsed_uuid = UUID(category_uuid)
    except ValueError:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid UUID format")
    db_category = crud_category.read_db_category_by_uuid(db=db, category_uuid=parsed_uuid)
    if db_category is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Category not found")
    return db_category

@router.put("/{category_uuid}", response_model=category_models.CategoryResponse)
def update_category(
    category_uuid: str,
    category: category_models.CategoryUpdate,
    db: Session = Depends(get_db),
    admin_id: int = Depends(get_current_admin_user_id),
):
    """
    Update a category's name or parent. (Admin only)
    """
    try:
        parsed_uuid = UUID(category_uuid)
    except ValueError:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid UUID format")

    # Resolve parent_category_uuid if provided
    from src.crud.crud_category import _UNSET
    parent_category_id = _UNSET
    update_data = category.model_dump(exclude_unset=True)
    if 'parent_category_uuid' in update_data:
        if category.parent_category_uuid is not None:
            parent = crud_category.read_db_category_by_uuid(db, category.parent_category_uuid)
            if not parent:
                raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Parent category not found")
            parent_category_id = parent.id
        else:
            parent_category_id = None  # Explicitly setting to None (removing parent)

    try:
        return crud_category.update_db_category_by_uuid(db=db, category_uuid=parsed_uuid, category_updates=category, parent_category_id=parent_category_id)
    except NotFoundError as e:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(e))
    except ValueError as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))

@router.delete("/{category_uuid}", status_code=status.HTTP_204_NO_CONTENT)
def delete_category(
    category_uuid: str,
    force: bool = False,
    db: Session = Depends(get_db),
    admin_id: int = Depends(get_current_admin_user_id),
):
    """
    Delete a global category. (Admin only)

    Use ?force=true to remove all references (budget allocations, transaction categories,
    financial plan expenses) and reassign child categories before deleting.
    """
    try:
        parsed_uuid = UUID(category_uuid)
    except ValueError:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid UUID format")
    try:
        crud_category.delete_db_category_by_uuid(db=db, category_uuid=parsed_uuid, force=force)
    except NotFoundError as e:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(e))
    except ValueError as e:
        # This error is raised if the category is still in use
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(e))
