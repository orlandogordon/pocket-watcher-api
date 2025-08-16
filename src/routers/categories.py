from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session
from typing import List

from src.crud import crud_category
from src.models import category as category_models
from src.db.core import get_db, NotFoundError

router = APIRouter(
    prefix="/categories",
    tags=["categories"],
)

# In a real app, you would add a dependency here to check for admin privileges.
def get_admin_user():
    # Placeholder for admin user check
    return {"username": "admin"}

@router.post("/", response_model=category_models.CategoryResponse, status_code=status.HTTP_201_CREATED)
def create_category(
    category: category_models.CategoryCreate,
    db: Session = Depends(get_db),
    admin: dict = Depends(get_admin_user) # Protect this endpoint
):
    """
    Create a new global category. (Admin only)
    """
    try:
        return crud_category.create_db_category(db=db, category_data=category)
    except ValueError as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))

@router.get("/", response_model=List[category_models.CategoryResponse])
def read_categories(
    skip: int = 0,
    limit: int = 100,
    db: Session = Depends(get_db)
):
    """
    Retrieve all global categories.
    """
    return crud_category.read_db_categories(db=db, skip=skip, limit=limit)

@router.get("/{category_id}", response_model=category_models.CategoryResponse)
def read_category(
    category_id: int,
    db: Session = Depends(get_db)
):
    """
    Retrieve a specific category by its ID.
    """
    db_category = crud_category.read_db_category(db=db, category_id=category_id)
    if db_category is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Category not found")
    return db_category

@router.put("/{category_id}", response_model=category_models.CategoryResponse)
def update_category(
    category_id: int,
    category: category_models.CategoryUpdate,
    db: Session = Depends(get_db),
    admin: dict = Depends(get_admin_user) # Protect this endpoint
):
    """
    Update a category's name or parent. (Admin only)
    """
    try:
        return crud_category.update_db_category(db=db, category_id=category_id, category_updates=category)
    except NotFoundError as e:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(e))
    except ValueError as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))

@router.delete("/{category_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_category(
    category_id: int,
    db: Session = Depends(get_db),
    admin: dict = Depends(get_admin_user) # Protect this endpoint
):
    """
    Delete a global category. (Admin only)
    """
    try:
        crud_category.delete_db_category(db=db, category_id=category_id)
    except NotFoundError as e:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(e))
    except ValueError as e:
        # This error is raised if the category is still in use
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(e))
