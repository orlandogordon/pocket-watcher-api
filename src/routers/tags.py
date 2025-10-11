from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session
from typing import List

from src.crud import crud_tag
from src.models import tag as tag_models
from src.models import transaction as transaction_models
from src.db.core import get_db, NotFoundError

router = APIRouter(
    prefix="/tags",
    tags=["tags"],
)

# This is a placeholder for a proper authentication dependency.
def get_current_user_id() -> int:
    return 1

@router.post("/", response_model=tag_models.TagResponse, status_code=status.HTTP_201_CREATED)
def create_tag(
    tag: tag_models.TagCreate,
    db: Session = Depends(get_db),
    user_id: int = Depends(get_current_user_id)
):
    """
    Create a new tag.
    """
    try:
        return crud_tag.create_db_tag(db=db, user_id=user_id, tag_data=tag)
    except ValueError as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))

@router.get("/", response_model=List[tag_models.TagResponse])
def read_tags(
    skip: int = 0,
    limit: int = 100,
    db: Session = Depends(get_db),
    user_id: int = Depends(get_current_user_id)
):
    """
    Retrieve all tags for the current user.
    """
    return crud_tag.read_db_tags(db=db, user_id=user_id, skip=skip, limit=limit)

@router.get("/search/", response_model=List[tag_models.TagResponse])
def search_tags(
    search_term: str,
    db: Session = Depends(get_db),
    user_id: int = Depends(get_current_user_id)
):
    """
    Search for tags by name.
    """
    return crud_tag.search_tags(db=db, user_id=user_id, search_term=search_term)

@router.get("/stats", response_model=List[tag_models.TagStats])
def get_all_tag_stats(
    db: Session = Depends(get_db),
    user_id: int = Depends(get_current_user_id)
):
    """
    Get statistics for all of the user's tags.
    """
    return crud_tag.get_all_tag_stats(db=db, user_id=user_id)

@router.get("/{tag_id}", response_model=tag_models.TagResponse)
def read_tag(
    tag_id: int,
    db: Session = Depends(get_db),
    user_id: int = Depends(get_current_user_id)
):
    """
    Retrieve a specific tag by its ID.
    """
    db_tag = crud_tag.read_db_tag(db=db, tag_id=tag_id, user_id=user_id)
    if db_tag is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Tag not found")
    return db_tag

@router.put("/{tag_id}", response_model=tag_models.TagResponse)
def update_tag(
    tag_id: int,
    tag: tag_models.TagUpdate,
    db: Session = Depends(get_db),
    user_id: int = Depends(get_current_user_id)
):
    """
    Update a tag's name or color.
    """
    try:
        return crud_tag.update_db_tag(db=db, tag_id=tag_id, user_id=user_id, tag_updates=tag)
    except NotFoundError as e:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(e))
    except ValueError as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))

@router.delete("/{tag_id}", response_model=tag_models.TagResponse)
def delete_tag(
    tag_id: int,
    db: Session = Depends(get_db),
    user_id: int = Depends(get_current_user_id)
):
    """
    Delete a tag. This will also remove all associations between this tag and any transactions.
    """
    db_tag = crud_tag.read_db_tag(db, tag_id=tag_id, user_id=user_id)
    if db_tag is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Tag not found")
    try:
        crud_tag.delete_db_tag(db=db, tag_id=tag_id, user_id=user_id)
    except ValueError as e:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(e))
    return db_tag

@router.get("/{tag_id}/stats", response_model=tag_models.TagStats)
def get_tag_stats(
    tag_id: int,
    db: Session = Depends(get_db),
    user_id: int = Depends(get_current_user_id)
):
    """
    Get statistics for a single tag.
    """
    try:
        return crud_tag.get_tag_stats(db=db, tag_id=tag_id, user_id=user_id)
    except NotFoundError as e:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(e))

@router.get("/{tag_id}/transactions", response_model=List[transaction_models.TransactionResponse])
def get_transactions_for_tag(
    tag_id: int,
    skip: int = 0,
    limit: int = 100,
    db: Session = Depends(get_db),
    user_id: int = Depends(get_current_user_id)
):
    """
    Get all transactions associated with a specific tag.
    """
    try:
        return crud_tag.get_transactions_for_tag(db=db, tag_id=tag_id, user_id=user_id, skip=skip, limit=limit)
    except NotFoundError as e:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(e))

@router.post("/transactions/", response_model=tag_models.TransactionTagResponse, status_code=status.HTTP_201_CREATED)
def add_tag_to_transaction(
    transaction_tag: tag_models.TransactionTagCreate,
    db: Session = Depends(get_db),
    user_id: int = Depends(get_current_user_id)
):
    """
    Add a tag to a single transaction.
    """
    try:
        return crud_tag.add_tag_to_transaction(
            db=db, user_id=user_id, 
            transaction_id=transaction_tag.transaction_id, 
            tag_id=transaction_tag.tag_id
        )
    except (NotFoundError, ValueError) as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))

@router.delete("/transactions/{transaction_id}/tags/{tag_id}", status_code=status.HTTP_204_NO_CONTENT)
def remove_tag_from_transaction(
    transaction_id: int,
    tag_id: int,
    db: Session = Depends(get_db),
    user_id: int = Depends(get_current_user_id)
):
    """
    Remove a tag from a single transaction.
    """
    try:
        if not crud_tag.remove_tag_from_transaction(db=db, user_id=user_id, transaction_id=transaction_id, tag_id=tag_id):
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Tag/Transaction association not found.")
    except (NotFoundError, ValueError) as e:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(e))

@router.post("/transactions/bulk-tag", status_code=status.HTTP_201_CREATED)
def bulk_tag_transactions(
    bulk_tag_request: tag_models.BulkTagRequest,
    db: Session = Depends(get_db),
    user_id: int = Depends(get_current_user_id)
):
    """
    Add a single tag to multiple transactions at once.
    """
    try:
        created_relations = crud_tag.bulk_tag_transactions(
            db=db,
            user_id=user_id,
            transaction_ids=bulk_tag_request.transaction_ids,
            tag_id=bulk_tag_request.tag_id
        )
        return {
            "message": f"{len(created_relations)} transaction(s) tagged successfully.",
            "tagged_count": len(created_relations)
        }
    except (NotFoundError, ValueError) as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))
