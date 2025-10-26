from fastapi import APIRouter, HTTPException, Request
from typing import List, Dict, Any
from fastapi.params import Depends
from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError
from src.db.core import NotFoundError, get_db
from src.models.transaction import TransactionCreate, TransactionUpdate, TransactionResponse, TransactionImport, TransactionRelationshipCreate, TransactionRelationshipUpdate, TransactionRelationship, TransactionBulkUpdate
from src.crud.crud_transaction import (
    create_db_transaction,
    read_db_transaction,
    update_db_transaction,
    delete_db_transaction,
    bulk_create_transactions,
    create_transaction_relationship,
    update_transaction_relationship,
    delete_transaction_relationship,
    bulk_update_db_transactions
)

router = APIRouter(
    prefix="/transactions",
    tags=["transactions"],
)

# A placeholder for user authentication
def get_current_user_id():
    return 1

@router.post("/")
def create_transaction(request: Request, transaction: TransactionCreate, db: Session = Depends(get_db)) -> TransactionResponse:
    user_id = get_current_user_id()
    try:
        db_transaction = create_db_transaction(db, user_id, transaction)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    except IntegrityError as e:
        raise HTTPException(status_code=400, detail="Database integrity error.") from e
    return TransactionResponse.model_validate(db_transaction)

@router.patch("/bulk-update")
def bulk_update_transactions(request: Request, bulk_update_data: TransactionBulkUpdate, db: Session = Depends(get_db)) -> Dict[str, Any]:
    user_id = get_current_user_id()
    
    update_payload = bulk_update_data.model_dump(exclude_unset=True, exclude={"transaction_ids"})
    
    if not update_payload:
        raise HTTPException(status_code=400, detail="No update fields provided.")

    try:
        updated_count = bulk_update_db_transactions(
            db=db, 
            user_id=user_id, 
            transaction_ids=bulk_update_data.transaction_ids, 
            updates=update_payload
        )
        return {"message": f"Successfully updated {updated_count} transactions."}
    except NotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

@router.post("/bulk-upload/")
def create_transactions(request: Request, transaction_import: TransactionImport, db: Session = Depends(get_db)) -> List[TransactionResponse]:
    user_id = get_current_user_id()
    try:
        created_transactions = bulk_create_transactions(db, user_id, transaction_import)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    except IntegrityError as e:
        raise HTTPException(status_code=400, detail="Database integrity error.") from e
    return [TransactionResponse.model_validate(t) for t in created_transactions]

@router.get("/{transaction_id}")
def read_transaction(request: Request, transaction_id: str, db: Session = Depends(get_db)) -> TransactionResponse:
    user_id = get_current_user_id()
    db_transaction = read_db_transaction(db, transaction_id=int(transaction_id), user_id=user_id)
    if not db_transaction:
        raise HTTPException(status_code=404, detail="Transaction not found")
    return TransactionResponse.model_validate(db_transaction)

@router.put("/{transaction_id}")
def update_transaction(request: Request, transaction_id: str, transaction: TransactionUpdate, db: Session = Depends(get_db)) -> TransactionResponse:
    user_id = get_current_user_id()
    try:
        db_transaction = update_db_transaction(db, transaction_id=int(transaction_id), user_id=user_id, transaction_updates=transaction)
    except NotFoundError as e:
        raise HTTPException(status_code=404, detail="Transaction not found") from e
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    return TransactionResponse.model_validate(db_transaction)

@router.delete("/{transaction_id}")
def delete_transaction(request: Request, transaction_id: str, db: Session = Depends(get_db)) -> TransactionResponse:
    user_id = get_current_user_id()
    db_transaction = read_db_transaction(db, transaction_id=int(transaction_id), user_id=user_id)
    if not db_transaction:
        raise HTTPException(status_code=404, detail="Transaction not found")
    try:
        delete_db_transaction(db, transaction_id=int(transaction_id), user_id=user_id)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    return TransactionResponse.model_validate(db_transaction)

@router.post("/{transaction_id}/relationships", status_code=201)
def create_relationship(transaction_id: int, relationship: TransactionRelationshipCreate, db: Session = Depends(get_db)) -> TransactionRelationship:
    """
    Create a relationship between two transactions.

    Relationship types:
    - REFUNDS: To transaction is a refund of from transaction
    - OFFSETS: Transactions offset each other
    - SPLITS: Part of a split transaction
    - FEES_FOR: To transaction is a fee for from transaction
    - REVERSES: To transaction reverses from transaction
    """
    user_id = get_current_user_id()
    try:
        db_relationship = create_transaction_relationship(db, user_id, transaction_id, relationship)
    except NotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e)) from e
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    return TransactionRelationship.model_validate(db_relationship)


@router.put("/relationships/{relationship_id}")
def update_relationship(relationship_id: int, relationship_update: TransactionRelationshipUpdate, db: Session = Depends(get_db)) -> TransactionRelationship:
    """
    Update an existing transaction relationship.
    All fields are optional - only provided fields will be updated.
    """
    user_id = get_current_user_id()

    # Convert to dict and exclude unset values
    update_data = relationship_update.model_dump(exclude_unset=True)

    if not update_data:
        raise HTTPException(status_code=400, detail="No update fields provided")

    try:
        db_relationship = update_transaction_relationship(db, user_id, relationship_id, update_data)
    except NotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e)) from e
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    return TransactionRelationship.model_validate(db_relationship)


@router.delete("/relationships/{relationship_id}", status_code=204)
def delete_relationship(relationship_id: int, db: Session = Depends(get_db)):
    """
    Delete a transaction relationship.
    """
    user_id = get_current_user_id()
    try:
        delete_transaction_relationship(db, user_id, relationship_id)
    except NotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e)) from e
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    return None
