from fastapi import APIRouter, HTTPException, Query, Request
from typing import List, Dict, Any, Optional
from fastapi.params import Depends
from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError
from datetime import date
from decimal import Decimal
from uuid import UUID
from src.db.core import NotFoundError, get_db
from src.models.transaction import (
    TransactionCreate, TransactionUpdate, TransactionResponse, TransactionImport,
    TransactionRelationshipCreateByUUID, TransactionRelationshipUpdate, TransactionRelationship,
    TransactionBulkUpdate, TransactionFilter, TransactionStats, TransactionTypeEnum,
    TransactionSplitRequest, SplitAllocationResponse,
    AmortizationScheduleCreate, AmortizationScheduleResponse,
    MonthlyAverageResponse,
)
from src.crud.crud_transaction import (
    create_db_transaction,
    read_db_transaction_by_uuid,
    read_db_transactions,
    get_transaction_stats,
    get_monthly_averages,
    update_db_transaction_by_uuid,
    delete_db_transaction_by_uuid,
    bulk_create_transactions,
    create_transaction_relationship_by_uuid,
    read_transaction_relationships_by_uuid,
    update_transaction_relationship_by_uuid,
    delete_transaction_relationship_by_uuid,
    bulk_update_db_transactions,
    set_transaction_splits,
    get_transaction_splits,
    delete_transaction_splits,
    create_or_replace_amortization_schedule,
    read_amortization_schedule,
    delete_amortization_schedule,
)
from src.crud.crud_account import read_db_account_by_uuid
from src.crud.crud_category import read_db_category_by_uuid, read_db_categories_by_uuids
from src.crud.crud_tag import read_db_tag_by_uuid, read_db_tags_by_uuids
from src.auth.dependencies import get_current_user_id
from src.auth.context import current_user_id

router = APIRouter(
    prefix="/transactions",
    tags=["transactions"],
)

def _parse_uuid(value: str) -> UUID:
    try:
        return UUID(value)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid UUID format")


def _build_filters(
    db: Session,
    user_id: int,
    account_uuid: Optional[str],
    category_uuids: Optional[List[str]],
    subcategory_uuids: Optional[List[str]],
    tag_uuids: Optional[List[str]],
    transaction_type: Optional[TransactionTypeEnum],
    merchant_name: Optional[str],
    date_from: Optional[date],
    date_to: Optional[date],
    amount_min: Optional[Decimal],
    amount_max: Optional[Decimal],
    description_search: Optional[str],
) -> TransactionFilter:
    """Build a TransactionFilter, resolving UUIDs to int IDs."""
    account_id = None
    if account_uuid:
        account = read_db_account_by_uuid(db, _parse_uuid(account_uuid), user_id)
        if not account:
            raise HTTPException(status_code=404, detail="Account not found")
        account_id = account.id

    category_ids = None
    if category_uuids:
        parsed = [_parse_uuid(u) for u in category_uuids]
        cats = read_db_categories_by_uuids(db, parsed)
        if len(cats) != len(parsed):
            found = {c.uuid for c in cats}
            missing = [str(u) for u in parsed if u not in found]
            raise HTTPException(status_code=404, detail=f"Categories not found: {', '.join(missing)}")
        category_ids = [c.id for c in cats]

    subcategory_ids = None
    if subcategory_uuids:
        parsed = [_parse_uuid(u) for u in subcategory_uuids]
        subcats = read_db_categories_by_uuids(db, parsed)
        if len(subcats) != len(parsed):
            found = {c.uuid for c in subcats}
            missing = [str(u) for u in parsed if u not in found]
            raise HTTPException(status_code=404, detail=f"Subcategories not found: {', '.join(missing)}")
        subcategory_ids = [c.id for c in subcats]

    tag_ids = None
    if tag_uuids:
        parsed = [_parse_uuid(u) for u in tag_uuids]
        tags = read_db_tags_by_uuids(db, parsed, user_id)
        if len(tags) != len(parsed):
            found = {t.id for t in tags}
            missing = [str(u) for u in parsed if u not in found]
            raise HTTPException(status_code=404, detail=f"Tags not found: {', '.join(missing)}")
        tag_ids = [t.tag_id for t in tags]

    return TransactionFilter(
        account_id=account_id,
        category_ids=category_ids,
        subcategory_ids=subcategory_ids,
        tag_ids=tag_ids,
        transaction_type=transaction_type,
        merchant_name=merchant_name,
        date_from=date_from,
        date_to=date_to,
        amount_min=amount_min,
        amount_max=amount_max,
        description_search=description_search,
    )


@router.get("/", response_model=List[TransactionResponse])
def list_transactions(
    request: Request,
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=500),
    account_uuid: Optional[str] = Query(None),
    category_uuid: Optional[List[str]] = Query(None),
    subcategory_uuid: Optional[List[str]] = Query(None),
    tag_uuid: Optional[List[str]] = Query(None),
    transaction_type: Optional[TransactionTypeEnum] = Query(None),
    merchant_name: Optional[str] = Query(None),
    date_from: Optional[date] = Query(None),
    date_to: Optional[date] = Query(None),
    amount_min: Optional[Decimal] = Query(None),
    amount_max: Optional[Decimal] = Query(None),
    description_search: Optional[str] = Query(None),
    order_by: str = Query("transaction_date"),
    order_desc: bool = Query(True),
    db: Session = Depends(get_db),
) -> List[TransactionResponse]:
    """List and filter transactions with pagination."""
    user_id = current_user_id()
    filters = _build_filters(
        db, user_id, account_uuid, category_uuid, subcategory_uuid, tag_uuid,
        transaction_type, merchant_name, date_from, date_to, amount_min, amount_max,
        description_search,
    )
    transactions = read_db_transactions(
        db, user_id, filters=filters, skip=skip, limit=limit,
        order_by=order_by, order_desc=order_desc,
    )
    return [TransactionResponse.model_validate(t) for t in transactions]


@router.get("/stats", response_model=TransactionStats)
def transaction_stats(
    request: Request,
    account_uuid: Optional[str] = Query(None),
    category_uuid: Optional[List[str]] = Query(None),
    subcategory_uuid: Optional[List[str]] = Query(None),
    tag_uuid: Optional[List[str]] = Query(None),
    transaction_type: Optional[TransactionTypeEnum] = Query(None),
    merchant_name: Optional[str] = Query(None),
    date_from: Optional[date] = Query(None),
    date_to: Optional[date] = Query(None),
    amount_min: Optional[Decimal] = Query(None),
    amount_max: Optional[Decimal] = Query(None),
    description_search: Optional[str] = Query(None),
    db: Session = Depends(get_db),
) -> TransactionStats:
    """Get aggregate transaction statistics with optional filters."""
    user_id = current_user_id()
    filters = _build_filters(
        db, user_id, account_uuid, category_uuid, subcategory_uuid, tag_uuid,
        transaction_type, merchant_name, date_from, date_to, amount_min, amount_max,
        description_search,
    )
    return get_transaction_stats(db, user_id, filters=filters)


@router.get("/stats/monthly-averages", response_model=MonthlyAverageResponse)
def monthly_averages(
    year: int = Query(..., ge=2000, le=2100),
    month: Optional[int] = Query(None, ge=1, le=12),
    account_uuid: Optional[List[str]] = Query(None),
    db: Session = Depends(get_db),
) -> MonthlyAverageResponse:
    """Get monthly average income/expenses/net with category breakdown for a calendar year."""
    user_id = current_user_id()

    account_ids = None
    if account_uuid:
        from src.db.core import AccountDB
        parsed = [_parse_uuid(u) for u in account_uuid]
        accounts = db.query(AccountDB).filter(
            AccountDB.uuid.in_(parsed),
            AccountDB.user_id == user_id,
        ).all()
        if len(accounts) != len(parsed):
            found = {a.uuid for a in accounts}
            missing = [str(u) for u in parsed if u not in found]
            raise HTTPException(status_code=404, detail=f"Accounts not found: {', '.join(missing)}")
        account_ids = [a.id for a in accounts]

    return get_monthly_averages(db, user_id, year, month=month, account_ids=account_ids)


@router.post("/", status_code=201)
def create_transaction(request: Request, transaction: TransactionCreate, db: Session = Depends(get_db)) -> TransactionResponse:
    user_id = current_user_id()

    # Resolve UUIDs to int IDs
    account_id = None
    if transaction.account_uuid:
        account = read_db_account_by_uuid(db, transaction.account_uuid, user_id)
        if not account:
            raise HTTPException(status_code=404, detail="Account not found")
        account_id = account.id

    category_id = None
    if transaction.category_uuid:
        cat = read_db_category_by_uuid(db, transaction.category_uuid)
        if not cat:
            raise HTTPException(status_code=404, detail="Category not found")
        category_id = cat.id

    subcategory_id = None
    if transaction.subcategory_uuid:
        subcat = read_db_category_by_uuid(db, transaction.subcategory_uuid)
        if not subcat:
            raise HTTPException(status_code=404, detail="Subcategory not found")
        subcategory_id = subcat.id

    try:
        db_transaction = create_db_transaction(db, user_id, transaction, account_id=account_id, category_id=category_id, subcategory_id=subcategory_id)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    except IntegrityError as e:
        raise HTTPException(status_code=400, detail="Database integrity error.") from e
    return TransactionResponse.model_validate(db_transaction)

@router.patch("/bulk-update")
def bulk_update_transactions(request: Request, bulk_update_data: TransactionBulkUpdate, db: Session = Depends(get_db)) -> Dict[str, Any]:
    user_id = current_user_id()

    # Resolve transaction UUIDs - bulk_update_db_transactions filters by TransactionDB.id (UUID)
    transaction_ids = list(bulk_update_data.transaction_uuids)

    # Build update payload with resolved int IDs
    update_payload = {}
    if bulk_update_data.comments is not None:
        update_payload["comments"] = bulk_update_data.comments
    if bulk_update_data.account_uuid is not None:
        account = read_db_account_by_uuid(db, bulk_update_data.account_uuid, user_id)
        if not account:
            raise HTTPException(status_code=404, detail="Account not found")
        update_payload["account_id"] = account.id
    if bulk_update_data.category_uuid is not None:
        cat = read_db_category_by_uuid(db, bulk_update_data.category_uuid)
        if not cat:
            raise HTTPException(status_code=404, detail="Category not found")
        update_payload["category_id"] = cat.id
    if bulk_update_data.subcategory_uuid is not None:
        subcat = read_db_category_by_uuid(db, bulk_update_data.subcategory_uuid)
        if not subcat:
            raise HTTPException(status_code=404, detail="Subcategory not found")
        update_payload["subcategory_id"] = subcat.id

    if not update_payload:
        raise HTTPException(status_code=400, detail="No update fields provided.")

    try:
        updated_count = bulk_update_db_transactions(
            db=db,
            user_id=user_id,
            transaction_ids=transaction_ids,
            updates=update_payload
        )
        return {"message": f"Successfully updated {updated_count} transactions."}
    except NotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

@router.post("/bulk-upload/", status_code=201)
def create_transactions(request: Request, transaction_import: TransactionImport, db: Session = Depends(get_db)) -> List[TransactionResponse]:
    user_id = current_user_id()

    # Resolve account UUID
    account = read_db_account_by_uuid(db, transaction_import.account_uuid, user_id)
    if not account:
        raise HTTPException(status_code=404, detail="Account not found")

    try:
        created_transactions = bulk_create_transactions(db, user_id, transaction_import, account_id=account.id)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    except IntegrityError as e:
        raise HTTPException(status_code=400, detail="Database integrity error.") from e
    return [TransactionResponse.model_validate(t) for t in created_transactions]

@router.get("/{transaction_uuid}")
def read_transaction(request: Request, transaction_uuid: str, db: Session = Depends(get_db)) -> TransactionResponse:
    user_id = current_user_id()
    parsed_uuid = _parse_uuid(transaction_uuid)

    db_transaction = read_db_transaction_by_uuid(db, transaction_uuid=parsed_uuid, user_id=user_id)
    if not db_transaction:
        raise HTTPException(status_code=404, detail="Transaction not found")
    return TransactionResponse.model_validate(db_transaction)

@router.put("/{transaction_uuid}")
def update_transaction(request: Request, transaction_uuid: str, transaction: TransactionUpdate, db: Session = Depends(get_db)) -> TransactionResponse:
    user_id = current_user_id()
    parsed_uuid = _parse_uuid(transaction_uuid)

    # Resolve optional account UUID, distinguishing "not sent" from "sent as null"
    account_id = None
    clear_account = False
    if 'account_uuid' in transaction.model_fields_set:
        if transaction.account_uuid is not None:
            account = read_db_account_by_uuid(db, transaction.account_uuid, user_id)
            if not account:
                raise HTTPException(status_code=404, detail="Account not found")
            account_id = account.id
        else:
            clear_account = True

    # Resolve optional category UUIDs, distinguishing "not sent" from "sent as null"
    category_id = None
    clear_category = False
    if 'category_uuid' in transaction.model_fields_set:
        if transaction.category_uuid is not None:
            cat = read_db_category_by_uuid(db, transaction.category_uuid)
            if not cat:
                raise HTTPException(status_code=404, detail="Category not found")
            category_id = cat.id
        else:
            clear_category = True

    subcategory_id = None
    clear_subcategory = False
    if 'subcategory_uuid' in transaction.model_fields_set:
        if transaction.subcategory_uuid is not None:
            subcat = read_db_category_by_uuid(db, transaction.subcategory_uuid)
            if not subcat:
                raise HTTPException(status_code=404, detail="Subcategory not found")
            subcategory_id = subcat.id
        else:
            clear_subcategory = True

    try:
        db_transaction = update_db_transaction_by_uuid(
            db, transaction_uuid=parsed_uuid, user_id=user_id, transaction_updates=transaction,
            account_id=account_id, clear_account=clear_account,
            category_id=category_id, subcategory_id=subcategory_id,
            clear_category=clear_category, clear_subcategory=clear_subcategory,
        )
    except NotFoundError as e:
        raise HTTPException(status_code=404, detail="Transaction not found") from e
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    return TransactionResponse.model_validate(db_transaction)

@router.delete("/{transaction_uuid}", status_code=204)
def delete_transaction(request: Request, transaction_uuid: str, db: Session = Depends(get_db)):
    user_id = current_user_id()
    parsed_uuid = _parse_uuid(transaction_uuid)

    db_transaction = read_db_transaction_by_uuid(db, transaction_uuid=parsed_uuid, user_id=user_id)
    if not db_transaction:
        raise HTTPException(status_code=404, detail="Transaction not found")
    try:
        delete_db_transaction_by_uuid(db, transaction_uuid=parsed_uuid, user_id=user_id)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    return None

@router.put("/{transaction_uuid}/splits", status_code=200)
def set_splits(transaction_uuid: str, split_request: TransactionSplitRequest,
               db: Session = Depends(get_db)) -> TransactionResponse:
    user_id = current_user_id()
    parsed_uuid = _parse_uuid(transaction_uuid)
    try:
        txn = set_transaction_splits(db, user_id, parsed_uuid, split_request)
    except NotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e)) from e
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    return TransactionResponse.model_validate(txn)


@router.get("/{transaction_uuid}/splits", status_code=200)
def get_splits(transaction_uuid: str,
               db: Session = Depends(get_db)) -> List[SplitAllocationResponse]:
    user_id = current_user_id()
    parsed_uuid = _parse_uuid(transaction_uuid)
    try:
        allocations = get_transaction_splits(db, user_id, parsed_uuid)
    except NotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e)) from e
    return [SplitAllocationResponse.model_validate(a) for a in allocations]


@router.delete("/{transaction_uuid}/splits", status_code=204)
def remove_splits(transaction_uuid: str, db: Session = Depends(get_db)):
    user_id = current_user_id()
    parsed_uuid = _parse_uuid(transaction_uuid)
    try:
        delete_transaction_splits(db, user_id, parsed_uuid)
    except NotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e)) from e
    return None


@router.put("/{transaction_uuid}/amortization", response_model=AmortizationScheduleResponse)
def set_amortization(transaction_uuid: str, schedule: AmortizationScheduleCreate,
                     db: Session = Depends(get_db)) -> AmortizationScheduleResponse:
    user_id = current_user_id()
    parsed_uuid = _parse_uuid(transaction_uuid)
    try:
        return create_or_replace_amortization_schedule(db, user_id, parsed_uuid, schedule)
    except NotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e)) from e
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e


@router.get("/{transaction_uuid}/amortization", response_model=AmortizationScheduleResponse)
def get_amortization(transaction_uuid: str,
                     db: Session = Depends(get_db)) -> AmortizationScheduleResponse:
    user_id = current_user_id()
    parsed_uuid = _parse_uuid(transaction_uuid)
    try:
        result = read_amortization_schedule(db, user_id, parsed_uuid)
    except NotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e)) from e
    if not result:
        raise HTTPException(status_code=404, detail="No amortization schedule found")
    return result


@router.delete("/{transaction_uuid}/amortization", status_code=204)
def remove_amortization(transaction_uuid: str, db: Session = Depends(get_db)):
    user_id = current_user_id()
    parsed_uuid = _parse_uuid(transaction_uuid)
    try:
        delete_amortization_schedule(db, user_id, parsed_uuid)
    except NotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e)) from e
    return None


@router.get("/{transaction_uuid}/relationships", response_model=List[TransactionRelationship])
def get_relationships(transaction_uuid: str, db: Session = Depends(get_db)) -> List[TransactionRelationship]:
    """Get all relationships for a transaction."""
    user_id = current_user_id()
    parsed_uuid = _parse_uuid(transaction_uuid)
    try:
        relationships = read_transaction_relationships_by_uuid(db, user_id, parsed_uuid)
    except NotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e)) from e
    return [TransactionRelationship.model_validate(r) for r in relationships]


@router.post("/{transaction_uuid}/relationships", status_code=201)
def create_relationship(transaction_uuid: str, relationship: TransactionRelationshipCreateByUUID, db: Session = Depends(get_db)) -> TransactionRelationship:
    """
    Create a relationship between two transactions using UUIDs.

    Relationship types:
    - REFUNDS: To transaction is a refund of from transaction
    - OFFSETS: Transactions offset each other
    - FEES_FOR: To transaction is a fee for from transaction
    - REVERSES: To transaction reverses from transaction
    """
    user_id = current_user_id()
    parsed_uuid = _parse_uuid(transaction_uuid)
    try:
        db_relationship = create_transaction_relationship_by_uuid(db, user_id, from_transaction_uuid=parsed_uuid, relationship_data=relationship)
    except NotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e)) from e
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    return TransactionRelationship.model_validate(db_relationship)


@router.put("/relationships/{relationship_uuid}")
def update_relationship(relationship_uuid: str, relationship_update: TransactionRelationshipUpdate, db: Session = Depends(get_db)) -> TransactionRelationship:
    """
    Update an existing transaction relationship.
    All fields are optional - only provided fields will be updated.
    """
    user_id = current_user_id()
    parsed_uuid = _parse_uuid(relationship_uuid)

    # Convert to dict and exclude unset values
    update_data = relationship_update.model_dump(exclude_unset=True)

    if not update_data:
        raise HTTPException(status_code=400, detail="No update fields provided")

    # Resolve to_transaction_uuid to int ID
    if 'to_transaction_uuid' in update_data:
        to_txn = read_db_transaction_by_uuid(db, update_data.pop('to_transaction_uuid'), user_id)
        if not to_txn:
            raise HTTPException(status_code=404, detail="Target transaction not found")
        update_data['to_transaction_id'] = to_txn.db_id

    try:
        db_relationship = update_transaction_relationship_by_uuid(db, user_id, relationship_uuid=parsed_uuid, relationship_updates=update_data)
    except NotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e)) from e
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    return TransactionRelationship.model_validate(db_relationship)


@router.delete("/relationships/{relationship_uuid}", status_code=204)
def delete_relationship(relationship_uuid: str, db: Session = Depends(get_db)):
    """
    Delete a transaction relationship.
    """
    user_id = current_user_id()
    parsed_uuid = _parse_uuid(relationship_uuid)
    try:
        delete_transaction_relationship_by_uuid(db, user_id, relationship_uuid=parsed_uuid)
    except NotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e)) from e
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    return None
