from fastapi import APIRouter, Depends, HTTPException, status, UploadFile, File, BackgroundTasks, Form, Query
from sqlalchemy.orm import Session
import uuid
import io
import time
from uuid import uuid4, UUID
from datetime import datetime, date, timedelta
from decimal import Decimal
from typing import Optional, List, Dict
import redis

from src.db.core import (
    get_db, UploadJobDB, SkippedTransactionDB, TransactionDB, InvestmentTransactionDB,
    AccountDB, TransactionType, SourceType, InvestmentTransactionType, AccountType,
    CategoryDB, TagDB, TransactionTagDB, ParsedImportDB,
)
from src.auth.dependencies import get_current_user_id
from src.services import s3, importer
from src.services.importer import PARSER_MAPPING
from src.services.redis_client import get_redis_dependency
from src.services.preview_session import (
    create_preview_session,
    get_preview_session,
    save_preview_session,
    delete_preview_session,
    extend_session_expiry,
    list_user_sessions,
)
from src.services.duplicate_analyzer import (
    analyze_regular_transactions,
    analyze_investment_transactions,
)
from src.services.description_cleanup import clean_descriptions
from src.services.system_tags import get_system_tag
from src.crud.crud_transaction import (
    generate_transaction_hash,
    update_account_balance_from_transaction,
)
from src.crud.crud_investment import (
    generate_investment_transaction_hash,
    map_transaction_type_to_enum,
    rebuild_holdings_from_transactions,
    _update_investment_account_balance,
)
from src.crud.crud_account import get_db_account_by_last_four
from src.models.preview import (
    EditTransactionRequest,
    BulkEditRequest,
    RejectItemRequest,
    RestoreItemRequest,
    ConfirmImportRequest,
    BulkRejectItemRequest,
    BulkRestoreItemRequest,
    PreviewSessionInfo,
)
from src.parser.models import ParsedInvestmentTransaction
from src.logging_config import get_logger

logger = get_logger(__name__)

router = APIRouter(
    prefix="/uploads",
    tags=["uploads"],
)


# ===== EXISTING ENDPOINTS (unchanged) =====


@router.post("/statement", status_code=status.HTTP_202_ACCEPTED)
async def upload_statement(
    background_tasks: BackgroundTasks,
    institution: str = Form(...),
    file: UploadFile = File(...),
    account_uuid: Optional[str] = Form(None),
    skip_duplicates: bool = Form(True),
    user_id: int = Depends(get_current_user_id),
    db: Session = Depends(get_db)
):
    """
    Upload a financial statement file (PDF or CSV) for asynchronous processing.

    This endpoint accepts a file, uploads it to a secure storage bucket,
    and schedules a background task to parse the statement and import the data.

    Args:
        institution: The name of the financial institution (e.g., 'amex', 'tdbank').
        file: The statement file to upload.
        account_uuid: (Optional) The UUID of the account to associate with all transactions from this file.
        skip_duplicates: (Optional) Whether to skip duplicate transactions. Default: True.
            - True: Skip duplicates (don't create them in database)
            - False: Create duplicates anyway

    Returns:
        JSON with upload_job_id for tracking progress and file_path (S3 key)
    """
    if file.content_type not in ["application/pdf", "text/csv"]:
        raise HTTPException(status_code=status.HTTP_415_UNSUPPORTED_MEDIA_TYPE, detail="Only PDF or CSV files are supported.")

    # Resolve account UUID to int ID
    account_id = None
    if account_uuid:
        try:
            parsed_account_uuid = UUID(account_uuid)
        except ValueError:
            raise HTTPException(status_code=400, detail="Invalid account UUID format")
        from src.crud.crud_account import read_db_account_by_uuid
        account = read_db_account_by_uuid(db, parsed_account_uuid, user_id)
        if not account:
            raise HTTPException(status_code=404, detail="Account not found")
        account_id = account.id

    # Generate a unique, secure filename
    file_extension = ".pdf" if file.content_type == "application/pdf" else ".csv"
    unique_filename = f"{uuid.uuid4()}{file_extension}"
    s3_key = f"statements/{user_id}/{unique_filename}"

    # Create upload job record
    upload_job = UploadJobDB(
        user_id=user_id,
        file_path=s3_key,
        institution=institution,
        account_id=account_id,
        skip_duplicates=skip_duplicates,
        status="PENDING"
    )
    db.add(upload_job)
    db.commit()
    db.refresh(upload_job)

    try:
        # Upload the file to S3
        s3.upload_file_to_s3(file_obj=file.file, bucket=s3.get_s3_bucket(), object_name=s3_key)

        # Add the processing job to the background
        background_tasks.add_task(
            importer.process_statement,
            db=db,
            user_id=user_id,
            upload_job_id=upload_job.id,
            s3_key=s3_key,
            institution=institution,
            file_content_type=file.content_type,
            account_id=account_id,
            skip_duplicates=skip_duplicates
        )

        return {
            "message": "File upload accepted and is being processed.",
            "upload_job_id": upload_job.id,
            "file_path": s3_key,
            "skip_duplicates": skip_duplicates
        }

    except Exception as e:
        # Mark job as failed if upload fails
        upload_job.status = "FAILED"
        upload_job.error_message = f"File upload failed: {str(e)}"
        db.commit()
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"An error occurred during file upload: {e}")


@router.get("/jobs")
def list_upload_jobs(
    skip: int = Query(0, ge=0),
    limit: int = Query(50, ge=1, le=100),
    user_id: int = Depends(get_current_user_id),
    db: Session = Depends(get_db)
):
    """
    List all upload jobs for the current user.

    Returns upload jobs ordered by created_at descending (newest first).
    """
    jobs = db.query(UploadJobDB).filter(
        UploadJobDB.user_id == user_id
    ).order_by(UploadJobDB.created_at.desc()).offset(skip).limit(limit).all()

    return {
        "jobs": jobs,
        "skip": skip,
        "limit": limit
    }


@router.get("/jobs/{job_id}")
def get_upload_job_status(
    job_id: int,
    user_id: int = Depends(get_current_user_id),
    db: Session = Depends(get_db)
):
    """
    Get status and results of a specific upload job.

    Returns detailed information about the upload job including:
    - Processing status (PENDING, PROCESSING, COMPLETED, FAILED)
    - Transaction counts (created and skipped)
    - Error messages (if failed)
    - Timestamps
    """
    job = db.query(UploadJobDB).filter(
        UploadJobDB.id == job_id,
        UploadJobDB.user_id == user_id
    ).first()

    if not job:
        raise HTTPException(status_code=404, detail="Upload job not found")

    return {
        "id": job.id,
        "status": job.status,
        "institution": job.institution,
        "account_id": job.account_id,
        "skip_duplicates": job.skip_duplicates,
        "file_path": job.file_path,
        "transactions_created": job.transactions_created,
        "transactions_skipped": job.transactions_skipped,
        "investment_transactions_created": job.investment_transactions_created,
        "investment_transactions_skipped": job.investment_transactions_skipped,
        "error_message": job.error_message,
        "created_at": job.created_at,
        "started_at": job.started_at,
        "completed_at": job.completed_at
    }


@router.get("/jobs/{job_id}/skipped")
def get_skipped_transactions(
    job_id: int,
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=500),
    user_id: int = Depends(get_current_user_id),
    db: Session = Depends(get_db)
):
    """
    Get detailed list of transactions that were skipped as duplicates for a specific upload job.

    Returns both the parsed transaction data (what would have been created) and the existing
    transaction in the database (the duplicate).
    """
    # Verify job ownership
    job = db.query(UploadJobDB).filter(
        UploadJobDB.id == job_id,
        UploadJobDB.user_id == user_id
    ).first()

    if not job:
        raise HTTPException(status_code=404, detail="Upload job not found")

    # Get skipped transactions
    skipped = db.query(SkippedTransactionDB).filter(
        SkippedTransactionDB.upload_job_id == job_id
    ).offset(skip).limit(limit).all()

    # Build results with existing transaction details
    results = []
    for s in skipped:
        existing_txn = None
        if s.transaction_type == "REGULAR" and s.existing_transaction_id:
            existing_txn = db.query(TransactionDB).filter(
                TransactionDB.id == s.existing_transaction_id
            ).first()
        elif s.transaction_type == "INVESTMENT" and s.existing_investment_transaction_id:
            existing_txn = db.query(InvestmentTransactionDB).filter(
                InvestmentTransactionDB.id == s.existing_investment_transaction_id
            ).first()

        results.append({
            "id": s.id,
            "transaction_type": s.transaction_type,
            "skipped_transaction": {
                "date": s.parsed_date,
                "amount": s.parsed_amount,
                "description": s.parsed_description,
                "transaction_type": s.parsed_transaction_type,
                "symbol": s.parsed_symbol,
                "quantity": s.parsed_quantity,
                "full_data": s.parsed_data_json
            },
            "existing_transaction": existing_txn,
            "reason": "duplicate",
            "created_at": s.created_at
        })

    return {
        "upload_job_id": job_id,
        "total_skipped": job.transactions_skipped + job.investment_transactions_skipped,
        "items": results,
        "skip": skip,
        "limit": limit
    }


# ===== PREVIEW FLOW ENDPOINTS =====


def _resolve_display_description(item: dict, parsed_data: dict) -> Optional[str]:
    """Choose the description to store on TransactionDB.

    Precedence: user edit > LLM/cache cleaned value > raw parser output.
    """
    edited = item.get("edited_data") or {}
    return (
        edited.get("description")
        or item.get("cleaned_description")
        or parsed_data.get("description")
    )


def _parse_iso(value: Optional[str]) -> Optional[datetime]:
    """Parse ISO datetime as produced by item['llm_processed_at']. None-safe."""
    if not value:
        return None
    return datetime.fromisoformat(value)


def _recompute_summary(session: dict) -> None:
    """Recompute the summary counts from the current session state."""
    rejected = sum(
        len(session["rejected"][k])
        for k in ["transactions", "investment_transactions"]
    )
    ready = sum(
        len(session["ready_to_import"][k])
        for k in ["transactions", "investment_transactions"]
    )

    session["summary"] = {
        "total_parsed": rejected + ready,
        "rejected": rejected,
        "ready_to_import": ready,
        "can_confirm": True,
    }


@router.post("/statement/preview", status_code=201)
async def create_statement_preview(
    file: UploadFile = File(...),
    institution: str = Form(...),
    account_uuid: Optional[str] = Form(None),
    user_id: int = Depends(get_current_user_id),
    db: Session = Depends(get_db),
    r: redis.Redis = Depends(get_redis_dependency),
):
    """
    Parse statement and create preview session with duplicate analysis.

    Returns a preview of all transactions found in the statement, separated into:
    - rejected: Transactions flagged as duplicates or unmapped types
    - ready_to_import: Unique transactions ready to be imported

    The preview is stored in Redis with a 12-hour TTL.
    Use the review, edit, and confirm endpoints to finalize the import.
    """
    if file.content_type not in ["application/pdf", "text/csv"]:
        raise HTTPException(
            status_code=status.HTTP_415_UNSUPPORTED_MEDIA_TYPE,
            detail="Only PDF or CSV files are supported."
        )

    # Validate parser exists
    parser = PARSER_MAPPING.get(institution.lower())
    if not parser:
        raise HTTPException(400, f"No parser for institution '{institution}'")

    # Resolve account UUID to int ID
    account_id = None
    if account_uuid:
        try:
            parsed_account_uuid = UUID(account_uuid)
        except ValueError:
            raise HTTPException(status_code=400, detail="Invalid account UUID format")
        from src.crud.crud_account import read_db_account_by_uuid
        account_obj = read_db_account_by_uuid(db, parsed_account_uuid, user_id)
        if not account_obj:
            raise HTTPException(status_code=404, detail="Account not found")
        account_id = account_obj.id

    # Parse the file
    file_bytes = await file.read()
    file_obj = io.BytesIO(file_bytes)
    is_csv = file.content_type == "text/csv"
    source_type = "CSV" if is_csv else "PDF"

    try:
        parsed_data = parser.parse(file_obj, is_csv=is_csv)
    except Exception as e:
        logger.error(f"Failed to parse {institution} statement: {e}")
        raise HTTPException(400, f"Failed to parse statement: {str(e)}")

    # Resolve account_id from parsed account info if not provided
    resolved_account_id = account_id
    account_info_dict = None
    if parsed_data.account_info:
        account_info_dict = {
            "account_number_last4": parsed_data.account_info.account_number_last4,
        }
        if not resolved_account_id:
            try:
                found = get_db_account_by_last_four(
                    db, user_id=user_id,
                    last_four=parsed_data.account_info.account_number_last4
                )
                if found:
                    resolved_account_id = found.id
                    account_info_dict["suggested_account_id"] = found.id
                    account_info_dict["suggested_account_name"] = found.account_name
            except Exception:
                logger.warning(f"Could not resolve account from last4: {parsed_data.account_info.account_number_last4}")

    # Analyze duplicates
    rejected_txns, ready_txns = analyze_regular_transactions(
        parsed_data.transactions, user_id, institution, resolved_account_id, db
    )
    rejected_inv, ready_inv = analyze_investment_transactions(
        parsed_data.investment_transactions, user_id, institution, resolved_account_id, db
    )

    # Run description cleanup across every preview item (both regular and investment,
    # both ready and rejected). Raw parser output stays untouched in parsed_data;
    # the cleaned display value lands on a sibling field so confirm can prefer it.
    all_items = rejected_txns + ready_txns + rejected_inv + ready_inv
    llm_summary = _apply_description_cleanup(db, user_id, all_items)

    total_rejected = len(rejected_txns) + len(rejected_inv)
    total_ready = len(ready_txns) + len(ready_inv)

    summary = {
        "total_parsed": total_rejected + total_ready,
        "rejected": total_rejected,
        "ready_to_import": total_ready,
        "can_confirm": True,
    }

    # Store in Redis
    session_id, expires_at = create_preview_session(
        r=r,
        user_id=user_id,
        institution=institution,
        account_id=resolved_account_id,
        filename=file.filename or "unknown",
        source_type=source_type,
        rejected={"transactions": rejected_txns, "investment_transactions": rejected_inv},
        ready_to_import={"transactions": ready_txns, "investment_transactions": ready_inv},
        summary=summary,
        account_info=account_info_dict,
        llm_summary=llm_summary,
    )

    return {
        "preview_session_id": session_id,
        "expires_at": expires_at,
        "summary": summary,
        "account_info": account_info_dict,
        "rejected": {"transactions": rejected_txns, "investment_transactions": rejected_inv},
        "ready_to_import": {"transactions": ready_txns, "investment_transactions": ready_inv},
        "llm_summary": llm_summary,
    }


def _apply_description_cleanup(db: Session, user_id: int, items: list[dict]) -> dict:
    """Run description cleanup for every preview item. Mutates each item in place,
    setting sibling fields: cleaned_description, llm_model, llm_processed_at, llm_status.

    Returns a session-level summary: source counts + a `degraded` flag so the
    frontend can show a banner when the LLM fell through.
    """
    raws = [(item.get("parsed_data") or {}).get("description") or "" for item in items]
    results = clean_descriptions(db, raws, user_id=user_id)

    source_counts = {"cache": 0, "regex_seed": 0, "llm": 0, "raw_fallthrough": 0}
    for item, result in zip(items, results):
        item["cleaned_description"] = result.cleaned
        item["llm_status"] = result.source
        item["llm_model"] = result.llm_model
        item["llm_processed_at"] = (
            result.llm_processed_at.isoformat() if result.llm_processed_at else None
        )
        source_counts[result.source] = source_counts.get(result.source, 0) + 1

    return {
        "source_counts": source_counts,
        "degraded": source_counts["raw_fallthrough"] > 0,
        "total": len(items),
    }


@router.get("/preview/sessions", response_model=List[PreviewSessionInfo])
async def list_preview_sessions(
    user_id: int = Depends(get_current_user_id),
    r: redis.Redis = Depends(get_redis_dependency),
):
    """List all active preview sessions for the current user."""
    return list_user_sessions(r, user_id)


@router.get("/preview/{session_id}")
async def get_preview(
    session_id: str,
    user_id: int = Depends(get_current_user_id),
    r: redis.Redis = Depends(get_redis_dependency),
):
    """Retrieve current preview state."""
    session = get_preview_session(r, session_id, user_id)
    if not session:
        raise HTTPException(404, "Preview session not found or expired")
    return {
        "preview_session_id": session_id,
        "expires_at": session["expires_at"],
        "summary": session["summary"],
        "account_info": session.get("account_info"),
        "rejected": session["rejected"],
        "ready_to_import": session["ready_to_import"],
        "llm_summary": session.get("llm_summary"),
    }


@router.post("/preview/{session_id}/reject-item")
async def reject_item(
    session_id: str,
    request: RejectItemRequest,
    user_id: int = Depends(get_current_user_id),
    r: redis.Redis = Depends(get_redis_dependency),
):
    """Move a ready-to-import item to rejected."""
    session = get_preview_session(r, session_id, user_id)
    if not session:
        raise HTTPException(404, "Preview session not found or expired")

    temp_id = request.temp_id
    found_item = None
    found_list_key = None
    found_index = None

    for list_key in ["transactions", "investment_transactions"]:
        for idx, item in enumerate(session["ready_to_import"][list_key]):
            if item["temp_id"] == temp_id:
                found_item = item
                found_list_key = list_key
                found_index = idx
                break
        if found_item:
            break

    if not found_item:
        raise HTTPException(404, f"Transaction {temp_id} not found in ready_to_import")

    session["ready_to_import"][found_list_key].pop(found_index)

    found_item["review_status"] = "rejected"
    session["rejected"][found_list_key].append(found_item)

    _recompute_summary(session)
    save_preview_session(r, session_id, session)

    return {
        "preview_session_id": session_id,
        "expires_at": session["expires_at"],
        "summary": session["summary"],
        "account_info": session.get("account_info"),
        "rejected": session["rejected"],
        "ready_to_import": session["ready_to_import"],
        "llm_summary": session.get("llm_summary"),
    }


@router.post("/preview/{session_id}/restore-item")
async def restore_item(
    session_id: str,
    request: RestoreItemRequest,
    user_id: int = Depends(get_current_user_id),
    r: redis.Redis = Depends(get_redis_dependency),
):
    """Move a rejected item back to ready-to-import, preserving is_duplicate."""
    session = get_preview_session(r, session_id, user_id)
    if not session:
        raise HTTPException(404, "Preview session not found or expired")

    temp_id = request.temp_id
    found_item = None
    found_list_key = None
    found_index = None

    for list_key in ["transactions", "investment_transactions"]:
        for idx, item in enumerate(session["rejected"][list_key]):
            if item["temp_id"] == temp_id:
                found_item = item
                found_list_key = list_key
                found_index = idx
                break
        if found_item:
            break

    if not found_item:
        raise HTTPException(404, f"Transaction {temp_id} not found in rejected")

    session["rejected"][found_list_key].pop(found_index)

    found_item.pop("review_status", None)
    session["ready_to_import"][found_list_key].append(found_item)

    _recompute_summary(session)
    save_preview_session(r, session_id, session)

    return {
        "preview_session_id": session_id,
        "expires_at": session["expires_at"],
        "summary": session["summary"],
        "account_info": session.get("account_info"),
        "rejected": session["rejected"],
        "ready_to_import": session["ready_to_import"],
        "llm_summary": session.get("llm_summary"),
    }


@router.post("/preview/{session_id}/bulk-reject-item")
async def bulk_reject_item(
    session_id: str,
    request: BulkRejectItemRequest,
    user_id: int = Depends(get_current_user_id),
    r: redis.Redis = Depends(get_redis_dependency),
):
    """Bulk move ready-to-import items to rejected."""
    session = get_preview_session(r, session_id, user_id)
    if not session:
        raise HTTPException(404, "Preview session not found or expired")

    lookup: Dict[str, tuple] = {}
    for list_key in ["transactions", "investment_transactions"]:
        for idx, item in enumerate(session["ready_to_import"][list_key]):
            lookup[item["temp_id"]] = (item, list_key, idx)

    processed = 0
    not_found = []
    to_remove: Dict[str, List[int]] = {"transactions": [], "investment_transactions": []}

    for temp_id in request.temp_ids:
        entry = lookup.get(temp_id)
        if not entry:
            not_found.append(temp_id)
            continue

        item, list_key, idx = entry
        to_remove[list_key].append(idx)

        item["review_status"] = "rejected"
        session["rejected"][list_key].append(item)
        processed += 1

    for list_key in to_remove:
        for idx in sorted(to_remove[list_key], reverse=True):
            session["ready_to_import"][list_key].pop(idx)

    _recompute_summary(session)
    save_preview_session(r, session_id, session)

    return {
        "preview_session_id": session_id,
        "expires_at": session["expires_at"],
        "summary": session["summary"],
        "account_info": session.get("account_info"),
        "rejected": session["rejected"],
        "ready_to_import": session["ready_to_import"],
        "llm_summary": session.get("llm_summary"),
        "processed": processed,
        "not_found": not_found,
    }


@router.post("/preview/{session_id}/bulk-restore-item")
async def bulk_restore_item(
    session_id: str,
    request: BulkRestoreItemRequest,
    user_id: int = Depends(get_current_user_id),
    r: redis.Redis = Depends(get_redis_dependency),
):
    """Bulk move rejected items back to ready-to-import."""
    session = get_preview_session(r, session_id, user_id)
    if not session:
        raise HTTPException(404, "Preview session not found or expired")

    lookup: Dict[str, tuple] = {}
    for list_key in ["transactions", "investment_transactions"]:
        for idx, item in enumerate(session["rejected"][list_key]):
            lookup[item["temp_id"]] = (item, list_key, idx)

    processed = 0
    not_found = []
    to_remove: Dict[str, List[int]] = {"transactions": [], "investment_transactions": []}

    for temp_id in request.temp_ids:
        entry = lookup.get(temp_id)
        if not entry:
            not_found.append(temp_id)
            continue

        item, list_key, idx = entry
        to_remove[list_key].append(idx)

        item.pop("review_status", None)
        session["ready_to_import"][list_key].append(item)
        processed += 1

    for list_key in to_remove:
        for idx in sorted(to_remove[list_key], reverse=True):
            session["rejected"][list_key].pop(idx)

    _recompute_summary(session)
    save_preview_session(r, session_id, session)

    return {
        "preview_session_id": session_id,
        "expires_at": session["expires_at"],
        "summary": session["summary"],
        "account_info": session.get("account_info"),
        "rejected": session["rejected"],
        "ready_to_import": session["ready_to_import"],
        "llm_summary": session.get("llm_summary"),
        "processed": processed,
        "not_found": not_found,
    }


@router.post("/preview/{session_id}/edit-transaction")
async def edit_transaction(
    session_id: str,
    request: EditTransactionRequest,
    user_id: int = Depends(get_current_user_id),
    r: redis.Redis = Depends(get_redis_dependency),
):
    """Edit a transaction in ready_to_import or rejected."""
    session = get_preview_session(r, session_id, user_id)
    if not session:
        raise HTTPException(404, "Preview session not found or expired")

    for section in ["ready_to_import", "rejected"]:
        for list_key in ["transactions", "investment_transactions"]:
            for item in session[section][list_key]:
                if item["temp_id"] == request.temp_id:
                    existing_edits = item.get("edited_data") or {}
                    existing_edits.update(request.edited_data)
                    item["edited_data"] = existing_edits
                    save_preview_session(r, session_id, session)
                    return {"success": True, "updated_transaction": item}

    raise HTTPException(404, f"Transaction {request.temp_id} not found")


@router.post("/preview/{session_id}/bulk-edit")
async def bulk_edit_transactions(
    session_id: str,
    request: BulkEditRequest,
    user_id: int = Depends(get_current_user_id),
    r: redis.Redis = Depends(get_redis_dependency),
):
    """Bulk edit multiple transactions in ready_to_import or rejected."""
    session = get_preview_session(r, session_id, user_id)
    if not session:
        raise HTTPException(404, "Preview session not found or expired")

    temp_id_set = set(request.temp_ids)
    updated_count = 0

    for section in ["ready_to_import", "rejected"]:
        for list_key in ["transactions", "investment_transactions"]:
            for item in session[section][list_key]:
                if item["temp_id"] in temp_id_set:
                    existing_edits = item.get("edited_data") or {}
                    existing_edits.update(request.edited_data)
                    item["edited_data"] = existing_edits
                    updated_count += 1

    if updated_count == 0:
        raise HTTPException(404, "No matching transactions found")

    save_preview_session(r, session_id, session)
    return {"success": True, "updated_count": updated_count}


@router.post("/statement/confirm", status_code=201)
async def confirm_statement_import(
    request: ConfirmImportRequest,
    user_id: int = Depends(get_current_user_id),
    db: Session = Depends(get_db),
    r: redis.Redis = Depends(get_redis_dependency),
):
    """
    Finalize import: create transactions in database from confirmed preview.

    Only transactions in ready_to_import will be created. Rejected items are skipped.
    """
    start_time = time.time()

    session = get_preview_session(r, request.preview_session_id, user_id)
    if not session:
        raise HTTPException(404, "Preview session not found or expired")

    institution = session["institution"]
    account_id = session["account_id"]
    source_type_enum = SourceType[session.get("source_type", "PDF")]

    # Resolve account for balance updates and metadata
    account = None
    if account_id:
        account = db.query(AccountDB).filter(
            AccountDB.id == account_id, AccountDB.user_id == user_id
        ).first()

    created_txns = []
    created_inv = []
    duplicates_imported = 0
    skipped_unmapped = 0

    # Pre-load category UUID→ID and tag UUID→ID maps for resolving edited_data
    category_uuid_map = {}
    tag_uuid_map = {}
    all_ready = session["ready_to_import"]["transactions"] + session["ready_to_import"]["investment_transactions"]
    cat_uuids = set()
    tag_uuids_needed = set()
    for item in all_ready:
        ed = item.get("edited_data") or {}
        if ed.get("category_uuid"):
            cat_uuids.add(ed["category_uuid"])
        if ed.get("subcategory_uuid"):
            cat_uuids.add(ed["subcategory_uuid"])
        for t in ed.get("tag_uuids", []):
            tag_uuids_needed.add(t)

    if cat_uuids:
        cat_uuid_objs = [UUID(u) for u in cat_uuids]
        cats = db.query(CategoryDB.uuid, CategoryDB.id).filter(CategoryDB.uuid.in_(cat_uuid_objs)).all()
        category_uuid_map = {str(c.uuid): c.id for c in cats}
    if tag_uuids_needed:
        tag_uuid_objs = [UUID(u) for u in tag_uuids_needed]
        tags = db.query(TagDB.id, TagDB.tag_id).filter(TagDB.id.in_(tag_uuid_objs)).all()
        tag_uuid_map = {str(t.id): t.tag_id for t in tags}

    # Collect tag associations to create after flush
    pending_tag_associations: list[tuple[TransactionDB, list[int]]] = []

    # Look up "Approved Duplicate" system tag for auto-tagging
    approved_dup_tag = get_system_tag(user_id, db, "Approved Duplicate")

    # Create upload job up front so each ParsedImportDB can link to it via relationship.
    # Counter fields are updated before commit once we know final totals.
    upload_job = UploadJobDB(
        user_id=user_id,
        file_path=None,
        institution=institution,
        account_id=account_id,
        skip_duplicates=False,
        status="COMPLETED",
        transactions_created=0,
        transactions_skipped=len(session["rejected"]["transactions"]),
        investment_transactions_created=0,
        investment_transactions_skipped=len(session["rejected"]["investment_transactions"]),
        started_at=datetime.utcnow(),
        completed_at=datetime.utcnow(),
    )
    db.add(upload_job)

    # --- Create regular transactions ---
    for item in session["ready_to_import"]["transactions"]:
        parsed_data = item["parsed_data"]
        final_data = {**parsed_data, **(item.get("edited_data") or {})}
        is_approved_dup = item.get("is_duplicate", False)
        display_description = _resolve_display_description(item, parsed_data)

        try:
            txn_type_enum = TransactionType[final_data["transaction_type"].upper()]
        except KeyError:
            logger.warning(f"Skipping transaction with unmapped type: {final_data['transaction_type']}")
            skipped_unmapped += 1
            continue

        effective_account_id = final_data.get("account_id") or account_id
        txn_date = date.fromisoformat(str(final_data["transaction_date"]))

        # Build hash from ORIGINAL parsed data so duplicate detection matches
        # on re-upload of the same statement, regardless of user edits
        parsed_type_value = TransactionType[parsed_data["transaction_type"].upper()].value
        txn_hash = generate_transaction_hash(
            user_id=user_id,
            institution_name=institution,
            transaction_date=parsed_data['transaction_date'],
            transaction_type_value=parsed_type_value,
            amount=parsed_data['amount'],
            description=parsed_data.get('description'),
            make_unique=is_approved_dup,
        )
        if is_approved_dup:
            duplicates_imported += 1

        # Resolve category/subcategory UUIDs to integer IDs
        category_id_val = final_data.get("category_id")
        subcategory_id_val = final_data.get("subcategory_id")
        if final_data.get("category_uuid"):
            category_id_val = category_uuid_map.get(final_data["category_uuid"], category_id_val)
        if final_data.get("subcategory_uuid"):
            subcategory_id_val = category_uuid_map.get(final_data["subcategory_uuid"], subcategory_id_val)

        db_txn = TransactionDB(
            id=uuid4(),
            user_id=user_id,
            account_id=effective_account_id,
            category_id=category_id_val,
            subcategory_id=subcategory_id_val,
            transaction_hash=txn_hash,
            source_type=source_type_enum,
            transaction_date=txn_date,
            amount=abs(Decimal(str(final_data["amount"]))),
            transaction_type=txn_type_enum,
            description=display_description,
            merchant_name=final_data.get("merchant_name"),
            comments=final_data.get("comments"),
        )
        db.add(db_txn)
        created_txns.append(db_txn)

        # Audit trail: frozen record of the raw parser output + preview-session edits
        db.add(ParsedImportDB(
            upload_job=upload_job,
            transaction_id=db_txn.id,
            raw_parsed_data=parsed_data,
            user_edits=item.get("edited_data"),
            llm_model=item.get("llm_model"),
            llm_processed_at=_parse_iso(item.get("llm_processed_at")),
        ))

        # Queue tag associations (need db_txn.db_id after flush)
        tag_ids = [tag_uuid_map[t] for t in final_data.get("tag_uuids", []) if t in tag_uuid_map]
        if is_approved_dup and approved_dup_tag:
            tag_ids.append(approved_dup_tag.tag_id)
        if tag_ids:
            pending_tag_associations.append((db_txn, tag_ids))

    # --- Create investment transactions ---
    for item in session["ready_to_import"]["investment_transactions"]:
        parsed_data = item["parsed_data"]
        final_data = {**parsed_data, **(item.get("edited_data") or {})}
        is_approved_dup = item.get("is_duplicate", False)
        inv_txn_date = date.fromisoformat(str(final_data["transaction_date"]))
        display_description = _resolve_display_description(item, parsed_data)

        # Build hash from ORIGINAL parsed data so duplicate detection matches
        parsed_inv_date = date.fromisoformat(str(parsed_data["transaction_date"]))
        parsed_inv = ParsedInvestmentTransaction(
            transaction_date=parsed_inv_date,
            transaction_type=parsed_data["transaction_type"],
            symbol=parsed_data.get("symbol"),
            api_symbol=parsed_data.get("api_symbol"),
            description=parsed_data.get("description", ""),
            quantity=Decimal(str(parsed_data["quantity"])) if parsed_data.get("quantity") else None,
            price_per_share=Decimal(str(parsed_data["price_per_share"])) if parsed_data.get("price_per_share") else None,
            total_amount=Decimal(str(parsed_data["total_amount"])),
        )

        inv_hash = generate_investment_transaction_hash(
            parsed_inv, user_id, institution, make_unique=is_approved_dup
        )
        if is_approved_dup:
            duplicates_imported += 1

        txn_type_enum = map_transaction_type_to_enum(final_data["transaction_type"])
        if not txn_type_enum:
            logger.warning(f"Skipping investment transaction with unmapped type: {final_data['transaction_type']}")
            skipped_unmapped += 1
            continue

        # Guard: strip share-based fields from non-share transaction types
        NON_SHARE_TYPES = {InvestmentTransactionType.INTEREST, InvestmentTransactionType.FEE,
                           InvestmentTransactionType.TRANSFER_IN, InvestmentTransactionType.TRANSFER_OUT}
        if txn_type_enum in NON_SHARE_TYPES:
            inv_symbol = None
            inv_quantity = None
            inv_price = None
            inv_api_symbol = None
        else:
            inv_symbol = final_data.get("symbol")
            inv_quantity = abs(Decimal(str(final_data["quantity"]))) if final_data.get("quantity") else None
            inv_price = Decimal(str(final_data["price_per_share"])) if final_data.get("price_per_share") else None
            inv_api_symbol = final_data.get("api_symbol")

        effective_account_id = final_data.get("account_id") or account_id

        db_inv = InvestmentTransactionDB(
            id=uuid4(),
            user_id=user_id,
            account_id=effective_account_id,
            holding_id=None,
            transaction_hash=inv_hash,
            transaction_type=txn_type_enum,
            symbol=inv_symbol,
            api_symbol=inv_api_symbol,
            quantity=inv_quantity,
            price_per_share=inv_price,
            total_amount=Decimal(str(final_data["total_amount"])),
            transaction_date=inv_txn_date,
            description=display_description,
            security_type=final_data.get("security_type"),
        )
        db.add(db_inv)
        created_inv.append(db_inv)

        # Audit trail: frozen record of the raw parser output + preview-session edits
        db.add(ParsedImportDB(
            upload_job=upload_job,
            investment_transaction_id=db_inv.id,
            raw_parsed_data=parsed_data,
            user_edits=item.get("edited_data"),
            llm_model=item.get("llm_model"),
            llm_processed_at=_parse_iso(item.get("llm_processed_at")),
        ))

    # Finalize upload job counters now that totals are known
    upload_job.transactions_created = len(created_txns)
    upload_job.investment_transactions_created = len(created_inv)

    # Flush to assign db_ids, then create tag associations
    if pending_tag_associations:
        db.flush()
        for db_txn, tag_ids in pending_tag_associations:
            for tag_id in tag_ids:
                db.add(TransactionTagDB(transaction_id=db_txn.db_id, tag_id=tag_id))

    # Commit all transactions
    try:
        db.commit()

        # Post-commit: update account balances for regular transactions
        # Skip for investment accounts — _update_investment_account_balance
        # handles the full balance via transaction replay.
        if account and account.account_type != AccountType.INVESTMENT:
            for t in created_txns:
                db.refresh(t)
                update_account_balance_from_transaction(db, account, t)

        # Post-commit: rebuild holdings from investment transactions
        if created_inv:
            # Collect unique account IDs from created investment transactions
            inv_account_ids = {inv_txn.account_id for inv_txn in created_inv if inv_txn.account_id}
            for inv_acct_id in inv_account_ids:
                rebuild_holdings_from_transactions(db, inv_acct_id)
                _update_investment_account_balance(db, inv_acct_id)
            db.commit()

        # Trigger backfill for any historical transactions (investment or regular)
        all_created = created_txns + created_inv
        if all_created and account_id:
            _trigger_backfill_if_needed(db, user_id, account_id, all_created)

    except Exception as e:
        db.rollback()
        logger.error(f"Failed to create transactions from preview: {e}", exc_info=True)
        raise HTTPException(500, f"Failed to create transactions: {str(e)}")

    # Clean up Redis session
    try:
        delete_preview_session(r, request.preview_session_id, user_id)
    except Exception as e:
        logger.warning(f"Failed to clean up preview session: {e}")

    elapsed_ms = int((time.time() - start_time) * 1000)

    return {
        "success": True,
        "transactions_created": len(created_txns),
        "investment_transactions_created": len(created_inv),
        "duplicates_imported": duplicates_imported,
        "skipped_unmapped_types": skipped_unmapped,
        "upload_job_id": upload_job.id,
        "processing_time_ms": elapsed_ms,
    }


def _trigger_backfill_if_needed(
    db: Session,
    user_id: int,
    account_id: int,
    created_transactions: list,
) -> None:
    """Trigger historical snapshot backfill if transactions are in the past.
    Delegates to the shared helper in services/account_snapshot.py."""
    from src.services.account_snapshot import trigger_backfill_if_needed

    earliest_date = min(t.transaction_date for t in created_transactions)
    trigger_backfill_if_needed(db, user_id, account_id, earliest_date)


@router.delete("/preview/{session_id}", status_code=204)
async def cancel_preview(
    session_id: str,
    user_id: int = Depends(get_current_user_id),
    r: redis.Redis = Depends(get_redis_dependency),
):
    """Cancel preview session and delete from Redis."""
    success = delete_preview_session(r, session_id, user_id)
    if not success:
        raise HTTPException(404, "Preview session not found")
    return None


@router.get("/preview/{session_id}/extend")
async def extend_preview(
    session_id: str,
    hours: int = Query(default=12, ge=1, le=48),
    user_id: int = Depends(get_current_user_id),
    r: redis.Redis = Depends(get_redis_dependency),
):
    """Extend preview session expiry."""
    new_expires = extend_session_expiry(r, session_id, user_id, hours * 3600)
    if not new_expires:
        raise HTTPException(404, "Preview session not found")
    return {"success": True, "expires_at": new_expires, "extended_by_hours": hours}
