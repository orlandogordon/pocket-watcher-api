from fastapi import APIRouter, Depends, HTTPException, status, UploadFile, File, Form, Query, Response
from fastapi.responses import StreamingResponse
from sqlalchemy.orm import Session
import io
import time
from uuid import uuid4, UUID
from datetime import datetime, date
from decimal import Decimal
from typing import Optional, List, Dict
import redis

from src.db.core import (
    get_db, NotFoundError, UploadJobDB, BulkImportBatchDB, SkippedTransactionDB, TransactionDB,
    InvestmentTransactionDB,
    AccountDB, TransactionType, SourceType, InvestmentTransactionType, AccountType,
    CategoryDB, TagDB, TransactionTagDB, ParsedImportDB,
)
from src.auth.dependencies import get_current_user_id
from src.services.importer import PARSER_MAPPING
from src.services.demo_guard import enforce_demo_upload_allowlist
from src.services.file_storage import get_storage, build_key
from src.services.bulk_import_runner import submit_bulk_import
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
from src.services.description_cleanup import process_preview_items
from src.services.transfer_classifier import classify_parsed_transactions
from src.constants.categories import all_category_uuids
from src.services.system_tags import get_system_tag, append_review_note
from src.crud.crud_transaction import (
    generate_transaction_hash,
    update_account_balance_from_transaction,
    delete_db_transaction_by_uuid,
)
from src.crud.crud_investment import (
    generate_investment_transaction_hash,
    map_transaction_type_to_enum,
    rebuild_holdings_from_transactions,
    _update_investment_account_balance,
    delete_db_investment_transaction_by_uuid,
)
from src.crud.crud_account import get_db_account_by_last_four, read_db_account_by_uuid
from src.models.preview import (
    EditTransactionRequest,
    BulkEditRequest,
    RejectItemRequest,
    RestoreItemRequest,
    ConfirmImportRequest,
    BulkRejectItemRequest,
    BulkRestoreItemRequest,
    BulkImportRequest,
    PreviewSessionInfo,
)
from src.parser.models import ParsedInvestmentTransaction
from src.logging_config import get_logger
from src.utils.time import to_utc_iso, utcnow

logger = get_logger(__name__)

router = APIRouter(
    prefix="/uploads",
    tags=["uploads"],
)

# Per-file upload cap for the bulk-import path (#59). Match the reverse-proxy
# client_max_body_size in the C5 deploy.
MAX_UPLOAD_BYTES = 25 * 1024 * 1024  # 25 MB


# Union of all fields the user is allowed to edit in a preview session.
# Anything outside this set is rejected — in particular, server-internal
# int IDs (account_id, category_id, subcategory_id) must never be set by
# the client because they bypass the UUID-resolution path and the
# INVESTMENT-account write guard. See backend todo #36.
ALLOWED_EDITED_DATA_KEYS: frozenset[str] = frozenset({
    # Regular-transaction editable fields
    "description", "merchant_name", "category_uuid", "subcategory_uuid",
    "comments", "tag_uuids", "transaction_type", "amount",
    "transaction_date",
    # Investment-transaction editable fields
    "symbol", "quantity", "price_per_share", "api_symbol",
    "total_amount", "security_type",
})


def _validate_edited_data(edited: Dict) -> None:
    """Reject any edited_data key not in ALLOWED_EDITED_DATA_KEYS. The
    union allowlist deliberately includes both regular and investment
    fields — editing an investment-only field on a regular row is a
    harmless no-op at confirm time, but accepting internal int IDs
    (account_id, etc.) bypasses safety guards."""
    rejected = sorted(k for k in edited.keys() if k not in ALLOWED_EDITED_DATA_KEYS)
    if rejected:
        raise HTTPException(
            status_code=400,
            detail=f"Disallowed edited_data keys: {', '.join(rejected)}",
        )


# ---------------------------------------------------------------------------
# Bulk statement import (#59): per-file upload -> kick off a batch -> poll.
# ---------------------------------------------------------------------------

@router.post("/files", status_code=201)
async def upload_statement_file(
    file: UploadFile = File(...),
    account_uuid: str = Form(...),
    institution: str = Form(...),
    user_id: int = Depends(get_current_user_id),
    db: Session = Depends(get_db),
):
    """Upload ONE statement file for an owned account and archive it to local
    storage. Returns the document uuid to pass to POST /uploads/bulk. The
    frontend calls this once per selected file (a few concurrent), never one
    giant multipart request — see #59."""
    if institution.lower() not in PARSER_MAPPING:
        raise HTTPException(status_code=400, detail=f"Unknown institution '{institution}'")

    try:
        parsed_account_uuid = UUID(account_uuid)
    except (ValueError, AttributeError):
        raise HTTPException(status_code=400, detail="Invalid account_uuid format")

    account = read_db_account_by_uuid(db, parsed_account_uuid, user_id)
    if not account:
        raise HTTPException(status_code=404, detail="Account not found")

    contents = await file.read()
    if not contents:
        raise HTTPException(status_code=400, detail="Empty file")
    if len(contents) > MAX_UPLOAD_BYTES:
        raise HTTPException(status_code=413, detail="File exceeds the per-file size limit")
    enforce_demo_upload_allowlist(contents)

    document_uuid = uuid4()
    storage_key = build_key(user_id, document_uuid, file.filename or "")
    get_storage().save(contents, storage_key)

    job = UploadJobDB(
        uuid=document_uuid,
        user_id=user_id,
        account_id=account.db_id,
        institution=institution,
        file_path=file.filename,
        status="UPLOADED",
        storage_key=storage_key,
        file_size=len(contents),
        content_type=file.content_type,
    )
    db.add(job)
    db.commit()

    return {"document_uuid": str(document_uuid), "filename": file.filename, "size": len(contents)}


@router.post("/bulk", status_code=202)
def start_bulk_import(
    request: BulkImportRequest,
    user_id: int = Depends(get_current_user_id),
    db: Session = Depends(get_db),
):
    """Kick off a background import over already-uploaded documents (all owned,
    none already imported). Returns a batch uuid to poll."""
    jobs = db.query(UploadJobDB).filter(
        UploadJobDB.uuid.in_(request.document_uuids),
        UploadJobDB.user_id == user_id,
    ).all()
    found = {j.uuid for j in jobs}
    missing = [str(u) for u in request.document_uuids if u not in found]
    if missing:
        raise HTTPException(status_code=404, detail=f"Documents not found: {', '.join(missing)}")

    # A FAILED job carries a batch_id but imported nothing — it must stay
    # retryable. Only a job that didn't fail counts as "already imported" (#65).
    already = [str(j.uuid) for j in jobs if j.batch_id is not None and j.status != "FAILED"]
    if already:
        raise HTTPException(status_code=400, detail=f"Documents already imported: {', '.join(already)}")

    batch = BulkImportBatchDB(
        uuid=uuid4(),
        user_id=user_id,
        status="PENDING",
        total_files=len(jobs),
        processed_files=0,
    )
    db.add(batch)
    db.flush()
    for j in jobs:
        j.batch_id = batch.db_id
        j.status = "PENDING"
    db.commit()

    submit_bulk_import(batch.db_id)
    return {"batch_uuid": str(batch.uuid), "total_files": batch.total_files}


def _batch_progress(db: Session, batch: BulkImportBatchDB) -> Dict:
    children = (
        db.query(UploadJobDB)
        .filter(UploadJobDB.batch_id == batch.db_id)
        .order_by(UploadJobDB.db_id)
        .all()
    )
    per_file = [{
        "document_uuid": str(j.uuid),
        "filename": j.file_path,
        "status": j.status,
        "transactions_created": j.transactions_created,
        "transactions_skipped": j.transactions_skipped,
        "investment_transactions_created": j.investment_transactions_created,
        "investment_transactions_skipped": j.investment_transactions_skipped,
        "llm_degraded": j.llm_degraded,
        **_reconciliation_fields(j.reconciliation_warning, j.reconciliation_delta, j.reconciliation_detail),
        "error_message": j.error_message,
    } for j in children]
    current = next((j.file_path for j in children if j.status == "PROCESSING"), None)
    return {
        "batch_uuid": str(batch.uuid),
        "status": batch.status,
        "total": batch.total_files,
        "processed": batch.processed_files,
        "current_filename": current,
        "created": sum(j.transactions_created + j.investment_transactions_created for j in children),
        "skipped": sum(j.transactions_skipped + j.investment_transactions_skipped for j in children),
        "needs_review": sum(j.needs_review for j in children),
        # Batch-level AI signal: true if any file imported un-enriched (#60).
        # Canonical `llm_degraded`, uniform with the single-file/document responses.
        "llm_degraded": any(j.llm_degraded for j in children),
        # Batch-level reconciliation signal: true if any file didn't reconcile
        # (#78); the offending files carry their own delta/detail in per_file.
        "reconciliation_warning": any(j.reconciliation_warning for j in children),
        "per_file": per_file,
        "created_at": batch.created_at,
        "completed_at": batch.completed_at,
    }


@router.get("/bulk")
def list_bulk_imports(
    skip: int = Query(0, ge=0),
    limit: int = Query(50, ge=1, le=100),
    user_id: int = Depends(get_current_user_id),
    db: Session = Depends(get_db),
):
    batches = (
        db.query(BulkImportBatchDB)
        .filter(BulkImportBatchDB.user_id == user_id)
        .order_by(BulkImportBatchDB.created_at.desc())
        .offset(skip).limit(limit).all()
    )
    return {
        "batches": [{
            "batch_uuid": str(b.uuid), "status": b.status, "total": b.total_files,
            "processed": b.processed_files, "created_at": b.created_at,
            "completed_at": b.completed_at,
        } for b in batches],
        "skip": skip, "limit": limit,
    }


@router.get("/bulk/{batch_uuid}")
def get_bulk_import(
    batch_uuid: UUID,
    user_id: int = Depends(get_current_user_id),
    db: Session = Depends(get_db),
):
    batch = db.query(BulkImportBatchDB).filter(
        BulkImportBatchDB.uuid == batch_uuid,
        BulkImportBatchDB.user_id == user_id,
    ).first()
    if not batch:
        raise HTTPException(status_code=404, detail="Bulk import not found")
    return _batch_progress(db, batch)


@router.delete("/bulk/{batch_uuid}")
def cancel_bulk_import(
    batch_uuid: UUID,
    user_id: int = Depends(get_current_user_id),
    db: Session = Depends(get_db),
):
    batch = db.query(BulkImportBatchDB).filter(
        BulkImportBatchDB.uuid == batch_uuid,
        BulkImportBatchDB.user_id == user_id,
    ).first()
    if not batch:
        raise HTTPException(status_code=404, detail="Bulk import not found")
    if batch.status in ("PENDING", "IN_PROGRESS"):
        batch.status = "CANCELLED"
        # Cancel the not-yet-processed files too, so they don't linger as
        # "PENDING" forever in the upload history (#4). Already-finished files
        # keep their COMPLETED/FAILED status.
        db.query(UploadJobDB).filter(
            UploadJobDB.batch_id == batch.db_id,
            UploadJobDB.status.in_(["PENDING", "PROCESSING"]),
        ).update({"status": "CANCELLED"}, synchronize_session=False)
        db.commit()
    return {"batch_uuid": str(batch.uuid), "status": batch.status}


# ---------------------------------------------------------------------------
# Document browsing / viewing (#59): archived statements, per account, owner-only.
# ---------------------------------------------------------------------------

def _document_summary(job: UploadJobDB) -> Dict:
    return {
        "document_uuid": str(job.uuid),
        "filename": job.file_path,
        "institution": job.institution,
        "status": job.status,
        "llm_degraded": job.llm_degraded,
        **_reconciliation_fields(job.reconciliation_warning, job.reconciliation_delta, job.reconciliation_detail),
        "account_uuid": str(job.account.uuid) if job.account else None,
        "transactions_created": job.transactions_created,
        "transactions_skipped": job.transactions_skipped,
        "investment_transactions_created": job.investment_transactions_created,
        "investment_transactions_skipped": job.investment_transactions_skipped,
        "file_size": job.file_size,
        "content_type": job.content_type,
        "created_at": job.created_at,
    }


def _owned_document(db: Session, document_uuid: UUID, user_id: int) -> UploadJobDB:
    job = db.query(UploadJobDB).filter(
        UploadJobDB.uuid == document_uuid,
        UploadJobDB.user_id == user_id,
        UploadJobDB.storage_key.isnot(None),
    ).first()
    if not job:
        raise HTTPException(status_code=404, detail="Document not found")
    return job


@router.get("/documents")
def list_documents(
    account_uuid: Optional[UUID] = Query(None),
    user_id: int = Depends(get_current_user_id),
    db: Session = Depends(get_db),
):
    """List the user's archived statement documents, optionally for one owned
    account. Only files actually stored (storage_key set) are documents."""
    q = db.query(UploadJobDB).filter(
        UploadJobDB.user_id == user_id,
        UploadJobDB.storage_key.isnot(None),
    )
    if account_uuid:
        account = read_db_account_by_uuid(db, account_uuid, user_id)
        if not account:
            raise HTTPException(status_code=404, detail="Account not found")
        q = q.filter(UploadJobDB.account_id == account.db_id)
    docs = q.order_by(UploadJobDB.created_at.desc()).all()
    return {"documents": [_document_summary(j) for j in docs]}


@router.get("/documents/{document_uuid}")
def get_document(
    document_uuid: UUID,
    user_id: int = Depends(get_current_user_id),
    db: Session = Depends(get_db),
):
    return _document_summary(_owned_document(db, document_uuid, user_id))


@router.get("/documents/{document_uuid}/content")
def get_document_content(
    document_uuid: UUID,
    user_id: int = Depends(get_current_user_id),
    db: Session = Depends(get_db),
):
    """Stream the original uploaded file back (inline) for the owner only."""
    job = _owned_document(db, document_uuid, user_id)
    storage = get_storage()
    if not storage.exists(job.storage_key):
        raise HTTPException(status_code=404, detail="Document file missing from storage")
    filename = job.file_path or str(job.uuid)
    return StreamingResponse(
        storage.open(job.storage_key),
        media_type=job.content_type or "application/octet-stream",
        headers={"Content-Disposition": f'inline; filename="{filename}"'},
    )


@router.delete("/documents/{document_uuid}", status_code=204)
def delete_document(
    document_uuid: UUID,
    user_id: int = Depends(get_current_user_id),
    db: Session = Depends(get_db),
):
    """Delete a document: removes the stored file AND cascades to the
    transactions it imported (reusing the per-transaction delete path so account
    balances + snapshots stay correct). The upload-job record is removed too."""
    job = _owned_document(db, document_uuid, user_id)

    # Collect the UUIDs first — deletes mutate the session.
    txn_uuids = [
        t.uuid for t in db.query(TransactionDB.uuid).filter(
            TransactionDB.upload_job_id == job.db_id,
            TransactionDB.user_id == user_id,
        )
    ]
    inv_uuids = [
        t.uuid for t in db.query(InvestmentTransactionDB.uuid).filter(
            InvestmentTransactionDB.upload_job_id == job.db_id,
            InvestmentTransactionDB.user_id == user_id,
        )
    ]
    for tu in txn_uuids:
        try:
            delete_db_transaction_by_uuid(db, tu, user_id)
        except NotFoundError:
            pass  # already gone (e.g. removed via an OFFSETS-pair cascade)
    for iu in inv_uuids:
        try:
            delete_db_investment_transaction_by_uuid(db, iu, user_id)
        except NotFoundError:
            pass

    if job.storage_key:
        get_storage().delete(job.storage_key)
    db.delete(job)
    db.commit()
    return Response(status_code=204)


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


@router.get("/jobs/{job_uuid}")
def get_upload_job_status(
    job_uuid: UUID,
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
        UploadJobDB.uuid == job_uuid,
        UploadJobDB.user_id == user_id
    ).first()

    if not job:
        raise HTTPException(status_code=404, detail="Upload job not found")

    return {
        "id": str(job.uuid),
        "status": job.status,
        "institution": job.institution,
        "account_id": job.account_id,
        "skip_duplicates": job.skip_duplicates,
        "file_path": job.file_path,
        "transactions_created": job.transactions_created,
        "transactions_skipped": job.transactions_skipped,
        "investment_transactions_created": job.investment_transactions_created,
        "investment_transactions_skipped": job.investment_transactions_skipped,
        "llm_degraded": job.llm_degraded,
        **_reconciliation_fields(job.reconciliation_warning, job.reconciliation_delta, job.reconciliation_detail),
        "error_message": job.error_message,
        "created_at": job.created_at,
        "started_at": job.started_at,
        "completed_at": job.completed_at
    }


@router.get("/jobs/{job_uuid}/skipped")
def get_skipped_transactions(
    job_uuid: UUID,
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
        UploadJobDB.uuid == job_uuid,
        UploadJobDB.user_id == user_id
    ).first()

    if not job:
        raise HTTPException(status_code=404, detail="Upload job not found")

    # Get skipped transactions
    skipped = db.query(SkippedTransactionDB).filter(
        SkippedTransactionDB.upload_job_id == job.db_id
    ).offset(skip).limit(limit).all()

    # Build results with existing transaction details
    results = []
    for s in skipped:
        existing_txn = None
        if s.transaction_type == "REGULAR" and s.existing_transaction_id:
            existing_txn = db.query(TransactionDB).filter(
                TransactionDB.uuid == s.existing_transaction_id
            ).first()
        elif s.transaction_type == "INVESTMENT" and s.existing_investment_transaction_id:
            existing_txn = db.query(InvestmentTransactionDB).filter(
                InvestmentTransactionDB.uuid == s.existing_investment_transaction_id
            ).first()

        results.append({
            "id": s.db_id,
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
        "upload_job_id": str(job.uuid),
        "total_skipped": job.transactions_skipped + job.investment_transactions_skipped,
        "items": results,
        "skip": skip,
        "limit": limit
    }


# ===== PREVIEW FLOW ENDPOINTS =====


def _resolve_display_description(item: dict, parsed_data: dict) -> Optional[str]:
    """Choose the description to store on TransactionDB.

    Precedence: user edit > raw parser output. (Description cleanup was
    removed in #35 — there is no longer a cleaned middle tier.)
    """
    edited = item.get("edited_data") or {}
    return edited.get("description") or parsed_data.get("description")


def _parse_iso(value: Optional[str]) -> Optional[datetime]:
    """Parse ISO datetime as produced by item['llm_processed_at']. None-safe."""
    if not value:
        return None
    return datetime.fromisoformat(value)


def _to_raw_suggestion(suggestion: Optional[dict]) -> Optional[dict]:
    """Reconstruct the DB-shape ``llm_suggestions`` payload from the preview-row
    nested ``llm_suggestion`` (``category_uuid`` / ``subcategory_uuid`` keys).
    Returns None when the row carried no suggestion."""
    if not suggestion:
        return None
    return {
        "merchant_name": suggestion.get("merchant_name"),
        "suggested_category_uuid": suggestion.get("category_uuid"),
        "suggested_subcategory_uuid": suggestion.get("subcategory_uuid"),
        "confidence": suggestion.get("confidence"),
    }


# _append_review_note moved to src/services/system_tags.append_review_note (#68)
# so the bulk-import path shares the exact same wording. Imported below.


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


def _llm_degraded_flag(llm_summary: Optional[dict]) -> bool:
    """Canonical top-level AI-offline flag. Mirrors ``llm_summary.degraded`` so
    the frontend reads the same ``llm_degraded`` field across the single-file
    preview/confirm, bulk status, document, and job responses (#60)."""
    return bool((llm_summary or {}).get("degraded"))


def _reconciliation_fields(warning: bool, delta, detail: Optional[str]) -> dict:
    """Canonical reconciliation block for every job/preview response (#78).
    Mirrors ``llm_degraded``: a top-level ``reconciliation_warning`` bool plus a
    ``reconciliation`` object carrying the off-by amount/detail for the UI badge
    (null when there's no warning)."""
    return {
        "reconciliation_warning": bool(warning),
        "reconciliation": (
            {"delta": str(delta) if delta is not None else None, "detail": detail}
            if warning else None
        ),
    }


def _session_reconciliation_fields(session: Optional[dict]) -> dict:
    """Reconciliation block built from a preview session's stored ``reconciliation``
    dict ({delta, detail} or None), for the preview-session and confirm responses."""
    rec = (session or {}).get("reconciliation")
    if not rec:
        return _reconciliation_fields(False, None, None)
    return _reconciliation_fields(True, rec.get("delta"), rec.get("detail"))


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
        account_id = account_obj.db_id

    # Parse the file
    file_bytes = await file.read()
    enforce_demo_upload_allowlist(file_bytes)
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
                    resolved_account_id = found.db_id
                    account_info_dict["suggested_account_id"] = found.db_id
                    account_info_dict["suggested_account_name"] = found.account_name
            except Exception:
                logger.warning(f"Could not resolve account from last4: {parsed_data.account_info.account_number_last4}")

    # Account is required for duplicate detection (account_id is part of
    # the transaction hash — see backend todo #52). If neither the user
    # supplied account_uuid nor auto-detect-by-last4 resolved one, we can't
    # build hashes, so fail the preview with an actionable error.
    if not resolved_account_id:
        raise HTTPException(
            status_code=400,
            detail=(
                "Could not resolve an account for this statement. "
                "Pass account_uuid explicitly, or ensure the statement's "
                "account number matches an existing account."
            ),
        )

    # Tier A: reclassify checking/savings outflows that look like payments
    # to other user-owned accounts (CC, INVESTMENT, LOAN, OTHER) as
    # TRANSFER_OUT before dedup runs. Returns suggestions keyed by parsed
    # position; we inject those onto the preview items below.
    source_account = db.query(AccountDB).filter(
        AccountDB.db_id == resolved_account_id,
        AccountDB.user_id == user_id,
    ).first()
    user_accounts = db.query(AccountDB).filter(AccountDB.user_id == user_id).all()
    tier_a_suggestions = classify_parsed_transactions(
        parsed_data.transactions, source_account, user_accounts
    )

    # Analyze duplicates
    rejected_txns, ready_txns = analyze_regular_transactions(
        parsed_data.transactions, user_id, resolved_account_id, db
    )

    if tier_a_suggestions:
        partner_lookup = {a.db_id: a for a in user_accounts}
        for item in rejected_txns + ready_txns:
            pos = item.get("statement_position")
            sug = tier_a_suggestions.get(pos) if pos is not None else None
            if sug is None or sug.suggested_partner_account_id is None:
                continue
            partner = partner_lookup.get(sug.suggested_partner_account_id)
            item["tier_a_suggestion"] = {
                "proposed_transaction_type": sug.transaction_type.value,
                "suggested_partner_account_uuid": str(partner.uuid) if partner else None,
                "suggested_partner_account_name": partner.account_name if partner else None,
                "matched_token": sug.matched_token,
            }
    rejected_inv, ready_inv = analyze_investment_transactions(
        parsed_data.investment_transactions, user_id, resolved_account_id, db
    )

    # Run LLM processing across the regular preview items (both ready and
    # rejected). Raw parser output stays untouched in parsed_data; the merchant +
    # category suggestion land on sibling fields so confirm can prefer them. Per
    # #35: description is preserved raw — no cleaned tier.
    # #70: investment rows have no merchant/category columns to enrich, so they're
    # excluded — they carry no llm_suggestion, which confirm already tolerates.
    llm_summary = _apply_llm_processing(db, user_id, institution, rejected_txns + ready_txns)

    total_rejected = len(rejected_txns) + len(rejected_inv)
    total_ready = len(ready_txns) + len(ready_inv)

    summary = {
        "total_parsed": total_rejected + total_ready,
        "rejected": total_rejected,
        "ready_to_import": total_ready,
        "can_confirm": True,
    }

    # Archive the original file so the eventual confirm produces a viewable
    # document (#59). Keyed by a fresh uuid; the confirm step copies the key onto
    # the UploadJobDB. Abandoned previews are cleaned on cancel (TTL-orphan sweep
    # is a follow-up).
    storage_key = build_key(user_id, uuid4(), file.filename or "")
    get_storage().save(file_bytes, storage_key)

    # Statement reconciliation (#78): a numeric mismatch is a non-blocking
    # warning carried through to confirm (and the UI badge). None when the parser
    # had no control totals to check (e.g. CSVs).
    rec = parsed_data.reconciliation
    reconciliation_dict = None
    if rec is not None and not rec.reconciled:
        reconciliation_dict = {"delta": str(rec.delta), "detail": rec.detail}

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
        reconciliation=reconciliation_dict,
        storage_key=storage_key,
        file_size=len(file_bytes),
        content_type=file.content_type,
    )

    return {
        "preview_session_id": session_id,
        "expires_at": expires_at,
        "summary": summary,
        "account_info": account_info_dict,
        "rejected": {"transactions": rejected_txns, "investment_transactions": rejected_inv},
        "ready_to_import": {"transactions": ready_txns, "investment_transactions": ready_inv},
        "llm_summary": llm_summary,
        "llm_degraded": _llm_degraded_flag(llm_summary),
        **_reconciliation_fields(
            reconciliation_dict is not None,
            rec.delta if reconciliation_dict else None,
            reconciliation_dict["detail"] if reconciliation_dict else None,
        ),
    }


def _apply_llm_processing(
    db: Session,
    user_id: int,
    institution: str,
    items: list[dict],
) -> dict:
    """Run LLM processing for every preview item. Mutates each item in place,
    setting sibling fields: llm_model, llm_processed_at, llm_status, and
    llm_suggestion (nested object with merchant + category UUIDs; present
    only for rows the LLM actually processed).

    The merchant on ``llm_suggestion`` is the post-extractor decision
    (regex > llm). The raw LLM output is preserved separately on
    ``llm_suggestion_raw`` for the audit trail.

    Returns a session-level summary: source counts + a `degraded` flag so the
    frontend can show a banner when the LLM fell through, plus regex-coverage
    counts for operational signal on parser format drift.
    """
    parsed_list = [item.get("parsed_data") or {} for item in items]
    results = process_preview_items(
        db, parsed_list, user_id=user_id, institution=institution,
    )

    source_counts = {"empty": 0, "llm": 0, "raw_fallthrough": 0}
    merchant_source_counts = {"regex": 0, "llm": 0, "null": 0}
    suggestions_made = 0
    for item, result in zip(items, results):
        item["llm_status"] = result.source
        item["llm_model"] = result.llm_model
        item["llm_processed_at"] = (
            to_utc_iso(result.llm_processed_at) if result.llm_processed_at else None
        )
        if result.llm_suggestion is not None:
            # Surface the suggestion to the preview UI under the #29-spec shape.
            # The merchant we put here is the post-extractor decision (regex
            # > llm), not the LLM's raw output — that's what the user reviews
            # and what confirm should persist by default.
            raw_sug = result.llm_suggestion
            item["llm_suggestion"] = {
                "merchant_name": result.merchant_name,
                "category_uuid": raw_sug["suggested_category_uuid"],
                "subcategory_uuid": raw_sug["suggested_subcategory_uuid"],
                "confidence": raw_sug["confidence"],
            }
            # Preserve the DB-shape copy of the LLM's raw output for the
            # audit trail. merchant_source records who picked the merchant.
            item["llm_suggestion_raw"] = raw_sug
            item["merchant_source"] = result.merchant_source
            suggestions_made += 1
        else:
            item["llm_suggestion"] = None
            item["merchant_source"] = result.merchant_source

        source_counts[result.source] = source_counts.get(result.source, 0) + 1
        merchant_source_counts[result.merchant_source or "null"] = (
            merchant_source_counts.get(result.merchant_source or "null", 0) + 1
        )

    logger.info(
        "merchant_extractor_coverage "
        f"institution={institution} regex={merchant_source_counts['regex']} "
        f"llm={merchant_source_counts['llm']} null={merchant_source_counts['null']} "
        f"total={len(items)}"
    )

    return {
        "source_counts": source_counts,
        "merchant_source_counts": merchant_source_counts,
        "degraded": source_counts["raw_fallthrough"] > 0,
        "suggestions_made": suggestions_made,
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
        "llm_degraded": _llm_degraded_flag(session.get("llm_summary")),
        **_session_reconciliation_fields(session),
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
        "llm_degraded": _llm_degraded_flag(session.get("llm_summary")),
        **_session_reconciliation_fields(session),
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
        "llm_degraded": _llm_degraded_flag(session.get("llm_summary")),
        **_session_reconciliation_fields(session),
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
        "llm_degraded": _llm_degraded_flag(session.get("llm_summary")),
        **_session_reconciliation_fields(session),
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
        "llm_degraded": _llm_degraded_flag(session.get("llm_summary")),
        **_session_reconciliation_fields(session),
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
    _validate_edited_data(request.edited_data)
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
    _validate_edited_data(request.edited_data)
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
            AccountDB.db_id == account_id, AccountDB.user_id == user_id
        ).first()

    # Guard: regular TransactionDB rows must not land on an INVESTMENT
    # account (mirrors the four /transactions/* router guards). The
    # investment_transactions queue is handled separately and is
    # legitimate on an INVESTMENT account.
    if (
        account
        and account.account_type == AccountType.INVESTMENT
        and session["ready_to_import"]["transactions"]
    ):
        raise HTTPException(
            status_code=400,
            detail=(
                "Regular transactions are not allowed on investment accounts. "
                "Use POST /investment-transactions/ instead."
            ),
        )

    created_txns = []
    created_inv = []
    duplicates_imported = 0
    skipped_unmapped = 0

    # Pre-load category UUID→ID and tag UUID→ID maps. Collect UUIDs from both
    # user edits (overrides) and LLM suggestions (accepted pre-fills).
    category_uuid_map = {}
    tag_uuid_map = {}
    all_ready = session["ready_to_import"]["transactions"] + session["ready_to_import"]["investment_transactions"]
    cat_uuids = set()
    tag_uuids_needed = set()
    predefined_uuid_set = set(all_category_uuids())
    for item in all_ready:
        ed = item.get("edited_data") or {}
        if ed.get("category_uuid"):
            cat_uuids.add(ed["category_uuid"])
        if ed.get("subcategory_uuid"):
            cat_uuids.add(ed["subcategory_uuid"])
        for t in ed.get("tag_uuids", []):
            tag_uuids_needed.add(t)
        sug = item.get("llm_suggestion") or {}
        if sug.get("category_uuid"):
            cat_uuids.add(sug["category_uuid"])
        if sug.get("subcategory_uuid"):
            cat_uuids.add(sug["subcategory_uuid"])

    if cat_uuids:
        cat_uuid_objs = [UUID(u) for u in cat_uuids]
        cats = db.query(CategoryDB.uuid, CategoryDB.db_id).filter(CategoryDB.uuid.in_(cat_uuid_objs)).all()
        category_uuid_map = {str(c.uuid): c.db_id for c in cats}
    if tag_uuids_needed:
        tag_uuid_objs = [UUID(u) for u in tag_uuids_needed]
        tags = db.query(TagDB.uuid, TagDB.db_id).filter(TagDB.uuid.in_(tag_uuid_objs)).all()
        tag_uuid_map = {str(t.uuid): t.db_id for t in tags}

    # Collect tag associations to create after flush
    pending_tag_associations: list[tuple[TransactionDB, list[int]]] = []

    # Look up system tags used for auto-tagging at confirm time.
    # "Needs Review" attaches to any regular transaction whose final state
    # has a null category_id or null merchant_name — surfaces ambiguous rows
    # in the user's review queue post-import. See backend todo #34.
    approved_dup_tag = get_system_tag(user_id, db, "Approved Duplicate")
    needs_review_tag = get_system_tag(user_id, db, "Needs Review")

    # Create upload job up front so each ParsedImportDB can link to it via relationship.
    # Counter fields are updated before commit once we know final totals.
    upload_job = UploadJobDB(
        uuid=uuid4(),
        user_id=user_id,
        file_path=session.get("filename"),
        institution=institution,
        account_id=account_id,
        skip_duplicates=False,
        status="COMPLETED",
        # AI-offline signal for the document, mirroring the bulk path (#60).
        llm_degraded=bool((session.get("llm_summary") or {}).get("degraded")),
        # Statement-reconciliation warning, mirroring the bulk path (#78).
        reconciliation_warning=bool(session.get("reconciliation")),
        reconciliation_delta=(
            Decimal(session["reconciliation"]["delta"]) if session.get("reconciliation") else None
        ),
        reconciliation_detail=(
            session["reconciliation"]["detail"] if session.get("reconciliation") else None
        ),
        # Archived original file (#59) — makes this import a viewable document.
        storage_key=session.get("storage_key"),
        file_size=session.get("file_size"),
        content_type=session.get("content_type"),
        transactions_created=0,
        transactions_skipped=len(session["rejected"]["transactions"]),
        investment_transactions_created=0,
        investment_transactions_skipped=len(session["rejected"]["investment_transactions"]),
        started_at=utcnow(),
        completed_at=utcnow(),
    )
    db.add(upload_job)
    db.flush()  # assign upload_job.db_id so created rows can link to this document

    # --- Create regular transactions ---
    suggestion_accept_count = 0
    suggestion_override_count = 0
    needs_review_count = 0
    for item in session["ready_to_import"]["transactions"]:
        parsed_data = item["parsed_data"]
        edited = item.get("edited_data") or {}
        final_data = {**parsed_data, **edited}
        is_approved_dup = item.get("is_duplicate", False)
        display_description = _resolve_display_description(item, parsed_data)
        suggestion = item.get("llm_suggestion") or None

        try:
            txn_type_enum = TransactionType[final_data["transaction_type"].upper()]
        except KeyError:
            logger.warning(f"Skipping transaction with unmapped type: {final_data['transaction_type']}")
            skipped_unmapped += 1
            continue

        txn_date = date.fromisoformat(str(final_data["transaction_date"]))

        # Build hash from ORIGINAL parsed data so duplicate detection matches
        # on re-upload of the same statement, regardless of user edits
        parsed_type_value = TransactionType[parsed_data["transaction_type"].upper()].value
        txn_hash = generate_transaction_hash(
            user_id=user_id,
            account_id=account_id,
            transaction_date=parsed_data['transaction_date'],
            transaction_type_value=parsed_type_value,
            amount=parsed_data['amount'],
            description=parsed_data.get('description'),
            make_unique=is_approved_dup,
        )
        if is_approved_dup:
            duplicates_imported += 1

        # Resolve category/subcategory UUIDs to integer IDs.
        # Precedence: user edit > LLM suggestion > parser-supplied value.
        # Defensive check: user's override UUIDs must be in the predefined set.
        user_cat_uuid = edited.get("category_uuid")
        user_sub_uuid = edited.get("subcategory_uuid")
        if user_cat_uuid and user_cat_uuid not in predefined_uuid_set:
            raise HTTPException(
                400, f"Unknown category_uuid in user edit: {user_cat_uuid}"
            )
        if user_sub_uuid and user_sub_uuid not in predefined_uuid_set:
            raise HTTPException(
                400, f"Unknown subcategory_uuid in user edit: {user_sub_uuid}"
            )

        sug_cat_uuid = suggestion.get("category_uuid") if suggestion else None
        sug_sub_uuid = suggestion.get("subcategory_uuid") if suggestion else None

        chosen_cat_uuid = user_cat_uuid or sug_cat_uuid
        chosen_sub_uuid = user_sub_uuid or sug_sub_uuid

        category_id_val = final_data.get("category_id")
        subcategory_id_val = final_data.get("subcategory_id")
        if chosen_cat_uuid:
            category_id_val = category_uuid_map.get(chosen_cat_uuid, category_id_val)
        if chosen_sub_uuid:
            subcategory_id_val = category_uuid_map.get(chosen_sub_uuid, subcategory_id_val)

        # Merchant name: user edit > suggestion > raw parser value (usually None).
        merchant_name_val = (
            edited.get("merchant_name")
            or (suggestion.get("merchant_name") if suggestion else None)
            or final_data.get("merchant_name")
        )

        # Accept/override telemetry — only meaningful when a suggestion was made.
        if suggestion:
            was_override = bool(
                user_sub_uuid and user_sub_uuid != sug_sub_uuid
            ) or bool(
                user_cat_uuid and user_cat_uuid != sug_cat_uuid
            )
            if was_override:
                suggestion_override_count += 1
            else:
                suggestion_accept_count += 1
            logger.info(
                "llm_suggestion_decision "
                f"user={user_id} suggestion_cat={sug_cat_uuid} suggestion_sub={sug_sub_uuid} "
                f"chosen_cat={chosen_cat_uuid} chosen_sub={chosen_sub_uuid} "
                f"override={was_override}"
            )

        db_txn = TransactionDB(
            uuid=uuid4(),
            user_id=user_id,
            account_id=account_id,
            category_id=category_id_val,
            subcategory_id=subcategory_id_val,
            transaction_hash=txn_hash,
            source_type=source_type_enum,
            transaction_date=txn_date,
            amount=abs(Decimal(str(final_data["amount"]))),
            transaction_type=txn_type_enum,
            description=display_description,
            merchant_name=merchant_name_val,
            comments=final_data.get("comments"),
            upload_job_id=upload_job.db_id,
        )
        db.add(db_txn)
        created_txns.append(db_txn)

        # Audit trail: frozen record of the raw parser output + preview-session
        # edits + the raw LLM suggestion (pre-override). Downstream analysis
        # diffs user_edits against llm_suggestions to measure accept/override.
        db.add(ParsedImportDB(
            upload_job=upload_job,
            transaction_id=db_txn.uuid,
            raw_parsed_data=parsed_data,
            user_edits=item.get("edited_data"),
            llm_suggestions=item.get("llm_suggestion_raw") or _to_raw_suggestion(suggestion),
            llm_model=item.get("llm_model"),
            llm_processed_at=_parse_iso(item.get("llm_processed_at")),
        ))

        # Queue tag associations (need db_txn.db_id after flush)
        tag_ids = [tag_uuid_map[t] for t in final_data.get("tag_uuids", []) if t in tag_uuid_map]
        if is_approved_dup and approved_dup_tag:
            tag_ids.append(approved_dup_tag.db_id)
        # Tag rows whose final state has no category or no merchant — covers
        # both the preview path (user left it blank) and the bulk path (LLM
        # nulls survive untouched). #34 — Option A: derived from final state.
        # Transfers are exempt: TRANSFER_IN/OUT intentionally have no category
        # (they're balance-neutral movements, not income/expense), so the
        # category-null heuristic would mis-flag every transfer.
        is_transfer = txn_type_enum in (TransactionType.TRANSFER_IN, TransactionType.TRANSFER_OUT)
        missing_category = category_id_val is None
        missing_merchant = not merchant_name_val
        if needs_review_tag and not is_transfer and (missing_category or missing_merchant):
            tag_ids.append(needs_review_tag.db_id)
            needs_review_count += 1
            # Record WHY on the transaction itself so the review inbox (#46)
            # shows what triggered the flag without opening each row.
            db_txn.comments = append_review_note(
                db_txn.comments,
                missing_category=missing_category,
                missing_merchant=missing_merchant,
            )
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
            parsed_inv, user_id, account_id, make_unique=is_approved_dup
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

        db_inv = InvestmentTransactionDB(
            uuid=uuid4(),
            user_id=user_id,
            account_id=account_id,
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
            upload_job_id=upload_job.db_id,
        )
        db.add(db_inv)
        created_inv.append(db_inv)

        # Audit trail: frozen record of the raw parser output + preview-session edits.
        # Investment rows also carry an LLM suggestion when one was made; persist
        # it alongside the regular-transaction audit shape.
        inv_suggestion = item.get("llm_suggestion") or None
        db.add(ParsedImportDB(
            upload_job=upload_job,
            investment_transaction_id=db_inv.uuid,
            raw_parsed_data=parsed_data,
            user_edits=item.get("edited_data"),
            llm_suggestions=item.get("llm_suggestion_raw") or _to_raw_suggestion(inv_suggestion),
            llm_model=item.get("llm_model"),
            llm_processed_at=_parse_iso(item.get("llm_processed_at")),
        ))

    # Finalize upload job counters now that totals are known
    upload_job.transactions_created = len(created_txns)
    upload_job.investment_transactions_created = len(created_inv)
    upload_job.needs_review = needs_review_count

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

    total_decisions = suggestion_accept_count + suggestion_override_count
    if total_decisions:
        logger.info(
            f"llm_suggestion_summary user={user_id} accepted={suggestion_accept_count} "
            f"overridden={suggestion_override_count} "
            f"accept_rate={suggestion_accept_count / total_decisions:.2%}"
        )

    return {
        "success": True,
        "transactions_created": len(created_txns),
        "investment_transactions_created": len(created_inv),
        "duplicates_imported": duplicates_imported,
        "skipped_unmapped_types": skipped_unmapped,
        "upload_job_id": str(upload_job.uuid),
        "processing_time_ms": elapsed_ms,
        "suggestion_accepted": suggestion_accept_count,
        "suggestion_overridden": suggestion_override_count,
        "llm_degraded": _llm_degraded_flag(session.get("llm_summary")),
        **_session_reconciliation_fields(session),
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
    """Cancel preview session, delete its archived file, and remove from Redis."""
    session = get_preview_session(r, session_id, user_id)
    if not session:
        raise HTTPException(404, "Preview session not found")
    storage_key = session.get("storage_key")
    if storage_key:
        get_storage().delete(storage_key)
    delete_preview_session(r, session_id, user_id)
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
