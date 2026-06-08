"""Shared statement-import pipeline (parse → suggest → bulk-insert → tag).

Extracted from ``scripts/bulk_upload.py`` so it lives in tracked source and can
be driven by both the local dev scripts and the bulk-upload HTTP job (#59).

Unlike the preview/confirm flow (which lets the user review LLM suggestions
before they hit the DB), this path **auto-accepts** every suggestion — its whole
point is to skip the review step. Low-confidence/null rows still get the user's
"Needs Review" system tag so they surface in the inbox afterward.

The entry point is :func:`process_file`, which is source-agnostic (bytes +
filename, not a path). It commits per file and, on error, rolls back and records
the error on the returned :class:`FileImportResult` rather than raising — so a
caller importing many files can continue past one bad file.
"""
from __future__ import annotations

import io
from dataclasses import dataclass, field
from typing import Optional
from uuid import UUID

from sqlalchemy.orm import Session

from src.db.core import CategoryDB, TransactionTagDB
from src.services.importer import PARSER_MAPPING
from src.services.description_cleanup import process_preview_items, CleanedResult
from src.services.system_tags import ensure_system_tags, get_system_tag
from src.crud import crud_transaction, crud_investment
from src.logging_config import get_logger

logger = get_logger(__name__)


@dataclass
class FileImportResult:
    """Per-file outcome, suitable for surfacing as bulk-job progress."""
    filename: str
    transactions_created: int = 0
    transactions_skipped: int = 0
    investments_created: int = 0
    investments_skipped: int = 0
    suggestions_applied: int = 0
    needs_review: int = 0
    # True when at least one row fell through LLM enrichment (backend
    # unreachable) — surfaced per-file so the bulk UI can flag un-enriched
    # imports (#60), mirroring the single-file llm_summary.degraded signal.
    degraded: bool = False
    error: Optional[str] = None

    @property
    def ok(self) -> bool:
        return self.error is None


def _parsed_amount(txn):
    # ParsedTransaction has .amount; ParsedInvestmentTransaction has .total_amount.
    return txn.amount if hasattr(txn, "amount") else txn.total_amount


def _build_result_lookup(parsed_txns, results: list[CleanedResult]) -> dict:
    """Index CleanedResults by (date, amount, raw_description) so we can match
    them back to created TransactionDB rows after the bulk insert filters out
    duplicates and unmapped types."""
    lookup = {}
    for txn, result in zip(parsed_txns, results):
        key = (txn.transaction_date, _parsed_amount(txn), txn.description or "")
        lookup[key] = result
    return lookup


def _resolve_category_uuids(db: Session, suggestions: list[dict]) -> dict:
    """Map predefined category UUIDs (string) -> CategoryDB.db_id (int) for every
    UUID referenced by the batch's suggestions."""
    uuids = set()
    for s in suggestions:
        if s.get("suggested_category_uuid"):
            uuids.add(s["suggested_category_uuid"])
        if s.get("suggested_subcategory_uuid"):
            uuids.add(s["suggested_subcategory_uuid"])
    if not uuids:
        return {}
    rows = (
        db.query(CategoryDB.uuid, CategoryDB.db_id)
        .filter(CategoryDB.uuid.in_([UUID(u) for u in uuids]))
        .all()
    )
    return {str(r.uuid): r.db_id for r in rows}


def _apply_cleanup_to_created(db, user_id, created_rows, parsed_txns, results: list[CleanedResult],
                              category_uuid_to_id: dict, has_category_columns: bool):
    """Walk the freshly-created DB rows and (when applicable) apply
    merchant_name/category_id/subcategory_id from the matching CleanedResult.
    Description stays as the parser produced it — #35 removed the cleaned
    middle tier.

    For regular-transaction rows (``has_category_columns=True``), also queues
    the user's "Needs Review" system tag onto rows whose final state has a
    null ``category_id`` or null ``merchant_name`` (#34). Investment rows have
    no category/merchant columns and no tag join table, so the tag step is
    skipped for them.

    Returns (suggestions_applied_count, fallthrough_count, needs_review_count).
    """
    lookup = _build_result_lookup(parsed_txns, results)
    suggestions_applied = 0
    fallthroughs = 0
    needs_review_tag = (
        get_system_tag(user_id, db, "Needs Review") if has_category_columns else None
    )
    needs_review_rows: list = []

    for row in created_rows:
        # TransactionDB.amount is abs() of parser amount; match on raw fields
        # using the parsed_data reference instead.
        # (Investment row uses .total_amount, not .amount — fall through if no match.)
        for txn in parsed_txns:
            if (
                txn.transaction_date == row.transaction_date
                and (txn.description or "") == row.description
            ):
                key = (txn.transaction_date, _parsed_amount(txn), txn.description or "")
                result = lookup.get(key)
                if result is None:
                    continue

                if result.is_fallthrough:
                    fallthroughs += 1

                if has_category_columns:
                    # merchant_name comes from the post-extractor decision
                    # (regex > llm), available even when the LLM fell through.
                    row.merchant_name = result.merchant_name
                    if result.llm_suggestion:
                        sug = result.llm_suggestion
                        cat_uuid = sug.get("suggested_category_uuid")
                        sub_uuid = sug.get("suggested_subcategory_uuid")
                        if cat_uuid and cat_uuid in category_uuid_to_id:
                            row.category_id = category_uuid_to_id[cat_uuid]
                        if sub_uuid and sub_uuid in category_uuid_to_id:
                            row.subcategory_id = category_uuid_to_id[sub_uuid]
                        suggestions_applied += 1

                    if needs_review_tag and (
                        row.category_id is None or not row.merchant_name
                    ):
                        needs_review_rows.append(row)
                break

    needs_review_count = 0
    if needs_review_tag and needs_review_rows:
        # Need db_id for the join row; flush so SQLAlchemy assigns it.
        db.flush()
        for row in needs_review_rows:
            db.add(TransactionTagDB(
                transaction_id=row.db_id,
                tag_id=needs_review_tag.db_id,
            ))
        needs_review_count = len(needs_review_rows)

    return suggestions_applied, fallthroughs, needs_review_count


def process_file(
    db: Session,
    *,
    file_bytes: bytes,
    filename: str,
    institution: str,
    account_id: int,
    user_id: int,
    upload_job_id: Optional[int] = None,
) -> FileImportResult:
    """Parse one statement's bytes and import it with LLM cleaning + auto-accepted
    category/merchant suggestions. Commits on success; rolls back and records the
    error on the result on failure (never raises for per-file errors).

    When ``upload_job_id`` is given, every created row is linked back to that
    document so a document delete can cascade to its transactions (#59)."""
    result = FileImportResult(filename=filename)

    parser = PARSER_MAPPING.get(institution.lower())
    if not parser:
        result.error = f"No parser for institution '{institution}'"
        logger.warning("bulk_import: %s — %s", filename, result.error)
        return result

    # Ensure the user's system tags exist — the auto-tagging path below depends
    # on "Needs Review" being looked-uppable. ensure_system_tags is idempotent.
    ensure_system_tags(user_id, db)

    try:
        is_csv = filename.lower().endswith(".csv")
        parsed_data = parser.parse(io.BytesIO(file_bytes), is_csv=is_csv)

        if parsed_data.transactions:
            parsed_items = [
                {
                    "description": t.description,
                    "amount": float(t.amount),
                    "transaction_type": t.transaction_type,
                    "transaction_date": t.transaction_date.isoformat(),
                }
                for t in parsed_data.transactions
            ]
            results = process_preview_items(
                db, parsed_items, user_id=user_id, institution=institution,
            )
            suggestions = [r.llm_suggestion for r in results if r.llm_suggestion]
            uuid_to_id = _resolve_category_uuids(db, suggestions)

            created, skipped = crud_transaction.bulk_create_transactions_from_parsed_data(
                db=db, user_id=user_id, transactions=parsed_data.transactions,
                account_id=account_id,
            )
            if upload_job_id is not None:
                for row in created:
                    row.upload_job_id = upload_job_id
            applied, fallthroughs, needs_review = _apply_cleanup_to_created(
                db, user_id, created, parsed_data.transactions, results,
                uuid_to_id, has_category_columns=True,
            )
            db.commit()
            result.transactions_created = len(created)
            result.transactions_skipped = len(skipped)
            result.suggestions_applied += applied
            result.needs_review += needs_review
            result.degraded = result.degraded or fallthroughs > 0

        if parsed_data.investment_transactions:
            # #70: investment rows have no merchant/category columns, so the LLM
            # enrichment pass produces nothing storable — skip it and create the
            # rows directly. (degraded is meaningless for investment-only files;
            # mixed files still get it from the regular-transaction path above.)
            created_inv, skipped_inv, _backfill_id = crud_investment.bulk_create_investment_transactions_from_parsed_data(
                db=db, user_id=user_id, transactions=parsed_data.investment_transactions,
                account_id=account_id,
            )
            if upload_job_id is not None:
                for row in created_inv:
                    row.upload_job_id = upload_job_id
            db.commit()
            result.investments_created = len(created_inv)
            result.investments_skipped = len(skipped_inv)

        logger.info(
            "bulk_import: %s — txns +%d/-%d, inv +%d/-%d, needs_review %d",
            filename, result.transactions_created, result.transactions_skipped,
            result.investments_created, result.investments_skipped, result.needs_review,
        )
        return result

    except Exception as e:
        db.rollback()
        result.error = str(e)
        logger.error("bulk_import: %s failed: %s", filename, e, exc_info=True)
        return result
