"""
Per-row LLM processing for the upload preview flow.

For each parsed transaction, this service produces:
  - The raw description, preserved verbatim (no cleanup, no normalization).
  - A merchant name from a regex/alias-table extractor when the row's shape
    is unambiguous; LLM fallback otherwise. Nullable when neither produces a
    confident answer.
  - A (category, subcategory) UUID suggestion from the LLM, when the LLM is
    reachable.

The previous three-stage pipeline (DB cache + regex-seed + LLM) was scoped to
producing ``cleaned_description`` strings. With description cleanup removed
in favor of raw descriptions (#35), the cache table no longer has useful
semantics and is bypassed here. Future work can repurpose it to cache the
merchant + category suggestion by raw hash; for now the LLM is called for
every row that has any description at all.

If the LLM is unreachable, rows fall through with no suggestion and the
caller can show a degradation banner. The audit trail
(``parsed_imports.raw_parsed_data``) is never dependent on LLM availability.

See backend todos #29 (category + merchant) and #35 (raw descriptions +
regex-first merchant extraction).
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Optional

from sqlalchemy.orm import Session

from src.logging_config import get_logger
from src.services.llm_client import (
    LLMUnavailableError,
    TransactionBatchResult,
    get_llm_client,
)
from src.services.merchant_extractor import extract_merchant

logger = get_logger(__name__)


@dataclass
class CleanedResult:
    """Result of processing a single preview item.

    ``cleaned`` always equals ``raw`` — description normalization was removed
    in #35. The field is preserved for downstream-consumer compatibility; new
    code should prefer ``raw`` directly.

    ``merchant_name`` is the post-processed merchant: regex-extractor output
    when present, otherwise the LLM's merchant_name (already filtered for
    confidence in ``llm_client.process_transaction_batch``). May be None.

    ``llm_suggestion`` is the raw LLM batch result (merchant + category UUIDs
    + confidence). None when the LLM was unreachable or not called.
    """
    raw: str
    cleaned: str
    source: str  # 'llm' | 'raw_fallthrough' | 'empty'
    llm_model: Optional[str] = None
    llm_processed_at: Optional[datetime] = None
    llm_suggestion: Optional[dict] = None
    merchant_name: Optional[str] = None
    merchant_source: Optional[str] = None  # 'regex' | 'llm' | None

    @property
    def is_fallthrough(self) -> bool:
        return self.source == "raw_fallthrough"


def process_preview_items(
    db: Session,
    parsed_items: list[dict],
    user_id: int,
    institution: Optional[str] = None,
    batch_size: int = 20,
) -> list[CleanedResult]:
    """Process a list of preview items. Order is preserved.

    Each ``parsed_items`` entry is a dict with at minimum a ``description``
    key; ``amount``, ``transaction_type``, ``transaction_date`` are passed
    through to the LLM when present. ``institution`` selects which regex
    merchant patterns apply (case-insensitive — see
    ``merchant_extractor._INSTITUTION_HANDLERS``).

    The ``user_id`` and ``db`` parameters are retained for API stability with
    earlier call sites; the cache stages they used to support are no longer
    invoked, but downstream callers expect the same signature.
    """
    raws = [str(p.get("description") or "") for p in parsed_items]

    # Pre-compute regex merchant for every row (cheap, deterministic, no I/O).
    regex_merchants: list[Optional[str]] = [
        extract_merchant(institution, raw) for raw in raws
    ]

    results: list[Optional[CleanedResult]] = [None] * len(raws)
    llm_indices: list[int] = []

    for i, raw in enumerate(raws):
        if not raw or not raw.strip():
            results[i] = CleanedResult(
                raw=raw,
                cleaned=raw,
                source="empty",
                merchant_name=None,
                merchant_source=None,
            )
            continue
        llm_indices.append(i)

    if llm_indices:
        _run_llm_batch(
            parsed_items=parsed_items,
            raws=raws,
            regex_merchants=regex_merchants,
            indices=llm_indices,
            results=results,
            batch_size=batch_size,
        )

    return [r for r in results if r is not None]  # type: ignore[misc]


# ---------- internals ----------


def _run_llm_batch(
    parsed_items: list[dict],
    raws: list[str],
    regex_merchants: list[Optional[str]],
    indices: list[int],
    results: list[Optional[CleanedResult]],
    batch_size: int,
) -> None:
    """Send the LLM-eligible rows through ``process_transaction_batch`` and
    compose ``CleanedResult`` entries. On LLM failure, fills remaining slots
    with ``raw_fallthrough`` — the regex-extracted merchant (if any) is still
    surfaced; only the category suggestion is missing.

    Deduplicates by raw description: rows sharing the same raw share one LLM
    call. Per-row regex merchant + LLM suggestion are still applied
    individually.
    """
    # Deduplicate within the batch — preserve the first parsed_item per unique
    # raw, since the LLM only sees the description plus amount/type/date and
    # near-duplicates produce near-identical category guesses.
    unique_raws_in_order: list[str] = []
    seen: set[str] = set()
    raw_to_parsed: dict[str, dict] = {}
    for i in indices:
        raw = raws[i]
        if raw not in seen:
            seen.add(raw)
            unique_raws_in_order.append(raw)
            raw_to_parsed[raw] = parsed_items[i]

    client = get_llm_client()
    result_by_raw: dict[str, TransactionBatchResult] = {}
    llm_failed = False

    for start in range(0, len(unique_raws_in_order), batch_size):
        batch_raws = unique_raws_in_order[start:start + batch_size]
        batch_parsed = [raw_to_parsed[r] for r in batch_raws]
        try:
            batch_results = client.process_transaction_batch(batch_parsed)
        except LLMUnavailableError as e:
            # Fall through this batch and try the next — timeouts are usually isolated.
            logger.warning(
                f"LLM unavailable for batch of {len(batch_raws)} descriptions; "
                f"falling through raw for this batch. {e}"
            )
            llm_failed = True
            continue
        for r, res in zip(batch_raws, batch_results):
            result_by_raw[r] = res

    now = datetime.utcnow()
    model_name = client.model_name

    for i in indices:
        raw = raws[i]
        regex_merchant = regex_merchants[i]
        llm_result = result_by_raw.get(raw)

        if llm_result is not None:
            # Merchant precedence: regex extractor > LLM (already
            # confidence-floored in process_transaction_batch).
            if regex_merchant is not None:
                merchant = regex_merchant
                merchant_source: Optional[str] = "regex"
            elif llm_result["merchant_name"] is not None:
                merchant = llm_result["merchant_name"]
                merchant_source = "llm"
            else:
                merchant = None
                merchant_source = None

            results[i] = CleanedResult(
                raw=raw,
                cleaned=raw,
                source="llm",
                llm_model=model_name,
                llm_processed_at=now,
                llm_suggestion=_suggestion_from_result(llm_result),
                merchant_name=merchant,
                merchant_source=merchant_source,
            )
        else:
            # LLM didn't run for this row (failure mid-batch). Regex merchant
            # still applies; no category suggestion.
            results[i] = CleanedResult(
                raw=raw,
                cleaned=raw,
                source="raw_fallthrough",
                merchant_name=regex_merchant,
                merchant_source="regex" if regex_merchant else None,
            )

    if llm_failed:
        logger.info(
            "LLM stage completed with fall-through — "
            f"{sum(1 for r in results if r and r.source == 'raw_fallthrough')} rows "
            "served raw without category suggestion."
        )


def _suggestion_from_result(r: TransactionBatchResult) -> dict:
    """Project a TransactionBatchResult down to the llm_suggestion shape
    stored on preview rows + persisted to ParsedImportDB.llm_suggestions.

    Note: ``merchant_name`` here is the LLM's raw output (post confidence
    floor). The CleanedResult-level ``merchant_name`` field is the
    post-extractor decision (regex > llm) — that's the value confirm-time
    persistence should use.
    """
    return {
        "merchant_name": r["merchant_name"],
        "suggested_category_uuid": r["suggested_category_uuid"],
        "suggested_subcategory_uuid": r["suggested_subcategory_uuid"],
        "confidence": r["confidence"],
    }
