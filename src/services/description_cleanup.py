"""
Transaction LLM processing pipeline.

Per preview item, produces a cleaned description AND (when the LLM fires) a
category + merchant suggestion in one round-trip.

Three-stage lookup per raw description (for the cleaned-description side):
    1. DB cache hit (hashed raw -> cached cleaned value)
    2. Regex seed match (common merchant patterns; populates cache)
    3. LLM fallback (batched via ``process_transaction_batch``; populates cache)

Category / merchant suggestions ride along with the LLM tier only — cache and
regex tiers do not produce suggestions, because the suggestion depends on the
full parsed row (amount, type, date) and not just the raw description. Rows
served from cache/regex get ``llm_suggestion = None`` and the frontend shows
no pre-filled category for them.

If the LLM is unreachable, misses fall through to the raw description unchanged
and are flagged so the caller can show a degradation banner. The audit trail
(``parsed_imports.raw_parsed_data``) is never dependent on LLM availability.

See backend todos #27 (description cleanup) and #29 (category + merchant).
"""

from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional

from sqlalchemy.orm import Session

from src.db.core import DescriptionCacheDB
from src.logging_config import get_logger
from src.services.llm_client import (
    LLMUnavailableError,
    TransactionBatchResult,
    get_llm_client,
)

logger = get_logger(__name__)


# ---------- regex seed patterns ----------
#
# Order matters — first match wins. Keep patterns tight; a false cache write
# propagates to every future match of the same raw text. When in doubt, omit
# the pattern and let the LLM handle it.
#
# Regex seeds are reserved for raw patterns where the merchant is the ENTIRE
# payload (no vendor suffix attached). Anything that can carry a second-level
# vendor — delivery marketplaces (DoorDash/Uber Eats/Grubhub), payment rails
# (PAYPAL */SQ */TST*/GOOGLE *) — is intentionally left to the LLM so it can
# apply the "keep aggregator + vendor" vs "strip processor, keep vendor" rules
# from the system prompt.

_SEED_PATTERNS: list[tuple[re.Pattern, str]] = [
    (re.compile(r"\bAMZN\s*MKTP\b", re.IGNORECASE), "Amazon"),
    (re.compile(r"\bAMAZON\.COM\b", re.IGNORECASE), "Amazon"),
    (re.compile(r"\bAMAZON\s+PRIME\b", re.IGNORECASE), "Amazon Prime"),
    (re.compile(r"\bSTARBUCKS\b", re.IGNORECASE), "Starbucks"),
    (re.compile(r"\bUBER\s+TRIP\b", re.IGNORECASE), "Uber Trip"),
    (re.compile(r"\bLYFT\b", re.IGNORECASE), "Lyft"),
    (re.compile(r"\bNETFLIX\b", re.IGNORECASE), "Netflix"),
    (re.compile(r"\bSPOTIFY\b", re.IGNORECASE), "Spotify"),
    (re.compile(r"\bAPPLE\.COM/BILL\b", re.IGNORECASE), "Apple"),
    (re.compile(r"\bVENMO\b", re.IGNORECASE), "Venmo"),
    (re.compile(r"\bTRADER\s*JOE'?S\b", re.IGNORECASE), "Trader Joe's"),
    (re.compile(r"\bWHOLE\s*FOODS\b", re.IGNORECASE), "Whole Foods"),
    (re.compile(r"\bCVS\b(?!\w)", re.IGNORECASE), "CVS"),
    (re.compile(r"\bWALGREENS\b", re.IGNORECASE), "Walgreens"),
    (re.compile(r"\bTARGET\b(?!\s+BANK)", re.IGNORECASE), "Target"),
    (re.compile(r"\bCOSTCO\s+WHSE\b", re.IGNORECASE), "Costco"),
]


# ---------- public API ----------


@dataclass
class CleanedResult:
    """Result of processing a single preview item.

    ``llm_suggestion`` is populated only when the LLM tier fires (source == 'llm').
    Cache/regex/fallthrough rows leave it as None — the frontend shows no
    category pre-fill for those items (by design, per #29)."""
    raw: str
    cleaned: str
    source: str  # 'cache' | 'regex_seed' | 'llm' | 'raw_fallthrough'
    llm_model: Optional[str] = None
    llm_processed_at: Optional[datetime] = None
    llm_suggestion: Optional[dict] = None

    @property
    def is_fallthrough(self) -> bool:
        return self.source == "raw_fallthrough"


def process_preview_items(
    db: Session,
    parsed_items: list[dict],
    user_id: int,
    batch_size: int = 20,
) -> list[CleanedResult]:
    """Clean + classify a list of preview items. Order is preserved.

    Each element of ``parsed_items`` is a dict with at minimum a ``description``
    key; ``amount``, ``transaction_type``, ``transaction_date`` are passed
    through to the LLM when present. The cache is scoped to ``user_id`` —
    raws often contain PII (Zelle memos, employer names) so one user's cache
    is never visible to another.

    Empty / None descriptions pass through with empty cleaned value and no
    suggestion. The LLM is called only for rows that miss cache + regex.
    """
    raws = [str(p.get("description") or "") for p in parsed_items]
    results: list[Optional[CleanedResult]] = [None] * len(raws)

    # Bucket indices by hash — duplicate raw strings in one batch share one lookup
    hash_to_indices: dict[str, list[int]] = {}
    index_to_hash: dict[int, str] = {}

    for i, raw in enumerate(raws):
        if not raw or not raw.strip():
            results[i] = CleanedResult(raw=raw, cleaned=raw, source="cache")
            continue
        h = _hash_description(raw)
        hash_to_indices.setdefault(h, []).append(i)
        index_to_hash[i] = h

    if not hash_to_indices:
        return [r for r in results if r is not None]  # all empties

    # Stage 1: DB cache lookup (scoped to this user)
    cached_rows = (
        db.query(DescriptionCacheDB)
        .filter(DescriptionCacheDB.user_id == user_id)
        .filter(DescriptionCacheDB.description_hash.in_(hash_to_indices.keys()))
        .all()
    )
    cached_by_hash = {row.description_hash: row for row in cached_rows}

    for h, row in cached_by_hash.items():
        for i in hash_to_indices[h]:
            results[i] = CleanedResult(
                raw=raws[i],
                cleaned=row.cleaned_description,
                source="cache",
                llm_model=row.llm_model,
            )

    remaining: list[int] = [i for i, r in enumerate(results) if r is None]

    # Stage 2: regex seed match — fills cache for future imports
    new_cache_rows: list[DescriptionCacheDB] = []
    still_remaining: list[int] = []

    for i in remaining:
        raw = raws[i]
        seed_match = _match_seed(raw)
        if seed_match is not None:
            results[i] = CleanedResult(raw=raw, cleaned=seed_match, source="regex_seed")
            new_cache_rows.append(
                DescriptionCacheDB(
                    user_id=user_id,
                    description_hash=index_to_hash[i],
                    raw_description=_truncate(raw),
                    cleaned_description=seed_match,
                    source="regex_seed",
                )
            )
        else:
            still_remaining.append(i)

    # Stage 3: LLM fallback — produces cleaned_description AND llm_suggestion
    if still_remaining:
        _process_llm_stage(
            db, parsed_items, raws, user_id, index_to_hash,
            still_remaining, results, new_cache_rows, batch_size,
        )

    # Persist all new cache rows in one shot.
    if new_cache_rows:
        _insert_cache_rows(db, user_id, new_cache_rows)

    return [r for r in results if r is not None]  # type: ignore[misc]


def clean_descriptions(
    db: Session,
    raws: list[str],
    user_id: int,
    batch_size: int = 20,
) -> list[CleanedResult]:
    """Back-compat shim for #27's description-only callers.

    Synthesizes minimal parsed_items from raw strings and delegates to
    ``process_preview_items``. Results never carry an ``llm_suggestion``
    (the amount / type / date context needed for a meaningful suggestion
    was dropped at the call boundary)."""
    parsed_items = [{"description": r} for r in raws]
    results = process_preview_items(db, parsed_items, user_id=user_id, batch_size=batch_size)
    # Strip suggestions — callers using clean_descriptions aren't prepared
    # to handle them and a partial suggestion from the LLM stage would be
    # misleading without the full parsed context.
    for r in results:
        r.llm_suggestion = None
    return results


# ---------- internals ----------


def _hash_description(raw: str) -> str:
    """SHA-256 of the normalized description. Normalization: lowercase,
    collapse internal whitespace. Case/whitespace differences should share
    a cache entry — they're the same merchant."""
    normalized = re.sub(r"\s+", " ", raw.strip().lower())
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


def _match_seed(raw: str) -> Optional[str]:
    for pattern, cleaned in _SEED_PATTERNS:
        if pattern.search(raw):
            return cleaned
    return None


def _truncate(s: str, limit: int = 500) -> str:
    return s if len(s) <= limit else s[:limit]


def _suggestion_from_result(r: TransactionBatchResult) -> dict:
    """Project a TransactionBatchResult down to the llm_suggestion shape
    stored on preview rows + persisted to ParsedImportDB.llm_suggestions."""
    return {
        "merchant_name": r["merchant_name"],
        "suggested_category_uuid": r["suggested_category_uuid"],
        "suggested_subcategory_uuid": r["suggested_subcategory_uuid"],
        "confidence": r["confidence"],
    }


def _process_llm_stage(
    db: Session,
    parsed_items: list[dict],
    raws: list[str],
    user_id: int,
    index_to_hash: dict[int, str],
    remaining: list[int],
    results: list[Optional[CleanedResult]],
    new_cache_rows: list[DescriptionCacheDB],
    batch_size: int,
) -> None:
    """Batch the remaining indices through the LLM. Writes results to ``results``
    and appends fresh cache rows to ``new_cache_rows``. On LLM failure, fills
    remaining slots with raw_fallthrough — NO cache write in that case.

    One LLM call per unique raw-description hash (deduplicated within the
    batch). When two preview rows share the same raw, they also share the
    same suggestion — acceptable for v1, since any downstream user override
    is captured per-row in ``user_edits`` regardless."""

    # Deduplicate: one LLM call per unique hash. Keep the FIRST parsed_item
    # for that hash (the amount / date context for the later duplicate rows
    # is discarded — they would have produced nearly identical category guesses).
    unique_hashes_in_order: list[str] = []
    seen: set[str] = set()
    hash_to_parsed: dict[str, dict] = {}
    for i in remaining:
        h = index_to_hash[i]
        if h not in seen:
            seen.add(h)
            unique_hashes_in_order.append(h)
            hash_to_parsed[h] = parsed_items[i]

    client = get_llm_client()
    result_by_hash: dict[str, TransactionBatchResult] = {}
    llm_failed = False

    for start in range(0, len(unique_hashes_in_order), batch_size):
        batch_hashes = unique_hashes_in_order[start:start + batch_size]
        batch_parsed = [hash_to_parsed[h] for h in batch_hashes]

        try:
            batch_results = client.process_transaction_batch(batch_parsed)
        except LLMUnavailableError as e:
            logger.warning(
                f"LLM unavailable; falling through raw for "
                f"{len(unique_hashes_in_order) - start} descriptions. {e}"
            )
            llm_failed = True
            break

        for h, res in zip(batch_hashes, batch_results):
            result_by_hash[h] = res

    now = datetime.utcnow()
    model_name = client.model_name

    for i in remaining:
        h = index_to_hash[i]
        raw = raws[i]
        if h in result_by_hash:
            r = result_by_hash[h]
            results[i] = CleanedResult(
                raw=raw,
                cleaned=r["cleaned_description"],
                source="llm",
                llm_model=model_name,
                llm_processed_at=now,
                llm_suggestion=_suggestion_from_result(r),
            )
        else:
            # LLM failed mid-run or never got to this hash
            results[i] = CleanedResult(raw=raw, cleaned=raw, source="raw_fallthrough")

    # Cache rows only for successful LLM outputs (one per unique hash, cleaned
    # description only — suggestions are per-row and not cached).
    for h, r in result_by_hash.items():
        new_cache_rows.append(
            DescriptionCacheDB(
                user_id=user_id,
                description_hash=h,
                raw_description=_truncate(hash_to_parsed[h].get("description") or ""),
                cleaned_description=_truncate(r["cleaned_description"]),
                source="llm",
                llm_model=model_name,
            )
        )

    if llm_failed:
        logger.info("LLM stage completed with fall-through — cache not written for failed items")


def _insert_cache_rows(db: Session, user_id: int, rows: list[DescriptionCacheDB]) -> None:
    """Insert new cache rows for this user, tolerant of races where another
    request has just written the same (user_id, hash). Dedupe within the batch
    and skip any hashes that already exist in the DB for this user."""

    by_hash: dict[str, DescriptionCacheDB] = {}
    for row in rows:
        by_hash.setdefault(row.description_hash, row)

    if not by_hash:
        return

    existing = {
        h for (h,) in db.query(DescriptionCacheDB.description_hash)
        .filter(DescriptionCacheDB.user_id == user_id)
        .filter(DescriptionCacheDB.description_hash.in_(by_hash.keys()))
        .all()
    }
    to_insert = [row for h, row in by_hash.items() if h not in existing]

    if not to_insert:
        return

    db.add_all(to_insert)
    try:
        db.flush()
    except Exception as e:
        # Race: another session inserted the same hash between our check and
        # flush. Safe to ignore — the existing row is equally valid.
        logger.debug(f"Cache insert race (rolling back adds, reading existing): {e}")
        db.rollback()
