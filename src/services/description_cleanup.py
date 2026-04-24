"""
Transaction-description cleanup pipeline.

Three-stage lookup per raw description:
    1. DB cache hit (hashed raw -> cached cleaned value)
    2. Regex seed match (common merchant patterns; populates cache)
    3. LLM fallback (batched; populates cache)

If the LLM is unreachable, misses fall through to the raw description unchanged
and are flagged so the caller can show a degradation banner. The audit trail
(`parsed_imports.raw_parsed_data`) is never dependent on LLM availability.

See backend todo #27 Phase 1.
"""

from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from datetime import datetime
from typing import Optional

from sqlalchemy.orm import Session

from src.db.core import DescriptionCacheDB
from src.logging_config import get_logger
from src.services.llm_client import LLMUnavailableError, get_llm_client

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
    """Result of cleaning a single raw description."""
    raw: str
    cleaned: str
    source: str  # 'cache' | 'regex_seed' | 'llm' | 'raw_fallthrough'
    llm_model: Optional[str] = None
    llm_processed_at: Optional[datetime] = None

    @property
    def is_fallthrough(self) -> bool:
        return self.source == "raw_fallthrough"


def clean_descriptions(
    db: Session,
    raws: list[str],
    user_id: int,
    batch_size: int = 20,
) -> list[CleanedResult]:
    """Clean a list of raw transaction descriptions. Order is preserved.

    The cache is scoped to user_id — raws often contain PII (Zelle memos,
    employer names, payment references) so one user's cache is never visible
    to another. Regex seeds are static patterns shared across users (no PII).

    Empty / None inputs pass through as 'cache' source with empty cleaned value
    (no DB write). The LLM is called only for strings that miss cache + regex.
    """
    results: list[Optional[CleanedResult]] = [None] * len(raws)

    # Bucket indices by hash — duplicate raw strings in one batch share one lookup
    hash_to_indices: dict[str, list[int]] = {}
    index_to_hash: dict[int, str] = {}

    for i, raw in enumerate(raws):
        if not raw or not raw.strip():
            results[i] = CleanedResult(raw=raw or "", cleaned=raw or "", source="cache")
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

    # Stage 3: LLM fallback
    if still_remaining:
        _process_llm_stage(
            db, raws, user_id, index_to_hash, still_remaining, results, new_cache_rows, batch_size
        )

    # Persist all new cache rows in one shot. Duplicate hashes from the same
    # request (within a batch) are de-duped before insert.
    if new_cache_rows:
        _insert_cache_rows(db, user_id, new_cache_rows)

    return [r for r in results if r is not None]  # type: ignore[misc]


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


def _process_llm_stage(
    db: Session,
    raws: list[str],
    user_id: int,
    index_to_hash: dict[int, str],
    remaining: list[int],
    results: list[Optional[CleanedResult]],
    new_cache_rows: list[DescriptionCacheDB],
    batch_size: int,
) -> None:
    """Batch the remaining indices through the LLM. Writes results to `results`
    and appends fresh cache rows to `new_cache_rows`. On LLM failure, fills
    remaining slots with raw_fallthrough — NO cache write in that case."""

    # Deduplicate: one LLM call per unique hash
    unique_hashes_in_order: list[str] = []
    seen: set[str] = set()
    hash_to_raw: dict[str, str] = {}
    for i in remaining:
        h = index_to_hash[i]
        if h not in seen:
            seen.add(h)
            unique_hashes_in_order.append(h)
            hash_to_raw[h] = raws[i]

    client = get_llm_client()
    cleaned_by_hash: dict[str, str] = {}
    llm_failed = False

    for start in range(0, len(unique_hashes_in_order), batch_size):
        batch_hashes = unique_hashes_in_order[start:start + batch_size]
        batch_raws = [hash_to_raw[h] for h in batch_hashes]

        try:
            batch_cleaned = client.clean_descriptions_batch(batch_raws)
        except LLMUnavailableError as e:
            logger.warning(
                f"LLM unavailable; falling through raw for {len(unique_hashes_in_order) - start} descriptions. {e}"
            )
            llm_failed = True
            break

        for h, cleaned in zip(batch_hashes, batch_cleaned):
            cleaned_by_hash[h] = cleaned

    now = datetime.utcnow()
    model_name = client.model_name

    for i in remaining:
        h = index_to_hash[i]
        raw = raws[i]
        if h in cleaned_by_hash:
            results[i] = CleanedResult(
                raw=raw,
                cleaned=cleaned_by_hash[h],
                source="llm",
                llm_model=model_name,
                llm_processed_at=now,
            )
        else:
            # LLM failed mid-run or never got to this hash
            results[i] = CleanedResult(raw=raw, cleaned=raw, source="raw_fallthrough")

    # Cache rows only for successful LLM outputs (one per unique hash)
    for h, cleaned in cleaned_by_hash.items():
        new_cache_rows.append(
            DescriptionCacheDB(
                user_id=user_id,
                description_hash=h,
                raw_description=_truncate(hash_to_raw[h]),
                cleaned_description=_truncate(cleaned),
                source="llm",
                llm_model=model_name,
            )
        )

    if llm_failed:
        # Non-fatal; caller surfaces via banner
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
