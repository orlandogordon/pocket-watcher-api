"""
Evaluate the configured (prompt, model) pair's merchant accuracy against a
hand-labeled golden set.

Runs each raw description in `eval/description_cleanup_golden.csv` directly
through the LLM client — DB cache and regex extraction are bypassed so we
measure pure model performance on merchant inference. The golden CSV's
``expected`` column (formerly cleaned-description target) is ignored under
#35; only ``expected_merchant`` is asserted. Empty expected_merchant rows
are skipped.

Usage:
    python scripts/eval_description_cleanup.py
    python scripts/eval_description_cleanup.py --golden eval/description_cleanup_golden.csv
    LLM_ENDPOINT=http://localhost:8081/v1 LLM_MODEL=qwen3.5-4b-q4 \\
        python scripts/eval_description_cleanup.py

To benchmark two models side-by-side, run two llama-server instances on different
ports and invoke this script twice with different LLM_ENDPOINT / LLM_MODEL values.
"""
from __future__ import annotations

import argparse
import csv
import os
import sys
import time
from pathlib import Path
from typing import Optional

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.services.llm_client import get_llm_client, LLMUnavailableError, reset_llm_client  # noqa: E402


def _load_golden(path: Path) -> list[tuple[str, str]]:
    """Returns (raw, expected_merchant). Rows with no expected_merchant are
    skipped — the description column from earlier iterations is no longer
    asserted (see #35)."""
    with path.open(encoding="utf-8") as f:
        reader = csv.DictReader(f)
        return [
            (row["raw"], row.get("expected_merchant", "") or "")
            for row in reader
            if (row.get("expected_merchant") or "").strip()
        ]


def _norm(s: str) -> str:
    """Case-insensitive, whitespace-collapsed comparison."""
    return " ".join(s.lower().split())


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--golden",
        default="eval/description_cleanup_golden.csv",
        help="Path to golden CSV with 'raw' and 'expected' columns",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=20,
        help="LLM batch size (matches production pipeline default)",
    )
    args = parser.parse_args()

    golden_path = Path(args.golden)
    if not golden_path.exists():
        print(f"ERROR: golden set not found at {golden_path}")
        return 2

    rows = _load_golden(golden_path)
    if not rows:
        print(f"ERROR: {golden_path} has no rows with expected_merchant set")
        return 2

    reset_llm_client()
    client = get_llm_client()

    print(f"Evaluating {len(rows)} rows against model='{client.model_name}' "
          f"(backend={os.getenv('LLM_BACKEND', 'llama_cpp')})")
    print(f"Endpoint: {os.getenv('LLM_ENDPOINT', 'http://localhost:8080/v1')}")
    print()

    raws = [r for r, _ in rows]
    expected_merchants = [m for _, m in rows]

    parsed_rows = [
        {"description": r, "amount": "", "transaction_type": "", "transaction_date": ""}
        for r in raws
    ]

    actual_merchants: list[Optional[str]] = []
    start = time.time()
    try:
        for i in range(0, len(parsed_rows), args.batch_size):
            batch = parsed_rows[i:i + args.batch_size]
            results = client.process_transaction_batch(batch)
            actual_merchants.extend(r["merchant_name"] for r in results)
    except LLMUnavailableError as e:
        print(f"ERROR: LLM unavailable — {e}")
        return 2
    elapsed_ms = int((time.time() - start) * 1000)

    merchant_failures: list[tuple[str, str, Optional[str]]] = []
    for raw, exp_merch, act_merch in zip(raws, expected_merchants, actual_merchants):
        if _norm(act_merch or "") != _norm(exp_merch):
            merchant_failures.append((raw, exp_merch, act_merch))

    merchant_passed = len(rows) - len(merchant_failures)
    merchant_pct = 100.0 * merchant_passed / len(rows)
    per_batch_ms = elapsed_ms / max(1, (len(rows) + args.batch_size - 1) // args.batch_size)

    print(f"Merchant accuracy: {merchant_passed}/{len(rows)} ({merchant_pct:.1f}%)")
    print(f"Total time: {elapsed_ms}ms   Avg per batch ({args.batch_size} items): {per_batch_ms:.0f}ms")
    print()

    if merchant_failures:
        print("MERCHANT FAILURES:")
        for raw, expected, actual in merchant_failures:
            print(f"  raw:      {raw}")
            print(f"  expected: {expected}")
            print(f"  actual:   {actual}")
            print()
        return 1

    print("All golden rows passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
