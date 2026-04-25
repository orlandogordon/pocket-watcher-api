"""
Evaluate the configured (prompt, model) pair against the category-suggestion
golden set.

Runs each parsed-transaction row in ``eval/category_suggestion_golden.csv``
directly through ``LLMClient.process_transaction_batch`` — regex seeds and
DB cache are bypassed so we measure pure model performance. Reports top-1
accuracy for subcategory (the harder of the two), parent-category accuracy,
a confusion matrix, and the self-reported confidence distribution split by
correct/incorrect.

Usage:
    python scripts/eval_category_suggestion.py
    python scripts/eval_category_suggestion.py --golden eval/category_suggestion_golden.csv
    LLM_ENDPOINT=http://localhost:8081/v1 LLM_MODEL=qwen3.5-4b-q4 \\
        python scripts/eval_category_suggestion.py

See backend todo #29.
"""
from __future__ import annotations

import argparse
import csv
import json
import os
import sys
import time
from collections import Counter, defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.constants.categories import name_by_uuid  # noqa: E402
from src.services.llm_client import (  # noqa: E402
    LLMUnavailableError,
    get_llm_client,
    reset_llm_client,
)


def _load_golden(path: Path) -> list[tuple[dict, str, str]]:
    out: list[tuple[dict, str, str]] = []
    with path.open(encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            parsed = json.loads(row["raw_parsed_data_json"])
            out.append((parsed, row["expected_category_uuid"], row["expected_subcategory_uuid"]))
    return out


def _bucket(confidences: list[float], correct_flags: list[bool]) -> dict[str, dict]:
    """Split self-reported confidence into decile-ish buckets and report
    counts + empirical accuracy per bucket."""
    buckets = {
        "[0.0-0.5)": [],
        "[0.5-0.8)": [],
        "[0.8-0.9)": [],
        "[0.9-0.95)": [],
        "[0.95-1.0]": [],
    }
    for c, ok in zip(confidences, correct_flags):
        if c < 0.5:
            buckets["[0.0-0.5)"].append(ok)
        elif c < 0.8:
            buckets["[0.5-0.8)"].append(ok)
        elif c < 0.9:
            buckets["[0.8-0.9)"].append(ok)
        elif c < 0.95:
            buckets["[0.9-0.95)"].append(ok)
        else:
            buckets["[0.95-1.0]"].append(ok)
    return {
        k: {"n": len(v), "accuracy": (sum(v) / len(v)) if v else 0.0}
        for k, v in buckets.items()
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--golden",
        default="eval/category_suggestion_golden.csv",
        help="Path to golden CSV (raw_parsed_data_json, expected_category_uuid, expected_subcategory_uuid)",
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
        print(f"ERROR: {golden_path} is empty")
        return 2

    reset_llm_client()
    client = get_llm_client()
    names = name_by_uuid()

    print(f"Evaluating {len(rows)} rows against model='{client.model_name}' "
          f"(backend={os.getenv('LLM_BACKEND', 'llama_cpp')})")
    print(f"Endpoint: {os.getenv('LLM_ENDPOINT', 'http://localhost:8080/v1')}")
    print()

    parsed_inputs = [p for p, _, _ in rows]
    expected_cat = [e for _, e, _ in rows]
    expected_sub = [e for _, _, e in rows]

    predicted_cat: list[str] = []
    predicted_sub: list[str] = []
    confidences: list[float] = []

    start = time.time()
    try:
        for i in range(0, len(parsed_inputs), args.batch_size):
            batch = parsed_inputs[i:i + args.batch_size]
            results = client.process_transaction_batch(batch)
            for r in results:
                predicted_cat.append(r["suggested_category_uuid"])
                predicted_sub.append(r["suggested_subcategory_uuid"])
                confidences.append(r["confidence"])
    except LLMUnavailableError as e:
        print(f"ERROR: LLM unavailable — {e}")
        return 2
    elapsed_ms = int((time.time() - start) * 1000)

    parent_correct = [p == e for p, e in zip(predicted_cat, expected_cat)]
    sub_correct = [p == e for p, e in zip(predicted_sub, expected_sub)]

    parent_acc = sum(parent_correct) / len(rows)
    sub_acc = sum(sub_correct) / len(rows)
    per_batch_ms = elapsed_ms / max(1, (len(rows) + args.batch_size - 1) // args.batch_size)

    print(f"Parent accuracy:      {sum(parent_correct)}/{len(rows)} ({100 * parent_acc:.1f}%)")
    print(f"Subcategory accuracy: {sum(sub_correct)}/{len(rows)} ({100 * sub_acc:.1f}%)")
    print(f"Total time: {elapsed_ms}ms   Avg per batch ({args.batch_size} items): {per_batch_ms:.0f}ms")
    print()

    # Confidence buckets (on subcategory accuracy — the harder signal)
    print("Confidence vs correctness (subcategory):")
    for bucket, stats in _bucket(confidences, sub_correct).items():
        print(f"  {bucket:>14}  n={stats['n']:>3}  accuracy={100 * stats['accuracy']:.1f}%")
    print()

    # Confusion matrix on parent categories (easy to read at this scale)
    confusion: dict[tuple[str, str], int] = defaultdict(int)
    for exp, pred, ok in zip(expected_cat, predicted_cat, parent_correct):
        if not ok:
            confusion[(names.get(exp, exp), names.get(pred, pred))] += 1
    if confusion:
        print("Parent-category confusion (expected -> predicted):")
        for (exp_name, pred_name), n in sorted(confusion.items(), key=lambda x: -x[1]):
            print(f"  {n:>3}x  {exp_name} -> {pred_name}")
        print()

    # Failure dump — most useful signal when iterating on prompt
    failures = [
        (p, ec, es, pc, ps, conf)
        for p, ec, es, pc, ps, conf
        in zip(parsed_inputs, expected_cat, expected_sub,
               predicted_cat, predicted_sub, confidences)
        if pc != ec or ps != es
    ]
    if failures:
        print(f"FAILURES ({len(failures)}):")
        for p, ec, es, pc, ps, conf in failures:
            desc = p.get("description", "")
            print(f"  raw:      {desc}")
            print(f"  expected: {names.get(ec, ec)} / {names.get(es, es)}")
            print(f"  actual:   {names.get(pc, pc)} / {names.get(ps, ps)}  (conf={conf:.2f})")
            print()

    return 0 if sub_acc >= 0.8 else 1


if __name__ == "__main__":
    raise SystemExit(main())
