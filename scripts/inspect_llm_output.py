"""
Spot-check the LLM cleanup output against real PDFs without touching the DB.

Two modes:
  1. Single file:    python scripts/inspect_llm_output.py <path-to-pdf> <institution>
  2. Directory walk: python scripts/inspect_llm_output.py <directory>

Directory mode walks the tree, auto-detects institution from the immediate
parent folder name (matching `PARSER_MAPPING` in `src/services/importer.py`),
and parses every PDF/CSV through the institution's parser, then runs each
batch through the LLM client.

Outputs one pair of CSVs per institution into `<output-dir>/`:
  - <institution>.csv:                 every row for that institution, full data
  - <institution>.low_confidence.csv:  only rows below the confidence threshold,
                                       sorted ascending by confidence — the most
                                       useful list for spotting new failure modes.

No DB writes, no preview session, no caching. Pure model performance against
real data at scale.

Examples:
    python scripts/inspect_llm_output.py "input/tdbank/View PDF Statement_2025-01-13.pdf" tdbank
    python scripts/inspect_llm_output.py input/
    python scripts/inspect_llm_output.py input/ --output-dir llm_inspect --threshold 0.75
"""
from __future__ import annotations

import argparse
import csv
import io
import sys
import time
from collections import defaultdict
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from typing import Any, Iterator

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.services.importer import PARSER_MAPPING  # noqa: E402
from src.services.llm_client import get_llm_client, LLMUnavailableError  # noqa: E402
from src.services.merchant_extractor import extract_merchant  # noqa: E402


CSV_FIELDS = [
    "institution",
    "filename",
    "kind",                 # 'standard' | 'investment'
    "transaction_date",
    "transaction_type",
    "description",          # raw, verbatim from parser (no cleanup since #35)
    "merchant",             # post-extractor decision: regex > llm > null
    "merchant_source",      # 'regex' | 'llm' | 'null'
    "llm_merchant_raw",     # what the LLM returned, before extractor override
    "confidence",
    "category_uuid",
    "subcategory_uuid",
]


@dataclass
class Row:
    institution: str
    filename: str
    kind: str
    transaction_date: str
    transaction_type: str
    description: str
    merchant: str
    merchant_source: str
    llm_merchant_raw: str
    confidence: float
    category_uuid: str
    subcategory_uuid: str

    def as_dict(self) -> dict[str, Any]:
        return {f: getattr(self, f) for f in CSV_FIELDS}


def _coerce_amount(v: Any) -> str:
    if isinstance(v, Decimal):
        return str(v)
    return str(v) if v is not None else ""


def _build_payload(t: Any, kind: str) -> dict:
    """Build the dict shape `_render_parsed_for_prompt` expects."""
    base = {
        "description": getattr(t, "description", "") or "",
        "transaction_type": str(getattr(t, "transaction_type", "")),
        "transaction_date": getattr(t, "transaction_date").isoformat() if getattr(t, "transaction_date", None) else "",
    }
    if kind == "investment":
        base["amount"] = _coerce_amount(getattr(t, "total_amount", ""))
        symbol = getattr(t, "symbol", None)
        if symbol:
            base["symbol"] = str(symbol)
        sec = getattr(t, "security_type", None)
        if sec is not None:
            base["security_type"] = sec.value if hasattr(sec, "value") else str(sec)
    else:
        base["amount"] = _coerce_amount(getattr(t, "amount", ""))
    return base


def _iter_files(root: Path) -> Iterator[tuple[Path, str]]:
    """Yield (file_path, institution) for every parseable file under `root`.

    Institution is the immediate parent folder name. Files whose parent
    isn't in PARSER_MAPPING are skipped with a stderr warning.
    """
    if root.is_file():
        return

    seen_unmapped: set[str] = set()
    for path in sorted(root.rglob("*")):
        if not path.is_file():
            continue
        if path.suffix.lower() not in (".pdf", ".csv"):
            continue
        institution = path.parent.name.lower()
        if institution not in PARSER_MAPPING:
            if institution not in seen_unmapped:
                print(f"  skip: no parser for folder '{institution}' (skipping all files under it)", file=sys.stderr)
                seen_unmapped.add(institution)
            continue
        yield path, institution


def _process_file(
    path: Path,
    institution: str,
    client: Any,
    batch_size: int,
) -> list[Row]:
    """Parse one statement, run rows through the LLM, return Row objects."""
    parser = PARSER_MAPPING[institution]
    is_csv = path.suffix.lower() == ".csv"
    try:
        with path.open("rb") as f:
            file_obj = io.BytesIO(f.read())
            parsed_data = parser.parse(file_obj, is_csv=is_csv)
    except Exception as e:
        print(f"  parse error: {path.name} -- {type(e).__name__}: {e}", file=sys.stderr)
        return []

    txn_specs: list[tuple[str, Any]] = []  # (kind, parsed_txn)
    txn_specs.extend(("standard", t) for t in (parsed_data.transactions or []))
    txn_specs.extend(("investment", t) for t in (parsed_data.investment_transactions or []))
    if not txn_specs:
        return []

    payloads = [_build_payload(t, kind) for kind, t in txn_specs]

    rows: list[Row] = []
    for i in range(0, len(payloads), batch_size):
        chunk = payloads[i:i + batch_size]
        try:
            results = client.process_transaction_batch(chunk)
        except LLMUnavailableError as e:
            print(f"  LLM unavailable on batch starting {i} of {path.name}: {e}", file=sys.stderr)
            return rows  # partial results
        for (kind, txn), payload, res in zip(txn_specs[i:i + batch_size], chunk, results):
            raw_desc = payload["description"]
            regex_merchant = extract_merchant(institution, raw_desc)
            llm_merchant = res["merchant_name"]
            if regex_merchant is not None:
                final_merchant = regex_merchant
                merchant_source = "regex"
            elif llm_merchant is not None:
                final_merchant = llm_merchant
                merchant_source = "llm"
            else:
                final_merchant = ""
                merchant_source = "null"
            rows.append(Row(
                institution=institution,
                filename=path.name,
                kind=kind,
                transaction_date=payload["transaction_date"],
                transaction_type=payload["transaction_type"],
                description=raw_desc,
                merchant=final_merchant or "",
                merchant_source=merchant_source,
                llm_merchant_raw=llm_merchant or "",
                confidence=float(res.get("confidence", 0.0)),
                category_uuid=res.get("suggested_category_uuid", "") or "",
                subcategory_uuid=res.get("suggested_subcategory_uuid", "") or "",
            ))
    return rows


def _write_csv(path: Path, rows: list[Row]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_FIELDS)
        writer.writeheader()
        for r in rows:
            writer.writerow(r.as_dict())


def _institution_csv_paths(output_dir: Path, institution: str) -> tuple[Path, Path]:
    full = output_dir / f"{institution}.csv"
    low_conf = output_dir / f"{institution}.low_confidence.csv"
    return full, low_conf


def main() -> int:
    p = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument("path", help="Path to a single PDF/CSV, or a directory to walk")
    p.add_argument(
        "institution",
        nargs="?",
        help="Institution key (required when path is a single file; ignored for directories)",
    )
    p.add_argument(
        "--output-dir", "-o",
        default="llm_inspect",
        help="Directory to write per-institution CSVs into (default: ./llm_inspect/). "
             "Each institution gets <institution>.csv and <institution>.low_confidence.csv.",
    )
    p.add_argument(
        "--threshold", "-t",
        type=float,
        default=0.7,
        help="Confidence threshold for the low-confidence report (default: 0.7)",
    )
    p.add_argument(
        "--batch-size",
        type=int,
        default=20,
        help="LLM batch size — matches production default (20)",
    )
    args = p.parse_args()

    root = Path(args.path)
    if not root.exists():
        print(f"ERROR: path not found: {root}")
        return 2

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    client = get_llm_client()

    files: list[tuple[Path, str]] = []
    if root.is_file():
        if not args.institution:
            print("ERROR: institution argument required when path is a single file")
            return 2
        institution = args.institution.lower()
        if institution not in PARSER_MAPPING:
            print(f"ERROR: no parser for institution '{institution}'. Available: {list(PARSER_MAPPING.keys())}")
            return 2
        files.append((root, institution))
    else:
        files = list(_iter_files(root))

    if not files:
        print("No parseable files found.")
        return 0

    print(f"Found {len(files)} file(s). Streaming through LLM (model={client.model_name}, batch={args.batch_size})")
    print(f"Output dir: {output_dir.resolve()}")
    print(f"Confidence threshold for low-conf reports: {args.threshold}")
    print()

    by_institution: dict[str, list[Row]] = defaultdict(list)
    start = time.time()
    for i, (path, institution) in enumerate(files, 1):
        elapsed = time.time() - start
        running = sum(len(rs) for rs in by_institution.values())
        print(f"  [{i}/{len(files)}] {institution}/{path.name}  (elapsed {elapsed:.0f}s, rows so far {running})")
        rows = _process_file(path, institution, client, args.batch_size)
        if rows:
            by_institution[institution].extend(rows)
            # Incremental flush of this institution's CSV so a long run doesn't
            # lose work on a crash. Low-confidence reports are written only
            # at the end.
            full_path, _ = _institution_csv_paths(output_dir, institution)
            _write_csv(full_path, by_institution[institution])

    elapsed = time.time() - start
    total_rows = sum(len(rs) for rs in by_institution.values())
    print(f"\nProcessed {total_rows} transactions across {len(files)} files in {elapsed:.0f}s.")
    print()

    print("Per-institution summary:")
    for institution in sorted(by_institution):
        rows = by_institution[institution]
        full_path, low_conf_path = _institution_csv_paths(output_dir, institution)
        low_conf = sorted(
            [r for r in rows if r.confidence < args.threshold],
            key=lambda r: r.confidence,
        )
        _write_csv(low_conf_path, low_conf)
        avg_conf = sum(r.confidence for r in rows) / len(rows) if rows else 0.0
        regex_n = sum(1 for r in rows if r.merchant_source == "regex")
        llm_n = sum(1 for r in rows if r.merchant_source == "llm")
        null_n = sum(1 for r in rows if r.merchant_source == "null")
        regex_pct = 100.0 * regex_n / len(rows) if rows else 0.0
        print(
            f"  {institution:<16} {len(rows):>5} rows  "
            f"avg conf {avg_conf:.2f}  "
            f"low-conf {len(low_conf):>4} (<{args.threshold})  "
            f"regex {regex_n:>4} ({regex_pct:.0f}%)  llm {llm_n:>4}  null {null_n:>4}"
        )

    print()
    print("Lowest-confidence rows across all institutions:")
    all_low = sorted(
        [r for rs in by_institution.values() for r in rs if r.confidence < args.threshold],
        key=lambda r: r.confidence,
    )
    for r in all_low[:10]:
        print(f"  conf={r.confidence:.2f}  {r.institution}  {r.description[:70]}")
        print(f"               -> merchant={r.merchant!r} ({r.merchant_source})")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
