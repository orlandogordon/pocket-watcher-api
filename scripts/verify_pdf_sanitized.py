#!/usr/bin/env python
"""Verify a PDF carries no residual PII before it becomes a test fixture.

This is a *tripwire*, not a sanitizer — it never modifies the file. Run it on a
candidate PDF you have already edited/redacted (e.g. in a PDF editor) and it
fails loudly if real data survived the edit.

It checks the two leak modes a visual edit usually misses:
  1. Covered-but-not-removed text — a "deleted" value drawn over with a white
     box still lives in the content stream and extracts right back out. Both
     pdfplumber (what the parsers actually use) and `pdftotext` (if on PATH, a
     second opinion) are scanned.
  2. Leftover document metadata — the Info dictionary (Author/Title/Subject/
     Keywords/Creator/Producer) routinely embeds your name or a
     "Statement_<name>_<acct>" title.
Plus the filename itself.

It does NOT semantically "know" what is PII. Reliable detection comes from the
--deny values YOU pass (your real account number, name, address); a few generic
PII-shaped patterns (SSN, long digit runs, email, phone) are a dumb backstop.
Limits: it cannot see PII baked into an image (scanned signature/logo — eyeball
the page), and it is weak on incremental-save history (a rewrite tool like qpdf
addresses that). XMP metadata is only checked when `pikepdf` is installed.

DENY VALUES ARE YOUR REAL DATA — never commit them. Pass via --deny on the
command line or --deny-file pointing at a gitignored file (one value per line).

Exit code 1 if any FAIL-level finding, else 0. WARN findings are printed for
human review but do not fail.

Usage:
  python scripts/verify_pdf_sanitized.py statement.pdf --deny "John Smith" --deny 49389145
  python scripts/verify_pdf_sanitized.py statement.pdf --deny-file ~/.pw_pii.txt
"""
from __future__ import annotations

import argparse
import re
import shutil
import subprocess
import sys
from pathlib import Path

import pdfplumber

# Info-dict fields that should be empty in a sanitized fixture (FAIL if not).
# Creator/Producer/CreationDate/ModDate are reported as WARN only — they
# fingerprint the software but are not personal data on their own.
_PII_METADATA_FIELDS = ("Author", "Title", "Subject", "Keywords")
_SOFTWARE_METADATA_FIELDS = ("Creator", "Producer", "CreationDate", "ModDate")

_SSN_RE = re.compile(r"\b\d{3}-\d{2}-\d{4}\b")
_EMAIL_RE = re.compile(r"\b[\w.+-]+@[\w-]+\.[\w.-]+\b")
_PHONE_RE = re.compile(r"\b(?:\+?1[ .-]?)?\(?\d{3}\)?[ .-]?\d{3}[ .-]?\d{4}\b")
# Runs of 6+ digits after gluing digit groups split by spaces/dashes (so
# "4938 9145" and "4938-9145" both surface as a single 8-digit run).
_DIGIT_RUN_RE = re.compile(r"\d{6,}")


class Finding:
    def __init__(self, level: str, where: str, detail: str):
        self.level = level  # "FAIL" | "WARN"
        self.where = where
        self.detail = detail

    def __str__(self) -> str:
        return f"[{self.level}] {self.where}: {self.detail}"


def _glue_digit_groups(text: str) -> str:
    """Remove a single space/dash sitting between two digits so grouped numbers
    (account/card formats) collapse into one run for matching."""
    return re.sub(r"(?<=\d)[ \-](?=\d)", "", text)


def extract_text_pdfplumber(path: Path) -> str:
    parts = []
    with pdfplumber.open(str(path)) as pdf:
        for page in pdf.pages:
            parts.append(page.extract_text() or "")
    return "\n".join(parts)


def extract_text_pdftotext(path: Path) -> str | None:
    if not shutil.which("pdftotext"):
        return None
    try:
        out = subprocess.run(
            ["pdftotext", str(path), "-"],
            capture_output=True, text=True, timeout=60,
        )
        return out.stdout
    except (subprocess.SubprocessError, OSError):
        return None


def read_info_metadata(path: Path) -> dict:
    with pdfplumber.open(str(path)) as pdf:
        return dict(pdf.metadata or {})


def read_xmp(path: Path) -> str | None:
    """Best-effort XMP read; returns None when pikepdf isn't installed."""
    try:
        import pikepdf
    except ImportError:
        return None
    try:
        with pikepdf.open(str(path)) as pdf:
            meta = pdf.open_metadata()
            return str(meta) if meta else ""
    except Exception:
        return None


def scan_text(label: str, text: str, deny: list[str]) -> list[Finding]:
    findings: list[Finding] = []
    if not text:
        return findings
    haystack = text.lower()
    glued = _glue_digit_groups(text)
    glued_lower = _glue_digit_groups(haystack)

    for value in deny:
        v = value.strip()
        if not v:
            continue
        if v.lower() in haystack:
            findings.append(Finding("FAIL", label, f"denied value present: {v!r}"))
            continue
        # For numeric values, also match across grouping (spaces/dashes).
        v_glued = _glue_digit_groups(v.lower())
        if v_glued and v_glued.isdigit() and v_glued in glued_lower:
            findings.append(Finding("FAIL", label, f"denied value present (grouped): {v!r}"))

    if _SSN_RE.search(text):
        findings.append(Finding("FAIL", label, "SSN-shaped value (\\d{3}-\\d{2}-\\d{4})"))

    for m in sorted(set(_DIGIT_RUN_RE.findall(glued))):
        findings.append(Finding("WARN", label, f"long digit run (review): {m}"))
    for m in sorted(set(_EMAIL_RE.findall(text))):
        findings.append(Finding("WARN", label, f"email-shaped (review): {m}"))
    for m in sorted(set(_PHONE_RE.findall(text))):
        findings.append(Finding("WARN", label, f"phone-shaped (review): {m}"))
    return findings


def scan_metadata(meta: dict, deny: list[str]) -> list[Finding]:
    findings: list[Finding] = []
    for field in _PII_METADATA_FIELDS:
        val = (meta.get(field) or "").strip()
        if val:
            findings.append(Finding("FAIL", f"metadata/{field}", f"non-empty: {val!r}"))
    for field in _SOFTWARE_METADATA_FIELDS:
        val = (meta.get(field) or "").strip()
        if val:
            findings.append(Finding("WARN", f"metadata/{field}", f"present (review): {val!r}"))
    # Any denied value anywhere in metadata is a hard fail.
    blob = " ".join(str(v) for v in meta.values()).lower()
    for value in deny:
        v = value.strip().lower()
        if v and v in blob:
            findings.append(Finding("FAIL", "metadata", f"denied value present: {value!r}"))
    return findings


def scan_filename(path: Path, deny: list[str]) -> list[Finding]:
    findings: list[Finding] = []
    name = path.name.lower()
    for value in deny:
        v = value.strip().lower()
        if v and v in name:
            findings.append(Finding("FAIL", "filename", f"denied value in name: {value!r}"))
    return findings


def verify(path: Path, deny: list[str]) -> list[Finding]:
    findings: list[Finding] = []

    pp_text = extract_text_pdfplumber(path)
    findings += scan_text("text/pdfplumber", pp_text, deny)

    pt_text = extract_text_pdftotext(path)
    if pt_text is None:
        findings.append(Finding("WARN", "text/pdftotext", "pdftotext not on PATH — only pdfplumber scanned"))
    else:
        findings += scan_text("text/pdftotext", pt_text, deny)

    findings += scan_metadata(read_info_metadata(path), deny)

    xmp = read_xmp(path)
    if xmp is None:
        findings.append(Finding("WARN", "metadata/xmp", "pikepdf not installed — XMP stream NOT checked"))
    elif xmp.strip():
        blob = xmp.lower()
        hit = any(v.strip() and v.strip().lower() in blob for v in deny)
        level = "FAIL" if hit else "WARN"
        findings.append(Finding(level, "metadata/xmp", "XMP stream present — review for PII"))

    findings += scan_filename(path, deny)
    return findings


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Fail if a PDF still contains PII (pre-fixture gate).")
    ap.add_argument("pdf", type=Path)
    ap.add_argument("--deny", action="append", default=[], metavar="VALUE",
                    help="Exact real value that must NOT appear (repeatable).")
    ap.add_argument("--deny-file", type=Path, default=None,
                    help="File of denied values, one per line (keep gitignored).")
    args = ap.parse_args(argv)

    if not args.pdf.is_file():
        print(f"error: not a file: {args.pdf}", file=sys.stderr)
        return 2

    deny = list(args.deny)
    if args.deny_file and args.deny_file.is_file():
        deny += [ln.strip() for ln in args.deny_file.read_text(encoding="utf-8").splitlines() if ln.strip()]

    findings = verify(args.pdf, deny)
    fails = [f for f in findings if f.level == "FAIL"]
    warns = [f for f in findings if f.level == "WARN"]

    for f in fails:
        print(str(f))
    for f in warns:
        print(str(f))

    if not deny:
        print("note: no --deny values given — only generic patterns + metadata were checked.")

    if fails:
        print(f"\nRESULT: FAIL ({len(fails)} blocking, {len(warns)} warnings) — do NOT commit {args.pdf.name}")
        return 1
    print(f"\nRESULT: PASS ({len(warns)} warnings to eyeball) — {args.pdf.name}. "
          "Still visually confirm no image-rendered PII.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
