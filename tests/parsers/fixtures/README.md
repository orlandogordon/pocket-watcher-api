# Parser test fixtures

Two tiers, by what is safe to commit:

## Committed — synthetic / sanitized (in this directory)

Small, hand-authored fixtures with **no real account numbers or PII**. Safe in
git and run everywhere (CI, fresh clone):

- `amex_sample.csv`, `schwab_sample.csv`, `ameriprise_sample.csv` — drive the
  parsers' deterministic CSV paths (`tests/test_investment_parser_csv.py`,
  `tests/test_uploads_flow.py`).

Do **not** commit a real PDF here. Sanitizing a statement well enough to commit
(removing covered-but-not-deleted text, document metadata, and incremental-save
history) is error-prone; prefer the gitignored `local/` tier below.

## Gitignored — real statements (`local/<institution>/`)

`tests/parsers/fixtures/local/` is **gitignored** (never committed — these are
real statements with PII). It is a *curated* corpus for local-only PDF-parser
regression: drop a few representative statements per institution into

```
tests/parsers/fixtures/local/schwab/*.pdf
tests/parsers/fixtures/local/tdameritrade/*.pdf
tests/parsers/fixtures/local/ameriprise/*.pdf
```

`tests/test_investment_pdf_parsers.py` parses them and asserts **structural
invariants only** (counts, normalized types, OCC option-symbol shape, date
sanity) — never specific amounts/dates/symbols. With no PDFs present the tests
**skip**, so the suite stays green without them. These tests are marked `slow`
(pdfplumber); exclude with `-m "not slow"`.

This folder is distinct from the app's `input/` working directory on purpose:
it makes the test corpus explicit and intentional rather than "whatever happens
to be in `input/`".
