## Other products to consider
- plain text accountng
- bean count
- hledger
- docuclipper
- streamlit
- time shift data

## TODO
- Breakout the image handling into a separate function
- Update the image handling logic to use output folder instead of png
- Consider class based approach to combine all parsers
- Implement more regex

## API Curl requests testing

- uvicorn src.main:app --reload

- Run Claude Code: npx @anthropic-ai/claude-code


## Testing

The test suite is `pytest`-based and runs the app in-process via `TestClient` —
**no running server, database, or Redis is required** (SQLite is in-memory and
Redis is faked).

Run everything with coverage:

```
./venv/Scripts/python.exe -m pytest --cov=src --cov-report=term-missing -q
```

(`python -m pytest ...` on any platform; the path above is the Windows venv.)

- **Coverage** lands around **76% on a fresh clone** and **~78% locally**. There
  is no enforced `--cov-fail-under` floor — coverage is a guardrail, not the
  target. The PDF `parse_statement`/`parse_pdf` parser bodies are excluded from
  measurement (see `pyproject.toml`) because they can only be exercised by real
  statement PDFs, which are never committed.
- **Markers** (`pyproject.toml`): `parser`, `integration`, `slow`. Deselect with
  e.g. `-m "not parser"`.
- **Parser regression corpus**: real statements live, gitignored, under
  `tests/parsers/fixtures/local/<institution>/` and contain PII, so they are
  never committed. Those tests **skip when the corpus is absent**. The committed
  synthetic CSV fixtures (`tests/parsers/fixtures/*.csv`) cover the CSV paths.
- **No PII in committed fixtures.** `scripts/verify_pdf_sanitized.py` is a
  tripwire that fails if a candidate PDF still carries PII;
  `tests/test_fixture_pii_guard.py` runs it over every committed
  `tests/parsers/fixtures/*.pdf` as part of the suite.

## Bulk Upload Script instruction
  Before you run it, you need to:

   1. Edit `scripts/bulk_upload.py` and update the ACCOUNT_MAPPING dictionary with the correct
      account_id for each folder. I've put in placeholder values.
   2. Start your FastAPI server in a separate terminal with the command: uvicorn src.main:app 
      --reload
   3. Run the script with: python scripts/bulk_upload.py

  The script will then go through your input folder and upload the files.

  A note on authentication: The script currently doesn't send any authentication headers. If
  your /uploads/ endpoint is protected, you'll need to add an Authorization header in the
  HEADERS dictionary within the script.



## TODO
- Add user id validation wherever it is mapped as a foreign key
- Fix user table to use db_id - id pattern
- Refine model validation, optional attributes in pydantic models
- Create update and delete logic
- Streamlined testing setup? (pre written bash script maybe?)
- Add model factory to UserCreate pydantic model id field (see transactionCreate)

- The add categories endpoint is not including the parent category id when given
- Financial plans takes in a target amount but doesn't store it in the db. instead it is storing a monthyl income value that i don't find to be as useful
- Financial plan entires bulk upload endpoint does not work
- The endpoint for assigning a tag to a transaction takes in the db id rather than the public (uuid)
- The bulk transaction-tag assignment endpoint is broken
- Transaction Relationship update and deletion endpoints seem to be missing
- The debt payment creation endpoint is not populating principal/interest amount data and remaining balance data. The endpoint also is not checking to make sure the account_id provided is a loan account (and maybe a credit_card) and returning an error if it is not. 
- Investment Transactions endpoint is not updating the account value based on the transaction processed. 
- Investment transaction parsing is not implemented. 


## Architectural Notes

- The application follows a standard Next.js App Router structure.
- Reusable UI components are located in `src/components`.
- Firebase configuration and utility functions are in `src/lib/firebase.ts`.
- Global styles and Tailwind CSS configuration are in `src/app/globals.css` and `tailwind.config.ts` respectively.
- The application is a Progressive Web App (PWA), with configuration in `public/manifest.json`.
- Firebase Firestore rules are available in `firebaserules.txt` file.

## Implementation standard.

- DO NOT over engineer things. Start with the simplest implementation.
- Always keep the performance and security as a first priority.
- Ask for any clarification rather just guessing things if you are not clear about anything.