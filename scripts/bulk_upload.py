"""
Bulk-upload local statement files into the DB without going through the
preview/confirm HTTP flow.

This is a dev-only convenience script. Unlike the preview flow (which gives
the user a chance to review LLM suggestions before they hit the DB), this
script auto-accepts every suggestion the LLM produces. That's intentional —
the script's whole point is to skip the review step.

Pipeline per file:
  1. Parse with the institution's parser (raw ParsedTransaction list).
  2. Run the parsed rows through process_preview_items to get cleaned
     descriptions + per-row category/merchant suggestions (#27 + #29).
  3. Bulk-insert via the same crud helpers the legacy path used. Hashes are
     computed from the RAW parser description so re-uploads stay deduped
     against transactions imported via either path.
  4. Post-process the freshly-created TransactionDB rows: overwrite the
     description with the cleaned value and apply the suggestion's
     merchant_name + category_id + subcategory_id. Investment rows only get
     the cleaned description (no category_id column on InvestmentTransactionDB).
"""
import os
import sys
import io
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

# --- Add project root to Python path ---
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.join(SCRIPT_DIR, "..")
sys.path.append(PROJECT_ROOT)

from src.db.core import DATABASE_URL, CategoryDB
from src.services.importer import PARSER_MAPPING
from src.services.description_cleanup import process_preview_items, CleanedResult
from src.crud import crud_transaction, crud_investment

# --- Configuration ---
INPUT_DIR = os.path.join(PROJECT_ROOT, "input")
STATEMENTS_DIR = os.path.join(INPUT_DIR, "statements")
CSV_DIR = os.path.join(INPUT_DIR, "transaction_csv")

# !!! IMPORTANT !!!
# Set the user_id for whom the transactions will be uploaded.
USER_ID = 1

# Replace the placeholder values with your actual account IDs from the database.
# The key is the name of the folder in the input directory.
ACCOUNT_MAPPING = {
    "tdbank": 1,
    "amex": 2,
    "amzn-synchrony": 3,
    # "schwab": 5,
    # "tdameritrade": 6,
    # "ameriprise": 8,
}

# --- Database Setup ---
engine = create_engine(DATABASE_URL, echo=False)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


def _build_result_lookup(parsed_txns, results: list[CleanedResult]) -> dict:
    """Index CleanedResults by (date, amount, raw_description) so we can match
    them back to created TransactionDB rows after the bulk insert filters out
    duplicates and unmapped types."""
    lookup = {}
    for txn, result in zip(parsed_txns, results):
        key = (txn.transaction_date, txn.amount, txn.description or "")
        lookup[key] = result
    return lookup


def _resolve_category_uuids(db, suggestions: list[dict]) -> dict:
    """Map predefined category UUIDs (string) -> CategoryDB.id (int) for every
    UUID referenced by the batch's suggestions."""
    uuids = set()
    for s in suggestions:
        if s.get("suggested_category_uuid"):
            uuids.add(s["suggested_category_uuid"])
        if s.get("suggested_subcategory_uuid"):
            uuids.add(s["suggested_subcategory_uuid"])
    if not uuids:
        return {}
    from uuid import UUID
    rows = (
        db.query(CategoryDB.uuid, CategoryDB.id)
        .filter(CategoryDB.uuid.in_([UUID(u) for u in uuids]))
        .all()
    )
    return {str(r.uuid): r.id for r in rows}


def _apply_cleanup_to_created(created_rows, parsed_txns, results: list[CleanedResult],
                              category_uuid_to_id: dict, has_category_columns: bool):
    """Walk the freshly-created DB rows and overwrite description + (when
    applicable) merchant_name/category_id/subcategory_id from the matching
    CleanedResult. Returns (suggestions_applied_count, fallthrough_count)."""
    lookup = _build_result_lookup(parsed_txns, results)
    suggestions_applied = 0
    fallthroughs = 0

    for row in created_rows:
        # TransactionDB.amount is abs() of parser amount; match on raw fields
        # using the parsed_data reference instead.
        # (Investment row uses .total_amount, not .amount — fall through if no match.)
        for txn in parsed_txns:
            if (
                txn.transaction_date == row.transaction_date
                and (txn.description or "") == row.description
            ):
                key = (txn.transaction_date, txn.amount, txn.description or "")
                result = lookup.get(key)
                if result is None:
                    continue

                row.description = result.cleaned
                if result.is_fallthrough:
                    fallthroughs += 1

                if has_category_columns and result.llm_suggestion:
                    sug = result.llm_suggestion
                    row.merchant_name = sug.get("merchant_name")
                    cat_uuid = sug.get("suggested_category_uuid")
                    sub_uuid = sug.get("suggested_subcategory_uuid")
                    if cat_uuid and cat_uuid in category_uuid_to_id:
                        row.category_id = category_uuid_to_id[cat_uuid]
                    if sub_uuid and sub_uuid in category_uuid_to_id:
                        row.subcategory_id = category_uuid_to_id[sub_uuid]
                    suggestions_applied += 1
                break

    return suggestions_applied, fallthroughs


def process_local_file(db, file_path, institution, account_id, user_id):
    """Parse a local file and import into the DB with LLM cleaning + auto-accepted
    category/merchant suggestions."""
    print(f"  Processing file: {os.path.basename(file_path)}")
    parser = PARSER_MAPPING.get(institution.lower())
    if not parser:
        print(f"    Warning: No parser found for institution '{institution}'. Skipping.")
        return

    try:
        with open(file_path, 'rb') as f:
            is_csv = file_path.lower().endswith('.csv')
            file_obj = io.BytesIO(f.read())
            parsed_data = parser.parse(file_obj, is_csv=is_csv)

        # --- Regular transactions ---
        if parsed_data.transactions:
            print(f"    Found {len(parsed_data.transactions)} standard transactions. Cleaning...")
            parsed_items = [
                {
                    "description": t.description,
                    "amount": float(t.amount),
                    "transaction_type": t.transaction_type,
                    "transaction_date": t.transaction_date.isoformat(),
                }
                for t in parsed_data.transactions
            ]
            results = process_preview_items(db, parsed_items, user_id=user_id)
            suggestions = [r.llm_suggestion for r in results if r.llm_suggestion]
            uuid_to_id = _resolve_category_uuids(db, suggestions)

            print(f"    Importing {len(parsed_data.transactions)} standard transactions...")
            created, _skipped = crud_transaction.bulk_create_transactions_from_parsed_data(
                db=db,
                user_id=user_id,
                transactions=parsed_data.transactions,
                institution_name=institution,
                account_id=account_id,
            )
            applied, fallthroughs = _apply_cleanup_to_created(
                created, parsed_data.transactions, results,
                uuid_to_id, has_category_columns=True,
            )
            db.commit()
            print(f"    Inserted {len(created)} (suggestions applied: {applied}, "
                  f"raw fallthroughs: {fallthroughs})")

        # --- Investment transactions ---
        if parsed_data.investment_transactions:
            print(f"    Found {len(parsed_data.investment_transactions)} investment transactions. Cleaning...")
            inv_items = [
                {
                    "description": t.description,
                    "amount": float(t.total_amount),
                    "transaction_type": t.transaction_type,
                    "transaction_date": t.transaction_date.isoformat(),
                }
                for t in parsed_data.investment_transactions
            ]
            inv_results = process_preview_items(db, inv_items, user_id=user_id)

            print(f"    Importing {len(parsed_data.investment_transactions)} investment transactions...")
            created_inv, _skipped_inv, _backfill_id = crud_investment.bulk_create_investment_transactions_from_parsed_data(
                db=db,
                user_id=user_id,
                transactions=parsed_data.investment_transactions,
                institution_name=institution,
                account_id=account_id,
            )
            # Investment rows: only overwrite description (no category columns).
            _apply_cleanup_to_created(
                created_inv, parsed_data.investment_transactions, inv_results,
                category_uuid_to_id={}, has_category_columns=False,
            )
            db.commit()
            print(f"    Inserted {len(created_inv)} investment transactions")

        print("    Successfully imported and committed to database.")

    except Exception as e:
        print(f"    ERROR processing file {os.path.basename(file_path)}: {e}")
        db.rollback()
        print("    Database transaction rolled back.")


def bulk_upload_local():
    print("--- Starting Local Bulk Upload ---")
    db = SessionLocal()

    try:
        for base_dir in [STATEMENTS_DIR, CSV_DIR]:
            if not os.path.exists(base_dir):
                print(f"Directory not found, skipping: {base_dir}")
                continue

            print(f"\nProcessing directory: {base_dir}")
            for institution_folder in os.listdir(base_dir):
                institution_path = os.path.join(base_dir, institution_folder)
                if os.path.isdir(institution_path):
                    account_id = ACCOUNT_MAPPING.get(institution_folder)
                    if account_id is None:
                        print(f"Warning: No account mapping found for '{institution_folder}'. Skipping.")
                        continue

                    print(f"Processing folder: '{institution_folder}' with account_id: {account_id}")
                    for filename in os.listdir(institution_path):
                        file_path = os.path.join(institution_path, filename)
                        if os.path.isfile(file_path) and (filename.lower().endswith('.pdf') or filename.lower().endswith('.csv')):
                            process_local_file(db, file_path, institution_folder, account_id, USER_ID)
    finally:
        db.close()
        print("\n--- Local Bulk Upload Finished ---")


if __name__ == "__main__":
    bulk_upload_local()
