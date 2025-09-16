import os
import sys
import io
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

# --- Add project root to Python path ---
# This allows us to import modules from the 'src' directory.
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.join(SCRIPT_DIR, "..")
sys.path.append(PROJECT_ROOT)

# --- Project Imports ---
# These are now possible because of the sys.path modification above.
from src.db.core import DATABASE_URL
from src.services.importer import PARSER_MAPPING
from src.crud import crud_transaction, crud_investment, crud_account

# --- Configuration ---
INPUT_DIR = os.path.join(PROJECT_ROOT, "input")
STATEMENTS_DIR = os.path.join(INPUT_DIR, "statements")
CSV_DIR = os.path.join(INPUT_DIR, "transaction_csv")

# --- User and Account Configuration ---
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
    # Add other mappings here as needed
}

# --- Database Setup ---
engine = create_engine(DATABASE_URL, echo=False)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

def process_local_file(db, file_path, institution, account_id, user_id):
    """
    Processes a single local file by parsing it and importing the data into the database.
    """
    print(f"  Processing file: {os.path.basename(file_path)}")
    parser = PARSER_MAPPING.get(institution.lower())
    if not parser:
        print(f"    Warning: No parser found for institution '{institution}'. Skipping.")
        return

    try:
        with open(file_path, 'rb') as f:
            # The parsers expect a file-like object.
            is_csv = file_path.lower().endswith('.csv')
            # Convert BufferedReader to BytesIO for parser compatibility
            file_content = f.read()
            file_obj = io.BytesIO(file_content)
            parsed_data = parser.parse(file_obj, is_csv=is_csv)

        # Import standard transactions
        if parsed_data.transactions:
            print(f"    Found {len(parsed_data.transactions)} standard transactions. Importing...")
            created_transactions = crud_transaction.bulk_create_transactions_from_parsed_data(
                db=db,
                user_id=user_id,
                transactions=parsed_data.transactions,
                institution_name=institution,
                account_id=account_id
            )
            print(f"    Successfully inserted {len(created_transactions)} standard transactions to database")

        # Import investment transactions
        if parsed_data.investment_transactions:
            print(f"    Found {len(parsed_data.investment_transactions)} investment transactions. Importing...")
            created_investment_transactions = crud_investment.bulk_create_investment_transactions_from_parsed_data(
                db=db,
                user_id=user_id,
                transactions=parsed_data.investment_transactions,
                institution_name=institution,
                account_id=account_id
            )
            print(f"    Successfully inserted {len(created_investment_transactions)} investment transactions to database")
        
        db.commit()
        print("    Successfully imported and committed to database.")

    except Exception as e:
        print(f"    ERROR processing file {os.path.basename(file_path)}: {e}")
        db.rollback()
        print("    Database transaction rolled back.")


def bulk_upload_local():
    """
    Iterates through input directories, parsing files and importing data directly into the database.
    """
    print("--- Starting Local Bulk Upload ---")
    db = SessionLocal()

    try:
        # Process both statements and CSV directories
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
