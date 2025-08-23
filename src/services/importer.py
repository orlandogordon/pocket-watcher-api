from sqlalchemy.orm import Session
from typing import Optional
import io

from src.services import s3
from src.parser import (
    amex,
    tdbank,
    amzn_syf,
    schwab,
    tdameritrade,
    ameriprise,
    # empower and fidelity are not ready yet
)
from src.crud import crud_transaction, crud_investment, crud_account
from src.db.core import get_db, NotFoundError

# A mapping from the institution string to the corresponding parser module
PARSER_MAPPING = {
    "amex": amex,
    "tdbank": tdbank,
    "amzn-synchrony": amzn_syf,
    "schwab": schwab,
    "tdameritrade": tdameritrade,
    "ameriprise": ameriprise,
}


def process_statement(
    db: Session, 
    user_id: int, 
    s3_key: str, 
    institution: str,
    file_content_type: str,
    account_id: Optional[int] = None
):
    """
    Background task to process a financial statement from S3.

    - Downloads the file from S3.
    - Uses the appropriate parser based on the institution.
    - Determines the account for the transactions.
    - Bulk-creates transactions and investment transactions.
    - Cleans up the S3 file after processing.
    """
    print(f"--- Starting background task for user {user_id}, file {s3_key} ---")

    parser = PARSER_MAPPING.get(institution.lower())
    if not parser:
        print(f"Error: No parser found for institution '{institution}'.")
        # Optionally, update a status in the DB to reflect the failure
        return

    file_obj = None
    try:
        # 1. Download file from S3 into an in-memory buffer
        file_obj = io.BytesIO()
        s3.download_file_from_s3(bucket=s3.get_s3_bucket(), object_name=s3_key, file_obj=file_obj)
        file_obj.seek(0) # Reset buffer position to the beginning

        # 2. Parse the file
        is_csv = file_content_type == 'text/csv'
        parsed_data = parser.parse(file_obj, is_csv=is_csv)

        # 3. Determine the final account_id
        final_account_id = account_id
        if not final_account_id and parsed_data.account_info and parsed_data.account_info.account_number:
            try:
                # Attempt to find the account in the DB based on parsed info
                found_account = crud_account.get_db_account_by_last_four(
                    db, user_id=user_id, last_four=parsed_data.account_info.account_number[-4:]
                )
                if found_account:
                    final_account_id = found_account.id
            except NotFoundError:
                print(f"Account with last four digits {parsed_data.account_info.account_number[-4:]} not found.")
                # The transactions will be created with account_id=None and needs_review=True

        # 4. Create Transactions
        if parsed_data.transactions:
            print(f"Importing {len(parsed_data.transactions)} standard transactions...")
            crud_transaction.bulk_create_transactions_from_parsed_data(
                db=db,
                user_id=user_id,
                transactions=parsed_data.transactions,
                institution_name=institution,
                account_id=final_account_id
            )

        # 5. Create Investment Transactions
        if parsed_data.investment_transactions:
            print(f"Importing {len(parsed_data.investment_transactions)} investment transactions...")
            crud_investment.bulk_create_investment_transactions_from_parsed_data(
                db=db,
                user_id=user_id,
                transactions=parsed_data.investment_transactions,
                institution_name=institution,
                account_id=final_account_id
            )

        print("--- Data import successful ---")

    except Exception as e:
        print(f"Error processing statement {s3_key}: {e}")
        # In a real-world scenario, you might move the file to a 'failed' folder
        # instead of deleting it, and log the error to a monitoring system.
    
    finally:
        # 6. Cleanup: Close the buffer and delete the file from S3
        if file_obj:
            file_obj.close()
        try:
            s3.delete_file_from_s3(bucket=s3.get_s3_bucket(), object_name=s3_key)
            print(f"Successfully cleaned up S3 object {s3_key}.")
        except Exception as e:
            print(f"Error during S3 cleanup for {s3_key}: {e}")

    print(f"--- Background task for {s3_key} finished. ---")
