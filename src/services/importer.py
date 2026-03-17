from sqlalchemy.orm import Session
from typing import Optional
from datetime import datetime
import io
from src.logging_config import get_logger

logger = get_logger(__name__)

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
from src.db.core import get_db, NotFoundError, UploadJobDB, SkippedTransactionDB

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
    upload_job_id: int,
    s3_key: str,
    institution: str,
    file_content_type: str,
    account_id: Optional[int] = None,
    skip_duplicates: bool = True,
):
    """
    Background task to process a financial statement from S3.

    - Downloads the file from S3.
    - Uses the appropriate parser based on the institution.
    - Determines the account for the transactions.
    - Bulk-creates transactions and investment transactions.
    - Tracks upload job progress and results.
    - Cleans up the S3 file after processing.

    Args:
        db: Database session
        user_id: User ID
        upload_job_id: ID of the upload job to track progress
        s3_key: S3 key of the uploaded file
        institution: Institution name
        file_content_type: File MIME type
        account_id: Optional account ID
        skip_duplicates: Whether to skip duplicate transactions
    """
    logger.info(f"Starting background task for user {user_id}, file {s3_key}, job {upload_job_id}")

    # Get the upload job and update status to PROCESSING
    job = db.query(UploadJobDB).get(upload_job_id)
    if not job:
        logger.error(f"Upload job {upload_job_id} not found")
        return

    job.status = "PROCESSING"
    job.started_at = datetime.utcnow()
    db.commit()

    parser = PARSER_MAPPING.get(institution.lower())
    if not parser:
        logger.error(f"No parser found for institution '{institution}'")
        job.status = "FAILED"
        job.error_message = f"No parser found for institution '{institution}'"
        job.completed_at = datetime.utcnow()
        db.commit()
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
        if not final_account_id and parsed_data.account_info and parsed_data.account_info.account_number_last4:
            try:
                # Attempt to find the account in the DB based on parsed info
                found_account = crud_account.get_db_account_by_last_four(
                    db, user_id=user_id, last_four=parsed_data.account_info.account_number_last4
                )
                if found_account:
                    final_account_id = found_account.id
            except NotFoundError:
                logger.warning(f"Account with last four digits {parsed_data.account_info.account_number_last4} not found")

        # 4. Create Transactions
        created_transactions = []
        skipped_transactions = []
        if parsed_data.transactions:
            logger.info(f"Importing {len(parsed_data.transactions)} standard transactions")
            created_transactions, skipped_transactions = crud_transaction.bulk_create_transactions_from_parsed_data(
                db=db,
                user_id=user_id,
                transactions=parsed_data.transactions,
                institution_name=institution,
                account_id=final_account_id,
                skip_duplicates=skip_duplicates
            )
            logger.info(f"Successfully inserted {len(created_transactions)} standard transactions to database")

            # Store skipped transaction details
            for skipped in skipped_transactions:
                parsed_txn = skipped['parsed_transaction']
                existing_txn = skipped['existing_transaction']

                skipped_record = SkippedTransactionDB(
                    upload_job_id=upload_job_id,
                    transaction_type="REGULAR",
                    parsed_date=parsed_txn.transaction_date,
                    parsed_amount=parsed_txn.amount,
                    parsed_description=parsed_txn.description,
                    parsed_transaction_type=parsed_txn.transaction_type,
                    existing_transaction_id=existing_txn.id,
                    parsed_data_json=parsed_txn.dict()
                )
                db.add(skipped_record)
            db.commit()

        # 5. Create Investment Transactions
        created_investment_transactions = []
        skipped_investment_transactions = []
        if parsed_data.investment_transactions:
            logger.info(f"Importing {len(parsed_data.investment_transactions)} investment transactions")
            created_investment_transactions, skipped_investment_transactions, backfill_job_id = crud_investment.bulk_create_investment_transactions_from_parsed_data(
                db=db,
                user_id=user_id,
                transactions=parsed_data.investment_transactions,
                institution_name=institution,
                account_id=final_account_id,
                skip_duplicates=skip_duplicates
            )
            logger.info(f"Successfully inserted {len(created_investment_transactions)} investment transactions to database")

            # Store skipped investment transaction details
            for skipped in skipped_investment_transactions:
                parsed_txn = skipped['parsed_transaction']
                existing_txn = skipped['existing_transaction']

                skipped_record = SkippedTransactionDB(
                    upload_job_id=upload_job_id,
                    transaction_type="INVESTMENT",
                    parsed_date=parsed_txn.transaction_date,
                    parsed_amount=parsed_txn.total_amount,
                    parsed_description=parsed_txn.description,
                    parsed_transaction_type=parsed_txn.transaction_type,
                    parsed_symbol=parsed_txn.symbol,
                    parsed_quantity=parsed_txn.quantity,
                    existing_investment_transaction_id=existing_txn.id,
                    parsed_data_json=parsed_txn.dict()
                )
                db.add(skipped_record)
            db.commit()

        # Update job with results
        job.transactions_created = len(created_transactions)
        job.transactions_skipped = len(skipped_transactions)
        job.investment_transactions_created = len(created_investment_transactions)
        job.investment_transactions_skipped = len(skipped_investment_transactions)
        job.status = "COMPLETED"
        job.completed_at = datetime.utcnow()
        db.commit()

        logger.info(f"Data import successful: {job.transactions_created} transactions, {job.investment_transactions_created} investment transactions created")

    except Exception as e:
        logger.error(f"Error processing statement {s3_key}: {e}")
        # Mark job as failed
        job.status = "FAILED"
        job.error_message = str(e)
        job.completed_at = datetime.utcnow()
        db.commit()

    finally:
        # 6. Cleanup: Close the buffer and delete the file from S3
        if file_obj:
            file_obj.close()
        try:
            s3.delete_file_from_s3(bucket=s3.get_s3_bucket(), object_name=s3_key)
            logger.info(f"Successfully cleaned up S3 object {s3_key}")

            # Clear file_path since file is deleted
            job.file_path = None
            db.commit()
        except Exception as e:
            logger.error(f"Error during S3 cleanup for {s3_key}: {e}")

    logger.info(f"Background task for job {upload_job_id} finished with status: {job.status}")
