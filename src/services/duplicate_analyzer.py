from typing import Dict, List, Tuple, Optional, Any
from sqlalchemy.orm import Session, joinedload

from src.db.core import TransactionDB, InvestmentTransactionDB, TransactionType
from src.crud.crud_transaction import generate_transaction_hash
from src.crud.crud_investment import generate_investment_transaction_hash, map_transaction_type_to_enum
from src.parser.models import ParsedTransaction, ParsedInvestmentTransaction
from src.logging_config import get_logger

logger = get_logger(__name__)


def _hash_regular_transaction(
    parsed_txn: ParsedTransaction,
    user_id: int,
    institution_name: str,
) -> str:
    """
    Build a SHA-256 hash for a parsed regular transaction.
    Delegates to the consolidated generate_transaction_hash().
    """
    txn_type_value = TransactionType[parsed_txn.transaction_type.upper()].value
    return generate_transaction_hash(
        user_id=user_id,
        institution_name=institution_name,
        transaction_date=parsed_txn.transaction_date,
        transaction_type_value=txn_type_value,
        amount=parsed_txn.amount,
        description=parsed_txn.description,
    )


def _serialize_existing_transaction(txn: TransactionDB) -> Dict:
    """Serialize an existing DB transaction for the preview response."""
    return {
        "id": str(txn.id),
        "transaction_date": str(txn.transaction_date),
        "amount": str(txn.amount),
        "description": txn.description,
        "transaction_type": txn.transaction_type.value,
        "institution_name": txn.account.institution_name if txn.account else None,
        "created_at": txn.created_at.isoformat() if txn.created_at else None,
    }


def _serialize_existing_investment_transaction(txn: InvestmentTransactionDB) -> Dict:
    """Serialize an existing DB investment transaction for the preview response."""
    return {
        "id": str(txn.id),
        "transaction_date": str(txn.transaction_date),
        "transaction_type": txn.transaction_type.value,
        "symbol": txn.symbol,
        "quantity": str(txn.quantity) if txn.quantity else None,
        "price_per_share": str(txn.price_per_share) if txn.price_per_share else None,
        "total_amount": str(txn.total_amount),
        "description": txn.description,
        "created_at": txn.created_at.isoformat() if txn.created_at else None,
    }


def analyze_regular_transactions(
    transactions: List[ParsedTransaction],
    user_id: int,
    institution_name: str,
    account_id: Optional[int],
    db: Session,
) -> Tuple[List[Dict], List[Dict]]:
    """
    Analyze regular transactions for duplicates.

    Returns:
        (rejected_items, ready_to_import_items)
        Each item is a dict suitable for JSON serialization and Redis storage.
    """
    if not transactions:
        return [], []

    # Pre-fetch all existing transaction hashes for this user
    existing_hashes: Dict[str, TransactionDB] = {
        t.transaction_hash: t
        for t in db.query(TransactionDB)
        .options(joinedload(TransactionDB.account))
        .filter(TransactionDB.user_id == user_id)
        .all()
    }

    within_statement: Dict[str, Dict] = {}
    rejected = []
    ready_to_import = []

    for i, parsed_txn in enumerate(transactions):
        temp_id = f"txn_{i:04d}"

        try:
            base_hash = _hash_regular_transaction(parsed_txn, user_id, institution_name)
        except KeyError:
            logger.warning(f"Unknown transaction type '{parsed_txn.transaction_type}' — flagging as rejected")
            rejected.append({
                "temp_id": temp_id,
                "review_status": "rejected",
                "is_duplicate": False,
                "duplicate_type": "unmapped_type",
                "duplicate_info": {
                    "duplicate_type": "unmapped_type",
                    "reason": f"Unknown transaction type: '{parsed_txn.transaction_type}'"
                },
                "parsed_data": {
                    "transaction_date": str(parsed_txn.transaction_date),
                    "amount": str(parsed_txn.amount),
                    "description": parsed_txn.description,
                    "transaction_type": parsed_txn.transaction_type.upper(),
                    "account_id": account_id,
                },
                "edited_data": None,
                "base_hash": None,
                "statement_position": i,
                "transaction_kind": "regular",
            })
            continue

        parsed_data = {
            "transaction_date": str(parsed_txn.transaction_date),
            "amount": str(parsed_txn.amount),
            "description": parsed_txn.description,
            "transaction_type": parsed_txn.transaction_type.upper(),
            "account_id": account_id,
        }

        is_db_duplicate = base_hash in existing_hashes
        is_within_statement_duplicate = base_hash in within_statement

        if is_db_duplicate and is_within_statement_duplicate:
            duplicate_type = "both"
        elif is_db_duplicate:
            duplicate_type = "database"
        elif is_within_statement_duplicate:
            duplicate_type = "within_statement"
        else:
            duplicate_type = None

        if duplicate_type is not None:
            duplicate_info: Dict[str, Any] = {
                "duplicate_type": duplicate_type,
            }
            if is_db_duplicate:
                existing_txn = existing_hashes[base_hash]
                duplicate_info["existing_transaction"] = _serialize_existing_transaction(existing_txn)
                duplicate_info["existing_transaction_id"] = str(existing_txn.id)
            if is_within_statement_duplicate:
                first = within_statement[base_hash]
                duplicate_info["first_occurrence_temp_id"] = first["temp_id"]
                duplicate_info["first_occurrence_position"] = first["position"]
                duplicate_info["statement_position"] = i

            rejected.append({
                "temp_id": temp_id,
                "review_status": "rejected",
                "is_duplicate": True,
                "duplicate_type": duplicate_type,
                "duplicate_info": duplicate_info,
                "parsed_data": parsed_data,
                "edited_data": None,
                "base_hash": base_hash,
                "statement_position": i,
                "transaction_kind": "regular",
            })
        else:
            ready_to_import.append({
                "temp_id": temp_id,
                "is_duplicate": False,
                "parsed_data": parsed_data,
                "edited_data": None,
                "base_hash": base_hash,
                "statement_position": i,
                "transaction_kind": "regular",
            })

        # Track first occurrence for within-statement detection
        if base_hash not in within_statement:
            within_statement[base_hash] = {"temp_id": temp_id, "position": i}

    logger.info(
        f"Regular txn analysis: {len(rejected)} rejected, "
        f"{len(ready_to_import)} ready to import"
    )
    return rejected, ready_to_import


def analyze_investment_transactions(
    transactions: List[ParsedInvestmentTransaction],
    user_id: int,
    institution_name: str,
    account_id: Optional[int],
    db: Session,
) -> Tuple[List[Dict], List[Dict]]:
    """
    Analyze investment transactions for duplicates.
    Same structure as analyze_regular_transactions but uses investment hash function.
    """
    if not transactions:
        return [], []

    # Pre-fetch existing investment transaction hashes
    existing_hashes: Dict[str, InvestmentTransactionDB] = {
        t.transaction_hash: t
        for t in db.query(InvestmentTransactionDB)
        .filter(InvestmentTransactionDB.user_id == user_id)
        .all()
    }

    within_statement: Dict[str, Dict] = {}
    rejected = []
    ready_to_import = []

    for i, parsed_txn in enumerate(transactions):
        temp_id = f"inv_{i:04d}"
        base_hash = generate_investment_transaction_hash(parsed_txn, user_id, institution_name)

        parsed_data = {
            "transaction_date": str(parsed_txn.transaction_date),
            "transaction_type": parsed_txn.transaction_type.upper(),
            "symbol": parsed_txn.symbol,
            "api_symbol": parsed_txn.api_symbol,
            "description": parsed_txn.description,
            "quantity": str(parsed_txn.quantity) if parsed_txn.quantity is not None else None,
            "price_per_share": str(parsed_txn.price_per_share) if parsed_txn.price_per_share is not None else None,
            "total_amount": str(parsed_txn.total_amount),
            "security_type": parsed_txn.security_type.value if parsed_txn.security_type else None,
            "account_id": account_id,
        }

        # Flag unmapped investment transaction types
        if not map_transaction_type_to_enum(parsed_txn.transaction_type):
            logger.warning(f"Unknown investment transaction type '{parsed_txn.transaction_type}' — flagging as rejected")
            rejected.append({
                "temp_id": temp_id,
                "review_status": "rejected",
                "is_duplicate": False,
                "duplicate_type": "unmapped_type",
                "duplicate_info": {
                    "duplicate_type": "unmapped_type",
                    "reason": f"Unknown investment transaction type: '{parsed_txn.transaction_type}'"
                },
                "parsed_data": parsed_data,
                "edited_data": None,
                "base_hash": base_hash,
                "statement_position": i,
                "transaction_kind": "investment",
            })
            continue

        is_db_duplicate = base_hash in existing_hashes
        is_within_statement_duplicate = base_hash in within_statement

        if is_db_duplicate and is_within_statement_duplicate:
            duplicate_type = "both"
        elif is_db_duplicate:
            duplicate_type = "database"
        elif is_within_statement_duplicate:
            duplicate_type = "within_statement"
        else:
            duplicate_type = None

        if duplicate_type is not None:
            duplicate_info: Dict[str, Any] = {"duplicate_type": duplicate_type}
            if is_db_duplicate:
                existing_txn = existing_hashes[base_hash]
                duplicate_info["existing_transaction"] = _serialize_existing_investment_transaction(existing_txn)
                duplicate_info["existing_transaction_id"] = str(existing_txn.id)
            if is_within_statement_duplicate:
                first = within_statement[base_hash]
                duplicate_info["first_occurrence_temp_id"] = first["temp_id"]
                duplicate_info["first_occurrence_position"] = first["position"]
                duplicate_info["statement_position"] = i

            rejected.append({
                "temp_id": temp_id,
                "review_status": "rejected",
                "is_duplicate": True,
                "duplicate_type": duplicate_type,
                "duplicate_info": duplicate_info,
                "parsed_data": parsed_data,
                "edited_data": None,
                "base_hash": base_hash,
                "statement_position": i,
                "transaction_kind": "investment",
            })
        else:
            ready_to_import.append({
                "temp_id": temp_id,
                "is_duplicate": False,
                "parsed_data": parsed_data,
                "edited_data": None,
                "base_hash": base_hash,
                "statement_position": i,
                "transaction_kind": "investment",
            })

        if base_hash not in within_statement:
            within_statement[base_hash] = {"temp_id": temp_id, "position": i}

    logger.info(
        f"Investment txn analysis: {len(rejected)} rejected, "
        f"{len(ready_to_import)} ready to import"
    )
    return rejected, ready_to_import
