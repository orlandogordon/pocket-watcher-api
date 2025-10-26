import csv
from pathlib import Path
from decimal import Decimal, InvalidOperation
from datetime import datetime
from typing import List, Optional, Union, IO
import io
from itertools import groupby

from src.parser.models import ParsedData, ParsedInvestmentTransaction, ParsedAccountInfo, SecurityType
from src.logging_config import get_logger

logger = get_logger(__name__)

def _parse_date(date_str: str) -> Optional[datetime.date]:
    """Parses a date string like 'MM/DD/YYYY'."""
    try:
        return datetime.strptime(date_str, "%m/%d/%Y").date()
    except ValueError:
        logger.warning(f"Could not parse date: {date_str}")
        return None

def _handle_investment_duplicates(transactions: List[ParsedInvestmentTransaction]) -> List[ParsedInvestmentTransaction]:
    """
    Handles duplicate investment transactions by appending a counter to the description.
    Groups by date, transaction_type, symbol, and description.
    """
    updated_transactions = []
    keyfunc = lambda t: (t.transaction_date, t.transaction_type, t.symbol, t.description)

    sorted_transactions = sorted(transactions, key=keyfunc)

    for key, group in groupby(sorted_transactions, key=keyfunc):
        group_list = list(group)
        if len(group_list) > 1:
            # Duplicates found
            for i, transaction in enumerate(group_list):
                if i == 0:
                    # First one is kept as is
                    updated_transactions.append(transaction)
                else:
                    # Subsequent ones get a modified description
                    new_description = f"{transaction.description} ({i + 1})"
                    updated_transactions.append(
                        ParsedInvestmentTransaction(
                            transaction_date=transaction.transaction_date,
                            transaction_type=transaction.transaction_type,
                            symbol=transaction.symbol,
                            description=new_description,
                            quantity=transaction.quantity,
                            price_per_share=transaction.price_per_share,
                            total_amount=transaction.total_amount,
                            is_duplicate=True
                        )
                    )
        else:
            # No duplicates for this key
            updated_transactions.append(group_list[0])

    return updated_transactions

def parse_csv(file_source: Union[Path, IO[bytes]]) -> ParsedData:
    """Parses an Ameriprise CSV from a file path or in-memory stream."""
    logger.info("Parsing investment transaction data from Ameriprise CSV...")
    investment_transactions: List[ParsedInvestmentTransaction] = []
    account_info: Optional[ParsedAccountInfo] = None
    account_number: Optional[str] = None

    text_stream = io.TextIOWrapper(file_source, encoding='utf-8') if isinstance(file_source, io.BytesIO) else open(file_source, 'r')
    
    # Skip header lines, which can vary
    lines = text_stream.readlines()
    header_index = 0
    for i, line in enumerate(lines):
        if line.strip().startswith('Date,Account'):
            header_index = i
            break
    
    csv_reader = csv.reader(lines[header_index + 1:])

    for row in csv_reader:
        if not row or len(row) < 7:
            continue

        try:
            date_str = row[0]
            if not account_number:
                account_number = row[1][-10:].replace(")", "")
                if account_number:
                    account_info = ParsedAccountInfo(account_number_last4=account_number[-4:])
            
            transaction_type = row[2].split('-')[0].strip()
            description = row[2].split('-')[1].strip() if '-' in row[2] else transaction_type
            amount_str = row[3].replace("$", "").replace("-", "").strip()
            quantity_str = row[4].replace("-", "").strip()
            price_str = row[5].replace("$", "").strip()
            symbol = row[6].strip() or None

            parsed_date = _parse_date(date_str)
            if not parsed_date:
                continue

            investment_transactions.append(
                ParsedInvestmentTransaction(
                    transaction_date=parsed_date,
                    transaction_type=transaction_type,
                    symbol=symbol,
                    description=description,
                    quantity=Decimal(quantity_str) if quantity_str else None,
                    price_per_share=Decimal(price_str) if price_str else None,
                    total_amount=Decimal(amount_str)
                )
            )
        except (ValueError, InvalidOperation, IndexError) as e:
            logger.warning(f"Skipping row in Ameriprise CSV due to parsing error: {row} -> {e}")
            continue

    if isinstance(file_source, Path):
        text_stream.close()

    logger.info(f"Successfully parsed {len(investment_transactions)} investment transactions from Ameriprise CSV")

    # Handle duplicates
    investment_transactions = _handle_investment_duplicates(investment_transactions)

    return ParsedData(account_info=account_info, investment_transactions=investment_transactions)