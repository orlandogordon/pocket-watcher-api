import csv
from pathlib import Path
from decimal import Decimal, InvalidOperation
from datetime import datetime
from typing import List, Optional, Union, IO
import io

from src.parser.models import ParsedData, ParsedInvestmentTransaction, ParsedAccountInfo

def _parse_date(date_str: str) -> Optional[datetime.date]:
    """Parses a date string like 'MM/DD/YYYY'."""
    try:
        return datetime.strptime(date_str, "%m/%d/%Y").date()
    except ValueError:
        print(f"Could not parse date: {date_str}")
        return None

def parse_csv(file_source: Union[Path, IO[bytes]]) -> ParsedData:
    """Parses an Ameriprise CSV from a file path or in-memory stream."""
    print("Parsing investment transaction data from Ameriprise CSV...")
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
            print(f"Skipping row in Ameriprise CSV due to parsing error: {row} -> {e}")
            continue

    if isinstance(file_source, Path):
        text_stream.close()

    return ParsedData(account_info=account_info, investment_transactions=investment_transactions)