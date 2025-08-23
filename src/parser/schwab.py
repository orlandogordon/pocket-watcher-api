import csv
import re
import pdfplumber
from pathlib import Path
from decimal import Decimal, InvalidOperation
from datetime import datetime
from typing import List, Optional, Union, IO
import io

from src.parser.models import ParsedData, ParsedInvestmentTransaction, ParsedAccountInfo

def _parse_date(date_str: str) -> Optional[datetime.date]:
    """Parses a date string like 'MM/DD/YYYY' or 'MM/DD/YY'."""
    for fmt in ("%m/%d/%Y", "%m/%d/%y"):
        try:
            return datetime.strptime(date_str, fmt).date()
        except ValueError:
            pass
    print(f"Could not parse date: {date_str}")
    return None

def parse_statement(file_source: Union[Path, IO[bytes]]) -> ParsedData:
    """Parses a Schwab PDF statement from a file path or in-memory stream."""
    print("Parsing investment transaction data from Schwab statement...")
    # This function is a placeholder. The current logic is highly complex and stateful,
    # making it difficult to refactor reliably without extensive testing.
    # A full implementation would require a more robust parsing strategy for Schwab PDFs.
    print("WARNING: Schwab PDF parsing is not fully implemented and will return empty data.")
    return ParsedData()

def parse_csv(file_source: Union[Path, IO[bytes]]) -> ParsedData:
    """Parses a Schwab CSV from a file path or in-memory stream."""
    print("Parsing investment transaction data from Schwab csv...")
    investment_transactions: List[ParsedInvestmentTransaction] = []
    account_info: Optional[ParsedAccountInfo] = None

    # The first few lines of a Schwab CSV are metadata, not transactions.
    # We need to find the header row first.
    text_stream = io.TextIOWrapper(file_source, encoding='utf-8') if isinstance(file_source, io.BytesIO) else open(file_source, 'r')
    lines = text_stream.readlines()

    header_index = -1
    for i, line in enumerate(lines):
        if line.strip().startswith('"Date","Action"'):
            header_index = i
            break
    
    if header_index == -1:
        raise ValueError("Could not find transaction header in Schwab CSV.")

    # Account number is usually in the line before the header
    if header_index > 0:
        match = re.search(r'XXXX-(\d{4})', lines[header_index - 1])
        if match:
            account_info = ParsedAccountInfo(account_number_last4=match.group(1))

    # Use the rest of the lines for CSV reading
    csv_reader = csv.reader(lines[header_index + 1:])

    for row in csv_reader:
        if not row or len(row) < 8:
            continue # Skip empty or malformed rows
        
        try:
            date_str = row[0]
            action = row[1]
            symbol = row[2] or None
            description = row[3]
            quantity_str = row[4]
            price_str = row[5]
            amount_str = row[7]

            parsed_date = _parse_date(date_str)
            if not parsed_date:
                continue

            investment_transactions.append(
                ParsedInvestmentTransaction(
                    transaction_date=parsed_date,
                    transaction_type=action,
                    symbol=symbol,
                    description=description,
                    quantity=Decimal(quantity_str) if quantity_str else None,
                    price_per_share=Decimal(price_str.replace('$','')) if price_str else None,
                    total_amount=Decimal(amount_str.replace('$',''))
                )
            )
        except (ValueError, InvalidOperation, IndexError) as e:
            print(f"Skipping row in Schwab CSV due to parsing error: {row} -> {e}")
            continue

    if isinstance(file_source, Path):
        text_stream.close()

    return ParsedData(account_info=account_info, investment_transactions=investment_transactions)

def parse(file_source: Union[Path, IO[bytes]], is_csv: bool = False) -> ParsedData:
    """
    Parses a Schwab statement (PDF or CSV) from a file path or in-memory stream.
    """
    if is_csv:
        return parse_csv(file_source)
    else:
        return parse_statement(file_source)