import csv
import pdfplumber
from pathlib import Path
from decimal import Decimal, InvalidOperation
from datetime import datetime
from typing import List, Optional, IO, Union
import io
from itertools import groupby

from src.parser.models import ParsedData, ParsedTransaction, ParsedAccountInfo

# A list of month prefixes to identify transaction lines
DATES = ['01/', '02/', '03/', '04/', '05/', '06/', '07/', '08/', '09/', '10/', '11/', '12/']

def _handle_duplicates(transactions: List[ParsedTransaction]) -> List[ParsedTransaction]:
    """
    Handles duplicate transactions by appending a counter to the description.
    """
    updated_transactions = []
    keyfunc = lambda t: (t.transaction_date, t.amount, t.description)
    
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
                        ParsedTransaction(
                            transaction_date=transaction.transaction_date,
                            description=new_description,
                            amount=transaction.amount,
                            transaction_type=transaction.transaction_type,
                            is_duplicate=True
                        )
                    )
        else:
            # No duplicates for this key
            updated_transactions.append(group_list[0])
            
    return updated_transactions

def _map_transaction_type(line: str, keywords: dict) -> List[bool]:
    """Determines the type of transactions being tracked based on section headers."""
    if line.startswith(keywords['payments']):
        return [True, False, False, False, False]
    elif line.startswith(keywords['credits']):
        return [False, True, False, False, False]
    elif line.startswith(keywords['purchases']):
        return [False, False, True, False, False]
    elif line.startswith(keywords['fees']):
        return [False, False, False, True, False]
    elif line.startswith(keywords['interest']):
        return [False, False, False, False, True]
    return [False] * 5

def _parse_date(date_str: str, year_map: dict) -> Optional[datetime.date]:
    """Parses a date string like 'MM/DD/YY' or 'MM/DD' using a year map."""
    try:
        month_day = date_str.split(' ')[0].replace("*", "")
        if len(month_day.split('/')) == 3:
            return datetime.strptime(month_day, "%m/%d/%y").date()
        month = month_day.split('/')[0]
        # Find the year from the map, falling back to the first available year if needed
        year = year_map.get(month, list(year_map.values())[0] if year_map else str(datetime.now().year))
        return datetime.strptime(f"{month_day}/{year}", "%m/%d/%Y").date()
    except (ValueError, IndexError) as e:
        print(f"Could not parse date: {date_str} - Error: {e}")
        return None

def parse_statement(file_source: Union[Path, IO[bytes]]) -> ParsedData:
    """Parses an Amex PDF statement from a file path or an in-memory stream."""
    print(f"Parsing transaction data from Amex statement...")
    parsed_transactions: List[ParsedTransaction] = []
    account_number: Optional[str] = None
    year_map = {}

    text = ''
    with pdfplumber.open(file_source) as pdf:
        for page in pdf.pages:
            text += page.extract_text(x_tolerance=2) or ''
    lines = text.split('\n')

    # First pass to find account number and date range to establish the year
    for i, line in enumerate(lines):
        if "Account Ending" in line or "Account #" in line:
            if not account_number:
                account_number = line.split('-')[-1].strip()
        if "Statement period" in line or "Closing date" in line:
            try:
                date_parts = line.replace(",", "").split()
                for part in date_parts:
                    if len(part) == 4 and part.isdigit():
                        year = part
                        for m in DATES:
                            year_map[m.strip('/')] = year
                        break
            except Exception: continue

    if not year_map:
        print("Warning: Could not determine year from statement. Using current year as fallback.")
        current_year = str(datetime.now().year)
        for m in DATES:
            year_map[m.strip('/')] = current_year

    tracking_payments, tracking_credits, tracking_purchases, tracking_fees, tracking_interest = [False] * 5
    parse_keywords = {
        'payments': "Payments Details",
        'credits': "Credits Details",
        'purchases': "New Charges Details",
        'fees': "Fees",
        'interest': "Interest Charged"
    }

    for line in lines:
        if any(line.startswith(prefix) for prefix in parse_keywords.values()):
            tracking_payments, tracking_credits, tracking_purchases, tracking_fees, tracking_interest = _map_transaction_type(line, parse_keywords)
            continue

        if not line or line[0:3] not in DATES:
            continue

        try:
            line_split = line.split()
            date_str = line_split[0]
            parsed_date = _parse_date(date_str, year_map)
            if not parsed_date:
                continue

            amount_str = line_split[-1].replace("$", "").replace("⧫", "")
            amount = Decimal(amount_str)
            description = " ".join(line_split[1:-1])

            transaction_type = ""
            if tracking_payments: transaction_type = "Payment"
            elif tracking_credits: transaction_type = "Credit"
            elif tracking_purchases: transaction_type = "Purchase"
            elif tracking_fees: transaction_type = "Fee"
            elif tracking_interest: transaction_type = "Interest"

            if transaction_type:
                parsed_transactions.append(
                    ParsedTransaction(
                        transaction_date=parsed_date,
                        description=description.strip(),
                        amount=amount,
                        transaction_type=transaction_type
                    )
                )
        except (ValueError, InvalidOperation, IndexError) as e:
            print(f"Skipping a row in AMEX statement due to parsing error: {line} -> {e}")
            continue

    parsed_transactions = _handle_duplicates(parsed_transactions)
    account_info = ParsedAccountInfo(account_number_last4=account_number.replace("-", "")) if account_number else None
    
    return ParsedData(
        account_info=account_info,
        transactions=parsed_transactions
    )

def parse_csv(file_source: Union[Path, IO[bytes]]) -> ParsedData:
    """Parses an Amex CSV from a file path or an in-memory stream."""
    print(f"Parsing transaction data from AMEX csv...")
    parsed_transactions: List[ParsedTransaction] = []
    account_info: Optional[ParsedAccountInfo] = None # CSVs don't contain account info

    # The CSV reader needs a text-based stream, not a byte stream
    text_stream = io.TextIOWrapper(file_source, encoding='utf-8') if isinstance(file_source, io.BytesIO) else open(file_source, 'r')

    reader = csv.reader(text_stream)
    next(reader)  # Skip header row

    for row in reader:
        try:
            date = datetime.strptime(row[0], "%m/%d/%Y").date()
            description = row[1]
            amount = Decimal(row[2])

            transaction_type = 'Credit' if amount < 0 else 'Purchase'
            amount = abs(amount) # Amount should always be positive

            parsed_transactions.append(
                ParsedTransaction(
                    transaction_date=date,
                    description=description.strip(),
                    amount=amount,
                    transaction_type=transaction_type
                )
            )
        except (ValueError, InvalidOperation, IndexError) as e:
            print(f"Skipping a row in AMEX CSV due to parsing error: {row} -> {e}")
            continue
    
    if isinstance(file_source, Path):
        text_stream.close()
    
    parsed_transactions = _handle_duplicates(parsed_transactions)
    return ParsedData(transactions=parsed_transactions, account_info=account_info)

def parse(file_source: Union[Path, IO[bytes]], is_csv: bool = False) -> ParsedData:
    """
    Parses a Amex statement (PDF or CSV) from a file path or in-memory stream.
    """
    if is_csv:
        return parse_csv(file_source)
    else:
        return parse_statement(file_source)
