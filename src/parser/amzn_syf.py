import csv
import pdfplumber
from pathlib import Path
from decimal import Decimal, InvalidOperation
from datetime import datetime
from typing import List, Optional, Union, IO
import io
from itertools import groupby

from src.parser.models import ParsedData, ParsedTransaction, ParsedAccountInfo

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
    """Parses a date string like 'MM/DD' using a year map."""
    try:
        month_str = date_str[0:2]
        year = year_map.get(month_str)
        if not year:
            # Fallback to first year in map if available, otherwise current year.
            # This handles cases where a transaction month is not in the statement period.
            year = list(year_map.values())[0] if year_map else str(datetime.now().year)
        return datetime.strptime(f"{date_str}/{year}", "%m/%d/%Y").date()
    except (ValueError, IndexError):
        return None

def parse_statement(file_source: Union[Path, IO[bytes]]) -> ParsedData:
    """Parses an Amazon Synchrony PDF statement from a file path or in-memory stream."""
    print("Parsing transaction data from Amazon (SYF) statement...")
    parsed_transactions: List[ParsedTransaction] = []
    account_number: Optional[str] = None
    year_map = {}

    text = ''
    with pdfplumber.open(file_source) as pdf:
        for page in pdf.pages:
            text += page.extract_text(x_tolerance=2) or ''
    lines = text.split('\n')

    # First pass to find account number and establish year from statement period
    for line in lines:
        if "Account Number ending in" in line and not account_number:
            account_number = line.split()[-1]
        
        if "Billing Cycle from" in line and not year_map:
            try:
                # Line is like: "31 Day Billing Cycle from 07/27/2024 to 08/26/2024"
                dates_part = line.split(" from ")[1]
                start_date_str, end_date_str = dates_part.split(" to ")
                
                start_month_num = int(start_date_str.split('/')[0])
                start_year = start_date_str.split('/')[2]
                
                end_month_num = int(end_date_str.split('/')[0])
                end_year = end_date_str.split('/')[2]

                if start_year == end_year:
                    for m_num in range(1, 13):
                        year_map[f"{m_num:02d}"] = start_year
                else: # Year boundary crossed
                    for m_num in range(1, 13):
                        if m_num >= start_month_num:
                            year_map[f"{m_num:02d}"] = start_year
                        elif m_num <= end_month_num:
                            year_map[f"{m_num:02d}"] = end_year
                        else:
                            # Default for months outside statement period
                            year_map[f"{m_num:02d}"] = end_year
            except (ValueError, IndexError) as e:
                print(f"Could not parse billing cycle line: {line} -> {e}")
                continue
        
        if account_number and year_map:
            break


    tracking_payments, tracking_credits, tracking_purchases, tracking_fees, tracking_interest = [False] * 5
    parse_keywords = {
        'payments': 'Payments -$',
        'credits': 'Other Credits -$',
        'purchases': 'Purchases and Other Debits',
        'fees': 'Total Fees Charged This Period',
        'interest': 'Total Interest Charged This Period'
    }
    skip_lines = [
        '(Continued on next page)', 
        'Transaction Detail (Continued)', 
        'Date Reference # Description Amount',
        '2025 Year-to-Date Fees and Interest',
        'Total Fees Charged',
        'Total Interest Charged', 
        'Total Interest Paid',
        'Interest Charge Calculation',
        'Your Annual Percentage Rate',
        'PAGE'  # To stop at page markers
    ]
    section_terminators = [
    'Year-to-Date Fees and Interest',  # This will match any year
    'Interest Charge Calculation', 
    'Your Annual Percentage Rate',
    'New Promotional Financing Plans',
    'Cardholder News and Information',
    'PAGE ',
    'Visit us at',
    'Total Fees Charged',
    'Total Interest Charged',
    'Total Interest Paid'
    ]
    i = 0
    while i < len(lines):
        line = lines[i]
        if any(line.startswith(prefix) for prefix in parse_keywords.values()):
            tracking_payments, tracking_credits, tracking_purchases, tracking_fees, tracking_interest = _map_transaction_type(line, parse_keywords)
            i += 1
            continue

        if not line or line[0:3] not in DATES:
            i += 1
            continue

        try:
            line_split = line.split()
            date_str = line_split[0]
            parsed_date = _parse_date(date_str, year_map)
            if not parsed_date:
                i += 1
                continue

            amount = Decimal(line_split[-1].replace("$", ""))
            description = " ".join(line_split[2:-1]) # Skip date and ref #

            # Handle multi-line descriptions
            while (i + 1) < len(lines) and lines[i+1] and lines[i+1][0:3] not in DATES and not any(lines[i+1].startswith(k) for k in parse_keywords.values()):
                next_line_stripped = lines[i+1].strip()
                
                # Stop if we hit a section terminator
                if any(term in next_line_stripped for term in section_terminators):
                    break
                    
                if not any(lines[i+1].startswith(s) for s in skip_lines):
                    description += " " + next_line_stripped
                i += 1

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
            print(f"Skipping a row in AMZN_SYF statement due to parsing error: {line} -> {e}")
        i += 1

    parsed_transactions = _handle_duplicates(parsed_transactions)
    account_info = ParsedAccountInfo(account_number_last4=account_number[-4:]) if account_number else None
    return ParsedData(account_info=account_info, transactions=parsed_transactions)

def parse_csv(file_source: Union[Path, IO[bytes]]) -> ParsedData:
    """Parses an Amazon Synchrony CSV from a file path or in-memory stream."""
    print("Parsing transaction data from Amazon (SYF) csv...")
    parsed_transactions: List[ParsedTransaction] = []
    account_info: Optional[ParsedAccountInfo] = None

    text_stream = io.TextIOWrapper(file_source, encoding='utf-8') if isinstance(file_source, io.BytesIO) else open(file_source, 'r')
    reader = csv.reader(text_stream)
    next(reader)  # Skip header

    for row in reader:
        try:
            date = datetime.strptime(row[0], "%m/%d/%Y").date()
            description = row[4]
            amount = Decimal(row[3])

            transaction_type = 'Credit/Payment' if amount < 0 else 'Purchase'
            amount = abs(amount)

            parsed_transactions.append(
                ParsedTransaction(
                    transaction_date=date,
                    description=description.strip(),
                    amount=amount,
                    transaction_type=transaction_type
                )
            )
        except (ValueError, InvalidOperation, IndexError) as e:
            print(f"Skipping row in AMZN_SYF CSV due to parsing error: {row} -> {e}")
            continue
    
    if isinstance(file_source, Path):
        text_stream.close()
    for transaction in parsed_transactions:print(transaction)
    
    parsed_transactions = _handle_duplicates(parsed_transactions)
    
    print(f"Total transactions parsed: {len(parsed_transactions)}")
    breakpoint()
    return ParsedData(transactions=parsed_transactions, account_info=account_info)

def parse(file_source: Union[Path, IO[bytes]], is_csv: bool = False) -> ParsedData:
    """
    Parses a Amazon Synchrony statement (PDF or CSV) from a file path or in-memory stream.
    """
    if is_csv:
        return parse_csv(file_source)
    else:
        return parse_statement(file_source)
