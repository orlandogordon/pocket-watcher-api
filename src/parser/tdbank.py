import csv
import re
import pdfplumber
from pathlib import Path
from decimal import Decimal, InvalidOperation
from datetime import datetime
from typing import List, Optional, Union, IO
import io

from src.parser.models import ParsedData, ParsedTransaction, ParsedAccountInfo


DATES = {
    'Jan': '01', 'Feb': '02', 'Mar': '03', 'Apr': '04', 'May': '05', 'Jun': '06', 
    'Jul': '07', 'Aug': '08', 'Sep': '09', 'Oct': '10', 'Nov': '11', 'Dec': '12'
}

def _parse_date_from_month_day(month_day: str, year_map: dict) -> Optional[datetime.date]:
    """Parses a MM/DD date string using a year map like {'01': '2023'}."""
    try:
        month = month_day.split('/')[0]
        year = year_map.get(month)
        if not year:
            return None
        return datetime.strptime(f"{month}/{year}", "%m/%d/%Y").date()
    except (ValueError, IndexError):
        return None

def parse_statement(file_source: Union[Path, IO[bytes]]) -> ParsedData:
    """Parses a TD Bank PDF statement from a file path or in-memory stream."""
    print("Parsing transaction data from TD Bank statement...")
    transactions: List[ParsedTransaction] = []
    account_number: Optional[str] = None
    year_map = {}

    with pdfplumber.open(file_source) as pdf:
        # Extract account number and statement period from the first page
        first_page_text = pdf.pages[0].extract_text()
        for line in first_page_text.split('\n'):
            if "Account #" in line:
                account_number = line.split('#')[-1].strip()
            elif "Statement Period:" in line:
                # Extracts years and maps them to months, e.g., {'01': '2023', '02': '2023'}
                match = re.search(r'(\d{1,2}/\d{1,2}/\d{4}) - (\d{1,2}/\d{1,2}/\d{4})', line)
                if match:
                    start_date = datetime.strptime(match.group(1), '%m/%d/%Y')
                    end_date = datetime.strptime(match.group(2), '%m/%d/%Y')
                    year_map[f"{start_date.month:02d}"] = str(start_date.year)
                    year_map[f"{end_date.month:02d}"] = str(end_date.year)

        # Find transaction tables across all pages
        for page in pdf.pages:
            # Use pdfplumber's table finding with explicit settings for TD Bank statements
            tables = page.find_tables({
                "vertical_strategy": "lines",
                "horizontal_strategy": "text",
                "snap_y_tolerance": 5,
                "join_y_tolerance": 5,
            })
            for table in tables:
                for row in table.extract():
                    if not row or not row[0] or not re.match(r'^\d{2}/\d{2}$', row[0]):
                        continue # Skip headers/invalid rows
                    
                    try:
                        date_str, description, amount_str = row[0], row[1], row[2]
                        if not description or not amount_str:
                            continue

                        parsed_date = _parse_date_from_month_day(date_str, year_map)
                        if not parsed_date:
                            continue
                        
                        # Amount can be in a combined column, split it
                        if ' ' in amount_str:
                            amount_str = amount_str.split(' ')[-1]
                        
                        amount = Decimal(amount_str.replace("$", "").replace(",", ""))
                        
                        # Determine transaction type based on which section it's in (heuristic)
                        # This is simplified; a more robust method would check section headers.
                        transaction_type = "Deposit" if amount > 0 else "Purchase"

                        transactions.append(
                            ParsedTransaction(
                                transaction_date=parsed_date,
                                description=description.strip().replace('\n', ' '),
                                amount=amount,
                                transaction_type=transaction_type
                            )
                        )
                    except (ValueError, InvalidOperation, IndexError) as e:
                        print(f"Skipping row in TD Bank statement due to parsing error: {row} -> {e}")
                        continue
        breakpoint()
    account_info = ParsedAccountInfo(account_number_last4=account_number[-4:]) if account_number else None
    return ParsedData(account_info=account_info, transactions=transactions)

def parse_csv(file_source: Union[Path, IO[bytes]]) -> ParsedData:
    """Parses a TD Bank CSV from a file path or in-memory stream."""
    print("Parsing transaction data from TD Bank csv...")
    parsed_transactions: List[ParsedTransaction] = []
    # Account number is not available in TD Bank CSVs, so account_info is None
    account_info: Optional[ParsedAccountInfo] = None

    text_stream = io.TextIOWrapper(file_source, encoding='utf-8') if isinstance(file_source, io.BytesIO) else open(file_source, 'r')
    reader = csv.reader(text_stream)
    next(reader) # Skip header

    for row in reader:
        try:
            date = datetime.strptime(row[0], "%Y-%m-%d").date()
            description = row[4]
            debit = row[5]
            credit = row[6]

            if credit:
                amount = Decimal(credit)
                transaction_type = 'Deposit'
            else:
                amount = Decimal(debit)
                transaction_type = 'Purchase'

            parsed_transactions.append(
                ParsedTransaction(
                    transaction_date=date,
                    description=description.strip(),
                    amount=amount,
                    transaction_type=transaction_type
                )
            )
        except (ValueError, InvalidOperation, IndexError) as e:
            print(f"Skipping row in TD Bank CSV due to parsing error: {row} -> {e}")
            continue

    if isinstance(file_source, Path):
        text_stream.close()

    return ParsedData(transactions=parsed_transactions, account_info=account_info)

def parse(file_source: Union[Path, IO[bytes]], is_csv: bool = False) -> ParsedData:
    """
    Parses a TD Bank statement (PDF or CSV) from a file path or in-memory stream.
    """
    if is_csv:
        return parse_csv(file_source)
    else:
        return parse_statement(file_source)