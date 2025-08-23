import csv
import pdfplumber
import re
from pathlib import Path
from decimal import Decimal, InvalidOperation
from datetime import datetime
from typing import List, Optional, Union, IO

from src.parser.models import ParsedData, ParsedInvestmentTransaction, ParsedAccountInfo

def _parse_date(date_str: str) -> Optional[datetime.date]:
    """Parses a date string like 'MM/DD/YY' or 'MM/DD/YYYY'."""
    for fmt in ("%m/%d/%y", "%m/%d/%Y"):
        try:
            return datetime.strptime(date_str, fmt).date()
        except ValueError:
            pass
    print(f"Could not parse date: {date_str}")
    return None

def parse_statement(file_source: Union[Path, IO[bytes]]) -> ParsedData:
    """Parses a TD Ameritrade PDF statement from a file path or in-memory stream."""
    print("Parsing investment transaction data from TD Ameritrade statement...")
    investment_transactions: List[ParsedInvestmentTransaction] = []
    account_info: Optional[ParsedAccountInfo] = None

    text = ''
    with pdfplumber.open(file_source) as pdf:
        for page in pdf.pages:
            text += page.extract_text(x_tolerance=2) or ''
    lines = text.split('\n')

    # Find account number
    for line in lines:
        if "Account Number" in line:
            try:
                # Attempt to find a number like XXXX-XXXX
                match = re.search(r'(\d{4}-\d{4})', line)
                if match:
                    account_number = match.group(1)[-4:]
                    account_info = ParsedAccountInfo(account_number_last4=account_number)
                    break
            except (IndexError, ValueError):
                continue

    # Regex to capture a typical transaction line
    # Format: DATE  JNL  DESCRIPTION  SYMBOL  QUANTITY  PRICE  AMOUNT
    transaction_regex = re.compile(r'^(\d{2}/\d{2}/\d{2})\s+.*?\s+([A-Z&\s]+?)\s+([A-Z]{1,5})?\s+(-?[\d,]+\.\d+)?\s+\$?([\d,]+\.\d+)?\s+\$?(-?[\d,]+\.\d+)')

    in_activity_section = False
    for line in lines:
        if "Account Activity" in line:
            in_activity_section = True
            continue
        if "TOTAL" in line or "Beginning Balance" in line:
            in_activity_section = False
            continue

        if not in_activity_section:
            continue

        match = transaction_regex.match(line)
        if match:
            try:
                date_str, description, symbol, quantity_str, price_str, amount_str = match.groups()
                
                parsed_date = _parse_date(date_str)
                if not parsed_date:
                    continue

                # Infer transaction type from description
                action = description.strip()

                investment_transactions.append(
                    ParsedInvestmentTransaction(
                        transaction_date=parsed_date,
                        transaction_type=action,
                        symbol=symbol.strip() if symbol else None,
                        description=description.strip(),
                        quantity=Decimal(quantity_str.replace(',','')) if quantity_str else None,
                        price_per_share=Decimal(price_str.replace(',','')) if price_str else None,
                        total_amount=Decimal(amount_str.replace(',',''))
                    )
                )
            except (ValueError, InvalidOperation, IndexError) as e:
                print(f"Skipping row in TD Ameritrade statement due to parsing error: {line} -> {e}")
                continue

    return ParsedData(account_info=account_info, investment_transactions=investment_transactions)
