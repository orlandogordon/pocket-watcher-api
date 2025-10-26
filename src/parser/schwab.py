import csv
import re
import pdfplumber
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
    """Parses a date string like 'MM/DD/YYYY' or 'MM/DD/YY'."""
    for fmt in ("%m/%d/%Y", "%m/%d/%y"):
        try:
            return datetime.strptime(date_str, fmt).date()
        except ValueError:
            pass
    logger.warning(f"Could not parse date: {date_str}")
    return None

def _classify_security_type(transaction_type: str, symbol: Optional[str]) -> SecurityType:
    """
    Classify the security type for Schwab transactions.

    Schwab transaction types:
    - Purchase, Buy, Sale, Sell → STOCK
    - Interest → INTEREST
    - Dividend → DIVIDEND
    - Fee → FEE
    - Deposit, Transfer (in) → DEPOSIT
    - Withdrawal, Transfer (out) → WITHDRAWAL
    - Split, Merger, Spinoff → OTHER
    """
    txn_type_lower = transaction_type.lower()

    # Securities (stocks) - has a symbol
    if any(word in txn_type_lower for word in ['purchase', 'buy', 'sale', 'sell']) and symbol:
        return SecurityType.STOCK

    # Interest
    if 'interest' in txn_type_lower:
        return SecurityType.INTEREST

    # Dividend
    if 'dividend' in txn_type_lower:
        return SecurityType.DIVIDEND

    # Fee
    if 'fee' in txn_type_lower:
        return SecurityType.FEE

    # Deposits/Transfers in
    if 'deposit' in txn_type_lower or ('transfer' in txn_type_lower and not symbol):
        return SecurityType.DEPOSIT

    # Withdrawals/Transfers out (if amount is negative)
    if 'withdrawal' in txn_type_lower:
        return SecurityType.WITHDRAWAL

    # Corporate actions
    if any(word in txn_type_lower for word in ['split', 'merger', 'spinoff']):
        return SecurityType.OTHER

    # Default
    return SecurityType.OTHER

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
                            is_duplicate=True,
                            security_type=transaction.security_type
                        )
                    )
        else:
            # No duplicates for this key
            updated_transactions.append(group_list[0])

    return updated_transactions

def parse_statement(file_source: Union[Path, IO[bytes]]) -> ParsedData:
    """Parses a Schwab PDF statement from a file path or in-memory stream."""
    logger.info("Parsing investment transaction data from Schwab statement...")
    investment_transactions: List[ParsedInvestmentTransaction] = []
    account_info: Optional[ParsedAccountInfo] = None
    year = str(datetime.now().year)

    text = ''
    with pdfplumber.open(file_source) as pdf:
        for page in pdf.pages:
            text += page.extract_text(x_tolerance=2) or ''
    lines = text.split('\n')

    # First pass: Find account number and year
    for i, line in enumerate(lines):
        # Look for account number like "5976-5634"
        if "AccountNumber" in line or "Account Number" in line:
            # Check next few lines for the account number pattern
            for j in range(i, min(i + 5, len(lines))):
                match = re.search(r'\b(\d{4})-(\d{4})\b', lines[j])
                if match:
                    account_info = ParsedAccountInfo(account_number_last4=match.group(2))
                    break

        # Extract year from statement period
        if "StatementPeriod" in line or "Statement Period" in line:
            # Look for year in format like "February1-28,2025"
            year_match = re.search(r'(19\d{2}|20\d{2})', line)
            if year_match:
                year = year_match.group(1)
                logger.debug(f"Found statement year: {year}")

    tracking_transactions = False
    current_date = None  # Track the last seen date for transactions without dates

    for i, line in enumerate(lines):
        # Look for the "Transaction Details" section header
        if "Transaction Details" in line:
            tracking_transactions = True
            # Skip the column headers (next 2-3 lines)
            continue

        # Stop at certain keywords
        if tracking_transactions and ("TotalTransactions" in line or "Total Transactions" in line or
                                       "Bank Sweep Activity" in line or "Endnotes" in line):
            tracking_transactions = False
            continue

        if not tracking_transactions:
            continue

        # Skip lines that are clearly not transactions
        if not line.strip() or line.strip().startswith('Commission') or 'EXP' in line:
            continue

        # Try to parse transaction lines
        # Expected format: MM/DD Category Action Symbol/CUSIP Description Quantity Price/Rate Interest Amount Gain/Loss
        # OR: Category Action Symbol/CUSIP Description ... (no date means use current_date)

        line_parts = line.split()
        if len(line_parts) < 2:
            continue

        # Check if line starts with a date (MM/DD format)
        date_match = re.match(r'^(\d{2}/\d{2})\s+', line)

        if date_match:
            # Line has a date - parse it and update current_date
            date_str = line_parts[0]  # MM/DD
            parsed_date = _parse_date(f"{date_str}/{year}")
            if not parsed_date:
                logger.warning(f"Could not parse date from: {date_str}/{year}")
                continue
            current_date = parsed_date
            category = line_parts[1]  # e.g., "Interest", "Purchase", "Sale"
            parts_start_index = 2  # Description starts at index 2
        else:
            # Line has no date - use current_date
            if not current_date:
                # No date seen yet, skip this line
                continue
            parsed_date = current_date
            category = line_parts[0]  # e.g., "Interest", "Purchase", "Sale"
            parts_start_index = 1  # Description starts at index 1

        try:
            # Category is the transaction type
            transaction_type = category

            # Validate transaction type - must be one of the known types
            valid_types = ['Purchase', 'Sale', 'Buy', 'Sell', 'Interest', 'Dividend', 'Transfer',
                          'Deposit', 'Withdrawal', 'Fee', 'Split', 'Merger', 'Spinoff']
            if not any(vt in category for vt in valid_types):
                # Not a valid transaction type, skip this line
                continue

            # Find numeric values from right to left: Amount, Price, Quantity
            # Format: ... Symbol Description Quantity Price Amount
            numeric_values = []
            for idx in range(len(line_parts) - 1, -1, -1):
                part = line_parts[idx].replace('$', '').replace(',', '')
                # Handle negative amounts in parentheses like (1,033.75)
                is_negative = part.startswith('(') and part.endswith(')')
                clean_part = part.replace('(', '').replace(')', '')
                try:
                    numeric_val = Decimal(clean_part)
                    numeric_values.append((idx, numeric_val, is_negative))
                    if len(numeric_values) >= 3:  # We found amount, price, quantity
                        break
                except (InvalidOperation, ValueError):
                    continue

            if not numeric_values:
                logger.warning(f"Could not find amount in line: {line}")
                continue

            # Parse numeric values: [0] is amount (rightmost), [1] is price, [2] is quantity
            amount_str = str(numeric_values[0][1])
            price_per_share = numeric_values[1][1] if len(numeric_values) > 1 else None
            quantity = numeric_values[2][1] if len(numeric_values) > 2 else None

            # If only one or two numeric values, might be Interest or other non-equity transaction
            if len(numeric_values) == 1:
                # Only amount, no quantity/price (e.g., Interest)
                quantity = None
                price_per_share = None

            # Description is everything between start and the first numeric value
            first_numeric_index = numeric_values[-1][0] if numeric_values else len(line_parts)
            description_parts = line_parts[parts_start_index:first_numeric_index]

            # Symbol is typically first part (all caps, 1-5 characters)
            symbol = None
            desc_start = 0
            if description_parts:
                first_part = description_parts[0]
                clean_first = re.sub(r'[^A-Z]', '', first_part)
                if clean_first and 1 <= len(clean_first) <= 5 and clean_first.isupper():
                    symbol = clean_first
                    desc_start = 1  # Description starts after symbol

            description = " ".join(description_parts[desc_start:]) if len(description_parts) > desc_start else category

            # Classify security type
            security_type = _classify_security_type(transaction_type, symbol)

            investment_transactions.append(
                ParsedInvestmentTransaction(
                    transaction_date=parsed_date,
                    transaction_type=transaction_type,
                    symbol=symbol,
                    description=description.strip(),
                    quantity=quantity,
                    price_per_share=price_per_share,
                    total_amount=Decimal(amount_str),
                    security_type=security_type
                )
            )

        except (ValueError, InvalidOperation, IndexError) as e:
            logger.warning(f"Skipping row in Schwab PDF due to parsing error: {line} -> {e}")
            continue

    logger.info(f"Successfully parsed {len(investment_transactions)} investment transactions from Schwab PDF")

    # Handle duplicates
    investment_transactions = _handle_investment_duplicates(investment_transactions)

    return ParsedData(account_info=account_info, investment_transactions=investment_transactions)

def parse_csv(file_source: Union[Path, IO[bytes]]) -> ParsedData:
    """Parses a Schwab CSV from a file path or in-memory stream."""
    logger.info("Parsing investment transaction data from Schwab csv...")
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

            # Classify security type
            security_type = _classify_security_type(action, symbol)

            investment_transactions.append(
                ParsedInvestmentTransaction(
                    transaction_date=parsed_date,
                    transaction_type=action,
                    symbol=symbol,
                    description=description,
                    quantity=Decimal(quantity_str) if quantity_str else None,
                    price_per_share=Decimal(price_str.replace('$','')) if price_str else None,
                    total_amount=Decimal(amount_str.replace('$','')),
                    security_type=security_type
                )
            )
        except (ValueError, InvalidOperation, IndexError) as e:
            logger.warning(f"Skipping row in Schwab CSV due to parsing error: {row} -> {e}")
            continue

    if isinstance(file_source, Path):
        text_stream.close()

    logger.info(f"Successfully parsed {len(investment_transactions)} investment transactions from Schwab CSV")

    # Handle duplicates
    investment_transactions = _handle_investment_duplicates(investment_transactions)

    return ParsedData(account_info=account_info, investment_transactions=investment_transactions)

def parse(file_source: Union[Path, IO[bytes]], is_csv: bool = False) -> ParsedData:
    """
    Parses a Schwab statement (PDF or CSV) from a file path or in-memory stream.
    """
    if is_csv:
        return parse_csv(file_source)
    else:
        return parse_statement(file_source)