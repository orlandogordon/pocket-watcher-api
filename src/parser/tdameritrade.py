import csv
import pdfplumber
import re
from pathlib import Path
from decimal import Decimal, InvalidOperation
from datetime import datetime
from typing import List, Optional, Union, IO
from itertools import groupby

from src.parser.models import ParsedData, ParsedInvestmentTransaction, ParsedAccountInfo
from src.logging_config import get_logger

logger = get_logger(__name__)

def _parse_date(date_str: str) -> Optional[datetime.date]:
    """Parses a date string like 'MM/DD/YY' or 'MM/DD/YYYY'."""
    if not date_str or not isinstance(date_str, str):
        return None
    date_str = date_str.strip()
    for fmt in ("%m/%d/%y", "%m/%d/%Y"):
        try:
            return datetime.strptime(date_str, fmt).date()
        except ValueError:
            pass
    logger.warning(f"Could not parse date: {date_str}")
    return None

def _handle_investment_duplicates(transactions: List[ParsedInvestmentTransaction]) -> List[ParsedInvestmentTransaction]:
    """
    Handles duplicate investment transactions by appending a counter to the description.
    Groups by date, transaction_type, symbol, and description.
    """
    updated_transactions = []
    # Use empty string for None symbols to allow sorting
    keyfunc = lambda t: (t.transaction_date, t.transaction_type, t.symbol or "", t.description)

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

def _extract_symbol_from_description(desc: str) -> Optional[str]:
    """Extract symbol from description (first all-caps word 1-5 chars)"""
    if not desc:
        return None
    # Look for all-caps ticker symbols (1-5 letters)
    match = re.search(r'\b([A-Z]{1,5})\b', desc)
    return match.group(1) if match else None

def parse_statement(file_source: Union[Path, IO[bytes]]) -> ParsedData:
    """Parses a TD Ameritrade PDF statement from a file path or in-memory stream."""
    logger.info("Parsing investment transaction data from TD Ameritrade statement...")
    investment_transactions: List[ParsedInvestmentTransaction] = []
    account_info: Optional[ParsedAccountInfo] = None

    text = ''
    with pdfplumber.open(file_source) as pdf:
        for page in pdf.pages:
            text += page.extract_text(x_tolerance=2) or ''
    lines = text.split('\n')

    # Find account number - format: "Statement for Account # 498-805590"
    for line in lines:
        if "Statement for Account #" in line or "Account Number" in line:
            try:
                # Attempt to find a number like XXX-XXXXXX or XXXX-XXXX
                match = re.search(r'#?\s*(\d{3,4}-\d{4,6})', line)
                if match:
                    account_number = match.group(1).split('-')[-1][-4:]
                    account_info = ParsedAccountInfo(account_number_last4=account_number)
                    logger.debug(f"Found account number ending in: {account_number}")
                    break
            except (IndexError, ValueError):
                continue

    # Parse Account Activity table
    # Multi-line format:
    # Line 1: 11/30/20 12/01/20 Margin Sell - Securities Sold SPDR S&P500 ETF TRUST - 1- $ 0.08 $ 7.34 3,160.17
    # Line 2: SPY Dec 21 20 394.0 C TO OPEN (continuation - option details)
    # Line 3: Commission/Fee 0.65
    # Line 4: Regulatory Fee 0.01

    in_activity_section = False
    header_found = False
    header_line_count = 0
    skip_next_lines = 0  # Track commission/fee lines to skip

    i = 0
    while i < len(lines):
        line = lines[i]

        # Look for the actual "Account Activity" table header (should be standalone on its own line)
        if line.strip() == "Account Activity":
            in_activity_section = True
            logger.debug(f"Found Account Activity section at line {i+1}")
            i += 1
            continue

        # Look for table header - it spans 2 lines:
        # Line 1: "Trade Settle Acct Transaction/ Symbol/"
        # Line 2: "Date Date Type Cash Activity* Description CUSIP Quantity Price Amount Balance"
        if in_activity_section and not header_found:
            if "Trade" in line and "Settle" in line:
                header_line_count = 1
                logger.debug(f"Found first line of table header at line {i+1}")
                i += 1
                continue
            elif header_line_count == 1 and "Date" in line and "Type" in line:
                header_found = True
                header_line_count = 0
                logger.debug(f"Found second line of table header at line {i+1}")
                i += 1
                continue

        # Stop at closing balance or when we hit a new page with repeated headers
        if in_activity_section and ("Closing Balance" in line or "TD Ameritrade Cash" in line or
                                     "Important Information" in line or "*For Cash Activity" in line or
                                     "page" in line.lower() and "of" in line.lower() and "Statement for Account" in line):
            logger.debug(f"Reached end of activity section at line {i+1}")
            i += 1
            # Check if this is just a page break and Account Activity continues
            if i < len(lines) and "Statement for Account" in line:
                # Skip to next Account Activity section
                continue
            else:
                break

        # Skip non-transaction lines
        if not in_activity_section or not header_found:
            i += 1
            continue
        if not line.strip() or line.startswith("*") or "Opening Balance" in line or "Payable:" in line:
            i += 1
            continue

        # Skip commission/fee lines
        if "Commission/Fee" in line or "Regulatory Fee" in line:
            i += 1
            continue

        # Try to parse transaction line - starts with a date
        # Pattern: MM/DD/YY MM/DD/YY Type ... Description ... - Quantity Price Amount Balance
        date_match = re.match(r'^(\d{2}/\d{2}/\d{2,4})\s+\d{2}/\d{2}/\d{2,4}\s+', line.strip())

        if date_match:
            try:
                # Parse the full transaction line
                # Extract from right to left: Balance, Amount, Price, Quantity
                parts = line.strip().split()
                if len(parts) < 4:
                    i += 1
                    continue

                # Last part is balance
                balance_str = parts[-1].replace(',', '').replace('$', '')

                # Second to last is amount (with possible parentheses for negative)
                amount_str = parts[-2].replace(',', '').replace('$', '')
                if '(' in amount_str:
                    amount_str = '-' + amount_str.replace('(', '').replace(')', '')

                # Skip if amount is just a dash (no amount)
                if amount_str == '-' or amount_str == '':
                    logger.debug(f"Skipping transaction with no amount: {line.strip()}")
                    i += 1
                    continue

                # Parse date
                date_str = parts[0]
                parsed_date = _parse_date(date_str)
                if not parsed_date:
                    logger.debug(f"Could not parse date from: {date_str}")
                    i += 1
                    continue

                # Extract transaction type (Margin, Cash, etc.) and action (Buy, Sell, etc.)
                txn_type = parts[2]  # "Margin", "Cash", etc.
                action = parts[3] if len(parts) > 3 else ""  # "Buy", "Sell", etc.

                # Find where the description starts and ends
                # Description is between action and the quantity/price/amount at the end
                description_parts = []
                in_description = False
                for j, part in enumerate(parts):
                    if j <= 3:  # Skip date, date, type, action
                        continue
                    # Check if we've hit the numeric fields at the end
                    # Price and quantity are usually the last 4 parts: quantity, price, amount, balance
                    if j >= len(parts) - 4:
                        break
                    description_parts.append(part)

                description = ' '.join(description_parts)

                # Try to extract quantity and price (3rd and 4th from end)
                quantity_str = None
                price_str = None
                if len(parts) >= 4:
                    try:
                        # Quantity is usually 3rd from end, price is 4th from end
                        potential_price = parts[-3].replace('$', '').replace(',', '')
                        potential_qty = parts[-4].replace('-', '').replace(',', '')

                        # Validate they look numeric
                        if re.match(r'^[\d.]+$', potential_price):
                            price_str = potential_price
                        if re.match(r'^[\d.]+$', potential_qty):
                            quantity_str = potential_qty
                    except:
                        pass

                # Check if next line is a continuation (option details, etc.)
                continuation = ""
                symbol = None
                if i + 1 < len(lines):
                    next_line = lines[i + 1].strip()
                    # Continuation lines don't start with a date and aren't commission/fee
                    if (not re.match(r'^\d{2}/\d{2}/\d{2}', next_line) and
                        "Commission/Fee" not in next_line and
                        "Regulatory Fee" not in next_line and
                        next_line and
                        "Opening Balance" not in next_line and
                        "Closing Balance" not in next_line):
                        continuation = next_line
                        # Extract symbol from continuation (first all-caps word)
                        symbol_match = re.match(r'^([A-Z]{1,5})\s', continuation)
                        if symbol_match:
                            symbol = symbol_match.group(1)

                # Combine description with continuation
                full_description = f"{description} {continuation}".strip()

                # Clean up amount
                clean_amount = Decimal(amount_str)

                # Clean up quantity and price
                clean_quantity = Decimal(quantity_str) if quantity_str else None
                clean_price = Decimal(price_str) if price_str else None

                full_type = f"{txn_type} {action}".strip()

                investment_transactions.append(
                    ParsedInvestmentTransaction(
                        transaction_date=parsed_date,
                        transaction_type=full_type,
                        symbol=symbol,
                        description=full_description,
                        quantity=clean_quantity,
                        price_per_share=clean_price,
                        total_amount=clean_amount
                    )
                )
                logger.debug(f"Parsed transaction: {parsed_date} | {full_type} | {symbol or 'N/A'} | {full_description[:50]} | ${clean_amount}")

            except (ValueError, InvalidOperation, IndexError) as e:
                logger.warning(f"Skipping row in TD Ameritrade statement due to parsing error: {line} -> {e}")

        i += 1

    logger.info(f"Successfully parsed {len(investment_transactions)} investment transactions from TD Ameritrade statement")

    # Handle duplicates
    investment_transactions = _handle_investment_duplicates(investment_transactions)

    return ParsedData(account_info=account_info, investment_transactions=investment_transactions)
