import csv
import pdfplumber
import re
from pathlib import Path
from decimal import Decimal, InvalidOperation
from datetime import datetime
from typing import List, Optional, Union, IO
from itertools import groupby

from src.parser.models import ParsedData, ParsedInvestmentTransaction, ParsedAccountInfo, SecurityType
from src.logging_config import get_logger

logger = get_logger(__name__)

def _parse_date(date_str: str, year: Optional[str] = None) -> Optional[datetime.date]:
    """Parses a date string like 'MM/DD' or 'MM/DD/YY'."""
    if not date_str or not isinstance(date_str, str):
        return None
    date_str = date_str.strip()

    # Try with year if provided
    if year and '/' in date_str and date_str.count('/') == 1:
        # MM/DD format, append year
        date_str = f"{date_str}/{year}"

    # Try different date formats
    for fmt in ("%m/%d/%Y", "%m/%d/%y"):
        try:
            return datetime.strptime(date_str, fmt).date()
        except ValueError:
            continue

    logger.warning(f"Could not parse date: {date_str} (year={year})")
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
                            api_symbol=transaction.api_symbol,
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

def _normalize_transaction_type(category: str) -> str:
    """
    Normalize Schwab transaction types to standard format.

    Standard types:
    - BUY: Purchase of securities
    - SELL: Sale of securities
    - DIVIDEND: Dividend payments
    - INTEREST: Interest income/charges
    - FEE: Fees and charges
    - TRANSFER: Deposits, withdrawals, fund transfers
    - OTHER: Corporate actions, adjustments, etc.
    """
    category_lower = category.lower()

    # Buy transactions
    if 'purchase' in category_lower or 'buy' in category_lower:
        return "BUY"

    # Sell transactions
    if 'sale' in category_lower or 'sell' in category_lower:
        return "SELL"

    # Dividend
    if 'dividend' in category_lower:
        return "DIVIDEND"

    # Interest
    if 'interest' in category_lower:
        return "INTEREST"

    # Fee
    if 'fee' in category_lower:
        return "FEE"

    # Transfers (deposits, withdrawals)
    if any(word in category_lower for word in ['deposit', 'withdrawal', 'transfer']):
        return "TRANSFER"

    # Corporate actions and other
    return "OTHER"

def _extract_symbol(symbol_cusip: str, category: str) -> Optional[str]:
    """
    Extract underlying ticker symbol from Symbol/CUSIP column for Schwab statements.

    Returns just the ticker symbol (e.g., "SPY", "MARA", "AAPL") for both stocks and options.
    """
    if not symbol_cusip or not symbol_cusip.strip():
        return None

    # Only extract symbols for buy/sell transactions
    normalized_type = _normalize_transaction_type(category)
    if normalized_type not in ["BUY", "SELL"]:
        return None

    symbol_cusip = symbol_cusip.strip()

    # Extract ticker from first line (may contain multiple lines or date info)
    first_line = symbol_cusip.split('\n')[0].strip() if '\n' in symbol_cusip else symbol_cusip

    # Match just the ticker (letters before any numbers/dates)
    ticker_match = re.match(r'^([A-Z]{1,5})', first_line)
    if ticker_match:
        return ticker_match.group(1)
    else:
        return first_line if first_line else None

def _format_api_symbol(symbol: str, description: str, security_type: Optional[SecurityType]) -> Optional[str]:
    """
    Format symbol for yfinance API calls.

    For stocks: Just returns the ticker (e.g., "AAPL")
    For options: Returns OCC format (e.g., "SPY240517P00500000")

    OCC format: TICKER + YYMMDD + C/P + 8-digit strike price
    Strike price is padded to 8 digits with 3 decimal places (multiply by 1000)
    Example: SPY $500 PUT expiring 05/17/24 → SPY240517P00500000
    """
    if not symbol:
        return None

    # For stocks, just return the ticker
    if security_type != SecurityType.OPTION:
        return symbol

    # For options, build OCC format
    if not description:
        return None

    desc_upper = description.upper()

    # Extract option type
    option_type = "C" if desc_upper.startswith("CALL") else "P"

    # Extract expiry date (pattern: EXP05/17/24 or EXP 05/17/24)
    expiry_match = re.search(r'EXP\s*(\d{2}/\d{2}/\d{2})', description)
    if not expiry_match:
        return None

    expiry = expiry_match.group(1)
    # Convert MM/DD/YY to YYMMDD
    try:
        expiry_date = datetime.strptime(expiry, "%m/%d/%y")
        expiry_formatted = expiry_date.strftime("%y%m%d")
    except ValueError:
        return None

    # Extract strike price (pattern: $XXX or $XX.XX)
    strike_match = re.search(r'\$(\d+(?:\.\d{2})?)', description)
    if not strike_match:
        return None

    strike = strike_match.group(1)
    # Convert to 8-digit format (multiply by 1000 for 3 decimal places)
    try:
        strike_float = float(strike)
        strike_int = int(strike_float * 1000)
        strike_formatted = f"{strike_int:08d}"
    except ValueError:
        return None

    # Build OCC format: TICKER + YYMMDD + C/P + 8-digit strike
    return f"{symbol}{expiry_formatted}{option_type}{strike_formatted}"

def parse_statement(file_source: Union[Path, IO[bytes]]) -> ParsedData:
    """Parses a Schwab PDF statement using table-based extraction."""
    logger.info("Parsing investment transaction data from Schwab statement...")
    investment_transactions: List[ParsedInvestmentTransaction] = []
    account_info: Optional[ParsedAccountInfo] = None

    # Statement year for date parsing
    statement_year = None

    # Table tracking variables per page
    page_horizontal_lines = {}  # Map page_num to list of y-positions
    page_end_markers = {}  # Track y-position of end markers per page
    vertical_lines = []
    activity_tables = []
    tracking_activity = False

    with pdfplumber.open(file_source) as pdf:
        # Extract account number and year from all pages
        for page in pdf.pages:
            lines = page.extract_text_lines()
            for line in lines:
                line_text = line['text']

                # Look for account number pattern directly (format: 4938-9145)
                if not account_info:
                    match = re.search(r'\b(\d{4})-(\d{4})\b', line_text)
                    if match:
                        account_number = match.group(2)
                        account_info = ParsedAccountInfo(account_number_last4=account_number)
                        logger.debug(f"Found account number ending in: {account_number}")

                # Extract year - look for pattern like "May1-31,2024" or "Statement Period ... 2024"
                # Just search for a 4-digit year in any line near the top
                if not statement_year:
                    year_match = re.search(r'(19\d{2}|20\d{2})', line_text)
                    if year_match:
                        statement_year = year_match.group(1)
                        logger.debug(f"Found statement year: {statement_year}")

        # Process each page for Transaction Details tables
        for page_num, page in enumerate(pdf.pages):
            lines = page.extract_text_lines()

            for i, line in enumerate(lines):
                line_text = line['text'].strip()

                # Detect "Transaction Details" section start
                if line_text == "Transaction Details":
                    tracking_activity = True
                    logger.debug(f"Page {page_num + 1}: Found Transaction Details section")
                    continue

                # Detect table header and set up column boundaries
                if tracking_activity and "Date" in line_text and "Category" in line_text and "CUSIP" in line_text:
                    # Header found - set vertical boundaries based on column positions
                    # Columns: Date(18.2), Category(52.3), Action(108.7), Symbol/CUSIP(196.3),
                    #          Description(271.9), Quantity(476.1), Price/Rate(539.2),
                    #          Charges/Interest(593.5), Amount(673.9), Gain/Loss(754.0)

                    if not vertical_lines:  # Only set once
                        # Precise boundaries based on visual alignment
                        vertical_lines = [
                            16,      # Before Date (18.2)
                            45,      # Before Category (52.3)
                            98,      # Before Action (108.7)
                            178,     # Before CUSIP (196.3)
                            252,     # Before Description (271.9)
                            442,     # Before Quantity (476.1)
                            512,     # Before perShare (539.2)
                            570,     # Before Interest (593.5)
                            630,     # Before Amount (673.9)
                            712,     # Before Gain/Loss
                            line['x1']  # End of row
                        ]
                        logger.debug(f"Page {page_num + 1}: Set up column boundaries: {[f'{v:.1f}' for v in vertical_lines]}")
                    continue

                # Collect transaction line positions
                # Lines can start with:
                # 1. Date (MM/DD) - first transaction of the day
                # 2. Category (Purchase, Sale, Interest, etc.) - subsequent transactions
                if tracking_activity and vertical_lines:
                    # Check if line starts with date OR with a known category
                    is_transaction_line = False

                    # Check for date at start
                    if re.match(r'^\d{2}/\d{2}\s+', line_text):
                        is_transaction_line = True

                    # Check for category at start (for transactions without dates)
                    category_patterns = ['Purchase', 'Sale', 'Buy', 'Sell', 'Interest', 'Dividend',
                                       'Fee', 'Deposit', 'Withdrawal', 'Transfer']
                    for pattern in category_patterns:
                        if line_text.startswith(pattern):
                            is_transaction_line = True
                            break

                    if is_transaction_line:
                        # Skip lines that are clearly continuations (commission/fee lines)
                        if not line_text.startswith('Commission') and not re.match(r'^[A-Z]\s+Commission', line_text):
                            if page_num not in page_horizontal_lines:
                                page_horizontal_lines[page_num] = []
                            page_horizontal_lines[page_num].append(line['top'])
                            logger.debug(f"Page {page_num + 1}: Collected transaction line at y={line['top']:.1f}")

                # Detect end of Transaction Details section
                # End markers: page number like "3 of 6" or "TotalTransactions" summary line
                if tracking_activity:
                    # Check for page number (e.g., "3of6" or "3 of 6")
                    is_page_number = re.match(r'^\d+\s*of\s*\d+$', line_text.replace(" ", ""))
                    # Check for Total Transactions line
                    is_total_line = "TotalTransactions" in line_text.replace(" ", "") or line_text.startswith("Total Transactions")

                    if is_page_number or is_total_line:
                        # Store the y-position of the end marker
                        page_end_markers[page_num] = line['top']
                        tracking_activity = False
                        logger.debug(f"Page {page_num + 1}: End of Transaction Details section at y={line['top']:.1f}: {line_text}")
                        continue

        # Build tables for each page
        for page_num, horizontal_lines in page_horizontal_lines.items():
            if not horizontal_lines or not vertical_lines:
                continue

            page = pdf.pages[page_num]
            horizontal_lines_sorted = sorted(horizontal_lines)

            # Use end marker position if available, otherwise use padding
            if page_num in page_end_markers:
                # Use end marker position as final boundary
                end_boundary = page_end_markers[page_num]
                logger.debug(f"Page {page_num + 1}: Using end marker at y={end_boundary:.1f} as final boundary")
            else:
                # Fall back to padding calculation
                if len(horizontal_lines_sorted) > 1:
                    avg_row_height = (horizontal_lines_sorted[-1] - horizontal_lines_sorted[0]) / len(horizontal_lines_sorted)
                    padding = avg_row_height * 3  # Enough for multi-line transactions
                else:
                    padding = 50
                end_boundary = horizontal_lines_sorted[-1] + padding
                logger.debug(f"Page {page_num + 1}: No end marker, using padding. Final boundary at y={end_boundary:.1f}")

            horizontal_lines_sorted.append(end_boundary)

            # Build table cells
            cells = []
            for h in range(len(horizontal_lines_sorted) - 1):
                for v in range(len(vertical_lines) - 1):
                    cells.append([vertical_lines[v], horizontal_lines_sorted[h],
                                vertical_lines[v+1], horizontal_lines_sorted[h+1]])

            # Create table
            table = pdfplumber.table.Table(page, tuple(cells))
            activity_tables.append(table)
            logger.info(f"Page {page_num + 1}: Created table with {len(cells)} cells, {len(horizontal_lines_sorted)-1} rows")

        # Extract data from all activity tables
        all_rows = []
        logger.info(f"Total activity tables created: {len(activity_tables)}")
        for idx, table in enumerate(activity_tables):
            try:
                extracted = table.extract()
                logger.info(f"Table {idx + 1}: extracted {len(extracted)} rows")
                if extracted:
                    logger.debug(f"Sample row: {extracted[0]}")
                all_rows.extend(extracted)
            except Exception as e:
                logger.error(f"Error extracting table {idx + 1}: {e}")

        logger.info(f"Extracted {len(all_rows)} total rows from {len(activity_tables)} activity tables")

        # Process rows to create transactions
        # Columns: Date | Category | Action | Symbol/CUSIP | Description | Quantity | Price/Rate | Charges/Interest | Amount | Gain/Loss
        skip_reasons = {"empty": 0, "too_few_cols": 0, "no_date": 0, "date_parse_fail": 0, "no_amount": 0, "parse_error": 0}
        current_date = None  # Track last seen date for transactions without dates

        for row_idx, row in enumerate(all_rows):
            if not row:
                skip_reasons["empty"] += 1
                continue

            logger.debug(f"Row {row_idx}: {len(row)} columns: {[str(c)[:30] if c else 'None' for c in row]}")

            if len(row) < 9:  # Need at least 9 columns
                skip_reasons["too_few_cols"] += 1
                continue

            try:
                # Parse columns
                date_str = str(row[0]).strip() if row[0] else ''
                category = str(row[1]).strip() if row[1] else ''
                action = str(row[2]).strip() if row[2] else ''  # Usually empty
                symbol_cusip = str(row[3]).strip() if row[3] else ''
                description = str(row[4]).strip() if row[4] else ''
                quantity_str = str(row[5]).strip() if row[5] else ''
                price_str = str(row[6]).strip() if row[6] else ''
                charges_str = str(row[7]).strip() if row[7] else ''
                amount_str = str(row[8]).strip() if row[8] else ''
                # Skip gain/loss (row[9])

                # Check if row has a date or uses previous date
                # Extract just the MM/DD portion if present
                date_match = re.match(r'^(\d{2}/\d{2})', date_str)
                if date_match:
                    # Has a date - parse it
                    clean_date_str = date_match.group(1)
                    parsed_date = _parse_date(clean_date_str, statement_year)
                    if not parsed_date:
                        skip_reasons["date_parse_fail"] += 1
                        continue
                    current_date = parsed_date
                else:
                    # No date - use current_date
                    if not current_date:
                        skip_reasons["no_date"] += 1
                        logger.warning(f"Row {row_idx} skipped - no date and no previous date set")
                        continue
                    parsed_date = current_date

                # Normalize transaction type
                transaction_type = _normalize_transaction_type(category)

                # Extract symbol (only for BUY/SELL) - just the underlying ticker
                symbol = _extract_symbol(symbol_cusip, category)

                # Set security_type only for BUY/SELL
                # Detect options by checking if description starts with CALL/PUT AND contains "EXP"
                security_type = None
                if transaction_type in ["BUY", "SELL"] and symbol:
                    desc_upper = description.upper() if description else ""
                    is_option = (desc_upper.startswith("CALL") or desc_upper.startswith("PUT")) and "EXP" in desc_upper
                    security_type = SecurityType.OPTION if is_option else SecurityType.STOCK

                # Format API symbol for yfinance (stocks and options)
                api_symbol = _format_api_symbol(symbol, description, security_type) if symbol else None

                # Parse numeric fields
                clean_quantity = None
                if quantity_str and quantity_str not in ['-', 'None']:
                    try:
                        # Remove parentheses for negative
                        qty_clean = quantity_str.replace('(', '-').replace(')', '').replace(',', '')
                        clean_quantity = Decimal(qty_clean)
                    except (InvalidOperation, ValueError):
                        pass

                clean_price = None
                if price_str and price_str not in ['-', 'None']:
                    try:
                        clean_price = Decimal(price_str.replace(',', '').replace('$', ''))
                    except (InvalidOperation, ValueError):
                        pass

                # Parse amount (handle parentheses for negative)
                if not amount_str or amount_str in ['-', 'None']:
                    skip_reasons["no_amount"] += 1
                    logger.warning(f"Row {row_idx} skipped - no amount")
                    continue

                amount_str = amount_str.replace(',', '').replace('$', '')
                if '(' in amount_str:
                    amount_str = '-' + amount_str.replace('(', '').replace(')', '')

                clean_amount = Decimal(amount_str)

                investment_transactions.append(
                    ParsedInvestmentTransaction(
                        transaction_date=parsed_date,
                        transaction_type=transaction_type,
                        symbol=symbol,
                        api_symbol=api_symbol,
                        description=description,
                        quantity=clean_quantity,
                        price_per_share=clean_price,
                        total_amount=clean_amount,
                        security_type=security_type
                    )
                )
                logger.debug(f"Parsed: {parsed_date} | {transaction_type} | {symbol or 'N/A'} | ${clean_amount}")

            except (ValueError, InvalidOperation, IndexError) as e:
                skip_reasons["parse_error"] += 1
                logger.warning(f"Skipping row {row_idx} due to parsing error: {row} -> {e}")
                continue

        # Log skip statistics
        logger.info(f"Row processing summary: {len(all_rows)} total rows")
        logger.info(f"  Parsed: {len(investment_transactions)}")
        logger.info(f"  Skipped - Empty rows: {skip_reasons['empty']}")
        logger.info(f"  Skipped - Too few columns: {skip_reasons['too_few_cols']}")
        logger.info(f"  Skipped - No valid date: {skip_reasons['no_date']}")
        logger.info(f"  Skipped - Date parse failed: {skip_reasons['date_parse_fail']}")
        logger.info(f"  Skipped - No amount: {skip_reasons['no_amount']}")
        logger.info(f"  Skipped - Parse errors: {skip_reasons['parse_error']}")

        logger.info(f"Successfully parsed {len(investment_transactions)} investment transactions from Schwab statement")

    # Handle duplicates
    investment_transactions = _handle_investment_duplicates(investment_transactions)

    return ParsedData(account_info=account_info, investment_transactions=investment_transactions)
