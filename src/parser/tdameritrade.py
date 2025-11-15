import csv
import pdfplumber
import re
from pathlib import Path
from decimal import Decimal, InvalidOperation
from datetime import datetime
from typing import List, Optional, Union, IO

from src.parser.models import ParsedData, ParsedInvestmentTransaction, ParsedAccountInfo, SecurityType
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

def _normalize_transaction_type(transaction_type: str, description: str) -> str:
    """
    Normalize TD Ameritrade transaction types to standard types:
    BUY, SELL, DIVIDEND, INTEREST, FEE, TRANSFER, OTHER

    TD Ameritrade uses formats like:
    - "Margin Buy Securities Purchased"
    - "Margin Sell Securities Sold"
    - "Div/Int Income"
    - "Funds Deposited"
    """
    txn_lower = transaction_type.lower()
    desc_lower = description.lower() if description else ""

    # Buy/Sell
    if "securities purchased" in txn_lower or "buy" in txn_lower:
        return "BUY"
    if "securities sold" in txn_lower or "sell" in txn_lower:
        return "SELL"

    # Dividends and Interest
    if "div" in txn_lower and re.search(r'\bdividend\b', desc_lower):
        return "DIVIDEND"
    if "int" in txn_lower or re.search(r'\binterest\b', desc_lower):
        return "INTEREST"

    # Deposits/Withdrawals = Transfer
    if "funds deposited" in txn_lower or "ach in" in desc_lower:
        return "TRANSFER"
    if "funds disbursed" in txn_lower or "ach out" in desc_lower:
        return "TRANSFER"

    # Fees
    if "fee" in txn_lower:
        return "FEE"

    # Everything else
    return "OTHER"

def _classify_security_type(transaction_type: str, description: str) -> SecurityType:
    """
    Classify the security type based on transaction type and description.

    TD Ameritrade transaction types:
    - "Margin Buy/Sell - Securities Purchased/Sold" -> STOCK or OPTION
    - "Funds Deposited/Disbursed" -> DEPOSIT/WITHDRAWAL
    - "Div/Int - Income/Expense" -> DIVIDEND/INTEREST
    - "Journal - Other" -> OTHER
    - "Fee - Other" -> FEE
    """
    txn_type_lower = transaction_type.lower()
    desc_lower = description.lower() if description else ""

    # Check for deposits/withdrawals (check transaction type first)
    if "funds deposited" in txn_type_lower or "ach in" in desc_lower or "deposit" in desc_lower:
        return SecurityType.DEPOSIT
    if "funds disbursed" in txn_type_lower or "ach out" in desc_lower or "withdrawal" in desc_lower:
        return SecurityType.WITHDRAWAL

    # Check for dividends/interest - transaction type will contain "div" or "int"
    # Then look at description to distinguish between them
    if "div" in txn_type_lower or "int" in txn_type_lower:
        # Look at description to determine if it's dividend or interest
        if re.search(r'\bdividend\b', desc_lower):
            return SecurityType.DIVIDEND
        elif re.search(r'\binterest\b', desc_lower):
            return SecurityType.INTEREST
        else:
            # Default to interest if we can't determine from description
            return SecurityType.INTEREST

    # Check for fees and adjustments
    if "fee" in txn_type_lower:
        return SecurityType.FEE
    if "journal" in txn_type_lower or "adjustment" in desc_lower:
        return SecurityType.OTHER

    # Check for securities - need to distinguish STOCK vs OPTION
    # Options typically have the contract format: "SYMBOL MMM DD YY PRICE.0 C/P"
    if "securities purchased" in txn_type_lower or "securities sold" in txn_type_lower or "buy" in txn_type_lower or "sell" in txn_type_lower:
        # Look for option contract pattern in description
        # Pattern: Symbol followed by month, day, year, strike price, and C/P
        option_pattern = r'\b[A-Z]{1,5}\s+[A-Z][a-z]{2}\s+\d{1,2}\s+\d{2}\s+[\d.]+\s+[CP]\b'
        if re.search(option_pattern, description, re.MULTILINE):
            return SecurityType.OPTION
        else:
            return SecurityType.STOCK

    # Default to OTHER if we can't classify
    return SecurityType.OTHER

def _extract_symbol(description: str, security_type: SecurityType) -> Optional[str]:
    """
    Extract underlying ticker symbol from description.

    Returns just the ticker (e.g., "AAPL", "SPY") for both stocks and options.
    """
    if not description:
        return None

    # Non-security transactions don't have symbols
    if security_type in [SecurityType.DEPOSIT, SecurityType.WITHDRAWAL, SecurityType.INTEREST,
                         SecurityType.DIVIDEND, SecurityType.FEE, SecurityType.OTHER]:
        return None

    lines = description.split('\n')

    if security_type == SecurityType.OPTION:
        # Search all lines for option contract format
        # Pattern: "SYMBOL MMM DD YY PRICE.0 C/P"
        # Extract just the SYMBOL part (underlying ticker)
        option_pattern = r'\b([A-Z]{1,5})\s+[A-Z][a-z]{2}\s+\d{1,2}\s+\d{2}\s+[\d.]+\s+[CP]\b'
        for line in lines:
            # Remove "TO OPEN" or "TO CLOSE" first
            line_cleaned = re.sub(r'\s+TO\s+(OPEN|CLOSE)\s*$', '', line, flags=re.IGNORECASE)
            match = re.search(option_pattern, line_cleaned)
            if match:
                # Return just the ticker (group 1), not the full contract string
                return match.group(1).strip()

        # If no match found, log warning and return None
        logger.warning(f"Option transaction but no option contract found in description: {description[:100]}")
        return None

    elif security_type == SecurityType.STOCK:
        # Extract ticker from first line - look for all-caps word (1-5 chars)
        # Usually appears after company name
        first_line = lines[0] if lines else ""

        # Look for ticker pattern: 1-5 uppercase letters as a standalone word
        # Match the LAST occurrence to get the ticker at the end of company name
        matches = re.findall(r'\b([A-Z]{1,5})\b', first_line)
        if matches:
            # Return the last match (usually the ticker after company name)
            return matches[-1]

        logger.warning(f"Stock transaction but no ticker found in description: {description[:100]}")
        return None

    return None

def _format_api_symbol(symbol: str, description: str, security_type: Optional[SecurityType]) -> Optional[str]:
    """
    Format symbol for yfinance API calls.

    For stocks: Just returns the ticker (e.g., "AAPL")
    For options: Returns OCC format (e.g., "SPY240517P00500000")

    TD Ameritrade options format in description: "SPY May 17 24 500.0 P"
    OCC format: TICKER + YYMMDD + C/P + 8-digit strike price
    """
    if not symbol:
        return None

    # For stocks, just return the ticker
    if security_type != SecurityType.OPTION:
        return symbol

    # For options, parse the option string and convert to OCC format
    if not description:
        return None

    # TD Ameritrade option format: "SYMBOL MMM DD YY PRICE.0 C/P"
    # Example: "SPY May 17 24 500.0 P"
    option_pattern = r'\b([A-Z]{1,5})\s+([A-Z][a-z]{2})\s+(\d{1,2})\s+(\d{2})\s+([\d.]+)\s+([CP])\b'

    lines = description.split('\n')
    for line in lines:
        match = re.search(option_pattern, line)
        if match:
            ticker = match.group(1)
            month_str = match.group(2)
            day = match.group(3)
            year = match.group(4)
            strike = match.group(5)
            option_type = match.group(6)

            # Convert month name to number
            try:
                month_num = datetime.strptime(month_str, "%b").month
                # Create date to format as YYMMDD
                expiry_date = datetime(2000 + int(year), month_num, int(day))
                expiry_formatted = expiry_date.strftime("%y%m%d")
            except ValueError:
                logger.warning(f"Could not parse option date: {month_str} {day} {year}")
                return None

            # Convert strike to 8-digit format (multiply by 1000 for 3 decimal places)
            try:
                strike_float = float(strike)
                strike_int = int(strike_float * 1000)
                strike_formatted = f"{strike_int:08d}"
            except ValueError:
                logger.warning(f"Could not parse strike price: {strike}")
                return None

            # Build OCC format: TICKER + YYMMDD + C/P + 8-digit strike
            return f"{ticker}{expiry_formatted}{option_type}{strike_formatted}"

    # If no option pattern found, return None
    logger.warning(f"Option transaction but could not format API symbol from: {description[:100]}")
    return None

def _extract_symbol_from_description(desc: str) -> Optional[str]:
    """
    DEPRECATED: Old symbol extraction function. Use _extract_symbol() instead.
    Extract symbol from description (first all-caps word 1-5 chars)
    """
    if not desc:
        return None
    # Look for all-caps ticker symbols (1-5 letters)
    match = re.search(r'\b([A-Z]{1,5})\b', desc)
    return match.group(1) if match else None

def parse_statement(file_source: Union[Path, IO[bytes]]) -> ParsedData:
    """Parses a TD Ameritrade PDF statement using table-based extraction."""
    logger.info("Parsing investment transaction data from TD Ameritrade statement...")
    investment_transactions: List[ParsedInvestmentTransaction] = []
    account_info: Optional[ParsedAccountInfo] = None

    # Table tracking variables per page
    page_horizontal_lines = {}  # Map page_num to list of y-positions
    page_end_markers = {}  # Track where activity section ends on each page
    vertical_lines = []
    activity_tables = []
    tracking_activity = False

    with pdfplumber.open(file_source) as pdf:
        # Extract account number from all pages
        for page in pdf.pages:
            lines = page.extract_text_lines()
            for line in lines:
                line_text = line['text']
                if "Statement for Account #" in line_text or "Account Number" in line_text:
                    match = re.search(r'#?\s*(\d{3,4}-\d{4,6})', line_text)
                    if match:
                        account_number = match.group(1).split('-')[-1][-4:]
                        account_info = ParsedAccountInfo(account_number_last4=account_number)
                        logger.debug(f"Found account number ending in: {account_number}")
                        break

        # Process each page for Account Activity tables
        for page_num, page in enumerate(pdf.pages):
            lines = page.extract_text_lines()

            for i, line in enumerate(lines):
                line_text = line['text'].strip()

                # Detect "Account Activity" section start
                if line_text == "Account Activity":
                    tracking_activity = True
                    logger.debug(f"Page {page_num + 1}: Found Account Activity section")
                    continue

                # Detect table header (spans 2 lines) and set up column boundaries
                if tracking_activity and "Date Date Type Cash Activity" in line_text:
                    # This is the second header line with column names
                    # Extract x positions for key columns
                    if 'chars' in line and not vertical_lines:  # Only set once
                        chars = line['chars']
                        char_count = 0
                        type_x = None
                        desc_x = None
                        qty_x = None
                        price_x = None
                        amt_x = None

                        for char in chars:
                            if char_count == line_text.find('Type') and type_x is None:
                                type_x = char['x0']
                            elif char_count == line_text.find('Description') and desc_x is None:
                                desc_x = char['x0']
                            elif char_count == line_text.find('Quantity') and qty_x is None:
                                qty_x = char['x0']
                            elif char_count == line_text.find('Price') and price_x is None:
                                price_x = char['x0']
                            elif char_count == line_text.find('Amount') and amt_x is None:
                                amt_x = char['x0']
                            char_count += 1

                        # Fixed vertical boundaries based on header analysis
                        # Header coordinates: Trade Date (32.6-52.1), Settle (79.7-99.2), Type (116.6-137.6),
                        # CashActivity (160.3-220.8), Description (291.4-340.9), Quantity (521.3-557.8),
                        # Price (601.0-623.0), Amount (662.9-696.9), Balance (736.6-771.1)
                        vertical_lines = [
                            20,      # Start of row
                            65,      # After Trade Date (between 52.1 and 79.7)
                            110,     # After Settle Date (between 99.2 and 116.6)
                            150,     # After Type (between 137.6 and 160.3)
                            290,     # After Cash Activity (just before Description at 291.4)
                            470,     # After Description (between 440.2 CUSIP and 521.3 Quantity)
                            580,     # After Quantity (between 557.8 and 601.0)
                            640,     # After Price (between 623.0 and 662.9)
                            715,     # After Amount (between 696.9 and 736.6)
                            line['x1']  # End of row (Balance)
                        ]

                        logger.debug(f"Page {page_num + 1}: Set up column boundaries: {[f'{v:.1f}' for v in vertical_lines]}")
                    continue

                # Collect ALL transaction line positions PER PAGE (lines starting with date)
                # Only collect lines that start with a date pattern (MM/DD/YY)
                if tracking_activity and vertical_lines and re.match(r'^\d{2}/\d{2}/\d{2}', line_text):
                    # Skip date range headers (e.g., "12/01/20 - 12/31/20")
                    is_date_range = re.match(r'^\d{2}/\d{2}/\d{2}\s*-\s*\d{2}/\d{2}/\d{2}', line_text)
                    # Skip commission/fee lines (they're part of multi-line descriptions)
                    if not is_date_range and "Commission/Fee" not in line_text and "Regulatory Fee" not in line_text:
                        if page_num not in page_horizontal_lines:
                            page_horizontal_lines[page_num] = []
                        page_horizontal_lines[page_num].append(line['top'])
                        logger.debug(f"Page {page_num + 1}: Collected transaction line at y={line['top']:.1f}")

                # Detect end of Account Activity section (not page breaks)
                if tracking_activity and ("Closing Balance" in line_text or "*For Cash Activity" in line_text):
                    # Store the y-position of the end marker for this page
                    page_end_markers[page_num] = line['top']
                    logger.debug(f"Page {page_num + 1}: Found end marker at y={line['top']:.1f}: {line_text[:30]}")
                    tracking_activity = False

        # After processing ALL pages, build tables for each page
        for page_num, horizontal_lines in page_horizontal_lines.items():
            if not horizontal_lines or not vertical_lines:
                continue

            page = pdf.pages[page_num]

            # Add final boundary using end marker or smart padding
            horizontal_lines_sorted = sorted(horizontal_lines)

            # If we have an end marker for this page, use it as the boundary
            # Otherwise use adaptive padding based on typical row height
            if page_num in page_end_markers:
                # Use the end marker position as the boundary (or slightly before it)
                end_boundary = page_end_markers[page_num] - 5  # 5 pixels before the marker
                logger.debug(f"Page {page_num + 1}: Using end marker at {end_boundary:.1f} as final boundary")
            else:
                # Calculate average row height for adaptive padding
                if len(horizontal_lines_sorted) > 1:
                    avg_row_height = (horizontal_lines_sorted[-1] - horizontal_lines_sorted[0]) / len(horizontal_lines_sorted)
                    # Use 2.5x average row height as padding (should capture multi-line descriptions with 4+ lines)
                    padding = min(avg_row_height * 2.5, 60)  # Cap at 60 pixels
                else:
                    padding = 60  # Default padding
                end_boundary = horizontal_lines_sorted[-1] + padding
                logger.debug(f"Page {page_num + 1}: Using adaptive padding of {padding:.1f}px, final boundary at {end_boundary:.1f}")

            horizontal_lines_sorted.append(end_boundary)

            # Build table cells for this page
            cells = []
            for h in range(len(horizontal_lines_sorted) - 1):
                for v in range(len(vertical_lines) - 1):
                    cells.append([vertical_lines[v], horizontal_lines_sorted[h],
                                vertical_lines[v+1], horizontal_lines_sorted[h+1]])

            # Create table for this page
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
        # Expected columns: Trade Date | Settle Date | Type | Cash Activity | Description | Quantity | Price | Amount | Balance
        skip_reasons = {"empty": 0, "too_few_cols": 0, "no_date": 0, "commission": 0, "date_parse_fail": 0, "no_amount": 0, "parse_error": 0}

        for row_idx, row in enumerate(all_rows):
            if not row:
                skip_reasons["empty"] += 1
                continue

            logger.debug(f"Row {row_idx}: {len(row)} columns: {[str(c)[:30] if c else 'None' for c in row]}")

            if len(row) < 9:
                skip_reasons["too_few_cols"] += 1
                continue

            try:
                # Parse columns (10 columns total)
                # Columns: [0]=Trade Date, [1]=Settle Date, [2]=Type, [3]=Cash Activity, [4]=Description, [5]=Quantity, [6]=Price, [7]=Amount, [8]=Balance
                trade_date_str = str(row[0]).strip() if row[0] else ''
                # Skip settle date (row[1])
                txn_type = str(row[2]).strip() if len(row) > 2 and row[2] else ''
                cash_activity = str(row[3]).strip() if len(row) > 3 and row[3] else ''
                description = str(row[4]).strip() if len(row) > 4 and row[4] else ''
                quantity_str = str(row[5]).strip() if len(row) > 5 and row[5] else ''
                price_str = str(row[6]).strip() if len(row) > 6 and row[6] else ''
                amount_str = str(row[7]).strip() if len(row) > 7 and row[7] else ''
                # Skip balance (row[8])

                # Skip if not a valid date
                if not re.match(r'^\d{2}/\d{2}/\d{2}', trade_date_str):
                    skip_reasons["no_date"] += 1
                    logger.warning(f"Row {row_idx} skipped - no valid date. Trade date column: '{trade_date_str[:50]}'")
                    continue

                # Parse date
                parsed_date = _parse_date(trade_date_str)
                if not parsed_date:
                    skip_reasons["date_parse_fail"] += 1
                    continue

                # Combine Type and Cash Activity to form full transaction type string
                full_transaction_type = f"{txn_type} {cash_activity}".strip() if txn_type or cash_activity else "Unknown"

                # Normalize to standard transaction type
                transaction_type = _normalize_transaction_type(full_transaction_type, description)

                # Classify security type (temporarily for symbol extraction)
                temp_security_type = _classify_security_type(full_transaction_type, description)

                # Extract symbol based on security type
                symbol = _extract_symbol(description, temp_security_type)

                # Only set security_type for BUY/SELL transactions
                security_type = None
                if transaction_type in ["BUY", "SELL"] and symbol:
                    security_type = temp_security_type

                # Format API symbol for yfinance
                api_symbol = _format_api_symbol(symbol, description, security_type) if symbol else None

                # Parse numeric fields
                clean_quantity = None
                if quantity_str and quantity_str != '-':
                    try:
                        clean_quantity = Decimal(quantity_str.replace(',', '').replace('$', '').replace('-', ''))
                    except (InvalidOperation, ValueError):
                        pass

                clean_price = None
                if price_str and price_str != '-':
                    try:
                        clean_price = Decimal(price_str.replace(',', '').replace('$', ''))
                    except (InvalidOperation, ValueError):
                        pass

                # Parse amount (handle parentheses for negative)
                if not amount_str or amount_str == '-':
                    skip_reasons["no_amount"] += 1
                    logger.warning(f"Row {row_idx} skipped - no amount. Date: {trade_date_str}, Desc: {description[:50]}, Amount column: '{amount_str}'")
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
        logger.info(f"  Skipped - Commission/Fee: {skip_reasons['commission']}")
        logger.info(f"  Skipped - Date parse failed: {skip_reasons['date_parse_fail']}")
        logger.info(f"  Skipped - No amount: {skip_reasons['no_amount']}")
        logger.info(f"  Skipped - Parse errors: {skip_reasons['parse_error']}")

        logger.info(f"Successfully parsed {len(investment_transactions)} investment transactions from TD Ameritrade statement")

    return ParsedData(account_info=account_info, investment_transactions=investment_transactions)
