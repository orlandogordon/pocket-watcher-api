"""
Ameriprise investment transaction parser
Supports both PDF and CSV formats

Features:
- Account number extraction
- Normalized transaction types (BUY, SELL, DIVIDEND, INTEREST, FEE, TRANSFER, OTHER)
- Security type classification (STOCK/OPTION)
- Symbol extraction (underlying ticker only)
- API symbol formatting (OCC format for options)
"""
import csv
import pdfplumber
import re
from pathlib import Path
from decimal import Decimal, InvalidOperation
from datetime import datetime
from typing import List, Optional, Union, IO
import io

from src.parser.models import ParsedData, ParsedInvestmentTransaction, ParsedAccountInfo, SecurityType
from src.logging_config import get_logger

logger = get_logger(__name__)


def _parse_date_csv(date_str: str) -> Optional[datetime.date]:
    """Parses a date string from CSV like 'MM/DD/YYYY'."""
    try:
        return datetime.strptime(date_str, "%m/%d/%Y").date()
    except ValueError:
        logger.warning(f"Could not parse date: {date_str}")
        return None


def _parse_date_pdf(date_str: str, statement_year: Optional[int] = None) -> Optional[datetime.date]:
    """
    Parses a date string from PDF like 'MM/DD/YYYY' or 'MM/DD/YY'
    If statement_year is provided, it will be used for 2-digit years
    """
    if not date_str or not date_str.strip():
        return None

    date_str = date_str.strip()

    # Try MM/DD/YYYY format first
    for fmt in ["%m/%d/%Y", "%m/%d/%y"]:
        try:
            parsed = datetime.strptime(date_str, fmt).date()
            # If 2-digit year and we have statement_year, use it
            if fmt == "%m/%d/%y" and statement_year and parsed.year < 2000:
                parsed = parsed.replace(year=statement_year)
            return parsed
        except ValueError:
            continue

    logger.warning(f"Could not parse date: {date_str}")
    return None


def _normalize_transaction_type(raw_type: str) -> str:
    """
    Map Ameriprise transaction types to standard types:
    BUY, SELL, DIVIDEND, INTEREST, FEE, TRANSFER, OTHER
    """
    raw_type_upper = raw_type.upper().strip()

    # BUY transactions
    if any(word in raw_type_upper for word in ['PURCHASE', 'BUY']):
        return 'BUY'

    # SELL transactions
    if any(word in raw_type_upper for word in ['SALE', 'SELL']):
        return 'SELL'

    # DIVIDEND transactions
    if 'DIVIDEND' in raw_type_upper:
        return 'DIVIDEND'

    # INTEREST transactions
    if 'INTEREST' in raw_type_upper:
        return 'INTEREST'

    # FEE transactions
    if any(word in raw_type_upper for word in ['FEE', 'BILL']):
        return 'FEE'

    # TRANSFER transactions (ACH, deposits, withdrawals)
    if any(word in raw_type_upper for word in ['ACH', 'DEPOSIT', 'WITHDRAWAL', 'TRANSFER']):
        return 'TRANSFER'

    # Everything else
    return 'OTHER'


def _classify_security_type(description: str, symbol: Optional[str], transaction_type: str) -> Optional[SecurityType]:
    """
    Classify security type based on description, symbol, and transaction type.
    Returns STOCK or OPTION for BUY/SELL transactions, None otherwise.
    """
    # Only classify for BUY/SELL transactions
    if transaction_type not in ['BUY', 'SELL']:
        return None

    if not symbol:
        return None

    desc_upper = description.upper() if description else ""

    # Check for option keywords
    option_keywords = ['CALL', 'PUT', 'OPTION', 'EXP']
    if any(keyword in desc_upper for keyword in option_keywords):
        return SecurityType.OPTION

    # If we have a symbol and it's a BUY/SELL transaction, assume it's a stock
    # (unless we already identified it as an option above)
    return SecurityType.STOCK


def _extract_symbol(description: str, security_type: Optional[SecurityType]) -> Optional[str]:
    """
    Extract underlying ticker symbol from description.
    For stocks: Returns the ticker
    For options: Returns only the underlying ticker (not the full contract)
    """
    if not description or not security_type:
        return None

    # For stocks, symbol is usually in the description after company name
    # Example: "BUY - APPLE INC" -> Symbol column has "AAPL"
    # The symbol is already provided in the CSV/PDF, so we don't need to extract it

    # For options, we'll need to parse the description
    # But typically Ameriprise provides the symbol separately

    return None  # Symbol should come from the dedicated symbol column


def _format_api_symbol(symbol: Optional[str], security_type: Optional[SecurityType],
                       description: str) -> Optional[str]:
    """
    Format API symbol for yfinance integration.
    - For stocks: Same as symbol
    - For options: OCC format (TICKER + YYMMDD + C/P + 8-digit strike)
    """
    if not symbol or security_type != SecurityType.OPTION:
        return symbol  # For stocks, api_symbol = symbol

    # For options, try to parse the option contract details from description
    # Ameriprise options in description might look like:
    # "CALL OPTION SPY EXP 05/17/2024 STRIKE 500.00"

    desc_upper = description.upper()

    # Try to extract expiration date
    exp_match = re.search(r'EXP\s+(\d{2})/(\d{2})/(\d{4})', desc_upper)
    if not exp_match:
        return symbol  # Can't format without expiration

    exp_month, exp_day, exp_year = exp_match.groups()

    # Determine call or put
    call_or_put = 'C' if 'CALL' in desc_upper else 'P' if 'PUT' in desc_upper else None
    if not call_or_put:
        return symbol

    # Try to extract strike price
    strike_match = re.search(r'STRIKE\s+(\d+\.?\d*)', desc_upper)
    if not strike_match:
        return symbol

    strike = float(strike_match.group(1))

    # Format: TICKER + YYMMDD + C/P + 8-digit strike (3 decimal places * 1000)
    strike_formatted = f"{int(strike * 1000):08d}"
    exp_formatted = f"{exp_year[-2:]}{exp_month}{exp_day}"

    return f"{symbol}{exp_formatted}{call_or_put}{strike_formatted}"


def parse_csv(file_source: Union[Path, IO[bytes]]) -> ParsedData:
    """
    Parses an Ameriprise CSV from a file path or in-memory stream.
    Enhanced with normalized types, security classification, and API symbols.
    """
    logger.info("Parsing investment transaction data from Ameriprise CSV...")
    investment_transactions: List[ParsedInvestmentTransaction] = []
    account_info: Optional[ParsedAccountInfo] = None
    account_number: Optional[str] = None

    text_stream = io.TextIOWrapper(file_source, encoding='utf-8') if isinstance(file_source, io.BytesIO) else open(file_source, 'r')

    # Read all lines
    lines = text_stream.readlines()

    # Extract account number from first line
    if lines and '"SPS ADV","' in lines[0]:
        match = re.search(r'"SPS ADV","([^"]+)"', lines[0])
        if match:
            account_number = match.group(1).strip()
            if account_number:
                account_info = ParsedAccountInfo(account_number_last4=account_number[-4:])

    # Find the header line for transactions
    header_index = 0
    for i, line in enumerate(lines):
        if 'Transaction Date' in line or line.strip().startswith('"Transaction Date"'):
            header_index = i
            break

    # Parse CSV rows starting after header
    csv_reader = csv.reader(lines[header_index + 1:])

    for row in csv_reader:
        if not row or len(row) < 7 or not row[0].strip():
            continue

        try:
            # CSV columns: Transaction Date, Account, Description, Amount, Quantity, Price, Symbol
            date_str = row[0].strip().strip('"')
            raw_description = row[2].strip().strip('"')
            amount_str = row[3].strip().strip('"').replace("$", "").replace(",", "")
            quantity_str = row[4].strip().strip('"').replace(",", "")
            price_str = row[5].strip().strip('"').replace("$", "").replace(",", "")
            symbol = row[6].strip().strip('"') or None

            # Parse the description to get transaction type
            # Format examples:
            # "DIVIDEND PAYMENT - MICROSOFT CORP 091125 1"
            # "BUY - APPLE INC"
            # "WRAP FEE BILLINGS - ASSET-BASED BILL VAL..."

            parts = raw_description.split(' - ', 1)
            raw_type = parts[0].strip()
            description = parts[1].strip() if len(parts) > 1 else raw_type

            # Normalize transaction type
            transaction_type = _normalize_transaction_type(raw_type)

            # Parse date
            parsed_date = _parse_date_csv(date_str)
            if not parsed_date:
                continue

            # Parse amounts
            # Amount can be negative (purchases) or positive (sales, dividends)
            amount = Decimal(amount_str) if amount_str else Decimal(0)
            quantity = Decimal(quantity_str) if quantity_str else None
            price = Decimal(price_str) if price_str else None

            # Classify security type (only for BUY/SELL)
            security_type = _classify_security_type(description, symbol, transaction_type)

            # Format API symbol
            api_symbol = _format_api_symbol(symbol, security_type, description)

            investment_transactions.append(
                ParsedInvestmentTransaction(
                    transaction_date=parsed_date,
                    transaction_type=transaction_type,
                    symbol=symbol,
                    description=description,
                    quantity=quantity,
                    price_per_share=price,
                    total_amount=amount,
                    security_type=security_type,
                    api_symbol=api_symbol
                )
            )
        except (ValueError, InvalidOperation, IndexError) as e:
            logger.warning(f"Skipping row in Ameriprise CSV due to parsing error: {row} -> {e}")
            continue

    if isinstance(file_source, Path):
        text_stream.close()

    logger.info(f"Successfully parsed {len(investment_transactions)} investment transactions from Ameriprise CSV")

    return ParsedData(account_info=account_info, investment_transactions=investment_transactions)


def parse_pdf(file_source: Union[Path, IO[bytes]]) -> ParsedData:
    """
    Parses an Ameriprise PDF statement using table-based approach.
    Uses fixed column boundaries to extract transaction data.
    """
    logger.info("Parsing investment transaction data from Ameriprise PDF...")
    investment_transactions: List[ParsedInvestmentTransaction] = []
    account_info: Optional[ParsedAccountInfo] = None

    # Column boundaries based on header analysis
    # Columns: Date, Transaction, Description, Symbol/CUSIP, Quantity, Price, Amount
    COL_DATE_START = 40
    COL_DATE_END = 100  # Adjusted to end right after date column
    COL_TRANSACTION_START = 100
    COL_TRANSACTION_END = 190
    COL_DESCRIPTION_START = 190
    COL_DESCRIPTION_END = 430
    COL_SYMBOL_START = 430
    COL_SYMBOL_END = 550
    COL_QUANTITY_START = 550
    COL_QUANTITY_END = 625
    COL_PRICE_START = 625
    COL_PRICE_END = 705  # Moved left by ~15 pixels
    COL_AMOUNT_START = 705
    COL_AMOUNT_END = 761  # End after "Amount" header (x1=760.50)

    with pdfplumber.open(file_source) as pdf:
        # Extract account number and statement year from any page
        account_number = None
        statement_year = None

        for page in pdf.pages:
            page_text = page.extract_text()

            # Look for account number pattern: "Account #: 0000 7595 8883 3 133"
            if not account_number:
                account_match = re.search(r'Account #:\s*([\d\s]+)', page_text)
                if account_match:
                    account_number = account_match.group(1).strip().replace(' ', '')
                    if len(account_number) >= 4:
                        account_info = ParsedAccountInfo(account_number_last4=account_number[-4:])

            # Extract statement year from header
            if not statement_year:
                year_match = re.search(r'(\d{4})\s+TO', page_text)
                if year_match:
                    statement_year = int(year_match.group(1))

        # Process each page
        for page_num, page in enumerate(pdf.pages, 1):
            text = page.extract_text()

            # Only process pages with transaction activity
            # Must have BOTH a section header AND the table headers
            has_activity_section = ("Your account activity" in text or "Trade activity" in text)
            has_table_headers = ("Date Transaction Description" in text or
                                "Date\nTransaction\nDescription" in text.replace(" ", "\n"))

            if not (has_activity_section and has_table_headers):
                continue

            logger.debug(f"Processing page {page_num}...")

            # Extract words with positions
            words = page.extract_words()

            # Find section boundaries to limit parsing to "Trade activity" section only
            # Look for keywords that mark the end of Trade activity section
            section_end_y = page.height  # Default to end of page

            for i, word in enumerate(words):
                word_text = word['text'].upper()

                # Check for "Total Securities purchased" (marks end of securities section)
                if word_text == 'TOTAL':
                    next_words = words[i+1:i+5] if i+1 < len(words) else []
                    next_text = ' '.join([w['text'].upper() for w in next_words])
                    if 'SECURITIES' in next_text and 'PURCHASED' in next_text:
                        section_end_y = word['top']
                        logger.debug(f"  Found Trade activity section end at y={section_end_y:.1f}")
                        break

            # Find transaction rows by looking for date pattern (only in Trade activity section)
            date_pattern = re.compile(r'^\d{2}/\d{2}/\d{4}$')
            transaction_rows = []

            for word in words:
                if date_pattern.match(word['text']) and COL_DATE_START <= word['x0'] < COL_DATE_END:
                    # Only include if it's BEFORE the section boundary
                    if word['top'] < section_end_y:
                        # This is a date in the date column - marks start of transaction row
                        transaction_rows.append({
                            'date_word': word,
                            'y_position': word['top']
                        })

            logger.debug(f"  Found {len(transaction_rows)} transaction rows in Trade activity section")

            # For each transaction row, extract data from columns
            for row in transaction_rows:
                y_pos = row['y_position']
                date_text = row['date_word']['text']

                # Get all words on this line (within 3 pixels vertically)
                line_words = [w for w in words if abs(w['top'] - y_pos) < 3]

                # Extract data from each column
                def get_column_text(start_x, end_x):
                    """Get all text in a column range"""
                    col_words = [w for w in line_words if start_x <= w['x0'] < end_x]
                    col_words_sorted = sorted(col_words, key=lambda x: x['x0'])
                    return ' '.join([w['text'] for w in col_words_sorted])

                # Extract fields
                date_str = date_text
                transaction = get_column_text(COL_TRANSACTION_START, COL_TRANSACTION_END)
                description = get_column_text(COL_DESCRIPTION_START, COL_DESCRIPTION_END)
                symbol = get_column_text(COL_SYMBOL_START, COL_SYMBOL_END).strip() or None
                quantity_str = get_column_text(COL_QUANTITY_START, COL_QUANTITY_END).strip()
                price_str = get_column_text(COL_PRICE_START, COL_PRICE_END).strip()
                amount_str = get_column_text(COL_AMOUNT_START, page.width).strip()

                # Parse date
                try:
                    parsed_date = datetime.strptime(date_str, "%m/%d/%Y").date()
                except ValueError:
                    logger.warning(f"  Skipping row - invalid date: {date_str}")
                    continue

                # Parse amounts
                try:
                    amount = Decimal(amount_str.replace('$', '').replace(',', ''))
                except (InvalidOperation, ValueError):
                    logger.warning(f"  Skipping row - invalid amount: {amount_str}")
                    continue

                quantity = None
                if quantity_str:
                    try:
                        quantity = Decimal(quantity_str.replace(',', ''))
                    except (InvalidOperation, ValueError):
                        pass

                price = None
                if price_str:
                    try:
                        price = Decimal(price_str.replace('$', '').replace(',', ''))
                    except (InvalidOperation, ValueError):
                        pass

                # Normalize transaction type
                transaction_type = _normalize_transaction_type(transaction)

                # Classify security type
                security_type = _classify_security_type(description, symbol, transaction_type)

                # Format API symbol
                api_symbol = _format_api_symbol(symbol, security_type, description)

                investment_transactions.append(
                    ParsedInvestmentTransaction(
                        transaction_date=parsed_date,
                        transaction_type=transaction_type,
                        symbol=symbol,
                        description=description,
                        quantity=quantity,
                        price_per_share=price,
                        total_amount=amount,
                        security_type=security_type,
                        api_symbol=api_symbol
                    )
                )

    logger.info(f"Successfully parsed {len(investment_transactions)} investment transactions from Ameriprise PDF")

    return ParsedData(account_info=account_info, investment_transactions=investment_transactions)


def parse(file_source: Union[Path, IO[bytes]], is_csv: bool = False) -> ParsedData:
    """
    Main entry point for parsing Ameriprise statements.
    Automatically detects CSV vs PDF format.
    """
    if is_csv or (isinstance(file_source, Path) and file_source.suffix.lower() == '.csv'):
        return parse_csv(file_source)
    else:
        return parse_pdf(file_source)
