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
import fitz  # PyMuPDF — reads both old (Type1) and new (Type0/Type3) Ameriprise PDFs
import re
from pathlib import Path
from decimal import Decimal, InvalidOperation
from datetime import datetime
from typing import List, Optional, Union, IO
import io

from src.parser.models import ParsedData, ParsedInvestmentTransaction, ParsedAccountInfo, SecurityType, classify_security_type
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


def _normalize_transaction_type(raw_type: str, description: str = "", amount: Optional[Decimal] = None) -> str:
    """
    Map Ameriprise transaction types to standard types:
    BUY, SELL, DIVIDEND, INTEREST, FEE, TRANSFER_IN, TRANSFER_OUT, OTHER

    Transfer direction (ACH / deposit / withdrawal / JOURNAL) comes from the
    SIGNED amount, not the description: Ameriprise labels an ACH pull *into* the
    account "ACH DIRECT WITHDRAWAL" under "Deposits" with a positive amount, so
    keyword-matching the description would mislabel deposits as withdrawals.
    JOURNAL rows move cash/positions between a client's own re-numbered
    sub-accounts during restructurings; symmetric in/out legs net to zero.
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

    # Transfer direction comes from the signed amount (see docstring).
    if any(word in raw_type_upper for word in ['ACH', 'DEPOSIT', 'WITHDRAWAL', 'TRANSFER', 'DISBURS', 'JOURNAL']):
        if amount is not None:
            return 'TRANSFER_OUT' if amount < 0 else 'TRANSFER_IN'
        # No amount context — fall back to an explicit direction word in the type.
        if 'WITHDRAWAL' in raw_type_upper or 'DISBURS' in raw_type_upper:
            return 'TRANSFER_OUT'
        return 'TRANSFER_IN'

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

    # Classify non-option securities (STOCK, ETF, MUTUAL_FUND)
    return classify_security_type(symbol)


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

            # Parse amounts first — the sign disambiguates transfer direction.
            # Amount can be negative (purchases) or positive (sales, dividends)
            amount = Decimal(amount_str) if amount_str else Decimal(0)
            quantity = Decimal(quantity_str) if quantity_str else None
            price = Decimal(price_str) if price_str else None

            # Pass raw_description so money-market / REINVEST-AT detection sees the full text.
            transaction_type, quantity, price, skip = _classify_row(
                raw_type, raw_description, amount, quantity, price
            )
            if skip:
                logger.debug(f"Skipping money-market sweep: {raw_description}")
                continue

            # Parse date
            parsed_date = _parse_date_csv(date_str)
            if not parsed_date:
                continue

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


def _classify_row(
    raw_type: str,
    description: str,
    amount: Optional[Decimal],
    quantity: Optional[Decimal],
    price: Optional[Decimal],
):
    """
    Apply Ameriprise-specific classification on top of _normalize_transaction_type.

    Returns ``(transaction_type, quantity, price, skip)``:
      - ``REINVEST DIV`` (security dividend reinvestment / DRIP) → ``BUY``,
        deriving qty/price from the "REINVEST AT <price>" text when absent. The
        paired ``DIVIDEND`` row books the cash; modelling the reinvest as a BUY
        (engine: −cash, +shares) nets cash to zero while growing the position.
      - A ``FEE`` with a positive (credit) amount is a fee rebate; the snapshot
        engine's FEE path only subtracts, so credits route through ``TRANSFER_IN``.
      - Any "MONEY MARKET" row except INTEREST is internal cash-sweep mechanics
        (purchases/sales/reinvestments of the insured money market account) and
        is skipped; real money-market INTEREST income is kept.
    """
    raw_u = (raw_type or "").upper()
    desc_u = (description or "").upper()

    if 'REINVEST' in raw_u:
        transaction_type = 'BUY'
        if price is None:
            m = re.search(r'REINVEST AT\s+([\d.,]+)', desc_u)
            if m:
                try:
                    price = Decimal(m.group(1).replace(',', ''))
                except InvalidOperation:
                    price = None
        if (quantity is None or quantity == 0) and price and amount is not None and price != 0:
            quantity = abs(amount) / price
    else:
        transaction_type = _normalize_transaction_type(raw_type, description, amount)

    if transaction_type == 'FEE' and amount is not None and amount > 0:
        transaction_type = 'TRANSFER_IN'

    if 'MONEY MARKET' in desc_u and transaction_type != 'INTEREST':
        return transaction_type, quantity, price, True

    return transaction_type, quantity, price, False


def _group_lines(words, tol: float = 2.5):
    """
    Cluster fitz word tuples ``(x0, y0, x1, y1, text, ...)`` into visual rows by
    their top (y0) within ``tol`` pixels, each row's words sorted left-to-right.
    Mirrors the prior pdfplumber "within 3px" row assembly.
    """
    rows: List[dict] = []
    for w in sorted(words, key=lambda t: (t[1], t[0])):
        for row in rows:
            if abs(row['top'] - w[1]) <= tol:
                row['words'].append(w)
                break
        else:
            rows.append({'top': w[1], 'words': [w]})
    for row in rows:
        row['words'].sort(key=lambda t: t[0])
    return rows


def parse_pdf(file_source: Union[Path, IO[bytes]]) -> ParsedData:
    """
    Parses an Ameriprise PDF statement using PyMuPDF (fitz) word geometry.

    fitz extracts text from both the old (Type1 + FontFile3) and the newer
    (Type0/Type3, no embedded font program) statement generators; pdfplumber/
    pdfminer decoded almost nothing from the latter, silently yielding 0
    transactions (#76). The fixed column boundaries below are identical across
    both formats.
    """
    logger.info("Parsing investment transaction data from Ameriprise PDF...")
    investment_transactions: List[ParsedInvestmentTransaction] = []
    account_info: Optional[ParsedAccountInfo] = None

    # Column boundaries (word x0): Date, Transaction, Description, Symbol/CUSIP,
    # Quantity, Price, Amount. Verified identical for the old & new formats.
    COL_DATE_START, COL_DATE_END = 40, 100
    COL_TRANSACTION_START, COL_TRANSACTION_END = 100, 190
    COL_DESCRIPTION_START, COL_DESCRIPTION_END = 190, 430
    COL_SYMBOL_START, COL_SYMBOL_END = 430, 550
    COL_QUANTITY_START, COL_QUANTITY_END = 550, 625
    COL_PRICE_START, COL_PRICE_END = 625, 705
    COL_AMOUNT_START = 705

    # fitz opens a filesystem path or an in-memory byte stream
    if isinstance(file_source, (str, Path)):
        doc = fitz.open(str(file_source))
    else:
        try:
            file_source.seek(0)
        except (AttributeError, OSError):
            pass
        doc = fitz.open(stream=file_source.read(), filetype="pdf")

    date_pattern = re.compile(r'^\d{2}/\d{2}/\d{4}$')

    def get_column_text(line_words, start_x, end_x):
        cells = [w for w in line_words if start_x <= w[0] < end_x]
        return ' '.join(w[4] for w in sorted(cells, key=lambda t: t[0]))

    try:
        full_text = "\n".join(page.get_text() for page in doc)

        # Account number: "Account #: 0000 7595 8883 3 133"
        acct_match = re.search(r'Account #:\s*([\d\s]+)', full_text)
        if acct_match:
            account_number = acct_match.group(1).strip().replace(' ', '')
            if len(account_number) >= 4:
                account_info = ParsedAccountInfo(account_number_last4=account_number[-4:])

        # Statement year from header (e.g. "AUG 01, 2025 TO AUG 31, 2025")
        statement_year = None
        year_match = re.search(r'(\d{4})\s+TO', full_text)
        if year_match:
            statement_year = int(year_match.group(1))

        for page_num, page in enumerate(doc, 1):
            words = page.get_text("words")  # (x0, y0, x1, y1, text, block, line, word)
            if not words:
                continue

            lines = _group_lines(words)

            # Activity pages carry a Date+Transaction+Description header row.
            header_tops = [
                row['top'] for row in lines
                if {'Date', 'Transaction', 'Description'} <= {w[4] for w in row['words']}
            ]
            if not header_tops:
                continue
            first_header_top = min(header_tops)

            # Stop at the next "Date"-labeled sub-table (money-market sweep / gain-loss detail).
            label_tops = [
                row['top'] for row in lines
                if any(w[4] == 'Date' and COL_DATE_START - 2 <= w[0] < COL_DATE_END for w in row['words'])
                and row['top'] > first_header_top + 1
            ]
            section_end_top = min(label_tops) if label_tops else float('inf')

            logger.debug(f"Processing page {page_num}...")

            for row in lines:
                if row['top'] <= first_header_top or row['top'] >= section_end_top:
                    continue
                line_words = row['words']
                date_cells = [
                    w for w in line_words
                    if date_pattern.match(w[4]) and COL_DATE_START <= w[0] < COL_DATE_END
                ]
                if not date_cells:
                    continue

                date_str = date_cells[0][4]
                transaction = get_column_text(line_words, COL_TRANSACTION_START, COL_TRANSACTION_END)
                description = get_column_text(line_words, COL_DESCRIPTION_START, COL_DESCRIPTION_END)
                symbol = get_column_text(line_words, COL_SYMBOL_START, COL_SYMBOL_END).strip() or None
                quantity_str = get_column_text(line_words, COL_QUANTITY_START, COL_QUANTITY_END).strip()
                price_str = get_column_text(line_words, COL_PRICE_START, COL_PRICE_END).strip()
                amount_str = get_column_text(line_words, COL_AMOUNT_START, page.rect.width).strip()

                parsed_date = _parse_date_pdf(date_str, statement_year)
                if not parsed_date:
                    continue

                try:
                    amount = Decimal(amount_str.replace('$', '').replace(',', ''))
                except (InvalidOperation, ValueError):
                    logger.warning(f"  Skipping row - invalid amount: {amount_str!r} ({transaction})")
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

                transaction_type, quantity, price, skip = _classify_row(
                    transaction, description, amount, quantity, price
                )
                if skip:
                    logger.debug(f"  Skipping money-market sweep: {transaction} - {description} ({amount})")
                    continue

                security_type = _classify_security_type(description, symbol, transaction_type)
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
    finally:
        doc.close()

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
