import csv
import re
import pdfplumber
from collections import Counter
from pathlib import Path
from decimal import Decimal, InvalidOperation
from datetime import datetime
from typing import List, Optional, IO, Union
import io

from src.parser.models import (
    ParsedData,
    ParsedTransaction,
    ParsedAccountInfo,
    reconcile_statement_balance,
)
from src.logging_config import get_logger

logger = get_logger(__name__)

# A list of month prefixes to identify transaction lines
DATES = ['01/', '02/', '03/', '04/', '05/', '06/', '07/', '08/', '09/', '10/', '11/', '12/']

# Account-summary balance anchors for post-parse reconciliation (todo #78). Amex
# interleaves these with disclosure prose and prints a decoy all-$0.00 "Minimum/
# Late Payment Warning" example block plus (some layouts) a Pay-Over-Time
# sub-balance breakdown, so the real Previous/New Balance is not the only match.
# Empirically robust across every layout 2022→2026: the *last* Previous Balance
# occurrence is the real opening, and the *most frequent* New Balance value is the
# real closing (the real figure repeats 2-3x; the $0.00 decoy appears once).
_PREV_BALANCE_RE = re.compile(r'Previous Balance\s+\$?([\d,]+\.\d{2})')
_NEW_BALANCE_RE = re.compile(r'New Balance\s*=?\s*\$?([\d,]+\.\d{2})')

def _clean_description(description: str) -> str:
    """Strip Amex-specific noise from a parsed description:

    - `AplPay ` prefix — Apple Pay tag. Without stripping, merchant extraction
      downstream resolves to "Aplpay" instead of the actual merchant, which
      is worse than no merchant at all because the LLM gets misleading input.
    - ` Pay Over Time` suffix — added when the user enables Pay-Over-Time on
      the charge. Pure UI/LLM noise; doesn't change the underlying merchant.
    """
    description = description.removeprefix("AplPay ")
    description = description.removesuffix(" Pay Over Time")
    return description


# The Amex "account activity" CSV export packs each Description as a fixed-width
# record: a left-justified, space-padded merchant field, then a left-justified
# city, then the state. The city always begins at this column (the merchant area
# — including any "AplPay " prefix — is 20 chars wide). The merchant/city
# boundary has NO separator, so a merchant longer than the field gets truncated
# mid-name and runs straight into the city with no space (a "BRANDNAME" field
# followed by "CITY" arrives as "BRANDNAMECITY"). PDF statements are not packed
# this way.
_ACTIVITY_CITY_COL = 20


def _depack_activity_csv(raw: str) -> tuple[str, bool]:
    """Split the Amex activity-CSV fixed-width Description into a clean,
    single-spaced "<merchant> <city> <state>" string and report whether the
    merchant field was full — i.e. truncated mid-name, so the real brand is
    unrecoverable and the caller should blank the merchant (-> Needs Review).

    Only rows carrying the tell-tale column padding (2+ consecutive spaces) are
    treated as this format; anything else (e.g. "Amex Send: Add Money") is
    returned unchanged with truncated=False.
    """
    if "  " not in raw or len(raw) <= _ACTIVITY_CITY_COL:
        return raw, False
    # A non-space in the field's last column means the name filled (overran) the
    # field and was cut off, gluing onto the city.
    truncated = raw[_ACTIVITY_CITY_COL - 1] != " "
    merchant = raw[:_ACTIVITY_CITY_COL]
    city_state = raw[_ACTIVITY_CITY_COL:]
    # Re-join across the boundary with a single space, collapsing the field
    # padding (and any internal padding like "BRAND  NAME").
    clean = re.sub(r"\s+", " ", f"{merchant} {city_state}").strip()
    return clean, truncated


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
        logger.warning(f"Could not parse date: {date_str} - Error: {e}")
        return None

def parse_statement(file_source: Union[Path, IO[bytes]]) -> ParsedData:
    """Parses an Amex PDF statement from a file path or an in-memory stream."""
    logger.info("Parsing transaction data from Amex statement...")
    parsed_transactions: List[ParsedTransaction] = []
    account_number: Optional[str] = None
    year_map = {}

    with pdfplumber.open(file_source) as pdf:
        # Join pages with '\n' so a section banner sitting on a page boundary
        # (e.g. "Fees" as the last line of page 7) does not get concatenated
        # onto the first line of the next page ("Date Description Type Amount"),
        # which would defeat exact-string banner matching below.
        text = '\n'.join((p.extract_text(x_tolerance=2) or '') for p in pdf.pages)
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
        logger.debug("Could not determine year from statement. Using current year as fallback.")
        current_year = str(datetime.now().year)
        for m in DATES:
            year_map[m.strip('/')] = current_year

    tracking_payments, tracking_credits, tracking_purchases, tracking_fees, tracking_interest = [False] * 5

    for line in lines:
        stripped = line.strip()
        # Check for section markers. Multiple Amex layouts in the wild:
        #   pre-2025:           "Payments t Amount" / "Payments Amount" / "Credits Amount"
        #   Oct 2024–Jun 2025:  "Payments Details" / "Credits Details"
        #   2022/2023/2024 with annual membership fee: "Fees - denotes Pay Over Time..."
        #   2024–2025 with footnote diamond:           "Fees ⧫ - Pay Over Time..."
        if "Payments t Amount" in line or "Payments Amount" in line or stripped == "Payments Details":
            tracking_payments, tracking_credits, tracking_purchases, tracking_fees, tracking_interest = [True, False, False, False, False]
            continue
        elif "Credits Amount" in line or stripped == "Credits Details":
            tracking_payments, tracking_credits, tracking_purchases, tracking_fees, tracking_interest = [False, True, False, False, False]
            continue
        elif ("Total New Charges" in line and "$" in line) or stripped == "Cash Advances":
            # Cash Advances is a separate body section; treat its rows as Purchase
            # so the credit-card balance update is correct (cash advances increase
            # balance like purchases).
            tracking_payments, tracking_credits, tracking_purchases, tracking_fees, tracking_interest = [False, False, True, False, False]
            continue
        elif stripped == "Fees" or stripped.startswith("Fees -") or stripped.startswith("Fees ⧫"):
            tracking_payments, tracking_credits, tracking_purchases, tracking_fees, tracking_interest = [False, False, False, True, False]
            continue
        elif "Interest Charged" in line and stripped == "Interest Charged":
            tracking_payments, tracking_credits, tracking_purchases, tracking_fees, tracking_interest = [False, False, False, False, True]
            continue

        if not line or line[0:3] not in DATES:
            continue
        # Skip date-prefixed non-transaction lines (payment due dates, hotel arrival/departure) — real txn lines end in $X.XX.
        if "$" not in line:
            continue

        try:
            line_split = line.split()
            date_str = line_split[0]
            parsed_date = _parse_date(date_str, year_map)
            if not parsed_date:
                continue

            amount_str = line_split[-1].replace("$", "").replace(",", "").replace("⧫", "").replace("â§«", "")
            amount = abs(Decimal(amount_str))
            description = _clean_description(" ".join(line_split[1:-1]))

            transaction_type = ""
            if tracking_payments: transaction_type = "TRANSFER_IN"
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
            logger.warning(f"Skipping a row in AMEX statement due to parsing error: {line} -> {e}")
            continue

    # Reconcile against the statement's own begin/end balance — an Amex card is a
    # liability, so charges (Purchase/Fee/Interest) raise the balance owed and
    # payments/credits (TRANSFER_IN/Credit) lower it. A numeric mismatch returns a
    # non-fatal warning on ParsedData (import-and-flag); an unclassified type still
    # raises (todo #78). Only runs when both anchors were found.
    reconciliation = None
    prev_balances = [Decimal(m.replace(',', '')) for m in _PREV_BALANCE_RE.findall(text)]
    new_balances = [Decimal(m.replace(',', '')) for m in _NEW_BALANCE_RE.findall(text)]
    if prev_balances and new_balances:
        opening = prev_balances[-1]
        closing = Counter(new_balances).most_common(1)[0][0]
        reconciliation = reconcile_statement_balance(
            parsed_transactions,
            expected_net_change=closing - opening,
            credit_types=frozenset({"PURCHASE", "FEE", "INTEREST"}),
            debit_types=frozenset({"TRANSFER_IN", "CREDIT"}),
            context="Amex statement",
        )

    account_info = ParsedAccountInfo(account_number_last4=account_number.replace("-", "")) if account_number else None

    return ParsedData(
        account_info=account_info,
        transactions=parsed_transactions,
        reconciliation=reconciliation,
    )

def parse_csv(file_source: Union[Path, IO[bytes]]) -> ParsedData:
    """Parses an Amex CSV from a file path or an in-memory stream."""
    logger.info("Parsing transaction data from AMEX csv...")
    parsed_transactions: List[ParsedTransaction] = []
    account_info: Optional[ParsedAccountInfo] = None # CSVs don't contain account info

    # The CSV reader needs a text-based stream, not a byte stream
    text_stream = io.TextIOWrapper(file_source, encoding='utf-8') if isinstance(file_source, io.BytesIO) else open(file_source, 'r')

    reader = csv.reader(text_stream)
    next(reader)  # Skip header row

    for row in reader:
        try:
            date = datetime.strptime(row[0], "%m/%d/%Y").date()
            depacked, merchant_truncated = _depack_activity_csv(row[1])
            description = _clean_description(depacked)
            amount = Decimal(row[2])

            transaction_type = 'Credit' if amount < 0 else 'Purchase'
            amount = abs(amount) # Amount should always be positive

            parsed_transactions.append(
                ParsedTransaction(
                    transaction_date=date,
                    description=description.strip(),
                    amount=amount,
                    transaction_type=transaction_type,
                    merchant_truncated=merchant_truncated,
                )
            )
        except (ValueError, InvalidOperation, IndexError) as e:
            logger.warning(f"Skipping a row in AMEX CSV due to parsing error: {row} -> {e}")
            continue
    
    if isinstance(file_source, Path):
        text_stream.close()

    logger.info(f"Parsed {len(parsed_transactions)} transactions from AMEX CSV")
    return ParsedData(transactions=parsed_transactions, account_info=account_info)

def parse(file_source: Union[Path, IO[bytes]], is_csv: bool = False) -> ParsedData:
    """
    Parses a Amex statement (PDF or CSV) from a file path or in-memory stream.
    """
    if is_csv:
        return parse_csv(file_source)
    else:
        return parse_statement(file_source)
