"""Venmo CSV statement parser.

Venmo monthly statement format (Statements page → Download CSV):

    Row 0: "Account Statement - (@username)"
    Row 1: "Account Activity"
    Row 2: column headers
    Row 3: beginning-balance row (single $ value at column 16)
    Rows 4..N-2: transaction rows
    Row N-1: ending-balance row + multi-line disclaimer

Only rows that affect the user's Venmo balance are emitted. Rows
funded by an external card or bank (e.g. paying someone with a Visa
*through* Venmo) are skipped — those already appear on the funding
account's own statement and re-importing them double-counts.

Venmo does not publish PDFs with transaction-level detail, so this
parser is CSV-only.
"""
import csv
import io
from datetime import datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import IO, List, Optional, Union

from src.parser.models import ParsedAccountInfo, ParsedData, ParsedTransaction
from src.logging_config import get_logger

logger = get_logger(__name__)


# Header row index map (after the leading empty column at index 0):
#   ID, Datetime, Type, Status, Note, From, To, Amount(total), ...,
#   Funding Source, Destination, ...
_COL = {
    "id": 1,
    "datetime": 2,
    "type": 3,
    "status": 4,
    "note": 5,
    "from": 6,
    "to": 7,
    "amount": 8,
    "funding_source": 14,
    "destination": 15,
}


def _parse_amount(raw: str) -> Decimal:
    """Venmo amount column: '+ $45.00', '- $1,612.62'. Returns signed Decimal."""
    cleaned = raw.replace(" ", "").replace("$", "").replace(",", "")
    return Decimal(cleaned)


def _is_balance_affecting(funding_source: str, destination: str) -> bool:
    """A Venmo row affects the user's Venmo balance when either:

    - destination is 'Venmo balance' (inflow lands in the balance), or
    - funding source is blank / 'Venmo balance' (outflow leaves the
      balance, including Standard Transfer cashouts where funding is
      implicit-blank and destination is a bank).

    External-funded outflows (Funding Source = Visa/Amex/bank, with no
    Venmo balance touch) are not balance-affecting — they appear on
    the funding account's statement and would double-count if imported
    here.
    """
    fs = funding_source.strip().lower()
    dest = destination.strip().lower()
    if dest == "venmo balance":
        return True
    if fs in ("", "venmo balance"):
        return True
    return False


def _classify(row_type: str, signed_amount: Decimal) -> Optional[str]:
    """Map Venmo Type + amount sign to a TransactionType enum value.
    Returns None for unrecognized types (caller skips)."""
    t = row_type.strip().lower()
    if t in ("payment", "charge", "merchant transaction", "card payment"):
        # "Card Payment" is a Venmo-internal type for Venmo-Debit-Card-related
        # credits/reversals into the balance. Direction follows the sign.
        return "DEPOSIT" if signed_amount > 0 else "PURCHASE"
    if t in ("standard transfer", "instant transfer"):
        # Cashout — Venmo balance → bank. Instant has a small fee; both are
        # outflows from balance.
        return "TRANSFER_OUT" if signed_amount < 0 else "TRANSFER_IN"
    if t in ("top up", "add money"):
        # Bank → Venmo. Positive from Venmo's perspective.
        return "TRANSFER_IN"
    if t == "direct deposit":
        return "DEPOSIT"
    return None


def _build_description(
    row_type: str,
    note: str,
    from_party: str,
    to_party: str,
    destination: str,
) -> str:
    note = note.strip()
    t = row_type.strip().lower()
    if t in ("standard transfer", "instant transfer"):
        dest = destination.strip()
        return f"Cash out to {dest}" if dest else "Cash out"
    if t == "card payment":
        # Venmo emits placeholder strings ('Card Payment', user's name, an
        # internal ref code) for From / To / Note. None of that is
        # user-meaningful, so use a stable label instead.
        return "Venmo Card Payment credit"
    parties = " → ".join(p for p in (from_party.strip(), to_party.strip()) if p)
    return f"{parties}: {note}" if (parties and note) else (parties or note)


def parse_csv(file_source: Union[Path, IO[bytes]]) -> ParsedData:
    """Parses a Venmo monthly statement CSV."""
    logger.info("Parsing transaction data from Venmo CSV...")
    parsed_transactions: List[ParsedTransaction] = []

    if isinstance(file_source, io.BytesIO):
        text_stream = io.TextIOWrapper(file_source, encoding="utf-8")
    else:
        text_stream = open(file_source, "r", encoding="utf-8")

    reader = csv.reader(text_stream)
    rows = list(reader)

    skipped_external = 0
    skipped_unknown = 0

    # Rows 0-2 are header/metadata; row 3 is the beginning-balance marker.
    # Real transactions start at row 4.
    for row in rows[3:]:
        if len(row) < 16:
            continue
        if not row[_COL["datetime"]].strip():
            # Ending-balance row + disclaimer rows have no datetime.
            continue

        try:
            txn_date = datetime.strptime(
                row[_COL["datetime"]].strip(), "%Y-%m-%dT%H:%M:%S"
            ).date()
            row_type = row[_COL["type"]].strip()
            note = row[_COL["note"]].strip()
            from_party = row[_COL["from"]].strip()
            to_party = row[_COL["to"]].strip()
            amount_raw = row[_COL["amount"]].strip()
            funding_source = row[_COL["funding_source"]].strip()
            destination = row[_COL["destination"]].strip()

            if not amount_raw:
                continue

            signed = _parse_amount(amount_raw)

            if not _is_balance_affecting(funding_source, destination):
                logger.debug(
                    f"Skipping external-funded Venmo row: {row_type} {amount_raw} "
                    f"funded by {funding_source!r}"
                )
                skipped_external += 1
                continue

            txn_type = _classify(row_type, signed)
            if txn_type is None:
                logger.warning(f"Skipping unknown Venmo row type: {row_type!r}")
                skipped_unknown += 1
                continue

            parsed_transactions.append(
                ParsedTransaction(
                    transaction_date=txn_date,
                    description=_build_description(
                        row_type, note, from_party, to_party, destination
                    ),
                    amount=abs(signed),
                    transaction_type=txn_type,
                )
            )
        except (ValueError, InvalidOperation, IndexError) as e:
            logger.warning(f"Skipping row in Venmo CSV due to parsing error: {row} -> {e}")

    if isinstance(file_source, Path):
        text_stream.close()

    logger.info(
        f"Parsed {len(parsed_transactions)} transactions from Venmo CSV "
        f"(skipped {skipped_external} external-funded, {skipped_unknown} unknown types)"
    )
    return ParsedData(transactions=parsed_transactions, account_info=None)


def parse(file_source: Union[Path, IO[bytes]], is_csv: bool = True) -> ParsedData:
    """Venmo statements are CSV-only — the PDF format Venmo offers is a
    summary view without transaction-level data."""
    if not is_csv:
        logger.warning("Venmo only supports CSV imports — proceeding as CSV")
    return parse_csv(file_source)
