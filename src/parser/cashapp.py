"""Cash App CSV statement parser.

Cash App lets users export their full activity history as CSV from the
Activity page. Columns:

    Date, Transaction ID, Transaction Type, Currency, Amount, Fee,
    Net Amount, Asset Type, Asset Price, Asset Amount, Status, Notes,
    Name of sender/receiver, Account

Only rows that affect the user's Cash App balance are emitted. P2P
rows funded by an external bank (Account = bank name, e.g. "TD Bank")
are skipped — those already appear on the funding bank's statement
and would double-count.

Cash App publishes monthly PDF statements as well, but the CSV is
strictly more complete (the PDF is a summary view), so this parser is
CSV-only for v1.
"""
import csv
import io
import re
from datetime import datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import IO, List, Optional, Union

from src.parser.models import ParsedAccountInfo, ParsedData, ParsedTransaction
from src.logging_config import get_logger

logger = get_logger(__name__)


# Cash App auto-generates Notes like "$60 Payment To Matt Mihm" for
# P2P rows where the user didn't write a memo. That's pure boilerplate
# duplicating the Name + Amount columns — strip so the description
# falls back to just the counterparty name.
_BOILERPLATE_NOTE = re.compile(r"^\$\d+(?:\.\d+)? Payment (To|From) ")


def _parse_amount(raw: str) -> Decimal:
    """Cash App amount column: '-$60.00', '$535.00'. Returns signed Decimal."""
    cleaned = raw.replace(" ", "").replace("$", "").replace(",", "")
    return Decimal(cleaned)


def _parse_date(raw: str) -> "datetime.date":
    """Cash App date: '2024-11-05 00:59:46 EST'. Drop time + tz."""
    return datetime.strptime(raw.split(" ")[0], "%Y-%m-%d").date()


def _is_balance_affecting(row_type: str, account: str) -> bool:
    """A Cash App row affects the Cash Balance when:

    - Transaction Type = "P2P" AND Account = "Cash Balance" (peer
      payment that landed in / left the balance), or
    - Transaction Type = "Withdrawal" (Cash Out from balance → bank;
      Account in this case is the destination bank, not the funding).

    Skips Account Notifications (system events, $0) and externally-
    funded P2P rows (Account = bank name) which appear on the funding
    bank's statement.
    """
    t = row_type.strip().lower()
    acct = account.strip().lower()
    if t == "withdrawal":
        return True
    if t == "p2p" and acct == "cash balance":
        return True
    return False


def _classify(row_type: str, signed_amount: Decimal) -> Optional[str]:
    """Map Cash App Type + amount sign to a TransactionType enum value.
    Returns None for unrecognized types (caller skips)."""
    t = row_type.strip().lower()
    if t == "p2p":
        return "DEPOSIT" if signed_amount > 0 else "PURCHASE"
    if t == "withdrawal":
        # Cash Out — Cash Balance → bank. Always negative amount.
        return "TRANSFER_OUT" if signed_amount < 0 else "TRANSFER_IN"
    return None


def _build_description(row_type: str, note: str, name: str, account: str) -> str:
    t = row_type.strip().lower()
    note = note.strip()
    name = name.strip()
    account = account.strip()
    if t == "withdrawal":
        return f"Cash out to {account}" if account else "Cash out"
    if _BOILERPLATE_NOTE.match(note):
        # "$60 Payment To Matt Mihm" — boilerplate that just restates
        # the Amount + Name columns. Drop it; the Name carries the
        # counterparty info.
        return name
    return f"{name}: {note}" if (name and note) else (name or note)


def parse_csv(file_source: Union[Path, IO[bytes]]) -> ParsedData:
    """Parses a Cash App activity CSV."""
    logger.info("Parsing transaction data from Cash App CSV...")
    parsed_transactions: List[ParsedTransaction] = []

    if isinstance(file_source, io.BytesIO):
        text_stream = io.TextIOWrapper(file_source, encoding="utf-8")
    else:
        text_stream = open(file_source, "r", encoding="utf-8")

    reader = csv.DictReader(text_stream)

    skipped_external = 0
    skipped_unknown = 0
    skipped_system = 0

    for row in reader:
        try:
            row_type = (row.get("Transaction Type") or "").strip()
            account = (row.get("Account") or "").strip()
            amount_raw = (row.get("Amount") or "").strip()

            if not amount_raw or not row_type:
                continue

            if row_type.lower() == "account notifications":
                skipped_system += 1
                continue

            if not _is_balance_affecting(row_type, account):
                logger.debug(
                    f"Skipping external-funded Cash App row: {row_type} {amount_raw} via {account!r}"
                )
                skipped_external += 1
                continue

            signed = _parse_amount(amount_raw)
            txn_date = _parse_date((row.get("Date") or "").strip())
            txn_type = _classify(row_type, signed)
            if txn_type is None:
                logger.warning(f"Skipping unknown Cash App row type: {row_type!r}")
                skipped_unknown += 1
                continue

            parsed_transactions.append(
                ParsedTransaction(
                    transaction_date=txn_date,
                    description=_build_description(
                        row_type,
                        row.get("Notes") or "",
                        row.get("Name of sender/receiver") or "",
                        account,
                    ),
                    amount=abs(signed),
                    transaction_type=txn_type,
                )
            )
        except (ValueError, InvalidOperation, KeyError) as e:
            logger.warning(f"Skipping row in Cash App CSV due to parsing error: {row} -> {e}")

    if isinstance(file_source, Path):
        text_stream.close()

    logger.info(
        f"Parsed {len(parsed_transactions)} transactions from Cash App CSV "
        f"(skipped {skipped_external} external-funded, "
        f"{skipped_system} system notifications, "
        f"{skipped_unknown} unknown types)"
    )
    return ParsedData(transactions=parsed_transactions, account_info=None)


def parse(file_source: Union[Path, IO[bytes]], is_csv: bool = True) -> ParsedData:
    """Cash App statements are CSV-only — the monthly PDF is a summary
    view without transaction-level detail."""
    if not is_csv:
        logger.warning("Cash App only supports CSV imports — proceeding as CSV")
    return parse_csv(file_source)
