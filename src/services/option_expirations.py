"""Auto-detect option contracts that expired without an EXPIRATION
transaction recorded.

Background: brokerage statement parsers often emit BUY/SELL rows but not
EXPIRATION rows (brokerages don't always report worthless OTM expirations).
Without an EXPIRATION the snapshot replay leaves the position open
indefinitely and the net-worth chart shows a phantom held contract forever.

Sweep logic per OCC-parseable api_symbol whose expiration is in the past:
- Replay txns up to expiration_date. If the contract still shows
  quantity > 0 there, it's an orphan.
- Look up the underlying close on expiration_date. Intrinsic > 0 = ITM;
  intrinsic == 0 = OTM; no underlying price = UNKNOWN.
- OTM: synthesize an EXPIRATION transaction with $0 proceeds (idempotent
  hash so re-runs don't duplicate). The replay then zeros the position
  from expiration forward and the snapshot is correct.
- ITM / UNKNOWN: flagged for manual review. A broker's default ITM
  behavior is auto-exercise into shares — silently writing $0 would lose
  value that converted into stock, so the user must reconcile by hand.

See backend todo #57.
"""
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from typing import List, Literal, Optional, Tuple
from uuid import uuid4

from sqlalchemy.orm import Session

from src.crud.crud_investment import generate_investment_transaction_hash
from src.db.core import InvestmentTransactionDB, InvestmentTransactionType
from src.logging_config import get_logger
from src.parser.models import ParsedInvestmentTransaction
from src.services.account_snapshot import get_account_state_on_date
from src.services.price_fetcher import (
    fetch_stock_price_historical,
    is_option_symbol,
    parse_option_symbol,
)

logger = get_logger(__name__)

Status = Literal["OTM", "ITM", "UNKNOWN"]
SYNTHETIC_EXPIRATION_DESCRIPTION = "Auto-detected expiration (OTM, $0 proceeds)"


@dataclass(frozen=True)
class OrphanExpiration:
    """One option contract that has expired with no closing transaction."""
    api_symbol: str
    underlying: str
    account_id: int
    user_id: int
    expiration_date: date
    quantity: Decimal
    avg_cost_basis: Decimal  # per-underlying-share basis from position replay
    underlying_close: Optional[Decimal]
    status: Status


@dataclass
class SweepSummary:
    orphans: List[OrphanExpiration]
    created: int = 0           # synthetic EXPIRATION rows written
    skipped_existing: int = 0  # OTM whose synth row already existed
    flagged_itm: int = 0       # ITM, left alone
    flagged_unknown: int = 0   # no underlying price, left alone


def _compute_intrinsic(option_type: str, strike: Decimal, underlying_close: Decimal) -> Decimal:
    """Per-share intrinsic value of the contract at expiration."""
    if option_type == "CALL":
        return max(Decimal("0"), underlying_close - strike)
    return max(Decimal("0"), strike - underlying_close)


def _classify(option_type: str, strike: Decimal, underlying_close: Optional[Decimal]) -> Status:
    """OTM if intrinsic == 0, ITM if > 0, UNKNOWN if no underlying price."""
    if underlying_close is None:
        return "UNKNOWN"
    return "OTM" if _compute_intrinsic(option_type, strike, underlying_close) == 0 else "ITM"


def _find_accounts_for_symbol(db: Session, api_symbol: str) -> List[Tuple[int, int]]:
    """(account_id, user_id) pairs that have transacted this contract."""
    return (
        db.query(InvestmentTransactionDB.account_id, InvestmentTransactionDB.user_id)
        .filter(
            InvestmentTransactionDB.api_symbol == api_symbol,
            InvestmentTransactionDB.security_type == "OPTION",
            InvestmentTransactionDB.account_id.isnot(None),
        )
        .distinct()
        .all()
    )


def find_orphan_expirations(
    db: Session, today: Optional[date] = None
) -> List[OrphanExpiration]:
    """Enumerate orphans across all accounts, sorted by (account_id,
    api_symbol) for deterministic output."""
    today = today or date.today()

    distinct_symbols = (
        db.query(InvestmentTransactionDB.api_symbol)
        .filter(
            InvestmentTransactionDB.security_type == "OPTION",
            InvestmentTransactionDB.api_symbol.isnot(None),
        )
        .distinct()
        .all()
    )

    orphans: List[OrphanExpiration] = []
    for (api_symbol,) in distinct_symbols:
        if not is_option_symbol(api_symbol):
            continue
        parsed = parse_option_symbol(api_symbol)
        if parsed is None:
            continue
        expiration = date.fromisoformat(parsed["expiration"])
        # Strictly past — a same-day expiration may still get a real
        # EXPIRATION row from a later import.
        if expiration >= today:
            continue
        strike = Decimal(str(parsed["strike"]))
        option_type = parsed["option_type"]

        for account_id, user_id in _find_accounts_for_symbol(db, api_symbol):
            state = get_account_state_on_date(db, account_id, expiration)
            holding = state["holdings"].get(api_symbol)
            if not holding or holding["quantity"] <= 0:
                continue

            underlying_close = fetch_stock_price_historical(parsed["underlying"], expiration)
            orphans.append(OrphanExpiration(
                api_symbol=api_symbol,
                underlying=parsed["underlying"],
                account_id=account_id,
                user_id=user_id,
                expiration_date=expiration,
                quantity=holding["quantity"],
                avg_cost_basis=holding["average_cost_basis"],
                underlying_close=underlying_close,
                status=_classify(option_type, strike, underlying_close),
            ))

    orphans.sort(key=lambda o: (o.account_id, o.api_symbol))
    return orphans


def _synthetic_parsed_txn(orphan: OrphanExpiration) -> ParsedInvestmentTransaction:
    """ParsedInvestmentTransaction shape used by the hash function. Fields
    are fixed so the hash is deterministic across re-runs."""
    return ParsedInvestmentTransaction(
        transaction_date=orphan.expiration_date,
        transaction_type=InvestmentTransactionType.EXPIRATION.value,
        symbol=orphan.underlying,
        api_symbol=orphan.api_symbol,
        description=SYNTHETIC_EXPIRATION_DESCRIPTION,
        quantity=orphan.quantity,
        price_per_share=Decimal("0"),
        total_amount=Decimal("0"),
        security_type=None,
    )


def create_synthetic_expiration(
    db: Session, orphan: OrphanExpiration
) -> Optional[InvestmentTransactionDB]:
    """Insert a $0 EXPIRATION row for the orphan. Returns None (and inserts
    nothing) if a transaction with the same deterministic hash already
    exists — re-runs are safe."""
    parsed = _synthetic_parsed_txn(orphan)
    txn_hash = generate_investment_transaction_hash(
        parsed, user_id=orphan.user_id, account_id=orphan.account_id
    )

    existing = (
        db.query(InvestmentTransactionDB)
        .filter(InvestmentTransactionDB.transaction_hash == txn_hash)
        .first()
    )
    if existing is not None:
        return None

    txn = InvestmentTransactionDB(
        uuid=uuid4(),
        user_id=orphan.user_id,
        account_id=orphan.account_id,
        transaction_hash=txn_hash,
        transaction_type=InvestmentTransactionType.EXPIRATION,
        symbol=orphan.underlying,
        api_symbol=orphan.api_symbol,
        quantity=orphan.quantity,
        price_per_share=Decimal("0"),
        total_amount=Decimal("0"),
        transaction_date=orphan.expiration_date,
        description=SYNTHETIC_EXPIRATION_DESCRIPTION,
        security_type="OPTION",
        created_at=datetime.utcnow(),
    )
    db.add(txn)
    db.commit()
    db.refresh(txn)
    return txn


def sweep(
    db: Session, *, dry_run: bool, today: Optional[date] = None
) -> SweepSummary:
    """Find orphans and (unless dry_run) auto-create EXPIRATION rows for
    OTM cases. ITM / UNKNOWN are surfaced in the summary for manual
    review, never auto-zeroed."""
    orphans = find_orphan_expirations(db, today=today)
    summary = SweepSummary(orphans=orphans)

    for orphan in orphans:
        if orphan.status == "ITM":
            summary.flagged_itm += 1
            logger.warning(
                "ITM at expiration — manual review needed: account=%s symbol=%s "
                "qty=%s underlying close on %s = %s (likely auto-exercised into "
                "shares; do not silently zero)",
                orphan.account_id, orphan.api_symbol, orphan.quantity,
                orphan.expiration_date, orphan.underlying_close,
            )
            continue
        if orphan.status == "UNKNOWN":
            summary.flagged_unknown += 1
            logger.warning(
                "No underlying close for %s on %s — cannot classify ITM/OTM, "
                "skipping auto-EXPIRATION",
                orphan.underlying, orphan.expiration_date,
            )
            continue

        # OTM
        if dry_run:
            summary.created += 1
            logger.info(
                "[dry-run] would synth EXPIRATION: account=%s symbol=%s qty=%s",
                orphan.account_id, orphan.api_symbol, orphan.quantity,
            )
            continue

        created = create_synthetic_expiration(db, orphan)
        if created is None:
            summary.skipped_existing += 1
        else:
            summary.created += 1

    return summary
