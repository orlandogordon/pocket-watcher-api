"""Holdings rebuild & cost-basis engine (crud_investment.rebuild_holdings_from_transactions).

Holdings are a materialized cache replayed from the transaction log, so the
cost-basis math lives here: weighted-average cost across buys, sells preserving
average cost while recording cost_basis_at_sale, option-vs-stock keying (an
option must not merge into the stock of the same underlying), expirations and
splits, and the same-day BUY-before-SELL replay ordering.
"""
from datetime import date
from decimal import Decimal

import pytest

from src.crud.crud_investment import rebuild_holdings_from_transactions
from src.services.account_snapshot import get_account_state_on_date, parse_split_ratio
from src.db.core import AccountType, InvestmentTransactionType
from tests.factories import make_account, make_investment_txn, make_user

pytestmark = pytest.mark.integration

# A valid OCC symbol: QQQ 2024-05-24 PUT strike 454.
_QQQ_PUT = "QQQ240524P00454000"


@pytest.fixture
def user(db):
    return make_user(db)


@pytest.fixture
def account(db, user):
    return make_account(db, user, account_type=AccountType.INVESTMENT, account_name="Brokerage")


def _buy(db, user, account, **kw):
    kw.setdefault("transaction_type", InvestmentTransactionType.BUY)
    return make_investment_txn(db, user, account, **kw)


def _holdings_by_symbol(db, account):
    return {h.symbol: h for h in rebuild_holdings_from_transactions(db, account.db_id)}


def test_weighted_average_cost_basis_across_buys(db, user, account):
    _buy(db, user, account, symbol="QQQ", quantity=Decimal("10"), price_per_share=Decimal("100"), transaction_date=date(2026, 1, 1))
    _buy(db, user, account, symbol="QQQ", quantity=Decimal("10"), price_per_share=Decimal("200"), transaction_date=date(2026, 1, 2))

    holdings = _holdings_by_symbol(db, account)
    assert set(holdings) == {"QQQ"}
    qqq = holdings["QQQ"]
    assert qqq.quantity == Decimal("20")
    assert qqq.average_cost_basis == Decimal("150")  # (10*100 + 10*200) / 20


def test_sell_preserves_average_cost_and_records_basis(db, user, account):
    _buy(db, user, account, symbol="QQQ", quantity=Decimal("10"), price_per_share=Decimal("100"), transaction_date=date(2026, 1, 1))
    _buy(db, user, account, symbol="QQQ", quantity=Decimal("10"), price_per_share=Decimal("200"), transaction_date=date(2026, 1, 2))
    sell = make_investment_txn(
        db, user, account, transaction_type=InvestmentTransactionType.SELL,
        symbol="QQQ", quantity=Decimal("5"), price_per_share=Decimal("300"),
        transaction_date=date(2026, 1, 3),
    )

    qqq = _holdings_by_symbol(db, account)["QQQ"]
    assert qqq.quantity == Decimal("15")
    assert qqq.average_cost_basis == Decimal("150")  # selling does not move the average
    db.refresh(sell)
    assert sell.cost_basis_at_sale == Decimal("150")


def test_full_sell_removes_holding(db, user, account):
    _buy(db, user, account, symbol="QQQ", quantity=Decimal("10"), price_per_share=Decimal("100"), transaction_date=date(2026, 1, 1))
    make_investment_txn(
        db, user, account, transaction_type=InvestmentTransactionType.SELL,
        symbol="QQQ", quantity=Decimal("10"), price_per_share=Decimal("120"),
        transaction_date=date(2026, 1, 2),
    )
    assert _holdings_by_symbol(db, account) == {}


def test_option_and_stock_do_not_merge(db, user, account):
    _buy(db, user, account, symbol="QQQ", quantity=Decimal("10"), price_per_share=Decimal("400"), transaction_date=date(2026, 1, 1))
    _buy(
        db, user, account, symbol="QQQ", api_symbol=_QQQ_PUT, security_type="OPTION",
        quantity=Decimal("1"), price_per_share=Decimal("5"), transaction_date=date(2026, 1, 1),
    )

    holdings = _holdings_by_symbol(db, account)
    assert set(holdings) == {"QQQ", _QQQ_PUT}
    opt = holdings[_QQQ_PUT]
    assert opt.security_type == "OPTION"
    assert opt.underlying_symbol == "QQQ"
    assert opt.option_type == "PUT"
    assert opt.strike_price == Decimal("454")
    assert opt.expiration_date == date(2024, 5, 24)


def test_expiration_removes_option_holding(db, user, account):
    _buy(
        db, user, account, symbol="QQQ", api_symbol=_QQQ_PUT, security_type="OPTION",
        quantity=Decimal("1"), price_per_share=Decimal("5"), transaction_date=date(2026, 1, 1),
    )
    make_investment_txn(
        db, user, account, transaction_type=InvestmentTransactionType.EXPIRATION,
        symbol="QQQ", api_symbol=_QQQ_PUT, security_type="OPTION",
        quantity=Decimal("1"), price_per_share=Decimal("0"), total_amount=Decimal("0"),
        transaction_date=date(2026, 1, 2),
    )
    assert _holdings_by_symbol(db, account) == {}


def test_sell_to_open_creates_short_then_buy_to_close_removes_it(db, user, account):
    # Write (sell-to-open) an option with no prior holding -> short position.
    sell = make_investment_txn(
        db, user, account, transaction_type=InvestmentTransactionType.SELL,
        symbol="QQQ", api_symbol=_QQQ_PUT, security_type="OPTION",
        quantity=Decimal("2"), price_per_share=Decimal("5"), transaction_date=date(2026, 1, 1),
    )
    short = _holdings_by_symbol(db, account)[_QQQ_PUT]
    assert short.quantity == Decimal("-2")
    assert short.average_cost_basis == Decimal("5")  # premium received per unit
    db.refresh(sell)
    assert sell.cost_basis_at_sale is None  # opening a short has no basis at sale

    # Buy-to-close the short -> flat -> holding removed.
    make_investment_txn(
        db, user, account, transaction_type=InvestmentTransactionType.BUY,
        symbol="QQQ", api_symbol=_QQQ_PUT, security_type="OPTION",
        quantity=Decimal("2"), price_per_share=Decimal("2"), transaction_date=date(2026, 1, 2),
    )
    assert _QQQ_PUT not in _holdings_by_symbol(db, account)


def test_split_adjusts_quantity_and_cost(db, user, account):
    _buy(db, user, account, symbol="QQQ", quantity=Decimal("10"), price_per_share=Decimal("100"), transaction_date=date(2026, 1, 1))
    make_investment_txn(
        db, user, account, transaction_type=InvestmentTransactionType.SPLIT,
        symbol="QQQ", quantity=Decimal("0"), price_per_share=Decimal("0"), total_amount=Decimal("0"),
        description="2:1 Stock Split", transaction_date=date(2026, 1, 2),
    )
    qqq = _holdings_by_symbol(db, account)["QQQ"]
    assert qqq.quantity == Decimal("20")
    assert qqq.average_cost_basis == Decimal("50")  # cost per share halves on a 2:1


def test_same_day_buy_processed_before_sell(db, user, account):
    # Insert the SELL first (lower row id) but on the same date as the BUY.
    # The replay's type_priority must still process the BUY first, or the sell
    # would be dropped (no holding yet) and the quantity would be wrong.
    make_investment_txn(
        db, user, account, transaction_type=InvestmentTransactionType.SELL,
        symbol="QQQ", quantity=Decimal("4"), price_per_share=Decimal("110"),
        transaction_date=date(2026, 1, 1),
    )
    _buy(db, user, account, symbol="QQQ", quantity=Decimal("10"), price_per_share=Decimal("100"), transaction_date=date(2026, 1, 1))

    qqq = _holdings_by_symbol(db, account)["QQQ"]
    assert qqq.quantity == Decimal("6")


def test_zero_quantity_buy_does_not_divide_by_zero(db, user, account):
    # TD Ameritrade money-market / cash-sweep rows arrive as BUY with quantity 0.
    # Replaying them must not raise DivisionByZero in the cost-basis math (#65).
    _buy(
        db, user, account, symbol="MMDA",
        quantity=Decimal("0"), price_per_share=Decimal("0"),
        total_amount=Decimal("500"), transaction_date=date(2026, 1, 1),
    )

    # Holdings rebuild already guards; the regression is the account-state path
    # (_update_investment_account_balance -> get_account_state_on_date).
    rebuild_holdings_from_transactions(db, account.db_id)
    state = get_account_state_on_date(db, account.db_id, date(2026, 1, 2))

    assert "MMDA" not in state["holdings"]  # zero-qty buy is not an active holding
    assert state["cash_balance"] == Decimal("-500")  # cash outflow still recorded


def test_null_quantity_sell_does_not_crash_account_state(db, user, account):
    # A SELL row can arrive with a null quantity (parser couldn't read the column).
    # The replay must coalesce None to 0, not crash (#65, get_account_state_on_date
    # SELL branch — `quantity -= None`).
    _buy(db, user, account, symbol="AAPL", quantity=Decimal("10"),
         price_per_share=Decimal("100"), total_amount=Decimal("1000"),
         transaction_date=date(2026, 1, 1))
    make_investment_txn(
        db, user, account, transaction_type=InvestmentTransactionType.SELL,
        symbol="AAPL", quantity=None, price_per_share=None,
        total_amount=Decimal("500"), transaction_date=date(2026, 1, 2),
    )

    state = get_account_state_on_date(db, account.db_id, date(2026, 1, 3))

    assert state["holdings"]["AAPL"]["quantity"] == Decimal("10")  # -= 0, unchanged
    assert state["cash_balance"] == Decimal("-500")  # -1000 buy + 500 sale


def test_parse_split_ratio_handles_zero_denominator():
    # A malformed "X:0" split must fall back to the no-op ratio, not raise (#65).
    assert parse_split_ratio("2:0 Stock Split") == Decimal("1.0")
