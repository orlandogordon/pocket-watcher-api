"""CSV-fixture-driven tests for the investment-statement parsers.

The Schwab and Ameriprise parsers each expose a deterministic ``parse(..., is_csv=True)``
path that exercises the logic where regressions actually bite — transaction-type
normalization, security-type classification, option OCC-symbol formatting, the
commission/fee split, and account-number extraction — without the fragile,
geometry-dependent PDF table extraction. (TD Ameritrade is PDF-only and raises
on CSV, so it is not covered here; it needs a sanitized PDF fixture.)

Fixtures are synthetic — no real account numbers or holdings.
"""
import io
from datetime import date
from decimal import Decimal
from pathlib import Path

import pytest

from src.parser import ameriprise, schwab
from src.parser.models import StatementParseError

pytestmark = pytest.mark.parser

_FIXTURES = Path(__file__).parent / "parsers" / "fixtures"

_SCHWAB_HEADER = (
    '"Date","Action","Symbol","Description","Quantity","Price","Fees & Comm","Amount"\n'
)


def _parse(parser, name):
    buf = io.BytesIO((_FIXTURES / name).read_bytes())
    return parser.parse(buf, is_csv=True)


def _parse_schwab_rows(*rows):
    csv_text = _SCHWAB_HEADER + "".join(r if r.endswith("\n") else r + "\n" for r in rows)
    return schwab.parse(io.BytesIO(csv_text.encode()), is_csv=True).investment_transactions


def _by_type(txns, txn_type):
    return [t for t in txns if t.transaction_type == txn_type]


# ===== SCHWAB =====

class TestSchwabCsv:
    def parsed(self):
        return _parse(schwab, "schwab_sample.csv").investment_transactions

    def test_row_count_includes_fee_split(self):
        # 5 source rows; the option Sell-to-Close splits into SELL + FEE → 6.
        assert len(self.parsed()) == 6

    def test_credit_interest(self):
        interest = _by_type(self.parsed(), "INTEREST")
        assert len(interest) == 1
        assert interest[0].total_amount == Decimal("0.09")
        assert interest[0].symbol is None
        assert interest[0].transaction_date == date(2024, 12, 30)

    def test_etf_buy_no_fee_single_row(self):
        buys = _by_type(self.parsed(), "BUY")
        assert len(buys) == 1
        voo = buys[0]
        assert voo.symbol == "VOO"
        assert voo.security_type.value == "ETF"
        assert voo.quantity == Decimal("2")
        assert voo.price_per_share == Decimal("480.00")
        assert voo.total_amount == Decimal("-960.00")
        assert voo.api_symbol == "VOO"

    def test_option_sell_to_close_formats_occ_and_splits_fee(self):
        txns = self.parsed()
        sells = _by_type(txns, "SELL")
        assert len(sells) == 1
        opt = sells[0]
        assert opt.symbol == "JPM"
        assert opt.security_type.value == "OPTION"
        # OCC: JPM + 240816 (08/16/24) + C (CALL) + strike 200 * 1000, 8 digits.
        assert opt.api_symbol == "JPM240816C00200000"
        # The split puts the commission back onto the gross security amount.
        assert opt.total_amount == Decimal("460.00")  # 459.33 + 0.67

        fees = _by_type(txns, "FEE")
        assert len(fees) == 1
        assert fees[0].total_amount == Decimal("-0.67")
        assert fees[0].transaction_date == date(2024, 6, 17)  # same row as the sell

    def test_dividend_and_transfer(self):
        txns = self.parsed()
        div = _by_type(txns, "DIVIDEND")
        assert len(div) == 1
        assert div[0].total_amount == Decimal("12.34")

        out = _by_type(txns, "TRANSFER_OUT")
        assert len(out) == 1
        assert out[0].total_amount == Decimal("-500.00")


# ===== #71: ROBUST NUMERIC CLEANING + FAIL-LOUD =====

class TestSchwabNumericAndFailLoud:
    def test_spaced_paren_negative_amount_is_not_dropped(self):
        # The #71 regression: '$ (960.00)' previously crashed amount parsing and
        # silently dropped the whole BUY. It must now import as a real -960 row.
        txns = _parse_schwab_rows(
            '"06/03/2024","Buy","VOO","VANGUARD S&P 500 ETF","2","$480.00","","$ (960.00)"'
        )
        assert len(txns) == 1
        assert txns[0].transaction_type == "BUY"
        assert txns[0].total_amount == Decimal("-960.00")

    def test_buy_missing_price_fails_loud(self):
        with pytest.raises(StatementParseError, match="price"):
            _parse_schwab_rows(
                '"06/03/2024","Buy","VOO","VANGUARD S&P 500 ETF","2","","","-$960.00"'
            )

    def test_buy_missing_quantity_fails_loud(self):
        with pytest.raises(StatementParseError, match="quantity"):
            _parse_schwab_rows(
                '"06/03/2024","Buy","VOO","VANGUARD S&P 500 ETF","","$480.00","","-$960.00"'
            )

    def test_unparseable_amount_on_cash_row_does_not_fail_statement(self):
        # A cash row whose amount column holds prose (boilerplate the extractor
        # grabbed) must NOT fail the whole statement — only malformed *trades* do.
        txns = _parse_schwab_rows(
            '"12/30/2024","Credit Interest","","SCHWAB1 INT","","","","$bogus"'
        )
        assert len(txns) == 1
        assert txns[0].total_amount == Decimal("0")

    def test_blank_amount_on_cash_row_defaults_to_zero(self):
        # A genuinely blank cash amount is a benign placeholder, not a parse error.
        txns = _parse_schwab_rows(
            '"12/30/2024","Credit Interest","","SCHWAB1 INT","","","",""'
        )
        assert len(txns) == 1
        assert txns[0].total_amount == Decimal("0")


# ===== AMERIPRISE =====

class TestAmeripriseCsv:
    def parsed_data(self):
        return _parse(ameriprise, "ameriprise_sample.csv")

    def test_account_number_last4(self):
        info = self.parsed_data().account_info
        assert info is not None
        assert info.account_number_last4 == "7890"

    def test_skips_money_market_reinvestment(self):
        txns = self.parsed_data().investment_transactions
        # 6 source rows, the money-market reinvest sweep is dropped → 5.
        assert len(txns) == 5
        assert all("MONEY MARKET" not in (t.description or "").upper() for t in txns)

    def test_stock_buy(self):
        txns = self.parsed_data().investment_transactions
        buys = _by_type(txns, "BUY")
        aapl = next(t for t in buys if t.symbol == "AAPL")
        assert aapl.security_type.value == "STOCK"
        assert aapl.total_amount == Decimal("-1500.00")
        assert aapl.quantity == Decimal("10")
        assert aapl.price_per_share == Decimal("150.00")
        assert aapl.api_symbol == "AAPL"

    def test_option_buy_formats_occ(self):
        txns = self.parsed_data().investment_transactions
        spy = next(t for t in txns if t.symbol == "SPY")
        assert spy.transaction_type == "BUY"
        assert spy.security_type.value == "OPTION"
        # SPY + 240517 (05/17/2024) + C (CALL) + strike 500 * 1000, 8 digits.
        assert spy.api_symbol == "SPY240517C00500000"

    def test_dividend_fee_and_transfer_types(self):
        txns = self.parsed_data().investment_transactions
        assert _by_type(txns, "DIVIDEND")[0].total_amount == Decimal("45.20")
        assert _by_type(txns, "FEE")[0].total_amount == Decimal("-25.00")
        assert _by_type(txns, "TRANSFER_IN")[0].total_amount == Decimal("500.00")
