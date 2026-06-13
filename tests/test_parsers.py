"""Tests for parser TRANSFER_IN/TRANSFER_OUT output."""
import re
import unittest
from decimal import Decimal
from pathlib import Path

INPUT_DIR = Path(__file__).parent / "parsers" / "fixtures" / "local"


class TestAmeripriseNormalize(unittest.TestCase):
    def test_withdrawal(self):
        from src.parser.ameriprise import _normalize_transaction_type
        self.assertEqual(_normalize_transaction_type("WITHDRAWAL", ""), "TRANSFER_OUT")

    def test_deposit(self):
        from src.parser.ameriprise import _normalize_transaction_type
        self.assertEqual(_normalize_transaction_type("DEPOSIT", ""), "TRANSFER_IN")

    def test_ach_deposit(self):
        from src.parser.ameriprise import _normalize_transaction_type
        self.assertEqual(_normalize_transaction_type("ACH", "ACH DIRECT DEPOSIT"), "TRANSFER_IN")

    def test_ach_direction_comes_from_amount_sign(self):
        # Ameriprise labels an ACH pull *into* the account "ACH DIRECT WITHDRAWAL"
        # and lists it under Deposits with a positive amount — so the sign, not
        # the description keyword, decides direction.
        from src.parser.ameriprise import _normalize_transaction_type
        desc = "ACH DIRECT WITHDRAWAL TRACE #123"
        self.assertEqual(_normalize_transaction_type("ACH", desc, Decimal("2017.00")), "TRANSFER_IN")
        self.assertEqual(_normalize_transaction_type("ACH", desc, Decimal("-2017.00")), "TRANSFER_OUT")
        # Without an amount, a generic ACH defaults to a deposit (no direction word).
        self.assertEqual(_normalize_transaction_type("ACH", desc), "TRANSFER_IN")

    def test_non_transfer_unchanged(self):
        from src.parser.ameriprise import _normalize_transaction_type
        self.assertEqual(_normalize_transaction_type("DIVIDEND PAYMENT", ""), "DIVIDEND")
        self.assertEqual(_normalize_transaction_type("BUY", ""), "BUY")
        self.assertEqual(_normalize_transaction_type("SELL", ""), "SELL")

    def test_journal_direction_from_amount_sign(self):
        # #76: JOURNAL rows move cash/positions between a client's own re-numbered
        # sub-accounts during account restructurings — classified as transfers by
        # the signed amount so symmetric legs net to zero across both statements.
        from src.parser.ameriprise import _normalize_transaction_type
        self.assertEqual(
            _normalize_transaction_type("JOURNAL", "APPLE INC TO: 72127402-1", Decimal("-267.46")),
            "TRANSFER_OUT",
        )
        self.assertEqual(
            _normalize_transaction_type("JOURNAL", "APPLE INC FROM: 75958883-1", Decimal("267.46")),
            "TRANSFER_IN",
        )


class TestAmeripriseClassifyRow(unittest.TestCase):
    """#76: _classify_row applies the statement-specific rules layered on top of
    _normalize_transaction_type (DRIP reinvest, fee rebate, money-market sweeps)."""

    def _classify(self, raw_type, desc, amount, qty=None, price=None):
        from src.parser.ameriprise import _classify_row
        amt = Decimal(amount) if amount is not None else None
        q = Decimal(qty) if qty is not None else None
        p = Decimal(price) if price is not None else None
        return _classify_row(raw_type, desc, amt, q, p)

    def test_security_dividend_reinvest_becomes_buy(self):
        # A DRIP line buys fractional shares with the just-paid dividend; modeled
        # as BUY (engine: -cash, +shares) so it nets the paired DIVIDEND to zero.
        ttype, qty, price, skip = self._classify(
            "REINVEST DIV", "MICROSOFT CORP REINVEST AT 404.941 DIVIDEND R", "-0.91"
        )
        self.assertFalse(skip)
        self.assertEqual(ttype, "BUY")
        self.assertEqual(price, Decimal("404.941"))
        # qty derived from |amount| / price
        self.assertEqual(qty, Decimal("0.91") / Decimal("404.941"))

    def test_money_market_interest_reinvest_skipped(self):
        _, _, _, skip = self._classify(
            "INTEREST REINVEST", "AMERIPRISE INSURED MONEY MARKET ACCOUNT", "-0.02"
        )
        self.assertTrue(skip)

    def test_money_market_cash_sweep_purchase_skipped(self):
        _, _, _, skip = self._classify(
            "PURCHASE", "AMERIPRISE INSURED MONEY MARKET ACCOUNT", "-1.83"
        )
        self.assertTrue(skip)

    def test_money_market_interest_income_kept(self):
        ttype, _, _, skip = self._classify(
            "INTEREST", "AMERIPRISE INSURED MONEY MARKET ACCOUNT 082925 APYE .16%", "0.02"
        )
        self.assertFalse(skip)
        self.assertEqual(ttype, "INTEREST")

    def test_fee_rebate_credit_becomes_transfer_in(self):
        # A positive-amount FEE is a rebate/credit; the snapshot engine's FEE path
        # only subtracts, so credits route through TRANSFER_IN (adds cash).
        ttype, _, _, skip = self._classify("FEE", "FEE REBATE REBATED FOR 009 DAYS", "2.46")
        self.assertFalse(skip)
        self.assertEqual(ttype, "TRANSFER_IN")

    def test_normal_fee_stays_fee(self):
        ttype, _, _, skip = self._classify("FEE", "ASSET-BASED BILL VAL 2,926.94", "-8.49")
        self.assertFalse(skip)
        self.assertEqual(ttype, "FEE")


class TestSchwabNormalize(unittest.TestCase):
    def test_withdrawal(self):
        from src.parser.schwab import _normalize_transaction_type
        self.assertEqual(_normalize_transaction_type("Withdrawal"), "TRANSFER_OUT")

    def test_deposit(self):
        from src.parser.schwab import _normalize_transaction_type
        self.assertEqual(_normalize_transaction_type("Deposit"), "TRANSFER_IN")

    def test_moneylink_transfer(self):
        from src.parser.schwab import _normalize_transaction_type
        self.assertEqual(_normalize_transaction_type("MoneyLink Transfer"), "TRANSFER_OUT")

    def test_non_transfer(self):
        from src.parser.schwab import _normalize_transaction_type
        self.assertEqual(_normalize_transaction_type("Buy"), "BUY")
        self.assertEqual(_normalize_transaction_type("Sell"), "SELL")

    def test_margin_interest_is_fee(self):
        # Margin interest is a cash debit; must be FEE (replay subtracts it), not
        # INTEREST (replay treats every INTEREST row as a credit).
        from src.parser.schwab import _normalize_transaction_type
        self.assertEqual(_normalize_transaction_type("Margin Interest"), "FEE")

    def test_credit_interest_still_interest(self):
        from src.parser.schwab import _normalize_transaction_type
        self.assertEqual(_normalize_transaction_type("Credit Interest"), "INTEREST")

    def test_interest_charge_by_negative_sign_is_fee(self):
        # A negative amount marks a charge even without "margin" wording -> FEE.
        from decimal import Decimal
        from src.parser.schwab import _normalize_transaction_type
        self.assertEqual(_normalize_transaction_type("Credit Interest", Decimal("-5.00")), "FEE")

    def test_interest_income_by_positive_sign_is_interest(self):
        from decimal import Decimal
        from src.parser.schwab import _normalize_transaction_type
        self.assertEqual(_normalize_transaction_type("Credit Interest", Decimal("0.09")), "INTEREST")


class TestSchwabStatementSymbolIntegrity(unittest.TestCase):
    """End-to-end #79 regression on the real corpus statement that exhibits the
    column-boundary clip. The fix derives the activity-table boundaries from the
    header word positions so they track Schwab's per-statement horizontal shift;
    the recovered ticker AND its (previously truncated) description must come
    through intact. Skips when the gitignored corpus is absent."""

    def test_clipped_statement_parses_clean(self):
        pdf = INPUT_DIR / "schwab" / "Brokerage Statement_2026-05-31_145.PDF"
        if not pdf.exists():
            self.skipTest("no local schwab corpus statement for #79")
        from src.parser.schwab import parse
        txns = parse(pdf).investment_transactions
        symbols = {t.symbol for t in txns if t.symbol}
        # ticker recovered, truncated form gone
        self.assertIn("TSLA", symbols)
        self.assertNotIn("SLA", symbols)
        # description no longer loses its leading glyph ("ESLAINC" -> "TESLA…")
        tsla = next(t for t in txns if t.symbol == "TSLA")
        self.assertTrue(
            tsla.description.upper().startswith("TESLA"),
            f"description still truncated: {tsla.description!r}",
        )


class TestTDAmeritradeNormalize(unittest.TestCase):
    def test_funds_deposited(self):
        from src.parser.tdameritrade import _normalize_transaction_type
        self.assertEqual(_normalize_transaction_type("Funds Deposited", ""), "TRANSFER_IN")

    def test_funds_disbursed(self):
        from src.parser.tdameritrade import _normalize_transaction_type
        self.assertEqual(_normalize_transaction_type("Funds Disbursed", ""), "TRANSFER_OUT")

    def test_ach_in(self):
        from src.parser.tdameritrade import _normalize_transaction_type
        self.assertEqual(_normalize_transaction_type("Other", "ach in from bank"), "TRANSFER_IN")

    def test_ach_out(self):
        from src.parser.tdameritrade import _normalize_transaction_type
        self.assertEqual(_normalize_transaction_type("Other", "ach out to bank"), "TRANSFER_OUT")

    def test_margin_interest_charge_is_fee(self):
        # Margin interest is a cash debit; must be FEE (replay subtracts it), not
        # INTEREST (replay treats every INTEREST row as a credit). The marker is
        # in the description on real TDA statements.
        from src.parser.tdameritrade import _normalize_transaction_type
        self.assertEqual(
            _normalize_transaction_type("Div/Int", "MARGIN INTEREST CHARGE - Payable: 02/26/2021"),
            "FEE",
        )

    def test_interest_credit_still_interest(self):
        from src.parser.tdameritrade import _normalize_transaction_type
        self.assertEqual(
            _normalize_transaction_type("Div/Int", "INTEREST CREDIT - Payable: 02/26/2021"),
            "INTEREST",
        )

    def test_interest_charge_by_negative_sign_is_fee(self):
        # A negative amount marks a charge even without "margin" wording -> FEE.
        from decimal import Decimal
        from src.parser.tdameritrade import _normalize_transaction_type
        self.assertEqual(_normalize_transaction_type("Div/Int", "INTEREST", Decimal("-2.50")), "FEE")

    def test_interest_income_by_positive_sign_is_interest(self):
        from decimal import Decimal
        from src.parser.tdameritrade import _normalize_transaction_type
        self.assertEqual(_normalize_transaction_type("Div/Int", "INTEREST", Decimal("2.50")), "INTEREST")


class TestTDAmeritradeCashJournalRecovery(unittest.TestCase):
    """#80: a cash-only journal ("Margin Journal - Other" courtesy adjustments)
    is recovered to a signed transfer; share journals / corporate actions are
    not, so they can't become phantom cash."""

    def _recover(self, full_type, symbol, qty, amount):
        from decimal import Decimal
        from src.parser.tdameritrade import _maybe_recover_cash_journal
        q = None if qty is None else Decimal(str(qty))
        a = None if amount is None else Decimal(str(amount))
        return _maybe_recover_cash_journal("OTHER", full_type, symbol, q, a)

    def test_cash_credit_journal_becomes_transfer_in(self):
        self.assertEqual(self._recover("Margin Journal - Other", None, 0, "0.01"), "TRANSFER_IN")

    def test_cash_debit_journal_becomes_transfer_out(self):
        self.assertEqual(self._recover("Margin Journal - Other", None, 0, "-0.01"), "TRANSFER_OUT")

    def test_null_quantity_cash_journal_recovers(self):
        self.assertEqual(self._recover("Cash Journal - Other", None, None, "5.00"), "TRANSFER_IN")

    def test_share_journal_stays_other(self):
        # Non-zero quantity = a position move; must NOT become cash.
        self.assertEqual(self._recover("Margin Journal - Other", "AAPL", 10, "0.00"), "OTHER")

    def test_journal_with_symbol_stays_other(self):
        self.assertEqual(self._recover("Margin Journal - Other", "AAPL", 0, "100.00"), "OTHER")

    def test_zero_amount_journal_stays_other(self):
        self.assertEqual(self._recover("Margin Journal - Other", None, 0, "0.00"), "OTHER")

    def test_non_journal_other_unchanged(self):
        self.assertEqual(self._recover("Some Other Activity", None, 0, "5.00"), "OTHER")

    def test_non_other_type_unchanged(self):
        # Only OTHER rows are eligible; a real type is never rewritten.
        from decimal import Decimal
        from src.parser.tdameritrade import _maybe_recover_cash_journal
        self.assertEqual(
            _maybe_recover_cash_journal("DIVIDEND", "Margin Journal - Other", None, Decimal("0"), Decimal("5.00")),
            "DIVIDEND",
        )


class TestTDAmeritradeRecoverSplit(unittest.TestCase):
    """#72: recover qty/price when a wide price spills a digit across the fixed
    Quantity|Price column boundary ('3 | 3283.0201' extracted as '33 | 283.0201').
    """

    def _recover(self, qty, price, target):
        from decimal import Decimal
        from src.parser.models import recover_misaligned_qty_price
        tol = max(abs(Decimal(target)) * Decimal("0.01"), Decimal("1"))
        return recover_misaligned_qty_price(Decimal(qty), Decimal(price), abs(Decimal(target)), tol)

    def test_recovers_leading_digit_spill(self):
        from decimal import Decimal
        # The real AMZN 2021-10-29 SELL: 33|283.0201 should recover to 3|3283.0201.
        self.assertEqual(self._recover("33", "283.0201", "9849.06"),
                         (Decimal("3"), Decimal("3283.0201")))

    def test_returns_none_when_irrecoverable(self):
        # No split of the digits reconciles with an unrelated amount.
        self.assertIsNone(self._recover("33", "283.0201", "5000.00"))

    def test_leaves_correct_split_alone(self):
        # A correctly-split row already reconciles, so no spurious recovery is
        # attempted (caller only invokes this on a mismatch); a single-digit
        # quantity has no digit to move back and yields None.
        self.assertIsNone(self._recover("3", "3283.0201", "9849.06"))

    def test_preserves_negative_quantity_sign(self):
        from decimal import Decimal
        # Schwab encodes sells with a negative quantity; the recovered split must
        # keep the sign (hypothetical -33|283.0201 -> -3|3283.0201).
        self.assertEqual(self._recover("-33", "283.0201", "9849.06"),
                         (Decimal("-3"), Decimal("3283.0201")))


class TestAmexParserIntegration(unittest.TestCase):
    def test_pdf_payment_is_transfer_in(self):
        pdf_files = list(INPUT_DIR.glob("amex/*.pdf"))
        if not pdf_files:
            self.skipTest("No Amex PDF files in tests/parsers/fixtures/local/amex/")
        from src.parser.amex import parse_statement
        for pdf_file in pdf_files:
            result = parse_statement(str(pdf_file))
            for txn in result.transactions:
                self.assertNotEqual(txn.transaction_type, "Payment",
                    f"Found raw 'Payment' type — should be TRANSFER_IN")


class TestAmexCleanDescription(unittest.TestCase):
    def test_strips_aplpay_prefix(self):
        from src.parser.amex import _clean_description
        self.assertEqual(_clean_description("AplPay TARGET THIRDTOWN NJ"), "TARGET THIRDTOWN NJ")

    def test_strips_pay_over_time_suffix(self):
        from src.parser.amex import _clean_description
        self.assertEqual(
            _clean_description("BEST BUY 015313 Pay Over Time"),
            "BEST BUY 015313",
        )

    def test_strips_both_when_combined(self):
        from src.parser.amex import _clean_description
        self.assertEqual(
            _clean_description("AplPay GRUBHUB*CHICKFILA NEW YORK NY Pay Over Time"),
            "GRUBHUB*CHICKFILA NEW YORK NY",
        )

    def test_leaves_clean_descriptions_unchanged(self):
        from src.parser.amex import _clean_description
        self.assertEqual(_clean_description("AMAZON MARKETPLACE SEATTLE WA"),
                         "AMAZON MARKETPLACE SEATTLE WA")

    def test_does_not_strip_aplpay_mid_string(self):
        """Only the literal prefix is stripped — not the substring."""
        from src.parser.amex import _clean_description
        # Hypothetical: a merchant whose actual name contains "AplPay"
        # should be untouched if the prefix isn't there.
        self.assertEqual(
            _clean_description("STORE WITH AplPay IN NAME"),
            "STORE WITH AplPay IN NAME",
        )

    def test_does_not_strip_pay_over_time_mid_string(self):
        from src.parser.amex import _clean_description
        self.assertEqual(
            _clean_description("Pay Over Time MERCHANT"),
            "Pay Over Time MERCHANT",
        )


class TestAmznSyfParserIntegration(unittest.TestCase):
    def test_pdf_payment_is_transfer_in(self):
        pdf_files = list(INPUT_DIR.glob("amzn-synchrony/*.pdf"))
        if not pdf_files:
            self.skipTest("No Amazon Synchrony PDF files in tests/parsers/fixtures/local/amzn-synchrony/")
        from src.parser.amzn_syf import parse_statement
        for pdf_file in pdf_files:
            result = parse_statement(str(pdf_file))
            for txn in result.transactions:
                self.assertNotEqual(txn.transaction_type, "Payment",
                    f"Found raw 'Payment' type — should be TRANSFER_IN")


class TestVenmoHelpers(unittest.TestCase):
    def test_parse_amount_signed(self):
        from src.parser.venmo import _parse_amount
        from decimal import Decimal
        self.assertEqual(_parse_amount("+ $45.00"), Decimal("45.00"))
        self.assertEqual(_parse_amount("- $45.00"), Decimal("-45.00"))
        self.assertEqual(_parse_amount("- $1,612.62"), Decimal("-1612.62"))

    def test_balance_affecting_inflow(self):
        from src.parser.venmo import _is_balance_affecting
        # Payment received: funding source blank, destination = Venmo balance.
        self.assertTrue(_is_balance_affecting("", "Venmo balance"))

    def test_balance_affecting_cashout(self):
        from src.parser.venmo import _is_balance_affecting
        # Standard Transfer: funding blank, destination = bank.
        self.assertTrue(_is_balance_affecting("", "TD BANK, NA *0000"))

    def test_balance_affecting_skips_external_funded(self):
        from src.parser.venmo import _is_balance_affecting
        # Payment sent via Visa: external funding, destination blank.
        self.assertFalse(_is_balance_affecting("Visa *0001", ""))
        # Charge paid via Amex Send: external funding, destination blank.
        self.assertFalse(_is_balance_affecting("Amex Send Account", ""))
        # Merchant Transaction via TD Bank checking: external funding.
        self.assertFalse(
            _is_balance_affecting("TD BANK, NA Personal Checking *0000", "")
        )

    def test_classify_payment(self):
        from src.parser.venmo import _classify
        from decimal import Decimal
        self.assertEqual(_classify("Payment", Decimal("45")), "DEPOSIT")
        self.assertEqual(_classify("Payment", Decimal("-45")), "PURCHASE")

    def test_classify_standard_transfer_is_cashout(self):
        from src.parser.venmo import _classify
        from decimal import Decimal
        self.assertEqual(_classify("Standard Transfer", Decimal("-100")), "TRANSFER_OUT")

    def test_classify_top_up(self):
        from src.parser.venmo import _classify
        from decimal import Decimal
        self.assertEqual(_classify("Top Up", Decimal("50")), "TRANSFER_IN")
        self.assertEqual(_classify("Add money", Decimal("50")), "TRANSFER_IN")

    def test_classify_instant_transfer_is_cashout(self):
        from src.parser.venmo import _classify
        from decimal import Decimal
        # Instant Transfer is a cashout variant (with fee) — same direction.
        self.assertEqual(_classify("Instant Transfer", Decimal("-50")), "TRANSFER_OUT")

    def test_classify_card_payment_follows_sign(self):
        from src.parser.venmo import _classify
        from decimal import Decimal
        # Venmo Mastercard refund/reversal: credits into Venmo balance.
        self.assertEqual(_classify("Card Payment", Decimal("100")), "DEPOSIT")

    def test_classify_unknown_returns_none(self):
        from src.parser.venmo import _classify
        from decimal import Decimal
        self.assertIsNone(_classify("Mystery Type", Decimal("10")))


class TestVenmoParserIntegration(unittest.TestCase):
    @staticmethod
    def _reported_balances(csv_path):
        """Read the Beginning/Ending Balance the Venmo statement reports about
        itself, so reconciliation needs no hardcoded (personal) figures."""
        import csv
        from decimal import Decimal

        def _money(value):
            return Decimal(value.replace("$", "").replace(",", "").strip())

        rows = list(csv.reader(csv_path.open(encoding="utf-8")))
        hdr = next(i for i, r in enumerate(rows) if "Beginning Balance" in r)
        cols = rows[hdr]
        bi, ei = cols.index("Beginning Balance"), cols.index("Ending Balance")
        begin = end = Decimal("0")
        for r in rows[hdr + 1:]:
            if len(r) > bi and r[bi].strip():
                begin = _money(r[bi])
            if len(r) > ei and r[ei].strip():
                end = _money(r[ei])
        return begin, end

    def test_all_fixtures_reconcile_to_reported_balances(self):
        """Every Venmo CSV present reconciles: the net of emitted transactions
        equals (ending - beginning) as the statement itself reports. Self-
        referential — no personal balances are hardcoded in the test."""
        from src.parser.venmo import parse_csv
        from decimal import Decimal
        csvs = sorted((INPUT_DIR / "venmo").glob("*.csv"))
        if not csvs:
            self.skipTest("No Venmo CSV fixtures in tests/parsers/fixtures/local/venmo/")
        for csv_path in csvs:
            begin, end = self._reported_balances(csv_path)
            data = parse_csv(csv_path)
            net = Decimal("0")
            for t in data.transactions:
                if t.transaction_type in ("DEPOSIT", "TRANSFER_IN"):
                    net += t.amount
                else:
                    net -= t.amount
            self.assertEqual(net, end - begin,
                             f"{csv_path.name}: net {net} != reported {end - begin}")

    def test_skips_external_funded_rows(self):
        """Jan 2023 has 14 raw rows, 9 of which are funded by Visa *0001.
        Only 5 deposits + 1 cashout should be emitted (= 6 transactions)."""
        from src.parser.venmo import parse_csv
        csv_path = INPUT_DIR / "venmo" / "VenmoStatement_January_2023.csv"
        if not csv_path.exists():
            self.skipTest("Venmo Jan 2023 fixture missing")
        data = parse_csv(csv_path)
        self.assertEqual(len(data.transactions), 6)


class TestCashAppHelpers(unittest.TestCase):
    def test_parse_amount_signed(self):
        from src.parser.cashapp import _parse_amount
        from decimal import Decimal
        self.assertEqual(_parse_amount("$535.00"), Decimal("535.00"))
        self.assertEqual(_parse_amount("-$60.00"), Decimal("-60.00"))

    def test_parse_date_drops_time_and_tz(self):
        from src.parser.cashapp import _parse_date
        from datetime import date
        self.assertEqual(_parse_date("2024-11-05 00:59:46 EST"), date(2024, 11, 5))
        self.assertEqual(_parse_date("2023-01-04 13:30:23 EDT"), date(2023, 1, 4))

    def test_balance_affecting_p2p_cash_balance(self):
        from src.parser.cashapp import _is_balance_affecting
        self.assertTrue(_is_balance_affecting("P2P", "Cash Balance"))

    def test_balance_affecting_withdrawal_always_included(self):
        from src.parser.cashapp import _is_balance_affecting
        # Withdrawal: Account is destination bank, balance still affected.
        self.assertTrue(_is_balance_affecting("Withdrawal", "TD Bank"))

    def test_balance_affecting_skips_external_p2p(self):
        from src.parser.cashapp import _is_balance_affecting
        self.assertFalse(_is_balance_affecting("P2P", "TD Bank"))

    def test_classify_p2p(self):
        from src.parser.cashapp import _classify
        from decimal import Decimal
        self.assertEqual(_classify("P2P", Decimal("100")), "DEPOSIT")
        self.assertEqual(_classify("P2P", Decimal("-20")), "PURCHASE")

    def test_classify_withdrawal_is_cashout(self):
        from src.parser.cashapp import _classify
        from decimal import Decimal
        self.assertEqual(_classify("Withdrawal", Decimal("-150")), "TRANSFER_OUT")

    def test_classify_unknown_returns_none(self):
        from src.parser.cashapp import _classify
        from decimal import Decimal
        self.assertIsNone(_classify("Bitcoin Purchase", Decimal("50")))

    def test_description_strips_boilerplate_note(self):
        from src.parser.cashapp import _build_description
        # "$535 Payment From Jane Doe" is auto-generated boilerplate;
        # description should fall back to just the Name column.
        self.assertEqual(
            _build_description("P2P", "$535 Payment From Jane Doe", "Jane Doe", "Cash Balance"),
            "Jane Doe",
        )

    def test_description_keeps_user_supplied_note(self):
        from src.parser.cashapp import _build_description
        # User-supplied memo (an emoji, a freeform string) should be kept.
        self.assertEqual(
            _build_description("P2P", "Uber home", "John Smith", "Cash Balance"),
            "John Smith: Uber home",
        )

    def test_description_withdrawal_uses_account(self):
        from src.parser.cashapp import _build_description
        self.assertEqual(
            _build_description("Withdrawal", "Cash Out", "", "TD Bank"),
            "Cash out to TD Bank",
        )


class TestCashAppParserIntegration(unittest.TestCase):
    def test_fixture_skips_external_and_system_rows(self):
        """Sample fixture has 36 raw rows: 12 balance-P2P + 8 withdrawals
        = 20 emitted; 14 externally-funded P2P + 2 Account Notifications
        skipped."""
        from src.parser.cashapp import parse_csv
        csv_path = INPUT_DIR / "cashapp" / "cash_app_report_1778789548189.csv"
        if not csv_path.exists():
            self.skipTest("Cash App fixture missing")
        data = parse_csv(csv_path)
        self.assertEqual(len(data.transactions), 20)

    def test_fixture_type_breakdown(self):
        """Of the 20 emitted: 8 cashouts, 10 deposits, 2 purchases
        (John Smith tally hose $20 + Uber home $30)."""
        from src.parser.cashapp import parse_csv
        from collections import Counter
        csv_path = INPUT_DIR / "cashapp" / "cash_app_report_1778789548189.csv"
        if not csv_path.exists():
            self.skipTest("Cash App fixture missing")
        data = parse_csv(csv_path)
        counts = Counter(t.transaction_type for t in data.transactions)
        self.assertEqual(counts["TRANSFER_OUT"], 8)
        self.assertEqual(counts["DEPOSIT"], 10)
        self.assertEqual(counts["PURCHASE"], 2)


class TestTdbankCleanDescription(unittest.TestCase):
    """#50 — TD compound-prefix description cleanup."""

    def test_debit_card_purchase_strips_prefix_keeps_merchant(self):
        from src.parser.tdbank import _clean_description
        self.assertEqual(
            _clean_description(
                "DEBITCARDPURCHASE,*****11111111111,AUT100920VISADDAPUR MICROSOFTXBOX MSBILLINFO *WA"
            ),
            "MICROSOFTXBOX MSBILLINFO *WA",
        )

    def test_debit_card_credit_handles_no_space_before_state(self):
        """The *<ST> tag isn't always preceded by a space — preserve as-is."""
        from src.parser.tdbank import _clean_description
        self.assertEqual(
            _clean_description(
                "DEBITCARDCREDIT,*****11111111111,AUT101720VISADDAREF AMZNMKTPUS AMZNCOMBILL*WA"
            ),
            "AMZNMKTPUS AMZNCOMBILL*WA",
        )

    def test_td_atm_debit(self):
        from src.parser.tdbank import _clean_description
        self.assertEqual(
            _clean_description(
                "TDATMDEBIT,*****22222222222,AUT061221DDAWITHDRAW 100MAINSTREET ANYTOWN*NJ"
            ),
            "100MAINSTREET ANYTOWN*NJ",
        )

    def test_non_td_atm_debit(self):
        from src.parser.tdbank import _clean_description
        self.assertEqual(
            _clean_description(
                "NONTDATMDEBIT,*****11111111111,AUT101820DDAWITHDRAW 200FIRSTAVE OTHERTOWN *NJ"
            ),
            "200FIRSTAVE OTHERTOWN *NJ",
        )

    def test_atm_cash_deposit_uses_space_separator(self):
        """ATMCASHDEPOSIT uses a space between card and AUT, not a comma."""
        from src.parser.tdbank import _clean_description
        self.assertEqual(
            _clean_description(
                "ATMCASHDEPOSIT,*****22222222222 AUT022021ATMCASHDEPOSIT 300SECONDBLVD OTHERTOWN *NJ"
            ),
            "300SECONDBLVD OTHERTOWN *NJ",
        )

    def test_visa_transfer_keeps_processor(self):
        from src.parser.tdbank import _clean_description
        self.assertEqual(
            _clean_description(
                "VISATRANSFER,*****11111111111,AUT010421VISATRANSFER CASHAPPCASHOUT VISADIRECT *CA"
            ),
            "CASHAPPCASHOUT VISADIRECT *CA",
        )

    def test_zelle_sent(self):
        from src.parser.tdbank import _clean_description
        self.assertEqual(
            _clean_description("TDZELLESENT, REF0000000001ZelleALEXJOHNSON"),
            "Zelle: ALEXJOHNSON",
        )

    def test_zelle_received(self):
        from src.parser.tdbank import _clean_description
        self.assertEqual(
            _clean_description("TDZELLERECEIVED, REF0000000002ZelleMARIAGARCIA"),
            "Zelle: MARIAGARCIA",
        )

    def test_zelle_with_space_before_zelle_keyword(self):
        """Some TD statements have a space between the token and 'Zelle'."""
        from src.parser.tdbank import _clean_description
        self.assertEqual(
            _clean_description("TDZELLESENT, REF0000000003 ZelleJOHNSMITH"),
            "Zelle: JOHNSMITH",
        )

    def test_already_clean_description_unchanged(self):
        from src.parser.tdbank import _clean_description
        self.assertEqual(
            _clean_description("TARGET THIRDTOWN NJ"),
            "TARGET THIRDTOWN NJ",
        )

    def test_unknown_prefix_unchanged(self):
        """Patterns we don't handle must pass through verbatim."""
        from src.parser.tdbank import _clean_description
        self.assertEqual(
            _clean_description("ELECTRONICPMT-WEB CHASE PAYMENT"),
            "ELECTRONICPMT-WEB CHASE PAYMENT",
        )

    def test_compound_prefix_mid_string_not_stripped(self):
        """Only matches at the start. A merchant name that happens to contain
        DEBITCARDPURCHASE inside it must not be touched."""
        from src.parser.tdbank import _clean_description
        self.assertEqual(
            _clean_description("STORE WITH DEBITCARDPURCHASE IN NAME"),
            "STORE WITH DEBITCARDPURCHASE IN NAME",
        )

    def test_dbcrdpurap_ap_variant(self):
        """Apple Pay debit card purchase variant — different prefix word,
        same compound shape, has AP suffix in the middle token."""
        from src.parser.tdbank import _clean_description
        self.assertEqual(
            _clean_description(
                "DBCRDPURAP,*****22222222222,AUT100723VISADDAPURAP COSTCOGAS 0739 THIRDTOWN *NJ"
            ),
            "COSTCOGAS 0739 THIRDTOWN *NJ",
        )

    def test_debitpos_variant(self):
        """DDAPURCHASE flavor (vs VISADDAPUR for card-network purchases)."""
        from src.parser.tdbank import _clean_description
        self.assertEqual(
            _clean_description(
                "DEBITPOS,*****11111111111,AUT101820DDAPURCHASE WAWA 937 OTHERTOWN *NJ"
            ),
            "WAWA 937 OTHERTOWN *NJ",
        )

    def test_tdatmdebitap_with_trailing_ap_token(self):
        """TDATMDEBITAP has an extra ` AP` token between the AUT chunk and
        the address — it must be consumed by the regex, not leak into the
        cleaned merchant."""
        from src.parser.tdbank import _clean_description
        self.assertEqual(
            _clean_description(
                "TDATMDEBITAP,*****22222222222,AUT122524DDAWITHDRAW AP 400THIRDAVENUE OTHERTOWN *NJ"
            ),
            "400THIRDAVENUE OTHERTOWN *NJ",
        )

    def test_poscredit_refund_variant(self):
        from src.parser.tdbank import _clean_description
        self.assertEqual(
            _clean_description(
                "POSCREDIT,*****22222222222,AUT061621DDAPURCHREF GNC 500FOURTHPLAZA THIRDTOWN *NJ"
            ),
            "GNC 500FOURTHPLAZA THIRDTOWN *NJ",
        )

    def test_ach_deposit_strips_prefix_keeps_account_suffix(self):
        """ACH family: strip the type prefix but keep the trailing
        ****<digits> reference — useful for matching recurring billing."""
        from src.parser.tdbank import _clean_description
        self.assertEqual(
            _clean_description(
                "ACHDEPOSIT,ACMECORPINCPAYROLL*BM***100000001"
            ),
            "ACMECORPINCPAYROLL*BM***100000001",
        )

    def test_ach_debit(self):
        from src.parser.tdbank import _clean_description
        self.assertEqual(
            _clean_description("ACHDEBIT,CRUNCHFITCLUBFEES****200000002"),
            "CRUNCHFITCLUBFEES****200000002",
        )

    def test_ach_iat_debit(self):
        from src.parser.tdbank import _clean_description
        self.assertEqual(
            _clean_description("ACHIATDEBIT,TSBRETURNSLTDIATPAYPAL****300000003"),
            "TSBRETURNSLTDIATPAYPAL****300000003",
        )

    def test_electronicpmt_web_strips_prefix_and_space(self):
        """ELECTRONICPMT-WEB has a comma + space delimiter, not just comma."""
        from src.parser.tdbank import _clean_description
        self.assertEqual(
            _clean_description(
                "ELECTRONICPMT-WEB, AMZ_STORECRD_PMTPAYMENT****40000000004"
            ),
            "AMZ_STORECRD_PMTPAYMENT****40000000004",
        )

    def test_realtimepymt(self):
        from src.parser.tdbank import _clean_description
        self.assertEqual(
            _clean_description("REALTIMEPYMT, VENMO"),
            "VENMO",
        )

    def test_ccddeposit(self):
        from src.parser.tdbank import _clean_description
        self.assertEqual(
            _clean_description("CCDDEPOSIT,EBAYINCJBYJVCDUPAYMENTSA3OWE1HQISDLSBM"),
            "EBAYINCJBYJVCDUPAYMENTSA3OWE1HQISDLSBM",
        )

    def test_zelle_without_token_unchanged(self):
        """If the Zelle pattern doesn't match (e.g. missing the alphanum token),
        leave it raw rather than emit a malformed cleanup."""
        from src.parser.tdbank import _clean_description
        self.assertEqual(
            _clean_description("TDZELLESENT,plain text"),
            "TDZELLESENT,plain text",
        )


class TestTdbankParserIntegration(unittest.TestCase):
    """End-to-end: real TD PDFs should produce no descriptions still
    matching the airtight 'polluted' signal after parsing."""

    # The compound shape: <TYPE>,*****<digits>[, ]AUT<6digits> — this is
    # TD-internal noise that no organic merchant name would ever match.
    AUT_COMPOUND_RE = re.compile(r"^[A-Z]+,\*+\d+[, ]AUT\d{6}")
    ZELLE_RAW_RE = re.compile(r"^TDZELLE(?:SENT|RECEIVED),")
    ACH_ELECTRONIC_RE = re.compile(
        r"^(?:ACHDEPOSIT|ACHDEBIT|ACHIATDEBIT|CCDDEPOSIT|"
        r"ELECTRONICPMT-WEB|RTPRCVD|REALTIMEPYMT),"
    )

    def test_no_polluted_descriptions_after_parse(self):
        pdf_files = list(INPUT_DIR.glob("tdbank/*.pdf"))
        if not pdf_files:
            self.skipTest("No TD Bank PDF files in tests/parsers/fixtures/local/tdbank/")
        from src.parser.tdbank import parse_statement
        for pdf_file in pdf_files:
            data = parse_statement(str(pdf_file))
            for txn in data.transactions:
                self.assertIsNone(
                    self.AUT_COMPOUND_RE.match(txn.description),
                    f"{pdf_file.name}: AUT-compound prefix not stripped: {txn.description!r}",
                )
                self.assertIsNone(
                    self.ZELLE_RAW_RE.match(txn.description),
                    f"{pdf_file.name}: raw Zelle prefix not stripped: {txn.description!r}",
                )
                self.assertIsNone(
                    self.ACH_ELECTRONIC_RE.match(txn.description),
                    f"{pdf_file.name}: ACH/electronic prefix not stripped: {txn.description!r}",
                )


class TestAmexActivityCsvDepack(unittest.TestCase):
    """The Amex 'account activity' CSV packs Description as a fixed-width record
    (merchant field then glued city + state). _depack_activity_csv splits at the
    city column, returns a clean single-spaced string, and flags rows whose
    merchant field was full (== truncated mid-name → caller blanks the merchant).
    """

    def setUp(self):
        from src.parser.amex import _depack_activity_csv
        self._depack = _depack_activity_csv

    # Synthetic fixtures (no real cardholder data). Each preserves the export's
    # structure: the city begins at column 20, and a non-space in column 19
    # means the merchant field overran and was truncated.
    def test_complete_padded_merchant_resolves(self):
        # Short merchant: field is space-padded, so the merchant is intact.
        clean, trunc = self._depack("AplPay BLUE CAFE    RIVERTON          CA")
        self.assertEqual(clean, "AplPay BLUE CAFE RIVERTON CA")
        self.assertFalse(trunc)

    def test_twelve_char_merchant_still_complete(self):
        # 12-char name fills all but the last column — still has a trailing pad,
        # so it is NOT truncated (the boundary case the column rule gets right).
        clean, trunc = self._depack("AplPay CORNER DINER RIVERTON          CA")
        self.assertEqual(clean, "AplPay CORNER DINER RIVERTON CA")
        self.assertFalse(trunc)

    def test_internal_field_padding_collapsed(self):
        clean, trunc = self._depack("AplPay SHOP  PLAZA  RIVERTON          CA")
        self.assertEqual(clean, "AplPay SHOP PLAZA RIVERTON CA")
        self.assertFalse(trunc)

    def test_non_aplpay_row_uses_same_city_column(self):
        clean, trunc = self._depack("GENERIC STREAM      METRO CITY        NY")
        self.assertEqual(clean, "GENERIC STREAM METRO CITY NY")
        self.assertFalse(trunc)

    def test_truncated_merchant_glued_to_city_is_flagged(self):
        # Field is full (last column non-space) → glued + truncated.
        clean, trunc = self._depack("AplPay TST* SUSHI BARIVERTON          CA")
        self.assertEqual(clean, "AplPay TST* SUSHI BA RIVERTON CA")
        self.assertTrue(trunc)

    def test_truncated_slash_name_flagged(self):
        clean, trunc = self._depack("AplPay FUEL/MART XYZRIVERTON          CA")
        self.assertEqual(clean, "AplPay FUEL/MART XYZ RIVERTON CA")
        self.assertTrue(trunc)

    def test_non_fixed_width_row_unchanged(self):
        # No padding signature → not the activity format; returned as-is.
        clean, trunc = self._depack("Amex Send: Add Money")
        self.assertEqual(clean, "Amex Send: Add Money")
        self.assertFalse(trunc)


class TestAmexActivityCsvParse(unittest.TestCase):
    """End-to-end through parse_csv: the de-packed description is stored and the
    truncation flag rides along on ParsedTransaction."""

    def _parse(self, *desc_rows: str):
        import io
        from src.parser.amex import parse_csv
        lines = ["Date,Description,Amount"]
        for d in desc_rows:
            # Quote the description so embedded commas/spaces survive the reader.
            lines.append(f'05/23/2026,"{d}",8.99')
        buf = io.BytesIO("\n".join(lines).encode("utf-8"))
        return parse_csv(buf).transactions

    def test_complete_and_truncated_rows(self):
        txns = self._parse(
            "AplPay BLUE CAFE    RIVERTON          CA",
            "AplPay TST* SUSHI BARIVERTON          CA",
        )
        self.assertEqual(txns[0].description, "BLUE CAFE RIVERTON CA")
        self.assertFalse(txns[0].merchant_truncated)
        # AplPay stripped, de-glued, and flagged truncated.
        self.assertEqual(txns[1].description, "TST* SUSHI BA RIVERTON CA")
        self.assertTrue(txns[1].merchant_truncated)


class TestReconcileStatementBalance(unittest.TestCase):
    """Statement-level reconciliation guard (todo #78). Asset convention:
    deposits/credits raise the balance, purchases/withdrawals/fees lower it."""

    CREDIT = frozenset({"DEPOSIT", "CREDIT", "INTEREST", "TRANSFER_IN"})
    DEBIT = frozenset({"PURCHASE", "WITHDRAWAL", "FEE", "TRANSFER_OUT"})

    def _txn(self, amount, ttype):
        from src.parser.models import ParsedTransaction
        from datetime import date
        return ParsedTransaction(
            transaction_date=date(2026, 5, 1),
            description="x",
            amount=Decimal(amount),
            transaction_type=ttype,
        )

    def _reconcile(self, txns, expected_net):
        from src.parser.models import reconcile_statement_balance
        return reconcile_statement_balance(
            txns,
            expected_net_change=Decimal(expected_net),
            credit_types=self.CREDIT,
            debit_types=self.DEBIT,
            context="test",
        )

    def test_reconciled_when_rows_match_net_change(self):
        # +2669.21 +2681.08 deposits, -253.45 purchases -> net +5096.84.
        txns = [
            self._txn("2669.21", "Deposit"),
            self._txn("2681.08", "Deposit"),
            self._txn("253.45", "Purchase"),
        ]
        result = self._reconcile(txns, "5096.84")
        self.assertTrue(result.reconciled)
        self.assertEqual(result.detail, "")

    def test_unreconciled_when_a_row_is_dropped(self):
        # Statement says the balance fell 3882.77, but a parser that dropped the
        # large payments hands back only deposits + a few small purchases (the
        # exact 2026-05-13 failure that motivated this guard). Numeric mismatch is
        # a non-fatal warning: reconciled=False, no raise.
        # Kept rows net to +7830.48 (4 deposits 8083.45 - purchases 252.97);
        # statement moved -3882.77 -> off by exactly 11713.25.
        short = [
            self._txn("2669.21", "Deposit"),
            self._txn("2681.08", "Deposit"),
            self._txn("2686.18", "Deposit"),
            self._txn("46.98", "Deposit"),
            self._txn("252.97", "Purchase"),
        ]
        result = self._reconcile(short, "-3882.77")
        self.assertFalse(result.reconciled)
        self.assertEqual(result.delta, Decimal("11713.25"))
        self.assertIn("11713.25", result.detail.replace(",", ""))

    def test_tolerance_absorbs_one_cent(self):
        self.assertTrue(self._reconcile([self._txn("100.00", "Deposit")], "100.01").reconciled)

    def test_unreconciled_just_outside_tolerance(self):
        self.assertFalse(self._reconcile([self._txn("100.00", "Deposit")], "100.02").reconciled)

    def test_unclassified_type_raises_not_silently_skipped(self):
        # A type in neither set is a parser bug, not statement drift -> hard fail.
        from src.parser.models import StatementParseError
        with self.assertRaises(StatementParseError):
            self._reconcile([self._txn("100.00", "Mystery")], "100.00")

    def test_credit_card_debt_convention(self):
        # Liability convention: charges raise the (debt) balance, payments lower
        # it. New-Previous = +5255.87 - 6186.16 = -930.29.
        from src.parser.models import reconcile_statement_balance
        txns = [
            self._txn("5255.87", "Purchase"),
            self._txn("6186.16", "Credit"),
        ]
        result = reconcile_statement_balance(
            txns,
            expected_net_change=Decimal("-930.29"),
            credit_types=frozenset({"PURCHASE", "FEE", "INTEREST"}),
            debit_types=frozenset({"CREDIT", "TRANSFER_IN"}),
            context="cc",
        )
        self.assertTrue(result.reconciled)


class TestTdBankStatementReconciliation(unittest.TestCase):
    """The TD parser must reconcile every real statement in the local corpus
    (skips when the gitignored corpus is absent, e.g. fresh clone / CI)."""

    def test_all_local_tdbank_statements_reconcile(self):
        corpus = INPUT_DIR / "tdbank"
        pdfs = sorted(corpus.glob("*.pdf")) if corpus.exists() else []
        if not pdfs:
            self.skipTest("no local tdbank corpus")
        from src.parser.tdbank import parse_statement
        from src.parser.models import StatementParseError
        for pdf in pdfs:
            with self.subTest(statement=pdf.name):
                try:
                    parsed = parse_statement(pdf)
                except StatementParseError as e:
                    self.fail(f"{pdf.name} raised during reconciliation: {e}")
                self.assertIsNotNone(parsed.reconciliation, f"{pdf.name}: no balances found")
                self.assertTrue(
                    parsed.reconciliation.reconciled,
                    f"{pdf.name} did not reconcile: {parsed.reconciliation.detail}",
                )


class TestAmazonSynchronyStatementReconciliation(unittest.TestCase):
    """The Amazon (SYF) parser must reconcile every real statement in the local
    corpus — including the consolidated multi-month bundles, which telescope from
    the oldest Previous Balance to the newest New Balance (todo #78). Skips when
    the gitignored corpus is absent (fresh clone / CI)."""

    def test_all_local_amzn_statements_reconcile(self):
        corpus = INPUT_DIR / "amzn-synchrony"
        pdfs = sorted(corpus.glob("*.pdf")) if corpus.exists() else []
        if not pdfs:
            self.skipTest("no local amzn-synchrony corpus")
        from src.parser.amzn_syf import parse_statement
        from src.parser.models import StatementParseError
        for pdf in pdfs:
            with self.subTest(statement=pdf.name):
                try:
                    parsed = parse_statement(pdf)
                except StatementParseError as e:
                    self.fail(f"{pdf.name} raised during reconciliation: {e}")
                self.assertIsNotNone(parsed.reconciliation, f"{pdf.name}: no balances found")
                self.assertTrue(
                    parsed.reconciliation.reconciled,
                    f"{pdf.name} did not reconcile: {parsed.reconciliation.detail}",
                )


class TestAmexStatementReconciliation(unittest.TestCase):
    """The Amex parser must reconcile every real statement in the local corpus,
    dodging the all-$0.00 "Minimum Payment Warning" decoy block and the
    Pay-Over-Time sub-balance breakdown (todo #78). Skips when the gitignored
    corpus is absent (fresh clone / CI)."""

    def test_all_local_amex_statements_reconcile(self):
        from src.parser.amex import parse_statement
        from src.parser.models import StatementParseError
        pdfs = []
        for sub in ("amex", "amex-gold-1005"):
            corpus = INPUT_DIR / sub
            if corpus.exists():
                pdfs.extend(sorted(corpus.glob("*.pdf")))
        if not pdfs:
            self.skipTest("no local amex corpus")
        for pdf in pdfs:
            with self.subTest(statement=pdf.name):
                try:
                    parsed = parse_statement(pdf)
                except StatementParseError as e:
                    self.fail(f"{pdf.name} raised during reconciliation: {e}")
                self.assertIsNotNone(parsed.reconciliation, f"{pdf.name}: no balances found")
                self.assertTrue(
                    parsed.reconciliation.reconciled,
                    f"{pdf.name} did not reconcile: {parsed.reconciliation.detail}",
                )


if __name__ == "__main__":
    unittest.main()
