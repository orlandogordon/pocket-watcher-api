"""Tests for parser TRANSFER_IN/TRANSFER_OUT output."""
import unittest
from pathlib import Path

INPUT_DIR = Path(__file__).parent.parent / "input"


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

    def test_ach_withdrawal(self):
        from src.parser.ameriprise import _normalize_transaction_type
        self.assertEqual(_normalize_transaction_type("ACH", "ACH DIRECT WITHDRAWAL TRACE #123"), "TRANSFER_OUT")

    def test_non_transfer_unchanged(self):
        from src.parser.ameriprise import _normalize_transaction_type
        self.assertEqual(_normalize_transaction_type("DIVIDEND PAYMENT", ""), "DIVIDEND")
        self.assertEqual(_normalize_transaction_type("BUY", ""), "BUY")
        self.assertEqual(_normalize_transaction_type("SELL", ""), "SELL")


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


class TestAmexParserIntegration(unittest.TestCase):
    def test_pdf_payment_is_transfer_in(self):
        pdf_files = list(INPUT_DIR.glob("amex/*.pdf"))
        if not pdf_files:
            self.skipTest("No Amex PDF files in input/")
        from src.parser.amex import parse_statement
        for pdf_file in pdf_files:
            result = parse_statement(str(pdf_file))
            for txn in result.transactions:
                self.assertNotEqual(txn.transaction_type, "Payment",
                    f"Found raw 'Payment' type — should be TRANSFER_IN")


class TestAmexCleanDescription(unittest.TestCase):
    def test_strips_aplpay_prefix(self):
        from src.parser.amex import _clean_description
        self.assertEqual(_clean_description("AplPay TARGET BRICK NJ"), "TARGET BRICK NJ")

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
            self.skipTest("No Amazon Synchrony PDF files in input/")
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
        self.assertTrue(_is_balance_affecting("", "TD BANK, NA *4636"))

    def test_balance_affecting_skips_external_funded(self):
        from src.parser.venmo import _is_balance_affecting
        # Payment sent via Visa: external funding, destination blank.
        self.assertFalse(_is_balance_affecting("Visa *1312", ""))
        # Charge paid via Amex Send: external funding, destination blank.
        self.assertFalse(_is_balance_affecting("Amex Send Account", ""))
        # Merchant Transaction via TD Bank checking: external funding.
        self.assertFalse(
            _is_balance_affecting("TD BANK, NA Personal Checking *4636", "")
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
    def test_jan_2023_reconciles_to_zero(self):
        """Beginning balance $180, ending balance $0 — net change must be -$180."""
        from src.parser.venmo import parse_csv
        from decimal import Decimal
        csv_path = INPUT_DIR / "venmo" / "VenmoStatement_January_2023.csv"
        if not csv_path.exists():
            self.skipTest("Venmo Jan 2023 fixture missing")
        data = parse_csv(csv_path)
        net = Decimal("0")
        for t in data.transactions:
            if t.transaction_type in ("DEPOSIT", "TRANSFER_IN"):
                net += t.amount
            else:
                net -= t.amount
        self.assertEqual(net, Decimal("-180.00"))

    def test_feb_2025_reconciles_to_four(self):
        """Beginning balance $0, ending balance $4 — net change must be +$4."""
        from src.parser.venmo import parse_csv
        from decimal import Decimal
        csv_path = INPUT_DIR / "venmo" / "VenmoStatement_February_2025.csv"
        if not csv_path.exists():
            self.skipTest("Venmo Feb 2025 fixture missing")
        data = parse_csv(csv_path)
        net = Decimal("0")
        for t in data.transactions:
            if t.transaction_type in ("DEPOSIT", "TRANSFER_IN"):
                net += t.amount
            else:
                net -= t.amount
        self.assertEqual(net, Decimal("4.00"))

    def test_skips_external_funded_rows(self):
        """Jan 2023 has 14 raw rows, 9 of which are funded by Visa *1312.
        Only 5 deposits + 1 cashout should be emitted (= 6 transactions)."""
        from src.parser.venmo import parse_csv
        csv_path = INPUT_DIR / "venmo" / "VenmoStatement_January_2023.csv"
        if not csv_path.exists():
            self.skipTest("Venmo Jan 2023 fixture missing")
        data = parse_csv(csv_path)
        self.assertEqual(len(data.transactions), 6)

    def test_full_year_fixtures_reconcile(self):
        """Reconcile every full-year + monthly fixture against the
        Beginning/Ending balance pair reported by Venmo itself."""
        from src.parser.venmo import parse_csv
        from decimal import Decimal
        import csv

        expected = {
            "venmo - 2020.csv": (Decimal("0.00"), Decimal("0.00")),
            "venmo - 2021.csv": (Decimal("0.00"), Decimal("15.00")),
            "venmo - 2022.csv": (Decimal("15.00"), Decimal("180.00")),
            "VenmoStatement_January_2023.csv": (Decimal("180.00"), Decimal("0.00")),
            "VenmoStatement_February_2025.csv": (Decimal("0.00"), Decimal("4.00")),
        }
        for fname, (begin, end) in expected.items():
            csv_path = INPUT_DIR / "venmo" / fname
            if not csv_path.exists():
                continue
            data = parse_csv(csv_path)
            net = Decimal("0")
            for t in data.transactions:
                if t.transaction_type in ("DEPOSIT", "TRANSFER_IN"):
                    net += t.amount
                else:
                    net -= t.amount
            self.assertEqual(
                net, end - begin,
                f"{fname}: net change {net} does not reconcile to {end - begin}",
            )


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
        # "$535 Payment From Chris Provel" is auto-generated boilerplate;
        # description should fall back to just the Name column.
        self.assertEqual(
            _build_description("P2P", "$535 Payment From Chris Provel", "Chris Provel", "Cash Balance"),
            "Chris Provel",
        )

    def test_description_keeps_user_supplied_note(self):
        from src.parser.cashapp import _build_description
        # User-supplied memo (an emoji, a freeform string) should be kept.
        self.assertEqual(
            _build_description("P2P", "Uber home", "Matt Mihm", "Cash Balance"),
            "Matt Mihm: Uber home",
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
        (Matt Mihm tally hose $20 + Uber home $30)."""
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


if __name__ == "__main__":
    unittest.main()
