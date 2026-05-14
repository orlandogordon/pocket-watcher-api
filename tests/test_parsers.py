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


if __name__ == "__main__":
    unittest.main()
