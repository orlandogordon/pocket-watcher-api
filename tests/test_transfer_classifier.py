"""Tier A transfer classifier unit tests."""
import unittest
from dataclasses import dataclass
from typing import Optional

from src.db.core import AccountType, TransactionType
from src.services.transfer_classifier import (
    P2P_DENYLIST,
    build_account_tokens,
    classify_outflow,
    classify_parsed_transactions,
)


@dataclass
class FakeAccount:
    id: int
    account_name: str
    institution_name: str
    account_type: AccountType
    account_number_last4: Optional[str] = None
    user_id: int = 1


@dataclass
class FakeParsed:
    description: str
    transaction_type: str  # mutated by Tier A


class TestTokenBuilder(unittest.TestCase):
    def test_amex_gold(self):
        account = FakeAccount(
            id=2, account_name="Amex Gold", institution_name="American Express",
            account_type=AccountType.CREDIT_CARD, account_number_last4="1005",
        )
        tokens = build_account_tokens(account)
        # Full normalized names included.
        self.assertIn("AMEXGOLD", tokens)
        self.assertIn("AMERICANEXPRESS", tokens)
        # Useful single words included.
        self.assertIn("AMEX", tokens)
        self.assertIn("AMERICAN", tokens)
        self.assertIn("EXPRESS", tokens)
        # Last4 included.
        self.assertIn("1005", tokens)
        # Stopwords dropped.
        self.assertNotIn("GOLD", tokens)

    def test_schwab_brokerage(self):
        account = FakeAccount(
            id=3, account_name="Schwab Brokerage", institution_name="Charles Schwab",
            account_type=AccountType.INVESTMENT,
        )
        tokens = build_account_tokens(account)
        self.assertIn("SCHWAB", tokens)
        self.assertIn("CHARLESSCHWAB", tokens)
        self.assertIn("SCHWABBROKERAGE", tokens)
        self.assertNotIn("BROKERAGE", tokens)  # stopword


class TestClassifyOutflow(unittest.TestCase):
    def setUp(self):
        self.checking = FakeAccount(
            id=1, account_name="TD Main Checking", institution_name="TD Bank",
            account_type=AccountType.CHECKING,
        )
        self.amex = FakeAccount(
            id=2, account_name="Amex Gold", institution_name="American Express",
            account_type=AccountType.CREDIT_CARD,
        )
        self.synchrony = FakeAccount(
            id=3, account_name="Amazon Store Card", institution_name="Synchrony Bank",
            account_type=AccountType.CREDIT_CARD,
        )
        self.schwab = FakeAccount(
            id=4, account_name="Schwab Brokerage", institution_name="Charles Schwab",
            account_type=AccountType.INVESTMENT,
        )
        self.venmo = FakeAccount(
            id=5, account_name="Venmo", institution_name="Venmo",
            account_type=AccountType.OTHER,
        )
        self.all_accounts = [self.checking, self.amex, self.synchrony, self.schwab, self.venmo]

    def test_amex_payment_classifies_as_transfer_out(self):
        result = classify_outflow(
            description="ELECTRONICPMT-WEB, AMEXEPAYMENTACHPMTM5552",
            source_account_id=self.checking.id,
            user_accounts=self.all_accounts,
        )
        self.assertEqual(result.transaction_type, TransactionType.TRANSFER_OUT)
        self.assertEqual(result.suggested_partner_account_id, self.amex.id)

    def test_synchrony_amazon_payment(self):
        result = classify_outflow(
            description="ELECTRONICPMT-WEB, AMAZONCORPSYFPAYMNT 78116246568",
            source_account_id=self.checking.id,
            user_accounts=self.all_accounts,
        )
        self.assertEqual(result.transaction_type, TransactionType.TRANSFER_OUT)
        self.assertEqual(result.suggested_partner_account_id, self.synchrony.id)

    def test_schwab_funding(self):
        result = classify_outflow(
            description="SCHWAB BROKERAGE MONEYLINK XFER",
            source_account_id=self.checking.id,
            user_accounts=self.all_accounts,
        )
        self.assertEqual(result.transaction_type, TransactionType.TRANSFER_OUT)
        self.assertEqual(result.suggested_partner_account_id, self.schwab.id)

    def test_venmo_stays_purchase_via_denylist(self):
        result = classify_outflow(
            description="ELECTRONICPMT-WEB, VENMOPAYMENT 1234",
            source_account_id=self.checking.id,
            user_accounts=self.all_accounts,
        )
        self.assertEqual(result.transaction_type, TransactionType.PURCHASE)
        self.assertIsNone(result.suggested_partner_account_id)

    def test_unrelated_merchant_stays_purchase(self):
        result = classify_outflow(
            description="STARBUCKS COFFEE #1234",
            source_account_id=self.checking.id,
            user_accounts=self.all_accounts,
        )
        self.assertEqual(result.transaction_type, TransactionType.PURCHASE)

    def test_longest_match_wins_on_multi_candidate(self):
        # Build a contrived case: Schwab and a hypothetical account just named
        # "Schwab" (single token). Description "SCHWABBROKERAGE" should pick
        # the more specific Schwab Brokerage match.
        plain_schwab = FakeAccount(
            id=6, account_name="Schwab", institution_name="Schwab",
            account_type=AccountType.CREDIT_CARD,
        )
        result = classify_outflow(
            description="SCHWABBROKERAGEXFER",
            source_account_id=self.checking.id,
            user_accounts=[self.checking, self.schwab, plain_schwab],
        )
        # SCHWABBROKERAGE (15 chars) > SCHWAB (6 chars).
        self.assertEqual(result.suggested_partner_account_id, self.schwab.id)

    def test_excludes_source_account(self):
        # Description mentions TD; source account is TD checking — should
        # NOT classify as TRANSFER_OUT to itself.
        result = classify_outflow(
            description="TD BANK INTEREST",
            source_account_id=self.checking.id,
            user_accounts=[self.checking],
        )
        self.assertEqual(result.transaction_type, TransactionType.PURCHASE)

    def test_excludes_checking_partner(self):
        # A second checking account is also NOT a transfer-target for Tier A
        # (only CC/INVESTMENT/LOAN/OTHER are partner candidates).
        td_savings = FakeAccount(
            id=7, account_name="TD Savings", institution_name="TD Bank",
            account_type=AccountType.SAVINGS,
        )
        result = classify_outflow(
            description="TD BANK XFER",
            source_account_id=self.checking.id,
            user_accounts=[self.checking, td_savings],
        )
        self.assertEqual(result.transaction_type, TransactionType.PURCHASE)


class TestClassifyParsedTransactions(unittest.TestCase):
    def test_mutates_type_and_returns_suggestion(self):
        source = FakeAccount(
            id=1, account_name="TD Checking", institution_name="TD Bank",
            account_type=AccountType.CHECKING,
        )
        amex = FakeAccount(
            id=2, account_name="Amex Gold", institution_name="American Express",
            account_type=AccountType.CREDIT_CARD,
        )
        parsed = [
            FakeParsed(description="AMEXEPAYMENT 5552", transaction_type="PURCHASE"),
            FakeParsed(description="STARBUCKS", transaction_type="PURCHASE"),
        ]
        suggestions = classify_parsed_transactions(parsed, source, [source, amex])
        self.assertEqual(parsed[0].transaction_type, "TRANSFER_OUT")
        self.assertEqual(parsed[1].transaction_type, "PURCHASE")
        self.assertEqual(suggestions[0].suggested_partner_account_id, 2)
        self.assertNotIn(1, suggestions)

    def test_noop_when_source_is_credit_card(self):
        # Tier A only runs on checking/savings sources.
        source = FakeAccount(
            id=1, account_name="Amex", institution_name="Amex",
            account_type=AccountType.CREDIT_CARD,
        )
        parsed = [FakeParsed(description="AMEXEPAYMENT", transaction_type="PURCHASE")]
        suggestions = classify_parsed_transactions(parsed, source, [source])
        self.assertEqual(suggestions, {})
        self.assertEqual(parsed[0].transaction_type, "PURCHASE")

    def test_denylist_includes_expected_terms(self):
        # Sanity that the denylist set is what we think it is.
        for term in ("VENMO", "ZELLE", "CASHAPP", "CASH APP", "PAYPAL"):
            self.assertIn(term, P2P_DENYLIST)


if __name__ == "__main__":
    unittest.main()
