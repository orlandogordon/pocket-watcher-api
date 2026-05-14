"""Tier A transfer classifier unit tests."""
import unittest
from dataclasses import dataclass, field
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
    match_aliases: Optional[list] = field(default_factory=list)


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
        # Last4 included.
        self.assertIn("1005", tokens)
        # Stopwords dropped.
        self.assertNotIn("GOLD", tokens)
        self.assertNotIn("EXPRESS", tokens)  # too generic — false-positive source
        self.assertNotIn("AMERICAN", tokens)  # too generic

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
        self.assertNotIn("CHARLES", tokens)  # stopword

    def test_amazon_store_card_drops_store_and_amazon(self):
        """The two most common false-positive sources from live data:
        'STORE' matching DERMSTORECOM etc., 'AMAZON' matching AMAZONCOM.
        Both should be dropped from the per-word token set."""
        account = FakeAccount(
            id=4, account_name="Amazon Store Card", institution_name="Synchrony Bank",
            account_type=AccountType.CREDIT_CARD,
        )
        tokens = build_account_tokens(account)
        self.assertIn("AMAZONSTORECARD", tokens)  # full normalized name
        self.assertIn("SYNCHRONY", tokens)
        self.assertIn("SYNCHRONYBANK", tokens)
        self.assertNotIn("STORE", tokens)
        self.assertNotIn("AMAZON", tokens)

    def test_aliases_added_to_tokens(self):
        """match_aliases are user-supplied alternative match strings —
        normalized and added to the token set as-is."""
        account = FakeAccount(
            id=5, account_name="Amazon Store Card", institution_name="Synchrony Bank",
            account_type=AccountType.CREDIT_CARD,
            match_aliases=["AMZ_STORECRD", "AMAZONCORPSYF"],
        )
        tokens = build_account_tokens(account)
        self.assertIn("AMZ_STORECRD", tokens)
        self.assertIn("AMAZONCORPSYF", tokens)


class TestClassifyOutflow(unittest.TestCase):
    def setUp(self):
        self.checking = FakeAccount(
            id=1, account_name="TD Main Checking", institution_name="TD Bank",
            account_type=AccountType.CHECKING, account_number_last4="4636",
        )
        self.amex = FakeAccount(
            id=2, account_name="Amex Gold", institution_name="American Express",
            account_type=AccountType.CREDIT_CARD, account_number_last4="1005",
        )
        self.synchrony = FakeAccount(
            id=3, account_name="Amazon Store Card", institution_name="Synchrony Bank",
            account_type=AccountType.CREDIT_CARD, account_number_last4="5685",
            match_aliases=["AMZ_STORECRD", "AMAZONCORPSYF", "PAYMENTFORAMZSTORECARD"],
        )
        self.schwab = FakeAccount(
            id=4, account_name="Schwab Brokerage", institution_name="Charles Schwab",
            account_type=AccountType.INVESTMENT, account_number_last4="9145",
        )
        # Venmo / Cash App are OTHER (not CHECKING) so Tier A treats them
        # as valid transfer partners. CHECKING-to-CHECKING transfers are
        # by design handled by Tier B pairing instead, not Tier A.
        self.venmo = FakeAccount(
            id=5, account_name="Venmo", institution_name="Venmo",
            account_type=AccountType.OTHER,
        )
        self.cashapp = FakeAccount(
            id=6, account_name="Cash App", institution_name="Cash App",
            account_type=AccountType.OTHER,
        )
        self.all_accounts = [
            self.checking, self.amex, self.synchrony, self.schwab,
            self.venmo, self.cashapp,
        ]

    # --- Legit transfers must still classify ----------------------------

    def test_amex_payment_classifies_as_transfer_out(self):
        result = classify_outflow(
            description="ELECTRONICPMT-WEB, AMEXEPAYMENTACHPMTM5552",
            source_account_id=self.checking.id,
            user_accounts=self.all_accounts,
        )
        self.assertEqual(result.transaction_type, TransactionType.TRANSFER_OUT)
        self.assertEqual(result.suggested_partner_account_id, self.amex.id)

    def test_synchrony_amzn_storecrd_via_alias(self):
        """AMZ_STORECRD on the bank statement is an abbreviation that
        doesn't substring-match 'Amazon Store Card'. The user-supplied
        alias 'AMZ_STORECRD' carries the match."""
        result = classify_outflow(
            description="ELECTRONICPMT-WEB, AMZ_STORECRD_PMTPAYMENT****78116246568",
            source_account_id=self.checking.id,
            user_accounts=self.all_accounts,
        )
        self.assertEqual(result.transaction_type, TransactionType.TRANSFER_OUT)
        self.assertEqual(result.suggested_partner_account_id, self.synchrony.id)

    def test_synchrony_amazoncorp_via_alias(self):
        result = classify_outflow(
            description="ELECTRONICPMT-WEB, AMAZONCORPSYFPAYMNT****78116246568",
            source_account_id=self.checking.id,
            user_accounts=self.all_accounts,
        )
        self.assertEqual(result.transaction_type, TransactionType.TRANSFER_OUT)
        self.assertEqual(result.suggested_partner_account_id, self.synchrony.id)

    def test_synchrony_paymentforamzstorecard_via_alias(self):
        result = classify_outflow(
            description="ELECTRONICPMT-WEB, PAYMENTFORAMZSTORECARD****05044",
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

    def test_venmo_payment_classifies_as_transfer_out(self):
        """Post-#49 step 1+2: Venmo is a real account, so VENMOPAYMENT on
        TD should pair to the Venmo account (was denylisted before)."""
        result = classify_outflow(
            description="ELECTRONICPMT-WEB, VENMOPAYMENT 1234",
            source_account_id=self.checking.id,
            user_accounts=self.all_accounts,
        )
        self.assertEqual(result.transaction_type, TransactionType.TRANSFER_OUT)
        self.assertEqual(result.suggested_partner_account_id, self.venmo.id)

    def test_cashapp_add_money_classifies_as_transfer_out(self):
        result = classify_outflow(
            description="ELECTRONICPMT-WEB, CASHAPP ADD MONEY",
            source_account_id=self.checking.id,
            user_accounts=self.all_accounts,
        )
        self.assertEqual(result.transaction_type, TransactionType.TRANSFER_OUT)
        self.assertEqual(result.suggested_partner_account_id, self.cashapp.id)

    # --- Confirmed false positives must NOT classify --------------------

    def test_walgreens_stays_purchase(self):
        """Generic 'STORE' substring inside WALGREENSSTORE used to flip
        this Walgreens debit purchase to TRANSFER_OUT to Amazon Store Card."""
        result = classify_outflow(
            description="DEBITPOS,*****30089881312,AUT110921DDAPURCHASE WALGREENSSTORE2479CHU TOMSRIVER *NJ",
            source_account_id=self.checking.id,
            user_accounts=self.all_accounts,
        )
        self.assertEqual(result.transaction_type, TransactionType.PURCHASE)

    def test_dermstore_stays_purchase(self):
        result = classify_outflow(
            description="DEBITCARDPURCHASE,*****30089881312,AUT030322VISADDAPUR WWW DERMSTORECOM WILMINGTON *DE",
            source_account_id=self.checking.id,
            user_accounts=self.all_accounts,
        )
        self.assertEqual(result.transaction_type, TransactionType.PURCHASE)

    def test_express_clothing_stays_purchase(self):
        """Generic 'EXPRESS' substring inside EXPRESSCOM used to flip this
        Express clothing purchase to TRANSFER_OUT to Amex Gold."""
        result = classify_outflow(
            description="DEBITCARDPURCHASE,*****30089881312,AUT070622VISADDAPUR EXPRESSCOM 8883971980 *OH",
            source_account_id=self.checking.id,
            user_accounts=self.all_accounts,
        )
        self.assertEqual(result.transaction_type, TransactionType.PURCHASE)

    def test_holiday_inn_express_stays_purchase(self):
        result = classify_outflow(
            description="DEBITCARDPURCHASE,*****30089881312,AUT082221VISADDAPUR HOLIDAYINNEXPRESS SU JERSEYCITY *NJ",
            source_account_id=self.checking.id,
            user_accounts=self.all_accounts,
        )
        self.assertEqual(result.transaction_type, TransactionType.PURCHASE)

    def test_amazon_retail_stays_purchase(self):
        """AMAZONCOM in a Visa debit purchase used to flip to TRANSFER_OUT
        to Amazon Store Card via the over-generic 'AMAZON' token."""
        result = classify_outflow(
            description="DEBITCARDPURCHASE,*****30089881312,AUT102520VISADDAPUR AMAZONCOM2T96D3WC1 AMZ AMZNCOMBILL*WA",
            source_account_id=self.checking.id,
            user_accounts=self.all_accounts,
        )
        self.assertEqual(result.transaction_type, TransactionType.PURCHASE)

    def test_fanduel_stays_purchase_after_card_mask_strip(self):
        """The mask '****03991459200' contains '9145' as a substring,
        which used to spuriously match Schwab's last4. Card-mask strip +
        digit-boundary on last4 prevents this."""
        result = classify_outflow(
            description="ELECTRONICPMT-WEB, FANDUELINCSTARDUST****03991459200",
            source_account_id=self.checking.id,
            user_accounts=self.all_accounts,
        )
        self.assertEqual(result.transaction_type, TransactionType.PURCHASE)

    # --- Targeted regression tests for the underlying fixes -------------

    def test_card_mask_is_stripped_before_matching(self):
        """Even if a 4-digit last4 happens to appear inside the card-mask,
        no match should fire."""
        # Schwab last4 = 9145. Mask contains 9145 as substring.
        result = classify_outflow(
            description="DEBITCARDPURCHASE,*****91450000,AUT070622VISADDAPUR RANDOMMERCHANT *CA",
            source_account_id=self.checking.id,
            user_accounts=self.all_accounts,
        )
        self.assertEqual(result.transaction_type, TransactionType.PURCHASE)

    def test_last4_requires_digit_boundary(self):
        """Even outside the mask, a 4-digit last4 should not match when
        embedded in a longer digit run."""
        # Amex last4 = 1005. Should not match inside 1000500000.
        result = classify_outflow(
            description="SOME REFERENCE 1000500000 CHARGE",
            source_account_id=self.checking.id,
            user_accounts=self.all_accounts,
        )
        self.assertEqual(result.transaction_type, TransactionType.PURCHASE)

    def test_last4_matches_when_digit_bounded(self):
        """The legitimate use of last4 — '...something 1005 something' —
        should still match."""
        result = classify_outflow(
            description="PAYMENT TO ACCT ENDING 1005",
            source_account_id=self.checking.id,
            user_accounts=self.all_accounts,
        )
        self.assertEqual(result.transaction_type, TransactionType.TRANSFER_OUT)
        self.assertEqual(result.suggested_partner_account_id, self.amex.id)

    # --- Existing behavioral guards -------------------------------------

    def test_unrelated_merchant_stays_purchase(self):
        result = classify_outflow(
            description="STARBUCKS COFFEE #1234",
            source_account_id=self.checking.id,
            user_accounts=self.all_accounts,
        )
        self.assertEqual(result.transaction_type, TransactionType.PURCHASE)

    def test_paypal_still_denylisted(self):
        """PayPal still has no corresponding user-owned account in this
        codebase, so the denylist still hard-skips it."""
        result = classify_outflow(
            description="ELECTRONICPMT-WEB, PAYPAL XFER",
            source_account_id=self.checking.id,
            user_accounts=self.all_accounts,
        )
        self.assertEqual(result.transaction_type, TransactionType.PURCHASE)

    def test_longest_match_wins_on_multi_candidate(self):
        plain_schwab = FakeAccount(
            id=7, account_name="Schwab", institution_name="Schwab",
            account_type=AccountType.CREDIT_CARD,
        )
        result = classify_outflow(
            description="SCHWABBROKERAGEXFER",
            source_account_id=self.checking.id,
            user_accounts=[self.checking, self.schwab, plain_schwab],
        )
        self.assertEqual(result.suggested_partner_account_id, self.schwab.id)

    def test_excludes_source_account(self):
        result = classify_outflow(
            description="TD BANK INTEREST",
            source_account_id=self.checking.id,
            user_accounts=[self.checking],
        )
        self.assertEqual(result.transaction_type, TransactionType.PURCHASE)

    def test_excludes_checking_partner(self):
        td_savings = FakeAccount(
            id=8, account_name="TD Savings", institution_name="TD Bank",
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
        source = FakeAccount(
            id=1, account_name="Amex", institution_name="Amex",
            account_type=AccountType.CREDIT_CARD,
        )
        parsed = [FakeParsed(description="AMEXEPAYMENT", transaction_type="PURCHASE")]
        suggestions = classify_parsed_transactions(parsed, source, [source])
        self.assertEqual(suggestions, {})

    def test_denylist_no_longer_includes_venmo_or_cashapp(self):
        """Post-#49 step 1+2 the Venmo/Cash App denylist entries were
        removed so their descriptions can pair to real accounts."""
        self.assertNotIn("VENMO", P2P_DENYLIST)
        self.assertNotIn("CASHAPP", P2P_DENYLIST)


if __name__ == "__main__":
    unittest.main()
