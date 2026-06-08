"""Tests for parser TRANSFER_IN/TRANSFER_OUT output."""
import re
import unittest
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


class TestTDAmeritradeRecoverSplit(unittest.TestCase):
    """#72: recover qty/price when a wide price spills a digit across the fixed
    Quantity|Price column boundary ('3 | 3283.0201' extracted as '33 | 283.0201').
    """

    def _recover(self, qty, price, target):
        from decimal import Decimal
        from src.parser.tdameritrade import _recover_misaligned_qty_price
        tol = max(abs(Decimal(target)) * Decimal("0.01"), Decimal("1"))
        return _recover_misaligned_qty_price(Decimal(qty), Decimal(price), abs(Decimal(target)), tol)

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


if __name__ == "__main__":
    unittest.main()
