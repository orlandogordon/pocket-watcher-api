"""Tests for src.services.merchant_extractor.

Coverage focuses on the post-#48/#50 description shapes that the extractor
must now handle (see todo #54). The substring-safety property — output is
always either a substring of input or a hand-curated alias value — is
asserted indirectly via the explicit expected merchants.
"""
import unittest

from src.services.merchant_extractor import extract_merchant


# ---------- Section A: post-strip TD purchase capture ----------

class TestTDBankPurchasePostStrip(unittest.TestCase):
    """The TD parser's _clean_description strips the AUT compound prefix,
    leaving a `<MERCHANT> <CITY> *<ST>` shape with single-space separators.
    The post-#50 regex must capture the merchant from this shape."""

    def test_microsoft_xbox(self):
        # Cleaned shape of DEBITCARDPURCHASE,*****...AUT...VISADDAPUR MICROSOFTXBOX MSBILLINFO *WA
        self.assertEqual(
            extract_merchant("tdbank", "MICROSOFTXBOX MSBILLINFO *WA"),
            "Microsoftxbox",
        )

    def test_wawa_with_store_number(self):
        # Trailing store-number digits stripped from merchant slot.
        self.assertEqual(
            extract_merchant("tdbank", "WAWA 939 TOMSRIVER *NJ"),
            "Wawa",
        )

    def test_walgreens_with_store_number(self):
        self.assertEqual(
            extract_merchant("tdbank", "WALGREENS 6321 BRICK *NJ"),
            "Walgreens",
        )

    def test_squashed_address_rejected(self):
        # 849FISCHERBLVD is an address, not a merchant — must return None
        # so the row falls through to LLM. Post-strip TD addresses have no
        # space between number and street name.
        self.assertIsNone(extract_merchant("tdbank", "849FISCHERBLVD TOMSRIVER *NJ"))

    def test_spaced_address_rejected(self):
        # The classic spaced address shape stays rejected.
        self.assertIsNone(extract_merchant("tdbank", "1120 TILTON RD NORTHFIELD *NJ"))

    def test_no_state_suffix_returns_none(self):
        # Without the trailing `*<ST>` anchor we can't be sure we have a
        # purchase row vs an ACH row that happens to look similar.
        self.assertIsNone(extract_merchant("tdbank", "WAWA FUEL TOMS RIVER"))


# ---------- Section B: Zelle prefix alias (post-#50) ----------

class TestZelleAlias(unittest.TestCase):
    """Post-#50 the TD parser rewrites Zelle rows to `Zelle: <counterparty>`.
    The merchant column stays as the literal "Zelle" so all Zelle activity
    groups together in analytics; counterparty info lives in the description.
    """

    def test_zelle_with_counterparty(self):
        self.assertEqual(
            extract_merchant("tdbank", "Zelle: MATTHEWMIHM"),
            "Zelle",
        )

    def test_zelle_with_multiword_counterparty(self):
        self.assertEqual(
            extract_merchant("tdbank", "Zelle: TRONGHIEN NGUYEN"),
            "Zelle",
        )

    def test_old_td_zelle_shape_no_longer_matches(self):
        # Sanity-check: pre-#50 shape should no longer hit the Zelle alias.
        # It also doesn't match any other pattern, so falls through to None.
        # If a re-import surfaces an old shape, the LLM picks it up.
        self.assertIsNone(extract_merchant("tdbank", "TD ZELLE SENT TRONGHIEN NGUYEN"))


# ---------- Section C: bare-token rejector on AmEx POS ----------

class TestAmexBareTokenRejection(unittest.TestCase):
    """After #48 strips `AplPay `, rows like `STORE TOMS RIVER NJ` capture
    merchant=`Store`, `MAX NEW YORK NY` captures `Max`, etc. A capture
    composed *entirely* of generic descriptor tokens is rejected; captures
    that contain a generic token as part of a multi-word name still pass."""

    def test_bare_store_rejected(self):
        self.assertIsNone(extract_merchant("amex", "STORE TOMS RIVER NJ"))

    def test_bare_max_rejected(self):
        self.assertIsNone(extract_merchant("amex", "MAX NEW YORK NY"))

    def test_bare_the_club_rejected(self):
        self.assertIsNone(extract_merchant("amex", "THE CLUB HOBOKEN NJ"))

    def test_generic_token_eaten_by_city_pattern(self):
        # Documented limitation: the AmEx POS regex captures merchant lazily,
        # so for `JOES CAFE BROOKLYN NY` the boundary lands at the first space
        # and merchant=`JOES` (the `CAFE` gets absorbed by the city group).
        # The bare-token rejector doesn't apply because `JOES` alone isn't
        # generic. Acceptable — the merchant is still meaningful for grouping.
        self.assertEqual(
            extract_merchant("amex", "JOES CAFE BROOKLYN NY"),
            "Joes",
        )

    def test_unambiguous_merchant_unchanged(self):
        # Regression guard: existing behavior on a clean row.
        self.assertEqual(
            extract_merchant("amex", "CHIPOTLE NEW YORK NY"),
            "Chipotle",
        )


# ---------- Section D: squashed-token aliases + trailing-ref strip ----------

class TestSquashedTokenAliases(unittest.TestCase):
    """Post-#50 the TD parser leaves ACH/ELECTRONICPMT rows as
    `<SQUASHED_PAYEE>****<digits>` (or `<SQUASHED_PAYEE><digits>` for some
    payees). The alias table holds the bare payee token; `_strip_trailing_ref`
    handles the variable suffix before lookup."""

    def test_crunch_with_star_ref(self):
        self.assertEqual(
            extract_merchant("tdbank", "CRUNCHFITCLUBFEES****300238869"),
            "Crunch Fitness",
        )

    def test_njclass_unifies_to_hesaa(self):
        # NJCLASS (pre-2024 shape) and HESAA (post-2024) are the same lender;
        # both should map to the same merchant for clean grouping.
        self.assertEqual(
            extract_merchant("tdbank", "STATEOFNJNJCLASSLN****41203"),
            "HESAA",
        )

    def test_hesaa_payment_p(self):
        # Bare-digit trailing ref (no leading stars) still strips.
        self.assertEqual(
            extract_merchant("tdbank", "HESAAPAYMENTP19515308"),
            "HESAA",
        )

    def test_dept_education_bare_digit_ref(self):
        self.assertEqual(
            extract_merchant("tdbank", "DEPTEDUCATIONSTUDENTLN0000"),
            "Dept of Education",
        )

    def test_pnc_bank_salary(self):
        self.assertEqual(
            extract_merchant("tdbank", "PNCBANKNAREGSALARY****40047586"),
            "PNC Bank",
        )

    def test_willis_north_america(self):
        # Mid-string `*BM` is part of the stable key, not a ref-id suffix —
        # only the trailing `***<digits>` is stripped before lookup.
        self.assertEqual(
            extract_merchant("tdbank", "WILLIS NORTHAMEPAYROLL*BM***000120888"),
            "Willis Towers Watson",
        )

    def test_willis_americas(self):
        self.assertEqual(
            extract_merchant("tdbank", "WILLIS AMERICASPAYROLL*BG***000120888"),
            "Willis Towers Watson",
        )

    def test_amex_payment_squashed(self):
        self.assertEqual(
            extract_merchant("tdbank", "AMEXEPAYMENTACHPMT****12345"),
            "American Express",
        )

    def test_amazon_store_card_squashed(self):
        self.assertEqual(
            extract_merchant("tdbank", "AMZ_STORECRD_PMTPAYMENT****78116246568"),
            "Amazon",
        )

    def test_actalent_direct_deposit(self):
        # Bare-digit ref, comma + dot preserved in alias key.
        self.assertEqual(
            extract_merchant("tdbank", "ACTALENT,INC.DIRDEP07844325"),
            "Actalent",
        )

    def test_robinhood_no_suffix(self):
        # No trailing digits at all — the strip is a no-op and the bare
        # token matches directly.
        self.assertEqual(
            extract_merchant("tdbank", "ROBINHOODDEBITS"),
            "Robinhood",
        )

    def test_microsoft_star_separator_not_stripped(self):
        # Sanity: `*` in the middle of a real merchant name (Microsoft uses
        # it as a separator) must not be treated as a ref-id boundary. This
        # row goes through the TD purchase regex, not the alias path, so the
        # test really asserts that the strip didn't break the alias logic
        # for something it shouldn't touch.
        result = extract_merchant("tdbank", "MICROSOFT*XBOX MSBILL.INFO *WA")
        # Not in alias table → falls to TD purchase regex → captures merchant.
        self.assertIsNotNone(result)
        self.assertIn("Microsoft", result)


# ---------- Section E: store-number / reference-id suffix stripping ----------

class TestStoreNumberStripping(unittest.TestCase):
    """POS rows append store ids and reference numbers between the brand and
    the city: a pure number, a `#`-prefixed store number, or a numeric-dominant
    reference code. These must be dropped from the merchant slot so rows for
    the same brand group together (continuation of the #54 extractor work)."""

    def test_wawa_double_store_number(self):
        self.assertEqual(
            extract_merchant("amex", "WAWA 8368 8368 EATONTOWN NJ"),
            "Wawa",
        )

    def test_joeys_pizza_trailing_ref_and_dash(self):
        # Trailing ref number dropped; the leftover dangling hyphen is trimmed.
        self.assertEqual(
            extract_merchant("amex", "JOEYS PIZZA- 000000002 MANAHAWKIN NJ"),
            "Joeys Pizza",
        )

    def test_chickfila_hash_store_and_long_ref(self):
        self.assertEqual(
            extract_merchant("amex", "CHICK-FIL-A #02744 000000000404966 BRICK NJ"),
            "Chick-fil-a",
        )

    def test_dunkin_hash_store_and_number(self):
        self.assertEqual(
            extract_merchant("amex", "DUNKIN #357459 357459 BARNEGAT NJ"),
            "Dunkin",
        )

    def test_spotify_alnum_reference_code(self):
        # `P40905479D` is a numeric-dominant reference code (8 digits) — junk.
        self.assertEqual(
            extract_merchant("amex", "SPOTIFY P40905479D 685603 NEW YORK NY"),
            "Spotify",
        )

    def test_multiword_brand_keeps_name_drops_number(self):
        # Only the trailing number is junk — the multi-word brand survives.
        self.assertEqual(
            extract_merchant("amex", "MAMMA ROSA RESTAURANT PIZ 145000001 BARNEGAT NJ"),
            "Mamma Rosa Restaurant Piz",
        )


# ---------- Section F: CVS + Apple billing aliases ----------

class TestBrandNormalizationAliases(unittest.TestCase):
    """Brands whose raw shape needs normalization beyond suffix stripping."""

    def test_cvs_pharmacy_slash_form(self):
        # Store numbers stripped AND the slash form normalized to "CVS Pharmacy"
        # (vs. the title-caser's "Cvs/pharmacy").
        self.assertEqual(
            extract_merchant("amex", "CVS/PHARMACY #05675 000005675 BARNEGAT NJ"),
            "CVS Pharmacy",
        )

    def test_cvs_bare_store_number(self):
        self.assertEqual(
            extract_merchant("amex", "CVS #1234 TOMS RIVER NJ"),
            "CVS Pharmacy",
        )

    def test_apple_com_bill_with_city(self):
        self.assertEqual(
            extract_merchant("amex", "APPLE.COM/BILL CUPERTINO CA"),
            "apple.com",
        )

    def test_apple_com_bill_with_phone(self):
        # The phone-number "city" would otherwise fail the POS regex; the
        # prefix alias catches the row regardless.
        self.assertEqual(
            extract_merchant("amex", "APPLE.COM/BILL 866-712-7753 CA"),
            "apple.com",
        )


# ---------- Section G: slash-joined token casing ----------

class TestSlashCasing(unittest.TestCase):
    """Tokens glued by '/' are cased on each side and rejoined, so the acronym
    lookup isn't defeated and no lowercased tail is left behind."""

    def test_wawa_fuel_convenience(self):
        # The blessed merchant shape — now with both sides correctly cased.
        self.assertEqual(
            extract_merchant("amex", "WAWA FUEL/CONVENIENCE TOMS RIVER NJ"),
            "Wawa Fuel/Convenience",
        )

    def test_slash_joined_descriptor(self):
        self.assertEqual(
            extract_merchant("amex", "KINGS BAR/GRILL HOBOKEN NJ"),
            "Kings Bar/Grill",
        )


# ---------- Negative cases that should still fall through to LLM ----------

class TestExtractorNegativeCases(unittest.TestCase):
    """Genuinely ambiguous rows should return None so the LLM gets a shot."""

    def test_empty_string(self):
        self.assertIsNone(extract_merchant("tdbank", ""))

    def test_whitespace_only(self):
        self.assertIsNone(extract_merchant("tdbank", "   "))

    def test_none_description(self):
        self.assertIsNone(extract_merchant("tdbank", None))

    def test_unknown_institution_returns_none(self):
        # No institution handler → alias / wrapped patterns are the only
        # match path. A plain unrecognized row falls through.
        self.assertIsNone(extract_merchant("unknown_bank", "RANDOM DESCRIPTION"))

    def test_generic_descriptor_suppressed(self):
        # "ANNUAL FEE" is in _GENERIC_DESCRIPTORS — never a merchant.
        self.assertIsNone(extract_merchant("amex", "ANNUAL FEE 2024"))


if __name__ == "__main__":
    unittest.main()
