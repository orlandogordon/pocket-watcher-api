"""
Pinned regression test for ``src.services.merchant_extractor``.

Fixture rows are derived from the ``llm_inspect/`` corpus — they cover the
shapes the extractor is expected to recognize, the bare-address rows that
must NOT produce a merchant (the K M Tire trap), and the parser-corrupted
brokerage rows that must fall through to None rather than masquerade as a
real brand.

Run:
    python scripts/test_merchant_extractor.py
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.services.merchant_extractor import extract_merchant  # noqa: E402


# (institution, raw_description, expected_merchant_or_None)
CASES: list[tuple[str, str, object]] = [
    # --- TD Bank purchase format (must capture the merchant slot) ---
    ("tdbank", "VISA DDA PUR AP 494300     COSTCO GAS  0739            BRICK         * NJ", "Costco Gas"),
    ("tdbank", "DDA PURCHASE AP 306043     COSTCO WHSE  1025           MANAHAWKIN    * NJ", "Costco Whse"),
    ("tdbank", "VISA DDA REF 494300     COSTCO WHSE  1025           MANAHAWKIN    * NJ", "Costco Whse"),
    ("tdbank", "VISA DDA PUR AP 444500     WALGREENS  6321             TOMS RIVER    * NJ", "Walgreens"),
    ("tdbank", "VISA DDA PUR AP 469216     IHOP  4697                  MANAHAWKIN    * NJ", "IHOP"),
    ("tdbank", "VISA DDA PUR AP 422899     K M TIRE AND AUTO CENTER    TOMS RIVER    * NJ", "K M Tire And Auto Center"),
    ("tdbank", "VISA DDA PUR AP 418310     TOMS RIVER VOLKSWAGEN       TOMS RIVER    * NJ", "Toms River Volkswagen"),
    ("tdbank", "VISA DDA PUR AP 400097     BAGELS AND BEYOND           MANAHAWKIN    * NJ", "Bagels And Beyond"),

    # --- TD Bank withdrawal / ATM rows (NO merchant in source — must reject) ---
    ("tdbank", "DDA WITHDRAW AP TW04C996   1120 TILTON RD              NORTHFIELD    * NJ", None),
    ("tdbank", "ATM CASH DEPOSIT TW04C196   1101 HOOPER AVENUE          TOMS RIVER    * NJ", None),
    ("tdbank", "ATM CASH DEPOSIT TW04B982   591 ROUTE 72                MANAHAWKIN    * NJ", None),

    # --- TD Bank alias-table fixed-string payees ---
    ("tdbank", "HESAA PAYMENT", "HESAA"),
    ("tdbank", "AMEX EPAYMENT ACH PMT", "American Express"),
    ("tdbank", "AMZ_STORECRD_PMT PAYMENT", "Amazon"),
    ("tdbank", "DEPT EDUCATION STUDENT LN", "Dept of Education"),
    ("tdbank", "CRUNCH FIT CLUB FEES", "Crunch Fitness"),
    ("tdbank", "SCHWAB BROKERAGE MONEYLINK", "Schwab"),
    ("tdbank", "PNC BANK NA PAYROLL", "PNC Bank"),
    ("tdbank", "PAYPAL TRANSFER", "PayPal"),
    ("tdbank", "STATE OF N.J. NJSTTAXRFD", "State of NJ"),
    ("tdbank", "IRS  TREAS 310   TAX REF", "IRS"),
    ("tdbank", "NYC FINANCE PARKING TK", "NYC Finance"),

    # --- TD Bank Zelle prefix ---
    ("tdbank", "TD ZELLE SENT 611400H09TUA Zelle MATTHEW MIHM", "Zelle"),
    ("tdbank", "TD ZELLE RECEIVED 608500I08U8A Zelle MAIA GUSCIORA", "Zelle"),

    # --- Schwab clean brokerage rows (should capture merchant) ---
    ("schwab", "ZILLOW GROUP INC CLASS CLASS C", "Zillow Group"),
    ("schwab", "ALPHABET INC CLASS CLASS A", "Alphabet"),
    ("schwab", "TARGET CORP EQUITY CLASS EQUITY", "Target"),

    # --- Schwab parser-corrupted rows (must NOT invent a brand) ---
    ("schwab", "ILLOW GROUPINCCLASS LASSC", None),
    ("schwab", "EDDITINCCLASS A", None),
    ("schwab", "ATADOGINCCLASS A", None),
    ("schwab", "LPHABETINCCLASS A", None),
    ("schwab", "AVAGROUPINC", None),

    # --- Schwab interest / transfer prefix aliases ---
    ("schwab", "SCHWAB1INT08/29-09/26", "Schwab"),
    ("schwab", "SCHWAB1INT09/27-10/29", "Schwab"),
    ("schwab", "TfrTDBankNationalA,MRORLANDOAGOR", "TD Bank"),

    # --- TDA brokerage rows ---
    ("tdameritrade", "ANTARES PHARMA INC ATRS COM", "Antares Pharma"),
    ("tdameritrade", "SNAP INC SNAP COM CL A", "Snap"),
    ("tdameritrade", "TWITTER INC TWTR COM", "Twitter"),
    # Logitech row has no INC/CORP/ETF marker -> regex falls through to LLM (correct)
    ("tdameritrade", "LOGITECH INTERNATIONAL SA LOGI COM", None),

    # --- TDA generic descriptors (must reject) ---
    ("tdameritrade", "ELECTRONIC FUNDING -", None),
    ("tdameritrade", "PURCHASE FDIC INSURED - DEPOSIT ACCOUNT", None),
    ("tdameritrade", "REDEMPTION FDIC INSURED - DEPOSIT ACCOUNT", None),

    # --- Ameriprise rows ---
    ("ameriprise", "MICROSOFT CORP 031226 1.00200", "Microsoft"),
    ("ameriprise", "APPLE INC 021325 1", "Apple"),
    # Tickers like QQQ get titlecased to "Qqq" — extractor doesn't know
    # tickers from words. Acceptable: rows still group consistently.
    ("ameriprise", "INVESCO QQQ ETF 032726 1.00100", "Invesco Qqq"),

    # --- Ameriprise generic descriptors (must reject) ---
    ("ameriprise", "ASSET-BASED BILL VAL 2,698.65 04/01/26 THRU 04/30/26", None),
    ("ameriprise", "AMERIPRISE INSURED MONEY MARKET ACCOUNT 013124", None),

    # --- Amazon Synchrony brand-prefix rows ---
    ("amzn-synchrony", "AMAZON RETAIL SEATTLE WA CDJWmsToYKwY Chef Works Unisex Portland Hal", "Amazon"),
    ("amzn-synchrony", "AMAZON MARKETPLACE SEATTLE WA dGrtlbntNWEt Dixie Ultra, Large Paper Bowls", "Amazon"),
    ("amzn-synchrony", "AMAZON PRIME CONS SEATTLE WA BzDgxKlemOkq", "Amazon Prime"),
    ("amzn-synchrony", "AMAZON DIGITAL SEATTLE WA BFmYypWHiKJT", "Amazon"),
    ("amzn-synchrony", "WHOLEFOODS# 10162 NEW YORK NY mWYABtUTIMPc", "Whole Foods"),
    ("amzn-synchrony", "WHOLEFOODS# 10861 NEW YORK NY BfBJFoVCGZAz", "Whole Foods"),

    # --- Amazon Synchrony generic descriptors (must reject) ---
    ("amzn-synchrony", "ONLINE PYMT-THANK YOU ATLANTA GA", None),
    ("amzn-synchrony", "MOBILE PAYMENT - THANK YOU", None),
    ("amzn-synchrony", "ONLINE PAYMENT - THANK YOU", None),
    ("amzn-synchrony", "CHARGE ON PURCHASES", None),

    # --- AmEx generic descriptors (must reject) ---
    ("amex", "ANNUAL MEMBERSHIP FEE", None),
    ("amex", "MOBILE PAYMENT - THANK YOU", None),

    # --- Empty / whitespace ---
    ("tdbank", "", None),
    ("tdbank", "   ", None),
    ("tdbank", None, None),

    # --- Unknown institution (no per-institution handler) — alias still works ---
    ("unknown_bank", "HESAA PAYMENT", "HESAA"),
    # --- Unknown institution + unmatched shape -> None ---
    ("unknown_bank", "VISA DDA PUR AP 494300 COSTCO GAS 0739 BRICK * NJ", None),
]


FAILED: list[str] = []


def check(label: str, cond: bool, detail: str = "") -> None:
    mark = "PASS" if cond else "FAIL"
    print(f"  [{mark}] {label}" + (f"  ({detail})" if detail else ""))
    if not cond:
        FAILED.append(label)


def main() -> int:
    print(f"Running {len(CASES)} merchant_extractor cases...\n")

    for institution, raw, expected in CASES:
        actual = extract_merchant(institution, raw)
        label = f"{institution!s:<16} | {raw!r}"
        if expected is None:
            check(label + " -> None", actual is None, f"got {actual!r}")
        else:
            check(label + f" -> {expected!r}", actual == expected, f"got {actual!r}")

    print()
    print(f"Total: {len(CASES)}, failed: {len(FAILED)}")
    return 0 if not FAILED else 1


if __name__ == "__main__":
    raise SystemExit(main())
