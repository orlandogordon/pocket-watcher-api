"""
LLM client abstraction.

Backends are selected via the LLM_BACKEND env var. Callers depend only on the
abstract interface; swapping backends (local llama.cpp <-> Anthropic API) is an
env-var change at factory level.

The canonical entry point is ``process_transaction_batch`` — it normalizes the
merchant (when one is present) and suggests a (category, subcategory) UUID
from the locked set in ``src.constants.categories`` in a single round trip.
The raw description is preserved verbatim by callers; the LLM no longer
rewrites it. The merchant column is nullable: rows whose source contains only
an address, generic descriptor, or parser-corrupted token return null and the
caller falls through.

See backend todos #29 (category + merchant) and #35 (raw descriptions +
regex-first merchant extraction).
"""

from __future__ import annotations

import json
import os
from abc import ABC, abstractmethod
from decimal import Decimal
from typing import Optional, TypedDict

from openai import OpenAI
from openai import APIConnectionError, APITimeoutError

from src.constants.categories import (
    all_parent_uuids,
    all_subcategory_uuids,
    render_for_prompt,
    subcategory_to_parent,
)
from src.logging_config import get_logger

logger = get_logger(__name__)

# LLM merchant outputs below this confidence are dropped to None — the model's
# confidence score is poorly calibrated on bare-address / corrupted-token rows
# (which can score >0.9 on confidently-invented brands), but the floor still
# catches the obvious noise tier (Mobile Payment, Annual Membership Fee, etc.).
_MERCHANT_CONFIDENCE_FLOOR = 0.85

# Category outputs below this confidence are dropped to (None, None). Pairs
# with the schema-level nullability of the category UUID fields: ambiguous
# rows surface as null and route through the "Needs Review" tag workflow at
# confirm time (#34) instead of being filed under Miscellaneous at high
# confidence. Tuned to GPT-OSS-20B (#64): a labeled band-precision sweep over
# the consumer (non-investment) eval rows put category precision at ~58% below
# 0.90 vs ~72–88% at/above it, so 0.90 is the precision-first cutoff. NOTE this
# is a model-specific value — Qwen ran at 0.8; revisit if LLM_MODEL changes.
_CATEGORY_CONFIDENCE_FLOOR = 0.90

# Reachability-probe timeout for ``health_check()``. Short on purpose: "offline"
# is a fine answer, and the probe must never contend with an in-flight import.
_HEALTH_TIMEOUT_S = 2.0


def _reasoning_extra_body(model: str) -> dict:
    """Per-model-family request knob that suppresses reasoning for this
    mechanical categorize/extract task (#64).

    The two local model lines disable reasoning differently, and both are
    expressed through llama-server's ``chat_template_kwargs`` pass-through so
    the value reaches the model's own jinja chat template:

    - **GPT-OSS** has a native low/med/high reasoning lever; force ``low`` for
      latency (the harmony template renders this as ``Reasoning: low``). The
      harder #30 PDF-parsing path may later dial this *up* — keep it a knob.
    - **Qwen 3.x** uses ``enable_thinking=False`` to skip its ``<think>`` pass.

    Keyed off the model name (not a separate env var) so the todo's "one-line
    ``.env`` revert" holds: flip ``LLM_MODEL`` back to the Qwen alias and the
    Qwen control returns automatically. llama-server ignores template kwargs a
    given template doesn't define, so a mismatch is harmless — but we key off
    the model anyway to stay explicit.
    """
    if "gpt-oss" in model.lower():
        # Default low for the mechanical batch; LLM_REASONING_EFFORT lets the
        # A/B eval (and the harder #30 PDF-parsing path) dial it up without a
        # code change. Accepts low|medium|high.
        effort = os.getenv("LLM_REASONING_EFFORT", "low").lower()
        return {"chat_template_kwargs": {"reasoning_effort": effort}}
    return {"chat_template_kwargs": {"enable_thinking": False}}


class LLMUnavailableError(Exception):
    """Raised when the LLM backend cannot be reached or fails all retries.

    Callers are expected to catch this and fall through to a graceful
    degradation path (e.g. return raw descriptions unchanged).
    """


class TransactionBatchResult(TypedDict):
    """One row of output from ``process_transaction_batch``.

    ``merchant_name`` is nullable: the model is instructed to return null for
    rows whose source contains no real brand (bare addresses, generic
    descriptors, parser-corrupted tokens). The post-processing layer also
    drops merchant to None when ``confidence`` falls below
    ``_MERCHANT_CONFIDENCE_FLOOR``.

    ``suggested_category_uuid`` and ``suggested_subcategory_uuid`` are
    nullable in the same shape: the model emits null when the row's purpose
    is genuinely ambiguous (P2P transfers, generic ACH with no payee
    context). Post-processing also drops the pair to (None, None) when
    ``confidence`` falls below ``_CATEGORY_CONFIDENCE_FLOOR``. The two UUID
    fields are always either both null or both populated — they never split.
    """
    merchant_name: Optional[str]
    suggested_category_uuid: Optional[str]
    suggested_subcategory_uuid: Optional[str]
    confidence: float


# ---------- prompt fragments ----------

_MERCHANT_RULES = """Merchant name rules (applied to `merchant_name`):
- `merchant_name` is JUST the normalized brand — no metadata, no location, no store number, no identifier. So "STARBUCKS STORE 12345 SEATTLE WA" -> merchant "Starbucks"; "COSTCO WHSE 1025 MANAHAWKIN NJ" -> merchant "Costco"; "APPLE.COM/BILL 866-712-7753" -> merchant "apple.com" (keep the apple.com identifier, drop the phone number); "VENMO 3125551234" -> merchant "Venmo".
- **Return null when there is no real brand in the source.** The merchant column is allowed to be null and SHOULD be null for:
  - Bare-address rows (e.g. "DDA WITHDRAW AP TW04C996 1120 TILTON RD NORTHFIELD * NJ" — there is no merchant, just a street address; return null. Do NOT invent a brand from the address).
  - ATM cash deposits at branch addresses (e.g. "ATM CASH DEPOSIT 1101 HOOPER AVENUE TOMS RIVER * NJ" — return null).
  - Generic transaction-type descriptors that name no payee: "Mobile Payment", "Online Payment", "Annual Membership Fee", "Charge On Purchases", "Asset-Based Bill", "FDIC Insured Deposit Account", "Interest Charge". Return null — these are descriptors, not merchants.
  - Parser-corrupted tokens with no recoverable brand: rows that begin mid-word (e.g. "EDDITINCCLASS A" — corrupted "REDDIT", but you cannot be SURE which company it is). Return null. Do NOT guess a plausible brand. Returning null is correct; inventing "Eddit Inc" is wrong.
  - Single-token AmEx authorization codes with no merchant context (e.g. "MENLO PARK, NJ-ANF 000011626" where no business name appears — return null).
- Aggregator-prefixed descriptions: take the vendor, not the aggregator. "DOORDASH*CHIPOTLE" -> merchant "Chipotle". "GRUBHUB*SHAKE SHACK" -> "Shake Shack". Plain "DOORDASH" (no vendor) -> "DoorDash".
- Legal-entity suffixes (`NA`, `N.A.`, `FSB`, `CU`, `INC`, `LLC`, `CO`, `LTD`, `PLC`) are formal corporate designations, not part of the brand. Treat them as a token boundary — the brand stops before the suffix. So "PNCBANKNAREGSALARY" decomposes as "PNC Bank" (brand) + "NA" (suffix) + "REG SALARY" (deposit type) — merchant is "PNC Bank".
- Strip type qualifiers: "Acme Corp Payroll" -> "Acme Corp", "Uber Trip" -> "Uber", "Uber Eats" -> "Uber Eats" (Uber Eats IS the brand).
- Card networks vs issuers — the stoplist is tiered:
  - VISA, MASTERCARD, MC are pure payment networks. NEVER select one as the merchant. If one of these tokens is the most prominent in the source, the actual merchant follows it (e.g. "VISADDAPURAP HARBORNYC ..." -> merchant is "HARBORNYC", not "Visa"). The token itself is rail noise.
  - AMEX, AMERICAN EXPRESS, DISCOVER, DISC are BOTH networks AND issuers. Treat as the merchant ONLY when the row is a payment/transfer (source contains ELECTRONICPMT, EPAYMENT, ACHPMT, WEBPMT, BILLPAY, or similar). On POS-purchase rows the same token is the network rail and should be stripped — pick whatever merchant token follows it instead.
- Payment processors (PAYPAL *, SQ *, TST*, STRIPE *) wrap the real merchant — emit the wrapped vendor, not the wrapper. "PAYPAL *STEAM GAMES" -> "Steam".
- Unrecognized merchants: if the merchant token is unfamiliar (no well-known brand match) but a brand IS clearly present in the source, output it AS-IS — preserve odd casing like "HARBORNYC". The bar is "is there a brand string in the source?" — if yes, return it; if not, return null."""


_CATEGORY_RULES = """Category rules (applied to `suggested_category_uuid` + `suggested_subcategory_uuid`):

**TOKEN PRIORITY — consult BEFORE picking a category.** When the description contains one of these signals, use the indicated category. Token priority OVERRIDES brand-name pattern-matching and OVERRIDES the temptation to default to Restaurants:
- `FUEL`, `GAS`, `GASOLINE`, or a dedicated gas station (EXXON, SHELL, BP, CHEVRON, MOBIL, SUNOCO, VALERO, MARATHON, CITGO, LUKOIL) → Transportation / Gas. For these fuel-only brands the gas signal beats the restaurant signal (e.g. `SHELL OIL` → Gas, not Restaurants).
- **Convenience-store chains that ALSO sell fuel (WAWA, SHEETZ, QUIKTRIP, ROYAL FARMS, CUMBERLAND FARMS, QUICKCHEK, CIRCLE K, 7-ELEVEN, SPEEDWAY) are genuinely ambiguous — emit null for BOTH category fields.** A single charge could be a fill-up, a coffee, a sandwich, or sundries, and a `FUEL/CONVENIENCE`-style descriptor does NOT disambiguate (it just names the store format, not what was bought). Do NOT file these under Transportation / Gas. Let the user categorize from the preview.
- `AUTO`, `AUTO INSURANCE`, `CAR INSURANCE`, or a known auto insurer (GEICO, PROGRESSIVE, ALLSTATE, USAA, LIBERTY MUTUAL, FARMERS, ESURANCE) → Transportation / Car Insurance — NOT Housing / Insurance.
- Home / renters insurance brands (LEMONADE, NATIONWIDE HOMEOWNERS, TRAVELERS HOME, USAA RENTERS) → Housing / Insurance.
- Health insurance brands (BLUE CROSS, BLUE SHIELD, AETNA, CIGNA, UNITEDHEALTHCARE, KAISER, HUMANA) → Health / Health Insurance.
- `WIRELESS`, `MOBILE`, `BROADBAND`, `CABLE`, `INTERNET`, electric/gas utility names (CONED, NATIONAL GRID, PSEG, PG&E, EDISON, DUKE ENERGY, PEPCO), wireless carriers (AT&T, T-MOBILE, VERIZON WIRELESS, SPECTRUM, COMCAST, XFINITY, FIOS, MINT MOBILE) → Housing / Utilities. **Wireless carriers are utilities, not insurance.**
- `PARKING`, `PARK` (in payment context), `MPAY2PARK`, `MPAY`, `PARKMOBILE`, `LAZ PARKING`, `SPOTHERO`, `METER`, and parking garages (`PARKING GARAGE`, `OFF STREET GARAGE`, `MUNICIPAL GARAGE`, `PUBLIC GARAGE`, `PARK GARAGE`, `TIBA` parking systems) → Transportation / Parking. A parking garage is parking, NOT auto repair.
- `FERRY`, `TRANSIT`, `METROCARD`, `OMNY`, MTA, PATH, NJT, BART, METRO, AMTRAK (commuter context), and contactless transit-fare systems (OMNY, SMARTRIP, CLIPPER, VENTRA, CHARLIE, `*OMNY`, `MTA*`) → Transportation / Public Transit — NOT Shopping. A transit-fare tap is never general merchandise.
- Auto dealerships, auto repair/service shops, and auto-parts stores (`MOTORS`, `AUTO BODY`, `AUTO REPAIR`, `SERVICE CENTER`, `TIRE`, `JIFFY LUBE`, `MIDAS`, `PEP BOYS`, `MEINEKE`, `VALVOLINE`, `MAVIS`, `AUTOZONE`, `ADVANCE AUTO`, `O'REILLY AUTO`, `NAPA AUTO`, or a car marque like VOLKSWAGEN/TOYOTA/HONDA/FORD/SUBARU on a non-payment POS purchase row) → Transportation / Car Maintenance — NOT Shopping. A charge at a dealer/repair shop/parts store is service or parts, not merchandise. (A *payment-shaped* row to a marque's financing arm is the Car Loan case below, not this.) **CAUTION: the bare word "GARAGE" does NOT mean auto repair — a parking garage (`OFF STREET GARAGE`, `PARKING GARAGE`, `MUNICIPAL/PUBLIC GARAGE`, `TIBA`) is Transportation / Parking (see above), never Car Maintenance.** Only treat "garage" as auto repair when paired with an explicit repair/service/auto-body signal.
- Gym + fitness brands (CRUNCH, PLANET FITNESS, EQUINOX, ANYTIME FITNESS, LA FITNESS, BLINK, CLASSPASS), auto club (AAA), professional dues → Subscriptions / Memberships. **`CLUBFEES` / `CLUB FEES` suffix is a membership signal, not an education signal.**
- `AMAZON PRIME`, `AMZN PRIME`, `PRIME MEMBERSHIP` (the Amazon Prime membership) → Subscriptions / Memberships — it is a membership bundle, NOT streaming. The ONLY exception is a standalone video subscription billed as `AMAZON PRIME VIDEO` / `PRIME VIDEO` (no "membership" context) → Subscriptions / Streaming. When in doubt between the two, plain "Amazon Prime" is the membership.
- Warehouse clubs only when the row is the ANNUAL MEMBERSHIP FEE itself (e.g. `COSTCO MEMBERSHIP FEE` → Subscriptions / Memberships). Routine in-store purchases at warehouse clubs (`COSTCO WHSE`, `SAMS CLUB STORE`, `BJ'S WHOLESALE` with no `MEMBERSHIP`/`FEE` token) are Food / Groceries (groceries-typical amount) or Shopping / General (large mixed cart).
- Supermarket / grocery chains (STOP & SHOP, `SUPERSTOPNSHOP`, SHOPRITE, WEGMANS, KROGER, PUBLIX, SAFEWAY, ALBERTSONS, GIANT, ACME, FOOD LION, WHOLE FOODS, TRADER JOE'S, ALDI, LIDL, H-E-B, MEIJER, HARRIS TEETER, VONS, RALPHS, FRESH MARKET, SPROUTS) → Food / Groceries — NOT Shopping / General. A grocery-store charge is groceries even when the specific items are unknown.
- SaaS brands (CANVA, LINKEDIN PREMIUM, `LINKEDIN PRE*`, ADOBE, MICROSOFT 365, GITHUB, NOTION, FIGMA, DROPBOX, CLAUDE.AI, OPENAI, CHATGPT, ANTHROPIC, GOOGLE ONE, ICLOUD STORAGE) and web-hosting / domain registrars (NAMECHEAP, `NAME CHEAP`, GODADDY, BLUEHOST, HOSTGATOR, SQUARESPACE, WIX, CLOUDFLARE, DIGITALOCEAN, VERCEL, NETLIFY, `WWW NAMECHEAP`) → Subscriptions / Software — NOT Shopping. A domain/hosting charge is software, not merchandise.
- Streaming brands (NETFLIX, HULU, SPOTIFY, DISNEY+, HBO MAX, PEACOCK, PARAMOUNT+, APPLE MUSIC, APPLE TV+, YOUTUBE PREMIUM) → Subscriptions / Streaming.
- News / media subscriptions (NYT, WSJ, WASHINGTON POST, ECONOMIST, SUBSTACK) → Subscriptions / News & Media.
- Pharmacy chains (CVS, CVS/PHARMACY, WALGREENS, RITE AID, DUANE READE) → Health / Prescriptions. **NOT Personal Care/Nails or Personal Care/Hair** — the new Personal Care subcategories are services (haircut, manicure, massage), not pharmacy products.
- `APPLE.COM/BILL` is genuinely ambiguous when standalone — it can be iCloud storage (Subscriptions/Software), Apple Music / Apple TV+ (Subscriptions/Streaming), an App Store app purchase, or AppleCare. **Emit null** unless the description carries a disambiguating signal that names an actual Apple PRODUCT (e.g. `APPLE.COM/BILL APPLE MUSIC` → Streaming; `APPLE.COM/BILL ICLOUD` → Software). Generic transaction descriptors are NOT product signals: `INTERNET CHARGE`, `INTERNET PURCHASE`, `RECURRING`, `WEB`, a phone number, or a store/order id do NOT disambiguate — `APPLE.COM/BILL INTERNET CHARGE` is still **null**, NOT Software.
- `INTEREST CHARGE`, `INTEREST CHARGE ON PURCHASES`, `CHARGE ON PURCHASES`, `CHARGES ON PURCHASES`, `FINANCE CHARGE`, `INTEREST ASSESSED` → Debt Payment / Credit Card (consistently — these are interest accrued on a credit-card balance, NOT Miscellaneous / Bank Fee). Treat a bare `CHARGE ON PURCHASES` line as the purchases-interest line item; always file it under Debt Payment / Credit Card, never Bank Fee.
- **Payments TO a credit card or retailer store card → Debt Payment / Credit Card, NOT Shopping.** A retailer/brand name combined with a payment signal on a card account — `PAYMENT`, `AUTOPAY`, `EPAY`, `ONLINE PYMT`, `BILL PAY`, `PAYMENT - THANK YOU` — is the balance being paid down, not a purchase. This holds even when the retailer's own name is in the description: `AMAZON SYNCB PAYMENT`, `AMAZONCORPSYFPAYMNT`, `SYNCHRONY BANK PAYMENT`, `COMENITY PAY <RETAILER>`, `<RETAILER> CARD PAYMENT`, `<STORE> CREDIT CARD AUTOPAY` all → Debt Payment / Credit Card. The brand name does not make it Shopping — a payment reduces a balance; direction/payment-shape overrides the brand. **Synchrony is the tell:** the tokens `SYNCB`, `SYF`, `SYNCHRONY` (Synchrony Financial) or `COMENITY` anywhere in a payment-shaped row mean a store-card payment → Debt Payment / Credit Card, even when fused to the retailer name like `AMAZONCORPSYFPAYMNT` (= Amazon + SYF + Paymnt).
- Fee-shaped descriptions with no merchant context (`MONTHLY MAINTENANCE FEE`, `FOREIGN TRANSACTION FEE`, `OVERDRAFT FEE`, `ATM FEE`, `NSF FEE`, `LATE PAYMENT FEE`, `WIRE TRANSFER FEE`, `STOP PAYMENT FEE`, `RETURNED ITEM FEE`) → Miscellaneous / Bank Fee. These are the bank charging the account itself.
- Zelle / Venmo / Cash App WITH spending context in the description (e.g. `Zelle: JOHN LANDLORD RENT MARCH`, `VENMO COFFEE WITH SUSAN`, `CASH APP TO MIKE GROCERIES`) → use the context: rent context → Housing / Rent, coffee → Food / Coffee Shops, groceries → Food / Groceries. Only return null when the P2P row has a counterparty name but NO spending context.
- `AMEX SEND`, `SEND: ADD MONEY`, `AMEX SEND: ADD MONEY`, `PAYPAL INST XFER`, `VENMO ADD MONEY` and similar P2P funding/transfer rows → **null for both category fields**. These move money into/out of a P2P balance; they are NOT credit-card payments (do not file under Debt Payment) and NOT purchases (do not file under Shopping). Same rule as a context-less Zelle/Venmo: emit null and let the user categorize.

**Restaurants requires a POSITIVE food signal** — a recognized restaurant brand (Chipotle, Shake Shack, Halal Guys, etc.), a food-service token in the merchant name (RESTAURANT, GRILL, DELI, PIZZA, BISTRO, BAKERY, TAVERN, EATERY, KITCHEN, NOODLES, RAMEN, SUSHI, TACO, BURGER), or a delivery aggregator + vendor (DOORDASH*X, GRUBHUB*X, UBER EATS *X). Do NOT default to Restaurants when the description is unfamiliar — check the token-priority list above first; if no priority token matches AND no food signal is present, emit null. The "guess Restaurants" default is wrong.

**Drinking-focused venues → Entertainment / Bars & Clubs, NOT Shopping and NOT Restaurants.** A `BAR`, `PUB`, `LOUNGE`, `NIGHTCLUB`, `CLUB`, `WINERY`, `VINEYARD`, `BREWERY`, `BREWING`, `TAPROOM`, `DISTILLERY`, or `SPEAKEASY` token (or a known such venue) goes to Bars & Clubs. A `TST*` / Toast or `SQ*` / Square POS prefix signals a hospitality merchant (bar or restaurant) — never general retail; pick Bars & Clubs for drinking venues, Food / Restaurants for eateries. A standalone liquor/wine retail STORE (`WINE & SPIRITS`, `LIQUORS`, `BOTTLE SHOP` selling bottles to go) is Shopping / General, not Bars & Clubs — the distinction is drink-on-premises venue vs. retail bottle shop.

- Pick the SUBCATEGORY first — the UUID MUST be one of the subcategory UUIDs listed below.
- Its parent category UUID MUST be the parent it's listed under — never mix (e.g. subcategory "General" ONLY pairs with parent "Shopping"; "Home Goods" ONLY pairs with parent "Shopping"; these are not interchangeable).
- NEVER emit a UUID that isn't in this list. NEVER invent one.
- **Both category fields are nullable. Return null for both when the row's purpose is genuinely ambiguous and you'd be guessing rather than reasoning.** Typical case: peer-to-peer transfers (Zelle, Venmo, Cash App) where the source string contains a counterparty's name but no spending context — the same $40 Venmo could be a restaurant split, concert ticket, rent contribution, or loan repayment. Other examples: generic ACH deposits with no recognizable sender, bare-amount entries with no payee. Return null for BOTH category and subcategory together — they always travel as a pair, never split. The user will categorize from the preview UI; null routes the row to a review queue rather than filing it under a catchall at high confidence.
- Do NOT pick a catchall category just to fill the field. "Shopping / General" is the right call when the row IS shopping but the specific subcategory is unclear (e.g. AMZN MKTP with no item context). It is the WRONG call when the row's category is unknown — emit null instead.
- When no subcategory is a clean fit but the category itself is clear (e.g. "this is shopping, just unclear what kind"), pick "Shopping / General". Reserve null for genuine purpose-ambiguity, not subcategory-ambiguity.
- **"Shopping / General" is for GENERAL-MERCHANDISE RETAIL ONLY — a store that sells physical goods whose specific type is unknown (Amazon with no item context; a department, variety, or discount store; an unrecognized retailer with a goods-shaped name). It is NOT a fallback for "I recognize the merchant but I'm unsure of the category."** This is the single most common misfile: a recognized NON-retail merchant gets dumped in Shopping / General. Before choosing it, ask: *is this merchant actually a retailer of goods?* If it is instead a service, venue, eatery, bar, grocery store, transit fare, car dealer/garage, utility, insurer, pharmacy, gym, or software/subscription, it does NOT belong in Shopping / General — route it to its real category via the token-priority list above, or emit null if genuinely unsure. A bar, a winery, a subway tap, a car dealer, a supermarket, and a domain registrar are NEVER Shopping / General.
- For income-shaped transactions (payroll, direct deposit, dividend, interest received), pick under "Income".
- Income / Investment Income is for DIVIDENDS, INTEREST, and brokerage gains — NOT for marketplace sales. Person-to-person marketplace sales (FACEBOOK MARKETPLACE SALE, CRAIGSLIST SALE, ETSY PAYOUT, EBAY SALE), garage sale proceeds, and one-off paid work (`PAYMENT FROM <person> FOR <work>`) → Income / Other Income.
- **Donations and crowdfunding contributions you MAKE go to Miscellaneous / Gifts & Charity, NOT Income.** A `DONATION` token, a fundraising/crowdfunding platform (GOFUNDME, KICKSTARTER, INDIEGOGO, PATREON, DONORBOX), a charity (RED CROSS, UNICEF, ST JUDE, WIKIMEDIA, NPR/PBS pledge, church/temple offering), or a `… FOR <name>` campaign on an OUTGOING row (PURCHASE/DEBIT, money leaving the account) is a gift made — file it under Gifts & Charity. Direction is the discriminator: do NOT route it to Income just because the platform (e.g. GoFundMe) is associated with receiving money — the user here is the giver. This is the outgoing mirror of the marketplace-sales rule above. Gifts purchased for other people go here too.
- Income / Taxes is BIDIRECTIONAL: tax refunds (IRS TREAS, STATE TAX REFUND) AND tax payments (IRS PAYMENT, ESTIMATED TAX, PROPERTY TAX) both go here.
- Mortgage payments go to Housing / Mortgage — NOT Debt Payment. Debt Payment is for credit cards, student loans, and car loans only.
- Student loan servicers (HESAA, `NJCLASS`, Nelnet, MOHELA, Sallie Mae, Navient, Great Lakes, EdFinancial, Dept of Education / DEPTEDUCATION) on payment-shaped rows go to Debt Payment / Student Loan. The merchant token may be concatenated with PAYMENT (e.g. HESAAPAYMENT, NELNETPAYMENT) — preserve all letters of the servicer name; do not drop trailing letters when the token splits. An education-loan-payment token — `ACHLNPYMT`, `ACH LN PYMT`, `LOAN PYMT`, `STUDENT LN`, `EDUCATIONALCOMP…ACHLNPYMT` — is a Debt Payment / Student Loan, **NOT Miscellaneous / Education** (Education is for tuition/courses/school fees, not loan repayment). An outgoing `NJCLASS`/`...CLASSLN` debit is a student-loan payment, NOT Income / Taxes — direction (money leaving) plus the loan token both point to Debt Payment / Student Loan. **`STATEOFNJ` (or any "State of <X>") does NOT make it a tax: `NJCLASS` is New Jersey's state student-loan program, so `STATEOFNJNJCLASSLN` = State-of-NJ NJCLASS Loan → Debt Payment / Student Loan, NOT Income / Taxes.** The `NJCLASS`/`CLASSLN` loan token overrides the `STATEOFNJ` government-entity signal. Only route to Income / Taxes when the row actually names a tax (IRS, tax refund/payment, property tax) with no loan token.
- Auto lenders (Ally, Capital One Auto, Toyota Financial Services, Honda Financial, Ford Credit) on payment-shaped rows go to Debt Payment / Car Loan.
- Home Depot, Lowe's, and similar home-improvement stores go to Housing / Home Repair — NOT Shopping / Home Goods.
- Cosmetics and beauty stores (Sephora, Ulta, MAC) go to Shopping / Toiletries.
- Apparel + department + off-price stores (NORDSTROM, NORDSTROM RACK, MACY'S, BLOOMINGDALE'S, SAKS, SAKS OFF 5TH, T.J. MAXX, MARSHALLS, ROSS, BURLINGTON, OLD NAVY, GAP, BANANA REPUBLIC, J.CREW, MADEWELL, ABERCROMBIE, HOLLISTER, AMERICAN EAGLE, AERIE, ANTHROPOLOGIE, URBAN OUTFITTERS, FREE PEOPLE, H&M, ZARA, UNIQLO, FOREVER 21, ASOS, LULULEMON, ATHLETA, NIKE, ADIDAS, FOOT LOCKER, DSW, FAMOUS FOOTWEAR, JCPENNEY, KOHL'S, DILLARD'S) → Shopping / Clothing. The store name alone is sufficient signal — "RACK" / "OFF 5TH" / discount-store suffixes don't change the category.
- Generic Amazon purchases (AMZN MKTP, AMAZON.COM with no further context) go to Shopping / General — the item is unknown, so don't commit to a specific Shopping subcat.
- For coffee shops specifically (Starbucks, Blue Bottle, etc.), use Food / Coffee Shops — not Restaurants.
- For streaming services (Netflix, Spotify, Hulu, Disney+, HBO Max, Apple TV+, YouTube Premium, Apple Music), use Subscriptions / Streaming — NOT Entertainment.
- Video games and gaming platforms (Steam, PlayStation, Xbox) go to Entertainment / Hobbies — NOT Subscriptions / Streaming."""


_FEW_SHOT_EXAMPLES = """Examples (raw input -> output JSON):

Input: {"description": "PURCHASE AUTHORIZED ON 03/14 STARBUCKS STORE 12345 SEATTLE WA CARD 1234", "amount": "4.75", "transaction_type": "PURCHASE"}
Output: {"merchant_name": "Starbucks", "suggested_category_uuid": "9bf074af-479f-4d55-853c-e807a4bbbe9e", "suggested_subcategory_uuid": "88accd63-6963-417a-b334-970d28a91cf5", "confidence": 0.98}

Input: {"description": "DOORDASH*CHIPOTLE 855-9731040 CA", "amount": "23.40", "transaction_type": "PURCHASE"}
Output: {"merchant_name": "Chipotle", "suggested_category_uuid": "9bf074af-479f-4d55-853c-e807a4bbbe9e", "suggested_subcategory_uuid": "dd2d9c68-4c00-444e-80ed-775a72087bea", "confidence": 0.95}

Input: {"description": "UBER EATS *SWEETGREEN HELP.UBER.COM", "amount": "18.40", "transaction_type": "PURCHASE"}
Output: {"merchant_name": "Sweetgreen", "suggested_category_uuid": "9bf074af-479f-4d55-853c-e807a4bbbe9e", "suggested_subcategory_uuid": "dd2d9c68-4c00-444e-80ed-775a72087bea", "confidence": 0.94}

Input: {"description": "DIRECT DEPOSIT ACME CORP PAYROLL", "amount": "3250.00", "transaction_type": "CREDIT"}
Output: {"merchant_name": "Acme Corp", "suggested_category_uuid": "17ac387d-1817-48d5-85c6-84bd2af576e9", "suggested_subcategory_uuid": "42e344f9-55f1-4f46-9c12-d548658409fb", "confidence": 0.99}

Input: {"description": "NETFLIX.COM LOS GATOS CA", "amount": "15.49", "transaction_type": "PURCHASE"}
Output: {"merchant_name": "Netflix", "suggested_category_uuid": "978bf5d7-68a7-49ce-9f6e-f05ff01f4e07", "suggested_subcategory_uuid": "d6762e10-a608-417a-a7a6-87a2977e59e1", "confidence": 0.99}

Input: {"description": "PAYPAL *STEAM GAMES 4029357733", "amount": "29.99", "transaction_type": "PURCHASE"}
Output: {"merchant_name": "Steam", "suggested_category_uuid": "78bd0a07-5447-4cb6-b2d6-315d3d4cb4a0", "suggested_subcategory_uuid": "1831cdfa-bc8a-45e7-a552-404ee54b3464", "confidence": 0.95}

Input: {"description": "ELECTRONICPMT-WEB, AMEXEPAYMENTACHPMTM7284", "amount": "3000.00", "transaction_type": "PURCHASE"}
Output: {"merchant_name": "American Express", "suggested_category_uuid": "54812989-bc35-4acb-aa11-a93aaa7b6b65", "suggested_subcategory_uuid": "b9328f2f-88f5-4128-90af-87130c967280", "confidence": 0.95}

Input: {"description": "MASTERCARD PURCHASE FERN COFFEE BAR PORTLAND OR", "amount": "6.25", "transaction_type": "PURCHASE"}
Output: {"merchant_name": "Fern Coffee Bar", "suggested_category_uuid": "9bf074af-479f-4d55-853c-e807a4bbbe9e", "suggested_subcategory_uuid": "88accd63-6963-417a-b334-970d28a91cf5", "confidence": 0.85}

Input: {"description": "ACHDEPOSIT,PNCBANKNAREGSALARY****40047586", "amount": "2523.89", "transaction_type": "DEPOSIT"}
Output: {"merchant_name": "PNC Bank", "suggested_category_uuid": "17ac387d-1817-48d5-85c6-84bd2af576e9", "suggested_subcategory_uuid": "42e344f9-55f1-4f46-9c12-d548658409fb", "confidence": 0.92}

Input: {"description": "ACHDEBIT,HESAAPAYMENTP18514286", "amount": "200.14", "transaction_type": "PURCHASE"}
Output: {"merchant_name": "HESAA", "suggested_category_uuid": "54812989-bc35-4acb-aa11-a93aaa7b6b65", "suggested_subcategory_uuid": "3280dd39-0173-4754-bdba-17b1a3981e1e", "confidence": 0.92}

Input: {"description": "WAWA FUEL/CONVENIENCE TOMS RIVER NJ", "amount": "42.18", "transaction_type": "PURCHASE"}
Output: {"merchant_name": "Wawa", "suggested_category_uuid": null, "suggested_subcategory_uuid": null, "confidence": 0.45}

Input: {"description": "ACHDEBIT,CRUNCHFITCLUBFEES****300238869", "amount": "39.99", "transaction_type": "PURCHASE"}
Output: {"merchant_name": "Crunch Fitness", "suggested_category_uuid": "978bf5d7-68a7-49ce-9f6e-f05ff01f4e07", "suggested_subcategory_uuid": "2eaf0bb4-12ef-4049-a905-bcdb9de0142b", "confidence": 0.92}

Input: {"description": "GEICO AUTO (800)841-3000 DC", "amount": "142.50", "transaction_type": "PURCHASE"}
Output: {"merchant_name": "GEICO", "suggested_category_uuid": "d0032366-ed8b-484b-9564-7f5e9721aa7e", "suggested_subcategory_uuid": "2a4476f0-7541-47d1-89b6-d3868c0c6a55", "confidence": 0.95}

Input: {"description": "MPAY2PARK 650000010961582 WALLINGFORD CT", "amount": "3.50", "transaction_type": "PURCHASE"}
Output: {"merchant_name": "MPay2Park", "suggested_category_uuid": "d0032366-ed8b-484b-9564-7f5e9721aa7e", "suggested_subcategory_uuid": "e2c18ac3-6d9e-4e34-a4d4-59f3ccb4116d", "confidence": 0.92}

Input: {"description": "LINKEDIN PRE*1234567 LINKEDIN.COM", "amount": "29.99", "transaction_type": "PURCHASE"}
Output: {"merchant_name": "LinkedIn", "suggested_category_uuid": "978bf5d7-68a7-49ce-9f6e-f05ff01f4e07", "suggested_subcategory_uuid": "d8a8d1c4-1ce3-4316-afc2-84e516652845", "confidence": 0.92}

Input: {"description": "VERIZON WIRELESS AUTOPAY", "amount": "85.00", "transaction_type": "PURCHASE"}
Output: {"merchant_name": "Verizon Wireless", "suggested_category_uuid": "f8ee90f0-2d76-4547-b9b4-71fbb2c506d6", "suggested_subcategory_uuid": "8b4be050-62fa-4520-b5af-012e0eb048f5", "confidence": 0.95}

Input: {"description": "APPLE.COM/BILL 866-712-7753 CA", "amount": "2.99", "transaction_type": "PURCHASE"}
Output: {"merchant_name": "apple.com", "suggested_category_uuid": null, "suggested_subcategory_uuid": null, "confidence": 0.4}

Input: {"description": "DDA WITHDRAW AP TW04C996  1120 TILTON RD  NORTHFIELD  * NJ", "amount": "200.00", "transaction_type": "PURCHASE"}
Output: {"merchant_name": null, "suggested_category_uuid": "0284c65f-1af6-48d2-9133-3d3ac3393ede", "suggested_subcategory_uuid": "d7a3041e-5253-492c-82ca-ca24fb25df26", "confidence": 0.7}

Input: {"description": "ATM CASH DEPOSIT TW04C196  1101 HOOPER AVENUE  TOMS RIVER  * NJ", "amount": "300.00", "transaction_type": "DEPOSIT"}
Output: {"merchant_name": null, "suggested_category_uuid": "17ac387d-1817-48d5-85c6-84bd2af576e9", "suggested_subcategory_uuid": "42e344f9-55f1-4f46-9c12-d548658409fb", "confidence": 0.7}

Input: {"description": "ANNUAL MEMBERSHIP FEE", "amount": "95.00", "transaction_type": "PURCHASE"}
Output: {"merchant_name": null, "suggested_category_uuid": "54812989-bc35-4acb-aa11-a93aaa7b6b65", "suggested_subcategory_uuid": "b9328f2f-88f5-4128-90af-87130c967280", "confidence": 0.85}

Input: {"description": "MOBILE PAYMENT - THANK YOU", "amount": "1500.00", "transaction_type": "TRANSFER_IN"}
Output: {"merchant_name": null, "suggested_category_uuid": "134bbe34-09df-4462-9d50-5dab2b03c089", "suggested_subcategory_uuid": "5247aeec-a479-4801-9f5e-07af3122f6f9", "confidence": 0.65}

Input: {"description": "EDDITINCCLASS A", "amount": "245.00", "transaction_type": "PURCHASE"}
Output: {"merchant_name": null, "suggested_category_uuid": null, "suggested_subcategory_uuid": null, "confidence": 0.3}

Input: {"description": "DIVIDEND VOO", "amount": "45.20", "transaction_type": "DIVIDEND"}
Output: {"merchant_name": "Vanguard", "suggested_category_uuid": "17ac387d-1817-48d5-85c6-84bd2af576e9", "suggested_subcategory_uuid": "fe41dac0-0a3b-4e33-a731-9aecc6217d42", "confidence": 0.95}

Input: {"description": "Zelle: MATTHEWMIHM", "amount": "40.00", "transaction_type": "PURCHASE"}
Output: {"merchant_name": "Zelle", "suggested_category_uuid": null, "suggested_subcategory_uuid": null, "confidence": 0.5}

Input: {"description": "VENMO PAYMENT 3125551234 SUSAN PARK", "amount": "65.00", "transaction_type": "PURCHASE"}
Output: {"merchant_name": "Venmo", "suggested_category_uuid": null, "suggested_subcategory_uuid": null, "confidence": 0.5}

Input: {"description": "RED CROSS DONATION 800-RED-CROSS", "amount": "75.00", "transaction_type": "PURCHASE"}
Output: {"merchant_name": "Red Cross", "suggested_category_uuid": "0284c65f-1af6-48d2-9133-3d3ac3393ede", "suggested_subcategory_uuid": "63e4c43b-a02e-4ac4-b820-46425a20d954", "confidence": 0.9}

Input: {"description": "ACH CREDIT XXXXX1234 ", "amount": "320.00", "transaction_type": "DEPOSIT"}
Output: {"merchant_name": null, "suggested_category_uuid": null, "suggested_subcategory_uuid": null, "confidence": 0.4}

Input: {"description": "DEBITCARDPURCHASE,*****12345678,AUT031524VISADDAPUR OMNY.INFO 8777896669 NEW YORK NY", "amount": "2.90", "transaction_type": "PURCHASE"}
Output: {"merchant_name": "OMNY", "suggested_category_uuid": "d0032366-ed8b-484b-9564-7f5e9721aa7e", "suggested_subcategory_uuid": "d07371fc-fbf5-4388-86de-a6b43c6be316", "confidence": 0.9}

Input: {"description": "DEBITPOS,*****12345678,AUT040124DDAPURCHASE SHOPRITE OF BRICK 0123 BRICK NJ", "amount": "84.31", "transaction_type": "PURCHASE"}
Output: {"merchant_name": "ShopRite", "suggested_category_uuid": "9bf074af-479f-4d55-853c-e807a4bbbe9e", "suggested_subcategory_uuid": "0b66599a-0919-46cb-8d86-ea0517a66f12", "confidence": 0.92}

Input: {"description": "VISA DDA PUR AP 555000     LARSON TOYOTA SERVICE   PUYALLUP      * WA", "amount": "412.60", "transaction_type": "PURCHASE"}
Output: {"merchant_name": "Larson Toyota", "suggested_category_uuid": "d0032366-ed8b-484b-9564-7f5e9721aa7e", "suggested_subcategory_uuid": "85c97dee-fea6-4c15-b594-285cc9daf747", "confidence": 0.88}

Input: {"description": "SQ *THE LOCAL TAPROOM ASBURY PARK NJ", "amount": "31.00", "transaction_type": "PURCHASE"}
Output: {"merchant_name": "The Local Taproom", "suggested_category_uuid": "78bd0a07-5447-4cb6-b2d6-315d3d4cb4a0", "suggested_subcategory_uuid": "65d418af-2b29-44a3-af3c-2093655e14c2", "confidence": 0.88}

Input: {"description": "VISA DDA PUR AP 400099     SQSP* INV12345     SQUARESPACE.COM  * NY", "amount": "23.00", "transaction_type": "PURCHASE"}
Output: {"merchant_name": "Squarespace", "suggested_category_uuid": "978bf5d7-68a7-49ce-9f6e-f05ff01f4e07", "suggested_subcategory_uuid": "d8a8d1c4-1ce3-4316-afc2-84e516652845", "confidence": 0.9}

Input: {"description": "ACHDEBIT,STATEOFNJNJCLASSLN****41203", "amount": "312.00", "transaction_type": "PURCHASE"}
Output: {"merchant_name": "NJCLASS", "suggested_category_uuid": "54812989-bc35-4acb-aa11-a93aaa7b6b65", "suggested_subcategory_uuid": "3280dd39-0173-4754-bdba-17b1a3981e1e", "confidence": 0.92}"""


def _build_system_prompt() -> str:
    return "\n\n".join([
        (
            "You identify the merchant brand and classify each transaction "
            "into a predefined category/subcategory. The raw description is "
            "preserved verbatim by the caller — do NOT rewrite or clean it. "
            "merchant_name is nullable: return null when the source contains "
            "no real brand. The category and subcategory UUIDs are also "
            "nullable: return null for both (always together) when the row's "
            "purpose is genuinely ambiguous and you'd be guessing. Output is "
            "machine-consumed — follow the schema exactly."
        ),
        _MERCHANT_RULES,
        _CATEGORY_RULES,
        render_for_prompt(),
        _FEW_SHOT_EXAMPLES,
    ])


# Built once at import time — the category list + few-shots are static, so the
# system prompt is constant across every call. llama-server's prompt cache
# benefits from the exact-prefix match.
_SYSTEM_PROMPT = _build_system_prompt()

# Subcategory UUID -> parent UUID. Used to post-correct the model's category
# choice: the JSON schema constrains each UUID field to its own enum but
# doesn't enforce parent-child consistency, so the model can (and does) ship
# invalid pairs like "Shopping + General Merchandise" where General Merchandise
# actually lives under Miscellaneous. We trust the (harder) subcategory pick
# and derive the parent from it.
_SUB_TO_PARENT = subcategory_to_parent()


def _build_batch_json_schema(count: int) -> dict:
    """Response must be {"results": [TransactionBatchResult, ...]} of exactly `count` items.

    The UUID fields are constrained to the predefined enum so the model cannot
    hallucinate an ID that doesn't resolve to a real CategoryDB row. Category
    UUIDs are nullable via ``anyOf: [enum-string, null]`` rather than
    ``type: ["string", "null"]`` because the latter would cause the enum to
    be evaluated against null on null-valued rows. The merchant field has no
    enum constraint, so the simpler ``type: ["string", "null"]`` works there.
    """
    return {
        "name": "transaction_batch",
        "strict": True,
        "schema": {
            "type": "object",
            "properties": {
                "results": {
                    "type": "array",
                    "minItems": count,
                    "maxItems": count,
                    "items": {
                        "type": "object",
                        "properties": {
                            # Nullable: model emits null when no real brand
                            # exists in the source (bare addresses, generic
                            # descriptors, parser-corrupted tokens).
                            "merchant_name": {"type": ["string", "null"]},
                            # Nullable enum: emit null for genuinely ambiguous
                            # rows (P2P transfers with no spending context).
                            # See _CATEGORY_RULES for null guidance.
                            "suggested_category_uuid": {
                                "anyOf": [
                                    {"type": "string", "enum": all_parent_uuids()},
                                    {"type": "null"},
                                ],
                            },
                            "suggested_subcategory_uuid": {
                                "anyOf": [
                                    {"type": "string", "enum": all_subcategory_uuids()},
                                    {"type": "null"},
                                ],
                            },
                            "confidence": {
                                "type": "number",
                                "minimum": 0.0,
                                "maximum": 1.0,
                            },
                        },
                        "required": [
                            "merchant_name",
                            "suggested_category_uuid",
                            "suggested_subcategory_uuid",
                            "confidence",
                        ],
                        "additionalProperties": False,
                    },
                },
            },
            "required": ["results"],
            "additionalProperties": False,
        },
    }


def _render_parsed_for_prompt(parsed: "list") -> str:
    """Serialize a batch of ParsedTransaction-shaped inputs for the user prompt.

    Accepts either ``ParsedTransaction`` objects (with .description, .amount,
    .transaction_type, .transaction_date attrs) or plain dicts with the same
    keys, so callers can pass whatever shape they already have.
    """
    def _one(p) -> dict:
        if isinstance(p, dict):
            desc = p.get("description", "")
            amount = p.get("amount", "")
            ttype = p.get("transaction_type", "")
            tdate = p.get("transaction_date", "")
        else:
            desc = getattr(p, "description", "")
            amount = getattr(p, "amount", "")
            ttype = getattr(p, "transaction_type", "")
            tdate = getattr(p, "transaction_date", "")
        if isinstance(amount, Decimal):
            amount = str(amount)
        if not isinstance(amount, str):
            amount = str(amount)
        return {
            "description": desc or "",
            "amount": amount,
            "transaction_type": str(ttype) if ttype else "",
            "transaction_date": str(tdate) if tdate else "",
        }

    lines = [
        f"{i + 1}. {json.dumps(_one(p), ensure_ascii=False)}"
        for i, p in enumerate(parsed)
    ]
    return (
        "Classify each transaction below. Return JSON matching the schema: "
        "an object with key 'results' whose value is an array of exactly "
        f"{len(parsed)} objects in the same order.\n\n"
        + "\n".join(lines)
    )


class LLMClient(ABC):
    """Uniform interface for all LLM backends."""

    @property
    @abstractmethod
    def model_name(self) -> str:
        """Identifier written to parsed_imports.llm_model (e.g. 'qwen3.5-9b-q4')."""

    @abstractmethod
    def process_transaction_batch(self, parsed: list) -> list[TransactionBatchResult]:
        """Classify a batch of parsed transactions: merchant (nullable) +
        category UUIDs (nullable, always paired) + confidence. Returns exactly
        ``len(parsed)`` results in the same order. Raises ``LLMUnavailableError``
        on any unrecoverable failure (connection, timeout, malformed JSON,
        count mismatch)."""

    @abstractmethod
    def health_check(self) -> tuple[bool, Optional[str]]:
        """Cheap reachability probe. Returns ``(online, model_id)``. Never
        raises — an unreachable backend is a normal ``(False, None)`` answer,
        not an error. Must not run a real completion."""


class LlamaCppClient(LLMClient):
    """OpenAI-compatible client targeting a local llama-server instance."""

    def __init__(
        self,
        endpoint: str,
        model: str,
        timeout_s: float = 120.0,
        max_retries: int = 1,
        extra_body: Optional[dict] = None,
    ):
        self._endpoint = endpoint
        self._model = model
        self._timeout_s = timeout_s
        # Per-model reasoning-suppression knob sent on every completion. Defaults
        # to the family inferred from the model name (#64); injectable for tests.
        self._extra_body = (
            extra_body if extra_body is not None else _reasoning_extra_body(model)
        )
        self._client = OpenAI(
            base_url=endpoint,
            api_key="not-needed",  # llama-server ignores this
            timeout=timeout_s,
            max_retries=max_retries,
        )

    @property
    def model_name(self) -> str:
        return self._model

    def process_transaction_batch(self, parsed: list) -> list[TransactionBatchResult]:
        if not parsed:
            return []

        try:
            response = self._client.chat.completions.create(
                model=self._model,
                messages=[
                    {"role": "system", "content": _SYSTEM_PROMPT},
                    {"role": "user", "content": _render_parsed_for_prompt(parsed)},
                ],
                response_format={
                    "type": "json_schema",
                    "json_schema": _build_batch_json_schema(len(parsed)),
                },
                temperature=0.0,
                # Reasoning mode blows past the latency budget and adds nothing
                # for this mechanical task. The exact kwarg is model-family
                # specific (Qwen enable_thinking vs GPT-OSS reasoning_effort) and
                # resolved at construction — see _reasoning_extra_body (#64).
                extra_body=self._extra_body,
            )
        except (APIConnectionError, APITimeoutError) as e:
            logger.warning(f"LLM backend unreachable ({type(e).__name__}): {e}")
            raise LLMUnavailableError(str(e)) from e
        except Exception as e:
            logger.error(f"LLM call failed: {e}", exc_info=True)
            raise LLMUnavailableError(str(e)) from e

        content = response.choices[0].message.content or ""
        try:
            payload = json.loads(content)
            results = payload["results"]
        except (json.JSONDecodeError, KeyError, TypeError) as e:
            logger.error(f"LLM returned malformed JSON: {content!r}")
            raise LLMUnavailableError(f"Malformed JSON: {e}") from e

        if len(results) != len(parsed):
            logger.error(
                f"LLM returned {len(results)} items for {len(parsed)} inputs; discarding"
            )
            raise LLMUnavailableError("Item count mismatch")

        out: list[TransactionBatchResult] = []
        for r in results:
            try:
                confidence = float(r.get("confidence", 0.0))

                # Category nullability: the model can emit null for either or
                # both UUID fields. Treat them as a pair — if either is null,
                # both go to None. Apply the confidence floor too: low-confidence
                # category guesses route through the Needs Review tag instead
                # of being filed at face value (#34).
                raw_sub = r["suggested_subcategory_uuid"]
                raw_cat = r["suggested_category_uuid"]
                if raw_sub is None or raw_cat is None or confidence < _CATEGORY_CONFIDENCE_FLOOR:
                    sub_uuid: Optional[str] = None
                    cat_uuid: Optional[str] = None
                else:
                    sub_uuid = str(raw_sub)
                    # Trust the subcategory, derive the parent — see _SUB_TO_PARENT comment.
                    cat_uuid = _SUB_TO_PARENT.get(sub_uuid, str(raw_cat))

                # Merchant nullability: schema allows null, and we additionally
                # drop merchant when confidence is below the floor (catches the
                # noise tier — Mobile/Annual Fee/Asset-Based — even when the
                # model emits a string for those rows).
                raw_merchant = r["merchant_name"]
                if raw_merchant is None:
                    merchant: Optional[str] = None
                else:
                    stripped = str(raw_merchant).strip()
                    if not stripped or confidence < _MERCHANT_CONFIDENCE_FLOOR:
                        merchant = None
                    else:
                        merchant = stripped

                out.append({
                    "merchant_name": merchant,
                    "suggested_category_uuid": cat_uuid,
                    "suggested_subcategory_uuid": sub_uuid,
                    "confidence": confidence,
                })
            except (KeyError, TypeError, ValueError) as e:
                logger.error(f"LLM result missing expected fields: {r!r}")
                raise LLMUnavailableError(f"Malformed result row: {e}") from e

        return out

    def health_check(self) -> tuple[bool, Optional[str]]:
        # Hit GET /v1/models with a short timeout — cheap, no completion. Any
        # failure (refused, timeout, bad response) means offline.
        try:
            page = self._client.with_options(timeout=_HEALTH_TIMEOUT_S).models.list()
            data = getattr(page, "data", None) or []
            model_id = data[0].id if data else self._model
            return True, model_id
        except Exception as e:
            logger.info("LLM health probe offline (%s: %s)", type(e).__name__, e)
            return False, None


class AnthropicClient(LLMClient):
    """Stub for production Anthropic backend — #30 implements this for real.

    Will use the native ``anthropic`` SDK (not the OpenAI-compat layer —
    Anthropic's OpenAI-compat layer ignores ``response_format``). Structured
    output is enforced via tool use: define a tool whose input_schema matches
    ``TransactionBatchResult`` with the category UUID enums, force
    ``tool_choice={"type": "tool", "name": ...}``, read ``response.content[0].input``.
    """

    def __init__(self, model: str, api_key: Optional[str]):
        self._model = model
        self._api_key = api_key

    @property
    def model_name(self) -> str:
        return self._model

    def process_transaction_batch(self, parsed: list) -> list[TransactionBatchResult]:
        raise NotImplementedError(
            "AnthropicClient is not implemented yet. Set LLM_BACKEND=llama_cpp."
        )

    def health_check(self) -> tuple[bool, Optional[str]]:
        # Not implemented — the self-hosted deployment runs llama_cpp only.
        return False, None


_client_singleton: Optional[LLMClient] = None


def get_llm_client() -> LLMClient:
    """Factory. Returns a cached client based on LLM_BACKEND env var.

    Env vars:
        LLM_BACKEND      'llama_cpp' (default) | 'anthropic'
        LLM_ENDPOINT     default 'http://localhost:8080/v1' (llama.cpp only)
        LLM_MODEL        model identifier; defaults depend on backend
        LLM_API_KEY      Anthropic API key (unused for llama.cpp)
        LLM_TIMEOUT_S    per-call timeout in seconds (default 120.0). Generous
                         on purpose — a 20-row batch on a local 9B can take
                         ~25s, and a too-tight timeout drops the whole batch's
                         category suggestions via LLMUnavailableError.
    """
    global _client_singleton
    if _client_singleton is not None:
        return _client_singleton

    backend = os.getenv("LLM_BACKEND", "llama_cpp").lower()
    timeout_s = float(os.getenv("LLM_TIMEOUT_S", "120.0"))

    if backend == "llama_cpp":
        _client_singleton = LlamaCppClient(
            endpoint=os.getenv("LLM_ENDPOINT", "http://localhost:8080/v1"),
            model=os.getenv("LLM_MODEL", "qwen3.5-9b-q4"),
            timeout_s=timeout_s,
        )
    elif backend == "anthropic":
        _client_singleton = AnthropicClient(
            model=os.getenv("LLM_MODEL", "claude-haiku-4-5-20251001"),
            api_key=os.getenv("LLM_API_KEY"),
        )
    else:
        raise ValueError(f"Unknown LLM_BACKEND: {backend!r}")

    return _client_singleton


def reset_llm_client() -> None:
    """Clear the cached client — for tests and env-var changes."""
    global _client_singleton
    _client_singleton = None
