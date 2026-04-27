"""
LLM client abstraction.

Backends are selected via the LLM_BACKEND env var. Callers depend only on the
abstract interface; swapping backends (local llama.cpp <-> Anthropic API) is an
env-var change at factory level.

The canonical entry point is ``process_transaction_batch`` — it cleans the raw
description, normalizes the merchant, and suggests a (category, subcategory)
UUID from the locked set in ``src.constants.categories`` in a single round
trip. ``clean_descriptions_batch`` is retained as a thin projection over it for
#27's existing call sites.

See backend todos #27 (description cleanup) and #29 (category + merchant).
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


class LLMUnavailableError(Exception):
    """Raised when the LLM backend cannot be reached or fails all retries.

    Callers are expected to catch this and fall through to a graceful
    degradation path (e.g. return raw descriptions unchanged).
    """


class TransactionBatchResult(TypedDict):
    """One row of output from ``process_transaction_batch``."""
    cleaned_description: str
    merchant_name: str
    suggested_category_uuid: str
    suggested_subcategory_uuid: str
    confidence: float


# ---------- prompt fragments ----------

_DESCRIPTION_CLEANUP_RULES = """Description cleanup rules (applied to `cleaned_description`):
- Goal: human-readable. Lead with the merchant in the FIRST tokens, then preserve metadata that distinguishes one transaction from another (store/branch number, city/state for physical purchases, row-specific identifier). The minimum possible string is NOT the goal — readability is. Don't over-strip.
- Format conventions (use a single " - " separator — space-dash-space — consistently):
  - Physical retail: `{Merchant}[ #{StoreNum}] - {City}, {ST}` (e.g. "Trader Joe's #543 - Brooklyn, NY", "Shell - Burbank, CA"). Drop the store-number segment when not present; drop the city when only a state is present (e.g. "HARBORNYC - NY").
  - Aggregator + vendor: `{Aggregator} - {Vendor}` with location appended when present in the source (e.g. "TST* JOE'S PIZZA - NEW YORK NY" -> "Joe's Pizza - New York, NY", processor stripped, vendor preserved with location).
  - Payment / transfer rows: `{Issuer} Payment {trailing-id}` (e.g. "Amex Payment 7284", "Discover Payment 4421"). Trailing identifier preserved verbatim regardless of length.
  - Deposits and bank-action rows with no merchant: `{Action}` plus any address/identifier the source carries (e.g. "ATM Withdrawal - 0123 Main St", "Acme Corp Payroll"). "Interest Charge" alone if no further detail.
  - Inbound deposits often concatenate `{Sender}{Suffix}{DepositType}{AccountSuffix}` with no spaces. Recognize these deposit-type tokens (sometimes mashed together with the sender's legal suffix): `SALARY`, `REG SALARY`, `PAYROLL`, `DIRECT DEPOSIT`, `INTEREST`, `INT PAYMENT`, `DIVIDEND`, `REFUND`, `REBATE`, `IRS REFUND`, `TAX REFUND`. Format as `{Sender} {DepositType} {AccountSuffix}` after splitting tokens correctly. So "PNCBANKNAREGSALARY****40047586" -> "PNC Bank Salary Deposit ****40047586".
- Preserve as much useful detail as the source carries — it's harmless and aids reconciliation:
  - Store / branch / warehouse numbers (`Trader Joe's #543`, `Home Depot #0345`, `Costco #1025`).
  - City and state when present in the source. Don't try to distinguish "physical purchase location" from "corporate HQ" — both are fine.
  - Row-specific identifiers (last-4 on card payments, full account suffix on transfers, payment reference numbers).
  - Phone numbers next to a merchant or location. They're harmless context.
- Strip ONLY these clearly-meaningless items:
  - Authorization prefixes: PURCHASE AUTHORIZED ON, POS DEBIT, DEBITPOSAP, DBCRDPURAP, DDAPURCHASEAP, DDAPURAP, ACH DEBIT, DIRECT DEPOSIT, ELECTRONICPMT, RECURRING PAYMENT, and similar.
  - Auth codes (e.g. `AUT121524`, embedded transaction dates).
  - Masked card numbers (`*****12345`, `XXXX1234`, `CARD 1234`).
  - Card-network tokens on POS rows (VISA, MASTERCARD, MC) — see merchant rules for the tiered policy on AMEX / DISCOVER.
- Preserve natural brand capitalization (Starbucks, Amazon, CVS Pharmacy, ConEd, Whole Foods Market). Preserve odd casing on unfamiliar merchants (HARBORNYC).
- Delivery and marketplace services are user-visible — KEEP them paired with the vendor using " - ". Examples: DoorDash, Uber Eats, Grubhub, Seamless, Instacart, Caviar, Postmates. So "DOORDASH*CHIPOTLE" -> "DoorDash - Chipotle". If no vendor is attached, return just the service name ("Uber Eats").
- Payment processors are just rails — STRIP them and keep the vendor. Examples: PAYPAL *, SQ * (Square), TST* (Toast), PY *, GOOGLE * (Google Pay), APPLE PAY. So "PAYPAL *STEAM GAMES" -> "Steam", "SQ *BLUE BOTTLE COFFEE OAKLAND CA" -> "Blue Bottle Coffee - Oakland, CA".
- Keep meaningful qualifiers that identify the income/expense type: "Acme Corp Payroll" (not "Acme Corp"), "Amazon Prime" (not "Amazon"), "Uber Trip" vs "Uber Eats", "Venmo Payment"."""


_MERCHANT_RULES = """Merchant name rules (applied to `merchant_name`):
- The merchant is JUST the normalized brand — no metadata. The cleaned_description carries extras (location, store number, identifier, phone, .com suffixes); merchant_name strips all of those down to the brand alone. So "Starbucks #12345 - Seattle, WA" -> merchant "Starbucks"; "Costco Whse #1025 - Manahawkin, NJ" -> merchant "Costco"; "Apple.com/bill 866-712-7753" -> merchant "Apple"; "Venmo Payment 3125551234" -> merchant "Venmo".
- Aggregator-prefixed descriptions: take the vendor, not the aggregator. "DoorDash - Chipotle" -> merchant "Chipotle". "Grubhub - Shake Shack" -> "Shake Shack". Plain "DoorDash" (no vendor) -> "DoorDash".
- Legal-entity suffixes (`NA`, `N.A.`, `FSB`, `CU`, `INC`, `LLC`, `CO`, `LTD`, `PLC`) are formal corporate designations, not part of the brand. Treat them as a token boundary — the brand stops before the suffix; whatever follows the suffix is descriptive (deposit type, payment shape, qualifier). Strip the suffix from merchant_name. So "PNCBANKNAREGSALARY" decomposes as "PNC Bank" (brand) + "NA" (suffix) + "REG SALARY" (deposit type) — merchant is "PNC Bank", description preserves the deposit type. Same boundary logic applies to any corporate suffix anywhere.
- Strip type qualifiers: "Acme Corp Payroll" -> "Acme Corp", "Uber Trip" -> "Uber", "Uber Eats" -> "Uber Eats" (Uber Eats IS the brand).
- Card networks vs issuers — the stoplist is tiered:
  - VISA, MASTERCARD, MC are pure payment networks. NEVER select one as the merchant. If one of these tokens is the most prominent in the source, the actual merchant follows it (e.g. "VISADDAPURAP HARBORNYC ..." -> merchant is "HARBORNYC", not "Visa"). The token itself is rail noise like PAYPAL or SQ.
  - AMEX, AMERICAN EXPRESS, DISCOVER, DISC are BOTH networks AND issuers. Treat as the merchant ONLY when the row is a payment/transfer (source contains ELECTRONICPMT, EPAYMENT, ACHPMT, WEBPMT, BILLPAY, or similar). On POS-purchase rows the same token is the network rail and should be stripped — pick whatever merchant token follows it instead.
- Unrecognized merchants: if the merchant token is unfamiliar (no well-known brand match), output it AS-IS — preserve odd casing like "HARBORNYC" rather than substituting a recognizable nearby token (network name, processor, city). Better to surface the raw merchant string than to mislabel.
- If the transaction has no merchant (ATM, interest, bank fee), return a short type label ("ATM", "Bank", "Interest")."""


_CATEGORY_RULES = """Category rules (applied to `suggested_category_uuid` + `suggested_subcategory_uuid`):
- Pick the SUBCATEGORY first — the UUID MUST be one of the subcategory UUIDs listed below.
- Its parent category UUID MUST be the parent it's listed under — never mix (e.g. subcategory "General Merchandise" ONLY pairs with parent "Miscellaneous"; "Home Goods" ONLY pairs with parent "Shopping"; these are not interchangeable).
- NEVER emit a UUID that isn't in this list. NEVER invent one.
- When no subcategory is a clean fit, pick "Miscellaneous / General Merchandise".
- For income-shaped transactions (payroll, direct deposit, dividend, interest received), pick under "Income".
- Mortgage payments go to Housing / Mortgage — NOT Debt Payment. Debt Payment is for credit cards, student loans, and car loans only.
- Home Depot, Lowe's, and similar home-improvement stores go to Housing / Home Repair — NOT Shopping / Home Goods.
- Cosmetics and beauty stores (Sephora, Ulta, MAC) go to Personal Care / Toiletries — NOT Shopping.
- Generic Amazon purchases (AMZN MKTP, AMAZON.COM with no further context) go to Miscellaneous / General Merchandise — the item is unknown, so don't commit to Shopping.
- For coffee shops specifically (Starbucks, Blue Bottle, etc.), use Food / Coffee Shops — not Restaurants.
- For streaming services (Netflix, Spotify, Hulu), use Entertainment / Streaming Services.
- Video games and gaming platforms (Steam, PlayStation, Xbox) go to Entertainment / Hobbies — NOT Streaming Services."""


_FEW_SHOT_EXAMPLES = """Examples (raw input -> output JSON):

Input: {"description": "PURCHASE AUTHORIZED ON 03/14 STARBUCKS STORE 12345 SEATTLE WA CARD 1234", "amount": "4.75", "transaction_type": "PURCHASE"}
Output: {"cleaned_description": "Starbucks #12345 - Seattle, WA", "merchant_name": "Starbucks", "suggested_category_uuid": "9bf074af-479f-4d55-853c-e807a4bbbe9e", "suggested_subcategory_uuid": "88accd63-6963-417a-b334-970d28a91cf5", "confidence": 0.98}

Input: {"description": "DOORDASH*CHIPOTLE 855-9731040 CA", "amount": "23.40", "transaction_type": "PURCHASE"}
Output: {"cleaned_description": "DoorDash - Chipotle", "merchant_name": "Chipotle", "suggested_category_uuid": "9bf074af-479f-4d55-853c-e807a4bbbe9e", "suggested_subcategory_uuid": "dd2d9c68-4c00-444e-80ed-775a72087bea", "confidence": 0.95}

Input: {"description": "UBER EATS *SWEETGREEN HELP.UBER.COM", "amount": "18.40", "transaction_type": "PURCHASE"}
Output: {"cleaned_description": "Uber Eats - Sweetgreen", "merchant_name": "Sweetgreen", "suggested_category_uuid": "9bf074af-479f-4d55-853c-e807a4bbbe9e", "suggested_subcategory_uuid": "dd2d9c68-4c00-444e-80ed-775a72087bea", "confidence": 0.94}

Input: {"description": "DIRECT DEPOSIT ACME CORP PAYROLL", "amount": "3250.00", "transaction_type": "CREDIT"}
Output: {"cleaned_description": "Acme Corp Payroll", "merchant_name": "Acme Corp", "suggested_category_uuid": "17ac387d-1817-48d5-85c6-84bd2af576e9", "suggested_subcategory_uuid": "42e344f9-55f1-4f46-9c12-d548658409fb", "confidence": 0.99}

Input: {"description": "NETFLIX.COM LOS GATOS CA", "amount": "15.49", "transaction_type": "PURCHASE"}
Output: {"cleaned_description": "Netflix", "merchant_name": "Netflix", "suggested_category_uuid": "78bd0a07-5447-4cb6-b2d6-315d3d4cb4a0", "suggested_subcategory_uuid": "d6762e10-a608-417a-a7a6-87a2977e59e1", "confidence": 0.99}

Input: {"description": "ACH DEBIT CONED 1-800-752-6633", "amount": "98.22", "transaction_type": "PURCHASE"}
Output: {"cleaned_description": "ConEd", "merchant_name": "ConEd", "suggested_category_uuid": "f8ee90f0-2d76-4547-b9b4-71fbb2c506d6", "suggested_subcategory_uuid": "8b4be050-62fa-4520-b5af-012e0eb048f5", "confidence": 0.96}

Input: {"description": "SHELL OIL 123 HWY 1 BURBANK CA", "amount": "52.00", "transaction_type": "PURCHASE"}
Output: {"cleaned_description": "Shell - Burbank, CA", "merchant_name": "Shell", "suggested_category_uuid": "d0032366-ed8b-484b-9564-7f5e9721aa7e", "suggested_subcategory_uuid": "936a458b-82eb-4278-b64f-4fba8f7ae8da", "confidence": 0.94}

Input: {"description": "TRADER JOE'S #543 QPS BROOKLYN NY", "amount": "67.10", "transaction_type": "PURCHASE"}
Output: {"cleaned_description": "Trader Joe's #543 - Brooklyn, NY", "merchant_name": "Trader Joe's", "suggested_category_uuid": "9bf074af-479f-4d55-853c-e807a4bbbe9e", "suggested_subcategory_uuid": "0b66599a-0919-46cb-8d86-ea0517a66f12", "confidence": 0.98}

Input: {"description": "INTEREST CHARGE ON PURCHASES", "amount": "12.55", "transaction_type": "INTEREST"}
Output: {"cleaned_description": "Interest Charge", "merchant_name": "Bank", "suggested_category_uuid": "54812989-bc35-4acb-aa11-a93aaa7b6b65", "suggested_subcategory_uuid": "b9328f2f-88f5-4128-90af-87130c967280", "confidence": 0.9}

Input: {"description": "WELLS FARGO HOME MORTGAGE", "amount": "2100.00", "transaction_type": "PURCHASE"}
Output: {"cleaned_description": "Wells Fargo Home Mortgage", "merchant_name": "Wells Fargo", "suggested_category_uuid": "f8ee90f0-2d76-4547-b9b4-71fbb2c506d6", "suggested_subcategory_uuid": "8c86ff04-3f6c-467c-a5cb-e9295521ae3a", "confidence": 0.97}

Input: {"description": "HOME DEPOT #0345 BROOKLYN NY", "amount": "78.40", "transaction_type": "PURCHASE"}
Output: {"cleaned_description": "Home Depot #0345 - Brooklyn, NY", "merchant_name": "Home Depot", "suggested_category_uuid": "f8ee90f0-2d76-4547-b9b4-71fbb2c506d6", "suggested_subcategory_uuid": "17e8d1a2-3965-49ea-8bfd-5645657172da", "confidence": 0.92}

Input: {"description": "SEPHORA #0125 NEW YORK", "amount": "56.20", "transaction_type": "PURCHASE"}
Output: {"cleaned_description": "Sephora #0125 - New York", "merchant_name": "Sephora", "suggested_category_uuid": "ee02d7ee-7f8f-4983-8693-694dc0a1faae", "suggested_subcategory_uuid": "1a1b3dd2-e0e7-42dc-beed-1cdd88a5441b", "confidence": 0.93}

Input: {"description": "AMZN MKTP US*AB1CD2EF3", "amount": "42.18", "transaction_type": "PURCHASE"}
Output: {"cleaned_description": "Amazon", "merchant_name": "Amazon", "suggested_category_uuid": "0284c65f-1af6-48d2-9133-3d3ac3393ede", "suggested_subcategory_uuid": "5247aeec-a479-4801-9f5e-07af3122f6f9", "confidence": 0.85}

Input: {"description": "PAYPAL *STEAM GAMES 4029357733", "amount": "29.99", "transaction_type": "PURCHASE"}
Output: {"cleaned_description": "Steam", "merchant_name": "Steam", "suggested_category_uuid": "78bd0a07-5447-4cb6-b2d6-315d3d4cb4a0", "suggested_subcategory_uuid": "1831cdfa-bc8a-45e7-a552-404ee54b3464", "confidence": 0.95}

Input: {"description": "DBCRDPURAP,*****30089881312,AUT121524VISADDAPURAP HARBORNYC 9179934001 *NY", "amount": "42.52", "transaction_type": "PURCHASE"}
Output: {"cleaned_description": "HARBORNYC - NY", "merchant_name": "HARBORNYC", "suggested_category_uuid": "0284c65f-1af6-48d2-9133-3d3ac3393ede", "suggested_subcategory_uuid": "5247aeec-a479-4801-9f5e-07af3122f6f9", "confidence": 0.55}

Input: {"description": "ELECTRONICPMT-WEB, AMEXEPAYMENTACHPMTM7284", "amount": "3000.00", "transaction_type": "PURCHASE"}
Output: {"cleaned_description": "Amex Payment 7284", "merchant_name": "Amex", "suggested_category_uuid": "54812989-bc35-4acb-aa11-a93aaa7b6b65", "suggested_subcategory_uuid": "b9328f2f-88f5-4128-90af-87130c967280", "confidence": 0.95}

Input: {"description": "MASTERCARD PURCHASE FERN COFFEE BAR PORTLAND OR", "amount": "6.25", "transaction_type": "PURCHASE"}
Output: {"cleaned_description": "Fern Coffee Bar - Portland, OR", "merchant_name": "Fern Coffee Bar", "suggested_category_uuid": "9bf074af-479f-4d55-853c-e807a4bbbe9e", "suggested_subcategory_uuid": "88accd63-6963-417a-b334-970d28a91cf5", "confidence": 0.85}

Input: {"description": "ACHDEPOSIT,PNCBANKNAREGSALARY****40047586", "amount": "2523.89", "transaction_type": "DEPOSIT"}
Output: {"cleaned_description": "PNC Bank Salary Deposit ****40047586", "merchant_name": "PNC Bank", "suggested_category_uuid": "17ac387d-1817-48d5-85c6-84bd2af576e9", "suggested_subcategory_uuid": "42e344f9-55f1-4f46-9c12-d548658409fb", "confidence": 0.92}

Input: {"description": "INTERESTPAYMENT****99887766", "amount": "12.45", "transaction_type": "DEPOSIT"}
Output: {"cleaned_description": "Interest Payment ****99887766", "merchant_name": "Bank", "suggested_category_uuid": "17ac387d-1817-48d5-85c6-84bd2af576e9", "suggested_subcategory_uuid": "fe41dac0-0a3b-4e33-a731-9aecc6217d42", "confidence": 0.90}"""


def _build_system_prompt() -> str:
    return "\n\n".join([
        (
            "You normalize messy transaction descriptions from bank and "
            "credit-card statements, identify the merchant, and classify each "
            "transaction into a predefined category/subcategory. Output is "
            "machine-consumed — follow the schema exactly."
        ),
        _DESCRIPTION_CLEANUP_RULES,
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
    hallucinate an ID that doesn't resolve to a real CategoryDB row.
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
                            "cleaned_description": {"type": "string"},
                            "merchant_name": {"type": "string"},
                            "suggested_category_uuid": {
                                "type": "string",
                                "enum": all_parent_uuids(),
                            },
                            "suggested_subcategory_uuid": {
                                "type": "string",
                                "enum": all_subcategory_uuids(),
                            },
                            "confidence": {
                                "type": "number",
                                "minimum": 0.0,
                                "maximum": 1.0,
                            },
                        },
                        "required": [
                            "cleaned_description",
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
        """Clean + classify a batch of parsed transactions. Returns exactly
        ``len(parsed)`` results in the same order. Raises ``LLMUnavailableError``
        on any unrecoverable failure (connection, timeout, malformed JSON,
        count mismatch)."""

    def clean_descriptions_batch(self, raws: list[str]) -> list[str]:
        """Back-compat wrapper for #27's description-only callers.

        Builds a minimal ``parsed`` payload from each raw and projects the
        ``cleaned_description`` field out of the consolidated batch result.
        Empty inputs are passed through without a model call."""
        if not raws:
            return []
        parsed = [
            {"description": r, "amount": "", "transaction_type": "", "transaction_date": ""}
            for r in raws
        ]
        results = self.process_transaction_batch(parsed)
        return [r["cleaned_description"] for r in results]

    def clean_description(self, raw: str) -> str:
        """Convenience single-item wrapper."""
        return self.clean_descriptions_batch([raw])[0]


class LlamaCppClient(LLMClient):
    """OpenAI-compatible client targeting a local llama-server instance."""

    def __init__(
        self,
        endpoint: str,
        model: str,
        timeout_s: float = 15.0,
        max_retries: int = 1,
    ):
        self._endpoint = endpoint
        self._model = model
        self._timeout_s = timeout_s
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
                # Qwen3 reasoning mode blows past the latency budget and adds
                # nothing for this mechanical task. Harmless on non-Qwen3 models
                # — the server ignores unknown template kwargs.
                extra_body={"chat_template_kwargs": {"enable_thinking": False}},
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
                sub_uuid = str(r["suggested_subcategory_uuid"])
                # Trust the subcategory, derive the parent — see _SUB_TO_PARENT comment.
                cat_uuid = _SUB_TO_PARENT.get(sub_uuid, str(r["suggested_category_uuid"]))
                out.append({
                    "cleaned_description": str(r["cleaned_description"]).strip(),
                    "merchant_name": str(r["merchant_name"]).strip(),
                    "suggested_category_uuid": cat_uuid,
                    "suggested_subcategory_uuid": sub_uuid,
                    "confidence": float(r.get("confidence", 0.0)),
                })
            except (KeyError, TypeError, ValueError) as e:
                logger.error(f"LLM result missing expected fields: {r!r}")
                raise LLMUnavailableError(f"Malformed result row: {e}") from e

        return out


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


_client_singleton: Optional[LLMClient] = None


def get_llm_client() -> LLMClient:
    """Factory. Returns a cached client based on LLM_BACKEND env var.

    Env vars:
        LLM_BACKEND      'llama_cpp' (default) | 'anthropic'
        LLM_ENDPOINT     default 'http://localhost:8080/v1' (llama.cpp only)
        LLM_MODEL        model identifier; defaults depend on backend
        LLM_API_KEY      Anthropic API key (unused for llama.cpp)
        LLM_TIMEOUT_S    per-call timeout in seconds (default 15.0)
    """
    global _client_singleton
    if _client_singleton is not None:
        return _client_singleton

    backend = os.getenv("LLM_BACKEND", "llama_cpp").lower()
    timeout_s = float(os.getenv("LLM_TIMEOUT_S", "15.0"))

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
