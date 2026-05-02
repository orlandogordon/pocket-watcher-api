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
    """
    merchant_name: Optional[str]
    suggested_category_uuid: str
    suggested_subcategory_uuid: str
    confidence: float


# ---------- prompt fragments ----------

_MERCHANT_RULES = """Merchant name rules (applied to `merchant_name`):
- `merchant_name` is JUST the normalized brand — no metadata, no location, no store number, no identifier. So "STARBUCKS STORE 12345 SEATTLE WA" -> merchant "Starbucks"; "COSTCO WHSE 1025 MANAHAWKIN NJ" -> merchant "Costco"; "Apple.com/bill 866-712-7753" -> merchant "Apple"; "VENMO 3125551234" -> merchant "Venmo".
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
- Pick the SUBCATEGORY first — the UUID MUST be one of the subcategory UUIDs listed below.
- Its parent category UUID MUST be the parent it's listed under — never mix (e.g. subcategory "General Merchandise" ONLY pairs with parent "Miscellaneous"; "Home Goods" ONLY pairs with parent "Shopping"; these are not interchangeable).
- NEVER emit a UUID that isn't in this list. NEVER invent one.
- When no subcategory is a clean fit, pick "Miscellaneous / General Merchandise".
- For income-shaped transactions (payroll, direct deposit, dividend, interest received), pick under "Income".
- Mortgage payments go to Housing / Mortgage — NOT Debt Payment. Debt Payment is for credit cards, student loans, and car loans only.
- Student loan servicers (HESAA, Nelnet, MOHELA, Sallie Mae, Navient, Great Lakes, EdFinancial, Dept of Education / DEPTEDUCATION) on payment-shaped rows go to Debt Payment / Student Loan. The merchant token may be concatenated with PAYMENT (e.g. HESAAPAYMENT, NELNETPAYMENT) — preserve all letters of the servicer name; do not drop trailing letters when the token splits.
- Auto lenders (Ally, Capital One Auto, Toyota Financial Services, Honda Financial, Ford Credit) on payment-shaped rows go to Debt Payment / Car Loan.
- Home Depot, Lowe's, and similar home-improvement stores go to Housing / Home Repair — NOT Shopping / Home Goods.
- Cosmetics and beauty stores (Sephora, Ulta, MAC) go to Personal Care / Toiletries — NOT Shopping.
- Generic Amazon purchases (AMZN MKTP, AMAZON.COM with no further context) go to Miscellaneous / General Merchandise — the item is unknown, so don't commit to Shopping.
- For coffee shops specifically (Starbucks, Blue Bottle, etc.), use Food / Coffee Shops — not Restaurants.
- For streaming services (Netflix, Spotify, Hulu), use Entertainment / Streaming Services.
- Video games and gaming platforms (Steam, PlayStation, Xbox) go to Entertainment / Hobbies — NOT Streaming Services."""


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
Output: {"merchant_name": "Netflix", "suggested_category_uuid": "78bd0a07-5447-4cb6-b2d6-315d3d4cb4a0", "suggested_subcategory_uuid": "d6762e10-a608-417a-a7a6-87a2977e59e1", "confidence": 0.99}

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

Input: {"description": "DDA WITHDRAW AP TW04C996  1120 TILTON RD  NORTHFIELD  * NJ", "amount": "200.00", "transaction_type": "PURCHASE"}
Output: {"merchant_name": null, "suggested_category_uuid": "0284c65f-1af6-48d2-9133-3d3ac3393ede", "suggested_subcategory_uuid": "d7a3041e-5253-492c-82ca-ca24fb25df26", "confidence": 0.7}

Input: {"description": "ATM CASH DEPOSIT TW04C196  1101 HOOPER AVENUE  TOMS RIVER  * NJ", "amount": "300.00", "transaction_type": "DEPOSIT"}
Output: {"merchant_name": null, "suggested_category_uuid": "17ac387d-1817-48d5-85c6-84bd2af576e9", "suggested_subcategory_uuid": "42e344f9-55f1-4f46-9c12-d548658409fb", "confidence": 0.7}

Input: {"description": "ANNUAL MEMBERSHIP FEE", "amount": "95.00", "transaction_type": "PURCHASE"}
Output: {"merchant_name": null, "suggested_category_uuid": "54812989-bc35-4acb-aa11-a93aaa7b6b65", "suggested_subcategory_uuid": "b9328f2f-88f5-4128-90af-87130c967280", "confidence": 0.85}

Input: {"description": "MOBILE PAYMENT - THANK YOU", "amount": "1500.00", "transaction_type": "TRANSFER_IN"}
Output: {"merchant_name": null, "suggested_category_uuid": "0284c65f-1af6-48d2-9133-3d3ac3393ede", "suggested_subcategory_uuid": "5247aeec-a479-4801-9f5e-07af3122f6f9", "confidence": 0.65}

Input: {"description": "EDDITINCCLASS A", "amount": "245.00", "transaction_type": "BUY"}
Output: {"merchant_name": null, "suggested_category_uuid": "1601d6e1-e0d7-44f7-8f47-207ca11538be", "suggested_subcategory_uuid": "a762c7e9-7a3d-4ab5-97e4-814b14d81e0b", "confidence": 0.6}

Input: {"description": "DIVIDEND VOO", "amount": "45.20", "transaction_type": "DIVIDEND"}
Output: {"merchant_name": "Vanguard", "suggested_category_uuid": "17ac387d-1817-48d5-85c6-84bd2af576e9", "suggested_subcategory_uuid": "fe41dac0-0a3b-4e33-a731-9aecc6217d42", "confidence": 0.95}"""


def _build_system_prompt() -> str:
    return "\n\n".join([
        (
            "You identify the merchant brand and classify each transaction "
            "into a predefined category/subcategory. The raw description is "
            "preserved verbatim by the caller — do NOT rewrite or clean it. "
            "merchant_name is nullable: return null when the source contains "
            "no real brand. Output is machine-consumed — follow the schema "
            "exactly."
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
                            # Nullable: model emits null when no real brand
                            # exists in the source (bare addresses, generic
                            # descriptors, parser-corrupted tokens).
                            "merchant_name": {"type": ["string", "null"]},
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
        category UUIDs + confidence. Returns exactly ``len(parsed)`` results
        in the same order. Raises ``LLMUnavailableError`` on any unrecoverable
        failure (connection, timeout, malformed JSON, count mismatch)."""


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
                confidence = float(r.get("confidence", 0.0))

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
