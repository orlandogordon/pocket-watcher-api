"""
LLM client abstraction.

Backends are selected via the LLM_BACKEND env var. Callers depend only on the
abstract interface; swapping backends (local llama.cpp <-> Anthropic API) is an
env-var change at factory level.

Current use site is transaction description cleanup (#27 Phase 1); #29 and #30
will extend this module with additional task-specific methods.
"""

from __future__ import annotations

import json
import os
from abc import ABC, abstractmethod
from typing import Optional

from openai import OpenAI
from openai import APIConnectionError, APITimeoutError

from src.logging_config import get_logger

logger = get_logger(__name__)


class LLMUnavailableError(Exception):
    """Raised when the LLM backend cannot be reached or fails all retries.

    Callers are expected to catch this and fall through to a graceful
    degradation path (e.g. return raw descriptions unchanged).
    """


DESCRIPTION_CLEANUP_SYSTEM_PROMPT = """You normalize messy transaction descriptions from bank and credit-card statements into clean merchant names for display in a personal-finance app.

Rules:
- Strip authorization prefixes (PURCHASE AUTHORIZED ON, POS DEBIT, ACH DEBIT, DIRECT DEPOSIT, RECURRING PAYMENT), dates, store/location numbers, city/state, phone numbers, and card-suffix fragments (CARD 1234, XXXX1234).
- Preserve natural brand capitalization (Starbucks, Amazon, CVS, ConEd).
- Delivery and marketplace services are user-visible choices — KEEP them paired with the vendor using " - ". Examples: DoorDash, Uber Eats, Grubhub, Seamless, Instacart, Caviar, Postmates. So "DOORDASH*CHIPOTLE" -> "DoorDash - Chipotle". If no vendor is attached, return just the service name ("Uber Eats").
- Payment processors are just rails — STRIP them and keep the vendor. Examples: PAYPAL *, SQ * (Square), TST* (Toast), PY *, GOOGLE * (Google Pay), APPLE PAY. So "PAYPAL *STEAM GAMES" -> "Steam", "SQ *BLUE BOTTLE" -> "Blue Bottle Coffee", "TST* JOE'S PIZZA" -> "Joe's Pizza".
- Keep meaningful qualifiers that identify the income/expense type: "Acme Corp Payroll" (not "Acme Corp"), "Amazon Prime" (not "Amazon"), "Uber Trip" vs "Uber Eats".
- If the raw string is already clean, return it unchanged.
- If the raw string has no identifiable merchant (e.g. "INTEREST CHARGE", "ATM WITHDRAWAL"), return it in Title Case with noise stripped.

Examples:
"PURCHASE AUTHORIZED ON 03/14 STARBUCKS STORE 12345 SEATTLE WA CARD 1234" -> "Starbucks"
"AMZN MKTP US*AB1CD2EF3" -> "Amazon"
"TST* JOE'S PIZZA - NEW YORK NY" -> "Joe's Pizza"
"UBER   EATS 8005928996 CA" -> "Uber Eats"
"DOORDASH*CHIPOTLE 855-9731040 CA" -> "DoorDash - Chipotle"
"UBER EATS *HALAL GUYS HELP.UBER.COM" -> "Uber Eats - Halal Guys"
"GRUBHUB*SHAKE SHACK" -> "Grubhub - Shake Shack"
"POS DEBIT CVS/PHARMACY 07623 BROOKLYN NY" -> "CVS"
"SQ *BLUE BOTTLE COFFEE OAKLAND CA" -> "Blue Bottle Coffee"
"PAYPAL *STEAM GAMES 4029357733" -> "Steam"
"ACH DEBIT CONED 1-800-752-6633" -> "ConEd"
"DIRECT DEPOSIT ACME CORP PAYROLL" -> "Acme Corp Payroll"
"INTEREST CHARGE ON PURCHASES" -> "Interest Charge"
"""


class LLMClient(ABC):
    """Uniform interface for all LLM backends."""

    @property
    @abstractmethod
    def model_name(self) -> str:
        """Identifier written to parsed_imports.llm_model (e.g. 'qwen3.5-9b-q4')."""

    @abstractmethod
    def clean_descriptions_batch(self, raws: list[str]) -> list[str]:
        """Clean a batch of raw descriptions. Must return exactly len(raws) results,
        in the same order. Raises LLMUnavailableError on unrecoverable failure."""

    def clean_description(self, raw: str) -> str:
        """Convenience single-item wrapper around clean_descriptions_batch."""
        return self.clean_descriptions_batch([raw])[0]


def _build_json_schema(count: int) -> dict:
    """Response must be {"cleaned": [str, str, ...]} of exactly `count` items."""
    return {
        "name": "cleaned_descriptions",
        "strict": True,
        "schema": {
            "type": "object",
            "properties": {
                "cleaned": {
                    "type": "array",
                    "items": {"type": "string"},
                    "minItems": count,
                    "maxItems": count,
                },
            },
            "required": ["cleaned"],
            "additionalProperties": False,
        },
    }


def _build_user_prompt(raws: list[str]) -> str:
    lines = [f"{i + 1}. {r}" for i, r in enumerate(raws)]
    return (
        "Clean the following transaction descriptions. Return JSON "
        "matching the schema: an object with key 'cleaned' whose value is an array of "
        f"exactly {len(raws)} strings in the same order.\n\n"
        + "\n".join(lines)
    )


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

    def clean_descriptions_batch(self, raws: list[str]) -> list[str]:
        if not raws:
            return []

        try:
            response = self._client.chat.completions.create(
                model=self._model,
                messages=[
                    {"role": "system", "content": DESCRIPTION_CLEANUP_SYSTEM_PROMPT},
                    {"role": "user", "content": _build_user_prompt(raws)},
                ],
                response_format={
                    "type": "json_schema",
                    "json_schema": _build_json_schema(len(raws)),
                },
                temperature=0.0,
                # Qwen3 ships with reasoning enabled by default; for a mechanical
                # normalization task the chain-of-thought is pure overhead and
                # blows past the 3s budget. Harmless for non-Qwen3 models — the
                # server just ignores unknown template kwargs.
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
            cleaned = payload["cleaned"]
        except (json.JSONDecodeError, KeyError, TypeError) as e:
            logger.error(f"LLM returned malformed JSON: {content!r}")
            raise LLMUnavailableError(f"Malformed JSON: {e}") from e

        if len(cleaned) != len(raws):
            logger.error(
                f"LLM returned {len(cleaned)} items for {len(raws)} inputs; discarding"
            )
            raise LLMUnavailableError("Item count mismatch")

        return [str(c).strip() for c in cleaned]


class AnthropicClient(LLMClient):
    """Stub for production Anthropic backend. Implemented when we deploy to prod.

    Will use the native `anthropic` SDK (not the OpenAI-compat layer — Anthropic's
    OpenAI-compat layer ignores `response_format`). Structured output is enforced
    via tool use: define a tool whose input_schema matches cleaned_descriptions,
    force tool_choice={"type": "tool", "name": ...}, read response.content[0].input.
    """

    def __init__(self, model: str, api_key: Optional[str]):
        self._model = model
        self._api_key = api_key

    @property
    def model_name(self) -> str:
        return self._model

    def clean_descriptions_batch(self, raws: list[str]) -> list[str]:
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
        LLM_TIMEOUT_S    per-call timeout in seconds (default 3.0)
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
