"""llm_client.LlamaCppClient — OpenAI chat client mocked at the boundary.

Verifies the post-processing the client layers on top of the raw model output:
the subcategory->parent correction (trust the harder subcategory pick, derive
the parent), the independent category (0.9) and merchant (0.85) confidence
floors, null-merchant passthrough, and the failure modes that must surface as
LLMUnavailableError (transport error, malformed JSON, result-count mismatch).
"""
import json
from unittest.mock import MagicMock

import pytest

from src.constants.categories import all_parent_uuids, subcategory_to_parent
from src.services.llm_client import (
    AnthropicClient,
    LlamaCppClient,
    LLMUnavailableError,
)

_SUB_TO_PARENT = subcategory_to_parent()
SUB = next(iter(_SUB_TO_PARENT))
CORRECT_PARENT = _SUB_TO_PARENT[SUB]
WRONG_PARENT = next(p for p in all_parent_uuids() if p != CORRECT_PARENT)


def _client(payload=None, *, content=None, raise_exc=None):
    c = LlamaCppClient(endpoint="http://test/v1", model="test-model")
    mock = MagicMock()
    if raise_exc is not None:
        mock.chat.completions.create.side_effect = raise_exc
    else:
        msg = MagicMock()
        msg.content = content if content is not None else json.dumps(payload)
        resp = MagicMock()
        resp.choices = [MagicMock(message=msg)]
        mock.chat.completions.create.return_value = resp
    c._client = mock
    return c


def _payload(merchant, sub, cat, conf):
    return {"results": [{
        "merchant_name": merchant,
        "suggested_subcategory_uuid": sub,
        "suggested_category_uuid": cat,
        "confidence": conf,
    }]}


def test_empty_batch_makes_no_api_call():
    c = _client(payload={"results": []})
    assert c.process_transaction_batch([]) == []
    c._client.chat.completions.create.assert_not_called()


def test_parent_is_derived_from_subcategory_not_model_value():
    # Model returns the WRONG parent; client must derive the correct one from the sub.
    c = _client(_payload("Starbucks", SUB, WRONG_PARENT, 0.98))
    [res] = c.process_transaction_batch([{"description": "SBUX"}])
    assert res["merchant_name"] == "Starbucks"
    assert res["suggested_subcategory_uuid"] == SUB
    assert res["suggested_category_uuid"] == CORRECT_PARENT


def test_low_category_confidence_nulls_the_pair_and_merchant():
    c = _client(_payload("Starbucks", SUB, CORRECT_PARENT, 0.5))
    [res] = c.process_transaction_batch([{"description": "SBUX"}])
    assert res["suggested_subcategory_uuid"] is None
    assert res["suggested_category_uuid"] is None
    assert res["merchant_name"] is None  # 0.5 < 0.85 merchant floor too


def test_merchant_floor_independent_of_category_floor():
    # 0.87: below the 0.90 category floor (drop the category pair), above the
    # 0.85 merchant floor (keep the merchant). The floors are independent, and
    # since #64 the category floor (0.90) sits ABOVE the merchant floor (0.85).
    c = _client(_payload("Starbucks", SUB, CORRECT_PARENT, 0.87))
    [res] = c.process_transaction_batch([{"description": "SBUX"}])
    assert res["suggested_subcategory_uuid"] is None
    assert res["suggested_category_uuid"] is None
    assert res["merchant_name"] == "Starbucks"


def test_null_merchant_passes_through_with_category_kept():
    c = _client(_payload(None, SUB, CORRECT_PARENT, 0.98))
    [res] = c.process_transaction_batch([{"description": "x"}])
    assert res["merchant_name"] is None
    assert res["suggested_subcategory_uuid"] == SUB


def test_malformed_json_raises_unavailable():
    c = _client(content="not valid json{")
    with pytest.raises(LLMUnavailableError):
        c.process_transaction_batch([{"description": "x"}])


def test_result_count_mismatch_raises_unavailable():
    c = _client(payload={"results": []})  # zero results for one input
    with pytest.raises(LLMUnavailableError):
        c.process_transaction_batch([{"description": "x"}])


def test_transport_error_raises_unavailable():
    c = _client(raise_exc=RuntimeError("connection refused"))
    with pytest.raises(LLMUnavailableError):
        c.process_transaction_batch([{"description": "x"}])


# ---- health_check (#60) ----

class _FakeModel:
    def __init__(self, id):
        self.id = id


class _FakeModelsPage:
    def __init__(self, ids):
        self.data = [_FakeModel(i) for i in ids]


class _FakeModelsNamespace:
    def __init__(self, ids=None, exc=None):
        self._ids = ids or []
        self._exc = exc

    def list(self):
        if self._exc is not None:
            raise self._exc
        return _FakeModelsPage(self._ids)


class _FakeHealthClient:
    """Stands in for the OpenAI client: with_options() returns self, and
    models.list() is the probe target health_check() hits."""
    def __init__(self, models):
        self.models = models

    def with_options(self, **kwargs):
        return self


def _health_client(ids=None, exc=None):
    c = LlamaCppClient(endpoint="http://test/v1", model="cfg-model")
    c._client = _FakeHealthClient(_FakeModelsNamespace(ids=ids, exc=exc))
    return c


def test_health_check_online_reports_served_model():
    assert _health_client(ids=["served-model"]).health_check() == (True, "served-model")


def test_health_check_online_empty_list_falls_back_to_config_model():
    assert _health_client(ids=[]).health_check() == (True, "cfg-model")


def test_health_check_offline_on_exception_never_raises():
    assert _health_client(exc=RuntimeError("connection refused")).health_check() == (False, None)


# ---- AnthropicClient (#82) — anthropic SDK mocked at the boundary ----

class _Block:
    """Stand-in for an anthropic content block."""
    def __init__(self, text=None, type="text"):
        self.type = type
        if text is not None:
            self.text = text


class _Resp:
    def __init__(self, blocks, stop_reason="end_turn"):
        self.content = blocks
        self.stop_reason = stop_reason


def _anthropic(payload=None, *, content=None, blocks=None, raise_exc=None,
               stop_reason="end_turn"):
    c = AnthropicClient(model="claude-haiku-4-5", api_key="test-key")
    mock = MagicMock()
    if raise_exc is not None:
        mock.messages.create.side_effect = raise_exc
    else:
        if blocks is None:
            text = content if content is not None else json.dumps(payload)
            blocks = [_Block(text=text)]
        mock.messages.create.return_value = _Resp(blocks, stop_reason)
    c._client = mock  # inject — skips lazy SDK construction
    return c


def test_anthropic_empty_batch_makes_no_api_call():
    c = _anthropic(payload={"results": []})
    assert c.process_transaction_batch([]) == []
    c._client.messages.create.assert_not_called()


def test_anthropic_uses_structured_outputs_and_cached_system():
    c = _anthropic(_payload("Starbucks", SUB, CORRECT_PARENT, 0.98))
    c.process_transaction_batch([{"description": "SBUX"}])
    kwargs = c._client.messages.create.call_args.kwargs
    assert kwargs["output_config"]["format"]["type"] == "json_schema"
    # system prompt sent as a cached block (cost control, #82 §5)
    assert kwargs["system"][0]["cache_control"] == {"type": "ephemeral"}


def test_anthropic_parent_is_derived_from_subcategory_not_model_value():
    c = _anthropic(_payload("Starbucks", SUB, WRONG_PARENT, 0.98))
    [res] = c.process_transaction_batch([{"description": "SBUX"}])
    assert res["suggested_subcategory_uuid"] == SUB
    assert res["suggested_category_uuid"] == CORRECT_PARENT  # shared post-processing


def test_anthropic_low_category_confidence_nulls_the_pair_and_merchant():
    c = _anthropic(_payload("Starbucks", SUB, CORRECT_PARENT, 0.5))
    [res] = c.process_transaction_batch([{"description": "SBUX"}])
    assert res["suggested_subcategory_uuid"] is None
    assert res["suggested_category_uuid"] is None
    assert res["merchant_name"] is None


def test_anthropic_malformed_json_raises_unavailable():
    c = _anthropic(content="not valid json{")
    with pytest.raises(LLMUnavailableError):
        c.process_transaction_batch([{"description": "x"}])


def test_anthropic_no_text_block_raises_unavailable():
    # Refusal / truncation: empty content array, no JSON to read.
    c = _anthropic(blocks=[], stop_reason="refusal")
    with pytest.raises(LLMUnavailableError):
        c.process_transaction_batch([{"description": "x"}])


def test_anthropic_result_count_mismatch_raises_unavailable():
    c = _anthropic(payload={"results": []})  # zero results for one input
    with pytest.raises(LLMUnavailableError):
        c.process_transaction_batch([{"description": "x"}])


def test_anthropic_transport_error_raises_unavailable():
    c = _anthropic(raise_exc=RuntimeError("connection refused"))
    with pytest.raises(LLMUnavailableError):
        c.process_transaction_batch([{"description": "x"}])


def test_anthropic_health_check_online_reports_model_id():
    c = AnthropicClient(model="claude-haiku-4-5", api_key="test-key")
    mock = MagicMock()
    mock.with_options.return_value.models.retrieve.return_value = _FakeModel("claude-haiku-4-5")
    c._client = mock
    assert c.health_check() == (True, "claude-haiku-4-5")


def test_anthropic_health_check_offline_never_raises():
    c = AnthropicClient(model="claude-haiku-4-5", api_key="test-key")
    mock = MagicMock()
    mock.with_options.return_value.models.retrieve.side_effect = RuntimeError("down")
    c._client = mock
    assert c.health_check() == (False, None)
