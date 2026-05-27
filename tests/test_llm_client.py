"""llm_client.LlamaCppClient — OpenAI chat client mocked at the boundary.

Verifies the post-processing the client layers on top of the raw model output:
the subcategory->parent correction (trust the harder subcategory pick, derive
the parent), the independent category (0.8) and merchant (0.85) confidence
floors, null-merchant passthrough, and the failure modes that must surface as
LLMUnavailableError (transport error, malformed JSON, result-count mismatch).
"""
import json
from unittest.mock import MagicMock

import pytest

from src.constants.categories import all_parent_uuids, subcategory_to_parent
from src.services.llm_client import LlamaCppClient, LLMUnavailableError

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
    # 0.82: above the 0.8 category floor (keep category), below 0.85 merchant floor (drop merchant).
    c = _client(_payload("Starbucks", SUB, CORRECT_PARENT, 0.82))
    [res] = c.process_transaction_batch([{"description": "SBUX"}])
    assert res["suggested_subcategory_uuid"] == SUB
    assert res["suggested_category_uuid"] == CORRECT_PARENT
    assert res["merchant_name"] is None


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
