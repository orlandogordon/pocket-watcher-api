"""description_cleanup.process_preview_items — LLM faked via the fake_llm fixture.

Covers the per-row merchant decision (regex extractor beats LLM; truncated
source blanks the merchant; LLM merchant used only when regex finds none), the
empty-description short-circuit, graceful fall-through when the LLM is
unavailable (regex merchant survives, no category suggestion), and order
preservation.
"""
import pytest

from src.services.description_cleanup import process_preview_items

pytestmark = pytest.mark.integration

_SUGGESTION = {
    "merchant_name": "Acme",
    "suggested_category_uuid": "cat-uuid",
    "suggested_subcategory_uuid": "sub-uuid",
    "confidence": 0.95,
}


def test_empty_description_short_circuits(db, fake_llm):
    [res] = process_preview_items(db, [{"description": ""}], user_id=1)
    assert res.source == "empty"
    assert res.merchant_name is None


def test_regex_merchant_beats_llm(db, fake_llm):
    fake_llm.suggestions["STARBUCKS SEATTLE WA"] = {**_SUGGESTION, "merchant_name": "WrongBrand"}
    [res] = process_preview_items(db, [{"description": "STARBUCKS SEATTLE WA"}],
                                  user_id=1, institution="amex")
    assert res.source == "llm"
    assert res.merchant_name == "Starbucks"   # regex extractor, not the LLM's value
    assert res.merchant_source == "regex"


def test_llm_merchant_used_when_no_regex_match(db, fake_llm):
    fake_llm.suggestions["ZZQW RANDOM NOISE XYZ"] = _SUGGESTION
    [res] = process_preview_items(db, [{"description": "ZZQW RANDOM NOISE XYZ"}],
                                  user_id=1, institution=None)
    assert res.merchant_name == "Acme"
    assert res.merchant_source == "llm"
    assert res.llm_suggestion is not None


def test_truncated_merchant_is_blanked(db, fake_llm):
    fake_llm.suggestions["STARBUCKS SEATTLE WA"] = _SUGGESTION
    [res] = process_preview_items(
        db, [{"description": "STARBUCKS SEATTLE WA", "merchant_truncated": True}],
        user_id=1, institution="amex",
    )
    assert res.merchant_name is None
    assert res.merchant_source is None


def test_llm_unavailable_falls_through_but_keeps_regex_merchant(db, fake_llm):
    fake_llm.unavailable = True
    [res] = process_preview_items(db, [{"description": "STARBUCKS SEATTLE WA"}],
                                  user_id=1, institution="amex")
    assert res.source == "raw_fallthrough"
    assert res.merchant_name == "Starbucks"   # regex still applies
    assert res.llm_suggestion is None


def test_order_preserved_across_rows(db, fake_llm):
    items = [{"description": "ZZQW RANDOM NOISE XYZ"}, {"description": ""}, {"description": "ANOTHER NOISE QQQ"}]
    results = process_preview_items(db, items, user_id=1, institution=None)
    assert [r.raw for r in results] == ["ZZQW RANDOM NOISE XYZ", "", "ANOTHER NOISE QQQ"]
