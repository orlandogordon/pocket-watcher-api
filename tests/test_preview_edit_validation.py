"""Tests for the edited_data allowlist on the preview-edit endpoints.

Backend todo #36: server-internal int IDs (account_id, category_id,
subcategory_id) and any other unknown keys must be rejected before
they can land in the preview session and override safety guards at
confirm time.

These tests exercise the validator directly and confirm both edit
endpoints invoke it before touching Redis.
"""
import asyncio
import unittest
from uuid import uuid4

from fastapi import HTTPException

from src.models.preview import BulkEditRequest, EditTransactionRequest
from src.routers.uploads import (
    ALLOWED_EDITED_DATA_KEYS,
    _validate_edited_data,
    bulk_edit_transactions,
    edit_transaction,
)


class ExplodingRedis:
    """Stand-in for redis.Redis that fails loudly if any method is
    called. The validator must reject before Redis is reached."""

    def __getattr__(self, name):
        def _explode(*args, **kwargs):
            raise AssertionError(
                f"Redis.{name} called — validator should have rejected first"
            )
        return _explode


class TestValidator(unittest.TestCase):
    def test_rejects_account_id(self):
        with self.assertRaises(HTTPException) as ctx:
            _validate_edited_data({"account_id": 1})
        self.assertEqual(ctx.exception.status_code, 400)
        self.assertIn("account_id", ctx.exception.detail)

    def test_rejects_internal_int_id_keys(self):
        """All three server-internal int-ID forms must be rejected."""
        for bad_key in ("account_id", "category_id", "subcategory_id"):
            with self.assertRaises(HTTPException) as ctx:
                _validate_edited_data({bad_key: 1})
            self.assertEqual(ctx.exception.status_code, 400, bad_key)
            self.assertIn(bad_key, ctx.exception.detail)

    def test_mixed_payload_message_lists_only_forbidden(self):
        """An allowed key in the same payload must not appear in the
        error message — only the rejected key."""
        with self.assertRaises(HTTPException) as ctx:
            _validate_edited_data({"description": "ok", "category_id": 5})
        self.assertIn("category_id", ctx.exception.detail)
        self.assertNotIn("description", ctx.exception.detail)

    def test_accepts_allowed_only_payload(self):
        # Should not raise.
        _validate_edited_data({
            "description": "renamed",
            "comments": "note",
            "category_uuid": str(uuid4()),
            "tag_uuids": [str(uuid4())],
        })

    def test_accepts_empty_payload(self):
        _validate_edited_data({})

    def test_allowlist_matches_documented_set(self):
        """Guard against accidental drift between the constant and the
        keys actually consumed by the confirm path."""
        expected_regular = {
            "description", "merchant_name", "category_uuid", "subcategory_uuid",
            "comments", "tag_uuids", "transaction_type", "amount",
            "transaction_date",
        }
        expected_investment = {
            "symbol", "quantity", "price_per_share", "api_symbol",
            "total_amount", "security_type",
        }
        self.assertEqual(
            ALLOWED_EDITED_DATA_KEYS,
            expected_regular | expected_investment,
        )


class TestEndpointWiring(unittest.TestCase):
    """Confirm both edit endpoints call the validator BEFORE any
    Redis access. ExplodingRedis lets us prove the validator runs first
    without spinning up a real Redis or building a session fixture."""

    def test_edit_transaction_rejects_before_redis(self):
        request = EditTransactionRequest(
            temp_id="t1",
            edited_data={"account_id": 7},
        )
        with self.assertRaises(HTTPException) as ctx:
            asyncio.run(edit_transaction(
                session_id="any",
                request=request,
                user_id=1,
                r=ExplodingRedis(),
            ))
        self.assertEqual(ctx.exception.status_code, 400)
        self.assertIn("account_id", ctx.exception.detail)

    def test_bulk_edit_rejects_before_redis(self):
        request = BulkEditRequest(
            temp_ids=["t1", "t2"],
            edited_data={"subcategory_id": 99},
        )
        with self.assertRaises(HTTPException) as ctx:
            asyncio.run(bulk_edit_transactions(
                session_id="any",
                request=request,
                user_id=1,
                r=ExplodingRedis(),
            ))
        self.assertEqual(ctx.exception.status_code, 400)
        self.assertIn("subcategory_id", ctx.exception.detail)


if __name__ == "__main__":
    unittest.main()
