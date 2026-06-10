"""Tests for the Needs Review explanation note composed at confirm time.

When a transaction is auto-tagged 'Needs Review' during statement import, the
reason is appended to its ``comments`` so the review inbox (#46 surfaces
``comments``) records WHY it was flagged. See ``append_review_note`` in
src.services.system_tags (shared by the preview/confirm and bulk paths, #68).
"""
import unittest

from src.services.system_tags import append_review_note


class TestAppendReviewNote(unittest.TestCase):
    def test_missing_category_only(self):
        self.assertEqual(
            append_review_note(None, missing_category=True, missing_merchant=False),
            "Auto-flagged for review: no category assigned.",
        )

    def test_missing_merchant_only(self):
        self.assertEqual(
            append_review_note(None, missing_category=False, missing_merchant=True),
            "Auto-flagged for review: no merchant identified.",
        )

    def test_missing_both(self):
        self.assertEqual(
            append_review_note(None, missing_category=True, missing_merchant=True),
            "Auto-flagged for review: no category assigned and no merchant identified.",
        )

    def test_preserves_existing_user_comment(self):
        # A comment the user entered during preview stays first; the note is
        # appended on its own line.
        self.assertEqual(
            append_review_note(
                "reimburse me", missing_category=True, missing_merchant=False
            ),
            "reimburse me\nAuto-flagged for review: no category assigned.",
        )

    def test_existing_comment_whitespace_trimmed(self):
        self.assertEqual(
            append_review_note(
                "  note  ", missing_category=False, missing_merchant=True
            ),
            "note\nAuto-flagged for review: no merchant identified.",
        )

    def test_no_trigger_returns_existing_unchanged(self):
        # Defensive: caller only invokes this once a trigger holds, but if
        # neither flag is set the comment is left untouched.
        self.assertEqual(
            append_review_note("keep me", missing_category=False, missing_merchant=False),
            "keep me",
        )
        self.assertIsNone(
            append_review_note(None, missing_category=False, missing_merchant=False)
        )


if __name__ == "__main__":
    unittest.main()
