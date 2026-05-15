"""Tests for consolidated transaction hash function."""
import unittest
from datetime import date

from src.crud.crud_transaction import generate_transaction_hash


class TestHashConsolidation(unittest.TestCase):
    """Verify the consolidated hash function produces correct results."""

    def test_deterministic(self):
        h1 = generate_transaction_hash(
            user_id=1, account_id=42,
            transaction_date=date(2025, 1, 15),
            transaction_type_value="PURCHASE",
            amount=100, description="Test",
        )
        h2 = generate_transaction_hash(
            user_id=1, account_id=42,
            transaction_date=date(2025, 1, 15),
            transaction_type_value="PURCHASE",
            amount=100, description="Test",
        )
        self.assertEqual(h1, h2)

    def test_make_unique_produces_different_hashes(self):
        kwargs = dict(
            user_id=1, account_id=42,
            transaction_date=date(2025, 1, 15),
            transaction_type_value="PURCHASE",
            amount=100, description="Test",
        )
        h1 = generate_transaction_hash(**kwargs, make_unique=True)
        h2 = generate_transaction_hash(**kwargs, make_unique=True)
        self.assertNotEqual(h1, h2)

    def test_transfer_in_and_out_produce_different_hashes(self):
        kwargs = dict(
            user_id=1, account_id=7,
            transaction_date=date(2025, 3, 1),
            amount=500, description="Transfer",
        )
        h_in = generate_transaction_hash(**kwargs, transaction_type_value="TRANSFER_IN")
        h_out = generate_transaction_hash(**kwargs, transaction_type_value="TRANSFER_OUT")
        self.assertNotEqual(h_in, h_out)

    def test_different_accounts_same_data_different_hashes(self):
        """Two accounts with otherwise-identical transactions must hash distinctly.

        Pre-#52, both would have hashed identically (institution-based) and the
        second import would have been falsely flagged as a duplicate.
        """
        kwargs = dict(
            user_id=1,
            transaction_date=date(2025, 1, 1),
            transaction_type_value="PURCHASE",
            amount=50, description="Coffee",
        )
        h1 = generate_transaction_hash(account_id=1, **kwargs)
        h2 = generate_transaction_hash(account_id=2, **kwargs)
        self.assertNotEqual(h1, h2)

    def test_none_description_matches_empty(self):
        kwargs = dict(
            user_id=1, account_id=42,
            transaction_date=date(2025, 1, 1),
            transaction_type_value="DEPOSIT",
            amount=100,
        )
        h1 = generate_transaction_hash(**kwargs, description=None)
        h2 = generate_transaction_hash(**kwargs, description="")
        self.assertEqual(h1, h2)

    def test_none_account_id_raises(self):
        with self.assertRaises(ValueError):
            generate_transaction_hash(
                user_id=1, account_id=None,
                transaction_date=date(2025, 1, 1),
                transaction_type_value="PURCHASE",
                amount=100, description="Test",
            )


if __name__ == "__main__":
    unittest.main()
