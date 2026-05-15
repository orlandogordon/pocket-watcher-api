"""Tests for investment path TRANSFER_IN/TRANSFER_OUT changes."""
import unittest

from src.db.core import InvestmentTransactionType
from src.crud.crud_investment import map_transaction_type_to_enum


class TestMapTransactionTypeToEnum(unittest.TestCase):
    """Verify all transfer-related strings map to correct directional type."""

    def test_deposit_maps_to_transfer_in(self):
        self.assertEqual(map_transaction_type_to_enum("DEPOSIT"), InvestmentTransactionType.TRANSFER_IN)

    def test_ach_maps_to_transfer_in(self):
        self.assertEqual(map_transaction_type_to_enum("ACH"), InvestmentTransactionType.TRANSFER_IN)

    def test_wire_maps_to_transfer_in(self):
        self.assertEqual(map_transaction_type_to_enum("WIRE"), InvestmentTransactionType.TRANSFER_IN)

    def test_journal_maps_to_transfer_in(self):
        self.assertEqual(map_transaction_type_to_enum("JOURNAL"), InvestmentTransactionType.TRANSFER_IN)

    def test_withdrawal_maps_to_transfer_out(self):
        self.assertEqual(map_transaction_type_to_enum("WITHDRAWAL"), InvestmentTransactionType.TRANSFER_OUT)

    def test_transfer_in_passthrough(self):
        self.assertEqual(map_transaction_type_to_enum("TRANSFER_IN"), InvestmentTransactionType.TRANSFER_IN)

    def test_transfer_out_passthrough(self):
        self.assertEqual(map_transaction_type_to_enum("TRANSFER_OUT"), InvestmentTransactionType.TRANSFER_OUT)

    def test_partial_match_ach_withdrawal(self):
        self.assertEqual(map_transaction_type_to_enum("ACH WITHDRAWAL"), InvestmentTransactionType.TRANSFER_OUT)

    def test_partial_match_funds_deposited(self):
        self.assertEqual(map_transaction_type_to_enum("FUNDS DEPOSITED"), InvestmentTransactionType.TRANSFER_IN)

    def test_non_transfer_types_unchanged(self):
        self.assertEqual(map_transaction_type_to_enum("BUY"), InvestmentTransactionType.BUY)
        self.assertEqual(map_transaction_type_to_enum("SELL"), InvestmentTransactionType.SELL)
        self.assertEqual(map_transaction_type_to_enum("DIVIDEND"), InvestmentTransactionType.DIVIDEND)
        self.assertEqual(map_transaction_type_to_enum("INTEREST"), InvestmentTransactionType.INTEREST)
        self.assertEqual(map_transaction_type_to_enum("FEE"), InvestmentTransactionType.FEE)


if __name__ == "__main__":
    unittest.main()
