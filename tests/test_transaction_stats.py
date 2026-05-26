"""Tests for transfer handling in get_transaction_stats.

A transfer is real money crossing a single account's boundary, so when
exactly one account is filtered it counts toward income/expense. Across all
or multiple accounts a transfer is internal movement that nets to zero and
stays excluded.
"""
import unittest
from decimal import Decimal
from unittest.mock import MagicMock, patch

from src.db.core import TransactionType
from src.models.transaction import TransactionFilter


class FakeTxn:
    def __init__(self, db_id, transaction_type, amount):
        self.db_id = db_id
        self.transaction_type = transaction_type
        self.amount = Decimal(str(amount))
        self.category_id = 1


def _run_stats(txns, filters):
    from src.crud import crud_transaction
    db = MagicMock()
    db.query.return_value.filter.return_value.all.return_value = txns
    with patch.object(crud_transaction, "_apply_transaction_filters", side_effect=lambda q, f: q), \
         patch.object(crud_transaction, "get_refund_adjustments", return_value=({}, set())):
        return crud_transaction.get_transaction_stats(db, user_id=1, filters=filters)


def _sample_txns():
    return [
        FakeTxn(1, TransactionType.CREDIT, 100),
        FakeTxn(2, TransactionType.PURCHASE, 40),
        FakeTxn(3, TransactionType.TRANSFER_IN, 500),
        FakeTxn(4, TransactionType.TRANSFER_OUT, 200),
    ]


class TestSingleAccountTransfers(unittest.TestCase):
    """Exactly one account selected: transfers fold into income/expense."""

    def test_transfer_in_counts_as_income_and_out_as_expense(self):
        stats = _run_stats(_sample_txns(), TransactionFilter(account_ids=[1]))
        self.assertEqual(stats.total_income, Decimal('600'))   # 100 credit + 500 transfer in
        self.assertEqual(stats.total_expenses, Decimal('240'))  # 40 purchase + 200 transfer out
        self.assertEqual(stats.net, Decimal('360'))
        self.assertEqual(stats.total_count, 4)


class TestMultiAndAllAccountTransfers(unittest.TestCase):
    """Zero or 2+ accounts selected: transfers stay excluded (net to zero)."""

    def test_all_accounts_no_filter_excludes_transfers(self):
        stats = _run_stats(_sample_txns(), None)
        self.assertEqual(stats.total_income, Decimal('100'))
        self.assertEqual(stats.total_expenses, Decimal('40'))
        self.assertEqual(stats.net, Decimal('60'))
        self.assertEqual(stats.total_count, 4)

    def test_empty_account_filter_excludes_transfers(self):
        stats = _run_stats(_sample_txns(), TransactionFilter())
        self.assertEqual(stats.total_income, Decimal('100'))
        self.assertEqual(stats.total_expenses, Decimal('40'))

    def test_multiple_accounts_exclude_transfers(self):
        stats = _run_stats(_sample_txns(), TransactionFilter(account_ids=[1, 2]))
        self.assertEqual(stats.total_income, Decimal('100'))
        self.assertEqual(stats.total_expenses, Decimal('40'))
        self.assertEqual(stats.net, Decimal('60'))


if __name__ == "__main__":
    unittest.main()
