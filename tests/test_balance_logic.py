"""Tests for TRANSFER_IN/TRANSFER_OUT balance logic."""
import unittest
from decimal import Decimal
from unittest.mock import MagicMock, patch

from src.db.core import AccountType, TransactionType
from src.services.account_snapshot import _reverse_balance_for_type


class FakeAccount:
    def __init__(self, account_type, balance):
        self.db_id = 1
        self.account_type = account_type
        self.balance = Decimal(str(balance))


class FakeTransaction:
    def __init__(self, transaction_type, amount):
        self.transaction_type = transaction_type
        self.amount = Decimal(str(amount))


class TestForwardBalanceCreditCard(unittest.TestCase):
    """Credit card: positive balance = debt owed."""

    def _run(self, txn_type, starting_balance, amount):
        from src.crud.crud_transaction import update_account_balance_from_transaction
        account = FakeAccount(AccountType.CREDIT_CARD, starting_balance)
        txn = FakeTransaction(txn_type, amount)
        db = MagicMock()
        captured = {}
        with patch("src.crud.crud_transaction.update_account_balance",
                    side_effect=lambda db, aid, nb: captured.update(balance=nb)):
            update_account_balance_from_transaction(db, account, txn)
        return captured['balance']

    def test_transfer_in_reduces_debt(self):
        result = self._run(TransactionType.TRANSFER_IN, 1000, 500)
        self.assertEqual(result, Decimal('500'))

    def test_transfer_out_increases_debt(self):
        result = self._run(TransactionType.TRANSFER_OUT, 1000, 200)
        self.assertEqual(result, Decimal('1200'))


class TestForwardBalanceChecking(unittest.TestCase):
    """Checking/savings: standard balance."""

    def _run(self, txn_type, starting_balance, amount):
        from src.crud.crud_transaction import update_account_balance_from_transaction
        account = FakeAccount(AccountType.CHECKING, starting_balance)
        txn = FakeTransaction(txn_type, amount)
        db = MagicMock()
        captured = {}
        with patch("src.crud.crud_transaction.update_account_balance",
                    side_effect=lambda db, aid, nb: captured.update(balance=nb)):
            update_account_balance_from_transaction(db, account, txn)
        return captured['balance']

    def test_transfer_in_increases_balance(self):
        result = self._run(TransactionType.TRANSFER_IN, 1000, 500)
        self.assertEqual(result, Decimal('1500'))

    def test_transfer_out_decreases_balance(self):
        result = self._run(TransactionType.TRANSFER_OUT, 1000, 300)
        self.assertEqual(result, Decimal('700'))


class TestReverseBalanceCreditCard(unittest.TestCase):
    def test_transfer_in_undoes_debt_reduction(self):
        result = _reverse_balance_for_type(
            AccountType.CREDIT_CARD, TransactionType.TRANSFER_IN,
            Decimal('500'), Decimal('500')
        )
        self.assertEqual(result, Decimal('1000'))

    def test_transfer_out_undoes_debt_increase(self):
        result = _reverse_balance_for_type(
            AccountType.CREDIT_CARD, TransactionType.TRANSFER_OUT,
            Decimal('200'), Decimal('1200')
        )
        self.assertEqual(result, Decimal('1000'))


class TestReverseBalanceChecking(unittest.TestCase):
    def test_transfer_in_undoes_incoming(self):
        result = _reverse_balance_for_type(
            AccountType.CHECKING, TransactionType.TRANSFER_IN,
            Decimal('500'), Decimal('1500')
        )
        self.assertEqual(result, Decimal('1000'))

    def test_transfer_out_undoes_outgoing(self):
        result = _reverse_balance_for_type(
            AccountType.CHECKING, TransactionType.TRANSFER_OUT,
            Decimal('300'), Decimal('700')
        )
        self.assertEqual(result, Decimal('1000'))


class TestForwardReverseSymmetry(unittest.TestCase):
    """Verify that reverse(forward(balance)) == balance for all transfer types."""

    def _test_symmetry(self, account_type, txn_type):
        from src.crud.crud_transaction import update_account_balance_from_transaction
        original_balance = Decimal('1000')
        amount = Decimal('250')

        account = FakeAccount(account_type, original_balance)
        txn = FakeTransaction(txn_type, amount)
        db = MagicMock()
        captured = {}
        with patch("src.crud.crud_transaction.update_account_balance",
                    side_effect=lambda db, aid, nb: captured.update(balance=nb)):
            update_account_balance_from_transaction(db, account, txn)

        new_balance = captured['balance']
        reversed_balance = _reverse_balance_for_type(account_type, txn_type, amount, new_balance)
        self.assertEqual(reversed_balance, original_balance)

    def test_cc_transfer_in(self):
        self._test_symmetry(AccountType.CREDIT_CARD, TransactionType.TRANSFER_IN)

    def test_cc_transfer_out(self):
        self._test_symmetry(AccountType.CREDIT_CARD, TransactionType.TRANSFER_OUT)

    def test_checking_transfer_in(self):
        self._test_symmetry(AccountType.CHECKING, TransactionType.TRANSFER_IN)

    def test_checking_transfer_out(self):
        self._test_symmetry(AccountType.CHECKING, TransactionType.TRANSFER_OUT)

    def test_savings_transfer_in(self):
        self._test_symmetry(AccountType.SAVINGS, TransactionType.TRANSFER_IN)

    def test_savings_transfer_out(self):
        self._test_symmetry(AccountType.SAVINGS, TransactionType.TRANSFER_OUT)


class TestBudgetStatsExclusion(unittest.TestCase):
    """TRANSFER_IN and TRANSFER_OUT should not be counted as income or expense."""

    def test_transfer_types_excluded_from_income_expense(self):
        income_types = {TransactionType.CREDIT, TransactionType.DEPOSIT, TransactionType.INTEREST}
        expense_types = {TransactionType.PURCHASE, TransactionType.WITHDRAWAL, TransactionType.FEE}

        self.assertNotIn(TransactionType.TRANSFER_IN, income_types)
        self.assertNotIn(TransactionType.TRANSFER_IN, expense_types)
        self.assertNotIn(TransactionType.TRANSFER_OUT, income_types)
        self.assertNotIn(TransactionType.TRANSFER_OUT, expense_types)


if __name__ == "__main__":
    unittest.main()
