"""Tests for daily-simple-interest math in crud_debt and the
debt_payments.transaction_id ON DELETE SET NULL behavior.
"""
import unittest
from datetime import date, datetime, timedelta
from decimal import Decimal
from uuid import uuid4

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

# Import core to register the SQLite FK-enforcement event listener.
import src.db.core  # noqa: F401
from src.db.core import (
    AccountDB,
    AccountType,
    Base,
    DebtPaymentDB,
    SourceType,
    TransactionDB,
    TransactionType,
    UserDB,
)
from src.crud.crud_debt import (
    _anchor_date_for_loan,
    _compute_daily_interest,
    create_debt_payment,
    current_accrued_interest,
)
from src.models.debt import DebtPaymentCreate


class LoanMathTestBase(unittest.TestCase):
    def setUp(self):
        self.engine = create_engine("sqlite:///:memory:")
        Base.metadata.create_all(self.engine)
        self.Session = sessionmaker(bind=self.engine)
        self.session = self.Session()

        self.user = UserDB(uuid=uuid4(), email="t@x", username="t", password_hash="x")
        self.session.add(self.user)
        self.session.flush()

        # LOAN account: $10,000 at 5% APR
        self.loan = AccountDB(
            uuid=uuid4(),
            user_id=self.user.db_id,
            account_name="Test Loan",
            account_type=AccountType.LOAN,
            institution_name="Acme",
            balance=Decimal("10000.00"),
            interest_rate=Decimal("0.0500"),
            balance_last_updated=datetime(2026, 1, 1),
        )
        self.session.add(self.loan)
        self.session.flush()

        # Checking account (payment source / non-LOAN control)
        self.checking = AccountDB(
            uuid=uuid4(),
            user_id=self.user.db_id,
            account_name="Test Checking",
            account_type=AccountType.CHECKING,
            institution_name="Acme",
            balance=Decimal("5000.00"),
        )
        self.session.add(self.checking)
        self.session.flush()

        self.session.commit()

    def tearDown(self):
        self.session.close()
        Base.metadata.drop_all(self.engine)
        self.engine.dispose()


class TestComputeDailyInterest(unittest.TestCase):
    """Pure math helper, no DB needed."""

    def test_thirty_days_at_five_percent_on_ten_thousand(self):
        # 10,000 * 0.05 * 30 / 365 = 41.0958904...
        result = _compute_daily_interest(
            Decimal("10000.00"), Decimal("0.0500"), 30, Decimal("0.01")
        )
        self.assertEqual(result, Decimal("41.10"))

    def test_sixty_days_doubles_interest(self):
        # Missed-payment scenario — twice the days = ~twice the interest.
        thirty = _compute_daily_interest(
            Decimal("10000.00"), Decimal("0.0500"), 30, Decimal("0.01")
        )
        sixty = _compute_daily_interest(
            Decimal("10000.00"), Decimal("0.0500"), 60, Decimal("0.01")
        )
        # 60-day interest is 82.19, 30-day is 41.10. Ratio ~2.0 (off-by-one rounding ok).
        self.assertAlmostEqual(float(sixty / thirty), 2.0, places=2)
        self.assertEqual(sixty, Decimal("82.19"))

    def test_zero_days(self):
        result = _compute_daily_interest(
            Decimal("10000.00"), Decimal("0.0500"), 0, Decimal("0.01")
        )
        self.assertEqual(result, Decimal("0.00"))

    def test_negative_days_clamped_to_zero(self):
        result = _compute_daily_interest(
            Decimal("10000.00"), Decimal("0.0500"), -5, Decimal("0.01")
        )
        self.assertEqual(result, Decimal("0.00"))


class TestAnchorDateFallback(LoanMathTestBase):
    def test_uses_last_payment_date_when_present(self):
        payment = DebtPaymentDB(
            uuid=uuid4(),
            loan_account_id=self.loan.db_id,
            payment_amount=Decimal("100.00"),
            principal_amount=Decimal("60.00"),
            interest_amount=Decimal("40.00"),
            payment_date=date(2026, 3, 15),
        )
        self.session.add(payment)
        self.session.commit()

        self.assertEqual(_anchor_date_for_loan(self.session, self.loan), date(2026, 3, 15))

    def test_falls_back_to_balance_last_updated(self):
        # No payments — anchor is balance_last_updated (Jan 1, 2026).
        self.assertEqual(
            _anchor_date_for_loan(self.session, self.loan),
            date(2026, 1, 1),
        )

    def test_falls_back_to_created_at_when_no_updates(self):
        self.loan.balance_last_updated = None
        self.loan.created_at = datetime(2025, 6, 15)
        self.session.commit()
        self.assertEqual(
            _anchor_date_for_loan(self.session, self.loan),
            date(2025, 6, 15),
        )

    def test_exclude_payment_id_skips_self(self):
        # update_debt_payment's case: when recomputing for an existing
        # payment, that payment must not be its own anchor.
        first = DebtPaymentDB(
            uuid=uuid4(),
            loan_account_id=self.loan.db_id,
            payment_amount=Decimal("100"),
            principal_amount=Decimal("60"),
            interest_amount=Decimal("40"),
            payment_date=date(2026, 2, 1),
        )
        second = DebtPaymentDB(
            uuid=uuid4(),
            loan_account_id=self.loan.db_id,
            payment_amount=Decimal("100"),
            principal_amount=Decimal("60"),
            interest_amount=Decimal("40"),
            payment_date=date(2026, 3, 1),
        )
        self.session.add_all([first, second])
        self.session.commit()

        anchor = _anchor_date_for_loan(
            self.session, self.loan, exclude_payment_id=second.db_id
        )
        self.assertEqual(anchor, date(2026, 2, 1))


class TestCreateDebtPaymentDailyInterest(LoanMathTestBase):
    def test_monthly_payment_thirty_days_after_anchor(self):
        # balance_last_updated = 2026-01-01, payment_date = 2026-01-31 (30 days).
        # Expected interest = 10000 * 0.05 * 30 / 365 = 41.10 (rounded)
        payment = create_debt_payment(
            self.session,
            user_id=self.user.db_id,
            payment_data=DebtPaymentCreate(
                loan_account_uuid=self.loan.uuid,
                payment_amount=Decimal("200.00"),
                payment_date=date(2026, 1, 31),
            ),
            loan_account_id=self.loan.db_id,
        )
        self.assertEqual(payment.interest_amount, Decimal("41.10"))
        self.assertEqual(payment.principal_amount, Decimal("158.90"))
        # Balance reduced by principal only.
        self.assertEqual(self.loan.balance, Decimal("10000.00") - Decimal("158.90"))

    def test_missed_payment_double_interest(self):
        # Skip January, pay in late February (~60 days after anchor).
        payment = create_debt_payment(
            self.session,
            user_id=self.user.db_id,
            payment_data=DebtPaymentCreate(
                loan_account_uuid=self.loan.uuid,
                payment_amount=Decimal("200.00"),
                payment_date=date(2026, 3, 2),  # 60 days after 2026-01-01
            ),
            loan_account_id=self.loan.db_id,
        )
        # 10000 * 0.05 * 60 / 365 = 82.19
        self.assertEqual(payment.interest_amount, Decimal("82.19"))
        self.assertEqual(payment.principal_amount, Decimal("117.81"))

    def test_second_payment_anchors_off_first(self):
        # First payment Jan 31 (30 days), second Mar 2 (30 days later).
        create_debt_payment(
            self.session,
            user_id=self.user.db_id,
            payment_data=DebtPaymentCreate(
                loan_account_uuid=self.loan.uuid,
                payment_amount=Decimal("200.00"),
                payment_date=date(2026, 1, 31),
            ),
            loan_account_id=self.loan.db_id,
        )
        # Balance is now 10000 - 158.90 = 9841.10
        # Second payment: 9841.10 * 0.05 * 30 / 365 = 40.4427... → 40.44
        second = create_debt_payment(
            self.session,
            user_id=self.user.db_id,
            payment_data=DebtPaymentCreate(
                loan_account_uuid=self.loan.uuid,
                payment_amount=Decimal("200.00"),
                payment_date=date(2026, 3, 2),
            ),
            loan_account_id=self.loan.db_id,
        )
        self.assertEqual(second.interest_amount, Decimal("40.44"))
        self.assertEqual(second.principal_amount, Decimal("159.56"))


class TestCurrentAccruedInterest(LoanMathTestBase):
    def test_returns_zero_for_non_loan_account(self):
        result = current_accrued_interest(self.session, self.checking)
        self.assertEqual(result, Decimal("0.00"))

    def test_returns_zero_when_rate_or_balance_missing(self):
        self.loan.interest_rate = None
        self.session.commit()
        self.assertEqual(
            current_accrued_interest(self.session, self.loan), Decimal("0.00")
        )

    def test_accrues_from_balance_last_updated_when_no_payments(self):
        # 90 days since anchor (Jan 1 → Apr 1).
        # 10000 * 0.05 * 90 / 365 = 123.28
        result = current_accrued_interest(
            self.session, self.loan, as_of=date(2026, 4, 1)
        )
        self.assertEqual(result, Decimal("123.29"))  # 123.2876... rounds to 123.29

    def test_accrues_from_last_payment_when_present(self):
        # Anchor moves forward when a payment exists.
        payment = DebtPaymentDB(
            uuid=uuid4(),
            loan_account_id=self.loan.db_id,
            payment_amount=Decimal("100"),
            principal_amount=Decimal("60"),
            interest_amount=Decimal("40"),
            payment_date=date(2026, 3, 1),
        )
        self.session.add(payment)
        self.session.commit()

        # 31 days from Mar 1 to Apr 1.
        # 10000 * 0.05 * 31 / 365 = 42.47
        result = current_accrued_interest(
            self.session, self.loan, as_of=date(2026, 4, 1)
        )
        self.assertEqual(result, Decimal("42.47"))


class TestDebtPaymentFKSetNull(LoanMathTestBase):
    def test_deleting_linked_transaction_sets_payment_transaction_id_to_null(self):
        # Bank-side checking outflow.
        txn = TransactionDB(
            uuid=uuid4(),
            user_id=self.user.db_id,
            account_id=self.checking.db_id,
            transaction_hash=str(uuid4()),
            source_type=SourceType.MANUAL,
            transaction_date=date(2026, 2, 1),
            amount=Decimal("200.00"),
            transaction_type=TransactionType.PURCHASE,
            description="Loan payment",
        )
        self.session.add(txn)
        self.session.flush()

        payment = DebtPaymentDB(
            uuid=uuid4(),
            loan_account_id=self.loan.db_id,
            payment_source_account_id=self.checking.db_id,
            transaction_id=txn.db_id,
            payment_amount=Decimal("200.00"),
            principal_amount=Decimal("160.00"),
            interest_amount=Decimal("40.00"),
            payment_date=date(2026, 2, 1),
        )
        self.session.add(payment)
        self.session.commit()
        payment_id = payment.db_id

        # Delete the bank-side transaction. With SET NULL, the debt_payment
        # row survives with transaction_id cleared.
        self.session.delete(txn)
        self.session.commit()

        surviving = self.session.query(DebtPaymentDB).filter_by(db_id=payment_id).first()
        self.assertIsNotNone(surviving)
        self.assertIsNone(surviving.transaction_id)
        # Other fields untouched.
        self.assertEqual(surviving.payment_amount, Decimal("200.00"))
        self.assertEqual(surviving.principal_amount, Decimal("160.00"))


if __name__ == "__main__":
    unittest.main()
