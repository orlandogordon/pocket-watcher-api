"""Tests for the orphan-expiration sweep (#57)."""
import unittest
from datetime import date
from decimal import Decimal
from unittest.mock import patch
from uuid import uuid4

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.db.core import (
    AccountDB,
    AccountType,
    Base,
    InvestmentTransactionDB,
    InvestmentTransactionType,
    UserDB,
)
from src.services.option_expirations import (
    SYNTHETIC_EXPIRATION_DESCRIPTION,
    create_synthetic_expiration,
    find_orphan_expirations,
    sweep,
)


def _seed_investment_account(session):
    user = UserDB(id=uuid4(), email="t@x.com", username="t", password_hash="x")
    session.add(user)
    session.flush()
    account = AccountDB(
        uuid=uuid4(), user_id=user.db_id, account_name="Schwab",
        account_type=AccountType.INVESTMENT, institution_name="Schwab",
        balance=Decimal("0"), initial_cash_balance=Decimal("10000"),
    )
    session.add(account)
    session.commit()
    return user, account


def _buy_option(session, user, account, *, api_symbol, txn_date, qty="1",
                price="4.85"):
    underlying = api_symbol.split("2")[0]
    txn = InvestmentTransactionDB(
        id=uuid4(), user_id=user.db_id, account_id=account.id,
        transaction_hash=str(uuid4()),
        transaction_type=InvestmentTransactionType.BUY,
        symbol=underlying, api_symbol=api_symbol,
        quantity=Decimal(qty), price_per_share=Decimal(price),
        total_amount=Decimal(qty) * Decimal(price) * Decimal(100),
        transaction_date=txn_date, security_type="OPTION",
    )
    session.add(txn)
    session.commit()
    return txn


def _sell_option(session, user, account, *, api_symbol, txn_date, qty="1",
                 price="3.00"):
    underlying = api_symbol.split("2")[0]
    txn = InvestmentTransactionDB(
        id=uuid4(), user_id=user.db_id, account_id=account.id,
        transaction_hash=str(uuid4()),
        transaction_type=InvestmentTransactionType.SELL,
        symbol=underlying, api_symbol=api_symbol,
        quantity=Decimal(qty), price_per_share=Decimal(price),
        total_amount=Decimal(qty) * Decimal(price) * Decimal(100),
        transaction_date=txn_date, security_type="OPTION",
    )
    session.add(txn)
    session.commit()
    return txn


class ExpirationBase(unittest.TestCase):
    def setUp(self):
        self.engine = create_engine("sqlite:///:memory:")
        Base.metadata.create_all(self.engine)
        Session = sessionmaker(bind=self.engine)
        self.session = Session()
        self.user, self.account = _seed_investment_account(self.session)
        self.today = date(2026, 5, 22)

    def tearDown(self):
        self.session.close()


class TestFindOrphanExpirations(ExpirationBase):
    def test_finds_expired_option_with_no_sell(self):
        # SPY $500 put expiring 2024-05-17 — still held, no SELL.
        _buy_option(self.session, self.user, self.account,
                    api_symbol="SPY240517P00500000", txn_date=date(2024, 5, 1))
        with patch("src.services.option_expirations.fetch_stock_price_historical",
                   return_value=Decimal("510")):  # above strike -> OTM put
            orphans = find_orphan_expirations(self.session, today=self.today)
        self.assertEqual(len(orphans), 1)
        self.assertEqual(orphans[0].api_symbol, "SPY240517P00500000")
        self.assertEqual(orphans[0].status, "OTM")
        self.assertEqual(orphans[0].quantity, Decimal("1"))

    def test_skips_when_sell_closed_position(self):
        _buy_option(self.session, self.user, self.account,
                    api_symbol="SPY240517P00500000", txn_date=date(2024, 5, 1))
        _sell_option(self.session, self.user, self.account,
                     api_symbol="SPY240517P00500000", txn_date=date(2024, 5, 10))
        with patch("src.services.option_expirations.fetch_stock_price_historical",
                   return_value=Decimal("510")):
            orphans = find_orphan_expirations(self.session, today=self.today)
        self.assertEqual(orphans, [])

    def test_skips_when_expiration_not_yet_passed(self):
        # Future expiration — not an orphan candidate yet.
        _buy_option(self.session, self.user, self.account,
                    api_symbol="SPY261218P00500000", txn_date=date(2026, 1, 1))
        orphans = find_orphan_expirations(self.session, today=self.today)
        self.assertEqual(orphans, [])

    def test_classifies_itm_put_correctly(self):
        # Put strike $500, underlying $490 on expiry → ITM by $10/share.
        _buy_option(self.session, self.user, self.account,
                    api_symbol="SPY240517P00500000", txn_date=date(2024, 5, 1))
        with patch("src.services.option_expirations.fetch_stock_price_historical",
                   return_value=Decimal("490")):
            orphans = find_orphan_expirations(self.session, today=self.today)
        self.assertEqual(orphans[0].status, "ITM")

    def test_classifies_itm_call_correctly(self):
        _buy_option(self.session, self.user, self.account,
                    api_symbol="AAPL240517C00150000", txn_date=date(2024, 5, 1))
        with patch("src.services.option_expirations.fetch_stock_price_historical",
                   return_value=Decimal("160")):  # above strike -> ITM call
            orphans = find_orphan_expirations(self.session, today=self.today)
        self.assertEqual(orphans[0].status, "ITM")

    def test_unknown_when_underlying_unavailable(self):
        _buy_option(self.session, self.user, self.account,
                    api_symbol="SPY240517P00500000", txn_date=date(2024, 5, 1))
        with patch("src.services.option_expirations.fetch_stock_price_historical",
                   return_value=None):
            orphans = find_orphan_expirations(self.session, today=self.today)
        self.assertEqual(orphans[0].status, "UNKNOWN")


class TestCreateSyntheticExpiration(ExpirationBase):
    def test_creates_expiration_row(self):
        _buy_option(self.session, self.user, self.account,
                    api_symbol="SPY240517P00500000", txn_date=date(2024, 5, 1))
        with patch("src.services.option_expirations.fetch_stock_price_historical",
                   return_value=Decimal("510")):
            orphans = find_orphan_expirations(self.session, today=self.today)

        result = create_synthetic_expiration(self.session, orphans[0])
        self.assertIsNotNone(result)
        self.assertEqual(result.transaction_type, InvestmentTransactionType.EXPIRATION)
        self.assertEqual(result.total_amount, Decimal("0"))
        self.assertEqual(result.transaction_date, date(2024, 5, 17))
        self.assertEqual(result.description, SYNTHETIC_EXPIRATION_DESCRIPTION)
        self.assertEqual(result.api_symbol, "SPY240517P00500000")

    def test_idempotent_does_not_duplicate(self):
        _buy_option(self.session, self.user, self.account,
                    api_symbol="SPY240517P00500000", txn_date=date(2024, 5, 1))
        with patch("src.services.option_expirations.fetch_stock_price_historical",
                   return_value=Decimal("510")):
            orphans = find_orphan_expirations(self.session, today=self.today)

        create_synthetic_expiration(self.session, orphans[0])
        # Second call returns None (skip) and doesn't duplicate.
        second = create_synthetic_expiration(self.session, orphans[0])
        self.assertIsNone(second)

        exp_rows = self.session.query(InvestmentTransactionDB).filter(
            InvestmentTransactionDB.transaction_type == InvestmentTransactionType.EXPIRATION
        ).all()
        self.assertEqual(len(exp_rows), 1)


class TestSweep(ExpirationBase):
    def test_otm_creates_synthetic_expiration(self):
        _buy_option(self.session, self.user, self.account,
                    api_symbol="SPY240517P00500000", txn_date=date(2024, 5, 1))
        with patch("src.services.option_expirations.fetch_stock_price_historical",
                   return_value=Decimal("510")):
            summary = sweep(self.session, dry_run=False, today=self.today)

        self.assertEqual(summary.created, 1)
        self.assertEqual(summary.flagged_itm, 0)

    def test_itm_does_not_create_and_flags(self):
        _buy_option(self.session, self.user, self.account,
                    api_symbol="SPY240517P00500000", txn_date=date(2024, 5, 1))
        with patch("src.services.option_expirations.fetch_stock_price_historical",
                   return_value=Decimal("490")):
            summary = sweep(self.session, dry_run=False, today=self.today)

        self.assertEqual(summary.created, 0)
        self.assertEqual(summary.flagged_itm, 1)
        exp_rows = self.session.query(InvestmentTransactionDB).filter(
            InvestmentTransactionDB.transaction_type == InvestmentTransactionType.EXPIRATION
        ).all()
        self.assertEqual(len(exp_rows), 0)

    def test_unknown_does_not_create_and_flags(self):
        _buy_option(self.session, self.user, self.account,
                    api_symbol="SPY240517P00500000", txn_date=date(2024, 5, 1))
        with patch("src.services.option_expirations.fetch_stock_price_historical",
                   return_value=None):
            summary = sweep(self.session, dry_run=False, today=self.today)

        self.assertEqual(summary.created, 0)
        self.assertEqual(summary.flagged_unknown, 1)

    def test_dry_run_does_not_write(self):
        _buy_option(self.session, self.user, self.account,
                    api_symbol="SPY240517P00500000", txn_date=date(2024, 5, 1))
        with patch("src.services.option_expirations.fetch_stock_price_historical",
                   return_value=Decimal("510")):
            summary = sweep(self.session, dry_run=True, today=self.today)

        self.assertEqual(summary.created, 1)
        exp_rows = self.session.query(InvestmentTransactionDB).filter(
            InvestmentTransactionDB.transaction_type == InvestmentTransactionType.EXPIRATION
        ).all()
        self.assertEqual(len(exp_rows), 0)

    def test_rerun_is_idempotent(self):
        # After the first sweep writes the EXPIRATION, the replay zeroes
        # the position, so the orphan disappears entirely on the second
        # pass. Only one EXPIRATION row exists at the end.
        _buy_option(self.session, self.user, self.account,
                    api_symbol="SPY240517P00500000", txn_date=date(2024, 5, 1))
        with patch("src.services.option_expirations.fetch_stock_price_historical",
                   return_value=Decimal("510")):
            sweep(self.session, dry_run=False, today=self.today)
            second = sweep(self.session, dry_run=False, today=self.today)

        self.assertEqual(second.orphans, [])
        self.assertEqual(second.created, 0)
        exp_rows = self.session.query(InvestmentTransactionDB).filter(
            InvestmentTransactionDB.transaction_type == InvestmentTransactionType.EXPIRATION
        ).all()
        self.assertEqual(len(exp_rows), 1)


if __name__ == "__main__":
    unittest.main()
