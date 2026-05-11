"""Tests for LOCF + freshness behavior in account_snapshot service."""
import unittest
from datetime import date
from decimal import Decimal
from uuid import uuid4

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.db.core import (
    Base,
    UserDB,
    AccountDB,
    AccountType,
    AccountValueHistoryDB,
)
from src.services.account_snapshot import (
    get_net_worth_history,
    get_account_value_history,
)


def _make_user(session) -> UserDB:
    user = UserDB(
        id=uuid4(),
        email="test@example.com",
        username="test",
        password_hash="x",
    )
    session.add(user)
    session.flush()
    return user


def _make_account(session, user_id: int, name: str, account_type: AccountType) -> AccountDB:
    acct = AccountDB(
        uuid=uuid4(),
        user_id=user_id,
        account_name=name,
        account_type=account_type,
        institution_name="TestBank",
        balance=Decimal("0"),
    )
    session.add(acct)
    session.flush()
    return acct


def _add_snapshot(session, account_id: int, value_date: date, balance, **extra) -> AccountValueHistoryDB:
    snap = AccountValueHistoryDB(
        uuid=uuid4(),
        account_id=account_id,
        value_date=value_date,
        balance=Decimal(str(balance)),
        **extra,
    )
    session.add(snap)
    session.flush()
    return snap


class TestLOCFBase(unittest.TestCase):
    def setUp(self):
        self.engine = create_engine("sqlite:///:memory:")
        Base.metadata.create_all(self.engine)
        self.Session = sessionmaker(bind=self.engine)
        self.session = self.Session()
        self.user = _make_user(self.session)

    def tearDown(self):
        self.session.close()
        Base.metadata.drop_all(self.engine)
        self.engine.dispose()


class TestSparseSnapshotDates(TestLOCFBase):
    """A has snapshots on Feb 1 and Feb 10; B has a snapshot on Feb 5 only.
    Every date in [Feb 1, Feb 15] should sum using LOCF."""

    def test_locf_fills_per_account(self):
        a = _make_account(self.session, self.user.db_id, "A", AccountType.CHECKING)
        b = _make_account(self.session, self.user.db_id, "B", AccountType.CHECKING)
        _add_snapshot(self.session, a.id, date(2026, 2, 1), 100)
        _add_snapshot(self.session, a.id, date(2026, 2, 10), 150)
        _add_snapshot(self.session, b.id, date(2026, 2, 5), 50)

        out = get_net_worth_history(
            self.session, self.user.db_id,
            start_date=date(2026, 2, 1), end_date=date(2026, 2, 15),
        )
        by_date = {p["date"]: p for p in out}

        self.assertEqual(len(out), 15)
        # Feb 1: only A contributes (100). B not yet observed.
        self.assertEqual(by_date["2026-02-01"]["net_worth"], 100.0)
        self.assertEqual(by_date["2026-02-01"]["accounts_total"], 1)
        # Feb 4: A still 100, B not yet observed.
        self.assertEqual(by_date["2026-02-04"]["net_worth"], 100.0)
        self.assertEqual(by_date["2026-02-04"]["accounts_total"], 1)
        # Feb 5: A carries 100, B fresh 50.
        self.assertEqual(by_date["2026-02-05"]["net_worth"], 150.0)
        self.assertEqual(by_date["2026-02-05"]["accounts_total"], 2)
        # Feb 9: A still 100, B carries 50.
        self.assertEqual(by_date["2026-02-09"]["net_worth"], 150.0)
        # Feb 10: A jumps to 150, B carries 50.
        self.assertEqual(by_date["2026-02-10"]["net_worth"], 200.0)
        # Feb 15: A carries 150, B carries 50.
        self.assertEqual(by_date["2026-02-15"]["net_worth"], 200.0)


class TestLookbackBeforeStart(TestLOCFBase):
    """Account's only snapshot is before start_date; it should still
    contribute its last-known balance to every date in the window."""

    def test_lookback(self):
        a = _make_account(self.session, self.user.db_id, "A", AccountType.SAVINGS)
        _add_snapshot(self.session, a.id, date(2026, 1, 20), 500)

        out = get_net_worth_history(
            self.session, self.user.db_id,
            start_date=date(2026, 2, 1), end_date=date(2026, 2, 5),
        )

        self.assertEqual(len(out), 5)
        for p in out:
            self.assertEqual(p["net_worth"], 500.0)
            self.assertEqual(p["accounts_total"], 1)
            self.assertEqual(p["accounts_fresh"], 0)
            self.assertEqual(p["oldest_snapshot_date"], date(2026, 1, 20))


class TestFreshnessFields(TestLOCFBase):
    def test_fresh_total_and_oldest(self):
        a = _make_account(self.session, self.user.db_id, "A", AccountType.CHECKING)
        b = _make_account(self.session, self.user.db_id, "B", AccountType.CHECKING)
        _add_snapshot(self.session, a.id, date(2026, 2, 1), 100)
        _add_snapshot(self.session, a.id, date(2026, 2, 10), 150)
        _add_snapshot(self.session, b.id, date(2026, 2, 5), 50)
        _add_snapshot(self.session, b.id, date(2026, 2, 10), 75)

        out = get_net_worth_history(
            self.session, self.user.db_id,
            start_date=date(2026, 2, 1), end_date=date(2026, 2, 15),
        )
        by_date = {p["date"]: p for p in out}

        # Feb 5: A is stale (Feb 1), B is fresh (Feb 5).
        p5 = by_date["2026-02-05"]
        self.assertEqual(p5["accounts_total"], 2)
        self.assertEqual(p5["accounts_fresh"], 1)
        self.assertEqual(p5["oldest_snapshot_date"], date(2026, 2, 1))

        # Feb 10: both fresh — oldest_snapshot_date should be None.
        p10 = by_date["2026-02-10"]
        self.assertEqual(p10["accounts_total"], 2)
        self.assertEqual(p10["accounts_fresh"], 2)
        self.assertIsNone(p10["oldest_snapshot_date"])

        # Feb 15: both carry forward from Feb 10.
        p15 = by_date["2026-02-15"]
        self.assertEqual(p15["accounts_fresh"], 0)
        self.assertEqual(p15["oldest_snapshot_date"], date(2026, 2, 10))


class TestLiabilityNegation(TestLOCFBase):
    def test_credit_card_subtracts_under_locf(self):
        asset = _make_account(self.session, self.user.db_id, "Asset", AccountType.CHECKING)
        cc = _make_account(self.session, self.user.db_id, "CC", AccountType.CREDIT_CARD)
        loan = _make_account(self.session, self.user.db_id, "Loan", AccountType.LOAN)
        _add_snapshot(self.session, asset.id, date(2026, 2, 1), 1000)
        _add_snapshot(self.session, cc.id, date(2026, 2, 1), 300)
        _add_snapshot(self.session, loan.id, date(2026, 2, 1), 200)

        out = get_net_worth_history(
            self.session, self.user.db_id,
            start_date=date(2026, 2, 1), end_date=date(2026, 2, 3),
        )

        # 1000 - 300 - 200 = 500 on every date (all carried forward after Feb 1).
        for p in out:
            self.assertEqual(p["net_worth"], 500.0)


class TestExcludeUntilFirstObservation(TestLOCFBase):
    """accounts_total grows as accounts gain their first snapshot."""

    def test_account_not_yet_observed_excluded(self):
        a = _make_account(self.session, self.user.db_id, "A", AccountType.CHECKING)
        b = _make_account(self.session, self.user.db_id, "B", AccountType.CHECKING)
        _add_snapshot(self.session, a.id, date(2026, 2, 1), 100)
        _add_snapshot(self.session, b.id, date(2026, 2, 5), 200)

        out = get_net_worth_history(
            self.session, self.user.db_id,
            start_date=date(2026, 2, 1), end_date=date(2026, 2, 5),
        )
        by_date = {p["date"]: p for p in out}

        # Feb 1-4: only A counted.
        for d in ["2026-02-01", "2026-02-02", "2026-02-03", "2026-02-04"]:
            self.assertEqual(by_date[d]["accounts_total"], 1)
            self.assertEqual(by_date[d]["net_worth"], 100.0)
        # Feb 5: both counted.
        self.assertEqual(by_date["2026-02-05"]["accounts_total"], 2)
        self.assertEqual(by_date["2026-02-05"]["net_worth"], 300.0)


class TestPerAccountLOCF(TestLOCFBase):
    def test_gaps_filled_with_carried_forward_flag(self):
        a = _make_account(self.session, self.user.db_id, "A", AccountType.CHECKING)
        _add_snapshot(self.session, a.id, date(2026, 2, 1), 100)
        _add_snapshot(self.session, a.id, date(2026, 2, 5), 120)

        out = get_account_value_history(
            self.session, a.id, self.user.db_id,
            start_date=date(2026, 2, 1), end_date=date(2026, 2, 7),
        )

        self.assertEqual(len(out), 7)
        # Feb 1: fresh, balance 100.
        self.assertEqual(out[0]["date"], date(2026, 2, 1))
        self.assertEqual(out[0]["balance"], Decimal("100"))
        self.assertFalse(out[0]["is_carried_forward"])
        # Feb 2-4: carried forward from Feb 1.
        for i in range(1, 4):
            self.assertEqual(out[i]["balance"], Decimal("100"))
            self.assertTrue(out[i]["is_carried_forward"])
        # Feb 5: fresh, balance 120.
        self.assertEqual(out[4]["date"], date(2026, 2, 5))
        self.assertEqual(out[4]["balance"], Decimal("120"))
        self.assertFalse(out[4]["is_carried_forward"])
        # Feb 6-7: carried forward from Feb 5.
        for i in range(5, 7):
            self.assertEqual(out[i]["balance"], Decimal("120"))
            self.assertTrue(out[i]["is_carried_forward"])

    def test_start_before_first_snapshot_clamps(self):
        """Per-account: don't fabricate $0 points before account has any data."""
        a = _make_account(self.session, self.user.db_id, "A", AccountType.CHECKING)
        _add_snapshot(self.session, a.id, date(2026, 2, 5), 100)

        out = get_account_value_history(
            self.session, a.id, self.user.db_id,
            start_date=date(2026, 2, 1), end_date=date(2026, 2, 7),
        )

        self.assertEqual(out[0]["date"], date(2026, 2, 5))
        self.assertEqual(len(out), 3)


if __name__ == "__main__":
    unittest.main()
