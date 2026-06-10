"""Tests for LOCF + freshness behavior in account_snapshot service."""
import unittest
from datetime import date
from decimal import Decimal
from unittest.mock import patch
from uuid import uuid4

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.db.core import (
    Base,
    UserDB,
    AccountDB,
    AccountType,
    AccountValueHistoryDB,
    InvestmentTransactionDB,
    InvestmentTransactionType,
)
from src.services.account_snapshot import (
    get_net_worth_history,
    get_account_value_history,
    recalculate_account_snapshots,
    _format_symbol_for_review,
    _build_missing_price_review_reason,
    _MAX_REASON_SYMBOLS,
)


def test_apply_position_trade_long_and_short():
    from src.services.account_snapshot import _apply_position_trade as apt
    D = Decimal
    # open long; add long -> weighted avg; reduce long -> basis unchanged
    assert apt(D('0'), D('0'), D('10'), D('5')) == (D('10'), D('5'))
    assert apt(D('10'), D('5'), D('10'), D('7')) == (D('20'), D('6'))
    assert apt(D('20'), D('6'), D('-5'), D('99')) == (D('15'), D('6'))
    # open short (sell-to-open); add to short -> weighted avg premium
    assert apt(D('0'), D('0'), D('-3'), D('8')) == (D('-3'), D('8'))
    assert apt(D('-3'), D('8'), D('-1'), D('4')) == (D('-4'), D('7'))
    # buy-to-close part of short -> basis unchanged; close fully -> flat
    assert apt(D('-4'), D('7'), D('1'), D('99')) == (D('-3'), D('7'))
    assert apt(D('-3'), D('7'), D('3'), D('99')) == (D('0'), D('0'))
    # flips: long->short and short->long take the trade price for the leftover
    assert apt(D('2'), D('5'), D('-5'), D('9')) == (D('-3'), D('9'))
    assert apt(D('-2'), D('5'), D('5'), D('9')) == (D('3'), D('9'))


def test_review_reason_summarizes_long_option_lists():
    # An options-heavy account must not produce an unbounded reason. Past the
    # cap the tail collapses to "(+N more)" — guards the readability cap that
    # accompanies the TEXT-column widening (the old VARCHAR(255) silently
    # dropped these snapshots on Postgres).
    n = _MAX_REASON_SYMBOLS + 5
    symbols = [f"SYM{i:02d}250117C00010000" for i in range(n)]
    reason = _build_missing_price_review_reason(symbols)
    assert reason.startswith("[stale-options] ")
    assert "(+5 more)" in reason
    assert reason.count(" CALL ") == _MAX_REASON_SYMBOLS


def _make_user(session) -> UserDB:
    user = UserDB(
        uuid=uuid4(),
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
        _add_snapshot(self.session, a.db_id, date(2026, 2, 1), 100)
        _add_snapshot(self.session, a.db_id, date(2026, 2, 10), 150)
        _add_snapshot(self.session, b.db_id, date(2026, 2, 5), 50)

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
        _add_snapshot(self.session, a.db_id, date(2026, 1, 20), 500)

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
        _add_snapshot(self.session, a.db_id, date(2026, 2, 1), 100)
        _add_snapshot(self.session, a.db_id, date(2026, 2, 10), 150)
        _add_snapshot(self.session, b.db_id, date(2026, 2, 5), 50)
        _add_snapshot(self.session, b.db_id, date(2026, 2, 10), 75)

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
        _add_snapshot(self.session, asset.db_id, date(2026, 2, 1), 1000)
        _add_snapshot(self.session, cc.db_id, date(2026, 2, 1), 300)
        _add_snapshot(self.session, loan.db_id, date(2026, 2, 1), 200)

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
        _add_snapshot(self.session, a.db_id, date(2026, 2, 1), 100)
        _add_snapshot(self.session, b.db_id, date(2026, 2, 5), 200)

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
        _add_snapshot(self.session, a.db_id, date(2026, 2, 1), 100)
        _add_snapshot(self.session, a.db_id, date(2026, 2, 5), 120)

        out = get_account_value_history(
            self.session, a.db_id, self.user.db_id,
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
        _add_snapshot(self.session, a.db_id, date(2026, 2, 5), 100)

        out = get_account_value_history(
            self.session, a.db_id, self.user.db_id,
            start_date=date(2026, 2, 1), end_date=date(2026, 2, 7),
        )

        self.assertEqual(out[0]["date"], date(2026, 2, 5))
        self.assertEqual(len(out), 3)


class TestMonthlyDownsample(TestLOCFBase):
    """Above the 365-day threshold the daily LOCF series is reduced
    to one point per calendar month, keeping the last day of the
    bucket. See backend todo #41."""

    def test_no_downsample_at_exactly_365_days(self):
        """365-day span is the boundary — stays daily (threshold is > 365)."""
        a = _make_account(self.session, self.user.db_id, "A", AccountType.CHECKING)
        _add_snapshot(self.session, a.db_id, date(2024, 1, 1), 100)

        out = get_net_worth_history(
            self.session, self.user.db_id,
            start_date=date(2024, 1, 1), end_date=date(2024, 12, 31),
        )
        self.assertEqual((date(2024, 12, 31) - date(2024, 1, 1)).days, 365)
        self.assertEqual(len(out), 366)  # both endpoints inclusive

    def test_downsample_above_365_days(self):
        """18-month span → 18 monthly buckets, last day of each month
        within the range."""
        a = _make_account(self.session, self.user.db_id, "A", AccountType.CHECKING)
        _add_snapshot(self.session, a.db_id, date(2024, 1, 1), 100)

        out = get_net_worth_history(
            self.session, self.user.db_id,
            start_date=date(2024, 1, 1), end_date=date(2025, 6, 30),
        )
        # Jan 2024 .. Jun 2025 = 18 months.
        self.assertEqual(len(out), 18)
        # Each kept point is the last day of its month.
        # Jan 2024 → Jan 31 2024; Feb 2024 → Feb 29 2024 (leap); etc.
        self.assertEqual(out[0]["date"], "2024-01-31")
        self.assertEqual(out[1]["date"], "2024-02-29")
        self.assertEqual(out[-1]["date"], "2025-06-30")

    def test_last_of_month_reducer_picks_last_snapshot(self):
        """Snapshots on Jan 5/20/31; the Jan bucket reflects Jan 31's
        balance, not the earlier-in-month values."""
        a = _make_account(self.session, self.user.db_id, "A", AccountType.CHECKING)
        _add_snapshot(self.session, a.db_id, date(2024, 1, 5), 100)
        _add_snapshot(self.session, a.db_id, date(2024, 1, 20), 200)
        _add_snapshot(self.session, a.db_id, date(2024, 1, 31), 350)

        out = get_net_worth_history(
            self.session, self.user.db_id,
            start_date=date(2024, 1, 1), end_date=date(2025, 6, 30),
        )
        jan = next(p for p in out if p["date"].startswith("2024-01"))
        self.assertEqual(jan["date"], "2024-01-31")
        self.assertEqual(jan["net_worth"], 350.0)

    def test_freshness_fields_come_from_kept_point(self):
        """If the kept day (last of month) is carried-forward, the
        bucket should reflect carried-forward freshness — not roll up
        across the bucket."""
        a = _make_account(self.session, self.user.db_id, "A", AccountType.CHECKING)
        _add_snapshot(self.session, a.db_id, date(2024, 1, 15), 100)
        # No snapshot Jan 16..31, so Jan 31 is carried-forward.

        out = get_net_worth_history(
            self.session, self.user.db_id,
            start_date=date(2024, 1, 1), end_date=date(2025, 6, 30),
        )
        jan = next(p for p in out if p["date"].startswith("2024-01"))
        self.assertEqual(jan["date"], "2024-01-31")
        self.assertEqual(jan["accounts_total"], 1)
        self.assertEqual(jan["accounts_fresh"], 0)
        self.assertEqual(jan["oldest_snapshot_date"], date(2024, 1, 15))

    def test_per_account_is_carried_forward_from_kept_point(self):
        """get_account_value_history's bucketed point inherits the
        kept day's is_carried_forward flag."""
        a = _make_account(self.session, self.user.db_id, "A", AccountType.CHECKING)
        _add_snapshot(self.session, a.db_id, date(2024, 1, 15), 100)

        out = get_account_value_history(
            self.session, a.db_id, self.user.db_id,
            start_date=date(2024, 1, 1), end_date=date(2025, 6, 30),
        )
        jan = next(p for p in out if p["date"].month == 1 and p["date"].year == 2024)
        self.assertEqual(jan["date"], date(2024, 1, 31))
        self.assertEqual(jan["balance"], Decimal("100"))
        self.assertTrue(jan["is_carried_forward"])


class TestHoldAtCostValuation(unittest.TestCase):
    """Snapshot recalc values held option contracts at cost basis when no
    market price is available (#57 hold-at-cost). Option quantity is in
    contracts and price is per underlying share, so a held contract is
    worth ``qty * price * 100``; the bulk price fetch is patched to return
    nothing so every holding takes the cost-basis fallback path."""

    OCC = "SPY240517P00500000"  # SPY $500 put, expires 2024-05-17

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

    def _investment_account(self, initial_cash) -> AccountDB:
        acct = AccountDB(
            uuid=uuid4(),
            user_id=self.user.db_id,
            account_name="Brokerage",
            account_type=AccountType.INVESTMENT,
            institution_name="TestBroker",
            balance=Decimal("0"),
            initial_cash_balance=Decimal(str(initial_cash)),
        )
        self.session.add(acct)
        self.session.flush()
        return acct

    def _option_txn(self, account, txn_type, *, qty, price, txn_date, api_symbol=None):
        api_symbol = api_symbol or self.OCC
        quantity = Decimal(str(qty))
        price_per_share = Decimal(str(price))
        txn = InvestmentTransactionDB(
            uuid=uuid4(),
            user_id=self.user.db_id,
            account_id=account.db_id,
            transaction_hash=str(uuid4()),
            transaction_type=txn_type,
            symbol=api_symbol[: api_symbol.index("2")],
            api_symbol=api_symbol,
            quantity=quantity,
            price_per_share=price_per_share,
            total_amount=quantity * price_per_share * Decimal("100"),
            transaction_date=txn_date,
            security_type="OPTION",
        )
        self.session.add(txn)
        self.session.commit()
        return txn

    def _recalc(self, account, start, end):
        with patch(
            "src.services.account_snapshot.fetch_bulk_historical_prices",
            return_value={},
        ):
            recalculate_account_snapshots(
                self.session, account.db_id, start_date=start, end_date=end,
                delay_between_prices=0,
            )

    def _snapshot(self, account, value_date) -> AccountValueHistoryDB:
        return self.session.query(AccountValueHistoryDB).filter(
            AccountValueHistoryDB.account_id == account.db_id,
            AccountValueHistoryDB.value_date == value_date,
        ).first()

    def test_held_option_contributes_qty_price_100(self):
        acct = self._investment_account(1000)
        self._option_txn(acct, InvestmentTransactionType.BUY,
                         qty=1, price=5, txn_date=date(2024, 5, 1))
        self._recalc(acct, date(2024, 5, 1), date(2024, 5, 1))

        snap = self._snapshot(acct, date(2024, 5, 1))
        # 1 contract * $5 * 100 = $500 of securities value.
        self.assertEqual(snap.securities_value, Decimal("500.00"))

    def test_no_phantom_loss_at_buy(self):
        """The key invariant: buying an option moves cash into securities
        of equal value — total unchanged. Guards against silently dropping
        to intrinsic-style (or unmultiplied) valuation."""
        acct = self._investment_account(1000)
        self._option_txn(acct, InvestmentTransactionType.BUY,
                         qty=1, price=5, txn_date=date(2024, 5, 1))
        self._recalc(acct, date(2024, 5, 1), date(2024, 5, 1))

        snap = self._snapshot(acct, date(2024, 5, 1))
        # cash 1000 - 500 spent = 500 cash + 500 securities = 1000 total.
        self.assertEqual(snap.cash_balance, Decimal("500.00"))
        self.assertEqual(snap.securities_value, Decimal("500.00"))
        self.assertEqual(snap.balance, Decimal("1000.00"))

    def test_otm_expiration_drops_total(self):
        acct = self._investment_account(1000)
        self._option_txn(acct, InvestmentTransactionType.BUY,
                         qty=1, price=5, txn_date=date(2024, 5, 1))
        self._option_txn(acct, InvestmentTransactionType.EXPIRATION,
                         qty=1, price=0, txn_date=date(2024, 5, 17))
        self._recalc(acct, date(2024, 5, 16), date(2024, 5, 17))

        before = self._snapshot(acct, date(2024, 5, 16))
        after = self._snapshot(acct, date(2024, 5, 17))
        self.assertEqual(before.balance, Decimal("1000.00"))
        # Expiration zeros the position: securities -> 0, only $500 cash left.
        self.assertEqual(after.securities_value, Decimal("0.00"))
        self.assertEqual(after.balance, Decimal("500.00"))

    def test_multiple_buys_weighted_average_cost(self):
        acct = self._investment_account(2000)
        self._option_txn(acct, InvestmentTransactionType.BUY,
                         qty=1, price=5, txn_date=date(2024, 5, 1))
        self._option_txn(acct, InvestmentTransactionType.BUY,
                         qty=1, price=7, txn_date=date(2024, 5, 2))
        self._recalc(acct, date(2024, 5, 2), date(2024, 5, 2))

        snap = self._snapshot(acct, date(2024, 5, 2))
        # avg cost (5 + 7)/2 = 6; 2 contracts * $6 * 100 = $1200.
        self.assertEqual(snap.securities_value, Decimal("1200.00"))

    def test_sell_preserves_average_cost(self):
        acct = self._investment_account(2000)
        self._option_txn(acct, InvestmentTransactionType.BUY,
                         qty=2, price=5, txn_date=date(2024, 5, 1))
        self._option_txn(acct, InvestmentTransactionType.SELL,
                         qty=1, price=6, txn_date=date(2024, 5, 2))
        self._recalc(acct, date(2024, 5, 2), date(2024, 5, 2))

        snap = self._snapshot(acct, date(2024, 5, 2))
        # 1 contract remains at the unchanged $5 avg cost: 1 * 5 * 100 = $500.
        self.assertEqual(snap.securities_value, Decimal("500.00"))

    def test_sell_to_open_short_is_negative_securities(self):
        # Writing an option (sell-to-open, no prior holding) is a short: it adds
        # premium to cash and contributes negative securities value at cost.
        acct = self._investment_account(1000)
        self._option_txn(acct, InvestmentTransactionType.SELL,
                         qty=1, price=5, txn_date=date(2024, 5, 1))
        self._recalc(acct, date(2024, 5, 1), date(2024, 5, 1))
        snap = self._snapshot(acct, date(2024, 5, 1))
        self.assertEqual(snap.securities_value, Decimal("-500.00"))  # -1 * 5 * 100
        self.assertEqual(snap.cash_balance, Decimal("1500.00"))      # 1000 + 500 premium
        self.assertEqual(snap.balance, Decimal("1000.00"))

    def test_buy_to_close_flattens_short(self):
        acct = self._investment_account(1000)
        self._option_txn(acct, InvestmentTransactionType.SELL,
                         qty=1, price=5, txn_date=date(2024, 5, 1))
        self._option_txn(acct, InvestmentTransactionType.BUY,
                         qty=1, price=2, txn_date=date(2024, 5, 2))
        self._recalc(acct, date(2024, 5, 1), date(2024, 5, 2))
        snap = self._snapshot(acct, date(2024, 5, 2))
        self.assertEqual(snap.securities_value, Decimal("0.00"))     # closed
        self.assertEqual(snap.cash_balance, Decimal("1300.00"))      # 1000 + 500 - 200

    def test_vertical_spread_values_at_net_debit(self):
        # Long $140 + short $145 (same day/qty) = bull call debit spread.
        acct = self._investment_account(1000)
        self._option_txn(acct, InvestmentTransactionType.BUY, qty=1, price=9,
                         txn_date=date(2024, 5, 1), api_symbol="PTON240517C00140000")
        self._option_txn(acct, InvestmentTransactionType.SELL, qty=1, price=7,
                         txn_date=date(2024, 5, 1), api_symbol="PTON240517C00145000")
        self._recalc(acct, date(2024, 5, 1), date(2024, 5, 1))
        snap = self._snapshot(acct, date(2024, 5, 1))
        # 900 long - 700 short = 200 net debit; cash 1000 - 900 + 700 = 800.
        self.assertEqual(snap.securities_value, Decimal("200.00"))
        self.assertEqual(snap.cash_balance, Decimal("800.00"))
        self.assertEqual(snap.balance, Decimal("1000.00"))

    def test_cost_basis_fallback_flags_stale_options(self):
        acct = self._investment_account(1000)
        self._option_txn(acct, InvestmentTransactionType.BUY,
                         qty=1, price=5, txn_date=date(2024, 5, 1))
        self._recalc(acct, date(2024, 5, 1), date(2024, 5, 1))

        snap = self._snapshot(acct, date(2024, 5, 1))
        self.assertTrue(snap.needs_review)
        self.assertIn("[stale-options]", snap.review_reason)
        self.assertIn("SPY", snap.review_reason)

    def test_successful_recalc_clears_stale_missing_price_flag(self):
        # First pass: empty prices (transient fetch failure) flags the row.
        acct = self._investment_account(1000)
        self._option_txn(acct, InvestmentTransactionType.BUY,
                         qty=1, price=5, txn_date=date(2024, 5, 1))
        self._recalc(acct, date(2024, 5, 1), date(2024, 5, 1))
        snap = self._snapshot(acct, date(2024, 5, 1))
        self.assertTrue(snap.needs_review)

        # Second pass: prices resolve, so the stale flag and reason clear.
        with patch(
            "src.services.account_snapshot.fetch_bulk_historical_prices",
            return_value={self.OCC: {date(2024, 5, 1): Decimal("6")}},
        ):
            recalculate_account_snapshots(
                self.session, acct.db_id,
                start_date=date(2024, 5, 1), end_date=date(2024, 5, 1),
                delay_between_prices=0,
            )
        self.session.refresh(snap)
        self.assertFalse(snap.needs_review)
        self.assertIsNone(snap.review_reason)

    def test_recalc_propagates_price_fetch_error(self):
        # A persistent rate-limit must escape recalc (not be swallowed into a
        # silent cost-basis fallback) so the backfill job can fail loudly.
        from src.services.price_fetcher import PriceFetchError
        acct = self._investment_account(1000)
        self._option_txn(acct, InvestmentTransactionType.BUY,
                         qty=1, price=5, txn_date=date(2024, 5, 1))
        with patch(
            "src.services.account_snapshot.fetch_bulk_historical_prices",
            side_effect=PriceFetchError("Rate limited fetching SPY after 4 attempts"),
        ):
            with self.assertRaises(PriceFetchError):
                recalculate_account_snapshots(
                    self.session, acct.db_id,
                    start_date=date(2024, 5, 1), end_date=date(2024, 5, 1),
                    delay_between_prices=0,
                )


class TestFormatSymbolForReview(unittest.TestCase):
    """The snapshot FYI on the data-health inbox renders the
    review_reason verbatim. For option holdings the OCC symbol
    (e.g. AAPL250117C00150000) is illegible — the formatter rewrites
    it as a human-readable contract identifier."""

    def test_stock_symbol_passes_through(self):
        self.assertEqual(_format_symbol_for_review("AAPL"), "AAPL")
        self.assertEqual(_format_symbol_for_review("BRK.B"), "BRK.B")

    def test_call_option_formats_with_strike_and_exp(self):
        # AAPL 2025-01-17 $150 call
        self.assertEqual(
            _format_symbol_for_review("AAPL250117C00150000"),
            "AAPL 2025-01-17 CALL $150",
        )

    def test_put_option_formats(self):
        # SPY 2025-06-20 $400 put
        self.assertEqual(
            _format_symbol_for_review("SPY___250620P00400000"),
            "SPY___ 2025-06-20 PUT $400",
        )

    def test_fractional_strike_strips_trailing_zeros(self):
        # 152.50 should render as $152.5, not $152.50 or $152.500
        self.assertEqual(
            _format_symbol_for_review("AAPL250117C00152500"),
            "AAPL 2025-01-17 CALL $152.5",
        )

    def test_malformed_option_symbol_falls_back_to_raw(self):
        # Long enough to fool is_option_symbol() but parse_option_symbol
        # returns None — fall back to the original string rather than
        # crashing the snapshot recalc.
        bogus = "X" * 15 + "C" + "X" * 8
        self.assertEqual(_format_symbol_for_review(bogus), bogus)


if __name__ == "__main__":
    unittest.main()
