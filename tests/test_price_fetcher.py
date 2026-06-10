"""price_fetcher service — yfinance mocked at the boundary.

Exercises the OCC symbol helpers (pure), the stock/option/historical fetch paths
with a faked yf.Ticker, the option bid/ask-midpoint-vs-last pricing strategy,
the stock-vs-option dispatch, and holding-value math. yf and time.sleep are
patched so no network or real delay occurs.
"""
from datetime import date
from decimal import Decimal
from unittest.mock import MagicMock

import pandas as pd
import pytest
from yfinance.exceptions import YFRateLimitError

from src.services import price_fetcher as pf


@pytest.fixture(autouse=True)
def _no_sleep(monkeypatch):
    monkeypatch.setattr(pf.time, "sleep", lambda *a, **k: None)


def _ticker(history_df=None, chain=None):
    t = MagicMock()
    if history_df is not None:
        t.history.return_value = history_df
    if chain is not None:
        t.option_chain.return_value = chain
    return t


def _patch_ticker(monkeypatch, ticker):
    monkeypatch.setattr(pf.yf, "Ticker", MagicMock(return_value=ticker))


# ===== pure OCC helpers =====

def test_parse_option_symbol_call():
    parsed = pf.parse_option_symbol("AAPL250117C00150000")
    assert parsed == {
        "underlying": "AAPL",
        "expiration": "2025-01-17",
        "option_type": "CALL",
        "strike": 150.0,
    }


def test_parse_option_symbol_put_and_short_symbol():
    assert pf.parse_option_symbol("SPY240524P00454000")["option_type"] == "PUT"
    assert pf.parse_option_symbol("AAPL")  is None  # too short


def test_is_option_symbol():
    assert pf.is_option_symbol("AAPL250117C00150000") is True
    assert pf.is_option_symbol("AAPL") is False


# ===== fetch_stock_price =====

def test_fetch_stock_price_uses_latest_close(monkeypatch):
    _patch_ticker(monkeypatch, _ticker(history_df=pd.DataFrame({"Close": [100.0, 101.25]})))
    assert pf.fetch_stock_price("AAPL") == Decimal("101.25")


def test_fetch_stock_price_empty_history_returns_none(monkeypatch):
    _patch_ticker(monkeypatch, _ticker(history_df=pd.DataFrame({"Close": []})))
    assert pf.fetch_stock_price("AAPL") is None


def test_fetch_stock_price_non_positive_returns_none(monkeypatch):
    _patch_ticker(monkeypatch, _ticker(history_df=pd.DataFrame({"Close": [0.0]})))
    assert pf.fetch_stock_price("AAPL") is None


def test_fetch_stock_price_all_retries_fail_returns_none(monkeypatch):
    monkeypatch.setattr(pf.yf, "Ticker", MagicMock(side_effect=RuntimeError("network")))
    assert pf.fetch_stock_price("AAPL", retries=2) is None


# ===== fetch_option_price =====

def _chain(calls=None, puts=None):
    c = MagicMock()
    c.calls = calls if calls is not None else pd.DataFrame({"strike": [], "lastPrice": [], "bid": [], "ask": []})
    c.puts = puts if puts is not None else pd.DataFrame({"strike": [], "lastPrice": [], "bid": [], "ask": []})
    return c


def test_fetch_option_price_uses_midpoint_when_spread_tight(monkeypatch):
    calls = pd.DataFrame({"strike": [150.0], "lastPrice": [3.0], "bid": [2.0], "ask": [2.2]})
    _patch_ticker(monkeypatch, _ticker(chain=_chain(calls=calls)))
    # spread (2.2-2.0)/2.0 = 0.1 < 0.5 -> midpoint (2.0+2.2)/2 = 2.1
    assert pf.fetch_option_price("AAPL", "2025-01-17", 150.0, "CALL") == Decimal("2.1")


def test_fetch_option_price_falls_back_to_last_on_wide_spread(monkeypatch):
    calls = pd.DataFrame({"strike": [150.0], "lastPrice": [3.0], "bid": [1.0], "ask": [5.0]})
    _patch_ticker(monkeypatch, _ticker(chain=_chain(calls=calls)))
    # spread 4.0 >= 0.5 -> ignore midpoint, use last 3.0
    assert pf.fetch_option_price("AAPL", "2025-01-17", 150.0, "CALL") == Decimal("3.0")


def test_fetch_option_price_strike_not_found_returns_none(monkeypatch):
    calls = pd.DataFrame({"strike": [155.0], "lastPrice": [3.0], "bid": [2.0], "ask": [2.2]})
    _patch_ticker(monkeypatch, _ticker(chain=_chain(calls=calls)))
    assert pf.fetch_option_price("AAPL", "2025-01-17", 150.0, "CALL") is None


# ===== fetch_price dispatch =====

def test_fetch_price_dispatches_stock_vs_option(monkeypatch):
    monkeypatch.setattr(pf, "fetch_stock_price", lambda s, **k: Decimal("10"))
    monkeypatch.setattr(pf, "fetch_option_price", lambda **k: Decimal("99"))
    assert pf.fetch_price("AAPL") == Decimal("10")
    assert pf.fetch_price("AAPL250117C00150000") == Decimal("99")


# ===== historical (weekend fallback) =====

def test_historical_falls_back_to_previous_trading_day(monkeypatch):
    empty = pd.DataFrame({"Close": []})
    populated = pd.DataFrame({"Close": [148.5]})
    ticker = MagicMock()
    # First call (target date) empty -> loop finds previous day populated.
    ticker.history.side_effect = [empty, populated]
    _patch_ticker(monkeypatch, ticker)
    assert pf.fetch_stock_price_historical("AAPL", date(2025, 1, 18)) == Decimal("148.5")


# ===== bulk historical fetch + backoff =====

def _hist_df():
    idx = pd.to_datetime([date(2024, 5, 1), date(2024, 5, 2)])
    return pd.DataFrame({"Close": [150.0, 151.5]}, index=idx)


def test_bulk_history_converts_close_by_date(monkeypatch):
    _patch_ticker(monkeypatch, _ticker(history_df=_hist_df()))
    out = pf.fetch_bulk_historical_prices(["AAPL"], date(2024, 5, 1), date(2024, 5, 2))
    assert out["AAPL"] == {date(2024, 5, 1): Decimal("150.0"), date(2024, 5, 2): Decimal("151.5")}


def test_bulk_history_options_skip_to_empty(monkeypatch):
    _patch_ticker(monkeypatch, _ticker(history_df=_hist_df()))
    out = pf.fetch_bulk_historical_prices(["AAPL250117C00150000"], date(2024, 5, 1), date(2024, 5, 2))
    assert out["AAPL250117C00150000"] == {}


def test_bulk_history_empty_is_genuine_no_data(monkeypatch):
    _patch_ticker(monkeypatch, _ticker(history_df=pd.DataFrame({"Close": []})))
    out = pf.fetch_bulk_historical_prices(["DELISTED"], date(2024, 5, 1), date(2024, 5, 2))
    assert out["DELISTED"] == {}


def test_bulk_history_rate_limit_retries_then_raises(monkeypatch):
    ticker = MagicMock()
    ticker.history.side_effect = YFRateLimitError()
    _patch_ticker(monkeypatch, ticker)
    with pytest.raises(pf.PriceFetchError):
        pf.fetch_bulk_historical_prices(["AAPL"], date(2024, 5, 1), date(2024, 5, 2))
    assert ticker.history.call_count == pf._BULK_HISTORY_RETRIES


def test_bulk_history_rate_limit_then_success(monkeypatch):
    ticker = MagicMock()
    ticker.history.side_effect = [YFRateLimitError(), _hist_df()]
    _patch_ticker(monkeypatch, ticker)
    out = pf.fetch_bulk_historical_prices(["AAPL"], date(2024, 5, 1), date(2024, 5, 2))
    assert out["AAPL"][date(2024, 5, 1)] == Decimal("150.0")


def test_bulk_history_other_error_degrades_to_empty(monkeypatch):
    ticker = MagicMock()
    ticker.history.side_effect = RuntimeError("boom")
    _patch_ticker(monkeypatch, ticker)
    out = pf.fetch_bulk_historical_prices(["AAPL"], date(2024, 5, 1), date(2024, 5, 2))
    assert out["AAPL"] == {}


# ===== update_holding_price =====

def test_update_holding_price_returns_value_and_change():
    holding = MagicMock(current_price=None, average_cost_basis=Decimal("100"), quantity=Decimal("10"))
    market_value, change = pf.update_holding_price(holding, Decimal("120"))
    assert market_value == Decimal("1200")
    assert change == Decimal("20")  # 120 - 100 (fell back to cost basis)
    assert holding.current_price == Decimal("120")
