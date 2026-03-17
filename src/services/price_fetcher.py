"""
Market Data Price Fetching Service

Fetches end-of-day prices for stocks and options using Yahoo Finance.
Supports fallback strategies and error handling for production use.
"""
import yfinance as yf
from typing import Dict, List, Optional, Tuple
from decimal import Decimal
from datetime import datetime, date, timedelta
import time

from src.logging_config import get_logger

logger = get_logger(__name__)


class PriceFetchError(Exception):
    """Raised when price fetching fails"""
    pass


def parse_option_symbol(symbol: str) -> Optional[Dict[str, any]]:
    """
    Parse OCC option symbol format: AAPL250117C00150000
    Returns: {
        'underlying': 'AAPL',
        'expiration': '2025-01-17',
        'option_type': 'CALL' or 'PUT',
        'strike': 150.00
    }
    """
    if len(symbol) < 15:
        return None

    try:
        # Extract components
        underlying = symbol[:-15]  # Everything before the last 15 chars
        date_str = symbol[-15:-9]  # YYMMDD
        option_type = symbol[-9]    # C or P
        strike_str = symbol[-8:]    # Strike price * 1000

        # Parse date (YYMMDD -> YYYY-MM-DD)
        year = 2000 + int(date_str[:2])
        month = int(date_str[2:4])
        day = int(date_str[4:6])
        expiration = date(year, month, day)

        # Parse strike
        strike = float(strike_str) / 1000

        # Parse option type
        opt_type = "CALL" if option_type.upper() == "C" else "PUT"

        return {
            'underlying': underlying,
            'expiration': expiration.isoformat(),
            'option_type': opt_type,
            'strike': strike
        }
    except Exception:
        return None


def is_option_symbol(symbol: str) -> bool:
    """Check if symbol is an option (OCC format) or stock"""
    return len(symbol) >= 15 and (symbol[-9] in ['C', 'P'])


def fetch_stock_price(symbol: str, retries: int = 3) -> Optional[Decimal]:
    """
    Fetch current/latest stock price from Yahoo Finance.
    Uses closing price from most recent trading day.
    """
    for attempt in range(retries):
        try:
            ticker = yf.Ticker(symbol)

            # Get most recent day of data
            hist = ticker.history(period="5d")  # Get last 5 days to ensure we have data

            if hist.empty:
                logger.warning(f"No price data available for {symbol}")
                return None

            # Use most recent closing price
            latest_close = hist['Close'].iloc[-1]

            if latest_close <= 0:
                logger.warning(f"Invalid price for {symbol}: {latest_close}")
                return None

            return Decimal(str(round(latest_close, 4)))

        except Exception as e:
            logger.warning(f"Error fetching price for {symbol} (attempt {attempt + 1}/{retries}): {str(e)}")
            if attempt < retries - 1:
                time.sleep(1)  # Wait before retry
            continue

    return None


def fetch_option_price(
    underlying: str,
    expiration: str,
    strike: float,
    option_type: str,
    retries: int = 3
) -> Optional[Decimal]:
    """
    Fetch option price using bid/ask midpoint or last price.

    Args:
        underlying: Stock symbol (e.g., 'AAPL')
        expiration: ISO date string (e.g., '2025-01-17')
        strike: Strike price (e.g., 150.00)
        option_type: 'CALL' or 'PUT'

    Returns:
        Option price as Decimal, or None if not found
    """
    for attempt in range(retries):
        try:
            ticker = yf.Ticker(underlying)

            # Get option chain for the expiration date
            chain = ticker.option_chain(expiration)

            # Select calls or puts
            options = chain.calls if option_type.upper() == 'CALL' else chain.puts

            # Find the specific strike
            option = options[options['strike'] == strike]

            if option.empty:
                logger.warning(f"Option not found: {underlying} {expiration} {strike} {option_type}")
                return None

            # Get pricing data
            last = option['lastPrice'].iloc[0]
            bid = option['bid'].iloc[0]
            ask = option['ask'].iloc[0]

            # Strategy: Use midpoint if reasonable, otherwise use last
            if bid > 0 and ask > 0:
                spread_pct = (ask - bid) / bid if bid > 0 else 1.0

                # Use midpoint if spread is reasonable (< 50%)
                if spread_pct < 0.5:
                    price = (bid + ask) / 2
                    return Decimal(str(round(price, 4)))

            # Fallback to last price
            if last > 0:
                return Decimal(str(round(last, 4)))

            logger.warning(f"No valid price for option: {underlying} {expiration} {strike} {option_type}")
            return None

        except Exception as e:
            logger.warning(f"Error fetching option price (attempt {attempt + 1}/{retries}): {str(e)}")
            if attempt < retries - 1:
                time.sleep(1)
            continue

    return None


def fetch_price(symbol: str) -> Optional[Decimal]:
    """
    Universal price fetcher - automatically detects if symbol is stock or option.

    Args:
        symbol: Stock ticker (e.g., 'AAPL') or OCC option symbol (e.g., 'AAPL250117C00150000')

    Returns:
        Current price as Decimal, or None if fetch fails
    """
    # Check if option
    if is_option_symbol(symbol):
        parsed = parse_option_symbol(symbol)
        if not parsed:
            logger.error(f"Failed to parse option symbol: {symbol}")
            return None

        return fetch_option_price(
            underlying=parsed['underlying'],
            expiration=parsed['expiration'],
            strike=parsed['strike'],
            option_type=parsed['option_type']
        )
    else:
        # It's a stock
        return fetch_stock_price(symbol)


def fetch_stock_price_historical(
    symbol: str,
    target_date: date,
    retries: int = 3
) -> Optional[Decimal]:
    """
    Fetch historical stock price for a specific date.

    Args:
        symbol: Stock ticker (e.g., 'AAPL')
        target_date: Date to fetch price for

    Returns:
        Closing price as Decimal, or None if not available

    Handles:
        - Weekends/holidays: Falls back to previous trading day
        - Delisted stocks: Returns None
        - Network errors: Retries with exponential backoff
    """
    for attempt in range(retries):
        try:
            ticker = yf.Ticker(symbol)

            # Try fetching data for target_date
            hist = ticker.history(start=target_date, end=target_date + timedelta(days=1))

            if hist.empty:
                # Market closed - find previous trading day (search back up to 7 days)
                logger.debug(f"No data for {symbol} on {target_date}, finding previous trading day")

                for days_back in range(1, 8):
                    fallback_date = target_date - timedelta(days=days_back)
                    hist = ticker.history(start=fallback_date, end=fallback_date + timedelta(days=1))

                    if not hist.empty:
                        logger.debug(f"Using {fallback_date} price for {target_date}")
                        return Decimal(str(round(hist['Close'].iloc[-1], 4)))

                logger.warning(f"Could not find historical price for {symbol} near {target_date}")
                return None

            return Decimal(str(round(hist['Close'].iloc[-1], 4)))

        except Exception as e:
            logger.warning(f"Error fetching historical price for {symbol} on {target_date} (attempt {attempt + 1}/{retries}): {str(e)}")
            if attempt < retries - 1:
                time.sleep(1)
            continue

    return None


def fetch_option_price_historical(
    underlying: str,
    expiration: str,
    strike: float,
    option_type: str,
    target_date: date,
    retries: int = 3
) -> Optional[Decimal]:
    """
    Fetch historical option price for a specific date.

    Args:
        underlying: Stock symbol (e.g., 'AAPL')
        expiration: ISO date string (e.g., '2025-01-17')
        strike: Strike price (e.g., 150.00)
        option_type: 'CALL' or 'PUT'
        target_date: Date to fetch price for

    Returns:
        Historical option price as Decimal
        None if option didn't exist on target_date or no data available
        Decimal('0.00') if option expired before target_date

    Note: Option historical data is less reliable than stock data.
    May need to fall back to intrinsic value calculation or cost basis.
    """
    expiration_date = datetime.strptime(expiration, '%Y-%m-%d').date()

    # Check if option expired before target_date
    if expiration_date < target_date:
        logger.debug(f"Option {underlying} {expiration} expired before {target_date}")
        return Decimal('0.00')  # Expired options have no value

    # Check if option existed yet (rough heuristic: listed ~45 days before expiration)
    listing_date = expiration_date - timedelta(days=45)
    if target_date < listing_date:
        logger.debug(f"Option {underlying} {expiration} not yet listed on {target_date}")
        return None  # Use cost basis as fallback

    # Try fetching historical option price
    # Note: yfinance option historical data is very limited
    # For now, return None and use cost basis as fallback
    # TODO: Implement option historical price fetching if data source becomes available
    logger.debug(f"Historical option pricing not implemented, using cost basis for {underlying} {expiration}")
    return None


def fetch_price_historical(
    symbol: str,
    target_date: date
) -> Optional[Decimal]:
    """
    Universal historical price fetcher.
    Auto-detects stock vs option symbol and routes to appropriate function.

    Args:
        symbol: Stock ticker (e.g., 'AAPL') or option OCC format (e.g., 'AAPL250117C00150000')
        target_date: Date to fetch price for

    Returns:
        Historical price as Decimal, or None if unavailable
    """
    # Check if option (OCC format)
    if is_option_symbol(symbol):
        parsed = parse_option_symbol(symbol)
        if not parsed:
            logger.error(f"Failed to parse option symbol: {symbol}")
            return None

        return fetch_option_price_historical(
            underlying=parsed['underlying'],
            expiration=parsed['expiration'],
            strike=parsed['strike'],
            option_type=parsed['option_type'],
            target_date=target_date
        )
    else:
        # Stock symbol
        return fetch_stock_price_historical(symbol, target_date)


def fetch_bulk_historical_prices(
    symbols: List[str],
    start_date: date,
    end_date: date
) -> Dict[str, Dict[date, Decimal]]:
    """
    Fetch historical prices for multiple symbols across date range.

    Performance optimization: Instead of fetching each symbol for each day
    (days × symbols API calls), fetch all days for each symbol at once
    (symbols API calls).

    Args:
        symbols: List of stock tickers or option OCC symbols
        start_date: First date to fetch
        end_date: Last date to fetch

    Returns:
        {
            'AAPL': {
                date(2024, 10, 1): Decimal('150.25'),
                date(2024, 10, 2): Decimal('151.00'),
                ...
            },
            'TSLA': { ... }
        }

    Performance:
        - 180 days × 20 symbols = 3,600 API calls (without bulk)
        - 20 symbols = 20 API calls (with bulk)
        - 180x reduction!
    """
    results = {}

    for symbol in symbols:
        # Separate stock vs option handling
        if is_option_symbol(symbol):
            # Options: Historical data limited, may need to fetch daily
            # For now, return empty dict (falls back to cost basis)
            results[symbol] = {}
        else:
            # Stocks: Fetch all dates at once
            try:
                ticker = yf.Ticker(symbol)
                hist = ticker.history(start=start_date, end=end_date + timedelta(days=1))

                if hist.empty:
                    logger.warning(f"No historical data for {symbol} in range {start_date} to {end_date}")
                    results[symbol] = {}
                    continue

                # Convert to dict[date -> price]
                symbol_prices = {}
                for idx, row in hist.iterrows():
                    price_date = idx.date()
                    symbol_prices[price_date] = Decimal(str(round(row['Close'], 4)))

                results[symbol] = symbol_prices

            except Exception as e:
                logger.error(f"Error fetching bulk historical prices for {symbol}: {str(e)}")
                results[symbol] = {}

        # Rate limiting between symbols
        time.sleep(0.5)

    return results


def fetch_bulk_prices(symbols: List[str], delay: float = 0.5) -> Dict[str, Optional[Decimal]]:
    """
    Fetch prices for multiple symbols with rate limiting.

    Args:
        symbols: List of stock or option symbols
        delay: Delay between requests in seconds (rate limiting)

    Returns:
        Dict mapping symbol -> price (None if fetch failed)
    """
    results = {}

    for i, symbol in enumerate(symbols):
        logger.debug(f"Fetching price {i+1}/{len(symbols)}: {symbol}")

        price = fetch_price(symbol)
        results[symbol] = price

        # Rate limiting
        if i < len(symbols) - 1:
            time.sleep(delay)

    return results


def update_holding_price(holding, new_price: Decimal) -> Tuple[Decimal, Decimal]:
    """
    Update a holding's current price and return the new market value.

    Args:
        holding: InvestmentHoldingDB object
        new_price: New price per share

    Returns:
        (market_value, price_change) tuple
    """
    old_price = holding.current_price or holding.average_cost_basis
    holding.current_price = new_price
    holding.last_price_update = datetime.utcnow()

    market_value = holding.quantity * new_price if holding.quantity else Decimal('0')
    price_change = new_price - old_price if old_price else Decimal('0')

    return market_value, price_change


# Example usage / testing
if __name__ == "__main__":
    # Test stock price
    print("Testing stock price fetch:")
    aapl_price = fetch_stock_price("AAPL")
    print(f"AAPL: ${aapl_price}")

    # Test option price
    print("\nTesting option price fetch:")
    option_price = fetch_option_price(
        underlying="AAPL",
        expiration="2025-01-17",
        strike=150.0,
        option_type="CALL"
    )
    print(f"AAPL Jan 17 2025 $150 Call: ${option_price}")

    # Test OCC symbol parsing and fetch
    print("\nTesting OCC symbol fetch:")
    occ_symbol = "AAPL250117C00150000"
    parsed = parse_option_symbol(occ_symbol)
    print(f"Parsed: {parsed}")
    price = fetch_price(occ_symbol)
    print(f"Price: ${price}")
