"""
Market Data Price Fetching Service

Fetches end-of-day prices for stocks and options using Yahoo Finance.
Supports fallback strategies and error handling for production use.
"""
import yfinance as yf
from typing import Dict, List, Optional, Tuple
from decimal import Decimal
from datetime import datetime, date
import time


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
                print(f"No price data available for {symbol}")
                return None

            # Use most recent closing price
            latest_close = hist['Close'].iloc[-1]

            if latest_close <= 0:
                print(f"Invalid price for {symbol}: {latest_close}")
                return None

            return Decimal(str(round(latest_close, 4)))

        except Exception as e:
            print(f"Error fetching price for {symbol} (attempt {attempt + 1}/{retries}): {str(e)}")
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
                print(f"Option not found: {underlying} {expiration} {strike} {option_type}")
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

            print(f"No valid price for option: {underlying} {expiration} {strike} {option_type}")
            return None

        except Exception as e:
            print(f"Error fetching option price (attempt {attempt + 1}/{retries}): {str(e)}")
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
            print(f"Failed to parse option symbol: {symbol}")
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
        print(f"Fetching price {i+1}/{len(symbols)}: {symbol}")

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
