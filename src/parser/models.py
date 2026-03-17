from pydantic import BaseModel, Field
from typing import List, Optional
from datetime import date
from decimal import Decimal
from enum import Enum

class SecurityType(str, Enum):
    """Type of security for investment transactions"""
    STOCK = "STOCK"
    ETF = "ETF"
    MUTUAL_FUND = "MUTUAL_FUND"
    OPTION = "OPTION"
    FUTURE = "FUTURE"
    BOND = "BOND"
    CRYPTO = "CRYPTO"
    DEPOSIT = "DEPOSIT"
    WITHDRAWAL = "WITHDRAWAL"
    INTEREST = "INTEREST"
    DIVIDEND = "DIVIDEND"
    FEE = "FEE"
    ADJUSTMENT = "ADJUSTMENT"
    OTHER = "OTHER"


# Common ETF tickers — extend as needed
KNOWN_ETFS = frozenset({
    # Broad market
    "SPY", "VOO", "VTI", "IVV", "QQQ", "QQQM", "DIA", "IWM", "VT", "VXUS",
    # Sector
    "XLF", "XLK", "XLE", "XLV", "XLI", "XLU", "XLP", "XLY", "XLC", "XLRE", "XLB", "SOXX",
    # Bond
    "BND", "AGG", "TLT", "IEF", "SHY", "TIP", "LQD", "HYG", "JNK", "VCIT", "VCSH",
    # International
    "EFA", "EEM", "VWO", "IEMG", "VEA", "IXUS", "FNDE",
    # Real estate
    "VNQ", "VNQI", "IYR", "SCHH",
    # Commodity
    "GLD", "SLV", "IAU", "USO", "GDX", "GDXJ",
    # Dividend
    "VYM", "SCHD", "HDV", "DVY", "SDY", "DGRO",
    # Growth/Value
    "VUG", "VTV", "IWF", "IWD", "VOOG", "VOOV",
    # Small/Mid cap
    "IJR", "IJH", "VB", "VO", "MDY",
    # Thematic
    "ARKK", "ARKW", "ARKG", "ARKF", "ARKQ",
    # Leveraged/Inverse (common ones)
    "TQQQ", "SQQQ", "SPXL", "SPXS", "UVXY", "SOXL", "SOXS",
})

# Common mutual fund patterns — tickers ending in X with 5 chars
KNOWN_MUTUAL_FUNDS = frozenset({
    "VTSAX", "VTIAX", "VBTLX", "VFIAX", "VSMPX", "VEXAX",
    "FXAIX", "FSKAX", "FTIHX", "FBNDX",
    "SWTSX", "SWPPX", "SWISX",
})


def classify_security_type(symbol: str, is_option: bool = False) -> SecurityType:
    """Classify a symbol as STOCK, ETF, MUTUAL_FUND, or OPTION."""
    if is_option:
        return SecurityType.OPTION
    upper = symbol.upper()
    if upper in KNOWN_ETFS:
        return SecurityType.ETF
    if upper in KNOWN_MUTUAL_FUNDS or (len(upper) == 5 and upper.endswith("X") and upper.isalpha()):
        return SecurityType.MUTUAL_FUND
    return SecurityType.STOCK


class ParsedTransaction(BaseModel):
    transaction_date: date
    description: str
    amount: Decimal
    transaction_type: str
    is_duplicate: bool = False

class ParsedInvestmentTransaction(BaseModel):
    transaction_date: date
    transaction_type: str
    symbol: Optional[str]
    api_symbol: Optional[str] = None  # Symbol for API calls (yfinance format)
    description: str
    quantity: Optional[Decimal]
    price_per_share: Optional[Decimal]
    total_amount: Decimal
    is_duplicate: bool = False
    security_type: Optional[SecurityType] = None

class ParsedAccountInfo(BaseModel):
    account_number_last4: str

class ParsedData(BaseModel):
    account_info: Optional[ParsedAccountInfo] = None
    transactions: List[ParsedTransaction] = Field(default_factory=list)
    investment_transactions: List[ParsedInvestmentTransaction] = Field(default_factory=list)
