from src.parser import (
    amex,
    tdbank,
    amzn_syf,
    schwab,
    tdameritrade,
    ameriprise,
    venmo,
    # empower and fidelity are not ready yet
)


# A mapping from the institution string to the corresponding parser module
PARSER_MAPPING = {
    "amex": amex,
    "tdbank": tdbank,
    "amzn-synchrony": amzn_syf,
    "schwab": schwab,
    "tdameritrade": tdameritrade,
    "ameriprise": ameriprise,
    "venmo": venmo,
}
