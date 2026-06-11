from src.parser import (
    amex,
    tdbank,
    amzn_syf,
    schwab,
    tdameritrade,
    ameriprise,
    # empower and fidelity are not ready yet
)

# venmo/cashapp are intentionally NOT registered (#77): they're pass-throughs,
# not accounts. Their parser modules stay (src/parser/{venmo,cashapp}.py) — the
# local enrich_p2p script reuses their column logic — but the upload flow no
# longer accepts them as institutions.

# A mapping from the institution string to the corresponding parser module
PARSER_MAPPING = {
    "amex": amex,
    "tdbank": tdbank,
    "amzn-synchrony": amzn_syf,
    "schwab": schwab,
    "tdameritrade": tdameritrade,
    "ameriprise": ameriprise,
}
