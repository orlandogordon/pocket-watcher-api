# Parser Update Plan: Duplicate Transaction Handling

To ensure consistent and robust transaction importing, the duplicate handling logic needs to be implemented for the remaining parsers. This involves updating each parser to correctly identify and manage duplicate transactions within a single file import by appending a counter to the description.

The following parsers need to be updated:

### 1. Schwab (`src/parser/schwab.py`)
- **Task:** Implement duplicate handling for investment transactions in the `parse_csv` function.
- **Context Needed:** The content of `src/parser/schwab.py`.
- **Notes:** The `parse_statement` (PDF) function is a placeholder and does not need changes. The logic must be adapted for `ParsedInvestmentTransaction` objects.

### 2. TD Ameritrade (`src/parser/tdameritrade.py`)
- **Task:** Implement duplicate handling for transactions. This will likely involve updating both `parse_statement` (PDF) and `parse_csv` functions if they exist.
- **Context Needed:** The content of `src/parser/tdameritrade.py`.

### 3. Ameriprise (`src/parser/ameriprise.py`)
- **Task:** Implement duplicate handling for transactions. This will likely involve updating both `parse_statement` (PDF) and `parse_csv` functions if they exist.
- **Context Needed:** The content of `src/parser/ameriprise.py`.

**Action Plan:**
For each parser, the following steps will be taken:
1. Read the content of the parser file.
2. Add the necessary imports (`itertools.groupby`).
3. Define a helper function (`_handle_duplicates` or `_handle_investment_duplicates`) to manage duplicate descriptions.
4. Integrate the helper function into the main parsing logic before the `ParsedData` object is returned.


**Database Schema TODOs**
In the transactions table: 
- We have a uuid where transaction hash is but I thought this would be the unique idenitifier with identifying data points like description date and amount. 
- We also probably don't need the posted_date, raw_data_json and needs review columns. 
- We also are uploading the parsed_description only to the description table when it should be in the parsed_description table and MAYBE in the description table too. 
- I also want to include a "potential_duplicate" tag automatically on any transactions that were deemed duplicate but still uploaded since they came from the same file. 