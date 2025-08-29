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


**Database TODOs**
- Create a script that can convert all the data in the database tables to a csv/json file and then an upload endpoint that can recieve this csv and populate a brand new database table with the data.
- Create a script that can bulk upload each statment and csv file in the input folder. The accounts associated with each folder can be hardcoded into the script since it will only be meant to quickly upload all of my transaction data. How that value gets hardcoded is the question too.  