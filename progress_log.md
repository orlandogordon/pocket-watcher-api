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
- Create a script that can convert all the data in the database tables to a csv/json file and then an upload endpoint (or import method) that can recieve this csv/json and populate a brand new database table with the data.
- Update the print statements to be proper logging and create a new table for storing error messages as non-compromising errors are encountered
- Confirm csv uploads/parsers work properly and do not duplicate statement transactions that are already uploaded

**Other Open Items**
- The "Create Category" endpoint is not including the parent category_id when given
- Create Financial Plan takes in a target amount but doesn't store it in the db. Instead it is storing a monthly income value that i don't find to be as useful. Let's have a conversation about this one before implementing.
- Financial Plan entires bulk upload endpoint does not work.
- The endpoint for assigning a tag to a transaction takes in the sequential db_id for the transaction rather than the public (uuid). We should be using the uuid here right?
- The bulk transaction-tag assignment endpoint is broken.
- Transaction Relationship update and deletion endpoints seem to be missing
- The debt payment creation endpoint is not populating principal/interest amount data and remaining balance data. The endpoint also is not checking to make sure the account_id provided is a loan account (and maybe a credit_card) and returning an error if it is not. 
- Create Investment Transactions endpoint is not updating the account value based on the transaction processed.
alembic revision --autogenerate -m "Initial database schema"

## Current Todo List (Updated September 22, 2025)

### Completed Items
- ✅ Fix Create Category endpoint to include parent category_id (RESOLVED: User error with parameter name)
- ✅ Review Financial Plan target amount vs monthly income storage issue (RESOLVED: Redesigned entire system)
- ✅ Redesign Financial Plan system for multi-month planning with income/expenses/one-time costs (COMPLETED)
- ✅ Update Financial Plan router endpoints for new system (COMPLETED)
- ✅ Create database migration for new Financial Plan schema (COMPLETED)
- ✅ **Fix debt payment creation endpoint** (COMPLETED - October 9, 2025)
  - Added account type validation (LOAN or CREDIT_CARD only)
  - Implemented automatic principal/interest calculation using loan interest rate
  - Calculate and update remaining balance after payment
  - Update account balance when debt payment is created
- ✅ **Fix Create Investment Transactions endpoint to update account value** (COMPLETED - October 9, 2025)
  - Investment transactions now update holdings properly
  - Account balance updated via end-of-day snapshot system (see below)
- ✅ **Implement Historical Net Worth Tracking System** (COMPLETED - October 9, 2025)
  - Created `account_value_history` table for daily snapshots across all account types
  - Added options metadata support to `investment_holdings` table (underlying, strike, expiration, type)
  - Integrated Yahoo Finance API (yfinance) for live market price fetching
  - Implemented price fetcher service supporting stocks and options (OCC format)
  - Created account snapshot service with account-type-specific calculations:
    - Investment accounts: cost basis, unrealized gains/losses
    - Loan accounts: principal/interest paid YTD
    - Other accounts: current balance tracking
  - Added API endpoints for net worth and account value history
  - Created EOD (end-of-day) job script for automated daily snapshots
  - Investment account balances now calculated from live market prices via snapshots

- ✅ **Fix bulk transaction-tag assignment endpoint** (COMPLETED - October 10, 2025)
  - Created `BulkTagRequest` Pydantic model for proper request validation
  - Updated `/tags/transactions/bulk-tag` endpoint to use structured request body
  - Added proper response with tagged count
  - Handles duplicate tagging gracefully (skips already-tagged transactions)
  - Created comprehensive test script (test_bulk_tag.py)
- ✅ **Add missing Transaction Relationship update and deletion endpoints** (COMPLETED - October 10, 2025)
  - Implemented `update_transaction_relationship()` in crud_transaction.py
  - Implemented `delete_transaction_relationship()` in crud_transaction.py
  - Created `TransactionRelationshipUpdate` Pydantic model for partial updates
  - Added PUT `/transactions/relationships/{relationship_id}` endpoint
  - Added DELETE `/transactions/relationships/{relationship_id}` endpoint
  - Enhanced POST endpoint documentation with relationship types (REFUNDS, OFFSETS, SPLITS, FEES_FOR, REVERSES)
  - All endpoints include proper ownership validation and error handling
  - Created comprehensive test script (test_transaction_relationships.py)

- ✅ **Complete Schwab Investment Transaction Parser** (COMPLETED - October 11, 2025)
  - Implemented full PDF parsing with date-tracking logic (transactions without dates use previous date)
  - Fixed quantity and price_per_share extraction from PDF columns (previously set to None)
  - Added duplicate handling using `_handle_investment_duplicates()` function
  - Integrated with CSV parser duplicate handling
  - Parser now correctly extracts: date, type, symbol, description, quantity, price, amount
  - Test results: 15 transactions from first PDF, 4 from second PDF with proper duplicate flagging

- ✅ **Add Missing Columns to investment_transactions Table** (COMPLETED - October 11, 2025)
  - Added `user_id` column with foreign key to users table
  - Added `transaction_hash` column for deduplication (SHA-256 hash)
  - Added `needs_review` boolean field to flag duplicate transactions
  - Created unique constraint on (user_id, transaction_hash)
  - Added indexes for performance (user_id + transaction_date)
  - Generated and applied Alembic migration (fresh initial migration)
  - Schema now matches regular transactions table for consistency

- ✅ **Fix Investment Holdings and Balance Updates** (COMPLETED - October 11, 2025)
  - Fixed Schwab PDF parser to extract quantity and price from their own columns
  - Holdings now automatically created for BUY/REINVESTMENT transactions
  - Holdings properly track quantity and average cost basis
  - Each transaction linked to holding via holding_id
  - Fixed transaction type mapping order (map before checking if holding should be created)
  - Implemented Option 1 for account balance: holdings track cost basis, account balance updated by separate snapshot job only
  - Verified working: QQQM (5 @ $206.75), SOXX (2 @ $215.865), VOO (2 @ $531.00)

- ✅ **Complete Schwab & TD Ameritrade Parsers with yfinance Integration** (COMPLETED - October 26, 2025)
  - Added `api_symbol` field to models and database for yfinance API calls
  - Implemented OCC format for options: TICKER + YYMMDD + C/P + 8-digit strike
  - Symbol field now contains underlying ticker only (not full option contract)
  - Normalized transaction types across all parsers: BUY, SELL, DIVIDEND, INTEREST, FEE, TRANSFER, OTHER
  - Security_type only set for BUY/SELL transactions (STOCK or OPTION)
  - Fixed symbol extraction for both parsers to return ticker only
  - Created database migration for api_symbol column
  - Created comprehensive **Parser Development Guide** (`PARSER_DEVELOPMENT_GUIDE.md`) with:
    * Design expectations and field requirements
    * Step-by-step table-based parser development
    * Visual debugging techniques with PNG overlay generation
    * Common issues and solutions
    * Complete test script templates
    * Validation checklist and shell commands
    * Reference to working implementations

### Remaining Open Items
1. **Audit all API endpoints to use UUIDs instead of db_ids in URL paths for security and consistency**
   - Need to examine: categories, tags, financial plans, budgets, debt, investments, transaction relationships
   - Ensure all public endpoints use UUIDs for path parameters
   - Keep db_ids internal for database relationships only
   - Update Postman collection after changes

2. **Complete Remaining Investment Transaction Parsers** (NEARLY COMPLETE)
   - ✅ Schwab parser (`src/parser/schwab_new.py`) - COMPLETED (October 26, 2025)
   - ✅ TD Ameritrade parser (`src/parser/tdameritrade_new.py`) - COMPLETED (October 26, 2025)
   - 🔄 Ameriprise parser (`src/parser/ameriprise.py`) - PENDING

   **TD Ameritrade Parser - Table-Based Rewrite (October 12, 2025)**

   Created new table-based parser (`src/parser/tdameritrade_new.py`) inspired by TD Bank parser approach.

   **Key Technical Insights:**
   - TD Ameritrade PDFs use a fixed-column table layout where text alignment is critical
   - Header row analysis revealed exact x-coordinate ranges for each column:
     * Trade Date: 32.6 - 52.1
     * Settle Date: 79.7 - 99.2
     * Type: 116.6 - 137.6
     * Cash Activity: 160.3 - 220.8
     * Description: 291.4 - 340.9
     * CUSIP: 440.2 - 467.7 (skipped in our extraction)
     * Quantity: 521.3 - 557.8
     * Price: 601.0 - 623.0
     * Amount: 662.9 - 696.9
     * Balance: 736.6 - 771.1

   **Vertical Column Boundaries (Final):**
   Fixed boundaries based on header analysis: [20, 65, 110, 150, 290, 470, 580, 640, 715, line['x1']]
   - Boundary placement is critical - too far left/right causes word splitting
   - Example issue: boundary at 256 split "Purchased" into "Purchas" and "ed"
   - Solution: boundary at 290 (just before Description starts at 291.4) captures full text

   **Multi-line Transaction Handling:**
   - Transactions span multiple visual lines in PDF:
     * Line 1: Main transaction data (date, type, company name, quantity, price, amount)
     * Line 2: Option contract details (e.g., "SPY Dec 21 20 394.0 C TO OPEN")
     * Lines 3-4: Commission/Fee and Regulatory Fee
   - Table extraction automatically captures all lines within cell's bounding box
   - Symbol extraction: Use second line of description (option contract), strip "TO OPEN"/"TO CLOSE"
   - Descriptions kept as-is with all lines intact (including commission/fee info per user request)

   **Table Building Approach:**
   - Collect horizontal lines per page by detecting date patterns: `^\d{2}/\d{2}/\d{2}`
   - Skip commission/fee lines when collecting (they appear inside transaction cells)
   - Build separate table for each page (pdfplumber limitation - tables cannot span pages)
   - Each table uses same fixed vertical boundaries across all pages

   **Major Issue Resolved - Row Skipping:**
   - Initial extraction: 147 rows from 14 tables
   - Original parsing: Only 23 transactions (110 skipped due to Commission/Fee check!)
   - Problem: Description column included commission/fee text from lines 3-4
   - Code was skipping entire transaction if "Commission" or "Fee" appeared in description
   - Solution: Removed the skip condition - descriptions kept as-is per user requirement
   - **Result: Now parsing 133 out of 147 rows successfully**

   **Remaining Issues (14 rows still skipped):**
   - 13 rows: "Trade" in date column (these are header rows bleeding into table data)
   - 1 row: No amount value
   - Problem: Header rows from multi-page tables being extracted as data rows
   - **NEXT STEP: Fix horizontal line collection to exclude header rows or skip rows with "Trade" in date column during parsing**

   **Test Results:**
   - 2020 PDF: 133 transactions parsed (vs 23 before fix)
   - Symbols correctly extracted: "AAPL Dec 18 20 125.0 C", "SPY Dec 21 20 370.0 C", "TSLA Dec 18 20 650.0 C"
   - Transaction types correct: "Margin Buy - Securities Purchased", "Margin Sell - Securities Sold"
   - Multi-line descriptions preserved with commission/fee info
   - Quantities, prices, amounts parsing correctly

   **Files Created:**
   - `src/parser/tdameritrade_new.py` - New table-based parser
   - `test_tda_new_parser.py` - Test script with first/last 10 transactions display
   - `analyze_header_coordinates.py` - Script to extract exact column x-coordinates
   - Various debug scripts for troubleshooting

   **TD Ameritrade Parser - Debugging and Refinement (October 24, 2025)**

   Created comprehensive debugging script to visualize table extraction and validate parser logic.

   **Issues Identified and Resolved:**

   1. **Header Row Contamination (FIXED)**
      - Problem: Date range headers like "12/01/20 - 12/31/20" were matching date regex and being parsed as transactions
      - Solution: Added date range pattern filter `^\d{2}/\d{2}/\d{2}\s*-\s*\d{2}/\d{2}/\d{2}` to skip these headers
      - Result: Eliminated 14 false transactions from being parsed

   2. **Incomplete Multi-line Descriptions (FIXED)**
      - Problem: Last transaction on each page was getting cut off, missing "Regulatory Fee" line
      - Initial padding: 15 pixels - insufficient for 4-line descriptions
      - Solution: Implemented smart padding system:
        * Pages with "Closing Balance" marker: Use marker position - 5px as boundary
        * Other pages: Adaptive padding of min(avg_row_height * 2.5, 60px)
        * Default padding increased from 15px → 60px
      - Result: All multi-line descriptions now fully captured

   3. **Footer Text Contamination (FIXED)**
      - Problem: On final page (page 18), 60px padding was capturing footer text
      - Solution: Detect and store "Closing Balance" y-position as end marker
      - Implementation: Use end marker - 5px as final boundary instead of fixed padding
      - Result: Clean extraction stopping exactly at account activity boundary

   **Debug Tools Created:**
   - `debug_raw_table_extraction.py` - Comprehensive debugging script that:
     * Extracts raw table data before filtering
     * Outputs detailed text log with all extraction decisions
     * Generates CSV with all extracted rows
     * Creates PNG visualizations showing table boundaries overlaid on PDF pages
     * Color-coded visualization: Red = cell boundaries, Blue = horizontal lines, Green = vertical lines
   - `test_tda_parser_output.py` - Parser validation script that:
     * Runs actual parser code
     * Outputs results to both TXT and CSV formats
     * Shows transaction count, duplicates, and full details

   **Final Parser Statistics:**
   - Successfully parsing 134 transactions from 2020 test PDF
   - All transactions have complete multi-line descriptions
   - Date range headers filtered out
   - Footer text excluded
   - Duplicate handling implemented

   **Known Issues - Symbol Parsing (TO BE ADDRESSED):**

   Current symbol extraction logic assumes option contracts with symbol on line 2:
   ```python
   # Current logic - looks at second line for option contracts
   if len(lines) > 1 and lines[1].strip():
       symbol = lines[1].strip()  # "SPY Dec 21 20 394.0 C"
   ```

   **Problems:**
   1. **Long Stock Names**: If company name wraps to 2+ lines, option contract info is on line 3+, not line 2
      - Example: "ADVANCED MICRO DEVICES INC -" (line 1) + "AMD Dec 24 20 101.0 C TO OPEN" (line 2)
      - Current code captures line 2 correctly
      - But if name is longer, option details move to line 3, we miss it

   2. **Regular Stock Purchases**: Non-option transactions have symbol on line 1, not line 2
      - Example: "AIRBNB INC ABNB" (line 1) + "COM CL A" (line 2)
      - Current code looks at line 2, gets "COM CL A" instead of "ABNB"
      - Should extract "ABNB" from line 1

   3. **Non-Security Transactions**: Interest, deposits, adjustments don't have symbols
      - Example: "INTEREST CREDIT -" / "ACH IN -" / "Courtesy Adjustment"
      - Symbol should be None/null for these

   **Proposed Solution:**
   - Add `security_type` field to distinguish: STOCK, OPTION, DEPOSIT, INTEREST, FEE, ADJUSTMENT
   - Determine security type from `transaction_type` field:
     * "Buy/Sell - Securities Purchased/Sold" → Check for option contract format
     * "Funds Deposited/Disbursed" → DEPOSIT/WITHDRAWAL (no symbol)
     * "Div/Int - Income/Expense" → INTEREST/DIVIDEND (no symbol)
     * "Journal - Other" → ADJUSTMENT (no symbol)
   - Symbol extraction logic:
     * OPTION: Search all description lines for pattern "SYMBOL MMM DD YY PRICE.0 C/P"
     * STOCK: Extract ticker from first line (first all-caps word after company name)
     * Other types: Set symbol to None

   **TD Ameritrade Parser - Symbol Parsing and Classification (October 24, 2025)**

   ✅ **COMPLETED:**
   1. Added `SecurityType` enum to `src/parser/models.py`:
      - STOCK, OPTION, DEPOSIT, WITHDRAWAL, INTEREST, DIVIDEND, FEE, ADJUSTMENT (later changed to OTHER)
   2. Added `security_type` field to `ParsedInvestmentTransaction` model (optional)
   3. Implemented `_classify_security_type()` function:
      - Classifies transactions based on transaction_type and description
      - Fixed Pinterest/interest substring bug by checking transaction type first
      - Uses regex `\binterest\b` for whole-word matching
      - Looks for "div" or "int" in transaction type, then checks description for "dividend" vs "interest"
      - Defaults to INTEREST if can't determine from description
      - Changed ADJUSTMENT → OTHER per user request
   4. Implemented improved `_extract_symbol()` function:
      - Only extracts symbols for STOCK/OPTION security types
      - OPTION: Searches all description lines for pattern "SYMBOL MMM DD YY PRICE.0 C/P"
      - STOCK: Extracts ticker from first line (last all-caps word)
      - Returns None for non-security transactions
   5. Updated parser to use new classification and extraction functions
   6. Updated duplicate handling to preserve security_type field
   7. Created test script: `test_tda_parser_output.py` with security_type in output

   **Next Steps for TD Ameritrade:**
   1. **Add normalized transaction types** (PENDING)
      - Implement `_normalize_transaction_type()` function
      - Map to standard types: BUY, SELL, DIVIDEND, INTEREST, FEE, TRANSFER, OTHER
   2. **Update security_type logic** (PENDING)
      - Only set security_type for BUY/SELL transactions
      - Leave security_type as None for all other transaction types
   3. Test parser with both 2020 and 2023 PDFs
   4. Verify quantity and price_per_share are being extracted correctly
   5. Test with upload endpoint and verify holdings are created/updated

   **Schwab Parser - Table-Based Rewrite (October 24, 2025)**

   Created new table-based Schwab parser (`src/parser/schwab_new.py`) to properly handle options transactions and multi-line descriptions.

   **Why Table-Based Approach:**
   - Original line-based parser (`schwab.py`) couldn't handle multi-line option formats
   - Options have Symbol/CUSIP like "SPY05/17/2024 PUT" on line 1, "500.00P" on line 2
   - Commission/fee info on additional lines below each transaction
   - Table-based extraction ensures all related lines are captured in one cell

   **Key Technical Implementation:**

   1. **Column Header Analysis:**
      - Analyzed exact x-coordinates of column headers: Date(18.2), Category(52.3), Action(108.7), CUSIP(196.3), Description(271.9), Quantity(476.1), perShare(539.2), Interest(593.5), Amount(673.9), Gain/Loss(752)
      - Created `analyze_schwab_headers.py` to extract precise positions
      - Visual debugging with `debug_schwab_visual.py` to overlay table boundaries on PDF images

   2. **Final Vertical Boundaries (user-tweaked for alignment):**
      ```python
      [16, 45, 98, 178, 252, 442, 512, 570, 630, 712, line['x1']]
      ```

   3. **Date Tracking Logic:**
      - First transaction of a day has date value (MM/DD format)
      - Subsequent transactions on same day have empty date column
      - Parser tracks `current_date` and uses it for transactions without dates
      - Statement year extracted from first page (pattern: "May1-31,2024")

   4. **Transaction Line Detection:**
      - Lines starting with date pattern: `^\d{2}/\d{2}\s+`
      - Lines starting with category: Purchase, Sale, Buy, Sell, Interest, Dividend, etc.
      - Skips Commission/Fee continuation lines

   5. **Normalized Transaction Types:**
      - Implemented `_normalize_transaction_type()` function
      - Maps Schwab categories to standard types: BUY, SELL, DIVIDEND, INTEREST, FEE, TRANSFER, OTHER

   6. **Symbol Extraction for Options:**
      - `_extract_symbol()` function handles multi-line option formats
      - Combines line 1 (SPY05/17/2024 PUT) + line 2 (500.00P)
      - Outputs format: "SPY May 17 24 500.00 P"
      - For stocks: extracts ticker from first line
      - Only extracts symbols for BUY/SELL transactions

   7. **Security Type:**
      - Only sets `security_type = STOCK` for BUY/SELL with symbols
      - All other transactions: `security_type = None`

   **Issues Encountered:**
   - ❌ End detection not working correctly - tables extending beyond intended boundaries
   - Implemented logic to stop at page numbers ("3 of 6") or "TotalTransactions" line
   - Need to move end detection BEFORE transaction line collection (currently checking after)

   **Files Created:**
   - `src/parser/schwab_new.py` - New table-based parser
   - `test_schwab_new_parser.py` - Test script with output to TXT and CSV
   - `analyze_schwab_headers.py` - Column position analysis tool
   - `debug_schwab_visual.py` - Visual debugging with PNG output showing table boundaries

   **✅ Schwab & TD Ameritrade Parser Completion (October 26, 2025)**

   Both parsers are now fully functional with comprehensive feature parity and API integration support.

   **Completed Features (Both Parsers):**

   1. ✅ **Account Number Extraction**
      - Schwab: Fixed to search for NNNN-NNNN pattern directly in text
      - TD Ameritrade: Already working

   2. ✅ **Security Type Classification**
      - Detects OPTION vs STOCK for BUY/SELL transactions
      - Schwab: Checks if description starts with CALL/PUT AND contains "EXP"
      - TD Ameritrade: Uses option contract pattern in description
      - Only sets security_type for BUY/SELL transactions
      - Returns None for DIVIDEND, INTEREST, FEE, TRANSFER, OTHER

   3. ✅ **Symbol Extraction - Underlying Ticker Only**
      - Symbol column contains ONLY the underlying ticker (e.g., "SPY", "MARA", "AAPL")
      - No longer includes full option contract details in symbol field
      - Options and stocks both use just the ticker symbol

   4. ✅ **API Symbol Field for yfinance Integration**
      - Added `api_symbol` field to `ParsedInvestmentTransaction` model
      - For stocks: Same as symbol (e.g., "AAPL")
      - For options: OCC format (e.g., "SPY240517P00500000")
      - OCC format: TICKER + YYMMDD + C/P + 8-digit strike (3 decimal places)
      - Created `_format_api_symbol()` function in both parsers
      - Database migration created: `0d6ca885ece1_add_api_symbol_to_investment_.py`

   5. ✅ **Normalized Transaction Types**
      - Created `_normalize_transaction_type()` function in both parsers
      - Maps institution-specific types to standard types:
        * BUY: Purchase/buy transactions
        * SELL: Sale/sell transactions
        * DIVIDEND: Dividend payments
        * INTEREST: Interest income/charges
        * FEE: Transaction fees
        * TRANSFER: Deposits, withdrawals, ACH
        * OTHER: Adjustments, journal entries, etc.

   **Test Results:**

   Schwab (`schwab_new.py`):
   - Account: 9145 extracted correctly
   - 17 transactions parsed from test PDF
   - Options detected: SPY, MARA, QQQ puts/calls
   - API symbols: SPY240517P00500000, MARA240531C00024000, etc.
   - All symbols show underlying ticker only

   TD Ameritrade (`tdameritrade_new.py`):
   - 133 transactions parsed from 2020 test PDF
   - Mixed stocks and options correctly classified
   - API symbols: AAPL201218C00125000, SPY201221C00370000, etc.
   - Symbol column: AAPL, SPY, PINS (ticker only)
   - Non-BUY/SELL transactions have no security_type (correct)

   **Files Created/Updated:**
   - `src/parser/models.py` - Added `api_symbol` field
   - `src/parser/schwab_new.py` - Complete table-based parser
   - `src/parser/tdameritrade_new.py` - Complete table-based parser
   - `test_schwab_new_parser.py` - Test script with API symbol output
   - `test_tda_parser_output.py` - Updated test script
   - `src/db/core.py` - Added `api_symbol` to InvestmentTransactionDB
   - `alembic/versions/0d6ca885ece1_*.py` - Migration for api_symbol
   - `PARSER_DEVELOPMENT_GUIDE.md` - Comprehensive guide for creating new parsers

   **Remaining Tasks:**
   1. ✅ Apply database migration: `alembic upgrade head`
   2. Update CRUD layer to handle api_symbol (check `crud_investment.py`)
   3. Update API models to include api_symbol (check `src/models/investment.py`)
   4. Test end-to-end with upload endpoint
   5. Verify EOD snapshot job uses api_symbol for price fetching
   6. Deprecate or update old parsers (`schwab.py`, `tdameritrade.py`)

   **Next Steps for Ameriprise:**
   1. Use `PARSER_DEVELOPMENT_GUIDE.md` as reference for development
   2. Locate sample files in `input/input/ameriprise/` or `input/ameriprise/`
   3. Create table-based parser following same pattern as Schwab and TD Ameritrade
   4. Implement all required features:
      - Account number extraction
      - Normalized transaction types (BUY, SELL, DIVIDEND, etc.)
      - Security type classification (STOCK/OPTION)
      - Symbol extraction (underlying ticker only)
      - API symbol formatting (OCC for options)
   5. Create visual debugging script to verify table boundaries
   6. Create test script and validate all transactions
   7. Test end-to-end with upload endpoint and holdings creation

3. **Duplicate Detection Logic - ALL PARSERS** (CRITICAL ISSUE)

   **Current Behavior (INCORRECT):**
   - Parsers flag transactions as duplicates if they appear multiple times in the same statement
   - Uses `_handle_investment_duplicates()` function to group by (date, type, symbol, description)
   - Adds counter to description: "Description (2)", "Description (3)", etc.
   - Sets `is_duplicate = True` for subsequent occurrences in same file

   **Required Behavior:**
   - **DO NOT flag duplicates based on transactions within the same statement**
   - If transactions are in the same statement, they are NOT duplicates of each other
   - **ONLY flag as duplicate if similar transaction already exists in the database**
   - Duplicate detection should happen during database import, not during parsing
   - This applies to BOTH PDF and CSV parsing

   **Affected Parsers:**
   - `src/parser/schwab.py` and `src/parser/schwab_new.py` ⚠️
   - `src/parser/tdameritrade.py` and `src/parser/tdameritrade_new.py` ⚠️
   - `src/parser/ameriprise.py` (when created)

   **Status:** NEW parsers (`schwab_new.py`, `tdameritrade_new.py`) still have duplicate handling logic that needs removal.

   **Implementation Changes Needed:**
   1. Remove or refactor `_handle_investment_duplicates()` from all parsers
   2. Keep transaction descriptions as-is (no "(2)", "(3)" counters)
   3. Parsers should always set `is_duplicate = False`
   4. Move duplicate detection to the transaction import/upload logic (in CRUD or router layer)
   5. Check against database using transaction hash (user_id + date + type + amount + description)
   6. Flag `needs_review = True` when matching transaction found in database

   **Priority:** HIGH - Should be completed before production use to avoid incorrect duplicate flagging

4. **Database Export/Import Functionality**
   - Create script to export all database tables to CSV/JSON
   - Create upload endpoint or import method to populate new database from export

5. **Logging Improvements**
   - Replace print statements with proper logging infrastructure
   - Set up environment-based log level configuration
   - Create error logging table for non-compromising errors
   - Add structured logging with correlation IDs
   - Confirm CSV uploads don't duplicate existing statement transactions

6. **Financial Plan Bulk Upload Endpoint**
   - Fix the bulk upload endpoint for financial plan entries (currently not working)

---

## Quick Reference: Immediate Next Steps

### Investment Transaction Parsers (Last Updated: October 26, 2025)

**Status:**
- ✅ Schwab parser - COMPLETE
- ✅ TD Ameritrade parser - COMPLETE
- 🔄 Ameriprise parser - TO BE CREATED
- ⚠️ Duplicate detection - NEEDS REFACTORING (all parsers)

**Immediate Actions Required:**

1. **Apply Database Migration** (REQUIRED)
   ```bash
   venv/Scripts/python.exe -m alembic upgrade head
   ```

2. **Update CRUD Layer** (check `src/crud/crud_investment.py`)
   - Verify `api_symbol` field is being saved to database
   - Ensure all investment transaction creation uses new field

3. **Update API Models** (check `src/models/investment.py`)
   - Add `api_symbol` field to request/response models
   - Test API endpoints return api_symbol

4. **Remove Duplicate Detection from Parsers** (HIGH PRIORITY)
   - Remove `_handle_investment_duplicates()` from:
     * `src/parser/schwab_new.py`
     * `src/parser/tdameritrade_new.py`
   - Move duplicate detection to CRUD/router layer
   - Check against database, not within statement

5. **Create Ameriprise Parser** (when ready)
   - Use `PARSER_DEVELOPMENT_GUIDE.md` as reference
   - Follow same pattern as Schwab and TD Ameritrade
   - Include all features: api_symbol, normalized types, security_type

6. **Test End-to-End**
   - Upload statements via API
   - Verify holdings created correctly
   - Test EOD snapshot job with api_symbol for price fetching

**Reference Documentation:**
- Parser Development: `PARSER_DEVELOPMENT_GUIDE.md`
- Progress Log: This file
- Working Examples: `src/parser/schwab_new.py`, `src/parser/tdameritrade_new.py`