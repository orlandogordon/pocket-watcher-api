# Investment Transaction Parser Development Guide

This guide provides step-by-step instructions for creating new investment transaction parsers for brokerage statements.

## Table of Contents
1. [Design Expectations](#design-expectations)
2. [Parser Architecture](#parser-architecture)
3. [Table-Based Parser Development](#table-based-parser-development)
4. [Debugging Table Boundaries](#debugging-table-boundaries)
5. [Testing and Validation](#testing-and-validation)
6. [Integration Checklist](#integration-checklist)

---

## Design Expectations

### Required Output Format

Every parser must return a `ParsedData` object containing:

```python
from src.parser.models import ParsedData, ParsedInvestmentTransaction, ParsedAccountInfo, SecurityType

ParsedData(
    account_info=ParsedAccountInfo(account_number_last4="1234"),
    investment_transactions=[
        ParsedInvestmentTransaction(
            transaction_date=date(2024, 5, 3),
            transaction_type="BUY",  # Normalized: BUY, SELL, DIVIDEND, INTEREST, FEE, TRANSFER, OTHER
            symbol="SPY",  # Underlying ticker only (not full option contract)
            api_symbol="SPY240517P00500000",  # yfinance OCC format for options, ticker for stocks
            description="PUTSPDRS&P500 $500 EXP 05/17/24",
            quantity=Decimal("1.0000"),
            price_per_share=Decimal("4.8500"),
            total_amount=Decimal("-485.66"),
            security_type=SecurityType.OPTION,  # Only for BUY/SELL, None for others
            is_duplicate=False  # Always False during parsing (duplicate check happens at database import)
        )
    ]
)
```

### Field Requirements

| Field | Required | Description |
|-------|----------|-------------|
| `transaction_date` | ✅ Yes | Date of transaction |
| `transaction_type` | ✅ Yes | Normalized type: BUY, SELL, DIVIDEND, INTEREST, FEE, TRANSFER, OTHER |
| `symbol` | For BUY/SELL | Underlying ticker (e.g., "AAPL", "SPY") |
| `api_symbol` | For BUY/SELL | yfinance format - OCC for options, ticker for stocks |
| `description` | ✅ Yes | Full transaction description (include all lines/details) |
| `quantity` | For BUY/SELL | Number of shares/contracts |
| `price_per_share` | For BUY/SELL | Price per share/contract |
| `total_amount` | ✅ Yes | Total transaction amount (negative for debits) |
| `security_type` | For BUY/SELL | STOCK or OPTION (None for non-securities) |
| `is_duplicate` | ✅ Yes | Always False during parsing |

### Transaction Type Normalization

All parsers must normalize institution-specific transaction types to standard types:

```python
def _normalize_transaction_type(raw_type: str, description: str) -> str:
    """
    Map institution transaction types to standard types:
    - BUY: Purchase/buy transactions
    - SELL: Sale/sell transactions
    - DIVIDEND: Dividend payments
    - INTEREST: Interest income/charges
    - FEE: Transaction fees, account fees
    - TRANSFER: Deposits, withdrawals, ACH transfers
    - OTHER: Everything else (adjustments, journal entries, etc.)
    """
    # Implementation varies by institution
```

### Symbol Format Standards

**For Stocks:**
- `symbol`: Ticker (e.g., "AAPL")
- `api_symbol`: Same ticker (e.g., "AAPL")

**For Options:**
- `symbol`: Underlying ticker only (e.g., "SPY")
- `api_symbol`: OCC format (e.g., "SPY240517P00500000")

**OCC Format:** `TICKER + YYMMDD + C/P + 8-digit strike`
- Example: SPY May 17 2024 $500 PUT → `SPY240517P00500000`
- Strike formatted as 8 digits with 3 decimal places (multiply by 1000)

### Security Type Classification

Only set `security_type` for BUY/SELL transactions:

```python
# For BUY/SELL
if transaction_type in ["BUY", "SELL"] and symbol:
    # Detect if option by checking for option-specific patterns
    if is_option:
        security_type = SecurityType.OPTION
    else:
        security_type = SecurityType.STOCK
else:
    # All other transaction types
    security_type = None
```

### Duplicate Detection

**IMPORTANT:** Parsers should **NOT** flag duplicates within the same statement.

```python
# ❌ WRONG - Do not do this during parsing
is_duplicate = True if seen_before else False

# ✅ CORRECT - Always False during parsing
is_duplicate = False
```

Duplicate detection happens during database import by checking transaction hashes against existing records.

---

## Parser Architecture

### Recommended Approach: Table-Based Parsing

For statements with tabular data (most brokerage statements), use table-based extraction:

**Advantages:**
- Handles multi-line descriptions correctly
- Preserves column alignment
- Captures commission/fee details
- More robust than line-by-line parsing

**When to Use:**
- TD Ameritrade: Fixed-column layout with multi-line transactions
- Schwab: Table format with date-tracking
- Most modern brokerage statements

### Parser Structure

```python
import pdfplumber
import re
from pathlib import Path
from typing import Union, IO, Optional
from datetime import datetime, date
from decimal import Decimal

from src.parser.models import ParsedData, ParsedInvestmentTransaction, ParsedAccountInfo, SecurityType

def _normalize_transaction_type(raw_type: str, description: str) -> str:
    """Normalize to standard types"""
    pass

def _extract_symbol(raw_data: str, security_type: SecurityType) -> Optional[str]:
    """Extract underlying ticker"""
    pass

def _format_api_symbol(symbol: str, raw_data: str, security_type: Optional[SecurityType]) -> Optional[str]:
    """Format for yfinance API"""
    pass

def parse_statement(file_source: Union[Path, IO[bytes]]) -> ParsedData:
    """Main parsing function"""
    pass
```

---

## Table-Based Parser Development

### Step 1: Analyze the PDF Structure

First, examine a sample statement to understand its layout:

```bash
# Activate virtual environment
venv/Scripts/activate

# Open PDF and examine structure
python -c "
import pdfplumber
pdf = pdfplumber.open('path/to/statement.pdf')
page = pdf.pages[0]

# Extract all text lines with positions
lines = page.extract_text_lines()
for line in lines[:20]:
    print(f\"{line['top']:6.1f} | {line['text']}\")
"
```

### Step 2: Identify Column Headers

Create a script to find exact column positions:

```python
# analyze_headers.py
import pdfplumber

pdf_path = "path/to/statement.pdf"

with pdfplumber.open(pdf_path) as pdf:
    for page_num, page in enumerate(pdf.pages):
        lines = page.extract_text_lines()

        for line in lines:
            # Look for the header row (contains column names)
            if "Date" in line['text'] and "Amount" in line['text']:
                print(f"Page {page_num + 1} - Header Row Found:")
                print(f"  Y-position: {line['top']:.1f}")
                print(f"  Text: {line['text']}")

                # Extract individual words with positions
                words = page.extract_words()
                header_words = [w for w in words if abs(w['top'] - line['top']) < 2]

                print("\nColumn Positions:")
                for word in header_words:
                    print(f"  {word['text']:20} | x0={word['x0']:6.1f} | x1={word['x1']:6.1f}")
                print()
```

**Run the script:**
```bash
venv/Scripts/python.exe analyze_headers.py
```

**Example Output:**
```
Page 1 - Header Row Found:
  Y-position: 156.4
  Text: Trade Date Settle Date Type Cash Activity Description Quantity Price Amount

Column Positions:
  Trade                | x0= 32.6 | x1= 52.1
  Date                 | x0= 53.2 | x1= 67.8
  Settle               | x0= 79.7 | x1= 99.2
  Date                 | x0=100.3 | x1=114.9
  Type                 | x0=116.6 | x1=137.6
  Cash                 | x0=160.3 | x1=180.8
  Activity             | x0=181.9 | x1=220.8
  Description          | x0=291.4 | x1=340.9
  Quantity             | x0=521.3 | x1=557.8
  Price                | x0=601.0 | x1=623.0
  Amount               | x0=662.9 | x1=696.9
```

### Step 3: Define Vertical Boundaries

Based on header analysis, define column boundaries:

```python
# Place boundaries BEFORE each column's x0 position
# Avoid splitting words by staying clear of text areas

vertical_boundaries = [
    20,    # Before Trade Date (starts at 32.6)
    65,    # Before Settle Date (starts at 79.7)
    110,   # Before Type (starts at 116.6)
    150,   # Before Cash Activity (starts at 160.3)
    290,   # Before Description (starts at 291.4)
    470,   # After Description, before Quantity
    580,   # Before Price (starts at 601.0)
    640,   # Before Amount (starts at 662.9)
    715,   # After Amount
    line['x1']  # End of page
]
```

---

## Debugging Table Boundaries

### Visual Debugging Script

Create a script that overlays table boundaries on PDF images:

```python
# debug_visual.py
import pdfplumber
import re

pdf_path = "path/to/statement.pdf"

with pdfplumber.open(pdf_path) as pdf:
    tracking_section = False
    vertical_lines = []
    page_horizontal_lines = {}

    # First pass - find transaction lines
    for page_num, page in enumerate(pdf.pages):
        lines = page.extract_text_lines()

        for line in lines:
            line_text = line['text'].strip()

            # Detect section start (adjust pattern for your statement)
            if line_text == "Account Activity":
                tracking_section = True
                continue

            # Detect section end
            if tracking_section and ("Total" in line_text or re.match(r'^\d+\s+of\s+\d+$', line_text)):
                tracking_section = False
                continue

            # Set vertical boundaries (from your analysis)
            if tracking_section and not vertical_lines:
                vertical_lines = [20, 65, 110, 150, 290, 470, 580, 640, 715, line['x1']]

            # Collect horizontal lines (transaction rows)
            if tracking_section and vertical_lines:
                # Detect transaction lines (adjust pattern for your statement)
                if re.match(r'^\d{2}/\d{2}/\d{2}', line_text):
                    if page_num not in page_horizontal_lines:
                        page_horizontal_lines[page_num] = []
                    page_horizontal_lines[page_num].append(line['top'])

    # Second pass - draw on images
    for page_num, h_lines in page_horizontal_lines.items():
        page = pdf.pages[page_num]

        # Convert page to image
        im = page.to_image(resolution=150)

        # Draw vertical lines (green)
        for v_line in vertical_lines:
            im.draw_line([(v_line, 0), (v_line, page.height)], stroke="green", stroke_width=1)

        # Draw horizontal lines (blue)
        h_lines_sorted = sorted(h_lines)

        # Add bottom boundary with padding
        if len(h_lines_sorted) > 1:
            avg_row_height = (h_lines_sorted[-1] - h_lines_sorted[0]) / len(h_lines_sorted)
            padding = avg_row_height * 3
        else:
            padding = 50
        h_lines_sorted.append(h_lines_sorted[-1] + padding)

        for h_line in h_lines_sorted:
            im.draw_line([(0, h_line), (page.width, h_line)], stroke="blue", stroke_width=1)

        # Draw cells (red rectangles)
        for h in range(len(h_lines_sorted) - 1):
            for v in range(len(vertical_lines) - 1):
                x0, y0 = vertical_lines[v], h_lines_sorted[h]
                x1, y1 = vertical_lines[v+1], h_lines_sorted[h+1]
                im.draw_rect((x0, y0, x1, y1), stroke="red", stroke_width=1)

        # Save image
        output_path = f"debug_page_{page_num + 1}.png"
        im.save(output_path)
        print(f"Saved: {output_path}")
        print(f"  Rows: {len(h_lines_sorted)-1}, Columns: {len(vertical_lines)-1}")

print("\nDone! Check the PNG files to verify table boundaries.")
```

**Run the script:**
```bash
venv/Scripts/python.exe debug_visual.py
```

**Review the output PNG files:**
- Green lines = vertical column boundaries
- Blue lines = horizontal row boundaries
- Red rectangles = individual cells

**Adjust boundaries if needed:**
- If text is split across cells, move vertical boundary
- If rows overlap, adjust horizontal boundary detection
- Re-run until all text fits cleanly in cells

### Common Table Extraction Issues

#### Issue 1: Text Split Across Cells

**Symptom:** "Purchased" appears as "Purchas" in one cell and "ed" in another

**Cause:** Vertical boundary placed in the middle of text

**Solution:** Move boundary to the left (before the column starts):
```python
# ❌ Wrong - boundary splits text
vertical_lines = [20, 65, 110, 256, ...]  # 256 is in middle of "Description" column

# ✅ Correct - boundary before column
vertical_lines = [20, 65, 110, 290, ...]  # 290 is before "Description" starts at 291.4
```

#### Issue 2: Multi-line Descriptions Cut Off

**Symptom:** Last line of transaction description missing

**Cause:** Insufficient padding at bottom of table

**Solution:** Increase padding or detect end markers:
```python
# Option 1: Increase padding
padding = avg_row_height * 3  # Increased from 2 to 3

# Option 2: Use end markers (better)
if "Closing Balance" in line_text:
    end_boundary = line['top'] - 5
```

#### Issue 3: Header Rows in Data

**Symptom:** Column headers appear as transaction rows

**Cause:** Header row detection not filtering properly

**Solution:** Add header pattern to skip list:
```python
# Skip date range headers
if re.match(r'^\d{2}/\d{2}/\d{2}\s*-\s*\d{2}/\d{2}/\d{2}', date_str):
    continue

# Skip column headers
if "Trade Date" in row[0] or "Date" == row[0]:
    continue
```

#### Issue 4: Footer Text in Last Page

**Symptom:** Footer text appears in last transaction

**Cause:** Padding extends beyond table boundary

**Solution:** Detect and store end marker position:
```python
# During line collection
if "Closing Balance" in line_text or "Total Transactions" in line_text:
    page_end_markers[page_num] = line['top']

# During table building
if page_num in page_end_markers:
    end_boundary = page_end_markers[page_num] - 5
else:
    end_boundary = h_lines_sorted[-1] + padding
```

---

## Testing and Validation

### Create Test Script

```python
# test_parser.py
import sys
from pathlib import Path
import csv

sys.path.insert(0, str(Path(__file__).parent))

from src.parser.your_parser import parse_statement

def test_parser(file_path: str):
    """Test parser and output results"""
    file_name = Path(file_path).stem
    txt_output = f"parser_test_output_{file_name}.txt"
    csv_output = f"parser_test_output_{file_name}.csv"

    print(f"Testing parser on: {Path(file_path).name}")
    print("=" * 100)

    # Parse the statement
    parsed_data = parse_statement(file_path)

    # Write to text file
    with open(txt_output, 'w', encoding='utf-8') as f:
        f.write(f"PARSER TEST RESULTS\n")
        f.write(f"=" * 100 + "\n\n")

        if parsed_data.account_info:
            f.write(f"Account Number (last 4): {parsed_data.account_info.account_number_last4}\n\n")

        f.write(f"Total Transactions: {len(parsed_data.investment_transactions)}\n\n")

        for idx, txn in enumerate(parsed_data.investment_transactions):
            f.write(f"Transaction #{idx + 1}\n")
            f.write(f"-" * 80 + "\n")
            f.write(f"Date: {txn.transaction_date}\n")
            f.write(f"Type: {txn.transaction_type}\n")
            f.write(f"Security Type: {txn.security_type.value if txn.security_type else 'N/A'}\n")
            f.write(f"Symbol: {txn.symbol or 'N/A'}\n")
            f.write(f"API Symbol: {txn.api_symbol or 'N/A'}\n")
            f.write(f"Description: {txn.description}\n")
            f.write(f"Quantity: {txn.quantity if txn.quantity else 'N/A'}\n")
            f.write(f"Price per Share: ${txn.price_per_share if txn.price_per_share else 'N/A'}\n")
            f.write(f"Total Amount: ${txn.total_amount}\n")
            f.write(f"Is Duplicate: {txn.is_duplicate}\n\n")

    # Write to CSV
    with open(csv_output, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow([
            'Transaction_Number', 'Date', 'Type', 'Security_Type', 'Symbol', 'API_Symbol',
            'Description', 'Quantity', 'Price_Per_Share', 'Total_Amount', 'Is_Duplicate'
        ])

        for idx, txn in enumerate(parsed_data.investment_transactions):
            writer.writerow([
                idx + 1,
                txn.transaction_date.strftime('%Y-%m-%d'),
                txn.transaction_type,
                txn.security_type.value if txn.security_type else '',
                txn.symbol or '',
                txn.api_symbol or '',
                txn.description.replace('\n', ' | ') if txn.description else '',
                str(txn.quantity) if txn.quantity else '',
                str(txn.price_per_share) if txn.price_per_share else '',
                str(txn.total_amount),
                'Yes' if txn.is_duplicate else 'No'
            ])

    print(f"\nParser completed!")
    print(f"  Total transactions: {len(parsed_data.investment_transactions)}")
    print(f"\nOutput files:")
    print(f"  Text: {txt_output}")
    print(f"  CSV:  {csv_output}")

if __name__ == "__main__":
    pdf_path = r"path\to\test\statement.pdf"
    test_parser(pdf_path)
```

### Validation Checklist

Run the test script and verify:

- [ ] **All transactions extracted** - Compare count to PDF
- [ ] **Dates correct** - No parsing errors, proper year handling
- [ ] **Transaction types normalized** - BUY, SELL, DIVIDEND, etc.
- [ ] **Symbols correct**:
  - [ ] Stocks: Just ticker (e.g., "AAPL")
  - [ ] Options: Just underlying ticker (e.g., "SPY")
- [ ] **API symbols correct**:
  - [ ] Stocks: Same as symbol
  - [ ] Options: OCC format (e.g., "SPY240517P00500000")
- [ ] **Security types**:
  - [ ] Set only for BUY/SELL transactions
  - [ ] STOCK vs OPTION correctly classified
  - [ ] None for DIVIDEND, INTEREST, FEE, TRANSFER, OTHER
- [ ] **Quantities and prices** - Extracted for BUY/SELL, None for others
- [ ] **Amounts** - Correct sign (negative for debits, positive for credits)
- [ ] **Descriptions complete** - All lines captured, no truncation
- [ ] **No duplicates flagged** - is_duplicate = False for all
- [ ] **No header rows** - Filtered out during extraction
- [ ] **No footer text** - Stopped at proper boundary

### Quick Validation Commands

```bash
# Check first 10 transactions
venv/Scripts/python.exe -c "
import csv
rows = list(csv.DictReader(open('parser_test_output.csv', encoding='utf-8')))
for r in rows[:10]:
    print(f\"{r['Type']:10} {r['Security_Type']:8} {r['Symbol']:6} {r['API_Symbol']}\")
"

# Check non-BUY/SELL transactions
venv/Scripts/python.exe -c "
import csv
rows = list(csv.DictReader(open('parser_test_output.csv', encoding='utf-8')))
non_buysell = [r for r in rows if r['Type'] not in ['BUY', 'SELL']]
for r in non_buysell[:10]:
    print(f\"{r['Type']:12} {r['Security_Type']:10} {r['Symbol']:10}\")
"

# Count transaction types
venv/Scripts/python.exe -c "
import csv
from collections import Counter
rows = list(csv.DictReader(open('parser_test_output.csv', encoding='utf-8')))
types = Counter(r['Type'] for r in rows)
for t, count in types.items():
    print(f\"{t:12} {count:3}\")
"
```

---

## Integration Checklist

### 1. Update CRUD Layer

Ensure `crud_investment.py` handles the new `api_symbol` field:

```python
# src/crud/crud_investment.py
def create_investment_transaction(
    db: Session,
    user_id: int,
    parsed_txn: ParsedInvestmentTransaction,
    account_id: Optional[int] = None
) -> InvestmentTransactionDB:
    # ...existing code...

    new_txn = InvestmentTransactionDB(
        user_id=user_id,
        account_id=account_id,
        transaction_hash=txn_hash,
        transaction_type=txn_type,
        symbol=parsed_txn.symbol,
        api_symbol=parsed_txn.api_symbol,  # Add this
        # ...rest of fields...
    )
```

### 2. Update API Models

Check if Pydantic models in `src/models/investment.py` include `api_symbol`:

```python
# src/models/investment.py
class InvestmentTransactionCreate(BaseModel):
    # ...existing fields...
    symbol: str
    api_symbol: Optional[str] = None  # Add this
    # ...rest of fields...

class InvestmentTransactionResponse(BaseModel):
    # ...existing fields...
    symbol: str
    api_symbol: Optional[str] = None  # Add this
    # ...rest of fields...
```

### 3. Run Database Migration

```bash
# Apply the migration
venv/Scripts/python.exe -m alembic upgrade head
```

### 4. Update Import Service

Check `src/services/importer.py` passes `api_symbol` correctly.

### 5. Add Parser to Registry

If you have a parser registry or factory, add the new parser:

```python
# src/parser/__init__.py or wherever parsers are registered
PARSER_MAP = {
    "tdbank": tdbank.parse_statement,
    "schwab": schwab_new.parse_statement,
    "tdameritrade": tdameritrade_new.parse_statement,
    "your_institution": your_parser.parse_statement,  # Add this
}
```

### 6. Test End-to-End

1. Upload a test statement via API
2. Verify transactions are created in database
3. Check holdings are created for BUY transactions
4. Verify EOD snapshot job can fetch prices using `api_symbol`

---

## Reference Implementations

For examples of working parsers, see:

- **Schwab (Table-based, Date-tracking)**: `src/parser/schwab_new.py`
- **TD Ameritrade (Table-based, Multi-line)**: `src/parser/tdameritrade_new.py`
- **TD Bank (Table-based, Simple)**: `src/parser/tdbank.py`

All include:
- Normalized transaction types
- Proper symbol extraction (ticker only)
- API symbol formatting (OCC for options)
- Security type classification
- Visual debugging scripts

---

## Common Pitfalls

1. **Don't modify descriptions** - Keep all lines, including commission/fees
2. **Don't flag duplicates during parsing** - Always set `is_duplicate = False`
3. **Don't put full option contract in symbol** - Extract underlying ticker only
4. **Don't set security_type for non-BUY/SELL** - Only BUY/SELL should have it
5. **Don't skip boundary testing** - Always create visual debug script first
6. **Don't forget normalization** - All parsers must use standard transaction types
7. **Don't hardcode year** - Extract from statement or infer from context

---

## Getting Help

If you encounter issues:

1. **Visual debugging first** - Create PNG overlays to see table boundaries
2. **Check reference parsers** - Compare against working implementations
3. **Test incrementally** - Validate each section before moving to next
4. **Use print debugging** - Log skip reasons and extraction decisions
5. **Review progress_log.md** - Check for similar issues encountered before
