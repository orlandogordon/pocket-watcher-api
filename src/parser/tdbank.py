import csv
import re
import pdfplumber
from pathlib import Path
from decimal import Decimal, InvalidOperation
from datetime import datetime
from typing import List, Optional, Union, IO
import io
from itertools import groupby

from src.parser.models import ParsedData, ParsedTransaction, ParsedAccountInfo


DATES = {
    'Jan': '01/', 'Feb': '02/', 'Mar': '03/', 'Apr': '04/', 'May': '05/', 'Jun': '06/', 
    'Jul': '07/', 'Aug': '08/', 'Sep': '09/', 'Oct': '10/', 'Nov': '11/', 'Dec': '12/'
}

def _handle_duplicates(transactions: List[ParsedTransaction]) -> List[ParsedTransaction]:
    """
    Handles duplicate transactions by appending a counter to the description.
    """
    updated_transactions = []
    keyfunc = lambda t: (t.transaction_date, t.amount, t.description)
    
    sorted_transactions = sorted(transactions, key=keyfunc)

    for key, group in groupby(sorted_transactions, key=keyfunc):
        group_list = list(group)
        if len(group_list) > 1:
            # Duplicates found
            for i, transaction in enumerate(group_list):
                if i == 0:
                    # First one is kept as is
                    updated_transactions.append(transaction)
                else:
                    # Subsequent ones get a modified description
                    new_description = f"{transaction.description} ({i + 1})"
                    updated_transactions.append(
                        ParsedTransaction(
                            transaction_date=transaction.transaction_date,
                            description=new_description,
                            amount=transaction.amount,
                            transaction_type=transaction.transaction_type,
                            is_duplicate=True
                        )
                    )
        else:
            # No duplicates for this key
            updated_transactions.append(group_list[0])
            
    return updated_transactions

def parse_statement(file_source: Union[Path, IO[bytes]]) -> ParsedData:
    """Parses a TD Bank PDF statement from a file path or in-memory stream."""
    print("Parsing transaction data from TD Bank statement...")
    
    # State tracking variables
    tracking_deposits = False
    tracking_purchases = False
    
    # Keywords for section detection
    start_parse_purchases_keywords = ['Payments', 'ElectronicPayments', 'ElectronicPayments(continued)', 'OtherWithdrawals']
    end_parse_keywords = ['Call 1-800-937-2000', 'Subtotal:']
    start_parse_deposits_keywords = ['Deposits', 'ElectronicDeposits', 'ElectronicDeposits(continued)', 'OtherCredits']
    
    # Data storage
    months = []
    years = []
    account_holder = ''
    account_number = ''
    deposit_data = []
    purchase_data = []
    transactions: List[ParsedTransaction] = []
    
    # Table detection variables
    horizontal_lines = []
    vertical_lines = []
    deposit_tables = []
    purchase_tables = []

    with pdfplumber.open(file_source) as pdf:
        # Build complete text and lines for all pages
        lines_dict = {}
        for page_num in range(len(pdf.pages)):
            lines_dict[page_num] = pdf.pages[page_num].extract_text_lines() 
        
        # Extract the statement period and account info
        for page_idx in lines_dict:
            for line in lines_dict[page_idx]:
                line_text = line['text']
                
                # Extract statement period
                if "StatementPeriod:" in line_text or "Statement Period:" in line_text:                    
                    for month_name, month_num in DATES.items():
                        if month_name in line_text:
                            # Find the position of the month name
                            month_pos = line_text.find(month_name)
                            # Extract a window of text after the month (should contain day and year)
                            text_after_month = line_text[month_pos + len(month_name):month_pos + len(month_name) + 10]
                            print(f"DEBUG: Found {month_name} at position {month_pos}, text after: '{text_after_month}'")
                            
                            # Try to extract the year from this segment
                            # Look for 4 consecutive digits that start with 19 or 20
                            year_match = re.search(r'(19\d{2}|20\d{2})', text_after_month)
                            if year_match:
                                if month_num not in months:
                                    months.append(month_num)
                                    years.append(year_match.group(1))
                                    print(f"DEBUG: Extracted month={month_num}, year={year_match.group(1)}")
                    
                    print(f"Extracted: months={months}, years={years}")
                
                # Extract account number
                elif " Account# " in line_text or " Account #" in line_text:
                    text_split = line_text.split()
                    if text_split:
                        account_holder = text_split[0]
                        account_number = text_split[-1]
                        # Clean account number
                        account_number = re.sub(r'[^0-9]', '', account_number)
        
        print(f"Final months: {months}, years: {years}")
        print(f"Account number: {account_number[-4:] if account_number else 'null'}")
        
        # Process transactions
        for page_idx in lines_dict:
            page = pdf.pages[page_idx]
            
            for line in lines_dict[page_idx]:
                line_text = line['text'].strip()
                
                # Track section changes
                if line_text in start_parse_deposits_keywords:
                    tracking_deposits = True
                    print(f"Started tracking deposits: {line_text}")
                elif line_text in start_parse_purchases_keywords:
                    tracking_purchases = True
                    print(f"Started tracking purchases: {line_text}")
                
                # Detect table header and set up column boundaries
                elif line_text == 'POSTINGDATE DESCRIPTION AMOUNT' and (tracking_deposits or tracking_purchases):
                    # For TD Bank, we need exactly 4 boundaries for 3 columns
                    desc_pos = line_text.find('DESCRIPTION')
                    amt_pos = line_text.find('AMOUNT')
                    
                    if 'chars' in line:
                        chars = line['chars']
                        desc_x = None
                        amt_x = None
                        char_count = 0
                        
                        for char in chars:
                            if char_count == desc_pos and desc_x is None:
                                desc_x = char['x0']
                            if char_count == amt_pos and amt_x is None:
                                amt_x = char['x0']
                            char_count += 1
                        
                        vertical_lines = [
                            line['x0'],
                            desc_x - 5 if desc_x else line['x0'] + 80,
                            amt_x - 50 if amt_x else line['x1'] - 100,
                            line['x1']
                        ]
                    else:
                        vertical_lines = [line['x0'], line['x0'] + 80, line['x1'] - 100, line['x1']]
                                    
                # Collect transaction line positions
                if (tracking_deposits or tracking_purchases) and re.match(r'^\d{2}/\d{2}.*\d.\d{2}$', line_text):
                    horizontal_lines.append(line['top'])
                
                # End section detection
                if (tracking_deposits or tracking_purchases) and any(line_text.startswith(prefix) for prefix in end_parse_keywords):
                    if horizontal_lines and vertical_lines:
                        horizontal_lines.append(line['top'])
                        
                        # Build cells
                        cells = []
                        for i in range(len(horizontal_lines) - 1):
                            for j in range(len(vertical_lines) - 1):
                                cells.append([vertical_lines[j], horizontal_lines[i], 
                                            vertical_lines[j+1], horizontal_lines[i+1]])
                        
                        # Create table
                        table = pdfplumber.table.Table(page, tuple(cells))
                        
                        if tracking_deposits:
                            deposit_tables.append(table)
                        elif tracking_purchases:
                            purchase_tables.append(table)
                        
                        print(f"Created {'deposit' if tracking_deposits else 'purchase'} table with {len(cells)} cells")
                    
                    # Reset
                    vertical_lines = []
                    horizontal_lines = []
                    tracking_deposits = False
                    tracking_purchases = False
        
        # Extract data from tables
        for table in deposit_tables:
            extracted = table.extract()
            deposit_data.extend(extracted)
            
        for table in purchase_tables:
            extracted = table.extract()
            purchase_data.extend(extracted)
        
        print(f"Total deposit rows: {len(deposit_data)}")
        print(f"Total purchase rows: {len(purchase_data)}")
        
        # Process deposits
        for row in deposit_data:
            if not row or len(row) < 3:
                continue
            
            # Handle both 3 and 4 column formats
            if len(row) == 3:
                date_str = str(row[0]) if row[0] else ''
                description = str(row[1]) if row[1] else ''
                amount_str = str(row[2]) if row[2] else ''
            elif len(row) >= 4 and not row[1]:  # Empty second column
                date_str = str(row[0]) if row[0] else ''
                description = str(row[2]) if row[2] else ''
                amount_str = str(row[3]) if row[3] else ''
            else:
                date_str = str(row[0]) if row[0] else ''
                description = str(row[1]) if row[1] else ''
                amount_str = str(row[2]) if row[2] else ''
            
            # Skip if not a valid date
            if not re.match(r'^\d{2}/\d{2}$', date_str):
                continue
            
            # If amount is embedded in description, extract it
            if not amount_str or not re.match(r'^\d+\.?\d*$', amount_str.replace(',', '').replace('$', '')):
                amount_match = re.search(r'(\d+\.\d{2})(?:\s|$|\n)', description)
                if amount_match:
                    amount_str = amount_match.group(1)
                    description = description[:amount_match.start()].strip()
            
            # Complete date from MM/DD to MM/DD/YYYY
            month_num = date_str[:2]  # Get "08" from "08/24"

            # Determine which year to use based on the month
            if month_num == months[0][:2] if months else None:  # Compare just month numbers
                full_date = date_str + "/" + years[0]
            elif len(months) > 1 and month_num == months[1][:2]:
                full_date = date_str + "/" + years[1]
            else:
                # Default to first year if month matching fails
                full_date = date_str + "/" + years[0] if years else "2020"
                print(f"Warning: Using default year for date: {date_str}")

            try:
                parsed_date = datetime.strptime(full_date, "%m/%d/%Y").date()
                amount = Decimal(amount_str.replace("$", "").replace(",", ""))
                
                transactions.append(
                    ParsedTransaction(
                        transaction_date=parsed_date,
                        description=description.replace('\n', ' ').strip(),
                        amount=amount,
                        transaction_type="Deposit"
                    )
                )
                
            except (ValueError, InvalidOperation) as e:
                print(f"Error parsing deposit: date={date_str}, amount={amount_str}, error={e}")
        
        # Process purchases
        for row in purchase_data:
            if not row or len(row) < 3:
                continue
            
            # Handle both 3 and 4 column formats
            if len(row) == 3:
                date_str = str(row[0]) if row[0] else ''
                description = str(row[1]) if row[1] else ''
                amount_str = str(row[2]) if row[2] else ''
            elif len(row) >= 4 and not row[1]:  # Empty second column
                date_str = str(row[0]) if row[0] else ''
                description = str(row[2]) if row[2] else ''
                amount_str = str(row[3]) if row[3] else ''
            else:
                date_str = str(row[0]) if row[0] else ''
                description = str(row[1]) if row[1] else ''
                amount_str = str(row[2]) if row[2] else ''
            
            # Skip if not a valid date
            if not re.match(r'^\d{2}/\d{2}$', date_str):
                continue
            
            # If amount is embedded in description, extract it
            if not amount_str or not re.match(r'^\d+\.?\d*$', amount_str.replace(',', '').replace('$', '')):
                amount_match = re.search(r'(\d+\.\d{2})(?:\s|$|\n)', description)
                if amount_match:
                    amount_str = amount_match.group(1)
                    description = description[:amount_match.start()].strip()

            # Complete date - from MM/DD to MM/DD/YYYY
            month_num = date_str[:2]  # Get "08" from "08/24"

            # Determine which year to use based on the month
            if month_num == months[0][:2] if months else None:  # Compare just month numbers
                full_date = date_str + "/" + years[0]
            elif len(months) > 1 and month_num == months[1][:2]:
                full_date = date_str + "/" + years[1]
            else:
                # Default to first year if month matching fails
                full_date = date_str + "/" + years[0] if years else "2020"
                print(f"Warning: Using default year for date: {date_str}")
            
            try:
                parsed_date = datetime.strptime(full_date, "%m/%d/%Y").date()
                amount = abs(Decimal(amount_str.replace("$", "").replace(",", "")))
                
                transactions.append(
                    ParsedTransaction(
                        transaction_date=parsed_date,
                        description=description.replace('\n', ' ').strip(),
                        amount=amount,
                        transaction_type="Purchase"
                    )
                )
                
            except (ValueError, InvalidOperation) as e:
                print(f"Error parsing purchase: date={date_str}, amount={amount_str}, error={e}")
    
    print(f"Total transactions parsed: {len(transactions)}")
    
    transactions = _handle_duplicates(transactions)

    # Create account info
    account_info = None
    if account_number and len(account_number) >= 4:
        account_info = ParsedAccountInfo(account_number_last4=account_number[-4:])
    
    return ParsedData(account_info=account_info, transactions=transactions)


def parse_csv(file_source: Union[Path, IO[bytes]]) -> ParsedData:
    """Parses a TD Bank CSV from a file path or in-memory stream."""
    print("Parsing transaction data from TD Bank csv...")
    parsed_transactions: List[ParsedTransaction] = []
    account_info: Optional[ParsedAccountInfo] = None

    if isinstance(file_source, io.BytesIO):
        text_stream = io.TextIOWrapper(file_source, encoding='utf-8')
    else:
        text_stream = open(file_source, 'r')
    
    reader = csv.reader(text_stream)
    next(reader)  # Skip header

    for row in reader:
        try:
            date = datetime.strptime(row[0], "%Y-%m-%d").date()
            description = row[4]
            debit = row[5]
            credit = row[6]

            if credit:
                amount = Decimal(credit)
                transaction_type = 'Deposit'
            else:
                amount = Decimal(debit)
                transaction_type = 'Purchase'

            parsed_transactions.append(
                ParsedTransaction(
                    transaction_date=date,
                    description=description.strip(),
                    amount=amount,
                    transaction_type=transaction_type
                )
            )
        except (ValueError, InvalidOperation, IndexError) as e:
            print(f"Skipping row in TD Bank CSV due to parsing error: {row} -> {e}")
            continue

    if isinstance(file_source, Path):
        text_stream.close()
    breakpoint()
    
    parsed_transactions = _handle_duplicates(parsed_transactions)
    
    return ParsedData(transactions=parsed_transactions, account_info=account_info)


def parse(file_source: Union[Path, IO[bytes]], is_csv: bool = False) -> ParsedData:
    """
    Parses a TD Bank statement (PDF or CSV) from a file path or in-memory stream.
    """
    if is_csv:
        return parse_csv(file_source)
    else:
        return parse_statement(file_source)
