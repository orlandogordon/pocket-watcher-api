import csv
import pdfplumber
from pathlib import Path
from collections import namedtuple

DATES={
    'Jan': '01/',
    'Feb': '02/', 
    'Mar': '03/', 
    'Apr': '04/', 
    'May': '05/', 
    'Jun': '06/', 
    'Jul': '07/', 
    'Aug': '08/', 
    'Sep': '09/', 
    'Oct': '10/', 
    'Nov': '11/', 
    'Dec': '12/'
    }

Statement_Result = namedtuple('Statement_Result', ['transaction_data'])

def parse_401k_statement(pdf_file):
    print(f"Parsing position data from Fidelity 401k statement: '{pdf_file}'.")
    # Setting up lists for transaction data
    transactions = []
    # Used to manage application state. When the header of the transactions table in the PDF is discovered, 'tracking_transactions' will flip to true
    # and the following lines analyzed will be added to the transactions list if they fit the expected format of a transaction entry
    tracking_transactions = False
    # Parsing logic configuration variables specific to each type of bank statement 
    start_parse_transactions_keywords = ['Transaction Details']
    end_parse_transactions_keywords = ['TotalTransactions']
    transaction_types = ['Purchase', 'Sale', 'Interest']
    ## Transaction data CSV headers
    transaction_data = []
    # Setting a year variable to complete dates in the data
    year = ''
    ## Complimentary Data to be Parsed
    brokerage_name = 'schwab'
    account_number = ''
    # Text that will hold the parsed pdf
    text = ''

    with pdfplumber.open(str(pdf_file)) as pdf:
        for page in pdf.pages:
            text += page.extract_text()
    lines = text.split('\n')
    
    print(pdf_file)
    for line in lines: print(line)
    breakpoint()
    for i in range(len(lines)):
        if "AccountNumber" in lines[i] or "StatementPeriod" in lines[i]:
            while "-" not in lines[i]: 
                i+=1
            text_split = lines[i].split(" ")
            account_number = text_split[1]
            year = f"/{text_split[-1].split(',')[-1]}"
            break

    for i in range(len(lines)):
        if lines[i] in start_parse_transactions_keywords:
            tracking_transactions = True
        elif (lines[i][0:3] in DATES.values() or lines[i].split(' ')[0] in transaction_types) and tracking_transactions:
            if lines[i+1][0:3] in DATES.values() and lines[i+1].split(' ')[1] not in transaction_types: lines[i+1] = ''
            if lines[i+2][0:3] in DATES.values() and lines[i+2].split(' ')[1] not in transaction_types: lines[i+2] = ''
            if tracking_transactions: transactions.append(lines[i].split(' '))
        elif any(lines[i].startswith(prefix) for prefix in end_parse_transactions_keywords):
            tracking_transactions = False

    # Refine transaction data and write to CSV
    for transaction in transactions:
        if transaction[0][0:3] in DATES.values():
            date = transaction.pop(0) + year
        elif transaction_data[-1][0][0:3] in DATES.values():
            date = transaction_data[-1][0]
        else:
            # Throw an error 
            raise KeyError

        transaction_type = transaction.pop(0)
        if transaction_type == 'Interest':
            symbol = ''
            price = ''
            quantity = ''
            amount = transaction.pop()
            description = " ".join(transaction)
        elif transaction_type in ['Purchase', 'Sale']:
            symbol = transaction.pop(0)
            description = transaction.pop(0)

            while '.000' not in transaction[0]:
                description += ' ' + transaction.pop(0)

            quantity = transaction.pop(0).replace("(", "").replace(")", "")
            price = transaction.pop(0)

            if float(transaction[0].replace("(", "").replace(")", "").replace(",", "").replace("$", '')) < float(price):
                price = f"{float(price) + float(transaction.pop(0))}"
            
            amount = transaction.pop(0).replace("(", "").replace(")", "")
       
        transaction_data.append([date, transaction_type, symbol, description, quantity, price, amount, brokerage_name, account_number])

    # pdf_file.rename(f"C:\\Users\\{project path}\\processed_statements\\{pdf_file.parts[-2]}\\{pdf_file.parts[-1]}")

    result = Statement_Result(transaction_data=transaction_data)
    
    return result

def parse_csv(csv_file):
    print(f"Parsing transaction data from Schwab CSV located at: '{csv_file}'.")
    # Initialize data list to hold csv data
    data = []
    ## Transaction data list
    transaction_data = []
    ## Complimentary Data to be Parsed
    brokerage_name = 'schwab'
    account_holder = ''
    account_number = ''
    # Column/Index -> Field Matching
    date_index = 0
    transaction_type_index = 1
    symbol_index = 2
    description_index = 3
    quantity_index = 4
    price_index = 5
    amount_index = 7

    # Read the CSV file
    with open(csv_file, mode='r', newline='') as infile:
        reader = csv.reader(infile)
        data = list(reader)[7:]
        data.pop()
        
    for row in data:
        date = row[date_index]
        account_number = ''
        transaction_type = row[transaction_type_index]
        description = row[description_index]
        amount = row[amount_index].replace("$", "").replace("-", "")
        quantity = row[quantity_index]
        price = row[price_index].replace("$", "")
        symbol = row[symbol_index]
        transaction_data.append([date, transaction_type, symbol, description, quantity, price, amount, brokerage_name, account_number])

    # pdf_file.rename(f"C:\\Users\\{project path}\\processed_statements\\{pdf_file.parts[-2]}\\{pdf_file.parts[-1]}")

    result = Statement_Result(transaction_data=transaction_data)

    return result

def write_csv(transactions_csv_file_path, transaction_data):
    # Open the file in write mode
    with open(transactions_csv_file_path, mode='a', newline='') as file:
        writer = csv.writer(file)
        writer.writerows(transaction_data)