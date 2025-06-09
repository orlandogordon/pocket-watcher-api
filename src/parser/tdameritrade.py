import csv
import pdfplumber
from pathlib import Path

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

def parse_statement(pdf_file):
    print(f"Parsing transaction data from TD Ameritrade statement: '{pdf_file}'.")
    # Setting up lists for transaction data
    transactions = []
    # Used to manage application state. When the header of the transactions table in the PDF is discovered, 'tracking_transactions' will flip to true
    # and the following lines analyzed will be added to the transactions list if they fit the expected format of a transaction entry
    tracking_transactions = False
    # Parsing logic configuration variables specific to each type of bank statement 
    start_parse_transactions_keywords = ['Account Activity']
    end_parse_transactions_keywords = ['Closing Balance', 'Statement for Account #', 'page ']
    transaction_types = ['Buy', 'Sell', 'Funds', 'Delivered', 'Funds', 'Div/Int', 'Div/Int', 'Journal']
    ## Transaction data CSV headers
    transaction_data = []
    # Setting a year variable to complete dates in the data
    year = ''
    ## Complimentary Data to be Parsed
    brokerage_name = 'tdameritrade'
    account_number = ''
    # Text that will hold the parsed pdf
    text = ''

    with pdfplumber.open(str(pdf_file)) as pdf:
        for page in pdf.pages:
            text += page.extract_text()
    lines = text.split('\n')
    
    print(pdf_file)

    # for line in lines: print(line)
    # breakpoint()
    for i in range(len(lines)):
        if "Statement for Account #" in lines[i]:
            account_number = lines[i][-4:]
            break

    for i in range(len(lines)):
        if lines[i] in start_parse_transactions_keywords:
            tracking_transactions = True
        elif lines[i][0:3] in DATES.values() and (tracking_transactions):
            lines[i] = lines[i].replace('$', '')
            line_split = [val for val in lines[i].split(" ") if val not in ['$', '']]

            date = line_split.pop(0)
            date_split = date.split('/')
            date_split[2] = '20' + date_split[2]
            date = '/'.join(date_split)
            
            line_split.pop(0)
            line_split.pop(0)

            while line_split[0] not in transaction_types: line_split.pop(0)

            transaction_type = line_split.pop(0)
            line_split.pop()
            amount = line_split.pop().replace("(", "").replace(")", "")
            price = line_split.pop()
            quantity = line_split.pop().replace("-", "")
            symbol = ''

            if lines[i+1][0:3] not in DATES.values() and not any(keyword in lines[i] for keyword in end_parse_transactions_keywords):
                line_split.append(lines[i+1])
            description = " ".join(line_split).replace("-", "")
            
            if tracking_transactions: transaction_data.append([date, transaction_type, symbol, description, quantity, price, amount, brokerage_name, account_number])
        elif any(keyword in lines[i] for keyword in end_parse_transactions_keywords):
            tracking_transactions = False

    # pdf_file.rename(f"C:\\Users\\{project path}\\processed_statements\\{pdf_file.parts[-2]}\\{pdf_file.parts[-1]}")
    
    return transaction_data

def write_csv(transactions_csv_file_path, transaction_data):
    # Open the file in write mode
    with open(transactions_csv_file_path, mode='a', newline='') as file:
        writer = csv.writer(file)
        writer.writerows(transaction_data)