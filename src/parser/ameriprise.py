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

def parse_csv(csv_file):
    print(f"Parsing transaction data from Ameriprise CSV located at: '{csv_file}'.")
    # Initialize data list to hold csv data
    data = []
    ## Transaction data list
    transaction_data = []
    ## Complimentary Data to be Parsed
    brokerage_name = 'ameriprise'
    account_holder = ''
    account_number = ''
    # Column/Index -> Field Matching
    date_index = 0
    account_index = 1
    transaction_type_index = 2
    description_index = 2
    amount_index = 3
    quantity_index = 4
    price_index = 5
    symbol_index = 6

    # Read the CSV file
    with open(csv_file, mode='r', newline='') as infile:
        reader = csv.reader(infile)
        data = list(reader)[7:]
        data.pop()

    for row in data:
        date = row[date_index]
        account_number = row[account_index][-10:].replace(")", "")
        transaction_type = row[transaction_type_index].split('-')[0]
        description = row[description_index].split('-')[1]
        amount = row[amount_index].replace("$", "").replace("-", "")
        quantity = row[quantity_index].replace("-", "")
        price = row[price_index].replace("$", "")
        symbol = row[symbol_index]
        transaction_data.append([date, transaction_type, symbol, description, quantity, price, amount, brokerage_name, account_number])

    # pdf_file.rename(f"C:\\Users\\{project path}\\processed_statements\\{pdf_file.parts[-2]}\\{pdf_file.parts[-1]}")

    return transaction_data

def write_csv(transactions_csv_file_path, transaction_data):
    # Open the file in write mode
    with open(transactions_csv_file_path, mode='a', newline='') as file:
        writer = csv.writer(file)
        writer.writerows(transaction_data)