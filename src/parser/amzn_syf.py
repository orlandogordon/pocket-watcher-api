import csv
import pdfplumber
from pathlib import Path

DATES=['01/', '02/', '03/', '04/', '05/', '06/', '07/', '08/', '09/', '10/', '11/', '12/']

def _map_transaction_type(line, keywords):
    if line.startswith(keywords['payments']):
        return [True, False, False, False, False]
    elif line.startswith(keywords['credits']):
        return [False, True, False, False, False]  
    elif line.startswith(keywords['purchases']):
        return [False, False, True, False, False]
    elif line.startswith(keywords['fees']):
        return [False, False, False, True, False]
    elif line.startswith(keywords['interest']):
        return [False, False, False, False, True]

def parse_statement(pdf_file):
    print(f"Parsing transaction data from Amazon (SYF) statement located at: '{pdf_file}'.")
    # Setting up lists for transaction and credit data
    transactions = []
    credits = []
    # Used to manage application state. When the header of the credits table in the PDF is discovered, 'tracking_credits' will flip to true
    # and the following lines analyzed will be added to the credits list if they fit the expected format of a transaction entry
    tracking_payments = False
    tracking_credits = False
    tracking_purchases = False
    tracking_fees = False
    tracking_interest = False
    # Parsing logic configuration variables specific to each type of bank statement 
    parse_keywords = {
        'payments': 'Payments -$', 
        'credits': 'Other Credits -$', 
        'purchases': 'Purchases and Other Debits', 
        'fees': 'Total Fees Charged This Period', 
        'interest': 'Total Interest Charged This Period'
        }
    skip_lines = ['(Continued on next page)', 'Transaction Detail (Continued)', 'Date Reference # Description Amount']
    ## Transaction and credit data placeholders
    transaction_data = []
    # Setting a years array to complete dates in the data
    year_map = {}  
    ## Complimentary Data to be Parsed
    bank_name = 'amazon-synchrony'
    account_holder = ''
    account_number = ''
    # Initialize pdf text variable
    text = ''
    
    with pdfplumber.open(str(pdf_file)) as pdf:
        for page in pdf.pages:
            text += page.extract_text()
    lines = text.split('\n')

    for i in range(len(lines)):
        if lines[i].startswith("Account Number") and (not account_holder or not account_number):
            account_holder = lines[i-1]
            account_number = lines[i][-4:]
        if lines[i].startswith("New Balance as of"):
            months = lines[i+2].split("to")
            months[0] = months[0].strip()
            months[1] = months[1].lstrip()
            months[0] = months[0].split(" ")[-1]
            year_map[months[0][0:3]] = months[0][-4:]
            year_map[months[1][0:3]] = months[1][-4:]

    for i in range(len(lines)):
        if any(lines[i].startswith(prefix) for prefix in parse_keywords.values()):
            tracking_payments, tracking_credits, tracking_purchases, tracking_fees, tracking_interest = _map_transaction_type(lines[i], parse_keywords)
        elif tracking_payments:
            if lines[i][0:3] in DATES:
                line_split = lines[i].split(" ")
                date_split= line_split.pop(0).split("/")
                date_split.append(year_map[date_split[0] + "/"])
                date = "/".join(date_split)
                amount = line_split.pop().replace("-", "").replace("$", "")
                line_split.pop(0) # Ignore the reference # column/data
                description = " ".join(line_split)
                category = ''
                transaction_type = 'Payment'
                while lines[i+1][0:3] not in DATES and not any(lines[i+1].startswith(prefix) for prefix in parse_keywords.values()): 
                    if any(lines[i+1].startswith(prefix) for prefix in skip_lines):
                        i += 1
                    else:
                        description += lines[i+1]
                        i += 1
                transaction_data.append([date, description, category, amount, transaction_type, bank_name, account_holder, account_number])
            elif any(lines[i+1].startswith(prefix) for prefix in parse_keywords.values()):
                tracking_payments, tracking_credits, tracking_purchases, tracking_fees, tracking_interest = _map_transaction_type(lines[i], parse_keywords)
        elif tracking_credits:
            if lines[i][0:3] in DATES:
                line_split = lines[i].split(" ")
                date_split= line_split.pop(0).split("/")
                date_split.append(year_map[date_split[0] + "/"])
                date = "/".join(date_split)
                amount = line_split.pop().replace("-", "").replace("$", "")
                line_split.pop(0) # Ignore the reference # column/data
                description = " ".join(line_split)
                category = ''
                transaction_type = 'Credit'
                while lines[i+1][0:3] not in DATES and not any(lines[i+1].startswith(prefix) for prefix in parse_keywords.values()): 
                    if any(lines[i+1].startswith(prefix) for prefix in skip_lines):
                        i += 1
                    else:
                        description += lines[i+1]
                        i += 1
                transaction_data.append([date, description, category, amount, transaction_type, bank_name, account_holder, account_number])
            elif any(lines[i].startswith(prefix) for prefix in parse_keywords.values()):
                tracking_payments, tracking_credits, tracking_purchases, tracking_fees, tracking_interest = _map_transaction_type(lines[i], parse_keywords)
        elif tracking_purchases:
            if lines[i][0:3] in DATES:
                line_split = lines[i].split(" ")
                date_split= line_split.pop(0).split("/")
                date_split.append(year_map[date_split[0] + "/"])
                date = "/".join(date_split)
                amount = line_split.pop().replace("-", "").replace("$", "")
                line_split.pop(0) # Ignore the reference # column/data
                description = " ".join(line_split)
                category = ''
                transaction_type = 'Purchase'
                while lines[i+1][0:3] not in DATES and not any(lines[i+1].startswith(prefix) for prefix in parse_keywords.values()): 
                    if any(lines[i+1].startswith(prefix) for prefix in skip_lines):
                        i += 1
                    else:
                        description += lines[i+1]
                        i += 1
                transaction_data.append([date, description, category, amount, transaction_type, bank_name, account_holder, account_number])
            elif any(lines[i].startswith(prefix) for prefix in parse_keywords.values()):
                tracking_payments, tracking_credits, tracking_purchases, tracking_fees, tracking_interest = _map_transaction_type(lines[i], parse_keywords)
        elif tracking_fees:
            if lines[i][0:3] in DATES:
                line_split = lines[i].split(" ")
                date_split= line_split.pop(0).split("/")
                date_split.append(year_map[date_split[0] + "/"])
                date = "/".join(date_split)
                amount = line_split.pop().replace("-", "").replace("$", "")
                line_split.pop(0) # Ignore the reference # column/data
                description = " ".join(line_split)
                category = ''
                transaction_type = 'Fee'
                while lines[i+1][0:3] not in DATES and not any(lines[i+1].startswith(prefix) for prefix in parse_keywords.values()): 
                    if any(lines[i+1].startswith(prefix) for prefix in skip_lines):
                        i += 1
                    else:
                        description += lines[i+1]
                        i += 1
                transaction_data.append([date, description, category, amount, transaction_type, bank_name, account_holder, account_number])
            elif any(lines[i].startswith(prefix) for prefix in parse_keywords.values()):
                tracking_payments, tracking_credits, tracking_purchases, tracking_fees, tracking_interest = _map_transaction_type(lines[i], parse_keywords)
        elif tracking_interest:
            if lines[i][0:3] in DATES:
                line_split = lines[i].split(" ")
                date_split= line_split.pop(0).split("/")
                date_split.append(year_map[date_split[0] + "/"])
                date = "/".join(date_split)
                amount = line_split.pop().replace("-", "").replace("$", "")
                line_split.pop(0) # Ignore the reference # column/data
                description = " ".join(line_split)
                category = ''
                transaction_type = 'Interest'
                while lines[i+1][0:3] not in DATES and "Year-to-Date Fees and Interest" not in lines[i+1]: 
                    if any(lines[i+1].startswith(prefix) for prefix in skip_lines):
                        i += 1
                    else:
                        description += lines[i+1]
                        i += 1
                transaction_data.append([date, description, category, amount, transaction_type, bank_name, account_holder, account_number])
            elif "Year-to-Date Fees and Interest" in lines[i]:
                tracking_payments, tracking_credits, tracking_purchases, tracking_fees, tracking_interest = [False] * 5


    # pdf_file.rename(f"C:\\Users\\{project path}\\processed_statements\\{pdf_file.parts[-2]}\\{pdf_file.parts[-1]}")
    
    return transaction_data

def parse_csv(csv_file):
    print(f"Parsing transaction data from Amazon (SYF) csv located at: '{csv_file}'.")
    # Initialize data list to hold csv data
    data = []
    ## Transaction data list, will be written to CSV
    transaction_data = []
    ## Complimentary Data to be Parsed
    bank_name = 'amazon-synchrony'
    account_holder = ''
    account_number = ''
    # Column/Index -> Field Mapping
    date_index = 0
    description_index = 4
    amount_index = 3

    # Read the CSV file
    with open(csv_file, mode='r', newline='') as infile:
        reader = csv.reader(infile)
        data = list(reader)[1:]

    for row in data:
        if float(row[amount_index]) < 0:
            date = row[date_index]
            amount = row[amount_index].replace("-", "")
            description = row[description_index]
            category = ''
            transaction_type = 'Credit/Payment'
            transaction_data.append([date, description, category, amount, transaction_type, bank_name, account_holder, account_number])
        else:
            date = row[date_index]
            amount = row[amount_index]
            description = row[description_index]
            category = ''
            transaction_type = 'Purchase'
            transaction_data.append([date, description, category, amount, transaction_type, bank_name, account_holder, account_number])

    # pdf_file.rename(f"C:\\Users\\{project path}\\processed_statements\\{pdf_file.parts[-2]}\\{pdf_file.parts[-1]}")

    return transaction_data

def write_csv(transactions_csv_file_path, transaction_data):
    # Open the file in write mode
    with open(transactions_csv_file_path, mode='a', newline='') as file:
        writer = csv.writer(file)
        writer.writerows(transaction_data)