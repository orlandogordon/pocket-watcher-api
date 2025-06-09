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
    print(f"Parsing transaction data from Amex statement located at: '{pdf_file}'.")
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
    ## Transaction and credit data placeholders
    transaction_data = []
    # Setting a years array to complete dates in the data
    year_map = {}  
    ## Complimentary Data to be Parsed
    bank_name = 'amex'
    account_holder = ''
    account_number = ''
    # Initialize pdf text variable
    text = ''
    
    with pdfplumber.open(str(pdf_file)) as pdf:
        for page in pdf.pages:
            text += page.extract_text()
    lines = text.split('\n')
    
    screen_reader_optimized = True if 'Screen Reader Optimized' in lines[1] else False 

    if screen_reader_optimized:
        parse_keywords = {
            'payments': "Payments Details", 
            'credits': "Credits Details", 
            'purchases': "New Charges Details", 
            'fees': "Fees", 
            'interest': "Interest Charged"
        }
        for i in range(len(lines)):
            if "Prepared for" in lines[i]:
                account_holder = lines[i+1]
                account_number = lines[i+2].split('-')[-1]
                break
        for i in range(len(lines)):
            if any(lines[i].startswith(prefix) for prefix in parse_keywords.values()):
                tracking_payments, tracking_credits, tracking_purchases, tracking_fees, tracking_interest = _map_transaction_type(lines[i], parse_keywords)
            elif tracking_payments:
                if lines[i][0:3] in DATES:
                    line_split = lines[i].split(" ")
                    date = line_split.pop(0).replace("*","")
                    amount = line_split.pop().replace("-", "").replace("$", "").replace("⧫", "")
                    description = " ".join(line_split)
                    category = ''
                    transaction_type = 'Payment'
                    transaction_data.append([date, description, category, amount, transaction_type, bank_name, account_holder, account_number])
                elif any(lines[i].startswith(prefix) for prefix in parse_keywords.values()):
                    tracking_payments, tracking_credits, tracking_purchases, tracking_fees, tracking_interest = _map_transaction_type(lines[i], parse_keywords)
            elif tracking_credits:
                if lines[i][0:3] in DATES:
                    line_split = lines[i].split(" ")
                    date = line_split.pop(0).replace("*","")
                    amount = line_split.pop().replace("-", "").replace("$", "").replace("⧫", "")
                    description = " ".join(line_split)
                    category = ''
                    transaction_type = 'Credit'
                    transaction_data.append([date, description, category, amount, transaction_type, bank_name, account_holder, account_number])
                elif any(lines[i].startswith(prefix) for prefix in parse_keywords.values()):
                    tracking_payments, tracking_credits, tracking_purchases, tracking_fees, tracking_interest = _map_transaction_type(lines[i], parse_keywords)
            elif tracking_purchases:
                if lines[i][0:3] in DATES:
                    line_split = lines[i].split(" ")
                    date = line_split.pop(0)
                    amount = line_split.pop().replace("-", "").replace("$", "").replace("⧫", "")
                    description = " ".join(line_split)
                    category = ''
                    transaction_type = 'Purchase'
                    transaction_data.append([date, description, category, amount, transaction_type, bank_name, account_holder, account_number])
                elif any(lines[i].startswith(prefix) for prefix in parse_keywords.values()):
                    tracking_payments, tracking_credits, tracking_purchases, tracking_fees, tracking_interest = _map_transaction_type(lines[i], parse_keywords)
            elif tracking_fees:
                if lines[i][0:3] in DATES:
                    line_split = lines[i].split(" ")
                    date = line_split.pop(0)
                    amount = line_split.pop().replace("-", "").replace("$", "").replace("⧫", "")
                    description = " ".join(line_split)
                    category = ''
                    transaction_type = 'Fee'
                    transaction_data.append([date, description, category, amount, transaction_type, bank_name, account_holder, account_number])
                elif any(lines[i].startswith(prefix) for prefix in parse_keywords.values()):
                    tracking_payments, tracking_credits, tracking_purchases, tracking_fees, tracking_interest = _map_transaction_type(lines[i], parse_keywords)
            elif tracking_interest:
                if lines[i][0:3] in DATES:
                    line_split = lines[i].split(" ")
                    date = line_split.pop(0)
                    amount = line_split.pop().replace("-", "").replace("$", "")
                    description = " ".join(line_split)
                    category = ''
                    transaction_type = 'Interest'
                    transaction_data.append([date, description, category, amount, transaction_type, bank_name, account_holder, account_number])
                elif "Interest Charge Calculation" in lines[i]:
                    tracking_payments, tracking_credits, tracking_purchases, tracking_fees, tracking_interest = [False] * 5
    else:
        parse_keywords = {
            'payments': "Payments t Amount", 
            'credits': "Credits Amount", 
            'purchases': "Detail - denotes Pay Over Time and/or Cash Advance activity", 
            'fees': "Fees - denotes Pay Over Time and/or Cash Advance activity", 
            'interest': "Interest Charged"
        }
        for i in range(len(lines)):
            if "Customer Care: " in lines[i]:
                text_split = lines[i].split(' ')
                while text_split[0] != 'Customer':
                    account_holder+=text_split.pop(0)
            elif "Account Ending" in lines[i]:
                account_number = lines[i].split('-')[1][0:5]
                break
        for i in range(len(lines)):
            if any(lines[i].startswith(prefix) for prefix in parse_keywords.values()):
                tracking_payments, tracking_credits, tracking_purchases, tracking_fees, tracking_interest = _map_transaction_type(lines[i], parse_keywords)
            elif tracking_payments:
                if lines[i][0:3] in DATES:
                    line_split = lines[i].split(" ")
                    date = line_split.pop(0).replace("*","")
                    amount = line_split.pop().replace("-", "").replace("$", "").replace("⧫", "")
                    description = " ".join(line_split)
                    category = ''
                    transaction_type = 'Payment'
                    transaction_data.append([date, description, category, amount, transaction_type, bank_name, account_holder, account_number])
                elif any(lines[i].startswith(prefix) for prefix in parse_keywords.values()):
                    tracking_payments, tracking_credits, tracking_purchases, tracking_fees, tracking_interest = _map_transaction_type(lines[i], parse_keywords)
            elif tracking_credits:
                if lines[i][0:3] in DATES:
                    line_split = lines[i].split(" ")
                    date = line_split.pop(0).replace("*","")
                    amount = line_split.pop().replace("-", "").replace("$", "").replace("⧫", "")
                    description = " ".join(line_split)
                    category = ''
                    transaction_type = 'Credit'
                    transaction_data.append([date, description, category, amount, transaction_type, bank_name, account_holder, account_number])
                elif any(lines[i].startswith(prefix) for prefix in parse_keywords.values()):
                    tracking_payments, tracking_credits, tracking_purchases, tracking_fees, tracking_interest = _map_transaction_type(lines[i], parse_keywords)
            elif tracking_purchases:
                if lines[i][0:3] in DATES:
                    line_split = lines[i].split(" ")
                    date = line_split.pop(0)
                    amount = line_split.pop().replace("-", "").replace("$", "").replace("⧫", "")
                    description = " ".join(line_split)
                    category = ''
                    transaction_type = 'Purchase'
                    transaction_data.append([date, description, category, amount, transaction_type, bank_name, account_holder, account_number])
                elif any(lines[i].startswith(prefix) for prefix in parse_keywords.values()):
                    tracking_payments, tracking_credits, tracking_purchases, tracking_fees, tracking_interest = _map_transaction_type(lines[i], parse_keywords)
            elif tracking_fees:
                if lines[i][0:3] in DATES:
                    line_split = lines[i].split(" ")
                    date = line_split.pop(0)
                    amount = line_split.pop().replace("-", "").replace("$", "").replace("⧫", "")
                    description = " ".join(line_split)
                    category = ''
                    transaction_type = 'Fee'
                    transaction_data.append([date, description, category, amount, transaction_type, bank_name, account_holder, account_number])
                elif any(lines[i].startswith(prefix) for prefix in parse_keywords.values()):
                    tracking_payments, tracking_credits, tracking_purchases, tracking_fees, tracking_interest = _map_transaction_type(lines[i], parse_keywords)
            elif tracking_interest:
                if lines[i][0:3] in DATES:
                    line_split = lines[i].split(" ")
                    date = line_split.pop(0)
                    amount = line_split.pop().replace("-", "").replace("$", "")
                    description = " ".join(line_split)
                    category = ''
                    transaction_type = 'Interest'
                    transaction_data.append([date, description, category, amount, transaction_type, bank_name, account_holder, account_number])
                elif "Year-to-Date Fees and Interest" in lines[i]:
                    tracking_payments, tracking_credits, tracking_purchases, tracking_fees, tracking_interest = [False] * 5

    # pdf_file.rename(f"C:\\Users\\{project path}\\processed_statements\\{pdf_file.parts[-2]}\\{pdf_file.parts[-1]}")

    result = transaction_data

    return result

def parse_csv(csv_file):
    print(f"Parsing transaction data from AMEX csv located at: '{csv_file}'.")
    # Initialize data list to hold csv data
    data = []
    ## Transaction data CSV headers
    transaction_data = []
    credit_data = []    
    ## Complimentary Data to be Parsed
    bank_name = 'amex'
    account_holder = ''
    account_number = ''
    # Column/Index -> Field Matching
    date_index = 0
    description_index = 1
    amount_index = 2

    # Read the CSV file
    with open(csv_file, mode='r', newline='') as infile:
        reader = csv.reader(infile)
        data = list(reader)[1:]

    for row in data:
        if float(row[amount_index]) < 0:
            date = row[date_index]
            amount = row[amount_index].replace("-", "")
            description = row[description_index] if len(row) == 3 else row[description_index+1]
            category = ''
            transaction_type = 'Credit'
            credit_data.append([date, description, category, amount, transaction_type, bank_name, account_holder, account_number])
        else:
            date = row[date_index]
            amount = row[amount_index]
            description = row[description_index] if len(row) == 3 else row[description_index+1]
            category = ''
            transaction_type = 'Purchase'
            transaction_data.append([date, description, category, amount, transaction_type, bank_name, account_holder, account_number])

    # pdf_file.rename(f"C:\\Users\\{project path}\\processed_statements\\{pdf_file.parts[-2]}\\{pdf_file.parts[-1]}")

    result = credit_data + transaction_data

    return result

def write_csv(transactions_csv_file_path, transaction_data):
    # Open the file in write mode
    with open(transactions_csv_file_path, mode='a', newline='') as file:
        writer = csv.writer(file)
        writer.writerows(transaction_data)