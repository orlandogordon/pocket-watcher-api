import os
import re
import csv
from PIL import Image
import pdfplumber
from pdfplumber.table import Table, Row, Column
from pathlib import Path
import pandas as pd

import pdfplumber.structure

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
    print(f"Parsing transaction data from TD Bank statement: '{pdf_file}'.")
    # Used to manage application state. When the header of the deposits table in the PDF is discovered, 'tracking_depositis' will flip to true
    # and the following lines analyzed will be added to the deposits list if they fit the expected format of a transaction entry
    tracking_deposits = False
    tracking_purchases = False
    # Parsing logic configuration variables specific to each type of bank statement 
    start_parse_purchases_keywords = ['Payments', 'ElectronicPayments', 'ElectronicPayments(continued)', 'OtherWithdrawals']
    end_parse_purchases_keywords = ['Call 1-800-937-2000', 'Subtotal:']
    start_parse_deposits_keywords = ['Deposits', 'ElectronicDeposits', 'ElectronicDeposits(continued)', 'OtherCredits']
    end_parse_deposits_keywords = ['Call 1-800-937-2000', 'Subtotal:']  
    # Setting a years array to complete dates in the data
    months = []
    years = []
    # Complimentary Data to be Parsed
    bank_name = 'tdbank'
    account_holder = ''
    account_number = ''
    # Lines that will be used to create each table and the parsed data from the tables
    horizontal_lines = []
    vertical_lines = []
    deposit_data = []
    purchase_data = []

    with pdfplumber.open(str(pdf_file)) as pdf:
        pdf_shortcut = str(pdf_file).replace('.pdf', '')
        lines_dict = {page : [pdf.pages[page].extract_text_lines()] for page in range(len(pdf.pages))}
        words_dict = {page : [pdf.pages[page].extract_words()] for page in range(len(pdf.pages))}
        pdfplumber_text = {f'page {page}' : [repr(pdf.pages[page].extract_text())] for page in range(len(pdf.pages))}
        pdf_extract_dict = {page : pdf.pages[page] for page in range(len(pdf.pages))} 
        horizontal_lines = []
        vertical_lines = []
        deposit_tables = []
        purchase_tables = []

        for page in lines_dict:
            for line in lines_dict[page][0]:
                if "StatementPeriod:" in line['text']:
                    months = [month[0:3] for month in line['text'].split(" ")[-1].split('-')]
                    months = [DATES[month] for month in months]
                    years = [f"/{date[-4:]}" for date in line['text'].split(" ")[-1].split('-')]
                elif " Account# " in line['text']:
                    text_split = line['text'].split(" ")
                    account_holder=text_split[0]
                    account_number=text_split[-1][-4:]

        for page in lines_dict:
            for line in lines_dict[page][0]:
                if line['text'] in start_parse_deposits_keywords:
                    tracking_deposits = True
                elif line['text'] in start_parse_purchases_keywords:
                    tracking_purchases = True
                elif line['text'] == 'POSTINGDATE DESCRIPTION AMOUNT' and (tracking_deposits or tracking_purchases):
                    vertical_lines.append(line['x0'])
                    trailing_word = ''
                    for char in line['chars']:
                        if trailing_word == 'POSTINGDATE':
                            vertical_lines.append(char['x0']-5)
                            trailing_word = ''
                        elif trailing_word == 'DESCRIPTION':
                            vertical_lines.append(char['x0']-50)
                            trailing_word = ''
                        trailing_word += char['text']
                    vertical_lines.append(line['x1'])
                if (tracking_deposits or tracking_purchases) and re.match(r'^\d{2}/\d{2}.*\d.\d{2}$', str(line['text'])):
                    horizontal_lines.append(line['top'])
                if (tracking_deposits or tracking_purchases) and any(line['text'].startswith(prefix) for prefix in [*end_parse_purchases_keywords, *end_parse_deposits_keywords]):
                    horizontal_lines.append(line['top'])
                    cells = []
                    
                    for i in range(len(horizontal_lines) - 1):
                        for j in range(len(vertical_lines)-1):
                            cells.append([vertical_lines[j],  horizontal_lines[i], vertical_lines[j+1],  horizontal_lines[i+1]])
                    
                    if tracking_deposits:
                        deposit_tables.append(Table(pdf.pages[page], tuple(cells)))
                    elif tracking_purchases:
                        purchase_tables.append(Table(pdf.pages[page], tuple(cells)))

                    #  Reset tracking variables
                    vertical_lines = []
                    horizontal_lines = []
                    tracking_deposits = False
                    tracking_purchases = False
        
        # Create debugging images and concatenate them
        images = []
        prefix = f'finetune_tables_for_{os.path.basename(pdf_shortcut)}'
        for page in pdf.pages:
            im = page.to_image(resolution=120)
            for table in deposit_tables:
                if page == table.page:
                    for cell in table.cells:
                        im.draw_rect(cell, stroke='red') 
            for table in purchase_tables:
                if page == table.page:
                    for cell in table.cells:
                        im.draw_rect(cell, stroke='red')

            im.save(f"output/{prefix}_page-{page.page_number}.png")

        for file_name in os.listdir('output'):
            if file_name.startswith(prefix) and file_name.lower().endswith(('.png', '.jpg', '.jpeg', '.gif', '.bmp')):
                images.append(Image.open(os.path.join('output', file_name)))

        widths, heights = zip(*(i.size for i in images))

        max_width = max(widths)
        total_height = sum(heights)

        new_image = Image.new('RGB', (max_width, total_height))

        y_offset = 0
        for im in images:
            new_image.paste(im, (0,y_offset))
            y_offset += im.size[1]
            im.close()
            os.remove(os.path.join(im.filename))

        new_image.save(f'output/{os.path.basename(pdf_shortcut)}.png')

    for table in deposit_tables:
        deposit_data.extend(table.extract())

    for table in purchase_tables:
        purchase_data.extend(table.extract())

    # Refine transaction data and write to CSV
    for i in range(len(deposit_data)):
        date = deposit_data[i][0]
        description = deposit_data[i][1]
        amount = deposit_data[i][2]

        if date[0:3] == months[0]:
            date += years[0]
        elif date[0:3] == months[1]:
            date += years[1]
        else:
            print(f"ERROR: Date format error: {date}")
        
        deposit_data[i] = [date, description, '', amount, 'Deposit', bank_name, account_holder, account_number]

    for i in range(len(purchase_data)):
        date = purchase_data[i][0]
        description = purchase_data[i][1]
        amount = purchase_data[i][2]

        if date[0:3] == months[0]:
            date += years[0]
        elif date[0:3] == months[1]:
            date += years[1]
        else: 
            print(f"ERROR: Date format error: {date}")
        
        purchase_data[i] = [date, description, '', amount, 'Purchase', bank_name, account_holder, account_number]
        
    # pdf_file.rename(f"C:\\Users\\{project path}\\processed_statements\\{pdf_file.parts[-2]}\\{pdf_file.parts[-1]}")
    return deposit_data + purchase_data

def parse_statement_legacy(pdf_file):
    print(f"Parsing transaction/deposit data from TD Bank statement: '{pdf_file}'.")
    # Setting up lists for transaction and deposit data
    transactions = []
    deposits = []
    # Used to manage application state. When the header of the deposits table in the PDF is discovered, 'tracking_depositis' will flip to true
    # and the following lines analyzed will be added to the deposits list if they fit the expected format of a transaction entry
    tracking_deposits = False
    tracking_transactions = False
    # Parsing logic configuration variables specific to each type of bank statement 
    start_parse_transactions_keywords = ['ElectronicPayments', 'ElectronicPayments(continued)']
    end_parse_transactions_keywords = ['Call 1-800-937-2000', 'Subtotal:']
    start_parse_deposits_keywords = ['ElectronicDeposits', 'ElectronicDeposits(continued)']
    end_parse_deposits_keywords = ['Call 1-800-937-2000', 'Subtotal:']
    ## Transaction data CSV headers
    transaction_data = []
    deposit_data = []    
    # Setting a years array to complete dates in the data
    months = []
    years = []
    ## Complimentary Data to be Parsed
    bank_name = 'tdbank'
    account_holder = ''
    account_number = ''
    # Text that will hold the parsed pdf
    text = ''
    ## Testing Table Parsing
    daily_account_activity = {'top': 10000, 'bottom': 10000}
    electronic_deposits = 0
    electronic_payments = 0
    horizontal_lines = []
    vertical_lines = []

    with pdfplumber.open(str(pdf_file)) as pdf:
        for page in pdf.pages:
            text += page.extract_text()
           
    lines = text.split('\n')
    
    for i in range(len(lines)):
        if "StatementPeriod:" in lines[i]:
            months = [month[0:3] for month in lines[i].split(" ")[-1].split('-')]
            months = [DATES[month] for month in months]
            years = [f"/{date[-4:]}" for date in lines[i].split(" ")[-1].split('-')]
        elif " Account# " in lines[i]:
            text_split = lines[i].split(" ")
            account_holder=text_split[0]
            account_number=text_split[-1][-4:]

    for i in range(len(lines)):
        if lines[i] in start_parse_transactions_keywords:
            tracking_transactions = True
        elif lines[i] in start_parse_deposits_keywords:
            tracking_deposits =True    
        elif lines[i][0:3] in DATES.values() and (tracking_transactions or tracking_deposits):
            if lines[i+1][0:3] not in DATES.values() and not any(lines[i+1].startswith(prefix) for prefix in [*end_parse_transactions_keywords, *end_parse_deposits_keywords]):                        
                entry = lines[i]
                entry_split=entry.split(',')
                insert = entry_split[-1].split(' ')[0] + lines[i+1] + " " + entry_split[-1].split(' ')[-1]
                entry_split[-1] = insert
                entry = " , ".join(entry_split)
                lines[i] = entry
            if tracking_deposits: deposits.append(lines[i])
            if tracking_transactions: transactions.append(lines[i])
        elif any(lines[i].startswith(prefix) for prefix in [*end_parse_transactions_keywords, *end_parse_deposits_keywords]):
            tracking_deposits = False
            tracking_transactions = False

    # Refine transaction data and write to CSV
    for transaction in transactions:
        transaction_split = transaction.split(' ')
        date = transaction_split.pop(0)
        if date[0:2] == months[0]:
            date += years[0]
        else:
            date += years[1]
        amount = transaction_split.pop()
        transaction_split = " ".join(transaction_split).split(',')
        description = transaction_split.pop()
        transaction_data.append([date, description, amount, bank_name, account_holder, account_number])

    # Refine deposit data and write to CSV
    for deposit in deposits:
        deposit_split = deposit.split(' ')
        date = deposit_split.pop(0)
        if date[0:2] == months[0]:
            date += years[0]
        else:
            date += years[1]
        amount = deposit_split.pop()
        description = " ".join(deposit_split)
        deposit_data.append([date, description, amount, bank_name, account_holder, account_number])

    # pdf_file.rename(f"C:\\Users\\{project path}\\processed_statements\\{pdf_file.parts[-2]}\\{pdf_file.parts[-1]}")
    
    return transaction_data + deposit_data

def parse_csv(csv_file):
    print(f"Parsing transaction data from TD Bank csv located at: '{csv_file}'.")
    # Initialize data list to hold csv data
    data = []
    ## Transaction data CSV headers
    transaction_data = []
    ## Complimentary Data to be Parsed
    bank_name = 'tdbank'
    account_holder = ''
    account_number = ''
    # Column/Index -> Field Matching
    transaction_type_index = 3
    date_index = 0
    description_index = 4
    debit_amount_index = 5
    credit_amount_index = 6
    account_number_index = 2

    # Read the CSV file
    with open(csv_file, mode='r', newline='') as infile:
        reader = csv.reader(infile)
        data = list(reader)[1:]

    for row in data:
        if row[transaction_type_index] == 'CREDIT':
            date_parts = row[date_index].split("-")
            date = f"{date_parts[1]}/{date_parts[2]}/{date_parts[0]}"
            amount = row[credit_amount_index]
            description = row[description_index]
            category = ''
            transaction_type = 'Deposit'
            account_number = row[account_number_index][-4:]
            transaction_data.append([date, description, category, amount, transaction_type, bank_name, account_holder, account_number])
        else:
            date_parts = row[date_index].split("-")
            date = f"{date_parts[1]}/{date_parts[2]}/{date_parts[0]}"
            amount = row[debit_amount_index]
            description = row[description_index]
            category = ''
            transaction_type = 'Purchase'
            account_number = row[account_number_index][-4:]
            transaction_data.append([date, description, category, amount, transaction_type, bank_name, account_holder, account_number])

    # pdf_file.rename(f"C:\\Users\\{project path}\\processed_statements\\{pdf_file.parts[-2]}\\{pdf_file.parts[-1]}")

    return transaction_data

def write_csv(transactions_csv_file_path, transaction_data):
    # Open the file in write mode
    with open(transactions_csv_file_path, mode='a', newline='') as file:
        writer = csv.writer(file)
        writer.writerows(transaction_data)