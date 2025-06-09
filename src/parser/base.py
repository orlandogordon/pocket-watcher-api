import sys
import csv
from pathlib import Path
from . import amex, tdbank, amzn_syf, schwab, tdameritrade, ameriprise

class ParserService:
    def __init__(self):
        self.parsers = {
            'amex': amex,
            'tdbank': tdbank,
            'amzn_syf': amzn_syf,
            'schwab': schwab,
            'tdameritrade': tdameritrade,
            'ameriprise': ameriprise
        }

    def parse_csv(self, file_path):
        for parser in self.parsers.values():
            if parser.can_parse_csv(file_path):
                return parser.parse_csv(file_path)
        raise ValueError(f"No suitable parser found for {file_path}")

    def parse_statement(self, file_path):
        for parser in self.parsers.values():
            if parser.can_parse_statement(file_path):
                return parser.parse_statement(file_path)
        raise ValueError(f"No suitable parser found for {file_path}")

def ParserProcess():
    # sys.path.append(str(Path(__file__).parent))
    root_dir=Path(__file__).parent.parent.parent
    print(f"Root directory: {root_dir}")
    print(f"Root directory: {Path(f"{root_dir}/input/statements")}")
    breakpoint()
    statements_path = Path(f"{root_dir}/input/statements")
    transaction_csv_path = Path(f"{root_dir}/input/transaction_csv")

    transactions_path = root_dir.joinpath(f"output/transactions.csv")
    investments_path = root_dir.joinpath(f"output/brokerage_transactions.csv")

    # Open the file in write mode
    with open(transactions_path, mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerows([['Date', 'Description', 'Category', 'Amount', 'Transaction Type', 'Bank Name', 'Account Holder', 'Account Number']])  # Consider adding 'Category', 'Tags', 'Account Nickname', 'Transaction Match' at the DB level    
        print(f"Transaction data CSV file created at: '{transactions_path}'.")
    with open(investments_path, mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerows([['Date', 'Transaction Type', 'Symbol', 'Description', 'Quantity', 'Price', 'Amount', 'Brokerage Name', 'Account Number']])  # Consider adding 'Category', 'Tags', 'Account Nickname', 'Transaction Match' at the DB level
        print(f"Brokerage Transaction data CSV file created at: '{investments_path}'.")
      
    print('*'*100)
    print("Beginning Transaction CSV Parsing Process")
    print('*'*100)

    for csv_file in transaction_csv_path.glob('./tdbank/*.csv'):
        parsed_data = tdbank.parse_csv(csv_file)
        tdbank.write_csv(transactions_path, parsed_data)
    breakpoint()
    for csv_file in transaction_csv_path.glob('./amex/*.csv'):
        parsed_data = amex.parse_csv(csv_file)
        amex.write_csv(transactions_path, parsed_data)

    for csv_file in transaction_csv_path.glob('./amzn-synchrony/*.csv'):
        parsed_data = amzn_syf.parse_csv(csv_file)
        amzn_syf.write_csv(transactions_path, parsed_data)

    print('*'*100)
    print("Beginning Bank Statement Parsing Process")
    print('*'*100)

    for pdf_file in statements_path.glob('./tdbank/*.pdf'):
        parsed_data = tdbank.parse_statement(pdf_file)
        tdbank.write_csv(transactions_path, parsed_data)
    for pdf_file in statements_path.glob('./amex/*.pdf'):
        parsed_data = amex.parse_statement(pdf_file)
        amex.write_csv(transactions_path, parsed_data)
    for pdf_file in statements_path.glob('./amzn-synchrony/*.pdf'):
        parsed_data = amzn_syf.parse_statement(pdf_file)
        amzn_syf.write_csv(transactions_path, parsed_data)

    print('*'*100)
    print("Beginning Brokerage Statement Parsing Process")
    print('*'*100)

    for pdf_file in statements_path.glob('./schwab/*.pdf'):
        parsed_data = schwab.parse_statement(pdf_file)
        schwab.write_csv(investments_path, parsed_data)

    for pdf_file in statements_path.glob('./tdameritrade/*.pdf'):
        parsed_data = tdameritrade.parse_statement(pdf_file)
        tdameritrade.write_csv(investments_path, parsed_data)

    print('*'*100)
    print("Beginning Brokerage CSV Parsing Process")
    print('*'*100)

    for csv_file in transaction_csv_path.glob('./schwab/*.csv'):
        parsed_data = schwab.parse_csv(csv_file)
        ameriprise.write_csv(investments_path, parsed_data)

    for csv_file in transaction_csv_path.glob('./ameriprise/*.csv'):
        parsed_data = ameriprise.parse_csv(csv_file)
        ameriprise.write_csv(investments_path, parsed_data)
