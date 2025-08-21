import subprocess
import time
import json
import os
import signal
import requests
import random
from datetime import date, timedelta
from decimal import Decimal

# --- Configuration ---
BASE_URL = "http://127.0.0.1:8000"
UVICORN_COMMAND = ["uvicorn", "src.main:app"]
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.join(SCRIPT_DIR, "..")

# --- Helper Functions ---
def start_server():
    """Starts the FastAPI server in a subprocess."""
    print("Starting FastAPI server...")
    process = subprocess.Popen(UVICORN_COMMAND, cwd=PROJECT_ROOT, shell=True)
    time.sleep(5)  # Give the server some time to start
    print(f"Server started with PID: {process.pid}")
    return process

def stop_server(process):
    """Shuts down the FastAPI server process."""
    if process:
        print(f"Shutting down server with PID: {process.pid}...")
        if os.name == 'nt':
            subprocess.run(['taskkill', '/F', '/T', '/PID', str(process.pid)], check=True)
        else:
            os.kill(process.pid, signal.SIGTERM)
        try:
            process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            print("Process did not terminate in time.")
        print("Server shut down.")

def run_api_request(method: str, endpoint: str, data: dict = None):
    """Makes an API request and returns the JSON response."""
    url = f"{BASE_URL}{endpoint}"
    try:
        json_data = json.dumps(data, default=str) if data else None
        headers = {'Content-Type': 'application/json'} if json_data else None
        response = requests.request(method, url, data=json_data, headers=headers, timeout=10)
        response.raise_for_status()
        if not response.text:
            return None
        return response.json()
    except requests.exceptions.HTTPError as e:
        print(f"Error: HTTP {e.response.status_code} for {url}\nResponse: {e.response.text}")
        return None
    except requests.exceptions.RequestException as e:
        print(f"An unexpected error occurred: {e}")
        return None

def generate_investment_transactions(num_transactions: int, account_id: int):
    """Generates a list of sample InvestmentTransactionCreate objects."""
    transactions = []
    transaction_types = ["BUY", "SELL", "DIVIDEND", "REINVESTMENT"]
    symbols = ["AAPL", "GOOG", "MSFT", "AMZN", "TSLA"]

    for _ in range(num_transactions):
        trans_type = random.choice(transaction_types)
        symbol = random.choice(symbols)
        quantity = round(Decimal(random.uniform(1.0, 10.0)), 2) if trans_type in ["BUY", "SELL", "REINVESTMENT"] else None
        price_per_share = round(Decimal(random.uniform(100.0, 1000.0)), 2) if trans_type in ["BUY", "SELL", "REINVESTMENT"] else None
        total_amount = round(Decimal(random.uniform(50.0, 5000.0)), 2)

        transactions.append({
            "account_id": account_id,
            "transaction_type": trans_type,
            "symbol": symbol,
            "quantity": float(quantity) if quantity else None,
            "price_per_share": float(price_per_share) if price_per_share else None,
            "total_amount": float(total_amount),
            "fees": float(round(Decimal(random.uniform(0.0, 5.0)), 2)),
            "transaction_date": (date.today() - timedelta(days=random.randint(1, 365))).isoformat(),
            "description": f"Investment {trans_type} for {symbol}"
        })
    return transactions

def test_bulk_upload():
    """Tests the bulk investment transactions upload endpoint."""
    account_id = 1 # Assumed to exist

    print(f"\n--- Testing Bulk Investment Transactions Upload for account_id={account_id} ---")

    num_transactions_to_upload = 5

    transactions_data = generate_investment_transactions(num_transactions_to_upload, account_id)

    payload = {
        "transactions": transactions_data
    }

    response = run_api_request("POST", "/investments/transactions/bulk-upload", payload)

    if response:
        print(f"Successfully uploaded {len(response)} investment transactions.")
        for i, trans in enumerate(response):
            print(f"  Transaction {i+1}: ID={trans.get('investment_transaction_id')}, Type={trans.get('transaction_type')}, Symbol={trans.get('symbol')}, Amount={trans.get('total_amount')}")
    else:
        print("Bulk investment transactions upload failed.")

if __name__ == "__main__":
    server_process = None
    try:
        server_process = start_server()
        test_bulk_upload()
    except Exception as e:
        print(f"An error occurred during the test: {e}")
    finally:
        if server_process:
            stop_server(server_process)
