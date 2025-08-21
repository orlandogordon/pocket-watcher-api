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
    # Use shell=True on Windows for Popen to find uvicorn
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

def generate_transactions(num_transactions: int, account_id: int, category_id: int = 1):
    """Generates a list of sample TransactionCreate objects."""
    transactions = []
    for _ in range(num_transactions):
        is_debit = random.choice([True, False])
        amount = round(Decimal(random.uniform(5.0, 500.0)), 2)
        transaction_type = "DEBIT" if is_debit else "CREDIT"
        
        transactions.append({
            "account_id": account_id,
            "transaction_date": (date.today() - timedelta(days=random.randint(1, 365))).isoformat(),
            "amount": float(-amount) if is_debit else float(amount), # Convert Decimal to float for JSON
            "transaction_type": transaction_type,
            "description": f"Test transaction {random.randint(1000, 9999)}",
            "merchant_name": f"Merchant {random.choice(['A', 'B', 'C'])}",
            "category_id": category_id, # Assumes category with ID 1 exists
            "source_type": "API"
        })
    return transactions

def test_bulk_upload():
    """Tests the bulk transaction upload endpoint."""
    user_id = 1 # Assumed to exist
    account_id = 1 # Assumed to exist
    category_id = 1 # Assumed to exist. Adjust if your seeded data uses a different ID.

    print(f"\n--- Testing Bulk Transaction Upload for user_id={user_id}, account_id={account_id} ---")

    num_transactions_to_upload = 5

    transactions_data = generate_transactions(num_transactions_to_upload, account_id, category_id)

    payload = {
        "account_id": account_id,
        "transactions": transactions_data,
        "source_type": "API"
    }

    response = run_api_request("POST", "/transactions/bulk-upload/", payload)

    if response:
        print(f"Successfully uploaded {len(response)} transactions.")
        for i, trans in enumerate(response):
            print(f"  Transaction {i+1}: ID={trans.get('id')}, Amount={trans.get('amount')}, Desc='{trans.get('description')}'")
    else:
        print("Bulk upload failed.")

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
