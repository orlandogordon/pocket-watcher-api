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

def generate_debt_payments(num_payments: int, loan_account_id: int, payment_source_account_id: int):
    """Generates a list of sample DebtPaymentCreate objects."""
    payments = []
    for _ in range(num_payments):
        payment_amount = round(Decimal(random.uniform(100.0, 1000.0)), 2)
        principal_amount = round(payment_amount * Decimal(random.uniform(0.5, 0.8)), 2)
        interest_amount = payment_amount - principal_amount

        payments.append({
            "loan_account_id": loan_account_id,
            "payment_source_account_id": payment_source_account_id,
            "payment_amount": float(payment_amount),
            "principal_amount": float(principal_amount),
            "interest_amount": float(interest_amount),
            "payment_date": (date.today() - timedelta(days=random.randint(1, 365))).isoformat(),
            "description": f"Debt payment {random.randint(100, 999)}"
        })
    return payments

def test_bulk_upload():
    """Tests the bulk debt payments upload endpoint."""
    loan_account_id = 1 # Assumed to exist
    payment_source_account_id = 1 # Assumed to exist

    print(f"\n--- Testing Bulk Debt Payments Upload for loan_account_id={loan_account_id} ---")

    num_payments_to_upload = 3

    payments_data = generate_debt_payments(num_payments_to_upload, loan_account_id, payment_source_account_id)

    payload = {
        "payments": payments_data
    }

    response = run_api_request("POST", "/debt/payments/bulk-upload", payload)

    if response:
        print(f"Successfully uploaded {len(response)} debt payments.")
        for i, payment in enumerate(response):
            print(f"  Payment {i+1}: ID={payment.get('payment_id')}, Amount={payment.get('payment_amount')}")
    else:
        print("Bulk debt payments upload failed.")

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
