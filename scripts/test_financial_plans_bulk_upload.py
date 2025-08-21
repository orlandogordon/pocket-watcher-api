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

def generate_financial_plan_entries(num_entries: int, start_category_id: int = 1):
    """Generates a list of sample FinancialPlanEntryCreate objects."""
    entries = []
    for i in range(num_entries):
        entries.append({
            "category_id": start_category_id + i, # Assumes categories with these IDs exist
            "monthly_amount": float(round(Decimal(random.uniform(50.0, 500.0)), 2))
        })
    return entries

def test_bulk_upload():
    """Tests the bulk financial plan entries upload endpoint."""
    plan_id = 1 # Assumed to exist
    start_category_id = 1 # Assumed to exist. Adjust if your seeded data uses a different ID.

    print(f"\n--- Testing Bulk Financial Plan Entries Upload for plan_id={plan_id} ---")

    num_entries_to_upload = 3

    entries_data = generate_financial_plan_entries(num_entries_to_upload, start_category_id)

    payload = {
        "entries": entries_data
    }

    response = run_api_request("POST", f"/financial_plans/{plan_id}/entries/bulk-upload", payload)

    if response:
        print(f"Successfully uploaded {len(response)} financial plan entries.")
        for i, entry in enumerate(response):
            print(f"  Entry {i+1}: ID={entry.get('entry_id')}, Monthly Amount={entry.get('monthly_amount')}")
    else:
        print("Bulk financial plan entries upload failed.")


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
