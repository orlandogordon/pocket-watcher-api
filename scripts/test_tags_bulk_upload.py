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

def run_api_request(method: str, endpoint: str, data: dict | list = None):
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

def test_bulk_tag_transactions():
    """Tests the bulk tag transactions endpoint."""
    # Assumes these IDs exist in your database. Adjust as needed.
    tag_id_to_apply = 1 
    transaction_ids_to_tag = [1, 2, 3, 4, 5] 

    print(f"\n--- Testing Bulk Tag Transactions for tag_id={tag_id_to_apply} ---")

    endpoint = f"/tags/transactions/bulk-tag?tag_id={tag_id_to_apply}"
    payload = transaction_ids_to_tag

    response = run_api_request("POST", endpoint, payload)

    if response:
        print(f"Bulk tag transactions successful: {response.get('message')}")
    else:
        print("Bulk tag transactions failed.")

if __name__ == "__main__":
    server_process = None
    try:
        server_process = start_server()
        test_bulk_tag_transactions()
    except Exception as e:
        print(f"An error occurred during the test: {e}")
    finally:
        if server_process:
            stop_server(server_process)
