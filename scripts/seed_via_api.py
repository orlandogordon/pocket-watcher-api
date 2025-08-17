import subprocess
import time
import json
import os
import signal
import requests
import random
from decimal import Decimal
from datetime import date, timedelta
from faker import Faker

# --- Configuration ---
BASE_URL = "http://127.0.0.1:8000"
UVICORN_COMMAND = ["uvicorn", "src.main:app"]
DB_URL = os.environ.get("DATABASE_URL", "sqlite:///test.db")

fake = Faker()

# --- Helper Function for API Requests ---
def run_api_request(method: str, endpoint: str, data: dict = None):
    """Makes an API request and returns the JSON response."""
    url = f"{BASE_URL}{endpoint}"
    try:
        # The default json encoder in requests cannot handle Decimal, so we need a custom one
        json_data = json.dumps(data, default=str) if data else None
        headers = {'Content-Type': 'application/json'} if json_data else None
        response = requests.request(method, url, data=json_data, headers=headers, timeout=10)
        response.raise_for_status()
        if not response.text:
            return None
        return response.json()
    except requests.exceptions.HTTPError as e:
        print(f"Error: HTTP {e.response.status_code} for {url}\nResponse: {e.response.text}")
        # breakpoint()
        return None
    except requests.exceptions.RequestException as e:
        print(f"An unexpected error occurred: {e}")
        # breakpoint()
        return None

def seed_categories():
    print("--- Seeding Categories ---")
    categories_structure = {
        "Income": ["Paycheck", "Bonus", "Freelance"],
        "Housing": ["Rent", "Mortgage", "Utilities"],
        "Transportation": ["Gas", "Public Transit", "Car Insurance"],
        "Food": ["Groceries", "Restaurants", "Coffee Shops"],
        "Personal Care": ["Haircut", "Pharmacy"],
        "Entertainment": ["Movies", "Concerts", "Streaming"],
        "Shopping": ["Clothing", "Electronics", "Gifts"],
        "Debt Payment": ["Credit Card", "Student Loan"],
        "Investments": ["Stock Purchase", "Retirement"],
        "Transfers": [],
    }
    categories_map = {}
    for cat_name, sub_cat_names in categories_structure.items():
        parent_cat = run_api_request("POST", "/categories/", {"name": cat_name})
        if parent_cat:
            categories_map[parent_cat['name']] = parent_cat
            categories_map[parent_cat['id']] = []
            for sub_cat_name in sub_cat_names:
                child_cat = run_api_request("POST", "/categories/", {"name": sub_cat_name, "parent_category_id": parent_cat['id']})
                if child_cat:
                    categories_map[parent_cat['id']].append(child_cat['id'])
    return categories_map

def seed_transactions(accounts, categories_map):
    print("--- Seeding Transactions ---")
    transactions = []
    if accounts.get("CHECKING"):
        for _ in range(200):
            # Exclude "Transfers" category from random selection for regular transactions
            non_transfer_categories = {k: v for k, v in categories_map.items() if isinstance(k, int)}
            parent_cat_id = random.choice(list(non_transfer_categories.keys()))
            
            trans_data = {
                "account_id": accounts["CHECKING"]["id"],
                "transaction_date": fake.date_between(start_date="-3y", end_date="today").isoformat(),
                "amount": round(random.uniform(-800, 800), 2),
                "transaction_type": "DEBIT" if random.random() > 0.4 else "CREDIT",
                "description": fake.bs(), "merchant_name": fake.company(),
                "category_id": parent_cat_id
            }
            trans = run_api_request("POST", "/transactions/", trans_data)
            if trans: transactions.append(trans)
    return transactions

def seed_transaction_relationships(accounts, categories_map):
    print("--- Seeding Transaction Relationships (Transfers) ---")
    if not all(k in accounts for k in ["CHECKING", "SAVINGS"]) or not categories_map.get("Transfers"):
        print("Skipping transfers: Checking or Savings account, or Transfers category not found.")
        return

    checking_id = accounts["CHECKING"]["id"]
    savings_id = accounts["SAVINGS"]["id"]
    transfer_category_id = categories_map["Transfers"]["id"]
    
    for _ in range(10): # Create 10 pairs of transfer transactions
        amount = Decimal(random.uniform(50, 500)).quantize(Decimal('0.01'))
        transfer_date = fake.date_between(start_date="-2y", end_date="today").isoformat()
        
        # Debit from Checking
        from_transaction = run_api_request("POST", "/transactions/", {
            "account_id": checking_id, "transaction_date": transfer_date,
            "amount": -amount, "transaction_type": "TRANSFER",
            "description": "Transfer to Savings", "merchant_name": "Internal Transfer",
            "category_id": transfer_category_id
        })
        
        # Credit to Savings
        to_transaction = run_api_request("POST", "/transactions/", {
            "account_id": savings_id, "transaction_date": transfer_date,
            "amount": amount, "transaction_type": "TRANSFER",
            "description": "Transfer from Checking", "merchant_name": "Internal Transfer",
            "category_id": transfer_category_id
        })

        if from_transaction and to_transaction:
            run_api_request("POST", f"/transactions/{from_transaction['db_id']}/relationships", {
                "to_transaction_id": to_transaction['db_id'],
                "relationship_type": "OFFSETS"
            })

def seed_tags(transactions):
    print("--- Seeding Tags ---")
    tags = []
    for _ in range(15):
        tag = run_api_request("POST", "/tags/", {"tag_name": fake.word(), "color": fake.hex_color()})
        if tag: tags.append(tag)
    
    if transactions and tags:
        for trans in random.sample(transactions, k=min(len(transactions), 50)):
            run_api_request("POST", "/tags/transactions/", {"transaction_id": trans['db_id'], "tag_id": random.choice(tags)['tag_id']})

def seed_investments(accounts):
    print("--- Seeding Investment Data ---")
    if not accounts.get("INVESTMENT"):
        return
    inv_id = accounts["INVESTMENT"]["id"]
    for symbol in ["AAPL", "GOOGL", "TSLA", "MSFT", "AMZN"]:
        holding = run_api_request("POST", "/investments/holdings/", {"account_id": inv_id, "symbol": symbol, "quantity": random.uniform(10, 100), "average_cost_basis": random.uniform(100, 500)})
        if holding:
            for _ in range(random.randint(10, 20)):
                price = Decimal(str(holding['average_cost_basis'])) * Decimal(str(random.uniform(0.8, 1.2)))
                qty = Decimal(str(random.uniform(1, 5)))
                run_api_request("POST", "/investments/transactions/", {"account_id": inv_id, "holding_id": holding['holding_id'], "transaction_type": "BUY", "symbol": symbol, "quantity": str(qty), "price_per_share": str(price), "total_amount": str(price * qty), "transaction_date": fake.date_between(start_date="-3y", end_date="today").isoformat()})

def seed_debt_data(accounts):
    print("--- Seeding Debt Data ---")
    if not all(k in accounts for k in ["LOAN", "CHECKING"]):
        return
        
    loan_id, checking_id = accounts["LOAN"]["id"], accounts["CHECKING"]["id"]
    plan = run_api_request("POST", "/debt/plans/", {"plan_name": "Aggressive Payoff Plan", "strategy": "AVALANCHE"})
    if not plan:
        return

    run_api_request("POST", "/debt/plans/accounts/", {"plan_id": plan['plan_id'], "account_id": loan_id, "priority": 1})
    
    # Seed Debt Repayment Schedule
    print("--- Seeding Debt Repayment Schedule ---")
    schedules = []
    for i in range(12): # Schedule for the next 12 months
        payment_date = (date.today().replace(day=1) + timedelta(days=31*i)).replace(day=1)
        schedules.append({
            "payment_month": payment_date.isoformat(),
            "scheduled_payment_amount": float(Decimal(random.uniform(300, 500)).quantize(Decimal('0.01')))
        })
    run_api_request("POST", "/debt/schedules/", {"account_id": loan_id, "schedules": schedules})

    # Seed historical debt payments
    for _ in range(24):
        run_api_request("POST", "/debt/payments/", {"loan_account_id": loan_id, "payment_source_account_id": checking_id, "payment_amount": random.uniform(250, 600), "payment_date": fake.date_between(start_date="-2y", end_date="today").isoformat()})

def seed_budgets(categories_map):
    print("--- Seeding Budgets ---")
    for i in range(3):
        start_date = date.today().replace(day=1) - timedelta(days=30*i)
        end_date = (start_date + timedelta(days=31)).replace(day=1) - timedelta(days=1)
        
        budget_categories_list = []
        if categories_map:
            cat_ids = [k for k in categories_map.keys() if isinstance(k, int)]
            for cat_id in random.sample(cat_ids, k=min(len(cat_ids), 5)):
                budget_categories_list.append({
                    "category_id": cat_id,
                    "allocated_amount": float(Decimal(random.uniform(400, 1200)).quantize(Decimal('0.01')))
                })

        budget_data = {
            "budget_name": f"Budget for {start_date.strftime('%B %Y')}",
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "categories": budget_categories_list
        }
        run_api_request("POST", "/budgets/", budget_data)

def seed_financial_plans(categories_map):
    print("--- Seeding Financial Plans ---")
    if not categories_map:
        return
        
    plan_data = {
        "plan_name": "Retirement Savings Plan",
        "monthly_income": float(Decimal(random.uniform(5000, 15000)).quantize(Decimal('0.01')))
    }
    plan = run_api_request("POST", "/financial_plans/", plan_data)
    
    if plan:
        print("--- Seeding Financial Plan Entries ---")
        cat_ids = [k for k in categories_map.keys() if isinstance(k, int)]
        for cat_id in random.sample(cat_ids, k=min(len(cat_ids), 4)):
            entry_data = {
                "category_id": cat_id,
                "monthly_amount": float(Decimal(random.uniform(100, 800)).quantize(Decimal('0.01')))
            }
            run_api_request("POST", f"/financial_plans/{plan['plan_id']}/entries", entry_data)

def main():
    """Starts the server, seeds a large amount of data, and shuts down the server."""
    
    print("--- Resetting database with Alembic ---")
    try:
        print("Downgrading database...")
        subprocess.run(["alembic", "downgrade", "base"], check=True, capture_output=True, text=True)
        print("Upgrading database...")
        subprocess.run(["alembic", "upgrade", "head"], check=True, capture_output=True, text=True)
        print("Database reset successfully.")
    except (subprocess.CalledProcessError, FileNotFoundError) as e:
        print(f"Error during database reset: {e}")
        if hasattr(e, 'stderr') and e.stderr:
            print(e.stderr)
        return

    server_process = subprocess.Popen(UVICORN_COMMAND)
    time.sleep(5)
    print(f"Server started with PID: {server_process.pid}")

    try:
        # 1. Get or Create User
        print("--- Ensuring User Exists ---")
        users = run_api_request("GET", "/users/")
        if not any(u.get("email") == "testuser@example.com" for u in users):
            run_api_request("POST", "/users/", {
                "email": "testuser@example.com", "username": "testuser",
                "password": "aStrongPassword123", "confirm_password": "aStrongPassword123",
                "first_name": "Test", "last_name": "User"
            })
        
        categories_map = seed_categories()

        # 2. Seed Accounts
        print("--- Seeding Accounts ---")
        account_types = [("CHECKING", "Main Checking"), ("SAVINGS", "Emergency Fund"), ("CREDIT_CARD", "Rewards Card"), ("LOAN", "Car Loan"), ("INVESTMENT", "Brokerage")]
        accounts = {t[0]: run_api_request("POST", "/accounts/", {"account_name": t[1], "account_type": t[0], "institution_name": fake.company(), "balance": float(Decimal(random.uniform(500, 20000)).quantize(Decimal('0.01')))}) for t in account_types}

        # 3. Seed Core Data
        transactions = seed_transactions(accounts, categories_map)
        seed_transaction_relationships(accounts, categories_map)
        seed_tags(transactions)
        seed_investments(accounts)
        seed_debt_data(accounts)
        seed_budgets(categories_map)
        seed_financial_plans(categories_map)

        print("\n--- Seeding Complete ---")

    finally:
        if server_process:
            print("\n--- Shutting down server ---")
            os.kill(server_process.pid, signal.SIGTERM)
            server_process.wait()
            print("Server shut down.")

if __name__ == "__main__":
    main()

