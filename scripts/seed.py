import sys
import os
import random
from sqlalchemy.orm import Session
from datetime import date, datetime, timedelta
from decimal import Decimal
from uuid import uuid4
from faker import Faker

# Add the project root to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.db.core import (
    session_local,
    UserDB,
    AccountDB,
    TransactionDB,
    TagDB,
    TransactionTagDB,
    BudgetDB,
    BudgetCategoryDB,
    DebtRepaymentPlanDB,
    DebtPlanAccountLinkDB,
    DebtPaymentDB,
    InvestmentHoldingDB,
    InvestmentTransactionDB,
    DebtRepaymentScheduleDB,
    TransactionRelationshipDB,
    CategoryDB,
    FinancialPlanDB,
    FinancialPlanEntryDB,
    AccountType,
    TransactionType,
    DebtStrategy,
    InvestmentTransactionType,
    SourceType,
    RelationshipType,
)

fake = Faker()

def seed_database():
    """
    Fills the database with a large set of sample data for multiple users.
    """
    db: Session = session_local()

    try:
        # Check if data exists to prevent duplicate seeding
        if db.query(UserDB).count() > 0:
            print("Database appears to be already seeded. Exiting.")
            return

        print("Seeding database with large set of sample data...")

        # 1. Create Categories and Sub-categories
        print("Creating categories...")
        categories_structure = {
            "Income": ["Paycheck", "Bonus", "Investment Income"],
            "Housing": ["Rent", "Mortgage", "Utilities", "Home Repair"],
            "Transportation": ["Gas", "Public Transit", "Car Maintenance", "Ride Share"],
            "Food": ["Groceries", "Restaurants", "Coffee Shops"],
            "Personal Care": ["Haircut", "Toiletries", "Pharmacy"],
            "Entertainment": ["Movies", "Concerts", "Streaming Services", "Hobbies"],
            "Debt Payment": ["Credit Card", "Student Loan", "Car Loan"],
            "Investments": ["Stock Purchase", "Retirement Contribution", "Crypto"],
            "Shopping": ["Clothing", "Electronics", "Home Goods"],
            "Miscellaneous": ["Bank Fee", "General Merchandise"],
        }
        
        categories_map = {}
        for cat_name, sub_cat_names in categories_structure.items():
            parent_cat = CategoryDB(name=cat_name)
            db.add(parent_cat)
            db.flush()
            categories_map[parent_cat.id] = []
            
            for sub_cat_name in sub_cat_names:
                child_cat = CategoryDB(name=sub_cat_name, parent_category_id=parent_cat.id)
                db.add(child_cat)
                db.flush()
                categories_map[parent_cat.id].append(child_cat.id)
        
        db.commit()
        print(f"{len(categories_map)} parent categories created.")

        # Main loop to create 10x data
        for i in range(10):
            print(f"--- Seeding User Batch {i+1}/10 ---")
            
            # 2. Create User
            user = UserDB(
                id=uuid4(),
                email=fake.email(),
                username=fake.user_name(),
                password_hash="hashed_password_placeholder",
                first_name=fake.first_name(),
                last_name=fake.last_name(),
                date_of_birth=fake.date_of_birth(minimum_age=18, maximum_age=70),
            )
            db.add(user)
            db.flush()

            # 3. Create Accounts
            account_types = [
                (AccountType.CHECKING, "Main Checking", Decimal("8000")),
                (AccountType.SAVINGS, "Emergency Fund", Decimal("25000")),
                (AccountType.CREDIT_CARD, "Rewards Card", Decimal("-1500")),
                (AccountType.LOAN, "Car Loan", Decimal("-12000")),
                (AccountType.INVESTMENT, "Brokerage", Decimal("50000")),
            ]
            
            user_accounts = []
            for acc_type, acc_name, balance in account_types:
                account = AccountDB(
                    user_id=user.db_id,
                    account_name=f"{acc_name} {i}",
                    account_type=acc_type,
                    institution_name=fake.company(),
                    balance=balance * Decimal(random.uniform(0.8, 1.2)),
                    interest_rate=Decimal('0.0525') if acc_type == AccountType.LOAN else None
                )
                db.add(account)
                user_accounts.append(account)
            db.flush()
            
            loan_accounts = [acc for acc in user_accounts if acc.account_type == AccountType.LOAN]
            investment_accounts = [acc for acc in user_accounts if acc.account_type == AccountType.INVESTMENT]
            checking_account = next((acc for acc in user_accounts if acc.account_type == AccountType.CHECKING), None)

            # 4. Create Transactions
            all_transactions = []
            purchases = []
            for account in user_accounts:
                if account.account_type in [AccountType.CREDIT_CARD, AccountType.LOAN, AccountType.INVESTMENT]:
                    continue 
                
                for _ in range(random.randint(50, 100)):
                    trans_date = fake.date_between(start_date="-2y", end_date="today")
                    parent_cat_id = random.choice(list(categories_map.keys()))
                    sub_cat_id = random.choice(categories_map[parent_cat_id]) if categories_map[parent_cat_id] else None
                    is_purchase = random.choice([True, False])
                    amount = Decimal(random.uniform(5.0, 500.0))
                    
                    transaction = TransactionDB(
                        id=uuid4(), user_id=user.db_id, account_id=account.id,
                        transaction_date=trans_date, amount=amount,
                        transaction_type=TransactionType.PURCHASE if is_purchase else TransactionType.CREDIT,
                        category_id=parent_cat_id, subcategory_id=sub_cat_id,
                        description=fake.catch_phrase(), merchant_name=fake.company(),
                        transaction_hash=fake.sha256(raw_output=False), source_type=SourceType.MANUAL,
                    )
                    db.add(transaction)
                    all_transactions.append(transaction)
                    if is_purchase:
                        purchases.append(transaction)
            db.flush()

            # 5. Create Tags and Link to Transactions
            print("Creating tags and linking to transactions...")
            user_tags = []
            for _ in range(random.randint(3, 8)):
                tag = TagDB(user_id=user.db_id, tag_name=fake.word(), color=fake.hex_color())
                db.add(tag)
                user_tags.append(tag)
            db.flush()

            if all_transactions and user_tags:
                for transaction in random.sample(all_transactions, min(len(all_transactions), 25)):
                    tag_to_link = random.choice(user_tags)
                    tt = TransactionTagDB(transaction_id=transaction.db_id, tag_id=tag_to_link.tag_id)
                    db.add(tt)

            # 6. Create Investment Holdings and Transactions
            print("Creating investment data...")
            if investment_accounts:
                investment_account = investment_accounts[0]
                holdings_data = [
                    {"symbol": "AAPL", "quantity": Decimal("50"), "cost": Decimal("175.00")},
                    {"symbol": "GOOGL", "quantity": Decimal("20"), "cost": Decimal("140.00")},
                    {"symbol": "VTSAX", "quantity": Decimal("100"), "cost": Decimal("105.50")},
                ]
                for h_data in holdings_data:
                    holding = InvestmentHoldingDB(
                        account_id=investment_account.id, symbol=h_data["symbol"],
                        quantity=h_data["quantity"], average_cost_basis=h_data["cost"]
                    )
                    db.add(holding)
                    db.flush()

                    for _ in range(random.randint(3, 6)):
                        inv_trans = InvestmentTransactionDB(
                            account_id=investment_account.id, holding_id=holding.holding_id,
                            transaction_type=random.choice([InvestmentTransactionType.BUY, InvestmentTransactionType.REINVESTMENT]),
                            symbol=holding.symbol, quantity=Decimal(random.uniform(1.0, 5.0)),
                            price_per_share=holding.average_cost_basis * Decimal(random.uniform(0.95, 1.05)),
                            total_amount=Decimal(random.uniform(100.0, 1000.0)),
                            transaction_date=fake.date_between(start_date="-2y", end_date="today"),
                        )
                        db.add(inv_trans)
                    
                    inv_trans_div = InvestmentTransactionDB(
                        account_id=investment_account.id, holding_id=holding.holding_id,
                        transaction_type=InvestmentTransactionType.DIVIDEND, symbol=holding.symbol,
                        total_amount=holding.quantity * Decimal(random.uniform(0.5, 2.0)),
                        transaction_date=fake.date_between(start_date="-1y", end_date="today"),
                    )
                    db.add(inv_trans_div)

            # 7. Create Debt Repayment Plan and Payments
            print("Creating debt repayment plan and payments...")
            if loan_accounts:
                debt_plan = DebtRepaymentPlanDB(
                    user_id=user.db_id, plan_name=f"Debt Annihilator Plan {i}",
                    strategy=random.choice(list(DebtStrategy)), status="ACTIVE"
                )
                db.add(debt_plan)
                db.flush()

                for priority, loan_acc in enumerate(loan_accounts):
                    link = DebtPlanAccountLinkDB(plan_id=debt_plan.plan_id, account_id=loan_acc.id, priority=priority + 1)
                    db.add(link)

                if checking_account:
                    for loan_acc in loan_accounts:
                        for _ in range(random.randint(6, 18)):
                            payment_date = fake.date_between(start_date="-18m", end_date="today")
                            payment_amount = Decimal(random.uniform(250.0, 600.0))
                            interest_amount = payment_amount * Decimal(random.uniform(0.15, 0.4))
                            principal_amount = payment_amount - interest_amount
                            
                            debt_payment = DebtPaymentDB(
                                loan_account_id=loan_acc.id, payment_source_account_id=checking_account.id,
                                payment_amount=payment_amount, principal_amount=principal_amount,
                                interest_amount=interest_amount, payment_date=payment_date,
                            )
                            db.add(debt_payment)

            # 8. Create Transaction Relationships (Refunds)
            if len(purchases) > 5:
                for _ in range(5):
                    purchase_to_refund = random.choice(purchases)
                    refund_transaction = TransactionDB(
                        id=uuid4(), user_id=user.db_id, account_id=purchase_to_refund.account_id,
                        transaction_date=purchase_to_refund.transaction_date + timedelta(days=random.randint(2, 10)),
                        amount=abs(purchase_to_refund.amount) * Decimal("0.5"), # Partial refund
                        transaction_type=TransactionType.CREDIT, category_id=purchase_to_refund.category_id,
                        description=f"Refund for: {purchase_to_refund.description}",
                        merchant_name=purchase_to_refund.merchant_name,
                        transaction_hash=fake.sha256(raw_output=False), source_type=SourceType.MANUAL,
                    )
                    db.add(refund_transaction)
                    db.flush()

                    relationship = TransactionRelationshipDB(
                        from_transaction_id=refund_transaction.db_id, to_transaction_id=purchase_to_refund.db_id,
                        relationship_type=RelationshipType.REFUNDS, amount_allocated=refund_transaction.amount
                    )
                    db.add(relationship)

            # 9. Create Debt Repayment Schedules
            for loan_acc in loan_accounts:
                for j in range(12):
                    payment_month = date.today().replace(day=1) + timedelta(days=31*j)
                    schedule = DebtRepaymentScheduleDB(
                        user_id=user.db_id, account_id=loan_acc.id,
                        payment_month=payment_month,
                        scheduled_payment_amount=Decimal(random.uniform(200.0, 500.0))
                    )
                    db.add(schedule)
            
            # 10. Create a Budget
            today = date.today()
            budget = BudgetDB(
                user_id=user.db_id, budget_name=f"{today.strftime('%B %Y')} Budget {i}",
                start_date=today.replace(day=1), end_date=(today.replace(day=1) + timedelta(days=31)).replace(day=1) - timedelta(days=1)
            )
            db.add(budget)
            db.flush()
            
            if len(categories_map.keys()) >= 5:
                for cat_id in random.sample(list(categories_map.keys()), k=5):
                    bc = BudgetCategoryDB(budget_id=budget.budget_id, category_id=cat_id, allocated_amount=Decimal(random.uniform(200, 1500)))
                    db.add(bc)

            # 11. Create a Financial Plan
            print("Creating financial plan...")
            financial_plan = FinancialPlanDB(
                user_id=user.db_id,
                plan_name=f"What-if Scenario {i}",
                monthly_income=Decimal(random.uniform(6000.0, 10000.0))
            )
            db.add(financial_plan)
            db.flush()

            if len(categories_map.keys()) >= 8:
                for cat_id in random.sample(list(categories_map.keys()), k=random.randint(4, 8)):
                    entry = FinancialPlanEntryDB(
                        plan_id=financial_plan.plan_id,
                        category_id=cat_id,
                        monthly_amount=Decimal(random.uniform(100.0, 800.0))
                    )
                    db.add(entry)

            db.commit()
            print(f"User {i+1} and associated data seeded.")

        print("Successfully seeded database.")

    except Exception as e:
        print(f"An error occurred: {e}")
        import traceback
        traceback.print_exc()
        db.rollback()
    finally:
        db.close()

if __name__ == "__main__":
    seed_database()