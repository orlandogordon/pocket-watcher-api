# Loanchy - Technical Overview

This document provides a technical overview of the Pocket Watcher API project, intended for development and maintenance purposes.

## Project Structure

- **Framework**: [FastAPI](https://fastapi.tiangolo.com/) (Python)
- **Python**: [Python](https://docs.python.org/3.13/#)
- **Database ORM**: [SQLAlchemy](https://docs.sqlalchemy.org/en/20/)
- **Data Validation/Type Hinting**: [Pydantic](https://docs.pydantic.dev/latest/)
- **Database Migration Tool**: [Alembic](https://alembic.sqlalchemy.org/en/latest/#)
- **PDF Parser**: [pdfplumber](https://github.com/jsvine/pdfplumber)
- **Asynchronous Server Gateway Interface**: [uvicorn](https://www.uvicorn.org/#quickstart)
- **Package Manager**: pip

## Key Scripts

- `uvicorn src.main:app --reload`: Starts the development server.

## Architectural Notes

- Database schema is defined in `src/db/core.py`.
- Each core app function (i.e. Users, Accounts, Transactions) has models and crud logic defined in it's own file like so: `src/db/transactions.py`.
- Each core app function has it's own file with routes defined in  `src/routers`.
- PDF and CSV parsing logic is defined in `src/parser`.
- The application is an API service that will support a personal finance application.
- Core application features include tracking transactions for a given user, allowing the user to create custom tags for transactions, allowing users to create custom budgets, allowing users to track debt/create debt repayment plans, and allowing users to track investment transactions and holdings to formulate their own "net worth" calculation.

## Implementation standard.

- DO NOT over engineer things. Start with a simple implementation.
- Always keep the performance and security as a first priority.
- Ask for any clarification rather just guessing things if you are not clear about anything.