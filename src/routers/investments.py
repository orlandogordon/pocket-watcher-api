from fastapi import APIRouter, HTTPException, Request
from fastapi.params import Depends
from sqlalchemy.orm import Session
from ..db.core import NotFoundError, get_db
from ..db.investments import (
    Investment,
    InvestmentCreate,
    InvestmentUpdate,
    read_db_investment,
    create_db_investment,
    update_db_investment,
    delete_db_investment,
)
# from .limiter import limiter


router = APIRouter(
    prefix="/investments",
)


# @limiter.limit("1/second")
@router.post("/")
def create_investment(request: Request, investment: InvestmentCreate, db: Session = Depends(get_db)) -> Investment:
    db_investment = create_db_investment(investment, db)
    return Investment(**db_investment.__dict__)


@router.get("/{investment_id}")
def read_investment(request: Request, investment_id: int, db: Session = Depends(get_db)) -> Investment:
    try:
        db_investment = read_db_investment(investment_id, db)
    except NotFoundError as e:
        raise HTTPException(status_code=404) from e
    return Investment(**db_investment.__dict__)


@router.get("/{investment_id}/automations")
def read_investment_automations(
    request: Request, investment_id: int, db: Session = Depends(get_db)
) -> list[Investment]:
    # try:
    #     transactions = read_db_transactions_for_transaction(transaction_id, db)
    # except NotFoundError as e:
    #     raise HTTPException(status_code=404) from e
    # return [Automation(**automation.__dict__) for automation in automations]
    return []


@router.put("/{investment_id}")
def update_investment(request: Request, investment_id: int, investment: InvestmentUpdate, db: Session = Depends(get_db)) -> Investment:
    try:
        db_investment = update_db_investment(investment_id, investment, db)
    except NotFoundError as e:
        raise HTTPException(status_code=404) from e
    return Investment(**db_investment.__dict__)


@router.delete("/{investment_id}")
def delete_investment(request: Request, investment_id: int, db: Session = Depends(get_db)) -> Investment:
    try:
        db_investment = delete_db_investment(investment_id, db)
    except NotFoundError as e:
        raise HTTPException(status_code=404) from e
    return Investment(**db_investment.__dict__)