from pydantic import BaseModel
from typing import Optional, List
from datetime import date, datetime
from decimal import Decimal


class AccountSnapshotResponse(BaseModel):
    """Response model for a single account value snapshot"""
    snapshot_id: int
    account_id: int
    value_date: date
    balance: Decimal
    total_cost_basis: Optional[Decimal]
    unrealized_gain_loss: Optional[Decimal]
    realized_gain_loss: Optional[Decimal]
    principal_paid_ytd: Optional[Decimal]
    interest_paid_ytd: Optional[Decimal]
    snapshot_source: str
    created_at: datetime

    class Config:
        from_attributes = True


class NetWorthDataPoint(BaseModel):
    """A single data point in net worth history"""
    date: str  # ISO format date string
    net_worth: float
    total_unrealized_gains: Optional[float]


class NetWorthHistoryResponse(BaseModel):
    """Response model for net worth history"""
    data: List[NetWorthDataPoint]
    start_date: Optional[date]
    end_date: Optional[date]
    total_days: int


class AccountValueHistoryResponse(BaseModel):
    """Response model for account value history"""
    account_id: int
    account_name: str
    account_type: str
    data: List[AccountSnapshotResponse]
