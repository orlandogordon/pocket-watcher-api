from pydantic import BaseModel, model_validator
from typing import Optional, List
from datetime import date, datetime
from decimal import Decimal
from uuid import UUID


class SnapshotUpdateRequest(BaseModel):
    """Request model to edit a single snapshot's values"""
    balance: Optional[Decimal] = None
    securities_value: Optional[Decimal] = None
    cash_balance: Optional[Decimal] = None
    total_cost_basis: Optional[Decimal] = None
    unrealized_gain_loss: Optional[Decimal] = None
    realized_gain_loss: Optional[Decimal] = None
    dismiss_review: Optional[bool] = None


class DismissReviewRequest(BaseModel):
    """Request model to dismiss needs_review flags on snapshots"""
    snapshot_uuids: List[UUID]
    reason: Optional[str] = "Dismissed by user"


class AccountSnapshotResponse(BaseModel):
    """Response model for a single account value snapshot"""
    snapshot_uuid: UUID
    account_uuid: UUID
    value_date: date
    balance: Decimal
    securities_value: Optional[Decimal] = None
    cash_balance: Optional[Decimal] = None
    total_cost_basis: Optional[Decimal]
    unrealized_gain_loss: Optional[Decimal]
    realized_gain_loss: Optional[Decimal]
    principal_paid_ytd: Optional[Decimal]
    interest_paid_ytd: Optional[Decimal]
    needs_review: bool = False
    review_reason: Optional[str] = None
    snapshot_source: str
    created_at: datetime

    class Config:
        from_attributes = True

    @model_validator(mode='before')
    @classmethod
    def resolve_uuids(cls, data):
        if hasattr(data, '__dict__'):
            if hasattr(data, 'account') and data.account:
                data.__dict__['account_uuid'] = data.account.uuid
            if hasattr(data, 'uuid') and data.uuid:
                data.__dict__['snapshot_uuid'] = data.uuid
        return data


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
    account_uuid: UUID
    account_name: str
    account_type: str
    data: List[AccountSnapshotResponse]


class SnapshotBackfillJobResponse(BaseModel):
    """Response model for snapshot backfill jobs"""
    id: int
    account_uuid: UUID
    start_date: date
    end_date: date
    status: str  # PENDING, IN_PROGRESS, COMPLETED, FAILED
    error_message: Optional[str]
    created_at: datetime
    started_at: Optional[datetime]
    completed_at: Optional[datetime]
    snapshots_created: Optional[int]
    snapshots_updated: Optional[int]
    snapshots_failed: Optional[int]
    snapshots_skipped: Optional[int]

    class Config:
        from_attributes = True

    @model_validator(mode='before')
    @classmethod
    def resolve_uuids(cls, data):
        if hasattr(data, '__dict__'):
            if hasattr(data, 'account') and data.account:
                data.__dict__['account_uuid'] = data.account.uuid
        return data
