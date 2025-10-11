from pydantic import BaseModel, Field, field_validator
from typing import Optional, List
from datetime import datetime


# ===== TAG PYDANTIC MODELS =====

class TagCreate(BaseModel):
    tag_name: str = Field(..., min_length=1, max_length=100, description="Tag name")
    color: Optional[str] = Field(None, pattern=r'^#[0-9A-Fa-f]{6}$', description="Hex color code")

    @field_validator('tag_name')
    @classmethod
    def validate_tag_name(cls, v: str) -> str:
        return v.strip()

    @field_validator('color')
    @classmethod
    def validate_color(cls, v: Optional[str]) -> Optional[str]:
        if v and not v.startswith('#'):
            v = f"#{v}"
        return v.upper() if v else v


class TagUpdate(BaseModel):
    """Update tag - all fields optional"""
    tag_name: Optional[str] = Field(None, min_length=1, max_length=100)
    color: Optional[str] = Field(None, pattern=r'^#[0-9A-Fa-f]{6}$')

    @field_validator('tag_name')
    @classmethod
    def validate_tag_name(cls, v: Optional[str]) -> Optional[str]:
        return v.strip() if v else v

    @field_validator('color')
    @classmethod
    def validate_color(cls, v: Optional[str]) -> Optional[str]:
        if v and not v.startswith('#'):
            v = f"#{v}"
        return v.upper() if v else v


class TagResponse(BaseModel):
    """Tag data returned to client"""
    tag_id: int
    tag_name: str
    color: Optional[str]
    created_at: datetime
    transaction_count: Optional[int] = None  # Number of transactions with this tag

    class Config:
        from_attributes = True


class TransactionTagCreate(BaseModel):
    """Add tag to transaction"""
    transaction_id: int = Field(..., description="Transaction DB ID")
    tag_id: int = Field(..., description="Tag ID")


class TransactionTagResponse(BaseModel):
    """Transaction-Tag relationship response"""
    transaction_id: int
    tag_id: int
    created_at: datetime

    class Config:
        from_attributes = True


class BulkTagRequest(BaseModel):
    """Bulk tag assignment request"""
    transaction_ids: List[int] = Field(..., description="List of transaction DB IDs to tag")
    tag_id: int = Field(..., description="Tag ID to apply to all transactions")


class TagStats(BaseModel):
    """Tag usage statistics"""
    tag_id: int
    tag_name: str
    color: Optional[str]
    transaction_count: int
    total_amount: float
    average_amount: float
    most_recent_use: Optional[datetime]
