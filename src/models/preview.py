from pydantic import BaseModel, Field
from typing import Optional, List, Dict, Any


class EditTransactionRequest(BaseModel):
    temp_id: str
    edited_data: Dict[str, Any]


class BulkEditRequest(BaseModel):
    temp_ids: List[str]
    edited_data: Dict[str, Any]


class RejectItemRequest(BaseModel):
    temp_id: str


class RestoreItemRequest(BaseModel):
    temp_id: str


class ConfirmImportRequest(BaseModel):
    preview_session_id: str


class BulkRejectItemRequest(BaseModel):
    temp_ids: List[str] = Field(..., min_length=1)


class BulkRestoreItemRequest(BaseModel):
    temp_ids: List[str] = Field(..., min_length=1)


class PreviewSummary(BaseModel):
    total_parsed: int
    rejected: int
    ready_to_import: int
    can_confirm: bool


class PreviewSessionInfo(BaseModel):
    preview_session_id: str
    institution: str
    filename: str
    created_at: str
    expires_at: str
    summary: PreviewSummary
