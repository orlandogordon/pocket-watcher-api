from pydantic import BaseModel, ConfigDict, Field, model_validator
from typing import Optional
from uuid import UUID

# ===== CATEGORY PYDANTIC MODELS =====

class CategoryBase(BaseModel):
    name: str = Field(..., min_length=1, max_length=100, description="Category name")
    parent_category_uuid: Optional[UUID] = Field(None, description="UUID of the parent category, for sub-categories")

class CategoryCreate(CategoryBase):
    pass

class CategoryUpdate(BaseModel):
    name: Optional[str] = Field(None, min_length=1, max_length=100, description="Category name")
    parent_category_uuid: Optional[UUID] = Field(None, description="UUID of the parent category, for sub-categories")

class CategoryResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: UUID = Field(validation_alias="uuid")
    name: str
    parent_category_uuid: Optional[UUID] = None

    @model_validator(mode='before')
    @classmethod
    def resolve_uuids(cls, data):
        if hasattr(data, '__dict__'):
            if hasattr(data, 'parent') and data.parent:
                data.__dict__['parent_category_uuid'] = data.parent.uuid
        return data
