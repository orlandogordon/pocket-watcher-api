from pydantic import BaseModel, Field
from typing import Optional

# ===== CATEGORY PYDANTIC MODELS =====

class CategoryBase(BaseModel):
    name: str = Field(..., min_length=1, max_length=100, description="Category name")
    parent_category_id: Optional[int] = Field(None, description="The ID of the parent category, for sub-categories")

class CategoryCreate(CategoryBase):
    pass

class CategoryUpdate(BaseModel):
    name: Optional[str] = Field(None, min_length=1, max_length=100, description="Category name")
    parent_category_id: Optional[int] = Field(None, description="The ID of the parent category, for sub-categories")

class CategoryResponse(CategoryBase):
    id: int
    
    class Config:
        from_attributes = True
