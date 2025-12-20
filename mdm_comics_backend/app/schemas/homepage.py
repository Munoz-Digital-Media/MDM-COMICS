"""
Homepage Section Configuration Schemas
"""
from typing import List, Literal, Optional
from uuid import uuid4

from pydantic import BaseModel, Field, field_validator

class HomepageSectionConfig(BaseModel):
    """Configuration for a single homepage section."""
    id: str = Field(default_factory=lambda: str(uuid4()))
    key: str
    title: str
    emoji: str
    visible: bool = True
    display_order: int = Field(ge=1, le=10)
    max_items: int = Field(default=5, ge=1, le=10)
    category_link: str
    data_source: Literal['products', 'bundles']

    @field_validator('key')
    @classmethod
    def validate_key(cls, v: str) -> str:
        valid_keys = {'bagged-boarded', 'graded', 'funko', 'supplies', 'bundles'}
        if v not in valid_keys:
            raise ValueError(f"Invalid section key: {v}")
        return v

class HomepageSectionsResponse(BaseModel):
    """Response schema for homepage sections."""
    sections: List[HomepageSectionConfig]
    updated_at: Optional[str] = None

class HomepageSectionUpdate(BaseModel):
    """Schema for updating a single section."""
    key: str
    visible: Optional[bool] = None
    display_order: Optional[int] = Field(None, ge=1, le=10)
    max_items: Optional[int] = Field(None, ge=1, le=10)

class HomepageSectionsUpdateRequest(BaseModel):
    """Request schema for bulk update of sections."""
    sections: List[HomepageSectionUpdate]
