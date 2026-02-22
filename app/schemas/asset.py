from __future__ import annotations
import enum
from datetime import datetime
from typing import List, Optional
from pydantic import BaseModel


class AssetType(str, enum.Enum):
    CURRENCY = "CURRENCY"
    CRYPTO = "CRYPTO"
    COMMODITY = "COMMODITY"
    EQUITY = "EQUITY"


class InstrumentCreate(BaseModel):
    symbol: str
    name: str
    asset_type: AssetType
    description: Optional[str] = None


class InstrumentUpdate(BaseModel):
    symbol: Optional[str] = None
    name: Optional[str] = None
    asset_type: Optional[AssetType] = None
    description: Optional[str] = None


class InstrumentResponse(BaseModel):
    id: int
    symbol: str
    name: str
    asset_type: AssetType
    description: Optional[str] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    model_config = {"from_attributes": True}


class InstrumentListResponse(BaseModel):
    items: List[InstrumentResponse]
    total: int
    page: int
    size: int
