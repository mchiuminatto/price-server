from __future__ import annotations
import enum
from datetime import datetime
from decimal import Decimal
from typing import List, Optional
from pydantic import BaseModel


class TimeFrame(str, enum.Enum):
    M1 = "M1"
    M5 = "M5"
    M15 = "M15"
    M30 = "M30"
    H1 = "H1"
    H4 = "H4"
    D1 = "D1"
    W1 = "W1"
    MN1 = "MN1"
    CUSTOM = "CUSTOM"


class PriceType(str, enum.Enum):
    TICK = "TICK"
    OHLC = "OHLC"
    OHLC_NON_REGULAR = "OHLC_NON_REGULAR"


class TickDataCreate(BaseModel):
    instrument_id: int
    timestamp: datetime
    bid: Decimal
    ask: Decimal
    volume: Optional[Decimal] = None


class TickDataResponse(BaseModel):
    id: int
    instrument_id: int
    timestamp: datetime
    bid: Decimal
    ask: Decimal
    volume: Optional[Decimal] = None

    model_config = {"from_attributes": True}


class OHLCDataCreate(BaseModel):
    instrument_id: int
    timestamp: datetime
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: Optional[Decimal] = None
    timeframe: TimeFrame
    price_type: PriceType = PriceType.OHLC


class OHLCDataResponse(BaseModel):
    id: int
    instrument_id: int
    timestamp: datetime
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: Optional[Decimal] = None
    timeframe: TimeFrame
    price_type: PriceType

    model_config = {"from_attributes": True}


class PaginatedTickResponse(BaseModel):
    items: List[TickDataResponse]
    total: int
    page: int
    size: int


class PaginatedOHLCResponse(BaseModel):
    items: List[OHLCDataResponse]
    total: int
    page: int
    size: int


class ExportJobResponse(BaseModel):
    job_id: str
    status: str
    message: str
    download_url: Optional[str] = None
