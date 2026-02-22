import io
from datetime import datetime
from typing import Optional
import pandas as pd
from fastapi import APIRouter, Depends, Form, UploadFile, File, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from app.database import get_db
from app.schemas.price import PaginatedTickResponse, PaginatedOHLCResponse, TimeFrame, PriceType
from app.services import price_service

router = APIRouter()


@router.post("/tick/upload")
async def upload_tick_data(
    file: UploadFile = File(...),
    instrument_id: int = Form(...),
    db: AsyncSession = Depends(get_db),
):
    content = await file.read()
    try:
        df = pd.read_csv(io.BytesIO(content))
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Failed to parse CSV: {e}")
    rows = []
    for _, row in df.iterrows():
        rows.append(
            {
                "instrument_id": instrument_id,
                "timestamp": pd.to_datetime(row["timestamp"]),
                "bid": row["bid"],
                "ask": row["ask"],
                "volume": (
                    row.get("volume")
                    if "volume" in row and not pd.isna(row.get("volume"))
                    else None
                ),
            }
        )
    inserted = await price_service.bulk_insert_tick_data(db, rows)
    return {"inserted": inserted, "instrument_id": instrument_id}


@router.post("/ohlc/upload")
async def upload_ohlc_data(
    file: UploadFile = File(...),
    instrument_id: int = Form(...),
    timeframe: str = Form(...),
    price_type: str = Form("OHLC"),
    db: AsyncSession = Depends(get_db),
):
    content = await file.read()
    try:
        df = pd.read_csv(io.BytesIO(content))
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Failed to parse CSV: {e}")
    rows = []
    for _, row in df.iterrows():
        rows.append(
            {
                "instrument_id": instrument_id,
                "timestamp": pd.to_datetime(row["timestamp"]),
                "open": row["open"],
                "high": row["high"],
                "low": row["low"],
                "close": row["close"],
                "volume": (
                    row.get("volume")
                    if "volume" in row and not pd.isna(row.get("volume"))
                    else None
                ),
                "timeframe": timeframe,
                "price_type": price_type,
            }
        )
    inserted = await price_service.bulk_insert_ohlc_data(db, rows)
    return {"inserted": inserted, "instrument_id": instrument_id}


@router.get("/tick/{instrument_id}", response_model=PaginatedTickResponse)
async def get_tick_data(
    instrument_id: int,
    from_date: Optional[datetime] = None,
    to_date: Optional[datetime] = None,
    limit: int = 1000,
    offset: int = 0,
    db: AsyncSession = Depends(get_db),
):
    items, total = await price_service.get_tick_data(
        db, instrument_id, from_date, to_date, limit, offset
    )
    page = (offset // limit) + 1 if limit > 0 else 1
    return PaginatedTickResponse(items=items, total=total, page=page, size=limit)


@router.get("/ohlc/{instrument_id}", response_model=PaginatedOHLCResponse)
async def get_ohlc_data(
    instrument_id: int,
    from_date: Optional[datetime] = None,
    to_date: Optional[datetime] = None,
    timeframe: Optional[TimeFrame] = None,
    price_type: Optional[PriceType] = None,
    limit: int = 1000,
    offset: int = 0,
    db: AsyncSession = Depends(get_db),
):
    items, total = await price_service.get_ohlc_data(
        db, instrument_id, from_date, to_date, timeframe, price_type, limit, offset
    )
    page = (offset // limit) + 1 if limit > 0 else 1
    return PaginatedOHLCResponse(items=items, total=total, page=page, size=limit)
