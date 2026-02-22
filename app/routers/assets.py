from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession
from app.database import get_db
from app.schemas.asset import (
    InstrumentCreate,
    InstrumentUpdate,
    InstrumentResponse,
    InstrumentListResponse,
)
from app.services import asset_service

router = APIRouter()


@router.get("", response_model=InstrumentListResponse)
async def list_assets(page: int = 1, size: int = 50, db: AsyncSession = Depends(get_db)):
    skip = (page - 1) * size
    items = await asset_service.get_instruments(db, skip=skip, limit=size)
    total = await asset_service.get_instrument_count(db)
    return InstrumentListResponse(items=items, total=total, page=page, size=size)


@router.post("", response_model=InstrumentResponse, status_code=status.HTTP_201_CREATED)
async def create_asset(data: InstrumentCreate, db: AsyncSession = Depends(get_db)):
    instrument = await asset_service.create_instrument(db, data)
    return instrument


@router.get("/{instrument_id}", response_model=InstrumentResponse)
async def get_asset(instrument_id: int, db: AsyncSession = Depends(get_db)):
    instrument = await asset_service.get_instrument_by_id(db, instrument_id)
    if not instrument:
        raise HTTPException(status_code=404, detail="Instrument not found")
    return instrument


@router.put("/{instrument_id}", response_model=InstrumentResponse)
async def update_asset(
    instrument_id: int, data: InstrumentUpdate, db: AsyncSession = Depends(get_db)
):
    instrument = await asset_service.update_instrument(db, instrument_id, data)
    if not instrument:
        raise HTTPException(status_code=404, detail="Instrument not found")
    return instrument


@router.delete("/{instrument_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_asset(instrument_id: int, db: AsyncSession = Depends(get_db)):
    deleted = await asset_service.delete_instrument(db, instrument_id)
    if not deleted:
        raise HTTPException(status_code=404, detail="Instrument not found")
