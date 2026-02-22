from typing import List, Optional
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func
from app.models.asset import Instrument
from app.schemas.asset import InstrumentCreate, InstrumentUpdate


async def get_instruments(db: AsyncSession, skip: int = 0, limit: int = 50) -> List[Instrument]:
    result = await db.execute(select(Instrument).offset(skip).limit(limit))
    return result.scalars().all()


async def get_instrument_count(db: AsyncSession) -> int:
    result = await db.execute(select(func.count()).select_from(Instrument))
    return result.scalar_one()


async def get_instrument_by_id(db: AsyncSession, instrument_id: int) -> Optional[Instrument]:
    result = await db.execute(select(Instrument).where(Instrument.id == instrument_id))
    return result.scalar_one_or_none()


async def get_instrument_by_symbol(db: AsyncSession, symbol: str) -> Optional[Instrument]:
    result = await db.execute(select(Instrument).where(Instrument.symbol == symbol))
    return result.scalar_one_or_none()


async def create_instrument(db: AsyncSession, data: InstrumentCreate) -> Instrument:
    instrument = Instrument(
        symbol=data.symbol,
        name=data.name,
        asset_type=data.asset_type,
        description=data.description,
    )
    db.add(instrument)
    await db.commit()
    await db.refresh(instrument)
    return instrument


async def update_instrument(
    db: AsyncSession, instrument_id: int, data: InstrumentUpdate
) -> Optional[Instrument]:
    instrument = await get_instrument_by_id(db, instrument_id)
    if not instrument:
        return None
    update_data = data.model_dump(exclude_unset=True)
    for key, value in update_data.items():
        setattr(instrument, key, value)
    await db.commit()
    await db.refresh(instrument)
    return instrument


async def delete_instrument(db: AsyncSession, instrument_id: int) -> bool:
    instrument = await get_instrument_by_id(db, instrument_id)
    if not instrument:
        return False
    await db.delete(instrument)
    await db.commit()
    return True
