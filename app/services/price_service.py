from typing import List, Optional, Tuple
from datetime import datetime
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func
from app.models.price import TickData, OHLCData, TimeFrame, PriceType


async def bulk_insert_tick_data(db: AsyncSession, rows: List[dict]) -> int:
    if not rows:
        return 0
    db.add_all([TickData(**row) for row in rows])
    await db.commit()
    return len(rows)


async def bulk_insert_ohlc_data(db: AsyncSession, rows: List[dict]) -> int:
    if not rows:
        return 0
    db.add_all([OHLCData(**row) for row in rows])
    await db.commit()
    return len(rows)


async def get_tick_data(
    db: AsyncSession,
    instrument_id: int,
    from_date: Optional[datetime] = None,
    to_date: Optional[datetime] = None,
    limit: int = 1000,
    offset: int = 0,
) -> Tuple[List[TickData], int]:
    query = select(TickData).where(TickData.instrument_id == instrument_id)
    count_query = (
        select(func.count()).select_from(TickData).where(TickData.instrument_id == instrument_id)
    )
    if from_date:
        query = query.where(TickData.timestamp >= from_date)
        count_query = count_query.where(TickData.timestamp >= from_date)
    if to_date:
        query = query.where(TickData.timestamp <= to_date)
        count_query = count_query.where(TickData.timestamp <= to_date)
    total_result = await db.execute(count_query)
    total = total_result.scalar_one()
    result = await db.execute(query.order_by(TickData.timestamp).offset(offset).limit(limit))
    return result.scalars().all(), total


async def get_ohlc_data(
    db: AsyncSession,
    instrument_id: int,
    from_date: Optional[datetime] = None,
    to_date: Optional[datetime] = None,
    timeframe: Optional[TimeFrame] = None,
    price_type: Optional[PriceType] = None,
    limit: int = 1000,
    offset: int = 0,
) -> Tuple[List[OHLCData], int]:
    query = select(OHLCData).where(OHLCData.instrument_id == instrument_id)
    count_query = (
        select(func.count()).select_from(OHLCData).where(OHLCData.instrument_id == instrument_id)
    )
    if from_date:
        query = query.where(OHLCData.timestamp >= from_date)
        count_query = count_query.where(OHLCData.timestamp >= from_date)
    if to_date:
        query = query.where(OHLCData.timestamp <= to_date)
        count_query = count_query.where(OHLCData.timestamp <= to_date)
    if timeframe:
        query = query.where(OHLCData.timeframe == timeframe)
        count_query = count_query.where(OHLCData.timeframe == timeframe)
    if price_type:
        query = query.where(OHLCData.price_type == price_type)
        count_query = count_query.where(OHLCData.price_type == price_type)
    total_result = await db.execute(count_query)
    total = total_result.scalar_one()
    result = await db.execute(query.order_by(OHLCData.timestamp).offset(offset).limit(limit))
    return result.scalars().all(), total
