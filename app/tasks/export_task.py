import csv
import os
import logging
from datetime import datetime
from typing import Optional
from sqlalchemy.ext.asyncio import async_sessionmaker

from app.services.price_service import get_tick_data, get_ohlc_data
from app.models.price import TimeFrame, PriceType

logger = logging.getLogger(__name__)

# In-memory job store for single-process development use.
# For production with multiple workers, replace with a persistent backend (e.g., Redis or a DB table).
jobs = {}


async def export_tick_data(
    job_id: str,
    instrument_id: int,
    from_date: Optional[datetime],
    to_date: Optional[datetime],
    db_session_factory: async_sessionmaker,
    export_dir: str,
):
    jobs[job_id] = {"status": "processing", "file_path": None, "error": None}
    try:
        async with db_session_factory() as db:
            items, total = await get_tick_data(
                db, instrument_id, from_date, to_date, limit=1000000, offset=0
            )
        file_path = os.path.join(export_dir, f"{job_id}.csv")
        os.makedirs(export_dir, exist_ok=True)
        with open(file_path, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["id", "instrument_id", "timestamp", "bid", "ask", "volume"])
            for item in items:
                writer.writerow(
                    [item.id, item.instrument_id, item.timestamp, item.bid, item.ask, item.volume]
                )
        jobs[job_id] = {"status": "completed", "file_path": file_path, "error": None}
    except Exception as e:
        logger.error(f"Export tick job {job_id} failed: {e}")
        jobs[job_id] = {"status": "failed", "file_path": None, "error": str(e)}


async def export_ohlc_data(
    job_id: str,
    instrument_id: int,
    from_date: Optional[datetime],
    to_date: Optional[datetime],
    timeframe: Optional[TimeFrame],
    price_type: Optional[PriceType],
    db_session_factory: async_sessionmaker,
    export_dir: str,
):
    jobs[job_id] = {"status": "processing", "file_path": None, "error": None}
    try:
        async with db_session_factory() as db:
            items, total = await get_ohlc_data(
                db, instrument_id, from_date, to_date, timeframe, price_type,
                limit=1000000, offset=0,
            )
        file_path = os.path.join(export_dir, f"{job_id}.csv")
        os.makedirs(export_dir, exist_ok=True)
        with open(file_path, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(
                [
                    "id",
                    "instrument_id",
                    "timestamp",
                    "open",
                    "high",
                    "low",
                    "close",
                    "volume",
                    "timeframe",
                    "price_type",
                ]
            )
            for item in items:
                writer.writerow(
                    [
                        item.id,
                        item.instrument_id,
                        item.timestamp,
                        item.open,
                        item.high,
                        item.low,
                        item.close,
                        item.volume,
                        item.timeframe,
                        item.price_type,
                    ]
                )
        jobs[job_id] = {"status": "completed", "file_path": file_path, "error": None}
    except Exception as e:
        logger.error(f"Export OHLC job {job_id} failed: {e}")
        jobs[job_id] = {"status": "failed", "file_path": None, "error": str(e)}
