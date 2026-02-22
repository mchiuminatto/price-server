import uuid
import os
from datetime import datetime
from typing import Optional
from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException
from fastapi.responses import FileResponse, JSONResponse
from sqlalchemy.ext.asyncio import AsyncSession
from app.database import get_db, async_session_maker
from app.config import settings
from app.schemas.price import ExportJobResponse, TimeFrame, PriceType
from app.tasks.export_task import jobs, export_tick_data, export_ohlc_data

router = APIRouter()


@router.post("/tick/{instrument_id}/export", response_model=ExportJobResponse, status_code=202)
async def export_tick(
    instrument_id: int,
    background_tasks: BackgroundTasks,
    from_date: Optional[datetime] = None,
    to_date: Optional[datetime] = None,
    db: AsyncSession = Depends(get_db),
):
    job_id = str(uuid.uuid4())
    jobs[job_id] = {"status": "pending", "file_path": None, "error": None}
    background_tasks.add_task(
        export_tick_data,
        job_id,
        instrument_id,
        from_date,
        to_date,
        async_session_maker,
        settings.ASYNC_EXPORT_DIR,
    )
    return ExportJobResponse(job_id=job_id, status="pending", message="Export started")


@router.post("/ohlc/{instrument_id}/export", response_model=ExportJobResponse, status_code=202)
async def export_ohlc(
    instrument_id: int,
    background_tasks: BackgroundTasks,
    from_date: Optional[datetime] = None,
    to_date: Optional[datetime] = None,
    timeframe: Optional[TimeFrame] = None,
    price_type: Optional[PriceType] = None,
    db: AsyncSession = Depends(get_db),
):
    job_id = str(uuid.uuid4())
    jobs[job_id] = {"status": "pending", "file_path": None, "error": None}
    background_tasks.add_task(
        export_ohlc_data,
        job_id,
        instrument_id,
        from_date,
        to_date,
        timeframe,
        price_type,
        async_session_maker,
        settings.ASYNC_EXPORT_DIR,
    )
    return ExportJobResponse(job_id=job_id, status="pending", message="Export started")


@router.get("/export/{job_id}/status", response_model=ExportJobResponse)
async def get_export_status(job_id: str):
    job = jobs.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    download_url = f"/prices/export/{job_id}/download" if job["status"] == "completed" else None
    return ExportJobResponse(
        job_id=job_id,
        status=job["status"],
        message=job.get("error") or job["status"],
        download_url=download_url,
    )


@router.get("/export/{job_id}/download")
async def download_export(job_id: str):
    job = jobs.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    if job["status"] != "completed":
        return JSONResponse(
            status_code=202, content={"status": job["status"], "message": "Export not ready"}
        )
    file_path = job["file_path"]
    if not file_path or not os.path.exists(file_path):
        raise HTTPException(status_code=404, detail="Export file not found")
    return FileResponse(file_path, filename=f"{job_id}.csv", media_type="text/csv")
