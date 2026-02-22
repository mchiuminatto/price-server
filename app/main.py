import os
import logging
from contextlib import asynccontextmanager
from fastapi import FastAPI
from app.config import settings
from app.database import init_db
from app.routers import assets, prices, async_prices

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    os.makedirs(settings.ASYNC_EXPORT_DIR, exist_ok=True)
    await init_db()
    yield


app = FastAPI(title=settings.APP_NAME, version=settings.APP_VERSION, lifespan=lifespan)

app.include_router(assets.router, prefix="/assets", tags=["assets"])
app.include_router(prices.router, prefix="/prices", tags=["prices"])
app.include_router(async_prices.router, prefix="/prices", tags=["async-prices"])


@app.get("/health")
async def health():
    return {"status": "ok", "version": settings.APP_VERSION}
