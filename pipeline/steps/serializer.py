"""
Step 1 – Work Queue Serializer

Lists all files under `settings.input_base_path` and publishes one
PipelinePayload message per file onto the `normalizer_queue` stream.

Run once (or on a schedule) to seed the pipeline.
"""

from __future__ import annotations

import logging

import redis.asyncio as aioredis

from pipeline.config import PipelineSettings
from pipeline.payload import PipelinePayload
from pipeline.queue import StreamQueue
from pipeline.storage import StorageBackend

logger = logging.getLogger(__name__)

NORMALIZER_QUEUE = "normalizer_queue"
NORMALIZER_GROUP = "normalizer_group"


async def serialize_work(
    settings: PipelineSettings,
    data_type: str = "tick",
) -> int:
    """
    Scan input storage, publish one message per file to normalizer_queue.
    Returns the number of files enqueued.
    """
    storage = StorageBackend.from_settings(settings)
    r = aioredis.from_url(settings.redis_url, decode_responses=False)

    queue = StreamQueue(
        client=r,
        stream=NORMALIZER_QUEUE,
        group=NORMALIZER_GROUP,
        consumer="serializer",
    )
    await queue.ensure_group()

    files = [
        p for p in storage.ls(settings.input_base_path)
        if p.endswith(".csv")
    ]
    logger.info("Found %d file(s) to enqueue.", len(files))

    for path in files:
        # Derive instrument from filename convention: e.g. EURUSD_2024.csv → EUR/USD
        filename = path.split("/")[-1]
        symbol = filename.split("_")[0]
        instrument = f"{symbol[:3]}/{symbol[3:]}" if len(symbol) >= 6 else symbol

        payload = PipelinePayload(
            source_path=path,
            instrument=instrument,
            data_type=data_type,  # type: ignore[arg-type]
        )
        await queue.publish(payload.model_dump())
        logger.info("Enqueued: %s", path)

    await r.aclose()
    return len(files)
