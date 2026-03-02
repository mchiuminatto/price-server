"""
Shared fixtures for pipeline tests.
Uses fakeredis for an in-process Redis substitute (no real server needed).
Uses tmp_path for a local filesystem storage backend.
"""

import fakeredis.aioredis as fakeredis
import pytest
import pytest_asyncio

from pipeline.config import PipelineSettings
from pipeline.queue import StreamQueue


@pytest.fixture
def pipeline_settings(tmp_path) -> PipelineSettings:
    """Settings pointing at tmp_path for both input and output storage."""
    return PipelineSettings(
        redis_url="redis://localhost:6379/0",  # overridden by fake_redis fixture
        storage_backend="local",
        input_base_path=str(tmp_path / "raw"),
        output_base_path=str(tmp_path / "processed"),
        worker_count=1,
    )


@pytest_asyncio.fixture
async def fake_redis():
    """In-process fake Redis client."""
    client = fakeredis.FakeRedis(decode_responses=False)
    yield client
    await client.aclose()


@pytest_asyncio.fixture
async def stream_queue(fake_redis):
    """A StreamQueue backed by fakeredis."""
    q = StreamQueue(
        client=fake_redis,
        stream="test_stream",
        group="test_group",
        consumer="test_consumer",
    )
    await q.ensure_group()
    return q
