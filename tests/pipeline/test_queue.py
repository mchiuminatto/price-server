"""Tests for the StreamQueue abstraction."""

import pytest

from pipeline.queue import StreamQueue


@pytest.mark.asyncio
async def test_publish_and_consume(fake_redis):
    q = StreamQueue(fake_redis, "mystream", "mygroup", "worker-1")
    await q.ensure_group()

    payload = {"source_path": "/data/raw/EURUSD.csv", "instrument": "EUR/USD"}
    await q.publish(payload)

    result = await q.consume()
    assert result is not None
    msg_id, received = result
    assert received["source_path"] == payload["source_path"]
    assert received["instrument"] == payload["instrument"]


@pytest.mark.asyncio
async def test_ack_removes_from_pending(fake_redis):
    q = StreamQueue(fake_redis, "ackstream", "ackgroup", "worker-1")
    await q.ensure_group()

    await q.publish({"foo": "bar"})
    result = await q.consume()
    assert result is not None
    msg_id, _ = result

    await q.ack(msg_id)

    # PEL should now be empty
    pending = await fake_redis.xpending("ackstream", "ackgroup")
    assert pending["pending"] == 0


@pytest.mark.asyncio
async def test_consume_returns_none_on_empty_stream(fake_redis):
    q = StreamQueue(fake_redis, "emptystream", "emptygroup", "worker-1", block_ms=100)
    await q.ensure_group()

    result = await q.consume()
    assert result is None


@pytest.mark.asyncio
async def test_ensure_group_is_idempotent(fake_redis):
    q = StreamQueue(fake_redis, "idempstream", "idempgroup", "worker-1")
    await q.ensure_group()
    # Should not raise on second call
    await q.ensure_group()
