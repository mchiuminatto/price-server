# filepath: /home/mchiuminatto/work/dev/price-server/tests/pipeline/test_serializer.py
"""Tests for the Work Queue Serializer (Step 1)."""

from __future__ import annotations

import os
import pytest

from pipeline.payload import PipelinePayload
from pipeline.steps.serializer import (
    SerializerWorker,
    _derive_instrument,
    _list_price_files,
)
from pipeline.storage import StorageBackend

# ---------------------------------------------------------------------------
# Unit tests – helper functions
# ---------------------------------------------------------------------------


class TestDeriveInstrument:
    """Tests for _derive_instrument()."""

    def test_six_char_pair(self):
        assert _derive_instrument("EURUSD_Ticks_2026.csv") == "EUR/USD"

    def test_six_char_pair_no_suffix(self):
        assert _derive_instrument("GBPUSD.csv") == "GBP/USD"

    def test_longer_stem(self):
        """Stems longer than 6 chars still split at the 3/3 boundary."""
        assert _derive_instrument("XAUUSD_2024.csv") == "XAU/USD"

    def test_short_stem_passthrough(self):
        """Stems shorter than 6 chars are returned as-is."""
        assert _derive_instrument("SHORT.csv") == "SHORT"

    def test_single_char_stem(self):
        assert _derive_instrument("A.csv") == "A"

    def test_exactly_six_chars(self):
        assert _derive_instrument("NZDUSD.csv") == "NZD/USD"


class TestListPriceFiles:
    """Tests for _list_price_files()."""

    def test_finds_csv_files(self, tmp_path, pipeline_settings):
        data_dir = tmp_path / "data"
        data_dir.mkdir()
        (data_dir / "EURUSD_2024.csv").write_text("header\n")
        (data_dir / "GBPUSD_2024.csv").write_text("header\n")
        (data_dir / "readme.txt").write_text("ignore me")
        (data_dir / "data.parquet").write_bytes(b"\x00")

        storage = StorageBackend.from_settings(pipeline_settings)
        files = _list_price_files(storage, str(data_dir))

        assert len(files) == 2
        assert all(f.endswith(".csv") for f in files)

    def test_returns_sorted(self, tmp_path, pipeline_settings):
        data_dir = tmp_path / "data"
        data_dir.mkdir()
        (data_dir / "C.csv").write_text("x\n")
        (data_dir / "A.csv").write_text("x\n")
        (data_dir / "B.csv").write_text("x\n")

        storage = StorageBackend.from_settings(pipeline_settings)
        files = _list_price_files(storage, str(data_dir))

        filenames = [os.path.basename(f) for f in files]
        assert filenames == ["A.csv", "B.csv", "C.csv"]

    def test_empty_directory(self, tmp_path, pipeline_settings):
        data_dir = tmp_path / "data"
        data_dir.mkdir()

        storage = StorageBackend.from_settings(pipeline_settings)
        files = _list_price_files(storage, str(data_dir))
        assert files == []

    def test_nonexistent_path(self, tmp_path, pipeline_settings):
        storage = StorageBackend.from_settings(pipeline_settings)
        files = _list_price_files(storage, str(tmp_path / "no_such_dir"))
        assert files == []


# ---------------------------------------------------------------------------
# Integration tests – SerializerWorker.process()
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_serializer_enqueues_all_files(tmp_path, pipeline_settings, fake_redis):
    """process() should publish one PipelinePayload per CSV file found."""
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    (data_dir / "EURUSD_Ticks_2026.csv").write_text("time,bid,ask,volume\n")
    (data_dir / "GBPUSD_Ticks_2026.csv").write_text("time,bid,ask,volume\n")
    (data_dir / "USDJPY_Ticks_2026.csv").write_text("time,bid,ask,volume\n")

    pipeline_settings.input_base_path = str(data_dir)

    worker = SerializerWorker(pipeline_settings, consumer_name="test")
    worker._redis = fake_redis
    worker._output_queues = []

    from pipeline.queue import StreamQueue

    q = StreamQueue(
        fake_redis,
        stream="normalizer_queue",
        group="normalizer_group",
        consumer="test",
    )
    await q.ensure_group()
    worker._output_queues.append(q)

    trigger = {"storage_path": str(data_dir), "data_type": "tick"}
    count = await worker.process(trigger)

    assert count == 3

    # Consume all 3 messages back and verify payloads
    consumed = []
    for _ in range(3):
        result = await q.consume()
        assert result is not None
        _, payload_dict = result
        consumed.append(PipelinePayload(**payload_dict))

    instruments = sorted(p.instrument for p in consumed)
    assert instruments == ["EUR/USD", "GBP/USD", "USD/JPY"]

    # All should have data_type = "tick"
    assert all(p.data_type == "tick" for p in consumed)


@pytest.mark.asyncio
async def test_serializer_uses_default_storage_path(tmp_path, pipeline_settings, fake_redis):
    """When trigger has no storage_path, it falls back to settings.input_base_path."""
    data_dir = tmp_path / "raw"
    data_dir.mkdir()
    (data_dir / "AUDUSD.csv").write_text("time,bid,ask,volume\n")

    pipeline_settings.input_base_path = str(data_dir)

    worker = SerializerWorker(pipeline_settings, consumer_name="test")
    worker._redis = fake_redis
    worker._output_queues = []

    from pipeline.queue import StreamQueue

    q = StreamQueue(
        fake_redis,
        stream="normalizer_queue",
        group="normalizer_group",
        consumer="test",
    )
    await q.ensure_group()
    worker._output_queues.append(q)

    # Trigger without storage_path
    trigger = {"data_type": "tick"}
    count = await worker.process(trigger)

    assert count == 1


@pytest.mark.asyncio
async def test_serializer_forwards_config_path(tmp_path, pipeline_settings, fake_redis):
    """process() should forward config_path into the published payloads."""
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    (data_dir / "NZDUSD.csv").write_text("time,bid,ask,volume\n")

    pipeline_settings.input_base_path = str(data_dir)

    worker = SerializerWorker(pipeline_settings, consumer_name="test")
    worker._redis = fake_redis
    worker._output_queues = []

    from pipeline.queue import StreamQueue

    q = StreamQueue(
        fake_redis,
        stream="normalizer_queue",
        group="normalizer_group",
        consumer="test",
    )
    await q.ensure_group()
    worker._output_queues.append(q)

    trigger = {
        "storage_path": str(data_dir),
        "data_type": "ohlc",
        "config_path": "/opt/pipeline/config.yaml",
    }
    count = await worker.process(trigger)
    assert count == 1

    result = await q.consume()
    assert result is not None
    _, payload_dict = result
    payload = PipelinePayload(**payload_dict)
    assert payload.data_type == "ohlc"
    assert payload.config_path == "/opt/pipeline/config.yaml"
    assert payload.instrument == "NZD/USD"


@pytest.mark.asyncio
async def test_serializer_zero_files(tmp_path, pipeline_settings, fake_redis):
    """process() with empty directory returns 0 and publishes nothing."""
    data_dir = tmp_path / "empty"
    data_dir.mkdir()

    worker = SerializerWorker(pipeline_settings, consumer_name="test")
    worker._redis = fake_redis
    worker._output_queues = []

    from pipeline.queue import StreamQueue

    q = StreamQueue(
        fake_redis,
        stream="normalizer_queue",
        group="normalizer_group",
        consumer="test",
        block_ms=100,  # short timeout so test doesn't hang
    )
    await q.ensure_group()
    worker._output_queues.append(q)

    trigger = {"storage_path": str(data_dir), "data_type": "tick"}
    count = await worker.process(trigger)
    assert count == 0

    # No messages should be waiting
    result = await q.consume()
    assert result is None
