"""Tests for the Abstraction Distributor (Step 5)."""

from __future__ import annotations

import pytest
import yaml

from pipeline.payload import PatcherResult, PipelinePayload
from pipeline.steps.distributor import DistributorWorker, _load_config, _make_label

# ---------------------------------------------------------------------------
# Unit tests
# ---------------------------------------------------------------------------


def test_make_label_ohlc():
    assert _make_label({"type": "ohlc", "timeframe": "5min"}) == "ohlc_5min"


def test_make_label_tick_bar():
    assert _make_label({"type": "tick_bar", "tick_count": 1000}) == "tick_bar_1000"


def test_make_label_pip_bar():
    assert _make_label({"type": "pip_bar", "pip_range": 10}) == "pip_bar_10"


def test_make_label_renko():
    assert _make_label({"type": "renko", "body_pips": 5}) == "renko_5"


def test_load_config_reads_yaml(tmp_path, pipeline_settings):
    cfg = {"abstractions": [{"type": "ohlc", "timeframe": "1min"}]}
    config_file = tmp_path / "pipeline_config.yaml"
    config_file.write_text(yaml.dump(cfg))

    pipeline_settings.config_path = str(config_file)

    from pipeline.storage import StorageBackend

    storage = StorageBackend.from_settings(pipeline_settings)
    result = _load_config(str(config_file), storage)

    assert len(result["abstractions"]) == 1
    assert result["abstractions"][0]["type"] == "ohlc"


def test_load_config_returns_empty_on_missing(tmp_path, pipeline_settings):
    from pipeline.storage import StorageBackend

    storage = StorageBackend.from_settings(pipeline_settings)
    result = _load_config("/nonexistent/config.yaml", storage)
    assert result == {}


# ---------------------------------------------------------------------------
# Integration tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_distributor_dispatches_to_correct_queues(tmp_path, pipeline_settings, fake_redis):
    # Write a config with two abstractions
    cfg = {
        "abstractions": [
            {"type": "ohlc", "timeframe": "1min"},
            {"type": "renko", "body_pips": 5},
        ]
    }
    config_file = tmp_path / "pipeline_config.yaml"
    config_file.write_text(yaml.dump(cfg))
    pipeline_settings.config_path = str(config_file)

    payload = PipelinePayload(
        source_path="EURUSD_2024.csv",
        instrument="EUR/USD",
        data_type="tick",
        config_path=str(config_file),
        patcher=PatcherResult(status="ok", action="append", output_path="/data/EURUSD.parquet"),
    )

    worker = DistributorWorker(pipeline_settings, consumer_name="test")
    worker._redis = fake_redis
    worker._output_queues = []

    from pipeline.queue import StreamQueue

    for stream in worker.output_streams:
        q = StreamQueue(fake_redis, stream=stream, group=f"{stream}_group", consumer="test")
        await q.ensure_group()
        worker._output_queues.append(q)

    result = await worker.process(payload)

    assert result is None  # distributor returns None (direct fan-out)
    assert payload.distributor is not None
    assert payload.distributor.status == "ok"
    assert "ohlc_1min" in payload.distributor.abstractions_dispatched
    assert "renko_5" in payload.distributor.abstractions_dispatched

    # Verify messages actually landed in the right streams
    ohlc_msgs = await fake_redis.xrange("ohlc_queue")
    renko_msgs = await fake_redis.xrange("renko_bar_queue")
    assert len(ohlc_msgs) == 1
    assert len(renko_msgs) == 1


@pytest.mark.asyncio
async def test_distributor_skips_on_upstream_error(pipeline_settings, fake_redis):
    payload = PipelinePayload(
        source_path="x.csv",
        instrument="EUR/USD",
        data_type="tick",
        patcher=PatcherResult(status="error", error="upstream failed"),
    )
    worker = DistributorWorker(pipeline_settings, consumer_name="test")
    result = await worker.process(payload)
    assert result is None
    assert payload.distributor is None
