"""Tests for Step 6 abstraction builder workers."""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from pipeline.payload import PatcherResult, PipelinePayload
from pipeline.steps.abstractions.ohlc import OHLCBuilderWorker
from pipeline.steps.abstractions.pip_bar import PipBarBuilderWorker
from pipeline.steps.abstractions.renko import RenkoBarBuilderWorker
from pipeline.steps.abstractions.tick_bar import TickBarBuilderWorker

# ---------------------------------------------------------------------------
# Shared tick fixture
# ---------------------------------------------------------------------------


def _make_tick_df(n: int = 200) -> pd.DataFrame:
    """Generate synthetic tick data."""
    timestamps = pd.date_range("2024-01-01", periods=n, freq="1s", tz="UTC")
    mid = 1.1000 + np.cumsum(np.random.uniform(-0.0001, 0.0001, n))
    return pd.DataFrame(
        {
            "timestamp": timestamps,
            "bid": mid - 0.00005,
            "ask": mid + 0.00005,
            "mid_price": mid,
            "volume": np.ones(n) * 1000,
        }
    )


def _write_tick_parquet(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.Table.from_pandas(df), str(path))


def _make_payload(tmp_path, pipeline_settings) -> PipelinePayload:
    df = _make_tick_df(500)
    parquet_path = tmp_path / "processed" / "tick" / "EURUSD.parquet"
    _write_tick_parquet(df, parquet_path)
    pipeline_settings.output_base_path = str(tmp_path / "processed")

    return PipelinePayload(
        source_path="EURUSD_2024.csv",
        instrument="EUR/USD",
        data_type="tick",
        patcher=PatcherResult(status="ok", action="create", output_path=str(parquet_path)),
    )


# ---------------------------------------------------------------------------
# Unit tests – build() helpers
# ---------------------------------------------------------------------------


def test_ohlc_build_produces_ohlc_columns():
    from pipeline.steps.abstractions.ohlc import OHLCBuilderWorker

    worker = OHLCBuilderWorker.__new__(OHLCBuilderWorker)
    df = _make_tick_df(120)
    result = worker.build(df, {"type": "ohlc", "timeframe": "1min"})
    assert set(["timestamp", "open", "high", "low", "close"]).issubset(result.columns)
    assert len(result) > 0
    assert (result["high"] >= result["low"]).all()


def test_tick_bar_build_groups_by_tick_count():
    from pipeline.steps.abstractions.tick_bar import TickBarBuilderWorker

    worker = TickBarBuilderWorker.__new__(TickBarBuilderWorker)
    df = _make_tick_df(100)
    result = worker.build(df, {"type": "tick_bar", "tick_count": 10})
    assert len(result) == 10
    assert (result["high"] >= result["low"]).all()


def test_pip_bar_build_produces_bars():
    from pipeline.steps.abstractions.pip_bar import PipBarBuilderWorker

    worker = PipBarBuilderWorker.__new__(PipBarBuilderWorker)
    df = _make_tick_df(500)
    result = worker.build(df, {"type": "pip_bar", "pip_range": 1, "pip_size": 0.0001})
    assert set(["timestamp", "open", "high", "low", "close"]).issubset(result.columns)
    assert (result["high"] >= result["low"]).all()


def test_renko_build_produces_bricks():
    from pipeline.steps.abstractions.renko import RenkoBarBuilderWorker

    worker = RenkoBarBuilderWorker.__new__(RenkoBarBuilderWorker)
    df = _make_tick_df(500)
    result = worker.build(df, {"type": "renko", "body_pips": 1, "pip_size": 0.0001})
    assert set(["timestamp", "open", "high", "low", "close", "direction"]).issubset(result.columns)
    assert set(result["direction"].unique()).issubset({"up", "down"})


# ---------------------------------------------------------------------------
# Integration tests – full worker process()
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_ohlc_worker_writes_parquet(tmp_path, pipeline_settings):
    payload = _make_payload(tmp_path, pipeline_settings)
    payload.abstraction_spec = {"type": "ohlc", "timeframe": "1min"}
    payload.abstraction_label = "ohlc_1min"

    worker = OHLCBuilderWorker(pipeline_settings, consumer_name="test")
    result = await worker.process(payload)

    assert "ohlc_1min" in result.abstractions
    assert result.abstractions["ohlc_1min"].status == "ok"
    assert result.abstractions["ohlc_1min"].rows > 0


@pytest.mark.asyncio
async def test_tick_bar_worker_writes_parquet(tmp_path, pipeline_settings):
    payload = _make_payload(tmp_path, pipeline_settings)
    payload.abstraction_spec = {"type": "tick_bar", "tick_count": 50}
    payload.abstraction_label = "tick_bar_50"

    worker = TickBarBuilderWorker(pipeline_settings, consumer_name="test")
    result = await worker.process(payload)

    assert "tick_bar_50" in result.abstractions
    assert result.abstractions["tick_bar_50"].status == "ok"


@pytest.mark.asyncio
async def test_pip_bar_worker_writes_parquet(tmp_path, pipeline_settings):
    payload = _make_payload(tmp_path, pipeline_settings)
    payload.abstraction_spec = {"type": "pip_bar", "pip_range": 1, "pip_size": 0.0001}
    payload.abstraction_label = "pip_bar_1"

    worker = PipBarBuilderWorker(pipeline_settings, consumer_name="test")
    result = await worker.process(payload)

    assert "pip_bar_1" in result.abstractions
    assert result.abstractions["pip_bar_1"].status == "ok"


@pytest.mark.asyncio
async def test_renko_worker_writes_parquet(tmp_path, pipeline_settings):
    payload = _make_payload(tmp_path, pipeline_settings)
    payload.abstraction_spec = {"type": "renko", "body_pips": 1, "pip_size": 0.0001}
    payload.abstraction_label = "renko_1"

    worker = RenkoBarBuilderWorker(pipeline_settings, consumer_name="test")
    result = await worker.process(payload)

    assert "renko_1" in result.abstractions
    assert result.abstractions["renko_1"].status == "ok"
