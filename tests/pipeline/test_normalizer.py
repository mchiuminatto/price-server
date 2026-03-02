"""Tests for the Price Normalizer step."""

import pandas as pd
import pytest

from pipeline.payload import PipelinePayload
from pipeline.steps.normalizer import NormalizerWorker, _normalize_ohlc, _normalize_tick

# ---------------------------------------------------------------------------
# Unit tests – pure transformation helpers
# ---------------------------------------------------------------------------


def test_normalize_tick_adds_derived_columns():
    df = pd.DataFrame(
        {
            "time": ["2024-01-01 00:00:00", "2024-01-01 00:00:01"],
            "bid": [1.09990, 1.09991],
            "ask": [1.09995, 1.09996],
            "volume": [1_000_000, 2_000_000],
        }
    )
    result = _normalize_tick(df)
    assert "timestamp" in result.columns
    assert "mid_price" in result.columns
    assert "spread" in result.columns
    assert pytest.approx(result["mid_price"].iloc[0]) == (1.09990 + 1.09995) / 2
    assert pytest.approx(result["spread"].iloc[0]) == 1.09995 - 1.09990


def test_normalize_ohlc_renames_columns():
    df = pd.DataFrame(
        {
            "time": ["2024-01-01 00:00:00"],
            "open_bid": [1.1],
            "high_bid_": [1.2],
            "low_bid": [1.0],
            "close_bid": [1.15],
            "volume_bis": [500],
            "open_ask": [1.101],
            "high_ask_": [1.201],
            "low_ask": [1.001],
            "close_ask": [1.151],
            "volume_ask": [500],
        }
    )
    result = _normalize_ohlc(df)
    assert "timestamp" in result.columns
    assert "high_bid" in result.columns
    assert "high_ask" in result.columns
    assert "volume_bid" in result.columns


# ---------------------------------------------------------------------------
# Integration test – full worker process()
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_normalizer_worker_writes_parquet(tmp_path, pipeline_settings):
    # Arrange: write a raw CSV to the input path
    raw_dir = tmp_path / "raw"
    raw_dir.mkdir(parents=True)
    csv_path = raw_dir / "EURUSD_2024.csv"
    csv_path.write_text("time,bid,ask,volume\n2024-01-01 00:00:00,1.0999,1.1001,1000000\n")

    pipeline_settings.input_base_path = str(raw_dir)
    pipeline_settings.output_base_path = str(tmp_path / "processed")

    payload = PipelinePayload(
        source_path=str(csv_path),
        instrument="EUR/USD",
        data_type="tick",
    )

    worker = NormalizerWorker(pipeline_settings, consumer_name="test")
    result = await worker.process(payload)

    assert result.normalizer is not None
    assert result.normalizer.status == "ok"
    assert result.normalizer.rows == 1
    assert result.normalizer.output_path.endswith(".parquet")


@pytest.mark.asyncio
async def test_normalizer_worker_handles_bad_file(tmp_path, pipeline_settings):
    payload = PipelinePayload(
        source_path="/nonexistent/path/file.csv",
        instrument="EUR/USD",
        data_type="tick",
    )
    worker = NormalizerWorker(pipeline_settings, consumer_name="test")
    result = await worker.process(payload)

    assert result.normalizer is not None
    assert result.normalizer.status == "error"
    assert result.normalizer.error is not None
