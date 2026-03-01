"""Tests for the Price Patcher step."""

import pytest
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from pipeline.payload import NormalizerResult, PipelinePayload
from pipeline.steps.patcher import PatcherWorker, _merge


# ---------------------------------------------------------------------------
# Unit tests
# ---------------------------------------------------------------------------

def _make_df(timestamps):
    return pd.DataFrame({
        "timestamp": pd.to_datetime(timestamps, utc=True),
        "bid": [1.0] * len(timestamps),
        "ask": [1.001] * len(timestamps),
    })


def test_merge_pure_append():
    existing = _make_df(["2024-01-01 00:00:00", "2024-01-01 00:01:00"])
    new = _make_df(["2024-01-01 00:02:00", "2024-01-01 00:03:00"])
    merged, action = _merge(existing, new)
    assert action == "append"
    assert len(merged) == 4


def test_merge_insert_backfill():
    existing = _make_df(["2024-01-01 00:02:00", "2024-01-01 00:03:00"])
    new = _make_df(["2024-01-01 00:00:00", "2024-01-01 00:01:00"])
    merged, action = _merge(existing, new)
    assert action == "insert"
    assert len(merged) == 4


def test_merge_deduplicates():
    existing = _make_df(["2024-01-01 00:00:00", "2024-01-01 00:01:00"])
    new = _make_df(["2024-01-01 00:01:00", "2024-01-01 00:02:00"])  # one overlap
    merged, _ = _merge(existing, new)
    assert len(merged) == 3


# ---------------------------------------------------------------------------
# Integration tests
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_patcher_creates_new_file(tmp_path, pipeline_settings):
    processed = tmp_path / "processed"
    processed.mkdir(parents=True)
    input_path = processed / "EURUSD_normalized.parquet"

    df = _make_df(["2024-01-01 00:00:00", "2024-01-01 00:01:00"])
    pq.write_table(pa.Table.from_pandas(df), str(input_path))

    pipeline_settings.output_base_path = str(processed)

    payload = PipelinePayload(
        source_path="EURUSD_2024.csv",
        instrument="EUR/USD",
        data_type="tick",
        normalizer=NormalizerResult(status="ok", output_path=str(input_path), rows=2),
    )

    worker = PatcherWorker(pipeline_settings, consumer_name="test")
    result = await worker.process(payload)

    assert result.patcher is not None
    assert result.patcher.status == "ok"
    assert result.patcher.action == "create"
    assert result.patcher.output_path.endswith(".staging")


@pytest.mark.asyncio
async def test_patcher_appends_to_existing(tmp_path, pipeline_settings):
    processed = tmp_path / "processed"
    tick_dir = processed / "tick"
    tick_dir.mkdir(parents=True)

    # Write the "existing" dataset
    existing_path = tick_dir / "EURUSD.parquet"
    existing_df = _make_df(["2024-01-01 00:00:00"])
    pq.write_table(pa.Table.from_pandas(existing_df), str(existing_path))

    # Write the new normalised file
    input_path = processed / "EURUSD_normalized.parquet"
    new_df = _make_df(["2024-01-01 00:01:00"])
    pq.write_table(pa.Table.from_pandas(new_df), str(input_path))

    pipeline_settings.output_base_path = str(processed)

    payload = PipelinePayload(
        source_path="EURUSD_2024.csv",
        instrument="EUR/USD",
        data_type="tick",
        normalizer=NormalizerResult(status="ok", output_path=str(input_path), rows=1),
    )

    worker = PatcherWorker(pipeline_settings, consumer_name="test")
    result = await worker.process(payload)

    assert result.patcher.status == "ok"
    assert result.patcher.action == "append"
