"""Tests for the File Mover step."""

import pytest
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from pipeline.payload import NormalizerResult, PatcherResult, PipelinePayload
from pipeline.steps.file_mover import FileMoverWorker


def _make_staging_file(path):
    df = pd.DataFrame({"timestamp": pd.to_datetime(["2024-01-01"], utc=True), "bid": [1.0]})
    pq.write_table(pa.Table.from_pandas(df), str(path))


@pytest.mark.asyncio
async def test_file_mover_promotes_staging_file(tmp_path, pipeline_settings):
    processed = tmp_path / "processed" / "tick"
    processed.mkdir(parents=True)

    staging = processed / "EURUSD.parquet.staging"
    _make_staging_file(staging)

    pipeline_settings.output_base_path = str(tmp_path / "processed")

    payload = PipelinePayload(
        source_path="EURUSD_2024.csv",
        instrument="EUR/USD",
        data_type="tick",
        normalizer=NormalizerResult(status="ok", output_path="", rows=1),
        patcher=PatcherResult(
            status="ok",
            action="create",
            output_path=str(staging),
        ),
    )

    worker = FileMoverWorker(pipeline_settings, consumer_name="test")
    result = await worker.process(payload)

    assert result.file_mover is not None
    assert result.file_mover.status == "ok"

    target = str(staging).removesuffix(".staging")
    assert result.file_mover.target_path == target
    assert (tmp_path / "processed" / "tick" / "EURUSD.parquet").exists()
    assert not staging.exists()


@pytest.mark.asyncio
async def test_file_mover_skips_on_upstream_error(pipeline_settings):
    payload = PipelinePayload(
        source_path="x.csv",
        instrument="EUR/USD",
        data_type="tick",
        patcher=PatcherResult(status="error", error="upstream failed"),
    )
    worker = FileMoverWorker(pipeline_settings, consumer_name="test")
    result = await worker.process(payload)

    assert result.file_mover is not None
    assert result.file_mover.status == "error"
