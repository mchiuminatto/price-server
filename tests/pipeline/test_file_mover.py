# filepath: /home/mchiuminatto/work/dev/price-server/tests/pipeline/test_file_mover.py
"""Tests for the File Mover step."""

from __future__ import annotations

import pytest

from pipeline.payload import PatcherResult, PipelinePayload
from pipeline.steps.file_mover import FileMoverWorker

# ---------------------------------------------------------------------------
# Integration tests – FileMoverWorker.process()
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_file_mover_moves_staging_file(tmp_path, pipeline_settings):
    """The worker should move a .staging file to the final path."""
    staging_path = tmp_path / "processed" / "tick" / "EURUSD.parquet.staging"
    staging_path.parent.mkdir(parents=True)
    staging_path.write_bytes(b"parquet-data")

    pipeline_settings.output_base_path = str(tmp_path / "processed")

    payload = PipelinePayload(
        source_path="EURUSD_2024.csv",
        instrument="EUR/USD",
        data_type="tick",
        patcher=PatcherResult(
            status="ok",
            action="create",
            output_path=str(staging_path),
        ),
    )

    worker = FileMoverWorker(pipeline_settings, consumer_name="test")
    result = await worker.process(payload)

    assert result.file_mover is not None
    assert result.file_mover.status == "ok"
    expected_target = str(staging_path).removesuffix(".staging")
    assert result.file_mover.target_path == expected_target
    # The staging file should no longer exist, the target should
    assert not staging_path.exists()
    assert (tmp_path / "processed" / "tick" / "EURUSD.parquet").exists()


@pytest.mark.asyncio
async def test_file_mover_skips_on_upstream_error(tmp_path, pipeline_settings):
    """When patcher status is error, file_mover should skip and mark error."""
    payload = PipelinePayload(
        source_path="EURUSD_2024.csv",
        instrument="EUR/USD",
        data_type="tick",
        patcher=PatcherResult(status="error", error="patcher failed"),
    )

    worker = FileMoverWorker(pipeline_settings, consumer_name="test")
    result = await worker.process(payload)

    assert result.file_mover is not None
    assert result.file_mover.status == "error"
    assert "upstream" in result.file_mover.error.lower()


@pytest.mark.asyncio
async def test_file_mover_skips_when_no_patcher(tmp_path, pipeline_settings):
    """When patcher result is None, file_mover should skip."""
    payload = PipelinePayload(
        source_path="EURUSD_2024.csv",
        instrument="EUR/USD",
        data_type="tick",
        patcher=None,
    )

    worker = FileMoverWorker(pipeline_settings, consumer_name="test")
    result = await worker.process(payload)

    assert result.file_mover is not None
    assert result.file_mover.status == "error"


@pytest.mark.asyncio
async def test_file_mover_handles_missing_source(tmp_path, pipeline_settings):
    """When the staging file doesn't exist, the worker should set error status."""
    pipeline_settings.output_base_path = str(tmp_path / "processed")

    payload = PipelinePayload(
        source_path="EURUSD_2024.csv",
        instrument="EUR/USD",
        data_type="tick",
        patcher=PatcherResult(
            status="ok",
            action="create",
            output_path=str(tmp_path / "nonexistent.parquet.staging"),
        ),
    )

    worker = FileMoverWorker(pipeline_settings, consumer_name="test")
    result = await worker.process(payload)

    assert result.file_mover is not None
    assert result.file_mover.status == "error"
    assert result.file_mover.error is not None
