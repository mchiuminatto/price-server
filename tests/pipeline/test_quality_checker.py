"""Tests for the Quality Checker step."""

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from pipeline.payload import NormalizerResult, PipelinePayload
from pipeline.steps.quality_checker import QualityCheckerWorker, _detect_gaps

# ---------------------------------------------------------------------------
# Unit tests
# ---------------------------------------------------------------------------


def test_detect_gaps_finds_large_gap():
    df = pd.DataFrame(
        {
            "timestamp": pd.to_datetime(
                [
                    "2024-01-01 00:00:00",
                    "2024-01-01 00:00:01",
                    "2024-01-01 00:05:00",  # 299-second gap → should be flagged
                ],
                utc=True,
            ),
        }
    )
    gaps = _detect_gaps(df, data_type="tick")
    assert len(gaps) == 1
    assert gaps["gap_seconds"].iloc[0] > 60


def test_detect_gaps_no_gaps():
    df = pd.DataFrame(
        {
            "timestamp": pd.to_datetime(
                [
                    "2024-01-01 00:00:00",
                    "2024-01-01 00:00:01",
                    "2024-01-01 00:00:02",
                ],
                utc=True,
            ),
        }
    )
    gaps = _detect_gaps(df, data_type="tick")
    assert len(gaps) == 0


# ---------------------------------------------------------------------------
# Integration test
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_quality_checker_produces_report(tmp_path, pipeline_settings):
    # Write a normalised parquet file with one gap
    processed_dir = tmp_path / "processed"
    processed_dir.mkdir(parents=True)
    parquet_path = processed_dir / "EURUSD_normalized.parquet"

    df = pd.DataFrame(
        {
            "timestamp": pd.to_datetime(
                [
                    "2024-01-01 00:00:00",
                    "2024-01-01 00:00:01",
                    "2024-01-01 00:10:00",  # big gap
                ],
                utc=True,
            ),
            "bid": [1.0999, 1.0998, 1.0997],
            "ask": [1.1001, 1.1000, 1.0999],
            "mid_price": [1.1000, 1.0999, 1.0998],
            "spread": [0.0002, 0.0002, 0.0002],
        }
    )
    pq.write_table(pa.Table.from_pandas(df), str(parquet_path))

    pipeline_settings.output_base_path = str(processed_dir)

    payload = PipelinePayload(
        source_path="EURUSD_2024.csv",
        instrument="EUR/USD",
        data_type="tick",
        normalizer=NormalizerResult(
            status="ok",
            output_path=str(parquet_path),
            rows=3,
        ),
    )

    worker = QualityCheckerWorker(pipeline_settings, consumer_name="test")
    result = await worker.process(payload)

    assert result.quality is not None
    assert result.quality.status == "ok"
    assert result.quality.gaps_found == 1


@pytest.mark.asyncio
async def test_quality_checker_skips_on_upstream_error(pipeline_settings):
    payload = PipelinePayload(
        source_path="x.csv",
        instrument="EUR/USD",
        data_type="tick",
        normalizer=NormalizerResult(status="error", error="upstream failed"),
    )
    worker = QualityCheckerWorker(pipeline_settings, consumer_name="test")
    result = await worker.process(payload)

    assert result.quality is not None
    assert result.quality.status == "error"
