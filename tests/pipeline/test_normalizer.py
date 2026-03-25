"""Tests for the Price Normalizer step (Step 2)."""

from __future__ import annotations

import pandas as pd
import pyarrow.parquet as pq
import pytest

from pipeline.payload import PipelinePayload
from pipeline.steps.normalizer import (
    DukascopyTickMapper,
    GenericOhlcMapper,
    GenericTickMapper,
    NormalizerWorker,
    build_output_path,
    detect_mapper,
    extract_metadata,
)


# ---------------------------------------------------------------------------
# Unit tests – DukascopyTickMapper
# ---------------------------------------------------------------------------


class TestDukascopyTickMapper:
    """Dukascopy columns: Time (<tz>), Ask, Bid, AskVolume, BidVolume."""

    def test_renames_dukascopy_columns(self):
        df = pd.DataFrame(
            {
                "Time (Europe/London)": ["2026-03-01 22:00:01"],
                "Ask": [1.17752],
                "Bid": [1.17751],
                "AskVolume": [0.9],
                "BidVolume": [0.9],
            }
        )
        mapper = DukascopyTickMapper()
        result = mapper.rename(df)
        assert list(result.columns) == ["timestamp", "ask", "bid", "ask_volume", "bid_volume"]

    def test_handles_different_timezone_annotation(self):
        df = pd.DataFrame(
            {
                "Time (UTC)": ["2026-03-01"],
                "Ask": [1.1],
                "Bid": [1.0],
                "AskVolume": [1.0],
                "BidVolume": [1.0],
            }
        )
        result = DukascopyTickMapper().rename(df)
        assert "timestamp" in result.columns


# ---------------------------------------------------------------------------
# Unit tests – GenericTickMapper
# ---------------------------------------------------------------------------


class TestGenericTickMapper:
    def test_renames_generic_columns(self):
        df = pd.DataFrame(
            {
                "time": ["2024-01-01 00:00:00"],
                "bid": [1.09990],
                "ask": [1.09995],
                "volume": [1_000_000],
            }
        )
        result = GenericTickMapper().rename(df)
        assert "timestamp" in result.columns
        assert "volume" in result.columns


# ---------------------------------------------------------------------------
# Unit tests – GenericOhlcMapper
# ---------------------------------------------------------------------------


class TestGenericOhlcMapper:
    def test_renames_ohlc_columns(self):
        df = pd.DataFrame(
            {
                "time": ["2024-01-01"],
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
        result = GenericOhlcMapper().rename(df)
        assert "timestamp" in result.columns
        assert "high_bid" in result.columns
        assert "high_ask" in result.columns
        assert "volume_bid" in result.columns


# ---------------------------------------------------------------------------
# Unit tests – detect_mapper
# ---------------------------------------------------------------------------


class TestDetectMapper:
    def test_detects_dukascopy_tick(self):
        df = pd.DataFrame(columns=["Time (Europe/London)", "Ask", "Bid", "AskVolume", "BidVolume"])
        mapper = detect_mapper(df, "tick")
        assert isinstance(mapper, DukascopyTickMapper)

    def test_detects_generic_tick(self):
        df = pd.DataFrame(columns=["time", "bid", "ask", "volume"])
        mapper = detect_mapper(df, "tick")
        assert isinstance(mapper, GenericTickMapper)

    def test_detects_ohlc(self):
        df = pd.DataFrame(columns=["time", "open_bid", "high_bid_", "low_bid"])
        mapper = detect_mapper(df, "ohlc")
        assert isinstance(mapper, GenericOhlcMapper)


# ---------------------------------------------------------------------------
# Unit tests – extract_metadata
# ---------------------------------------------------------------------------


class TestExtractMetadata:
    def test_extracts_date_range(self):
        df = pd.DataFrame(
            {
                "timestamp": pd.to_datetime(
                    ["2026-03-01 00:00:00", "2026-03-05 23:59:59"], utc=True
                )
            }
        )
        meta = extract_metadata(df)
        assert "2026-03-01" in meta["date_from"]
        assert "2026-03-05" in meta["date_to"]

    def test_empty_dataframe(self):
        df = pd.DataFrame({"timestamp": pd.Series([], dtype="datetime64[ns, UTC]")})
        meta = extract_metadata(df)
        assert meta["date_from"] == ""
        assert meta["date_to"] == ""


# ---------------------------------------------------------------------------
# Unit tests – build_output_path
# ---------------------------------------------------------------------------


class TestBuildOutputPath:
    def test_appends_normalized_suffix(self):
        result = build_output_path("/data/raw/EURUSD_2024.csv", "/data/processed")
        assert result == "/data/processed/EURUSD_2024_normalized.parquet"


# ---------------------------------------------------------------------------
# Integration tests – NormalizerWorker.process()
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_normalizer_worker_dukascopy_tick(tmp_path, pipeline_settings):
    """Full flow: Dukascopy tick CSV → normalised Parquet with metadata."""
    raw_dir = tmp_path / "raw"
    raw_dir.mkdir(parents=True)
    csv_path = raw_dir / "EURUSD_Ticks_2026.csv"
    csv_path.write_text(
        "Time (Europe/London),Ask,Bid,AskVolume,BidVolume\n"
        "2026.03.01 22:00:01.195,1.17752,1.17751,0.9,0.9\n"
        "2026.03.01 22:00:02.730,1.17751,1.17718,0.9,0.9\n"
    )

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
    assert result.normalizer.rows == 2
    assert result.normalizer.instrument == "EUR/USD"
    assert "2026" in result.normalizer.date_from
    assert "2026" in result.normalizer.date_to
    assert result.normalizer.output_path.endswith(".parquet")

    # Verify the Parquet file was written correctly
    table = pq.read_table(result.normalizer.output_path)
    df = table.to_pandas()
    assert "timestamp" in df.columns
    assert "ask" in df.columns
    assert "bid" in df.columns
    assert "ask_volume" in df.columns
    assert "bid_volume" in df.columns
    assert "mid_price" in df.columns
    assert "spread" in df.columns


@pytest.mark.asyncio
async def test_normalizer_worker_generic_tick(tmp_path, pipeline_settings):
    """Legacy generic tick CSV → normalised Parquet."""
    raw_dir = tmp_path / "raw"
    raw_dir.mkdir(parents=True)
    csv_path = raw_dir / "EURUSD_2024.csv"
    csv_path.write_text(
        "time,bid,ask,volume\n"
        "2024-01-01 00:00:00,1.0999,1.1001,1000000\n"
    )

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
async def test_normalizer_worker_ohlc(tmp_path, pipeline_settings):
    """OHLC CSV → normalised Parquet."""
    raw_dir = tmp_path / "raw"
    raw_dir.mkdir(parents=True)
    csv_path = raw_dir / "EURUSD_OHLC.csv"
    csv_path.write_text(
        "time,open_bid,high_bid_,low_bid,close_bid,volume_bis,"
        "open_ask,high_ask_,low_ask,close_ask,volume_ask\n"
        "2024-01-01 00:00:00,1.1,1.2,1.0,1.15,500,"
        "1.101,1.201,1.001,1.151,500\n"
    )

    pipeline_settings.input_base_path = str(raw_dir)
    pipeline_settings.output_base_path = str(tmp_path / "processed")

    payload = PipelinePayload(
        source_path=str(csv_path),
        instrument="EUR/USD",
        data_type="ohlc",
    )

    worker = NormalizerWorker(pipeline_settings, consumer_name="test")
    result = await worker.process(payload)

    assert result.normalizer is not None
    assert result.normalizer.status == "ok"
    assert result.normalizer.rows == 1

    table = pq.read_table(result.normalizer.output_path)
    df = table.to_pandas()
    assert "timestamp" in df.columns
    assert "high_bid" in df.columns
    assert "volume_bid" in df.columns
    # OHLC should NOT have mid_price/spread
    assert "mid_price" not in df.columns


@pytest.mark.asyncio
async def test_normalizer_worker_handles_bad_file(tmp_path, pipeline_settings):
    """Gracefully handles missing file."""
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


@pytest.mark.asyncio
async def test_normalizer_metadata_date_range(tmp_path, pipeline_settings):
    """Verify date_from and date_to are correctly extracted."""
    raw_dir = tmp_path / "raw"
    raw_dir.mkdir(parents=True)
    csv_path = raw_dir / "GBPUSD_Ticks.csv"
    csv_path.write_text(
        "Time (UTC),Ask,Bid,AskVolume,BidVolume\n"
        "2026.03.01 00:00:00,1.35,1.34,1.0,1.0\n"
        "2026.03.03 12:00:00,1.36,1.35,1.0,1.0\n"
        "2026.03.06 23:59:59,1.37,1.36,1.0,1.0\n"
    )

    pipeline_settings.output_base_path = str(tmp_path / "processed")

    payload = PipelinePayload(
        source_path=str(csv_path),
        instrument="GBP/USD",
        data_type="tick",
    )

    worker = NormalizerWorker(pipeline_settings, consumer_name="test")
    result = await worker.process(payload)

    assert result.normalizer.status == "ok"
    assert result.normalizer.rows == 3
    assert "2026-03-01" in result.normalizer.date_from
    assert "2026-03-06" in result.normalizer.date_to
    assert result.normalizer.instrument == "GBP/USD"
