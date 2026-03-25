"""
Step 2 – Price Normalizer

Reads raw CSV price files (tick or OHLC), normalises column names using a
provider-specific column mapper, extracts metadata (date range, row count),
and writes a Parquet file.

SOLID principles applied:
  - SRP: Column mapping, metadata extraction, and I/O are separate concerns.
  - OCP: New providers are added by registering a ColumnMapper — existing
    code is not modified.
  - LSP: All mappers satisfy the ColumnMapper protocol and are interchangeable.
  - ISP: ColumnMapper is a minimal protocol (two members).
  - DIP: NormalizerWorker depends on the ColumnMapper protocol, not concrete
    implementations.

Input queue : normalizer_queue
Output queue: quality_checker_queue
"""

from __future__ import annotations

import logging
import re
from pathlib import PurePosixPath
from typing import Protocol, runtime_checkable

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from pipeline.payload import NormalizerResult, PipelinePayload
from pipeline.worker import BaseWorker

logger = logging.getLogger(__name__)


# ── Column Mapper protocol (DIP + ISP) ──────────────────────────────


@runtime_checkable
class ColumnMapper(Protocol):
    """Maps raw provider columns to canonical names."""

    canonical_columns: list[str]

    def rename(self, df: pd.DataFrame) -> pd.DataFrame:
        """Return *df* with columns renamed to the canonical schema."""
        ...


# ── Concrete mappers (OCP — add a new class + register it) ──────────


class DukascopyTickMapper:
    """
    Dukascopy tick format:
        Time (<tz>), Ask, Bid, AskVolume, BidVolume
    → timestamp, ask, bid, ask_volume, bid_volume
    """

    canonical_columns = ["timestamp", "ask", "bid", "ask_volume", "bid_volume"]

    _RENAME: dict[str, str] = {
        "ask": "ask",
        "bid": "bid",
        "askvolume": "ask_volume",
        "bidvolume": "bid_volume",
    }

    def rename(self, df: pd.DataFrame) -> pd.DataFrame:
        df = _strip_columns(df)
        mapping = _match_columns(df.columns, self._RENAME, time_col="timestamp")
        return df.rename(columns=mapping)


class GenericTickMapper:
    """
    Generic / legacy tick CSV format:
        time, bid, ask, volume
    """

    canonical_columns = ["timestamp", "ask", "bid", "volume"]

    _RENAME: dict[str, str] = {
        "time": "timestamp",
        "ask": "ask",
        "bid": "bid",
        "volume": "volume",
    }

    def rename(self, df: pd.DataFrame) -> pd.DataFrame:
        df = _strip_columns(df)
        mapping = _match_columns(df.columns, self._RENAME, time_col="timestamp")
        return df.rename(columns=mapping)


class GenericOhlcMapper:
    """
    Generic OHLC CSV format with bid/ask split:
        time, open_bid, high_bid_, low_bid, close_bid, volume_bis,
              open_ask, high_ask_, low_ask, close_ask, volume_ask
    """

    canonical_columns = [
        "timestamp",
        "open_bid", "high_bid", "low_bid", "close_bid", "volume_bid",
        "open_ask", "high_ask", "low_ask", "close_ask", "volume_ask",
    ]

    _RENAME: dict[str, str] = {
        "time": "timestamp",
        "open_bid": "open_bid",
        "high_bid_": "high_bid",
        "low_bid": "low_bid",
        "close_bid": "close_bid",
        "volume_bis": "volume_bid",
        "open_ask": "open_ask",
        "high_ask_": "high_ask",
        "low_ask": "low_ask",
        "close_ask": "close_ask",
        "volume_ask": "volume_ask",
    }

    def rename(self, df: pd.DataFrame) -> pd.DataFrame:
        df = _strip_columns(df)
        mapping = _match_columns(df.columns, self._RENAME, time_col="timestamp")
        return df.rename(columns=mapping)


# ── Mapper registry (OCP — extend without touching worker) ──────────

_TICK_MAPPERS: list[ColumnMapper] = [DukascopyTickMapper(), GenericTickMapper()]
_OHLC_MAPPERS: list[ColumnMapper] = [GenericOhlcMapper()]


def detect_mapper(df: pd.DataFrame, data_type: str) -> ColumnMapper:
    """
    Pick the best ColumnMapper for the given DataFrame by checking which
    mapper's expected raw columns match the most actual columns.
    """
    candidates = _TICK_MAPPERS if data_type == "tick" else _OHLC_MAPPERS
    if len(candidates) == 1:
        return candidates[0]

    lowered = {c.lower().strip() for c in df.columns}

    def _score(mapper: ColumnMapper) -> int:
        rename_map: dict[str, str] = mapper._RENAME  # type: ignore[attr-defined]
        return sum(1 for k in rename_map if k in lowered)

    return max(candidates, key=_score)


# ── Metadata extraction (SRP) ───────────────────────────────────────


def extract_metadata(df: pd.DataFrame) -> dict[str, str]:
    """Extract date_from, date_to from a normalised DataFrame."""
    ts = df["timestamp"].dropna()
    if ts.empty:
        return {"date_from": "", "date_to": ""}
    return {
        "date_from": str(ts.min()),
        "date_to": str(ts.max()),
    }


# ── Parquet writer (SRP) ────────────────────────────────────────────


def write_parquet(df: pd.DataFrame, path: str, storage) -> None:
    """Write *df* as a Parquet file through the storage backend."""
    table = pa.Table.from_pandas(df)
    storage.makedirs(str(PurePosixPath(path).parent))
    with storage.open(path, "wb") as f:
        pq.write_table(table, f)


# ── Output path builder (SRP) ───────────────────────────────────────


def build_output_path(source: str, base: str) -> str:
    """Derive the normalised Parquet output path from the source CSV path."""
    p = PurePosixPath(source)
    return str(PurePosixPath(base) / (p.stem + "_normalized.parquet"))


# ── Worker (orchestrates, delegates to collaborators) ────────────────


class NormalizerWorker(BaseWorker):
    """
    Step 2 worker.

    Reads raw CSV → detects provider → normalises columns → extracts
    metadata → writes Parquet → enriches payload → forwards to Step 3.
    """

    input_stream = "normalizer_queue"
    output_streams = ["quality_checker_queue"]
    consumer_group = "normalizer_group"

    async def process(self, payload: PipelinePayload) -> PipelinePayload:
        logger.info("[Normalizer] Processing %s", payload.source_path)

        try:
            # 1. Read raw CSV
            with self.storage.open(payload.source_path) as f:
                df = pd.read_csv(f)

            # 2. Detect provider and normalise columns
            mapper = detect_mapper(df, payload.data_type)
            df = mapper.rename(df)

            # 3. Parse timestamps
            df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)

            # 4. Add derived columns for tick data
            if payload.data_type == "tick" and "bid" in df.columns and "ask" in df.columns:
                df["mid_price"] = (df["bid"] + df["ask"]) / 2
                df["spread"] = df["ask"] - df["bid"]

            # 5. Extract metadata
            meta = extract_metadata(df)

            # 6. Write Parquet
            output_path = build_output_path(
                payload.source_path,
                self.settings.output_base_path,
            )
            write_parquet(df, output_path, self.storage)

            # 7. Enrich payload
            payload.normalizer = NormalizerResult(
                status="ok",
                output_path=output_path,
                rows=len(df),
                instrument=payload.instrument,
                date_from=meta["date_from"],
                date_to=meta["date_to"],
            )
            logger.info(
                "[Normalizer] Done → %s (%d rows, %s to %s)",
                output_path, len(df), meta["date_from"], meta["date_to"],
            )

        except Exception as exc:
            logger.exception("[Normalizer] Error processing %s", payload.source_path)
            payload.normalizer = NormalizerResult(status="error", error=str(exc))

        return payload


# ── Private helpers ──────────────────────────────────────────────────


def _strip_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Lowercase and strip whitespace from column names; drop timezone
    annotations like ``Time (Europe/London)`` → ``time``."""
    cleaned = {}
    for col in df.columns:
        new = re.sub(r"\s*\(.*?\)\s*", "", col).strip().lower()
        cleaned[col] = new
    return df.rename(columns=cleaned)


def _match_columns(
    columns: pd.Index,
    rename_map: dict[str, str],
    time_col: str,
) -> dict[str, str]:
    """Build the final rename dict, mapping existing lowered names to
    canonical names.  Any column starting with 'time' is mapped to
    *time_col*."""
    mapping: dict[str, str] = {}
    for col in columns:
        low = col.lower().strip()
        if low.startswith("time"):
            mapping[col] = time_col
        elif low in rename_map:
            mapping[col] = rename_map[low]
    return mapping
