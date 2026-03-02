"""
Step 2 – Price Normalizer

Reads raw CSV price files (tick or OHLC), normalises column names,
merges bid/ask into a single DataFrame, and writes a Parquet file.

Input queue : normalizer_queue
Output queue: quality_checker_queue
"""

from __future__ import annotations

import logging
from pathlib import PurePosixPath

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from pipeline.payload import NormalizerResult, PipelinePayload
from pipeline.worker import BaseWorker

logger = logging.getLogger(__name__)

# Column name mappings ─ raw → canonical
TICK_RENAME: dict[str, str] = {
    "time": "timestamp",
    "ask": "ask",
    "bid": "bid",
    "volume": "volume",
}

OHLC_RENAME: dict[str, str] = {
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


class NormalizerWorker(BaseWorker):
    input_stream = "normalizer_queue"
    output_streams = ["quality_checker_queue"]
    consumer_group = "normalizer_group"

    async def process(self, payload: PipelinePayload) -> PipelinePayload:
        logger.info("[Normalizer] Processing %s", payload.source_path)

        try:
            with self.storage.open(payload.source_path) as f:
                df = pd.read_csv(f)

            if payload.data_type == "tick":
                df = _normalize_tick(df)
            else:
                df = _normalize_ohlc(df)

            output_path = _build_output_path(
                payload.source_path,
                self.settings.output_base_path,
                suffix="_normalized",
            )
            _write_parquet(df, output_path, self.storage)

            payload.normalizer = NormalizerResult(
                status="ok",
                output_path=output_path,
                rows=len(df),
            )
            logger.info("[Normalizer] Done → %s (%d rows)", output_path, len(df))

        except Exception as exc:
            logger.exception("[Normalizer] Error processing %s", payload.source_path)
            payload.normalizer = NormalizerResult(status="error", error=str(exc))

        return payload


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _normalize_tick(df: pd.DataFrame) -> pd.DataFrame:
    df = df.rename(columns={k: v for k, v in TICK_RENAME.items() if k in df.columns})
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
    df["mid_price"] = (df["bid"] + df["ask"]) / 2
    df["spread"] = df["ask"] - df["bid"]
    return df


def _normalize_ohlc(df: pd.DataFrame) -> pd.DataFrame:
    df = df.rename(columns={k: v for k, v in OHLC_RENAME.items() if k in df.columns})
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
    return df


def _build_output_path(source: str, base: str, suffix: str) -> str:
    p = PurePosixPath(source)
    return str(PurePosixPath(base) / (p.stem + suffix + ".parquet"))


def _write_parquet(df: pd.DataFrame, path: str, storage) -> None:
    table = pa.Table.from_pandas(df)
    storage.makedirs(str(PurePosixPath(path).parent))
    with storage.open(path, "wb") as f:
        pq.write_table(table, f)
