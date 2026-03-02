"""
Step 4 – Price Patcher

Appends new price data into an existing Parquet dataset (append-only),
or creates a new one if none exists yet.

Input queue : updater_queue
Output queue: abstraction_distributor_queue
"""

from __future__ import annotations

import logging
from pathlib import PurePosixPath
from typing import Literal

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from pipeline.payload import PatcherResult, PipelinePayload
from pipeline.worker import BaseWorker

logger = logging.getLogger(__name__)


class PatcherWorker(BaseWorker):
    input_stream = "updater_queue"
    output_streams = ["abstraction_distributor_queue"]
    consumer_group = "patcher_group"

    async def process(self, payload: PipelinePayload) -> PipelinePayload:
        if payload.normalizer is None or payload.normalizer.status == "error":
            logger.warning("[Patcher] Skipping %s — upstream failed.", payload.file_id)
            payload.patcher = PatcherResult(status="error", error="upstream failed")
            return payload

        # Propagate config_path from settings if not already in payload
        if not payload.config_path:
            payload.config_path = self.settings.config_path

        input_path = payload.normalizer.output_path
        logger.info("[Patcher] Patching %s", input_path)

        try:
            with self.storage.open(input_path) as f:
                new_df = pq.read_table(f).to_pandas()

            target_path = _build_target_path(
                payload.instrument,
                payload.data_type,
                self.settings.output_base_path,
            )

            action: Literal["create", "append"]
            if self.storage.exists(target_path):
                with self.storage.open(target_path) as f:
                    existing_df = pq.read_table(f).to_pandas()
                merged_df = _append(existing_df, new_df)
                action = "append"
            else:
                merged_df = new_df
                action = "create"

            _write_parquet(merged_df, target_path, self.storage)

            payload.patcher = PatcherResult(
                status="ok",
                action=action,
                output_path=target_path,
            )
            logger.info("[Patcher] Done (%s) → %s", action, target_path)

        except Exception as exc:
            logger.exception("[Patcher] Error on %s", input_path)
            payload.patcher = PatcherResult(status="error", error=str(exc))

        return payload


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _append(existing: pd.DataFrame, new: pd.DataFrame) -> pd.DataFrame:
    """Append-only: concatenate and deduplicate by timestamp."""
    combined = pd.concat([existing, new], ignore_index=True)
    return (
        combined.drop_duplicates(subset=["timestamp"])
        .sort_values("timestamp")
        .reset_index(drop=True)
    )


def _build_target_path(instrument: str, data_type: str, base: str) -> str:
    symbol = instrument.replace("/", "")
    return str(PurePosixPath(base) / data_type / f"{symbol}.parquet")


def _write_parquet(df: pd.DataFrame, path: str, storage) -> None:
    storage.makedirs(str(PurePosixPath(path).parent))
    with storage.open(path, "wb") as f:
        pq.write_table(pa.Table.from_pandas(df), f)
