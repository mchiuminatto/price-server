"""
Step 4 – Price Patcher

Appends / inserts new price data into an existing Parquet dataset,
or creates a new one if none exists.

Input queue : updater_queue
Output queue: file_mover_queue
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
    output_stream = "file_mover_queue"
    consumer_group = "patcher_group"

    async def process(self, payload: PipelinePayload) -> PipelinePayload:
        if payload.normalizer is None or payload.normalizer.status == "error":
            logger.warning("[Patcher] Skipping %s — upstream failed.", payload.file_id)
            payload.patcher = PatcherResult(status="error", error="upstream failed")
            return payload

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

            action: Literal["create", "append", "insert"]
            if self.storage.exists(target_path):
                with self.storage.open(target_path) as f:
                    existing_df = pq.read_table(f).to_pandas()
                merged_df, action = _merge(existing_df, new_df)
            else:
                merged_df = new_df
                action = "create"

            staging_path = target_path + ".staging"
            _write_parquet(merged_df, staging_path, self.storage)

            payload.patcher = PatcherResult(
                status="ok",
                action=action,
                output_path=staging_path,
            )
            logger.info("[Patcher] Done (%s) → %s", action, staging_path)

        except Exception as exc:
            logger.exception("[Patcher] Error on %s", input_path)
            payload.patcher = PatcherResult(status="error", error=str(exc))

        return payload


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _merge(
    existing: pd.DataFrame, new: pd.DataFrame
) -> tuple[pd.DataFrame, Literal["append", "insert"]]:
    """
    Combine existing and new data, deduplicate, and sort by timestamp.
    Determines whether the new data is a pure append or an insert/backfill.
    """
    combined = pd.concat([existing, new], ignore_index=True)
    combined = combined.drop_duplicates(subset=["timestamp"]).sort_values("timestamp")

    max_existing = existing["timestamp"].max()
    min_new = new["timestamp"].min()
    action: Literal["append", "insert"] = "append" if min_new >= max_existing else "insert"
    return combined.reset_index(drop=True), action


def _build_target_path(instrument: str, data_type: str, base: str) -> str:
    symbol = instrument.replace("/", "")
    return str(PurePosixPath(base) / data_type / f"{symbol}.parquet")


def _write_parquet(df: pd.DataFrame, path: str, storage) -> None:
    storage.makedirs(str(PurePosixPath(path).parent))
    with storage.open(path, "wb") as f:
        pq.write_table(pa.Table.from_pandas(df), f)
