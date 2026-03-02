"""
Shared base for all Step 6 abstraction builder workers.

Subclasses implement `build()` which receives the normalised tick DataFrame
and the abstraction spec dict, and returns the output DataFrame.
"""

from __future__ import annotations

import logging
from abc import abstractmethod
from pathlib import PurePosixPath

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from pipeline.payload import AbstractionResult, PipelinePayload
from pipeline.worker import BaseWorker

logger = logging.getLogger(__name__)


class AbstractionBuilderWorker(BaseWorker):
    """
    Base for OHLC / tick-bar / pip-bar / renko workers.

    Subclasses set:
        input_stream  = "<type>_queue"
        output_streams = ["price_abstraction_queue"]
        consumer_group = "<type>_group"
    and implement build().
    """

    output_streams = ["price_abstraction_queue"]

    async def process(self, payload: PipelinePayload) -> PipelinePayload:
        if payload.patcher is None or payload.patcher.status == "error":
            logger.warning(
                "[%s] Skipping %s — upstream failed.", self.__class__.__name__, payload.file_id
            )
            return payload

        spec: dict = payload.abstraction_spec
        label: str = payload.abstraction_label or self.__class__.__name__

        source_path = payload.patcher.output_path
        logger.info("[%s] Building '%s' from %s", self.__class__.__name__, label, source_path)

        try:
            with self.storage.open(source_path) as f:
                tick_df = pq.read_table(f).to_pandas()

            result_df = self.build(tick_df, spec)

            output_path = _build_output_path(
                base=self.settings.output_base_path,
                instrument=payload.instrument,
                label=label,
            )
            _write_parquet(result_df, output_path, self.storage)

            payload.abstractions[label] = AbstractionResult(
                status="ok",
                abstraction_type=spec.get("type", ""),
                output_path=output_path,
                rows=len(result_df),
            )
            logger.info(
                "[%s] Done '%s' → %s (%d rows)",
                self.__class__.__name__,
                label,
                output_path,
                len(result_df),
            )

        except Exception as exc:
            logger.exception("[%s] Error building '%s'", self.__class__.__name__, label)
            payload.abstractions[label] = AbstractionResult(
                status="error",
                abstraction_type=spec.get("type", ""),
                error=str(exc),
            )

        return payload

    @abstractmethod
    def build(self, tick_df: pd.DataFrame, spec: dict) -> pd.DataFrame:
        """Transform tick data into the abstraction DataFrame."""
        ...


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _build_output_path(base: str, instrument: str, label: str) -> str:
    symbol = instrument.replace("/", "")
    return str(PurePosixPath(base) / "abstractions" / symbol / f"{label}.parquet")


def _write_parquet(df: pd.DataFrame, path: str, storage) -> None:
    storage.makedirs(str(PurePosixPath(path).parent))
    with storage.open(path, "wb") as f:
        pq.write_table(pa.Table.from_pandas(df), f)
