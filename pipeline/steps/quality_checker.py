"""
Step 3 – Quality Checker

Detects gaps and anomalies in the normalised price series.

Input queue : quality_checker_queue
Output queue: updater_queue
"""

from __future__ import annotations

import logging
from pathlib import PurePosixPath

import pandas as pd
import pyarrow.parquet as pq

from pipeline.payload import PipelinePayload, QualityResult
from pipeline.worker import BaseWorker

logger = logging.getLogger(__name__)

# Maximum allowed gap between consecutive ticks (seconds)
MAX_TICK_GAP_SECONDS = 60
# Maximum allowed gap between consecutive OHLC bars (minutes, assuming 1-min bars)
MAX_OHLC_GAP_BARS = 5


class QualityCheckerWorker(BaseWorker):
    input_stream = "quality_checker_queue"
    output_stream = "updater_queue"
    consumer_group = "quality_checker_group"

    async def process(self, payload: PipelinePayload) -> PipelinePayload:
        if payload.normalizer is None or payload.normalizer.status == "error":
            logger.warning("[QualityChecker] Skipping %s — normalizer failed.", payload.file_id)
            payload.quality = QualityResult(
                status="error", error="upstream normalizer failed"
            )
            return payload

        input_path = payload.normalizer.output_path
        logger.info("[QualityChecker] Checking %s", input_path)

        try:
            with self.storage.open(input_path) as f:
                df = pq.read_table(f).to_pandas()

            gaps = _detect_gaps(df, payload.data_type)

            report_path = _build_report_path(input_path, self.settings.output_base_path)
            _write_report(gaps, report_path, self.storage)

            payload.quality = QualityResult(
                status="ok",
                gaps_found=len(gaps),
                report_path=report_path,
            )
            logger.info(
                "[QualityChecker] Done — %d gap(s) found. Report → %s",
                len(gaps),
                report_path,
            )

        except Exception as exc:
            logger.exception("[QualityChecker] Error on %s", input_path)
            payload.quality = QualityResult(status="error", error=str(exc))

        return payload


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _detect_gaps(df: pd.DataFrame, data_type: str) -> pd.DataFrame:
    """Return a DataFrame of detected gap intervals."""
    df = df.sort_values("timestamp")
    deltas = df["timestamp"].diff().dt.total_seconds().dropna()

    threshold = (
        MAX_TICK_GAP_SECONDS if data_type == "tick" else MAX_OHLC_GAP_BARS * 60
    )
    gap_mask = deltas > threshold
    gap_df = df.loc[gap_mask, ["timestamp"]].copy()
    gap_df["gap_seconds"] = deltas[gap_mask]
    return gap_df.reset_index(drop=True)


def _build_report_path(input_path: str, base: str) -> str:
    p = PurePosixPath(input_path)
    return str(PurePosixPath(base) / (p.stem + "_quality_report.parquet"))


def _write_report(gaps: pd.DataFrame, path: str, storage) -> None:
    import pyarrow as pa
    import pyarrow.parquet as pq

    storage.makedirs(str(PurePosixPath(path).parent))
    with storage.open(path, "wb") as f:
        pq.write_table(pa.Table.from_pandas(gaps), f)
