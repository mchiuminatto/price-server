"""
Step 5 – File Mover

Promotes the staging Parquet file produced by the Patcher to its final
location in the target storage (local or S3).

Input queue : file_mover_queue
Output queue: done_queue  (audit trail — optional consumer)
"""

from __future__ import annotations

import logging
from pathlib import PurePosixPath

from pipeline.payload import FileMoverResult, PipelinePayload
from pipeline.worker import BaseWorker

logger = logging.getLogger(__name__)


class FileMoverWorker(BaseWorker):
    input_stream = "file_mover_queue"
    output_stream = "done_queue"
    consumer_group = "file_mover_group"

    async def process(self, payload: PipelinePayload) -> PipelinePayload:
        if payload.patcher is None or payload.patcher.status == "error":
            logger.warning("[FileMover] Skipping %s — upstream failed.", payload.file_id)
            payload.file_mover = FileMoverResult(status="error", error="upstream failed")
            return payload

        staging_path = payload.patcher.output_path
        target_path = staging_path.removesuffix(".staging")

        logger.info("[FileMover] Moving %s → %s", staging_path, target_path)

        try:
            self.storage.move(staging_path, target_path)
            payload.file_mover = FileMoverResult(status="ok", target_path=target_path)
            logger.info("[FileMover] Done → %s", target_path)

        except Exception as exc:
            logger.exception("[FileMover] Error moving %s", staging_path)
            payload.file_mover = FileMoverResult(status="error", error=str(exc))

        return payload
