"""
Step 1 – Work Queue Serializer

Consumes a trigger message from ``pipeline_trigger_queue``, scans the
storage path specified in that message, and publishes one
``PipelinePayload`` per discovered CSV file onto ``normalizer_queue``.

Trigger payload (JSON fields):
    storage_type : "local" | "s3"
    storage_path : absolute path or S3 prefix to scan

Output payload per file:
    PipelinePayload with:
        source_path      – full path to the CSV file
        instrument       – derived from filename (e.g. "EURUSD" → "EUR/USD")
        data_type        – forwarded from trigger (default "tick")
        config_path      – forwarded from trigger
        payload_version  – semantic version string
"""

from __future__ import annotations

import logging
from typing import Any

from pipeline.config import PipelineSettings
from pipeline.payload import PipelinePayload
from pipeline.queue import StreamQueue
from pipeline.storage import StorageBackend
from pipeline.worker import BaseWorker

logger = logging.getLogger(__name__)

PAYLOAD_VERSION = "1.0.0"

TRIGGER_STREAM = "pipeline_trigger_queue"
TRIGGER_GROUP = "pipeline_trigger_group"
NORMALIZER_STREAM = "normalizer_queue"
NORMALIZER_GROUP = "normalizer_group"

# File extensions considered as valid price data inputs
_ACCEPTED_EXTENSIONS = (".csv",)


def _derive_instrument(filename: str) -> str:
    """
    Derive a "BASE/QUOTE" instrument label from a filename stem.

    Examples
    --------
    ``EURUSD_Ticks_2026.csv`` → ``"EUR/USD"``
    ``GBPUSD_2024.csv``       → ``"GBP/USD"``
    ``XAUUSD.csv``            → ``"XAU/USD"``
    ``SHORT.csv``             → ``"SHORT"``   (passthrough when < 6 chars)
    """
    stem = filename.split(".")[0].split("_")[0]
    if len(stem) >= 6:
        return f"{stem[:3]}/{stem[3:6]}"
    return stem


def _list_price_files(storage: StorageBackend, path: str) -> list[str]:
    """Return all accepted price files under *path*, sorted for determinism."""
    try:
        entries = storage.ls(path, detail=False)
    except FileNotFoundError:
        logger.warning("Storage path not found: %s", path)
        return []

    files = sorted(
        e for e in entries
        if any(e.endswith(ext) for ext in _ACCEPTED_EXTENSIONS)
    )
    return files


class SerializerWorker(BaseWorker):
    """
    Step 1 worker.

    Reads one trigger message → lists files → publishes N normalizer messages.
    """

    input_stream: str = TRIGGER_STREAM
    output_streams: list[str] = [NORMALIZER_STREAM]
    consumer_group: str = TRIGGER_GROUP

    async def process(self, payload: dict[str, Any]) -> int:
        """
        Core logic: list files and publish to normalizer_queue.

        Parameters
        ----------
        payload:
            Raw dict from the trigger message.  Expected keys:
            ``storage_path``, optionally ``data_type``, ``config_path``.

        Returns
        -------
        int
            Number of files enqueued.
        """
        storage_path: str = payload.get("storage_path", self.settings.input_base_path)
        data_type: str = payload.get("data_type", "tick")
        config_path: str = payload.get("config_path", self.settings.config_path)

        storage = StorageBackend.from_settings(self.settings)
        files = _list_price_files(storage, storage_path)

        logger.info(
            "[SerializerWorker] Found %d file(s) under '%s'.",
            len(files),
            storage_path,
        )

        normalizer_queue = self._output_queues[0]

        for file_path in files:
            filename = file_path.split("/")[-1]
            instrument = _derive_instrument(filename)

            envelope = PipelinePayload(
                source_path=file_path,
                instrument=instrument,
                data_type=data_type,  # type: ignore[arg-type]
                config_path=config_path,
            )
            await normalizer_queue.publish(envelope.model_dump())
            logger.info("[SerializerWorker] Enqueued: %s → %s", file_path, instrument)

        return len(files)

    # ------------------------------------------------------------------
    # Override _step so the base-class loop calls our process()
    # ------------------------------------------------------------------

    async def _step(self) -> None:  # type: ignore[override]
        assert self._input_queue is not None

        result = await self._input_queue.consume()
        if result is None:
            return

        msg_id, raw_payload = result
        try:
            count = await self.process(raw_payload)
            logger.info("[SerializerWorker] Step complete – %d file(s) enqueued.", count)
        except Exception as exc:  # noqa: BLE001
            logger.exception("[SerializerWorker] Error processing trigger: %s", exc)
        finally:
            await self._input_queue.ack(msg_id)
