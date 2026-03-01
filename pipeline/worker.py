"""
Base class for all pipeline step workers.

Subclasses only need to implement `process()`.
The base class handles the consume → process → publish → ack loop,
plus error handling and logging.
"""

from __future__ import annotations

import asyncio
import logging
from abc import ABC, abstractmethod
from typing import Any

import redis.asyncio as aioredis

from pipeline.config import PipelineSettings
from pipeline.payload import PipelinePayload
from pipeline.queue import StreamQueue
from pipeline.storage import StorageBackend

logger = logging.getLogger(__name__)


class BaseWorker(ABC):
    """
    Template for a pipeline step worker.

    Each worker:
      1. Reads one message from `input_queue`
      2. Calls `process()` which returns an enriched payload
      3. Publishes the result to `output_queue` (if set)
      4. Acks the input message
    """

    #: Override in subclasses
    input_stream: str = ""
    output_stream: str = ""
    consumer_group: str = ""

    def __init__(
        self,
        settings: PipelineSettings,
        consumer_name: str,
    ) -> None:
        self.settings = settings
        self.consumer_name = consumer_name
        self._redis: aioredis.Redis | None = None
        self._input_queue: StreamQueue | None = None
        self._output_queue: StreamQueue | None = None
        self.storage = StorageBackend.from_settings(settings)

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def start(self) -> None:
        self._redis = aioredis.from_url(self.settings.redis_url, decode_responses=False)

        self._input_queue = StreamQueue(
            self._redis,
            stream=self.input_stream,
            group=self.consumer_group,
            consumer=self.consumer_name,
        )
        await self._input_queue.ensure_group()

        if self.output_stream:
            self._output_queue = StreamQueue(
                self._redis,
                stream=self.output_stream,
                group=f"{self.output_stream}_group",
                consumer=self.consumer_name,
            )
            await self._output_queue.ensure_group()

        logger.info(
            "[%s] Worker '%s' started. Listening on '%s'",
            self.__class__.__name__,
            self.consumer_name,
            self.input_stream,
        )

    async def stop(self) -> None:
        if self._redis:
            await self._redis.aclose()
        logger.info("[%s] Worker '%s' stopped.", self.__class__.__name__, self.consumer_name)

    # ------------------------------------------------------------------
    # Main loop
    # ------------------------------------------------------------------

    async def run(self) -> None:
        await self.start()
        try:
            while True:
                await self._step()
        except asyncio.CancelledError:
            logger.info("[%s] Cancelled.", self.__class__.__name__)
        finally:
            await self.stop()

    async def _step(self) -> None:
        result = await self._input_queue.consume()
        if result is None:
            return  # timeout, loop again

        msg_id, raw = result
        try:
            payload = PipelinePayload(**raw)
            enriched = await self.process(payload)
            if self._output_queue and enriched:
                await self._output_queue.publish(enriched.model_dump())
            await self._input_queue.ack(msg_id)
        except Exception:
            logger.exception(
                "[%s] Failed to process message %s", self.__class__.__name__, msg_id
            )
            # Message stays in PEL for manual inspection / retry

    # ------------------------------------------------------------------
    # Override in subclasses
    # ------------------------------------------------------------------

    @abstractmethod
    async def process(self, payload: PipelinePayload) -> PipelinePayload | None:
        """
        Process the payload and return the enriched version.
        Return None to drop the message (no forwarding).
        """
        ...
