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
      1. Reads one message from `input_stream`
      2. Calls `process()` which returns an enriched payload
      3. Publishes the result to each stream in `output_streams`
      4. Acks the input message

    For single-output steps set `output_streams = ["my_queue"]`.
    For fan-out steps (e.g. Step 5) set multiple streams.
    """

    input_stream: str = ""
    output_streams: list[str] = []  # replaces the old single output_stream
    consumer_group: str = ""

    def __init__(self, settings: PipelineSettings, consumer_name: str) -> None:
        self.settings = settings
        self.consumer_name = consumer_name
        self._redis: aioredis.Redis | None = None
        self._input_queue: StreamQueue | None = None
        self._output_queues: list[StreamQueue] = []
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

        for stream in self.output_streams:
            q = StreamQueue(
                self._redis,
                stream=stream,
                group=f"{stream}_group",
                consumer=self.consumer_name,
            )
            await q.ensure_group()
            self._output_queues.append(q)

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
            return

        msg_id, raw = result
        try:
            payload = PipelinePayload(**raw)
            enriched = await self.process(payload)
            if enriched:
                for q in self._output_queues:
                    await q.publish(enriched.model_dump())
            await self._input_queue.ack(msg_id)
        except Exception:
            logger.exception("[%s] Failed to process message %s", self.__class__.__name__, msg_id)

    # ------------------------------------------------------------------
    # Override in subclasses
    # ------------------------------------------------------------------

    @abstractmethod
    async def process(self, payload: PipelinePayload) -> PipelinePayload | None:
        """
        Process the payload and return the enriched version.
        Return None to drop the message without forwarding.
        """
        ...
