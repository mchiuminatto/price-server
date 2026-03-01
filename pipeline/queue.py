"""
Redis Streams queue abstraction.

Each pipeline step reads from an input stream and writes to an output stream.
Consumer groups ensure at-least-once delivery and allow multiple workers per step.
"""

import json
import logging
from typing import Any

import redis.asyncio as aioredis

logger = logging.getLogger(__name__)


class StreamQueue:
    """Thin async wrapper around a single Redis Stream with a consumer group."""

    def __init__(
        self,
        client: aioredis.Redis,
        stream: str,
        group: str,
        consumer: str,
        block_ms: int = 2000,
    ) -> None:
        self._r = client
        self.stream = stream
        self.group = group
        self.consumer = consumer
        self.block_ms = block_ms

    async def ensure_group(self) -> None:
        """Create the stream and consumer group if they don't exist yet."""
        try:
            await self._r.xgroup_create(self.stream, self.group, id="0", mkstream=True)
            logger.info("Created consumer group '%s' on stream '%s'", self.group, self.stream)
        except aioredis.ResponseError as exc:
            if "BUSYGROUP" not in str(exc):
                raise

    async def publish(self, payload: dict[str, Any]) -> str:
        """Append a JSON-serialised payload to the stream. Returns the message id."""
        msg_id = await self._r.xadd(self.stream, {"data": json.dumps(payload)})
        logger.debug("Published to '%s': %s", self.stream, msg_id)
        return msg_id

    async def consume(self) -> tuple[str, dict[str, Any]] | None:
        """
        Block until a new message arrives, then return (msg_id, payload).
        Returns None on timeout.
        """
        results = await self._r.xreadgroup(
            groupname=self.group,
            consumername=self.consumer,
            streams={self.stream: ">"},
            count=1,
            block=self.block_ms,
        )
        if not results:
            return None
        _, messages = results[0]
        msg_id, fields = messages[0]
        return msg_id, json.loads(fields[b"data"])

    async def ack(self, msg_id: str) -> None:
        """Acknowledge a processed message so it leaves the pending-entry list."""
        await self._r.xack(self.stream, self.group, msg_id)
        logger.debug("Acked '%s' on '%s'", msg_id, self.stream)
