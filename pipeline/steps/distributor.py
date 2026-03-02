"""
Step 5 – Abstraction Distributor

Reads the list of abstractions to build from the YAML config referenced in
the payload, then fans-out one event per abstraction onto the corresponding
queue: ohlc_queue, tick_bar_queue, pip_bar_queue, renko_bar_queue.

Input queue : abstraction_distributor_queue
Output queues: ohlc_queue, tick_bar_queue, pip_bar_queue, renko_bar_queue
"""

from __future__ import annotations

import copy
import logging

import yaml

from pipeline.payload import DistributorResult, PipelinePayload
from pipeline.worker import BaseWorker

logger = logging.getLogger(__name__)

# Map abstraction type → output stream name
ABSTRACTION_QUEUE_MAP: dict[str, str] = {
    "ohlc": "ohlc_queue",
    "tick_bar": "tick_bar_queue",
    "pip_bar": "pip_bar_queue",
    "renko": "renko_bar_queue",
}


class DistributorWorker(BaseWorker):
    input_stream = "abstraction_distributor_queue"
    # Fan-out to all abstraction queues
    output_streams = list(ABSTRACTION_QUEUE_MAP.values())
    consumer_group = "distributor_group"

    async def process(self, payload: PipelinePayload) -> None:
        """
        Fan-out: publish one message per abstraction directly to the
        appropriate queue — does NOT return a single enriched payload.
        """
        if payload.patcher is None or payload.patcher.status == "error":
            logger.warning("[Distributor] Skipping %s — upstream failed.", payload.file_id)
            return None

        config = _load_config(payload.config_path, self.storage)
        abstractions = config.get("abstractions", [])

        if not abstractions:
            logger.warning("[Distributor] No abstractions defined in config.")
            payload.distributor = DistributorResult(status="ok", abstractions_dispatched=[])
            return None

        dispatched: list[str] = []

        for spec in abstractions:
            abs_type = spec.get("type")
            queue_name = ABSTRACTION_QUEUE_MAP.get(abs_type)
            if not queue_name:
                logger.warning("[Distributor] Unknown abstraction type '%s', skipping.", abs_type)
                continue

            # Build a child payload for this abstraction
            label = _make_label(spec)
            child = copy.deepcopy(payload)
            child.abstraction_spec = spec
            child.abstraction_label = label
            child.distributor = DistributorResult(status="ok", abstractions_dispatched=[label])

            # Publish directly to the target queue
            target_queue = next((q for q in self._output_queues if q.stream == queue_name), None)
            if target_queue:
                await target_queue.publish(child.model_dump())
                dispatched.append(label)
                logger.info("[Distributor] Dispatched '%s' → %s", label, queue_name)

        # Update and ack — the base _step() won't re-publish since we return None
        payload.distributor = DistributorResult(status="ok", abstractions_dispatched=dispatched)
        logger.info("[Distributor] Dispatched %d abstraction(s).", len(dispatched))
        return None  # prevent base class from re-publishing to all output queues


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _load_config(config_path: str, storage) -> dict:
    """Load and parse the YAML pipeline config file."""
    try:
        with storage.open(config_path) as f:
            return yaml.safe_load(f) or {}
    except Exception:
        logger.exception("[Distributor] Could not load config from '%s'", config_path)
        return {}


def _make_label(spec: dict) -> str:
    """Build a human-readable label for an abstraction spec."""
    abs_type = spec.get("type", "unknown")
    if abs_type == "ohlc":
        return f"ohlc_{spec.get('timeframe', '?')}"
    if abs_type == "tick_bar":
        return f"tick_bar_{spec.get('tick_count', '?')}"
    if abs_type == "pip_bar":
        return f"pip_bar_{spec.get('pip_range', '?')}"
    if abs_type == "renko":
        return f"renko_{spec.get('body_pips', '?')}"
    return abs_type
