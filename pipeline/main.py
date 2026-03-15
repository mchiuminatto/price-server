"""
Pipeline entrypoint.

Launches all step workers as concurrent asyncio tasks.

Usage:
    python -m pipeline.main                        # run all workers
    python -m pipeline.main --serialize-only       # only seed the queue
"""

from __future__ import annotations

import argparse
import asyncio
import logging

from pipeline.config import settings
from pipeline.steps.abstractions.ohlc import OHLCBuilderWorker
from pipeline.steps.abstractions.pip_bar import PipBarBuilderWorker
from pipeline.steps.abstractions.renko import RenkoBarBuilderWorker
from pipeline.steps.abstractions.tick_bar import TickBarBuilderWorker
from pipeline.steps.distributor import DistributorWorker
from pipeline.steps.normalizer import NormalizerWorker
from pipeline.steps.patcher import PatcherWorker
from pipeline.steps.quality_checker import QualityCheckerWorker

from pipeline.steps.serializer import SerializerWorker


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger(__name__)


def _make_workers() -> list:
    n = settings.worker_count
    return (
        # Step 1 – Serializer (single worker — reads trigger, fans out to normalizer)
        [SerializerWorker(settings, consumer_name="serializer_0")]
        # Step 2 – Normalizer
        + [NormalizerWorker(settings, consumer_name=f"normalizer_{i}") for i in range(n)]
        # Step 3 – Quality Checker
        + [QualityCheckerWorker(settings, consumer_name=f"quality_{i}") for i in range(n)]
        # Step 4 – Patcher (append-only)
        + [PatcherWorker(settings, consumer_name=f"patcher_{i}") for i in range(n)]
        # Step 5 – Abstraction Distributor (single worker — pure fan-out)
        + [DistributorWorker(settings, consumer_name="distributor_0")]
        # Step 6 – Abstraction Builders (one worker type per abstraction)
        + [OHLCBuilderWorker(settings, consumer_name=f"ohlc_{i}") for i in range(n)]
        + [TickBarBuilderWorker(settings, consumer_name=f"tick_bar_{i}") for i in range(n)]
        + [PipBarBuilderWorker(settings, consumer_name=f"pip_bar_{i}") for i in range(n)]
        + [RenkoBarBuilderWorker(settings, consumer_name=f"renko_{i}") for i in range(n)]
        # NOTE: FileMoverWorker (file_mover_queue → done_queue) is an optional
        # housekeeping step not in the main data path. Import and add it here
        # if you need staging-file promotion after abstraction building.
    )


async def run_pipeline() -> None:
    workers = _make_workers()
    tasks = [asyncio.create_task(w.run()) for w in workers]
    logger.info(
        "Pipeline started: %d worker types, %d workers per step.",
        8,
        settings.worker_count,
    )
    try:
        await asyncio.gather(*tasks)
    except asyncio.CancelledError:
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)


async def run_serialize_only() -> None:
    """
    Run *only* the Step 1 worker.

    The worker listens on ``pipeline_trigger_queue`` and processes
    trigger messages exactly like it would in the full pipeline.
    It keeps running until cancelled (Ctrl-C).
    """
    worker = SerializerWorker(settings, consumer_name="serializer_0")
    logger.info(
        "Starting serializer worker – waiting for trigger messages on '%s' …",
        worker.input_stream,
    )
    await worker.run()



def main() -> None:
    parser = argparse.ArgumentParser(description="Price pipeline")
    parser.add_argument(
        "--serialize-only",
        action="store_true",
        help="Only run the work queue serializer (Step 1), listening on pipeline_trigger_queue.",
    )
    args = parser.parse_args()

    if args.serialize_only:
        asyncio.run(run_serialize_only())
    else:
        asyncio.run(run_pipeline())


if __name__ == "__main__":
    main()
