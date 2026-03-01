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
from pipeline.steps.file_mover import FileMoverWorker
from pipeline.steps.normalizer import NormalizerWorker
from pipeline.steps.patcher import PatcherWorker
from pipeline.steps.quality_checker import QualityCheckerWorker
from pipeline.steps.serializer import serialize_work

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger(__name__)


def _make_workers() -> list:
    worker_count = settings.worker_count
    return [
        NormalizerWorker(settings, consumer_name=f"normalizer_{i}")
        for i in range(worker_count)
    ] + [
        QualityCheckerWorker(settings, consumer_name=f"quality_{i}")
        for i in range(worker_count)
    ] + [
        PatcherWorker(settings, consumer_name=f"patcher_{i}")
        for i in range(worker_count)
    ] + [
        FileMoverWorker(settings, consumer_name=f"mover_{i}")
        for i in range(worker_count)
    ]


async def run_pipeline() -> None:
    workers = _make_workers()
    tasks = [asyncio.create_task(w.run()) for w in workers]
    logger.info("Pipeline running with %d workers per step.", settings.worker_count)
    try:
        await asyncio.gather(*tasks)
    except asyncio.CancelledError:
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)


async def run_serialize_only(data_type: str) -> None:
    n = await serialize_work(settings, data_type=data_type)
    logger.info("Serializer enqueued %d file(s).", n)


def main() -> None:
    parser = argparse.ArgumentParser(description="Price pipeline")
    parser.add_argument(
        "--serialize-only",
        action="store_true",
        help="Only run the work queue serializer (Step 1), then exit.",
    )
    parser.add_argument(
        "--data-type",
        choices=["tick", "ohlc"],
        default="tick",
        help="Price data type (default: tick)",
    )
    args = parser.parse_args()

    if args.serialize_only:
        asyncio.run(run_serialize_only(args.data_type))
    else:
        asyncio.run(run_pipeline())


if __name__ == "__main__":
    main()
