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
from pipeline.steps.serializer import serialize_work

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger(__name__)


def _make_workers() -> list:
    n = settings.worker_count
    return (
        # Step 2 – Normalizer
        [NormalizerWorker(settings, consumer_name=f"normalizer_{i}") for i in range(n)]
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
