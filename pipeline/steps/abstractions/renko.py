"""
Step 6d – Renko Bar Builder

Builds Renko bars where each brick has a fixed size in pips (body_pips).
A new brick is added only when price moves at least body_pips in one direction.

Input queue : renko_bar_queue
Output queue: price_abstraction_queue
"""

from __future__ import annotations

import pandas as pd

from pipeline.steps.abstractions.base import AbstractionBuilderWorker


class RenkoBarBuilderWorker(AbstractionBuilderWorker):
    input_stream = "renko_bar_queue"
    consumer_group = "renko_bar_group"

    def build(self, tick_df: pd.DataFrame, spec: dict) -> pd.DataFrame:
        """
        Build Renko bars.

        spec keys:
            body_pips (float): brick size in pips (e.g. 5)
            pip_size  (float): size of one pip (default 0.0001)
        """
        body_pips = float(spec.get("body_pips", 5))
        pip_size = float(spec.get("pip_size", 0.0001))
        brick_size = body_pips * pip_size

        df = tick_df.sort_values("timestamp").reset_index(drop=True)
        mid = (
            df["mid_price"].to_numpy()
            if "mid_price" in df.columns
            else ((df["bid"] + df["ask"]) / 2).to_numpy()
        )
        timestamps = df["timestamp"].to_numpy()

        bricks = []
        last_close = mid[0]

        for i in range(1, len(mid)):
            price = mid[i]
            while price >= last_close + brick_size:
                brick_open = last_close
                brick_close = last_close + brick_size
                bricks.append(
                    {
                        "timestamp": timestamps[i],
                        "open": brick_open,
                        "high": brick_close,
                        "low": brick_open,
                        "close": brick_close,
                        "direction": "up",
                    }
                )
                last_close = brick_close

            while price <= last_close - brick_size:
                brick_open = last_close
                brick_close = last_close - brick_size
                bricks.append(
                    {
                        "timestamp": timestamps[i],
                        "open": brick_open,
                        "high": brick_open,
                        "low": brick_close,
                        "close": brick_close,
                        "direction": "down",
                    }
                )
                last_close = brick_close

        return pd.DataFrame(bricks)
