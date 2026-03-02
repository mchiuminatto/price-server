"""
Step 6b – Tick Bar Builder

Groups ticks into bars of a fixed number of ticks (e.g. every 1000 ticks).
Each bar has open/high/low/close/volume computed over that tick window.

Input queue : tick_bar_queue
Output queue: price_abstraction_queue
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from pipeline.steps.abstractions.base import AbstractionBuilderWorker


class TickBarBuilderWorker(AbstractionBuilderWorker):
    input_stream = "tick_bar_queue"
    consumer_group = "tick_bar_group"

    def build(self, tick_df: pd.DataFrame, spec: dict) -> pd.DataFrame:
        """
        Build tick bars — one bar per N ticks.

        spec keys:
            tick_count (int): number of ticks per bar
        """
        tick_count = int(spec.get("tick_count", 1000))

        df = tick_df.sort_values("timestamp").reset_index(drop=True)
        mid = df["mid_price"] if "mid_price" in df.columns else (df["bid"] + df["ask"]) / 2
        vol = df["volume"] if "volume" in df.columns else pd.Series(np.zeros(len(df)))

        bar_index = df.index // tick_count
        bars = pd.DataFrame(
            {
                "timestamp": df.groupby(bar_index)["timestamp"].first(),
                "open": mid.groupby(bar_index).first(),
                "high": mid.groupby(bar_index).max(),
                "low": mid.groupby(bar_index).min(),
                "close": mid.groupby(bar_index).last(),
                "volume": vol.groupby(bar_index).sum(),
                "tick_count": df.groupby(bar_index).size(),
            }
        ).reset_index(drop=True)

        return bars
