"""
Step 6c – Pip Bar Builder

Builds non-regular bars where each bar spans exactly N pips of price movement.
A new bar opens whenever the price moves pip_range pips from the bar open.

Input queue : pip_bar_queue
Output queue: price_abstraction_queue
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from pipeline.steps.abstractions.base import AbstractionBuilderWorker


class PipBarBuilderWorker(AbstractionBuilderWorker):
    input_stream = "pip_bar_queue"
    consumer_group = "pip_bar_group"

    def build(self, tick_df: pd.DataFrame, spec: dict) -> pd.DataFrame:
        """
        Build pip bars — one bar per N pips of price movement.

        spec keys:
            pip_range (float): pip range per bar (e.g. 10)
            pip_size  (float): size of one pip (default 0.0001)
        """
        pip_range = float(spec.get("pip_range", 10))
        pip_size = float(spec.get("pip_size", 0.0001))
        threshold = pip_range * pip_size

        df = tick_df.sort_values("timestamp").reset_index(drop=True)
        mid = (
            df["mid_price"].to_numpy()
            if "mid_price" in df.columns
            else ((df["bid"] + df["ask"]) / 2).to_numpy()
        )
        timestamps = df["timestamp"].to_numpy()
        vol = df["volume"].to_numpy() if "volume" in df.columns else np.zeros(len(df))

        bars = []
        bar_open = mid[0]
        bar_ts = timestamps[0]
        bar_high = mid[0]
        bar_low = mid[0]
        bar_vol = 0.0

        for i in range(len(mid)):
            price = mid[i]
            bar_high = max(bar_high, price)
            bar_low = min(bar_low, price)
            bar_vol += vol[i]

            if abs(price - bar_open) >= threshold:
                bars.append(
                    {
                        "timestamp": bar_ts,
                        "open": bar_open,
                        "high": bar_high,
                        "low": bar_low,
                        "close": price,
                        "volume": bar_vol,
                    }
                )
                bar_open = price
                bar_ts = timestamps[i]
                bar_high = price
                bar_low = price
                bar_vol = 0.0

        return pd.DataFrame(bars)
