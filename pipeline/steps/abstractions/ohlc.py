"""
Step 6a – OHLC Builder

Resamples normalised tick data into regular OHLC bars at the timeframe
specified in the abstraction spec (e.g. "1min", "5min", "1h").

Input queue : ohlc_queue
Output queue: price_abstraction_queue
"""

from __future__ import annotations

import pandas as pd

from pipeline.steps.abstractions.base import AbstractionBuilderWorker


class OHLCBuilderWorker(AbstractionBuilderWorker):
    input_stream = "ohlc_queue"
    consumer_group = "ohlc_group"

    def build(self, tick_df: pd.DataFrame, spec: dict) -> pd.DataFrame:
        """
        Resample mid_price into OHLC bars.

        spec keys:
            timeframe (str): pandas offset alias, e.g. "1min", "5min", "1h"
        """
        timeframe = spec.get("timeframe", "1min")

        df = tick_df.set_index("timestamp").sort_index()
        mid = df["mid_price"] if "mid_price" in df.columns else (df["bid"] + df["ask"]) / 2

        ohlc = mid.resample(timeframe).ohlc()
        ohlc["volume"] = df["volume"].resample(timeframe).sum() if "volume" in df.columns else 0
        ohlc = ohlc.dropna(subset=["open"]).reset_index()
        ohlc.rename(columns={"timestamp": "timestamp"}, inplace=True)
        return ohlc
