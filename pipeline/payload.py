"""
Canonical pipeline message payload.

Each step receives this envelope, enriches its own `steps` entry, and
forwards the updated envelope to the next queue.
"""

from __future__ import annotations

import uuid
from typing import Any, Literal

from pydantic import BaseModel, Field


class StepResult(BaseModel):
    status: Literal["ok", "error"] = "ok"
    error: str | None = None
    extra: dict[str, Any] = Field(default_factory=dict)


class NormalizerResult(StepResult):
    output_path: str = ""
    rows: int = 0


class QualityResult(StepResult):
    gaps_found: int = 0
    report_path: str = ""


class PatcherResult(StepResult):
    action: Literal["append", "create", ""] = ""
    output_path: str = ""


class DistributorResult(StepResult):
    abstractions_dispatched: list[str] = Field(default_factory=list)


class AbstractionResult(StepResult):
    abstraction_type: str = ""
    output_path: str = ""
    rows: int = 0


class PipelinePayload(BaseModel):
    file_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    source_path: str
    instrument: str
    data_type: Literal["tick", "ohlc"]
    config_path: str = ""  # path to pipeline_config.yaml

    # Routing envelope set by the Distributor for each fan-out child message
    abstraction_spec: dict[str, Any] = Field(default_factory=dict)
    abstraction_label: str = ""

    # Enriched progressively as the message flows through the pipeline
    normalizer: NormalizerResult | None = None
    quality: QualityResult | None = None
    patcher: PatcherResult | None = None
    distributor: DistributorResult | None = None
    # keyed by abstraction label e.g. "ohlc_1m", "pip_bar_10"
    abstractions: dict[str, AbstractionResult] = Field(default_factory=dict)
