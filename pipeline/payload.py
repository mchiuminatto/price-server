"""
Canonical pipeline message payload.

Each step receives this envelope, enriches its own `steps` entry, and
forwards the updated envelope to the next queue.
"""

from __future__ import annotations

from typing import Any, Literal
from pydantic import BaseModel, Field
import uuid


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
    action: Literal["append", "insert", "create", ""] = ""
    output_path: str = ""


class FileMoverResult(StepResult):
    target_path: str = ""


class PipelinePayload(BaseModel):
    file_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    source_path: str
    instrument: str
    data_type: Literal["tick", "ohlc"]

    # Enriched progressively as the message flows through steps
    normalizer: NormalizerResult | None = None
    quality: QualityResult | None = None
    patcher: PatcherResult | None = None
    file_mover: FileMoverResult | None = None
