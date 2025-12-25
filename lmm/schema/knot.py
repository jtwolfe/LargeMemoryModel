from __future__ import annotations

from typing import Any, Optional

from pydantic import BaseModel, Field

from .bead import BeadRef


class Knot(BaseModel):
    id: str
    braid_id: str
    primary_episode_id: str
    start_ts: str
    end_ts: str
    delta_range: dict[str, str]

    inbox_watermark: dict[str, Any] = Field(default_factory=dict)
    arrivals_during: list[str] = Field(default_factory=list)

    summary: str = ""
    thought_summary_bead_ref: Optional[BeadRef] = None

    planned_tools: list[dict[str, Any]] = Field(default_factory=list)
    executed_tools: list[dict[str, Any]] = Field(default_factory=list)
    hypotheses: list[dict[str, Any]] = Field(default_factory=list)
    metrics: dict[str, Any] = Field(default_factory=dict)


