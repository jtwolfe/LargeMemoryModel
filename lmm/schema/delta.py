from __future__ import annotations

from datetime import datetime, timezone
from enum import StrEnum
from typing import Any, Optional

from pydantic import BaseModel, Field


class DeltaKind(StrEnum):
    tick = "tick"
    message = "message"
    knot_lifecycle = "knot_lifecycle"
    observation = "observation"
    bead_ref = "bead_ref"
    bead_write = "bead_write"
    bead_version = "bead_version"
    tool_call = "tool_call"
    tool_result = "tool_result"
    microagent = "microagent"
    hypothesis = "hypothesis"
    trust = "trust"


class ProvenanceKind(StrEnum):
    user = "user"
    assistant = "assistant"
    tool = "tool"
    perception = "perception"
    system = "system"
    dream = "dream"


class Provenance(BaseModel):
    kind: ProvenanceKind
    source: Optional[str] = None
    model: Optional[str] = None
    tool: Optional[str] = None
    episode_id: Optional[str] = None
    knot_id: Optional[str] = None


class DeltaRefs(BaseModel):
    beads: list[dict[str, Any]] = Field(default_factory=list)
    knots: list[str] = Field(default_factory=list)
    episodes: list[str] = Field(default_factory=list)
    deltas: list[str] = Field(default_factory=list)


class GenericDelta(BaseModel):
    """Storage envelope for all braid events."""

    id: str
    braid_id: str
    kind: DeltaKind
    ts: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    provenance: Provenance
    confidence: float = Field(ge=0.0, le=1.0, default=0.5)
    payload: dict[str, Any] = Field(default_factory=dict)
    refs: Optional[DeltaRefs] = None
    tags: list[str] = Field(default_factory=list)


