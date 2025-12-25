from __future__ import annotations

from enum import StrEnum
from typing import Any

from pydantic import BaseModel, Field


class EpisodeEdgeType(StrEnum):
    related_to = "related_to"
    forked_from = "forked_from"
    resumed_from = "resumed_from"
    soft_link = "soft_link"
    contradicts = "contradicts"
    depends_on = "depends_on"


class EpisodeEdge(BaseModel):
    type: EpisodeEdgeType
    to_episode_id: str
    confidence: float = Field(ge=0.0, le=1.0, default=0.5)


class Episode(BaseModel):
    id: str
    braid_id: str
    labels: dict[str, Any] = Field(default_factory=lambda: {"topics": [], "intents": [], "modalities": []})
    primary_knot_span: dict[str, str] = Field(default_factory=dict)
    knot_refs: list[str] = Field(default_factory=list)
    edges: list[EpisodeEdge] = Field(default_factory=list)
    summary_cache: dict[str, Any] = Field(default_factory=dict)
    anchor_quotes: list[dict[str, Any]] = Field(default_factory=list)


