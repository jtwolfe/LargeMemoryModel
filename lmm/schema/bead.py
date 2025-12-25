from __future__ import annotations

from enum import StrEnum
from typing import Any, Optional

from pydantic import BaseModel, Field


class BeadType(StrEnum):
    tool_bead = "tool_bead"
    skill_bead = "skill_bead"
    memory_bead = "memory_bead"
    microagent_bead = "microagent_bead"
    reasoning_bead = "reasoning_bead"


class BeadRef(BaseModel):
    bead_id: str
    bead_version_id: Optional[str] = None
    role: str = "used"


class BeadVersion(BaseModel):
    id: str
    bead_id: str
    type: BeadType
    created_ts: str
    data: dict[str, Any] = Field(default_factory=dict)


class Bead(BaseModel):
    """Logical bead with versioning."""

    id: str
    type: BeadType
    active_version_id: Optional[str] = None
    versions: dict[str, BeadVersion] = Field(default_factory=dict)


