from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Optional
from uuid import uuid4

from ..schema.bead import Bead, BeadRef, BeadType, BeadVersion
from ..schema.delta import GenericDelta, Provenance, ProvenanceKind, DeltaKind
from ..schema.knot import Knot


@dataclass
class KnotCommitResult:
    knot: Knot
    deltas: list[GenericDelta]


class InMemoryEpisodicStore:
    """A minimal in-memory episodic log.

    This is a skeleton implementation: append deltas and commit knots over delta ranges.
    """

    def __init__(self, braid_id: str):
        self.braid_id = braid_id
        self._deltas: list[GenericDelta] = []
        self._knots: list[Knot] = []
        self._beads: dict[str, Bead] = {}

    @property
    def deltas(self) -> list[GenericDelta]:
        return list(self._deltas)

    @property
    def knots(self) -> list[Knot]:
        return list(self._knots)

    def append_delta(self, delta: GenericDelta) -> None:
        self._deltas.append(delta)

    def append_message_delta(self, role: str, content: str, provenance_kind: ProvenanceKind) -> GenericDelta:
        d = GenericDelta(
            id=str(uuid4()),
            braid_id=self.braid_id,
            kind=DeltaKind.message,
            provenance=Provenance(kind=provenance_kind),
            payload={"role": role, "content": content},
            confidence=0.7 if provenance_kind in (ProvenanceKind.user, ProvenanceKind.tool) else 0.6,
        )
        self.append_delta(d)
        return d

    def upsert_reasoning_summary_bead(self, narrative: str, structured: dict[str, Any]) -> BeadRef:
        bead_id = "reasoning-summary"
        bead = self._beads.get(bead_id)
        if bead is None:
            bead = Bead(id=bead_id, type=BeadType.reasoning_bead)
            self._beads[bead_id] = bead

        version_id = str(uuid4())
        v = BeadVersion(
            id=version_id,
            bead_id=bead_id,
            type=BeadType.reasoning_bead,
            created_ts=datetime.now(timezone.utc).isoformat(),
            data={"narrative": narrative, "structured": structured},
        )
        bead.versions[version_id] = v
        bead.active_version_id = version_id
        return BeadRef(bead_id=bead_id, bead_version_id=version_id, role="created")

    def commit_knot(
        self,
        primary_episode_id: str,
        start_delta_id: str,
        end_delta_id: str,
        start_ts: str,
        end_ts: str,
        summary: str,
        thought_summary_bead_ref: Optional[BeadRef] = None,
        arrivals_during: Optional[list[str]] = None,
        planned_tools: Optional[list[dict[str, Any]]] = None,
        executed_tools: Optional[list[dict[str, Any]]] = None,
    ) -> Knot:
        knot = Knot(
            id=str(uuid4()),
            braid_id=self.braid_id,
            primary_episode_id=primary_episode_id,
            start_ts=start_ts,
            end_ts=end_ts,
            delta_range={"start_delta_id": start_delta_id, "end_delta_id": end_delta_id},
            summary=summary,
            thought_summary_bead_ref=thought_summary_bead_ref,
            arrivals_during=arrivals_during or [],
            planned_tools=planned_tools or [],
            executed_tools=executed_tools or [],
        )
        self._knots.append(knot)
        return knot


