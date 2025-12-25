from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Optional
from uuid import uuid4

from ..schema.bead import Bead, BeadRef, BeadType, BeadVersion
from ..schema.delta import GenericDelta, Provenance, ProvenanceKind, DeltaKind
from ..schema.episode import Episode, EpisodeState
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
        self._episodes: dict[str, Episode] = {}
        self._active_episode_id: str | None = None

    @property
    def deltas(self) -> list[GenericDelta]:
        return list(self._deltas)

    @property
    def knots(self) -> list[Knot]:
        return list(self._knots)

    def get_recent_deltas(self, limit: int) -> list[GenericDelta]:
        if limit <= 0:
            return []
        return list(self._deltas[-limit:])

    def get_recent_knots(self, limit: int) -> list[Knot]:
        if limit <= 0:
            return []
        return list(self._knots[-limit:])

    def close(self) -> None:
        return

    # --- Episodes (Phase 3) ---
    def get_active_episode(self) -> Episode | None:
        if self._active_episode_id is None:
            return None
        return self._episodes.get(self._active_episode_id)

    def set_active_episode_id(self, episode_id: str) -> None:
        self._active_episode_id = episode_id

    def upsert_episode(self, ep: Episode) -> None:
        self._episodes[ep.id] = ep

    def get_episode(self, episode_id: str) -> Episode | None:
        return self._episodes.get(episode_id)

    def list_episodes(self, *, state: EpisodeState | None = None, limit: int = 50) -> list[Episode]:
        eps = list(self._episodes.values())
        if state is not None:
            eps = [e for e in eps if e.state == state]
        return eps[-limit:]

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

    def append_tool_call_delta(
        self,
        *,
        tool_name: str,
        args: dict[str, Any],
        provenance_kind: ProvenanceKind,
        episode_id: str | None = None,
        knot_id: str | None = None,
        microagent_bead_ref: dict[str, Any] | None = None,
        tool_bead_ref: dict[str, Any] | None = None,
    ) -> GenericDelta:
        d = GenericDelta(
            id=str(uuid4()),
            braid_id=self.braid_id,
            kind=DeltaKind.tool_call,
            provenance=Provenance(kind=provenance_kind, tool=tool_name, episode_id=episode_id, knot_id=knot_id),
            payload={
                "tool": tool_name,
                "args": args,
                "microagent_bead_ref": microagent_bead_ref,
                "tool_bead_ref": tool_bead_ref,
            },
            confidence=0.6,
        )
        self.append_delta(d)
        return d

    def append_tool_result_delta(
        self,
        *,
        tool_name: str,
        result: dict[str, Any],
        ok: bool,
        provenance_kind: ProvenanceKind,
        episode_id: str | None = None,
        knot_id: str | None = None,
        microagent_bead_ref: dict[str, Any] | None = None,
        tool_bead_ref: dict[str, Any] | None = None,
    ) -> GenericDelta:
        d = GenericDelta(
            id=str(uuid4()),
            braid_id=self.braid_id,
            kind=DeltaKind.tool_result,
            provenance=Provenance(kind=provenance_kind, tool=tool_name, episode_id=episode_id, knot_id=knot_id),
            payload={
                "tool": tool_name,
                "ok": ok,
                "result": result,
                "microagent_bead_ref": microagent_bead_ref,
                "tool_bead_ref": tool_bead_ref,
            },
            confidence=0.7 if ok else 0.4,
        )
        self.append_delta(d)
        return d

    def upsert_bead_version(self, *, bead_id: str, bead_type: BeadType, data: dict[str, Any]) -> BeadRef:
        bead = self._beads.get(bead_id)
        if bead is None:
            bead = Bead(id=bead_id, type=bead_type)
            self._beads[bead_id] = bead

        version_id = str(uuid4())
        v = BeadVersion(
            id=version_id,
            bead_id=bead_id,
            type=bead_type,
            created_ts=datetime.now(timezone.utc).isoformat(),
            data=data,
        )
        bead.versions[version_id] = v
        bead.active_version_id = version_id
        return BeadRef(bead_id=bead_id, bead_version_id=version_id, role="created")

    def upsert_reasoning_summary_bead(self, narrative: str, structured: dict[str, Any]) -> BeadRef:
        return self.upsert_bead_version(
            bead_id="reasoning-summary",
            bead_type=BeadType.reasoning_bead,
            data={"narrative": narrative, "structured": structured},
        )

    def get_recent_bead_versions(self, *, bead_type: Optional[BeadType], limit: int) -> list[dict[str, Any]]:
        if limit <= 0:
            return []
        rows: list[dict[str, Any]] = []
        for bead in self._beads.values():
            if bead_type is not None and bead.type != bead_type:
                continue
            for v in bead.versions.values():
                rows.append(
                    {
                        "bead_id": bead.id,
                        "bead_version_id": v.id,
                        "type": v.type.value,
                        "created_ts": v.created_ts,
                        "data": v.data,
                    }
                )
        rows.sort(key=lambda r: r["created_ts"])
        return rows[-limit:]

    def commit_knot(
        self,
        primary_episode_id: str,
        start_delta_id: str,
        end_delta_id: str,
        start_ts: str,
        end_ts: str,
        summary: str,
        *,
        knot_id: str | None = None,
        thought_summary_bead_ref: Optional[BeadRef] = None,
        arrivals_during: Optional[list[str]] = None,
        planned_tools: Optional[list[dict[str, Any]]] = None,
        executed_tools: Optional[list[dict[str, Any]]] = None,
    ) -> Knot:
        knot = Knot(
            id=knot_id or str(uuid4()),
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


