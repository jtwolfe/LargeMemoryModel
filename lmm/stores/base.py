from __future__ import annotations

from typing import Any, Protocol

from ..schema.bead import BeadRef
from ..schema.delta import GenericDelta, ProvenanceKind
from ..schema.knot import Knot


class EpisodicStore(Protocol):
    """Storage interface for episodic log + knot commits (v0).

    Concrete implementations:
    - InMemoryEpisodicStore (dev/tests)
    - Neo4jEpisodicStore (Phase 1 persistence)
    """

    braid_id: str

    def append_delta(self, delta: GenericDelta) -> None: ...

    def append_message_delta(self, role: str, content: str, provenance_kind: ProvenanceKind) -> GenericDelta: ...

    def append_tool_call_delta(
        self, *, tool_name: str, args: dict[str, Any], provenance_kind: ProvenanceKind
    ) -> GenericDelta: ...

    def append_tool_result_delta(
        self, *, tool_name: str, result: dict[str, Any], ok: bool, provenance_kind: ProvenanceKind
    ) -> GenericDelta: ...

    def upsert_reasoning_summary_bead(self, narrative: str, structured: dict[str, Any]) -> BeadRef: ...

    def commit_knot(self, **kwargs: Any) -> Knot: ...

    def get_recent_deltas(self, limit: int) -> list[GenericDelta]: ...

    def get_recent_knots(self, limit: int) -> list[Knot]: ...

    def close(self) -> None: ...


