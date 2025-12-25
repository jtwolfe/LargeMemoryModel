from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, Optional
from uuid import uuid4

from ..schema.bead import BeadRef, BeadType
from ..schema.delta import DeltaKind, GenericDelta, Provenance, ProvenanceKind
from ..schema.knot import Knot
from ..schema.episode import Episode, EpisodeEdge, EpisodeEdgeType, EpisodeState


class Neo4jEpisodicStore:
    """Neo4j-backed episodic store (Phase 1 persistence).

    Storage model (v0, pragmatic):
    - (:Braid {id, next_delta_seq, next_knot_seq})
    - (:Delta {id, braid_id, seq, kind, ts, confidence, provenance_json, payload_json, tags_json})
    - (:Knot {id, braid_id, seq, ...knot fields..., *_json})
    - (:Bead {id, type, active_version_id})
    - (:BeadVersion {id, bead_id, braid_id, type, created_ts, data_json})
    """

    def __init__(self, *, braid_id: str, uri: str, user: str, password: str) -> None:
        self.braid_id = braid_id

        try:
            from neo4j import GraphDatabase  # type: ignore
        except Exception as exc:  # pragma: no cover
            raise RuntimeError(
                "Neo4j driver is not installed. Add `neo4j` to requirements and reinstall."
            ) from exc

        self._driver = GraphDatabase.driver(uri, auth=(user, password))
        self._ep_edge_type_seen: bool = False
        self._ensure_schema()
        self._ensure_braid()

    def close(self) -> None:
        try:
            self._driver.close()
        except Exception:
            return

    def _ensure_schema(self) -> None:
        statements = [
            "CREATE CONSTRAINT braid_id IF NOT EXISTS FOR (b:Braid) REQUIRE b.id IS UNIQUE",
            "CREATE CONSTRAINT delta_id IF NOT EXISTS FOR (d:Delta) REQUIRE d.id IS UNIQUE",
            "CREATE CONSTRAINT knot_id IF NOT EXISTS FOR (k:Knot) REQUIRE k.id IS UNIQUE",
            "CREATE CONSTRAINT bead_id IF NOT EXISTS FOR (b:Bead) REQUIRE b.id IS UNIQUE",
            "CREATE CONSTRAINT bead_version_id IF NOT EXISTS FOR (v:BeadVersion) REQUIRE v.id IS UNIQUE",
            "CREATE CONSTRAINT episode_id IF NOT EXISTS FOR (e:Episode) REQUIRE e.id IS UNIQUE",
            "CREATE INDEX delta_braid_seq IF NOT EXISTS FOR (d:Delta) ON (d.braid_id, d.seq)",
            "CREATE INDEX knot_braid_seq IF NOT EXISTS FOR (k:Knot) ON (k.braid_id, k.seq)",
            "CREATE INDEX episode_braid_state IF NOT EXISTS FOR (e:Episode) ON (e.braid_id, e.state)",
        ]
        with self._driver.session() as session:
            for q in statements:
                try:
                    session.run(q)
                except Exception:
                    # Neo4j community edition may vary; keep best-effort.
                    pass

    def _relationship_type_exists(self, rel_type: str) -> bool:
        # Avoid warnings from Neo4j about relationship types that don't exist yet.
        if rel_type == "EP_EDGE" and self._ep_edge_type_seen:
            return True
        try:
            with self._driver.session() as session:
                types = session.run("CALL db.relationshipTypes() YIELD relationshipType RETURN relationshipType").values(
                    "relationshipType"
                )
            found = rel_type in [t[0] for t in types]
            if rel_type == "EP_EDGE" and found:
                self._ep_edge_type_seen = True
            return found
        except Exception:
            return False

    def _ensure_braid(self) -> None:
        with self._driver.session() as session:
            session.run(
                "MERGE (b:Braid {id: $id}) "
                "ON CREATE SET b.next_delta_seq = 0, b.next_knot_seq = 0, b.active_episode_id = '' "
                "ON MATCH SET b.active_episode_id = coalesce(b.active_episode_id, '')",
                id=self.braid_id,
            )

    # --- Episodes (Phase 3) ---
    def set_active_episode_id(self, episode_id: str) -> None:
        with self._driver.session() as session:
            session.run(
                "MATCH (b:Braid {id: $id}) SET b.active_episode_id = $eid",
                id=self.braid_id,
                eid=episode_id,
            )

    def get_active_episode(self) -> Optional[Episode]:
        with self._driver.session() as session:
            rec = session.run(
                "MATCH (b:Braid {id: $id}) RETURN b.active_episode_id AS eid",
                id=self.braid_id,
            ).single()
        eid = (rec or {}).get("eid")
        if not eid:
            return None
        return self.get_episode(str(eid))

    def upsert_episode(self, ep: Episode) -> None:
        with self._driver.session() as session:
            session.run(
                "MERGE (e:Episode {id: $id}) "
                "SET e.braid_id = $bid, e.state = $state, e.labels_json = $labels, "
                "e.primary_knot_span_json = $span, e.knot_refs_json = $knot_refs, "
                "e.summary_cache_json = $summary, e.anchor_quotes_json = $anchors",
                id=ep.id,
                bid=ep.braid_id,
                state=str(ep.state.value),
                labels=json.dumps(ep.labels or {}),
                span=json.dumps(ep.primary_knot_span or {}),
                knot_refs=json.dumps(ep.knot_refs or []),
                summary=json.dumps(ep.summary_cache or {}),
                anchors=json.dumps(ep.anchor_quotes or []),
            )
            # edges (simple): recreate outgoing edges
            session.run("MATCH (e:Episode {id:$id})-[r:EP_EDGE]->() DELETE r", id=ep.id)
            for edge in ep.edges:
                session.run(
                    "MATCH (e:Episode {id:$id}) "
                    "MERGE (t:Episode {id:$to}) "
                    "MERGE (e)-[r:EP_EDGE {type:$type}]->(t) "
                    "SET r.confidence = $conf",
                    id=ep.id,
                    to=edge.to_episode_id,
                    type=str(edge.type.value),
                    conf=float(edge.confidence),
                )

    def get_episode(self, episode_id: str) -> Optional[Episode]:
        with self._driver.session() as session:
            rec = session.run("MATCH (e:Episode {id:$id}) RETURN e", id=episode_id).single()
            if rec is None:
                return None
            e = dict(rec["e"])
            edges_rows = []
            if self._relationship_type_exists("EP_EDGE"):
                edges_rows = session.run(
                    "MATCH (e:Episode {id:$id})-[r:EP_EDGE]->(t:Episode) "
                    "RETURN r.type AS type, t.id AS to, r.confidence AS confidence",
                    id=episode_id,
                ).data()
        edges: list[EpisodeEdge] = []
        for row in edges_rows:
            edges.append(
                EpisodeEdge(
                    type=EpisodeEdgeType(row["type"]),
                    to_episode_id=row["to"],
                    confidence=float(row.get("confidence") or 0.5),
                )
            )
        return Episode(
            id=e["id"],
            braid_id=e.get("braid_id") or self.braid_id,
            state=EpisodeState(e.get("state") or EpisodeState.active.value),
            labels=json.loads(e.get("labels_json") or "{}"),
            primary_knot_span=json.loads(e.get("primary_knot_span_json") or "{}"),
            knot_refs=json.loads(e.get("knot_refs_json") or "[]"),
            edges=edges,
            summary_cache=json.loads(e.get("summary_cache_json") or "{}"),
            anchor_quotes=json.loads(e.get("anchor_quotes_json") or "[]"),
        )

    def list_episodes(self, *, state: Optional[EpisodeState] = None, limit: int = 50) -> list[Episode]:
        where = ""
        params: dict[str, Any] = {"bid": self.braid_id, "limit": int(limit)}
        if state is not None:
            where = "AND e.state = $state"
            params["state"] = state.value
        with self._driver.session() as session:
            ids = session.run(
                "MATCH (e:Episode {braid_id:$bid}) "
                f"WHERE true {where} "
                "RETURN e.id AS id ORDER BY e.id DESC LIMIT $limit",
                **params,
            ).values("id")
        out: list[Episode] = []
        for row in ids:
            ep = self.get_episode(str(row[0]))
            if ep is not None:
                out.append(ep)
        out.reverse()
        return out

    def _next_seq(self, field: str) -> int:
        with self._driver.session() as session:
            rec = session.run(
                "MATCH (b:Braid {id: $id}) "
                f"SET b.{field} = coalesce(b.{field}, 0) + 1 "
                f"RETURN b.{field} AS seq",
                id=self.braid_id,
            ).single()
            return int(rec["seq"])

    def append_delta(self, delta: GenericDelta) -> None:
        seq = self._next_seq("next_delta_seq")
        with self._driver.session() as session:
            session.run(
                "CREATE (d:Delta {"
                "id: $id, braid_id: $braid_id, seq: $seq, kind: $kind, ts: $ts, confidence: $confidence, "
                "provenance_json: $prov, payload_json: $payload, tags_json: $tags"
                "})",
                id=delta.id,
                braid_id=delta.braid_id,
                seq=seq,
                kind=str(delta.kind.value),
                ts=delta.ts.isoformat(),
                confidence=float(delta.confidence),
                prov=delta.provenance.model_dump_json(),
                payload=json.dumps(delta.payload),
                tags=json.dumps(delta.tags),
            )

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

    def upsert_reasoning_summary_bead(self, narrative: str, structured: dict[str, Any]) -> BeadRef:
        return self.upsert_bead_version(
            bead_id="reasoning-summary",
            bead_type=BeadType.reasoning_bead,
            data={"narrative": narrative, "structured": structured},
        )

    def upsert_bead_version(self, *, bead_id: str, bead_type: BeadType, data: dict[str, Any]) -> BeadRef:
        version_id = str(uuid4())
        created_ts = datetime.now(timezone.utc).isoformat()
        data_json = json.dumps(data)
        with self._driver.session() as session:
            session.run(
                "MERGE (b:Bead {id: $bead_id}) "
                "ON CREATE SET b.type = $type, b.active_version_id = $vid "
                "ON MATCH SET b.active_version_id = $vid "
                "WITH b "
                "CREATE (v:BeadVersion {id: $vid, bead_id: $bead_id, braid_id: $bid, type: $type, created_ts: $ts, data_json: $data}) "
                "MERGE (b)-[:HAS_VERSION]->(v)",
                bead_id=bead_id,
                bid=self.braid_id,
                vid=version_id,
                type=str(bead_type.value),
                ts=created_ts,
                data=data_json,
            )
        return BeadRef(bead_id=bead_id, bead_version_id=version_id, role="created")

    def get_recent_bead_versions(self, *, bead_type: Optional[BeadType], limit: int) -> list[dict[str, Any]]:
        if limit <= 0:
            return []
        where = ""
        params: dict[str, Any] = {"limit": int(limit), "bid": self.braid_id}
        if bead_type is not None:
            where = "AND v.type = $type"
            params["type"] = bead_type.value
        with self._driver.session() as session:
            rows = session.run(
                "MATCH (v:BeadVersion {braid_id: $bid}) "
                f"WHERE true {where} "
                "RETURN v ORDER BY v.created_ts DESC LIMIT $limit",
                **params,
            ).values("v")
        out: list[dict[str, Any]] = []
        for row in rows:
            v = dict(row[0])
            out.append(
                {
                    "bead_id": v.get("bead_id"),
                    "bead_version_id": v.get("id"),
                    "braid_id": v.get("braid_id"),
                    "type": v.get("type"),
                    "created_ts": v.get("created_ts"),
                    "data": json.loads(v.get("data_json") or "{}"),
                }
            )
        out.reverse()
        return out

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
        seq = self._next_seq("next_knot_seq")
        with self._driver.session() as session:
            session.run(
                "CREATE (k:Knot {"
                "id: $id, braid_id: $braid_id, seq: $seq, primary_episode_id: $ep, "
                "start_ts: $start_ts, end_ts: $end_ts, "
                "start_delta_id: $sd, end_delta_id: $ed, "
                "summary: $summary, thought_bead_ref_json: $thought_ref, "
                "arrivals_json: $arrivals, planned_tools_json: $planned, executed_tools_json: $executed"
                "})",
                id=knot.id,
                braid_id=knot.braid_id,
                seq=seq,
                ep=knot.primary_episode_id,
                start_ts=knot.start_ts,
                end_ts=knot.end_ts,
                sd=start_delta_id,
                ed=end_delta_id,
                summary=knot.summary,
                thought_ref=thought_summary_bead_ref.model_dump_json() if thought_summary_bead_ref else None,
                arrivals=json.dumps(knot.arrivals_during),
                planned=json.dumps(knot.planned_tools),
                executed=json.dumps(knot.executed_tools),
            )
        return knot

    def get_recent_deltas(self, limit: int) -> list[GenericDelta]:
        if limit <= 0:
            return []
        with self._driver.session() as session:
            rows = session.run(
                "MATCH (d:Delta {braid_id: $bid}) "
                "RETURN d ORDER BY d.seq DESC LIMIT $limit",
                bid=self.braid_id,
                limit=int(limit),
            ).values("d")
        deltas: list[GenericDelta] = []
        for row in reversed(rows):
            # neo4j Result.values("d") returns a list of lists, each containing the value(s).
            node = row[0]
            d = dict(node)
            deltas.append(
                GenericDelta(
                    id=d["id"],
                    braid_id=d["braid_id"],
                    kind=DeltaKind(d["kind"]),
                    ts=datetime.fromisoformat(d["ts"]),
                    provenance=Provenance.model_validate_json(d["provenance_json"]),
                    confidence=float(d["confidence"]),
                    payload=json.loads(d["payload_json"] or "{}"),
                    tags=json.loads(d.get("tags_json") or "[]"),
                )
            )
        return deltas

    def get_recent_knots(self, limit: int) -> list[Knot]:
        if limit <= 0:
            return []
        with self._driver.session() as session:
            rows = session.run(
                "MATCH (k:Knot {braid_id: $bid}) "
                "RETURN k ORDER BY k.seq DESC LIMIT $limit",
                bid=self.braid_id,
                limit=int(limit),
            ).values("k")
        knots: list[Knot] = []
        for row in reversed(rows):
            node = row[0]
            k = dict(node)
            thought_ref = k.get("thought_bead_ref_json")
            knots.append(
                Knot(
                    id=k["id"],
                    braid_id=k["braid_id"],
                    primary_episode_id=k["primary_episode_id"],
                    start_ts=k["start_ts"],
                    end_ts=k["end_ts"],
                    delta_range={"start_delta_id": k["start_delta_id"], "end_delta_id": k["end_delta_id"]},
                    summary=k.get("summary") or "",
                    thought_summary_bead_ref=BeadRef.model_validate_json(thought_ref) if thought_ref else None,
                    arrivals_during=json.loads(k.get("arrivals_json") or "[]"),
                    planned_tools=json.loads(k.get("planned_tools_json") or "[]"),
                    executed_tools=json.loads(k.get("executed_tools_json") or "[]"),
                )
            )
        return knots


