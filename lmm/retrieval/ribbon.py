from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from ..schema.delta import GenericDelta
from ..schema.knot import Knot


@dataclass
class ContextRibbon:
    """A minimal context ribbon artifact.

    v0 skeleton: carries recent messages + episode/knot metadata.
    """

    recent_messages: list[dict[str, Any]]
    knot_count: int
    delta_count: int


class InMemoryRibbonBuilder:
    def build(self, deltas: list[GenericDelta], knots: list[Knot], max_messages: int = 20) -> ContextRibbon:
        msgs: list[dict[str, Any]] = []
        for d in reversed(deltas):
            if d.kind.value == "message":
                msgs.append({"role": d.payload.get("role"), "content": d.payload.get("content")})
                if len(msgs) >= max_messages:
                    break
        msgs.reverse()
        return ContextRibbon(recent_messages=msgs, knot_count=len(knots), delta_count=len(deltas))


