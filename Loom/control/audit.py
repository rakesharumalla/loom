"""Audit records (memory_audit). Implemented in Loom; rows flow to Delta via Lakebase CDF.

Every record carries the surface (rest | mcp | sdk | web | job); parity tests compare records
ignoring event_id, ts, request_id and surface.
"""
from __future__ import annotations

import uuid
from dataclasses import dataclass, field

from Loom.ports import ControlPlanePort

VOLATILE_KEYS = ("event_id", "ts", "request_id", "surface")


def new_request_id() -> str:
    return str(uuid.uuid4())


@dataclass
class Auditor:
    store: ControlPlanePort
    surface: str
    request_id: str
    caller: str
    rows: list[dict] = field(default_factory=list)

    _flushed: int = 0

    def record(self, action: str, decision: str, space_id: str | None = None, **detail) -> None:
        self.rows.append({"request_id": self.request_id, "caller": self.caller, "surface": self.surface,
                          "action": action, "space_id": space_id, "decision": decision, "detail": detail})

    def flush(self) -> None:
        """One write per request (one transaction on Lakebase)."""
        pending = self.rows[self._flushed:]
        if pending:
            self.store.write_audit_many(pending)
            self._flushed = len(self.rows)


def comparable(rows: list[dict]) -> list[dict]:
    """Strip per-call fields so audit rows from different surfaces can be compared."""
    out = [{k: v for k, v in r.items() if k not in VOLATILE_KEYS} for r in rows]
    return sorted(out, key=lambda r: (r["action"], r.get("space_id") or "", str(r["detail"])))
