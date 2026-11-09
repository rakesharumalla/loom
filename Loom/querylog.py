"""Per-request capture of the backend calls (SQL statements, REST requests) behind one UI action.

Only the web UI routes open a capture; outside one, record() and note() are no-ops. The adapters feed
it from two central hooks (adapters/databricks/capture.py). Worker threads see the caller's capture
when their function is wrapped with carry().
"""
from __future__ import annotations

import contextvars
import json
import threading
import time
from contextlib import contextmanager
from typing import Any, Callable

_current: contextvars.ContextVar["Capture | None"] = contextvars.ContextVar("loom_querylog", default=None)
MAX_STR = 240


class Capture:
    def __init__(self):
        self.t0 = time.perf_counter()
        self.calls: list[dict] = []
        self._lock = threading.Lock()

    def add(self, row: dict) -> None:
        with self._lock:
            if row["kind"] == "note" and any(c["kind"] == "note" and c["text"] == row["text"] for c in self.calls):
                return
            self.calls.append(row)


@contextmanager
def capture():
    c = Capture()
    tok = _current.set(c)
    try:
        yield c
    finally:
        _current.reset(tok)


def active() -> bool:
    return _current.get() is not None


def record(kind: str, target: str, text: str, started: float, ok: bool = True, detail: str | None = None) -> None:
    """kind: sql | rest. started: time.perf_counter() when the call began."""
    c = _current.get()
    if c is None:
        return
    now = time.perf_counter()
    c.add({"kind": kind, "target": target, "text": text, "at_ms": round((started - c.t0) * 1000, 1),
           "ms": round((now - started) * 1000, 1), "ok": ok, "detail": detail})


def note(text: str, unless: str | None = None) -> None:
    """A step that did not reach a backend (e.g. served from an in-process cache).
    unless: skip the note when a call already captured in this request contains this text."""
    c = _current.get()
    if c is not None and not (unless and any(unless in x["text"] for x in c.calls)):
        c.add({"kind": "note", "target": "Loom", "text": text, "at_ms": round((time.perf_counter() - c.t0) * 1000, 1),
               "ms": 0.0, "ok": True, "detail": None})


def carry(fn: Callable) -> Callable:
    """Run fn in a worker thread under the caller's capture (thread pools do not copy contextvars)."""
    c = _current.get()

    def run(*a, **kw):
        tok = _current.set(c)
        try:
            return fn(*a, **kw)
        finally:
            _current.reset(tok)
    return run


def shorten(v: Any) -> Any:
    """Display copy of call parameters: embedding vectors and long payloads are abbreviated."""
    if isinstance(v, str):
        if len(v) > MAX_STR and v.startswith("[") and v.endswith("]") and _is_float(v[1:v.find(",")]):
            nums = v[1:-1].split(",")
            return f"[{','.join(nums[:3])},…] ({len(nums)} floats)"
        return v if len(v) <= MAX_STR else v[:MAX_STR] + f"… ({len(v)} chars)"
    if isinstance(v, dict):
        return {k: shorten(x) for k, x in v.items()}
    if isinstance(v, (list, tuple)):
        if len(v) > 16 and all(isinstance(x, float) for x in v):
            return f"[{','.join(f'{x:.4f}' for x in v[:3])},…] ({len(v)} floats)"
        return type(v)(shorten(x) for x in v)
    obj = getattr(v, "obj", None)   # psycopg Jsonb wrapper
    if obj is not None:
        return shorten(json.dumps(obj, default=str))
    return v


def _is_float(s: str) -> bool:
    try:
        float(s)
        return True
    except ValueError:
        return False
