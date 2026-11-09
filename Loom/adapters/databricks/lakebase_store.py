"""StoragePort and ControlPlanePort on Lakebase (Postgres). Replaces MongoDB metadata + S3 bookkeeping.

Small text is stored inline in file_versions; large or binary payloads go to the UC Volume
(content-addressed, write-once) and the row stores the volume path. Every multi-row change
(file write, delete, restore, overlay merge, space configure) is one ACID transaction.

Locks: per-path writes take pg_advisory_xact_lock(hashtext(kb_id || path)) inside the transaction,
serializing concurrent writers to the same path without a lock table. Long-lived leases (for example
a resync holding a KB) are rows in `leases` with an expiry, acquired by a conditional upsert.
"""
from __future__ import annotations

from psycopg.types.json import Jsonb

from Loom.adapters.databricks.lakebase_conn import LakebaseConnector
from Loom.adapters.databricks.volume_store import VolumeStore
from Loom.ports import DiffEntry, FileVersion
from Loom.spaces.model import Member, Space, SpaceRecord
from pipelines.ingest_core import sha256


class InjectedFailure(RuntimeError):
    """Raised on purpose to prove rollback leaves no partial state."""


_FV = """SELECT f.file_id::text, f.kb_id, f.space_id, f.path, v.version, v.content_hash, v.size,
                v.inline_text IS NOT NULL, v.volume_path, v.created_by, v.created_at, v.content_type
         FROM loom.files f JOIN loom.file_versions v ON v.file_id = f.file_id"""


def _fv(r) -> FileVersion:
    return FileVersion(r[0], r[1], r[2], r[3], r[4], r[5], r[6], r[7], r[8], r[9], r[10], r[11])


class LakebaseStorage:
    def __init__(self, conn: LakebaseConnector, volume: VolumeStore, inline_max_bytes: int = 65536):
        self.conn, self.volume, self.inline_max = conn, volume, inline_max_bytes

    # ------------------------------------------------------------- KBs
    def create_kb(self, kb_id, name, space_id):
        with self.conn.tx() as c:
            c.execute("INSERT INTO loom.kbs (kb_id, name, space_id) VALUES (%s,%s,%s) ON CONFLICT DO NOTHING",
                      (kb_id, name, space_id))

    def add_kb_member(self, kb_id, principal, role):
        with self.conn.tx() as c:
            c.execute("""INSERT INTO loom.kb_members VALUES (%s,%s,%s)
                         ON CONFLICT (kb_id, principal) DO UPDATE SET role = EXCLUDED.role""", (kb_id, principal, role))

    def list_kb_members(self, kb_id):
        with self.conn.tx() as c:
            return [tuple(r) for r in c.execute(
                "SELECT principal, role FROM loom.kb_members WHERE kb_id = %s ORDER BY 1", (kb_id,))]

    # ------------------------------------------------------------- files
    def _payload(self, data: bytes) -> tuple[str | None, str | None]:
        """(inline_text, volume_path). The Volume write happens here, before the metadata transaction."""
        if len(data) <= self.inline_max:
            try:
                return data.decode("utf-8"), None
            except UnicodeDecodeError:
                pass
        return None, self.volume.put_blob(data)

    def put_file(self, kb_id, path, data, actor, content_type="text/markdown", _fail_after_volume=False):
        inline, vpath = self._payload(data)
        digest = sha256(data)
        with self.conn.tx() as c:
            c.execute("SELECT pg_advisory_xact_lock(hashtext(%s))", (f"{kb_id}|{path}",))
            row = c.execute("""
                INSERT INTO loom.files (kb_id, space_id, path) SELECT %s, space_id, %s FROM loom.kbs WHERE kb_id = %s
                ON CONFLICT (kb_id, path) DO UPDATE SET updated_at = now()
                RETURNING file_id::text, current_version, space_id""", (kb_id, path, kb_id)).fetchone()
            if row is None:
                raise KeyError(f"unknown kb {kb_id}")
            file_id, cur, _ = row
            version = cur + 1
            c.execute("""INSERT INTO loom.file_versions (file_id, version, content_hash, inline_text, volume_path,
                                                         size, content_type, created_by)
                         VALUES (%s,%s,%s,%s,%s,%s,%s,%s)""",
                      (file_id, version, digest, inline, vpath, len(data), content_type, actor))
            c.execute("UPDATE loom.files SET current_version = %s, deleted = false, updated_at = now() WHERE file_id = %s",
                      (version, file_id))
            if _fail_after_volume:
                raise InjectedFailure(f"injected failure after writing {kb_id}:{path} v{version}")
            r = c.execute(_FV + " WHERE f.file_id = %s AND v.version = %s", (file_id, version)).fetchone()
        return _fv(r)

    def stat(self, kb_id, path):
        with self.conn.tx() as c:
            r = c.execute(_FV + " WHERE f.kb_id = %s AND f.path = %s AND v.version = f.current_version AND NOT f.deleted",
                          (kb_id, path)).fetchone()
        return _fv(r) if r else None

    def read_file(self, kb_id, path, version=None):
        with self.conn.tx() as c:
            r = c.execute("""SELECT v.inline_text, v.volume_path FROM loom.files f
                             JOIN loom.file_versions v ON v.file_id = f.file_id
                             WHERE f.kb_id = %s AND f.path = %s AND v.version = coalesce(%s, f.current_version)""",
                          (kb_id, path, version)).fetchone()
        if r is None:
            raise FileNotFoundError(f"{kb_id}:{path}")
        return r[0].encode() if r[0] is not None else self.volume.get_blob(r[1])

    def list_files(self, kb_id, prefix=""):
        with self.conn.tx() as c:
            return [_fv(r) for r in c.execute(
                _FV + " WHERE f.kb_id = %s AND f.path LIKE %s AND v.version = f.current_version AND NOT f.deleted ORDER BY f.path",
                (kb_id, prefix + "%"))]

    def history(self, kb_id, path):
        with self.conn.tx() as c:
            return [_fv(r) for r in c.execute(_FV + " WHERE f.kb_id = %s AND f.path = %s ORDER BY v.version",
                                              (kb_id, path))]

    def delete_file(self, kb_id, path, actor):
        with self.conn.tx() as c:
            c.execute("SELECT pg_advisory_xact_lock(hashtext(%s))", (f"{kb_id}|{path}",))
            c.execute("UPDATE loom.files SET deleted = true, updated_at = now() WHERE kb_id = %s AND path = %s",
                      (kb_id, path))

    def file_id(self, kb_id, path) -> str | None:
        with self.conn.tx() as c:
            r = c.execute("SELECT file_id::text FROM loom.files WHERE kb_id = %s AND path = %s", (kb_id, path)).fetchone()
        return r[0] if r else None

    def deleted_files(self, kb_id) -> list[tuple[str, str]]:
        with self.conn.tx() as c:
            return [tuple(r) for r in c.execute(
                "SELECT file_id::text, path FROM loom.files WHERE kb_id = %s AND deleted", (kb_id,))]

    # ------------------------------------------------------------- snapshots, diff, restore
    def _manifest(self, c, kb_id) -> dict:
        return {r[0]: {"file_id": r[1], "version": r[2], "content_hash": r[3]} for r in c.execute(
            """SELECT f.path, f.file_id::text, f.current_version, v.content_hash FROM loom.files f
               JOIN loom.file_versions v ON v.file_id = f.file_id AND v.version = f.current_version
               WHERE f.kb_id = %s AND NOT f.deleted""", (kb_id,))}

    def snapshot(self, kb_id, label, actor):
        with self.conn.tx() as c:
            return c.execute("""INSERT INTO loom.snapshots (kb_id, label, created_by, manifest)
                                VALUES (%s,%s,%s,%s) RETURNING snapshot_id::text""",
                             (kb_id, label, actor, Jsonb(self._manifest(c, kb_id)))).fetchone()[0]

    @staticmethod
    def _diff(before: dict, after: dict) -> list[DiffEntry]:
        out = []
        for p in sorted(set(before) | set(after)):
            b, a = before.get(p, {}).get("content_hash"), after.get(p, {}).get("content_hash")
            if b != a:
                out.append(DiffEntry(p, "added" if b is None else "removed" if a is None else "modified", b, a))
        return out

    def _snap(self, c, snapshot_id) -> dict:
        r = c.execute("SELECT manifest FROM loom.snapshots WHERE snapshot_id = %s", (snapshot_id,)).fetchone()
        if r is None:
            raise KeyError(f"unknown snapshot {snapshot_id}")
        return r[0]

    def diff(self, kb_id, from_snapshot, to_snapshot=None):
        with self.conn.tx() as c:
            before = self._snap(c, from_snapshot)
            after = self._snap(c, to_snapshot) if to_snapshot else self._manifest(c, kb_id)
        return self._diff(before, after)

    def restore(self, kb_id, snapshot_id, actor):
        """Make the KB match the snapshot by appending new versions (history is preserved)."""
        with self.conn.tx() as c:
            target, current = self._snap(c, snapshot_id), self._manifest(c, kb_id)
            changes = self._diff(current, target)
            for d in changes:
                c.execute("SELECT pg_advisory_xact_lock(hashtext(%s))", (f"{kb_id}|{d.path}",))
                if d.change == "added" or d.change == "modified":   # restore the snapshot's version content
                    src = target[d.path]
                    nv = c.execute("""
                        INSERT INTO loom.file_versions (file_id, version, content_hash, inline_text, volume_path, size,
                                                        content_type, created_by)
                        SELECT v.file_id, f.current_version + 1, v.content_hash, v.inline_text, v.volume_path, v.size,
                               v.content_type, %s
                        FROM loom.file_versions v JOIN loom.files f ON f.file_id = v.file_id
                        WHERE v.file_id = %s AND v.version = %s RETURNING version""",
                                   (actor, src["file_id"], src["version"])).fetchone()[0]
                    c.execute("UPDATE loom.files SET current_version = %s, deleted = false, updated_at = now() WHERE file_id = %s",
                              (nv, src["file_id"]))
                else:   # present now, absent in snapshot
                    c.execute("UPDATE loom.files SET deleted = true, updated_at = now() WHERE kb_id = %s AND path = %s",
                              (kb_id, d.path))
        return changes

    # ------------------------------------------------------------- overlays
    def create_overlay(self, kb_id, name, actor):
        base = self.snapshot(kb_id, f"overlay-base:{name}", actor)
        with self.conn.tx() as c:
            return c.execute("""INSERT INTO loom.overlays (kb_id, name, base_snapshot_id, created_by)
                                VALUES (%s,%s,%s,%s) RETURNING overlay_id::text""", (kb_id, name, base, actor)).fetchone()[0]

    def overlay_put(self, overlay_id, path, data):
        inline, vpath = self._payload(data)
        with self.conn.tx() as c:
            c.execute("""INSERT INTO loom.overlay_entries VALUES (%s,%s,'put',%s,%s,%s,%s)
                         ON CONFLICT (overlay_id, path) DO UPDATE SET op='put', content_hash=EXCLUDED.content_hash,
                           inline_text=EXCLUDED.inline_text, volume_path=EXCLUDED.volume_path, size=EXCLUDED.size""",
                      (overlay_id, path, sha256(data), inline, vpath, len(data)))

    def overlay_delete(self, overlay_id, path):
        with self.conn.tx() as c:
            c.execute("""INSERT INTO loom.overlay_entries (overlay_id, path, op) VALUES (%s,%s,'delete')
                         ON CONFLICT (overlay_id, path) DO UPDATE SET op='delete', content_hash=NULL, inline_text=NULL,
                           volume_path=NULL, size=NULL""", (overlay_id, path))

    def overlay_read(self, overlay_id, path):
        """Read through the overlay: overlay entry if present, else the KB's current content."""
        with self.conn.tx() as c:
            r = c.execute("""SELECT e.op, e.inline_text, e.volume_path, o.kb_id FROM loom.overlays o
                             LEFT JOIN loom.overlay_entries e ON e.overlay_id = o.overlay_id AND e.path = %s
                             WHERE o.overlay_id = %s""", (path, overlay_id)).fetchone()
        if r is None:
            raise KeyError(overlay_id)
        if r[0] == "delete":
            return None
        if r[0] == "put":
            return r[1].encode() if r[1] is not None else self.volume.get_blob(r[2])
        return self.read_file(r[3], path) if self.stat(r[3], path) else None

    def merge_overlay(self, overlay_id, actor):
        with self.conn.tx() as c:
            o = c.execute("SELECT kb_id, status FROM loom.overlays WHERE overlay_id = %s FOR UPDATE",
                          (overlay_id,)).fetchone()
            if o is None or o[1] != "open":
                raise ValueError(f"overlay {overlay_id} not open")
            kb_id = o[0]
            before = self._manifest(c, kb_id)
            for op, path, h, inline, vpath, size in c.execute(
                    "SELECT op, path, content_hash, inline_text, volume_path, size FROM loom.overlay_entries "
                    "WHERE overlay_id = %s ORDER BY path", (overlay_id,)).fetchall():
                c.execute("SELECT pg_advisory_xact_lock(hashtext(%s))", (f"{kb_id}|{path}",))
                if op == "delete":
                    c.execute("UPDATE loom.files SET deleted = true, updated_at = now() WHERE kb_id = %s AND path = %s",
                              (kb_id, path))
                    continue
                fid, cur = c.execute("""
                    INSERT INTO loom.files (kb_id, space_id, path) SELECT %s, space_id, %s FROM loom.kbs WHERE kb_id = %s
                    ON CONFLICT (kb_id, path) DO UPDATE SET updated_at = now() RETURNING file_id::text, current_version""",
                                     (kb_id, path, kb_id)).fetchone()
                c.execute("""INSERT INTO loom.file_versions (file_id, version, content_hash, inline_text, volume_path, size, created_by)
                             VALUES (%s,%s,%s,%s,%s,%s,%s)""", (fid, cur + 1, h, inline, vpath, size, actor))
                c.execute("UPDATE loom.files SET current_version = %s, deleted = false WHERE file_id = %s", (cur + 1, fid))
            c.execute("UPDATE loom.overlays SET status = 'merged' WHERE overlay_id = %s", (overlay_id,))
            return self._diff(before, self._manifest(c, kb_id))

    # ------------------------------------------------------------- leases
    def acquire_lease(self, resource, holder, ttl_s):
        with self.conn.tx() as c:
            r = c.execute("""INSERT INTO loom.leases (resource, holder, expires_at)
                             VALUES (%s, %s, now() + make_interval(secs => %s))
                             ON CONFLICT (resource) DO UPDATE SET holder = EXCLUDED.holder, expires_at = EXCLUDED.expires_at
                             WHERE loom.leases.expires_at < now() OR loom.leases.holder = EXCLUDED.holder
                             RETURNING holder""", (resource, holder, ttl_s)).fetchone()
        return r is not None

    def release_lease(self, resource, holder):
        with self.conn.tx() as c:
            c.execute("DELETE FROM loom.leases WHERE resource = %s AND holder = %s", (resource, holder))


# ======================================================================= control plane
class LakebaseControl:
    def __init__(self, conn: LakebaseConnector):
        self.conn = conn

    def upsert_space(self, rec: SpaceRecord) -> bool:
        s = rec.space
        with self.conn.tx() as c:
            inserted = c.execute("""
                INSERT INTO loom.spaces (space_id, scope, name, owner_principal, config, ephemeral)
                VALUES (%s,%s,%s,%s,%s,%s)
                ON CONFLICT (space_id) DO UPDATE SET name = EXCLUDED.name, owner_principal = EXCLUDED.owner_principal,
                  config = EXCLUDED.config, ephemeral = EXCLUDED.ephemeral
                RETURNING (xmax = 0)""", (s.space_id, s.scope, s.name, s.owner_principal, Jsonb(s.config),
                                          s.ephemeral)).fetchone()[0]
            for t in ("space_members", "space_guardrails", "space_bindings"):
                c.execute(f"DELETE FROM loom.{t} WHERE space_id = %s", (s.space_id,))
            for m in rec.members:
                c.execute("INSERT INTO loom.space_members VALUES (%s,%s,%s,%s)", (s.space_id, m.principal, m.kind, m.perm))
            for direction, rules in rec.guardrails.items():
                c.execute("INSERT INTO loom.space_guardrails VALUES (%s,%s,%s)", (s.space_id, direction, Jsonb(rules)))
            for backend, ref in rec.bindings.items():
                c.execute("INSERT INTO loom.space_bindings VALUES (%s,%s,%s)", (s.space_id, backend, ref))
        return bool(inserted)

    def _load(self, c, where: str, params: tuple) -> list[SpaceRecord]:
        spaces = c.execute(f"SELECT space_id, scope, name, owner_principal, config, ephemeral FROM loom.spaces {where}",
                           params).fetchall()
        if not spaces:
            return []
        ids = [r[0] for r in spaces]
        mem, gr, bd = {}, {}, {}
        for sid, p, k, perm in c.execute("SELECT * FROM loom.space_members WHERE space_id = ANY(%s) ORDER BY 1,2,3", (ids,)):
            mem.setdefault(sid, []).append(Member(p, k, perm))
        for sid, d, rules in c.execute("SELECT * FROM loom.space_guardrails WHERE space_id = ANY(%s)", (ids,)):
            gr.setdefault(sid, {"read": {}, "write": {}})[d] = rules
        for sid, b, ref in c.execute("SELECT * FROM loom.space_bindings WHERE space_id = ANY(%s)", (ids,)):
            bd.setdefault(sid, {})[b] = ref
        return [SpaceRecord(Space(r[0], r[1], r[2], r[3], r[4] or {}, r[5]), mem.get(r[0], []),
                            gr.get(r[0], {"read": {}, "write": {}}), bd.get(r[0], {})) for r in spaces]

    def get_space(self, space_id):
        with self.conn.tx() as c:
            out = self._load(c, "WHERE space_id = %s", (space_id,))
        return out[0] if out else None

    def list_spaces(self):
        with self.conn.tx() as c:
            return self._load(c, "", ())

    def delete_space(self, space_id):
        with self.conn.tx() as c:
            c.execute("DELETE FROM loom.spaces WHERE space_id = %s", (space_id,))

    def write_audit(self, row):
        with self.conn.tx() as c:
            return c.execute("""INSERT INTO loom.memory_audit (request_id, caller, surface, action, space_id, decision, detail)
                                VALUES (%s,%s,%s,%s,%s,%s,%s) RETURNING event_id::text""",
                             (row["request_id"], row["caller"], row["surface"], row["action"], row["space_id"],
                              row["decision"], Jsonb(row["detail"]))).fetchone()[0]

    def write_audit_many(self, rows):
        with self.conn.tx() as c, c.cursor() as cur:
            cur.executemany("""INSERT INTO loom.memory_audit (request_id, caller, surface, action, space_id, decision, detail)
                               VALUES (%s,%s,%s,%s,%s,%s,%s)""",
                            [(r["request_id"], r["caller"], r["surface"], r["action"], r["space_id"], r["decision"],
                              Jsonb(r["detail"])) for r in rows])

    def _rows(self, sql, params=()):
        with self.conn.tx() as c:
            cur = c.execute(sql, params)
            cols = [d.name for d in cur.description]
            return [dict(zip(cols, r)) for r in cur.fetchall()]

    def audit_for(self, request_id):
        return self._rows("SELECT * FROM loom.memory_audit WHERE request_id = %s ORDER BY ts, event_id", (request_id,))

    def tail_audit(self, n=50):
        return self._rows("SELECT * FROM loom.memory_audit ORDER BY ts DESC LIMIT %s", (n,))

    def write_trace(self, row):
        with self.conn.tx() as c:
            c.execute("INSERT INTO loom.composition_traces (request_id, caller, surface, query, trace) VALUES (%s,%s,%s,%s,%s)",
                      (row["request_id"], row["caller"], row["surface"], row["query"], Jsonb(row["trace"])))

    def write_conflict(self, row):
        with self.conn.tx() as c:
            return c.execute("""INSERT INTO loom.conflict_decisions (subject, outcome, rule, winner, losers, rationale, flags)
                                VALUES (%s,%s,%s,%s,%s,%s,%s) RETURNING decision_id::text""",
                             (row["subject"], row["outcome"], row["rule"], Jsonb(row["winner"]), Jsonb(row["losers"]),
                              row["rationale"], Jsonb(row["flags"]))).fetchone()[0]

    def list_conflicts(self, n=50):
        return self._rows("SELECT * FROM loom.conflict_decisions ORDER BY ts DESC LIMIT %s", (n,))

    def create_promotion(self, row):
        with self.conn.tx() as c:
            return c.execute("""INSERT INTO loom.promotion_requests (proposer, source_space, target_space, kind, payload)
                                VALUES (%s,%s,%s,%s,%s) RETURNING request_id::text""",
                             (row["proposer"], row["source_space"], row["target_space"], row["kind"],
                              Jsonb(row["payload"]))).fetchone()[0]

    def get_promotion(self, request_id):
        rows = self._rows("SELECT *, request_id::text AS request_id FROM loom.promotion_requests WHERE request_id = %s",
                          (request_id,))
        return rows[0] if rows else None

    def decide_promotion(self, request_id, status, approver):
        with self.conn.tx() as c:
            c.execute("UPDATE loom.promotion_requests SET status = %s, approver = %s, decided_at = now() WHERE request_id = %s",
                      (status, approver, request_id))

    def list_promotions(self, status=None):
        return self._rows("SELECT *, request_id::text AS request_id FROM loom.promotion_requests "
                          "WHERE %s::text IS NULL OR status = %s ORDER BY ts DESC", (status, status))

    def write_feedback(self, row):
        with self.conn.tx() as c:
            c.execute("INSERT INTO loom.feedback (caller, query, source_ref, rating, comment) VALUES (%s,%s,%s,%s,%s)",
                      (row["caller"], row["query"], row["source_ref"], row["rating"], row.get("comment")))
