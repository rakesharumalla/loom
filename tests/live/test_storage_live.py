"""Step 3 acceptance: transactional file writes (Volume + Lakebase), rollback proof, snapshot/diff/restore,
overlays and leases on a scratch KB in Lakebase."""
import os
import uuid

import pytest

from Loom.adapters.databricks.lakebase_store import InjectedFailure

KB = f"kb-livetest-{uuid.uuid4().hex[:8]}"


@pytest.fixture(scope="module")
def storage(live):
    _, _, raw = live
    st, conn = raw["storage"], raw["conn"]
    with conn.tx() as c:
        c.execute("INSERT INTO loom.spaces (space_id, scope, name, owner_principal) VALUES (%s,'project','live test','admin')",
                  (f"project:{KB}",))
    st.create_kb(KB, "live test", f"project:{KB}")
    yield st
    with conn.tx() as c:
        c.execute("DELETE FROM loom.spaces WHERE space_id = %s", (f"project:{KB}",))
        c.execute("DELETE FROM loom.kbs WHERE kb_id = %s", (KB,))


def counts(conn, path):
    with conn.tx() as c:
        f = c.execute("SELECT count(*) FROM loom.files WHERE kb_id=%s AND path=%s", (KB, path)).fetchone()[0]
        v = c.execute("""SELECT count(*) FROM loom.file_versions v JOIN loom.files f USING (file_id)
                         WHERE f.kb_id=%s AND f.path=%s""", (KB, path)).fetchone()[0]
    return f, v


def test_small_inline_and_large_volume(storage, live):
    small = storage.put_file(KB, "notes/small.md", b"# small\ninline text", "admin")
    assert small.inline and small.volume_path is None and small.version == 1
    big = os.urandom(200_000)
    fv = storage.put_file(KB, "raw/big.bin", big, "admin", content_type="application/octet-stream")
    assert not fv.inline and fv.volume_path.startswith("/Volumes/") and fv.content_hash in fv.volume_path
    assert storage.read_file(KB, "raw/big.bin") == big
    # write-once / content-addressed: same bytes -> same blob path, new version row
    fv2 = storage.put_file(KB, "raw/big.bin", big, "admin", content_type="application/octet-stream")
    assert fv2.volume_path == fv.volume_path and fv2.version == 2


def test_rollback_leaves_no_partial_state(storage, live):
    conn = live[2]["conn"]
    payload = os.urandom(120_000)                       # large: goes to the Volume first
    with pytest.raises(InjectedFailure):
        storage.put_file(KB, "raw/fail.bin", payload, "admin", _fail_after_volume=True)
    assert counts(conn, "raw/fail.bin") == (0, 0)      # no files row, no version row
    assert storage.stat(KB, "raw/fail.bin") is None
    # existing file: failed v2 must not change current_version
    storage.put_file(KB, "notes/keep.md", b"v1", "admin")
    with pytest.raises(InjectedFailure):
        storage.put_file(KB, "notes/keep.md", b"v2 never lands", "admin", _fail_after_volume=True)
    assert storage.stat(KB, "notes/keep.md").version == 1 and storage.read_file(KB, "notes/keep.md") == b"v1"
    assert counts(conn, "notes/keep.md") == (1, 1)
    # the orphaned blob is unreferenced (GC-able), never pointed at by metadata
    blob = live[2]["storage"].volume.blob_path(__import__("pipelines.ingest_core", fromlist=["x"]).sha256(payload))
    assert live[2]["storage"].volume.exists(blob)


def test_snapshot_diff_restore(storage):
    storage.put_file(KB, "doc/a.md", b"A1", "admin")
    storage.put_file(KB, "doc/b.md", b"B1", "admin")
    snap = storage.snapshot(KB, "before-edit", "admin")
    storage.put_file(KB, "doc/a.md", b"A2", "admin")
    storage.delete_file(KB, "doc/b.md", "admin")
    storage.put_file(KB, "doc/c.md", b"C1", "admin")
    d = {e.path: e.change for e in storage.diff(KB, snap)}
    assert d == {"doc/a.md": "modified", "doc/b.md": "removed", "doc/c.md": "added"}
    restored = {e.path: e.change for e in storage.restore(KB, snap, "admin")}
    assert restored == {"doc/a.md": "modified", "doc/b.md": "added", "doc/c.md": "removed"}
    assert storage.diff(KB, snap) == []
    assert storage.read_file(KB, "doc/a.md") == b"A1" and storage.stat(KB, "doc/a.md").version == 3
    assert [v.version for v in storage.history(KB, "doc/a.md")] == [1, 2, 3]


def test_overlay_and_lease(storage):
    storage.put_file(KB, "ov/x.md", b"base", "admin")
    ov = storage.create_overlay(KB, "draft", "alice")
    storage.overlay_put(ov, "ov/x.md", b"draft edit")
    storage.overlay_put(ov, "ov/new.md", b"new in overlay")
    assert storage.read_file(KB, "ov/x.md") == b"base"            # base untouched until merge
    assert storage.overlay_read(ov, "ov/x.md") == b"draft edit"
    changes = {e.path: e.change for e in storage.merge_overlay(ov, "alice")}
    assert changes == {"ov/x.md": "modified", "ov/new.md": "added"}
    assert storage.acquire_lease(f"{KB}:resync", "worker-1", 30)
    assert not storage.acquire_lease(f"{KB}:resync", "worker-2", 30)
    storage.release_lease(f"{KB}:resync", "worker-1")
    assert storage.acquire_lease(f"{KB}:resync", "worker-2", 1)
    storage.release_lease(f"{KB}:resync", "worker-2")
