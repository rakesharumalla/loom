"""Step 10: ephemeral project space on a Lakebase branch + its own memory store; scale-to-zero cold vs warm.

Prints measured timings only (client-side, from wherever this runs). Documented limits are quoted with links.
Run: uv run python scripts/ephemeral_demo.py [--cold-samples 2]
"""
from __future__ import annotations

import argparse
import sys
import time
import uuid
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from databricks.sdk import WorkspaceClient  # noqa: E402
from databricks.sdk.service import postgres as pg  # noqa: E402
from databricks.sdk.common.types.fieldmask import FieldMask  # noqa: E402
from google.protobuf.duration_pb2 import Duration  # noqa: E402

from config import CONFIG  # noqa: E402
from Loom.adapters.databricks.lakebase_conn import LakebaseConnector  # noqa: E402
from Loom.contract import ConfigureRequest, RetrieveRequest  # noqa: E402
from Loom.wiring import build  # noqa: E402
from scripts.seed import identities  # noqa: E402

LIMITS = """Documented limits (https://docs.databricks.com/aws/en/oltp/projects/manage-projects#limits):
  branches per project 500; Postgres databases per branch 500; roles per branch 500;
  concurrently active computes 20; read replicas per branch 6; projects per workspace 1000;
  branch TTL up to 30 days. Scale-to-zero timeout 60 s .. 7 days
  (https://docs.databricks.com/aws/en/oltp/projects/scale-to-zero).
  Managed memory: no documented store limit (https://docs.databricks.com/aws/en/agents/agent-memory/managed-memory)."""


def t(label, f, out):
    s = time.perf_counter()
    r = f()
    out[label] = round(time.perf_counter() - s, 2)
    print(f"[eph] {label}: {out[label]} s")
    return r


def state(w, ep):
    st = w.postgres.get_endpoint(ep).status
    return str(getattr(st, "current_state", "") or "")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--cold-samples", type=int, default=2)
    args = ap.parse_args()
    w = WorkspaceClient(profile=CONFIG.profile)
    proj = f"projects/{CONFIG.lakebase_project}"
    bid = f"eph-{uuid.uuid4().hex[:6]}"
    branch = f"{proj}/branches/{bid}"
    ep = None
    timings: dict[str, float] = {}
    ids = identities(w)
    print(LIMITS)
    try:
        t("branch_create", lambda: w.postgres.create_branch(parent=proj, branch_id=bid, branch=pg.Branch(spec=pg.BranchSpec(
            source_branch=CONFIG.branch_path, ttl=Duration(seconds=7200)))).wait(), timings)
        # creating a branch provisions its read-write compute; set the documented minimum suspend timeout
        ep = list(w.postgres.list_endpoints(parent=branch))[0].name
        t("compute_set_suspend_60s", lambda: w.postgres.update_endpoint(
            name=ep, endpoint=pg.Endpoint(spec=pg.EndpointSpec(endpoint_type=pg.EndpointType.ENDPOINT_TYPE_READ_WRITE,
                                                     suspend_timeout_duration=Duration(seconds=60))),
            update_mask=FieldMask(field_mask=["spec.suspend_timeout_duration"])).wait(), timings)
        svc, ingestor, raw = build(w, branch_path=branch)
        n_prod = t("first_query_on_branch", lambda: _count(raw["conn"]), timings)
        print(f"[eph] branch inherited {n_prod} chunks from production (copy-on-write, nothing copied)")
        key = bid
        req = ConfigureRequest(space={"scope": "project", "key": key, "name": f"Ephemeral {key}", "owner": "alice",
                                      "ephemeral": True},
                               members=[{"principal": "alice", "kind": "user", "perm": "write"},
                                        {"principal": "bob", "kind": "user", "perm": "read"}])
        resp = t("configure_space_and_memory_store", lambda: svc.configure(ids["admin"], req, "job"), timings)
        kb = resp.bindings["kb"]
        docs = sorted((ROOT / "data" / "corpus_gen" / "project-atlas").rglob("*.md"))[:10]

        def load():
            for f in docs:
                svc.ports.storage.put_file(kb, f"subset/{f.name}", f.read_bytes(), "alice")
                ingestor.index_file(kb, f"subset/{f.name}")
        t("load_subset_10_docs", load, timings)
        from pipelines.ingest_core import parse_front_matter
        meta, body = parse_front_matter(docs[0].read_text())
        q = RetrieveRequest(query=f"{meta.get('title', docs[0].stem)} retries signing requests", k=3)
        r = t("query", lambda: svc.retrieve(ids["alice"], q, "job"), timings)
        print(f"[eph] top hit: {r.results[0].provenance.space_id} {r.results[0].provenance.path}")
        prod_svc, _, _ = build(w)
        assert prod_svc.registry.get(f"project:{key}") is None
        print("[eph] production registry does not contain the ephemeral space")

        # scale-to-zero on the ephemeral compute (60 s timeout = documented minimum)
        warm = []
        for _ in range(3):
            s = time.perf_counter()
            _select1(raw["conn"])
            warm.append(round((time.perf_counter() - s) * 1000))
        print(f"[eph] warm new-connection SELECT 1 ms: {warm}")
        cold = []
        for i in range(args.cold_samples):
            waited = 0
            time.sleep(70)   # > 60 s suspend timeout
            waited = 70
            while not any(k in state(w, ep).upper() for k in ("IDLE", "SUSPEND")) and waited < 300:
                time.sleep(10)
                waited += 10
            print(f"[eph] compute state after {waited + 0}s idle wait: {state(w, ep)}")
            s = time.perf_counter()
            _select1(raw["conn"])
            cold.append(round((time.perf_counter() - s) * 1000))
            print(f"[eph] cold sample {i + 1}: first SELECT 1 on a suspended compute {cold[-1]} ms")
        timings["warm_select1_ms_median"] = sorted(warm)[len(warm) // 2]
        timings["cold_select1_ms"] = cold
    finally:
        try:
            from Loom.adapters.databricks.memory import ManagedMemory
            from Loom.spaces.model import store_name_for
            t("teardown_memory_store", lambda: ManagedMemory(w).delete_store(store_name_for(f"project:{bid}")), timings)
        except Exception as e:
            print(f"[eph] memory store delete: {e}")
        try:
            t("teardown_branch", lambda: w.postgres.delete_branch(branch).wait(), timings)
        except Exception as e:
            print(f"[eph] branch delete: {e}")
    print("\n[eph] summary (client-side seconds unless noted):", timings)
    return 0


def _count(conn) -> int:
    with conn.tx() as c:
        c.execute("SET LOCAL ROLE loom_writer")
        c.execute("SELECT set_config('loom.writer','on',true)")
        return c.execute("SELECT count(*) FROM loom.chunks").fetchone()[0]


def _select1(conn: LakebaseConnector) -> None:
    with conn.connect(autocommit=True) as c:   # new connection each time (the pool would hide reconnects)
        c.execute("SELECT 1")


if __name__ == "__main__":
    sys.exit(main())
