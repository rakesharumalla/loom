"""Distillation demo (step 9): run the job, approve the lesson's promotion as Bob, then a FRESH agent
session asks about the symptom and resolves it from the lesson (without the original transcripts)."""
import json
import sys
import time
import uuid
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from databricks.sdk import WorkspaceClient  # noqa: E402

from config import CONFIG  # noqa: E402
from Loom.contract import RetrieveRequest  # noqa: E402
from Loom.wiring import build  # noqa: E402
from pipelines import distill_job  # noqa: E402
from scripts.seed import identities  # noqa: E402

w = WorkspaceClient(profile=CONFIG.profile)
from Loom.spaces import policy  # noqa: E402

svc, _, _ = build(w)
ids = identities(w)
if "--approve-pending" not in sys.argv:
    rep = distill_job.run(w, compare_managed="--compare-managed" in sys.argv)
    print(json.dumps(rep, indent=2, default=str))
pending = [p for p in svc.ports.control.list_promotions("pending") if p["proposer"] == "service" and p["kind"] == "lesson"]
for p in pending:
    target = svc.registry.get(p["target_space"])
    # the demo picks a human approver who holds write on the target (the proposer cannot approve)
    approver = next(n for n in ("bob", "alice", "carol") if policy.decide(ids[n], target, "write").allowed)
    d = svc.decide_promotion(ids[approver], p["request_id"], True, "web")
    print(f"[distill] {approver} approved promotion -> {d.target_space}: {d.ref}")
session = f"fresh-{uuid.uuid4().hex[:6]}"
t = time.perf_counter()
r = svc.retrieve(ids["bob"], RetrieveRequest(query="ETH withdrawal queue is stuck after we upgraded a node and the "
                                             "signer is timing out, what do I do?", k=5, session_id=session), "mcp")
print(f"[distill] fresh session {session}: retrieve {(time.perf_counter() - t) * 1000:.0f} ms (client-side)")
for x in r.results:
    print(" ", x.provenance.backend, x.provenance.space_id, x.provenance.path, x.provenance.authority,
          x.provenance.extra.get("source_sessions"))
lesson = next((x for x in r.results if "Lesson:" in x.content), None)
print("\n[distill] top lesson returned:\n" + (lesson.content[:900] if lesson else "NONE"))
