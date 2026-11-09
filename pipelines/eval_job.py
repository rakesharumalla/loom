"""MLflow evaluation of composed retrieval over the labeled question sets (step 11).

For each question, runs the full `retrieve` (composition + policy + guardrails + fusion) as the question's
persona and scores whether the labeled source is in the top k (hit@k) and its reciprocal rank (MRR).
Logs params, metrics, and a per-question table to an MLflow experiment in the workspace.
"""
from __future__ import annotations

import os
import sys
import time
from pathlib import Path

def _repo_root() -> Path:
    """Repo root (the folder holding Loom/). Serverless spark_python_task runs without __file__."""
    cands = []
    try:
        cands.append(Path(__file__).resolve().parents[1])
    except NameError:
        pass
    if sys.argv and sys.argv[0]:
        cands += [Path(sys.argv[0]).resolve().parent.parent, Path(sys.argv[0]).resolve().parent]
    cands += [Path.cwd(), Path.cwd().parent]
    return next((c for c in cands if (c / "Loom").is_dir()), cands[0])


ROOT = _repo_root()
sys.path.insert(0, str(ROOT))

import mlflow  # noqa: E402
import yaml  # noqa: E402
from databricks.sdk import WorkspaceClient  # noqa: E402

from config import CONFIG  # noqa: E402
from Loom.contract import RetrieveRequest  # noqa: E402

K = 5


def questions() -> list[dict]:
    data = ROOT / "data"
    out = [{"set": "sample", "q": q["q"], "persona": q["persona"], "space": q["expect"]["space"],
            "path": q["expect"]["path"]} for q in yaml.safe_load((data / "sample_questions.yaml").read_text())["questions"]]
    out += [{"set": "bench", "q": q["q"], "persona": q["persona"], "space": q["space"], "path": q["path"]}
            for q in yaml.safe_load((data / "bench_questions.yaml").read_text())["questions"]]
    return out


def main() -> int:
    from Loom.wiring import build
    from scripts.seed import identities
    in_db = "DATABRICKS_RUNTIME_VERSION" in os.environ
    w = WorkspaceClient() if in_db else WorkspaceClient(profile=CONFIG.profile)
    if not in_db:
        os.environ.setdefault("DATABRICKS_CONFIG_PROFILE", CONFIG.profile)
    mlflow.set_tracking_uri("databricks")
    mlflow.set_experiment(f"/Users/{w.current_user.me().user_name}/loom-retrieval-eval")
    svc, _, raw = build(w, index_now=False)
    ids = identities(w)
    rows = []
    for q in questions():
        t = time.perf_counter()
        r = svc.retrieve(ids[q["persona"]], RetrieveRequest(query=q["q"], k=K), "job")
        ms = (time.perf_counter() - t) * 1000
        refs = [(x.provenance.space_id, x.provenance.path) for x in r.results]
        rank = next((i + 1 for i, ref in enumerate(refs) if ref == (q["space"], q["path"])), None)
        rows.append({**q, "rank": rank, "hit": int(rank is not None), "rr": 1 / rank if rank else 0.0,
                     "latency_ms": round(ms), "included_spaces": ",".join(v.space_id for v in r.trace.considered if v.included)})
    with mlflow.start_run(run_name=f"loom-eval-{time.strftime('%Y%m%d-%H%M')}"):
        mlflow.log_params({"k": K, "embedding_endpoint": CONFIG.embedding_endpoint, "rrf_k": 60,
                           "candidates_per_list": raw["search"].candidates, "prefilter": raw["search"].prefilter,
                           "client": "databricks-job" if in_db else "laptop"})
        for s in ("sample", "bench", "all"):
            sub = [x for x in rows if s == "all" or x["set"] == s]
            mlflow.log_metric(f"{s}_hit_at_{K}", sum(x["hit"] for x in sub) / len(sub))
            mlflow.log_metric(f"{s}_mrr", sum(x["rr"] for x in sub) / len(sub))
        lat = sorted(x["latency_ms"] for x in rows)
        mlflow.log_metric("retrieve_p50_ms", lat[len(lat) // 2])
        mlflow.log_metric("retrieve_p95_ms", lat[int(0.95 * (len(lat) - 1))])
        mlflow.log_table({k: [x[k] for x in rows] for k in rows[0]}, "per_question.json")
        run = mlflow.active_run().info.run_id
    for s in ("sample", "bench"):
        sub = [x for x in rows if x["set"] == s]
        print(f"{s}: hit@{K} {sum(x['hit'] for x in sub)}/{len(sub)}, MRR {sum(x['rr'] for x in sub) / len(sub):.3f}")
    print(f"retrieve p50 {lat[len(lat) // 2]} ms, p95 {lat[int(0.95 * (len(lat) - 1))]} ms "
          f"({'in-region job' if in_db else 'laptop client'}); MLflow run {run}")
    misses = [x for x in rows if not x["hit"]]
    for m in misses:
        print(f"  miss: [{m['persona']}] {m['q']} -> expected {m['space']}:{m['path']}")
    return 0


if __name__ == "__main__":
    rc = main()
    if rc:   # serverless job runners treat any SystemExit, even 0, as a failure
        sys.exit(rc)
