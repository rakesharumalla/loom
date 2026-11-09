from Loom.contract import RetrieveRequest
from Loom.seed import seed_transcripts
from pipelines import distill_job

LESSON = {"lessons": [{"title": "Withdrawal queue stuck after node upgrade", "slug": "withdrawal-queue-node-upgrade",
                       "symptoms": ["withdrawal queue depth grows after a node upgrade", "signer RPC timeouts"],
                       "resolution_steps": ["Confirm the node is synced", "Restart the withdrawal batcher",
                                            "Drain the queue at 200 requests per minute"],
                       "root_cause": "The batcher caches the node RPC endpoint at startup.",
                       "source_sessions": ["inc-2026-08-03-bob", "inc-2026-09-09-bob"], "confidence": 0.9,
                       "proposed_scope": "team:care"}],
          "facts": [{"subject": "withdrawal_batcher.rpc_cache", "statement": "The withdrawal batcher caches the node RPC endpoint at startup.",
                     "source_sessions": ["inc-2026-09-09-bob"]}]}


def test_distill_to_staging_then_promotion_then_fresh_session(svc, ids):
    names = seed_transcripts(svc)
    assert len(names) == 5
    seen = {}
    rep = distill_job.run(svc=svc, service=ids["service"], llm=lambda p: (seen.setdefault("p", p), LESSON)[1])
    assert rep["transcripts"] == 5 and "inc-2026-09-30-carol" in seen["p"]
    les = rep["lessons"][0]
    assert les["status"] == "promotion_pending" and les["ref"].startswith("kb-personal-service:")
    q = RetrieveRequest(query="withdrawal queue stuck after node upgrade signer timeouts", session_id="fresh-1")
    assert not any("Lesson:" in x.content for x in svc.retrieve(ids["bob"], q, "mcp").results)
    svc.decide_promotion(ids["bob"], les["promotion_request_id"], True, "web")
    r = svc.retrieve(ids["bob"], q, "mcp")
    hit = next(x for x in r.results if "Lesson:" in x.content)
    assert hit.provenance.space_id == "team:care" and hit.provenance.authority == "inferred"
    assert hit.provenance.extra["source_sessions"] == LESSON["lessons"][0]["source_sessions"]
    assert rep["facts"][0]["status"] == "stored"
