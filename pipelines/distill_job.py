"""Semi-offline distillation (data plane): session transcripts -> facts + markdown incident lessons.

Reads transcripts from managed agent sessions (they are listable; no synthetic transcripts table is needed).
A hosted LLM (default databricks-claude-sonnet-5-5) extracts (a) facts with provenance and (b) non-code
lessons such as incident resolutions, written as markdown with front matter.

Nothing reaches a shared space directly: output is contributed through LoomService (policy, guardrails,
conflict resolver) into the batch identity's staging space `personal:service`, with a promotion request
to the proposed scope. A human approver with write on the target applies it.
Implemented in the pipeline; the LLM is a Databricks building block.
"""
from __future__ import annotations

import argparse
import json
import re
import sys
import time
from datetime import datetime, timezone
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

import yaml  # noqa: E402
from databricks.sdk import WorkspaceClient  # noqa: E402
from databricks.sdk.service.serving import ChatMessage, ChatMessageRole  # noqa: E402

from config import CONFIG  # noqa: E402
from Loom.contract import ContributeRequest  # noqa: E402

STAGING = "personal:service"
PROMPT = """You distill support/incident transcripts into reusable knowledge for AI agents.
Transcripts (JSON): {transcripts}

Return ONLY a JSON object with:
- "lessons": list of recurring incidents seen in 2+ sessions, each with
  "title" (short), "slug" (kebab-case), "symptoms" (list of strings), "resolution_steps" (ordered list),
  "root_cause" (string), "source_sessions" (list of session ids that support it),
  "confidence" (0..1, based on how consistently the sessions agree), "proposed_scope" (one of {scopes}).
- "facts": list of durable facts, each with "subject" (dotted lowercase key), "statement" (one sentence),
  "source_sessions" (list).
Do not invent details that are not in the transcripts. No account numbers or personal data."""


def read_transcripts(svc) -> list[dict]:
    out = []
    for rec in svc.registry.all():
        store = rec.bindings.get("sessions")
        if rec.space.scope != "personal" or not store:
            continue
        for s in svc.ports.sessions.list_sessions(store):
            if (s.get("metadata") or {}).get("kind") != "incident_transcript":
                continue
            items = svc.ports.sessions.items(s["name"])
            out.append({"session_id": s["session_id"], "actor": s["actor_id"], "space": rec.space_id,
                        "turns": [{"role": i.get("role"), "content": i.get("content")} for i in items]})
    return sorted(out, key=lambda t: t["session_id"])


def llm_json(w: WorkspaceClient, prompt: str) -> dict:
    r = w.serving_endpoints.query(name=CONFIG.llm_endpoint, max_tokens=3000,  # sonnet-5-5 rejects temperature
                                  messages=[ChatMessage(role=ChatMessageRole.USER, content=prompt)])
    text = r.choices[0].message.content
    if isinstance(text, list):  # some endpoints return content blocks
        text = "".join(b.get("text", "") if isinstance(b, dict) else str(b) for b in text)
    m = re.search(r"\{.*\}", text, re.S)
    return json.loads(m.group(0) if m else text)


def lesson_markdown(lesson: dict, created_at: str) -> str:
    fm = {"title": lesson["title"], "symptoms": lesson["symptoms"], "resolution_steps": lesson["resolution_steps"],
          "root_cause": lesson.get("root_cause", ""), "source_sessions": lesson["source_sessions"],
          "authority": "inferred", "created_at": created_at, "confidence": lesson["confidence"],
          "proposed_scope": lesson["proposed_scope"], "distilled_by": CONFIG.llm_endpoint}
    steps = "\n".join(f"{i}. {s}" for i, s in enumerate(lesson["resolution_steps"], 1))
    symptoms = "\n".join(f"- {s}" for s in lesson["symptoms"])
    return (f"---\n{yaml.safe_dump(fm, sort_keys=False)}---\n# Lesson: {lesson['title']}\n\n"
            f"Distilled from {len(lesson['source_sessions'])} sessions ({', '.join(lesson['source_sessions'])}). "
            f"Authority: inferred (confidence {lesson['confidence']}).\n\n## Symptoms\n{symptoms}\n\n"
            f"## Root cause\n{lesson.get('root_cause', '')}\n\n## Resolution\n{steps}\n")


def run(w: WorkspaceClient | None = None, compare_managed: bool = False, svc=None, service=None, llm=None) -> dict:
    if svc is None:
        from Loom.wiring import build
        from scripts.seed import identities
        w = w or WorkspaceClient(profile=CONFIG.profile)
        svc, _, _ = build(w)
        service = identities(w)["service"]
    llm = llm or (lambda prompt: llm_json(w, prompt))
    t0 = time.perf_counter()
    transcripts = read_transcripts(svc)
    scopes = [r.space_id for r in svc.registry.all() if r.space.scope in ("team", "project", "company")]
    t = time.perf_counter()
    out = llm(PROMPT.format(transcripts=json.dumps(transcripts), scopes=scopes))
    llm_s = time.perf_counter() - t
    created = datetime.now(timezone.utc).isoformat(timespec="seconds")
    report = {"transcripts": len(transcripts), "llm_s": round(llm_s, 2), "lessons": [], "facts": []}
    for lesson in out.get("lessons", []):
        slug = re.sub(r"[^a-z0-9-]+", "-", lesson.get("slug") or lesson["title"].lower()).strip("-")
        target = lesson["proposed_scope"] if lesson["proposed_scope"] in scopes else None
        resp = svc.contribute(service, ContributeRequest(content=lesson_markdown(lesson, created), kind="lesson",
                                                         target_space=STAGING, path=f"lessons/{slug}.md",
                                                         propose_promotion_to=target), "job", _origin="inferred")
        report["lessons"].append({"title": lesson["title"], "status": resp.status, "ref": resp.ref,
                                  "proposed_scope": target, "promotion_request_id": resp.promotion_request_id,
                                  "source_sessions": lesson["source_sessions"], "confidence": lesson["confidence"],
                                  "reason": resp.reason})
    for fact in out.get("facts", []):
        resp = svc.contribute(service, ContributeRequest(content=fact["statement"], kind="fact", target_space=STAGING,
                                                         subject=fact["subject"]), "job", _origin="inferred")
        report["facts"].append({"subject": fact["subject"], "status": resp.status,
                                "conflict": (resp.conflict or {}).get("rule"), "sources": fact.get("source_sessions")})
    if compare_managed:
        report["managed_extract_memories"] = managed_compare(w, svc, transcripts)
    report["job_s"] = round(time.perf_counter() - t0, 2)
    return report


def managed_compare(w, svc, transcripts) -> list:
    """Optional: Databricks managed extract_memories with dry_run=True (writes nothing). Shown for comparison;
    a non-dry run would write straight into a store and bypass Loom policy, so the demo never does that."""
    out = []
    for t in transcripts[:1]:
        rec = svc.registry.get(t["space"])
        try:
            r = w.agent_kit.extract_memories(session_store=f"session-stores/{rec.bindings['sessions']}",
                                             session_id=t["session_id"],
                                             memory_store=f"memory-stores/{rec.bindings['memory']}", dry_run=True)
            out.append({"session": t["session_id"], "result": str(r)[:800]})
        except Exception as e:
            out.append({"session": t["session_id"], "error": str(e)[:300]})
    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--compare-managed", action="store_true")
    args, _ = ap.parse_known_args()
    import os
    w = WorkspaceClient() if "DATABRICKS_RUNTIME_VERSION" in os.environ else None
    print(json.dumps(run(w, args.compare_managed), indent=2, default=str))
    return 0


if __name__ == "__main__":
    rc = main()
    if rc:   # serverless job runners treat any SystemExit, even 0, as a failure
        sys.exit(rc)
