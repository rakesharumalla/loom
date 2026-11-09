"""Load the synthetic demo data through Loom: spaces via configure, facts via contribute,
corpus files via the ingest path (StoragePort + Ingestor), the same path a source sync uses."""
from __future__ import annotations

from pathlib import Path

import yaml

from Loom.contract import ConfigureRequest, ContributeRequest
from Loom.service import LoomService
from Loom.spaces.model import Identity
from pipelines.ingest_core import Ingestor

DATA = Path(__file__).resolve().parents[1] / "data"


def load_yaml(name: str) -> dict:
    return yaml.safe_load((DATA / name).read_text())


def seed_spaces(svc: LoomService, admin: Identity, surface: str = "job") -> list[str]:
    out = []
    for spec in load_yaml("spaces_seed.yaml")["spaces"]:
        out.append(svc.configure(admin, ConfigureRequest(**spec), surface).space_id)
    return out


def seed_corpus(svc: LoomService, ingestor: Ingestor, actor: str = "seed", root: Path | None = None,
                workers: int = 1) -> list:
    """Load files (only changed ones get a new version) and index them (only changed chunks re-embed)."""
    spaces = load_yaml("memory_seed.yaml")["corpus_spaces"]
    root = root or DATA / "corpus"
    jobs = []
    for d, space_id in spaces.items():
        if not (root / d).exists():
            continue
        kb = svc.registry.get(space_id).bindings["kb"]
        jobs += [(kb, str(f.relative_to(root / d)), f) for f in sorted((root / d).rglob("*.md"))]

    def one(job):
        kb, path, f = job
        data = f.read_bytes()
        cur = svc.ports.storage.stat(kb, path)
        if cur is None or cur.content_hash != _sha(data):
            svc.ports.storage.put_file(kb, path, data, actor)
        return ingestor.index_file(kb, path)

    if workers <= 1:
        return [one(j) for j in jobs]
    from concurrent.futures import ThreadPoolExecutor
    with ThreadPoolExecutor(workers) as ex:
        return list(ex.map(one, jobs))


def seed_facts(svc: LoomService, identities: dict[str, Identity], surface: str = "job") -> list:
    from Loom.control.composer import FACT_PREFIX, memory_actor
    from Loom.control.conflict import normalize_subject
    out = []
    for f in load_yaml("memory_seed.yaml")["facts"]:
        rec, who = svc.registry.get(f["target"]), identities[f["as"]]
        existing = svc.ports.memory.list(rec.bindings["memory"], memory_actor(rec, who),
                                         path_prefix=f"{FACT_PREFIX}{normalize_subject(f['subject'])}/")
        if any(e.content == f["content"] for e in existing):
            continue   # idempotent re-seed
        req = ContributeRequest(content=f["content"], kind="fact", target_space=f["target"],
                                subject=f["subject"], observed_at=f["observed_at"])
        out.append(svc.contribute(identities[f["as"]], req, surface))
    return out


def _sha(b: bytes) -> str:
    from pipelines.ingest_core import sha256
    return sha256(b)


def seed_transcripts(svc: LoomService) -> list[str]:
    """Write the synthetic incident transcripts into each actor's managed session store (stand-in for the
    agent runtime that records sessions). Idempotent: existing sessions are left as they are."""
    out = []
    for t in load_yaml("transcripts.yaml")["transcripts"]:
        rec = svc.registry.get(f"personal:{t['actor']}")
        name = svc.ports.sessions.open(rec.bindings["sessions"], t["actor"], t["session_id"],
                                       metadata={"kind": "incident_transcript"})
        if not svc.ports.sessions.items(name):
            svc.ports.sessions.append(name, [{"type": "message", **turn} for turn in t["turns"]])
        out.append(name)
    return out
