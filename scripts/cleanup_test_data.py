"""Remove entries created by live tests / UI tries (subjects ending in a random hex tag) from Loom memory stores.
Seeded demo facts are kept. Run: uv run python scripts/cleanup_test_data.py"""
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from databricks.sdk import WorkspaceClient  # noqa: E402

from config import CONFIG  # noqa: E402
from Loom.adapters.databricks.memory import ManagedMemory  # noqa: E402
from Loom.control.composer import FACT_PREFIX  # noqa: E402
from Loom.spaces.model import SHARED_ACTOR  # noqa: E402
from Loom.wiring import build  # noqa: E402

TEST_SUBJECT = re.compile(r"(\.[0-9a-f]{4,6}|^preference\.summary_format\.[0-9a-z]{4}|\.zz\d+)$")  # live tests + UI tries
w = WorkspaceClient(profile=CONFIG.profile)
svc, _, _ = build(w, index_now=False)
mem = ManagedMemory(w)
n = 0
for rec in svc.registry.all():
    store = rec.bindings.get("memory")
    if not store:
        continue
    actor = rec.space.key if rec.space.scope == "personal" else SHARED_ACTOR
    for e in mem.list(store, actor, path_prefix=FACT_PREFIX):
        subject = e.path[len(FACT_PREFIX):].split("/")[0]
        if TEST_SUBJECT.search(subject):
            mem.delete(e.name)
            n += 1
print(f"removed {n} test entries")
