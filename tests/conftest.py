import pytest

from Loom.service import LoomService, Ports
from Loom.seed import seed_corpus, seed_facts, seed_spaces
from Loom.spaces.model import Identity
from pipelines.ingest_core import Ingestor
from tests import fakes
from tests.fakes import FakeControl, FakeEmbedder, FakeMemory, FakeSearch, FakeSessions, FakeStorage

IDS = {
    "alice": Identity("alice", "app-alice", ("loom-compliance",)),
    "bob": Identity("bob", "app-bob", ("loom-support",)),
    "carol": Identity("carol", "app-carol", ("loom-trading-ops",)),
    "admin": Identity("admin", "app-admin", (), is_platform_admin=True),
    "service": Identity("service", "app-service", ()),
}


def build_service() -> LoomService:
    fakes.reset()
    ports = Ports(FakeControl(), FakeStorage(), FakeSearch(), FakeMemory(), FakeSessions())
    svc = LoomService(ports)
    ing = Ingestor(ports.storage, ports.search, FakeEmbedder(), svc.access_for_space)
    svc.indexer = lambda kb, path: ing.index_file(kb, path)
    seed_spaces(svc, IDS["admin"])
    seed_corpus(svc, ing)
    seed_facts(svc, IDS)
    return svc


@pytest.fixture
def svc():
    return build_service()


@pytest.fixture
def ids():
    return IDS
