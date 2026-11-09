"""Parse -> chunk -> embed -> upsert, hash-based and incremental. Used by the ingest job and by the
facade when a contribution should be searchable immediately. Implemented in the pipeline.

Chunk size: databricks-gte-large-en accepts 512 model tokens, so chunks target ~500 tokens with
~50 overlap, approximated as 350 / 35 whitespace words (about 1.4 model tokens per English word).
"""
from __future__ import annotations

import hashlib
import re
import time
from dataclasses import dataclass, field
from typing import Callable, Protocol

import yaml

from Loom.ports import Chunk, SearchPort, StoragePort

CHUNK_WORDS = 350
OVERLAP_WORDS = 35


class Embedder(Protocol):
    def embed(self, texts: list[str]) -> list[list[float]]: ...


def sha256(data: bytes | str) -> str:
    return hashlib.sha256(data.encode() if isinstance(data, str) else data).hexdigest()


def parse_front_matter(text: str) -> tuple[dict, str]:
    m = re.match(r"^---\n(.*?)\n---\n?(.*)$", text, re.S)
    if not m:
        return {}, text
    try:
        meta = yaml.safe_load(m.group(1)) or {}
    except yaml.YAMLError:
        return {}, text
    return (meta if isinstance(meta, dict) else {}), m.group(2)


def chunk_text(text: str, chunk_words: int = CHUNK_WORDS, overlap: int = OVERLAP_WORDS) -> list[str]:
    words = text.split()
    if not words:
        return []
    step = max(1, chunk_words - overlap)
    out = []
    for start in range(0, len(words), step):
        out.append(" ".join(words[start:start + chunk_words]))
        if start + chunk_words >= len(words):
            break
    return out


def _json_safe(v):
    if isinstance(v, dict):
        return {k: _json_safe(x) for k, x in v.items()}
    if isinstance(v, list):
        return [_json_safe(x) for x in v]
    return v if isinstance(v, (str, int, float, bool)) or v is None else str(v)


@dataclass
class IndexStats:
    path: str
    action: str
    chunks_total: int = 0
    chunks_embedded: int = 0
    chunks_deleted: int = 0
    seconds: float = 0.0
    detail: dict = field(default_factory=dict)


class Ingestor:
    def __init__(self, storage: StoragePort, search: SearchPort, embedder: Embedder,
                 access_for_space: Callable[[str], list[str]]):
        self.storage, self.search, self.embedder = storage, search, embedder
        self.access_for_space = access_for_space

    def index_file(self, kb_id: str, path: str) -> IndexStats:
        t0 = time.perf_counter()
        fv = self.storage.stat(kb_id, path)
        if fv is None:
            raise FileNotFoundError(f"{kb_id}:{path}")
        text = self.storage.read_file(kb_id, path).decode("utf-8", errors="replace")
        meta, body = parse_front_matter(text)
        title = meta.get("title") or path.rsplit("/", 1)[-1]
        pieces = chunk_text(body)
        existing = self.search.existing_hashes(fv.file_id)
        access = self.access_for_space(fv.space_id)
        md = _json_safe({k: v for k, v in meta.items()} | {"title": title})
        changed: list[Chunk] = []
        for i, piece in enumerate(pieces):
            content = f"{title}\n\n{piece}" if i == 0 else piece
            h = sha256(content + "|" + ",".join(sorted(access)) + "|" + sha256(str(sorted(md.items()))))
            if existing.get(i) == h:
                continue
            changed.append(Chunk(f"{fv.file_id}:{i}", kb_id, fv.space_id, fv.file_id, fv.version, i, path,
                                 content, h, access, None, md))
        if changed:
            vecs = self.embedder.embed([c.content for c in changed])
            for c, v in zip(changed, vecs):
                c.embedding = v
            self.search.upsert_chunks(changed)
        stale = [f"{fv.file_id}:{i}" for i in existing if i >= len(pieces)]
        removed = self.search.delete_chunks(stale) if stale else 0
        action = "skip" if not changed and not removed else "upsert"
        return IndexStats(path, action, len(pieces), len(changed), removed, time.perf_counter() - t0,
                          {"version": fv.version, "file_id": fv.file_id})

    def remove_file(self, file_id: str) -> int:
        return self.search.delete_file_chunks(file_id)
