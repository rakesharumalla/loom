"""SearchPort on Lakebase Search: lakebase_ann (vector) + lakebase_bm25 (keyword), fused with RRF.

https://docs.databricks.com/aws/en/oltp/projects/lakebase-search
https://docs.databricks.com/aws/en/oltp/projects/lakebase-vector  (probes, epsilon, prefilter GUCs)
https://docs.databricks.com/aws/en/oltp/projects/lakebase-text    (BM25: lower score = more relevant)

Filters (space_id, access principals) are in the WHERE clause of both candidate queries. By default
Postgres applies non-vector filters after the ANN index returns candidates; `prefilter=True` sets
lakebase_ann.prefilter = on so they are evaluated before full-precision reranking. RLS on `chunks`
(sql/rls.sql) re-checks access as defense in depth.
"""
from __future__ import annotations

from psycopg.types.json import Jsonb

from Loom.adapters.databricks.lakebase_conn import LakebaseConnector
from Loom.control.fusion import RRF_K, rrf
from Loom.ports import Chunk, Hit

BM25_INDEX = "loom.chunks_tsv_bm25"  # to_bm25query needs the schema-qualified name


def _vec(v: list[float]) -> str:
    return "[" + ",".join(f"{x:.6f}" for x in v) + "]"


class LakebaseSearch:
    def __init__(self, conn: LakebaseConnector, embedder, candidates: int = 40, prefilter: bool = True,
                 probes: str | None = None, epsilon: str | None = None):
        self.conn, self.embedder = conn, embedder
        self.candidates, self.prefilter, self.probes, self.epsilon = candidates, prefilter, probes, epsilon

    # ------------------------------------------------------------- session settings
    def _read_settings(self, cur, principals: list[str]) -> None:
        """All per-transaction settings in one statement (one round trip)."""
        cur.execute("SET LOCAL ROLE loom_reader")   # no BYPASSRLS: chunks RLS applies
        cur.execute("""SELECT set_config('loom.caller_principals', %s, true),
                              set_config('lakebase_ann.prefilter', %s, true),
                              set_config('lakebase_ann.probes', coalesce(%s, current_setting('lakebase_ann.probes')), true),
                              set_config('lakebase_ann.epsilon', coalesce(%s, current_setting('lakebase_ann.epsilon')), true)""",
                    (",".join(principals), "on" if self.prefilter else "off", self.probes, self.epsilon))

    @staticmethod
    def _writer(cur) -> None:
        cur.execute("SET LOCAL ROLE loom_writer")
        cur.execute("SELECT set_config('loom.writer', 'on', true)")

    # ------------------------------------------------------------- search
    def _candidates(self, space_ids, query, caller_principals, mode, kb_ids, qvec, with_rows: bool):
        """Per-list candidates (best first) from ONE query; optionally with row data for the final hits."""
        n = self.candidates
        filt = "space_id = ANY(%(spaces)s) AND access_groups && %(principals)s"
        if kb_ids:
            filt += " AND kb_id = ANY(%(kbs)s)"
        params = {"spaces": space_ids, "principals": caller_principals, "kbs": kb_ids or [], "n": n, "qt": query}
        parts = []
        if mode in ("hybrid", "vector"):
            params["q"] = _vec(qvec if qvec is not None else self.embedder.embed([query])[0])
            parts.append(f"""SELECT 'vector' AS list, chunk_id, row_number() OVER (ORDER BY d) AS r FROM (
                SELECT chunk_id, embedding <=> %(q)s::vector AS d FROM loom.chunks
                WHERE {filt} ORDER BY embedding <=> %(q)s::vector LIMIT %(n)s) v""")
        if mode in ("hybrid", "bm25"):
            parts.append(f"""SELECT 'bm25' AS list, chunk_id, row_number() OVER (ORDER BY s) AS r FROM (
                SELECT chunk_id, content_tsv <@> to_bm25query(to_tsvector('english', %(qt)s), '{BM25_INDEX}') AS s
                FROM loom.chunks WHERE {filt} AND content_tsv @@ replace(plainto_tsquery('english', %(qt)s)::text, '&', '|')::tsquery
                ORDER BY s LIMIT %(n)s) t""")
        ranked = " UNION ALL ".join(parts)
        if with_rows:
            sql = f"""WITH ranked AS ({ranked})
                SELECT r.list, r.chunk_id, r.r, c.kb_id, c.space_id, c.file_id::text, c.version, c.path, c.content,
                       c.metadata, c.updated_at
                FROM ranked r JOIN loom.chunks c ON c.chunk_id = r.chunk_id ORDER BY 1, 3"""
        else:
            sql = ranked + " ORDER BY 1, 3"
        with self.conn.tx() as c, c.cursor() as cur:
            self._read_settings(cur, caller_principals)
            cur.execute(sql, params)
            return cur.fetchall()

    def ranked_ids(self, space_ids, query, caller_principals, mode="hybrid", kb_ids=None, qvec=None):
        """Per-list candidate ids (best first). Exposed for the benchmark."""
        lists: dict[str, list[str]] = {"vector": [], "bm25": []}
        for row in self._candidates(space_ids, query, caller_principals, mode, kb_ids, qvec, with_rows=False):
            lists[row[0]].append(row[1])
        return {k: v for k, v in lists.items() if mode == "hybrid" or k == mode}

    def search(self, space_ids, query, caller_principals, k=10, kb_ids=None, mode="hybrid", qvec=None) -> list[Hit]:
        if not space_ids:
            return []
        lists: dict[str, list[str]] = {"vector": [], "bm25": []}
        rows = {}
        for r in self._candidates(space_ids, query, caller_principals, mode, kb_ids, qvec, with_rows=True):
            lists[r[0]].append(r[1])
            rows[r[1]] = r   # rows hidden by RLS never come back (defense in depth)
        fused = rrf({k2: v for k2, v in lists.items() if v}, k=RRF_K)[:k]
        return [Hit(cid, rows[cid][3], rows[cid][4], rows[cid][5], rows[cid][6], rows[cid][7], rows[cid][8],
                    rows[cid][9] or {}, ranks.get("vector"), ranks.get("bm25"), score, rows[cid][10])
                for cid, score, ranks in fused]

    # ------------------------------------------------------------- writes
    def upsert_chunks(self, chunks: list[Chunk]) -> int:
        if not chunks:
            return 0
        with self.conn.tx() as c, c.cursor() as cur:
            self._writer(cur)
            cur.executemany("""
                INSERT INTO loom.chunks (chunk_id, kb_id, space_id, file_id, version, chunk_index, path, content,
                                         content_hash, embedding, content_tsv, access_groups, metadata, updated_at)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s::vector, to_tsvector('english', %s), %s, %s, now())
                ON CONFLICT (chunk_id) DO UPDATE SET version = EXCLUDED.version, path = EXCLUDED.path,
                  content = EXCLUDED.content, content_hash = EXCLUDED.content_hash, embedding = EXCLUDED.embedding,
                  content_tsv = EXCLUDED.content_tsv, access_groups = EXCLUDED.access_groups,
                  metadata = EXCLUDED.metadata, updated_at = now()""",
                [(ch.chunk_id, ch.kb_id, ch.space_id, ch.file_id, ch.version, ch.chunk_index, ch.path, ch.content,
                  ch.content_hash, _vec(ch.embedding), ch.content, ch.access_groups, Jsonb(ch.metadata))
                 for ch in chunks])
        return len(chunks)

    def delete_chunks(self, chunk_ids: list[str]) -> int:
        with self.conn.tx() as c, c.cursor() as cur:
            self._writer(cur)
            cur.execute("DELETE FROM loom.chunks WHERE chunk_id = ANY(%s)", (chunk_ids,))
            return cur.rowcount

    def delete_file_chunks(self, file_id: str) -> int:
        with self.conn.tx() as c, c.cursor() as cur:
            self._writer(cur)
            cur.execute("DELETE FROM loom.chunks WHERE file_id = %s", (file_id,))
            return cur.rowcount

    def existing_hashes(self, file_id: str) -> dict[int, str]:
        with self.conn.tx() as c, c.cursor() as cur:
            self._writer(cur)
            cur.execute("SELECT chunk_index, content_hash FROM loom.chunks WHERE file_id = %s", (file_id,))
            return dict(cur.fetchall())

    def facts_for_subject(self, subject, space_ids, caller_principals) -> list[Hit]:
        if not space_ids:
            return []
        with self.conn.tx() as c, c.cursor() as cur:
            self._read_settings(cur, caller_principals)
            cur.execute("""SELECT chunk_id, kb_id, space_id, file_id::text, version, path, content, metadata, updated_at
                           FROM loom.chunks WHERE metadata->'facts' ? %s AND chunk_index = 0
                             AND space_id = ANY(%s) AND access_groups && %s""",
                        (subject, space_ids, caller_principals))
            return [Hit(r[0], r[1], r[2], r[3], r[4], r[5], r[6], r[7] or {}, None, None, 0.0, r[8])
                    for r in cur.fetchall()]

    def retag_space(self, space_id: str, access_groups: list[str]) -> int:
        with self.conn.tx() as c, c.cursor() as cur:
            self._writer(cur)
            cur.execute("UPDATE loom.chunks SET access_groups = %s WHERE space_id = %s AND access_groups <> %s",
                        (access_groups, space_id, access_groups))
            return cur.rowcount

    def reindex_bm25(self) -> float:
        """BM25 corpus statistics are computed at index build time; REINDEX refreshes them."""
        import time
        t = time.perf_counter()
        with self.conn.connect(autocommit=True) as c:
            c.execute(f"REINDEX INDEX {BM25_INDEX}")
        return time.perf_counter() - t
