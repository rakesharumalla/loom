-- Lakebase Search indexes. Built AFTER the initial load:
-- "BM25 computes corpus statistics at build time, not incrementally"
-- https://docs.databricks.com/aws/en/oltp/projects/lakebase-search
-- https://docs.databricks.com/aws/en/oltp/projects/lakebase-vector
-- https://docs.databricks.com/aws/en/oltp/projects/lakebase-text
SET search_path = loom, public;

CREATE INDEX IF NOT EXISTS chunks_embedding_ann ON chunks USING lakebase_ann (embedding vector_cosine_ops);
CREATE INDEX IF NOT EXISTS chunks_tsv_bm25 ON chunks USING lakebase_bm25 (content_tsv);
