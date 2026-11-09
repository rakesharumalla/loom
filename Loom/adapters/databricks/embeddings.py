"""Embeddings from a Databricks-hosted Foundation Model endpoint (default databricks-gte-large-en, 1024-d).
https://docs.databricks.com/aws/en/machine-learning/foundation-model-apis/supported-models
"""
from __future__ import annotations

from databricks.sdk import WorkspaceClient

from config import CONFIG


class DatabricksEmbedder:
    def __init__(self, w: WorkspaceClient, endpoint: str | None = None, batch: int = 16):
        self.w, self.endpoint, self.batch = w, endpoint or CONFIG.embedding_endpoint, batch

    def embed(self, texts: list[str]) -> list[list[float]]:
        out: list[list[float]] = []
        for i in range(0, len(texts), self.batch):
            r = self.w.serving_endpoints.query(name=self.endpoint, input=texts[i:i + self.batch])
            out += [d.embedding for d in sorted(r.data, key=lambda d: d.index)]
        return out
