"""Reciprocal Rank Fusion, as in the Lakebase Search hybrid example (k=60):
https://docs.databricks.com/aws/en/oltp/projects/lakebase-search#combine-results-with-hybrid-search
"""
from __future__ import annotations

RRF_K = 60


def rrf(rank_lists: dict[str, list[str]], k: int = RRF_K) -> list[tuple[str, float, dict[str, int]]]:
    """rank_lists: name -> ids best-first. Returns (id, score, {name: 1-based rank}) best-first,
    ties broken by id for stable ordering."""
    scores: dict[str, float] = {}
    ranks: dict[str, dict[str, int]] = {}
    for name, ids in rank_lists.items():
        for pos, _id in enumerate(ids, start=1):
            if name in ranks.setdefault(_id, {}):
                continue  # duplicate id within one list: keep its best rank
            ranks[_id][name] = pos
            scores[_id] = scores.get(_id, 0.0) + 1.0 / (k + pos)
    return sorted(((i, s, ranks[i]) for i, s in scores.items()), key=lambda t: (-t[1], t[0]))
