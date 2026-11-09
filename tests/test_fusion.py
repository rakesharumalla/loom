import pytest

from Loom.control.fusion import rrf


def test_rrf_scores_and_order():
    out = rrf({"vector": ["a", "b", "c"], "bm25": ["c", "a"]})
    ids = [i for i, _, _ in out]
    assert ids == ["a", "c", "b"]
    a = dict((i, (s, r)) for i, s, r in out)["a"]
    assert a[0] == pytest.approx(1 / 61 + 1 / 62) and a[1] == {"vector": 1, "bm25": 2}


def test_rrf_single_list_and_ties_stable():
    assert [i for i, _, _ in rrf({"bm25": ["x", "y"]})] == ["x", "y"]
    out = rrf({"v": ["b"], "t": ["a"]})
    assert [i for i, _, _ in out] == ["a", "b"]  # equal scores -> id order


def test_rrf_duplicate_in_list_counts_once():
    out = rrf({"v": ["a", "a", "b"]})
    assert dict((i, r) for i, _, r in out)["a"] == {"v": 1}
