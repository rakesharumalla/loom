from pipelines.ingest_core import chunk_text, parse_front_matter, sha256


def test_chunk_sizes_and_overlap():
    words = [f"w{i}" for i in range(1000)]
    chunks = chunk_text(" ".join(words), chunk_words=350, overlap=35)
    assert [len(c.split()) for c in chunks] == [350, 350, 350, 55]
    assert chunks[0].split()[-35:] == chunks[1].split()[:35]
    assert chunks[-1].split()[-1] == "w999"


def test_chunk_small_and_empty():
    assert chunk_text("") == []
    assert chunk_text("one two three") == ["one two three"]


def test_front_matter():
    meta, body = parse_front_matter("---\ntitle: Limits\nauthority: official_policy\n---\nBody text")
    assert meta == {"title": "Limits", "authority": "official_policy"} and body == "Body text"
    assert parse_front_matter("no front matter") == ({}, "no front matter")


def test_hash_stable():
    assert sha256("a") == sha256(b"a") and sha256("a") != sha256("b")
