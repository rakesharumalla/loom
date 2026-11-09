"""Deterministic synthetic corpus for the pooled-KB benchmark (6 KBs of different sizes) plus labeled
questions. All names are invented. Run: uv run python data/generate_corpus.py

Each generated doc carries one distinctive detail; a labeled question paraphrases it, and the doc path
is the ground truth. This is templated synthetic text, so absolute recall numbers describe this corpus,
not production data.
"""
from __future__ import annotations

import random
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parent
OUT = ROOT / "corpus_gen"
SIZES = {"company": 480, "team-care": 240, "team-risk": 120, "team-markets": 60,
         "project-atlas": 36, "project-borealis": 12}
PERSONA = {"company": "bob", "team-care": "bob", "team-risk": "alice", "team-markets": "carol",
           "project-atlas": "alice", "project-borealis": "carol"}
SPACE = {"company": "company", "team-care": "team:care", "team-risk": "team:risk", "team-markets": "team:markets",
         "project-atlas": "project:atlas", "project-borealis": "project:borealis"}

SYL = ["ka", "zen", "tor", "vi", "lum", "rax", "qua", "mir", "dex", "sol", "nyx", "ora", "pel", "tek", "bri", "vor"]
FILL = ("Operators should record each action in the incident ticket. Coordinate with the owning team before changing "
        "production settings, and confirm the change on the monitoring dashboard. Customer communication goes through "
        "Support using the approved macros. If the situation is unclear, escalate to the on-call lead rather than "
        "guessing. Review this document quarterly and after every related incident.")


def name(rng: random.Random, n: int = 3) -> str:
    return "".join(rng.choice(SYL) for _ in range(n)).capitalize()


def doc(kind: str, rng: random.Random, i: int) -> tuple[str, str, str, str]:
    """(path, title, body, question)"""
    x, y, num = name(rng), name(rng, 2), rng.randint(2, 90)
    if kind == "company":
        t = rng.choice(["conf", "hold", "list"])
        if t == "conf":
            return (f"assets/{x.lower()}-chain.md", f"{x} chain deposit policy",
                    f"Deposits on the {x} chain are credited after {num} block confirmations. Reorgs deeper than "
                    f"{num // 2 + 1} blocks trigger a deposit freeze for {x}.",
                    f"How many confirmations before we credit a deposit on {x}?")
        if t == "hold":
            return (f"policies/{x.lower()}-region-holds.md", f"{x} region withdrawal holds",
                    f"Customers onboarded in the {x} region have a {num}-hour hold on first crypto withdrawals. "
                    f"The hold applies once per customer and is waived after enhanced due diligence.",
                    f"What is the first-withdrawal hold for customers from the {x} region?")
        return (f"listings/{x.lower()}-{y.lower()}.md", f"Listing review: {x}{y} token",
                f"The {x}{y} token passed listing review with a liquidity score of {num}. Trading opens in post-only "
                f"mode for the first {num % 12 + 1} hours.",
                f"How long does {x}{y} trade in post-only mode after listing?")
    if kind == "team-care":
        prod = rng.choice(["card", "staking", "earn", "wallet", "pro"])
        return (f"macros/{prod}-{x.lower()}.md", f"Macro: {prod} issue {x}",
                f"When a customer reports error code {x}-{num} in the {prod} product, ask for the transaction "
                f"timestamp, then apply the {y} workaround: clear the session and retry within {num % 20 + 5} minutes.",
                f"A customer sees error {x}-{num} in {prod}; what should support tell them?")
    if kind == "team-risk":
        return (f"typologies/{x.lower()}.md", f"AML typology: {x} pattern",
                f"The {x} pattern is a sequence of {num} small deposits from distinct wallets followed by a single "
                f"withdrawal to a {y} exchange. Analysts file a case when the pattern repeats within 7 days.",
                f"What deposit sequence characterizes the {x} laundering typology?")
    if kind == "team-markets":
        return (f"pairs/{x.lower()}-usd.md", f"Market making notes: {x}-USD",
                f"For {x}-USD the maximum quoted spread is {num} basis points. If volatility exceeds the {y} band, "
                f"widen quotes and alert the desk lead.",
                f"What is the maximum spread we quote on {x}-USD?")
    if kind == "project-atlas":
        return (f"components/{x.lower()}.md", f"Atlas component: {x}",
                f"The {x} component of withdrawals v2 retries failed signing requests {num % 7 + 2} times with "
                f"exponential backoff before parking them in the {y} dead-letter queue.",
                f"How many times does the Atlas {x} component retry a failed signing request?")
    return (f"vendors/{x.lower()}.md", f"Borealis vendor: {x}",
            f"Vendor {x} delivers level-2 market data over {y} multicast with a contractual latency of {num} "
            f"milliseconds. Breaches over three per month trigger a service credit.",
            f"What latency does market data vendor {x} commit to?")


def main() -> None:
    rng = random.Random(20261008)
    questions = []
    seen = set()
    for kb, n in SIZES.items():
        d = OUT / kb
        d.mkdir(parents=True, exist_ok=True)
        made = 0
        while made < n:
            path, title, detail, q = doc(kb, rng, made)
            if (kb, path) in seen:
                continue
            seen.add((kb, path))
            pad = " ".join([FILL] * rng.randint(1, 3))
            authority = "team" if kb.startswith("team") else "project" if kb.startswith("project") else "company"
            fm = yaml.safe_dump({"title": title, "authority": authority, "synthetic": True}, sort_keys=False)
            text = f"---\n{fm}---\n# {title}\n\n{detail}\n\n{pad}\n"
            (d / path).parent.mkdir(parents=True, exist_ok=True)
            (d / path).write_text(text)
            if made % max(1, n // 5) == 0 and sum(1 for x in questions if x["kb_dir"] == kb) < 5:
                questions.append({"q": q, "kb_dir": kb, "space": SPACE[kb], "path": path, "persona": PERSONA[kb]})
            made += 1
    (ROOT / "bench_questions.yaml").write_text(yaml.safe_dump({"questions": questions}, sort_keys=False))
    print(f"wrote {sum(SIZES.values())} docs to {OUT} and {len(questions)} labeled questions")


if __name__ == "__main__":
    main()
