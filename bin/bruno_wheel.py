#!/usr/bin/env python3
"""Bruno's Wheel — generative connection probe.

Takes unconnected entity pairs from the CCS, generates hypothetical
relationships, then SELECTS which ones the topology actually wants.

Generation is ONE PART of the wheel. Selection is the other.
Without selection, generation is noise.

The wheel:
  1. SAMPLE: pick N unconnected entity pairs from focal_entities
  2. GENERATE: ask the model to propose a relationship (one sentence)
  3. SCORE: embed the proposal, compare to existing edge embeddings
  4. FILTER: only keep proposals that score above threshold
  5. FALSIFY: check if the proposed connection is testable
  6. REPORT: output scored candidates, not conclusions
"""
import json
import math
import random
import sqlite3
import sys
import time
import urllib.request
from itertools import combinations
from pathlib import Path

DB = "/mnt/hdd/chronicle-data/processed.db"
OLLAMA_EMBED = "http://localhost:11434/api/embeddings"
GROQ_API = "https://api.groq.com/openai/v1/chat/completions"
EMBED_MODEL = "snowflake-arctic-embed2"
GEN_MODEL = "llama-3.3-70b-versatile"
OUTPUT = Path.home() / "chronicle" / "data" / "bruno_wheel.json"

MAX_PAIRS = 6
SIMILARITY_THRESHOLD = 0.35
MAX_CANDIDATES = 3


def embed(text, timeout=60):
    text = text[:1500]
    body = json.dumps({"model": EMBED_MODEL, "prompt": text}).encode()
    req = urllib.request.Request(
        OLLAMA_EMBED, data=body, headers={"Content-Type": "application/json"}
    )
    resp = urllib.request.urlopen(req, timeout=timeout)
    return json.loads(resp.read())["embedding"]


def generate(prompt, timeout=30):
    import os, subprocess
    api_key = os.environ.get("GROQ_API_KEY", "")
    body = json.dumps({
        "model": GEN_MODEL,
        "messages": [{"role": "user", "content": prompt}],
        "temperature": 0.7,
        "max_tokens": 150,
    })
    result = subprocess.run(
        ["curl", "-s", "-X", "POST", GROQ_API,
         "-H", f"Authorization: Bearer {api_key}",
         "-H", "Content-Type: application/json",
         "-d", body],
        capture_output=True, text=True, timeout=timeout
    )
    data = json.loads(result.stdout)
    return data["choices"][0]["message"]["content"].strip()


def cosine(a, b):
    dot = sum(x * y for x, y in zip(a, b))
    na = math.sqrt(sum(x * x for x in a))
    nb = math.sqrt(sum(x * x for x in b))
    if na == 0 or nb == 0:
        return 0.0
    return dot / (na * nb)


def get_current_state():
    db = sqlite3.connect(DB)
    row = db.execute("""
        SELECT snapshot FROM cognitive_state_history ORDER BY id DESC LIMIT 1
    """).fetchone()
    db.close()
    if not row:
        return None
    return json.loads(row[0])


def get_connected_entities(state):
    """Entities that appear in relational_map text."""
    rmap = state.get("relational_map", {})
    rmap_text = " ".join(str(v) for v in rmap.values()).lower()
    rmap_text += " ".join(str(k) for k in rmap.keys()).lower()

    entities = state.get("focal_entities", [])
    connected = set()
    unconnected = set()

    for e in entities:
        name = e.get("name", "") if isinstance(e, dict) else str(e)
        if not name:
            continue
        if name.lower() in rmap_text:
            connected.add(name)
        else:
            unconnected.add(name)

    return connected, unconnected


def run():
    state = get_current_state()
    if not state:
        print("No CCS state found")
        return

    version = state.get("version", "?")
    connected, unconnected = get_connected_entities(state)
    rmap = state.get("relational_map", {})

    print(f"Bruno's Wheel — CCS v{version}")
    print(f"  Connected entities: {len(connected)}")
    print(f"  Unconnected entities: {len(unconnected)}")
    print(f"  Existing edges: {len(rmap)}")
    print("=" * 60)

    # STEP 1: SAMPLE — pick pairs mixing connected + unconnected
    all_entities = list(connected | unconnected)
    if len(all_entities) < 2:
        print("Not enough entities to spin the wheel")
        return

    # Prefer pairs where at least one entity is unconnected
    candidate_pairs = []
    for a, b in combinations(all_entities, 2):
        a_conn = a in connected
        b_conn = b in connected
        if not a_conn or not b_conn:
            candidate_pairs.append((a, b))

    random.shuffle(candidate_pairs)
    pairs = candidate_pairs[:MAX_PAIRS]

    if not pairs:
        print("No unconnected pairs to explore")
        return

    print(f"\n  Spinning wheel on {len(pairs)} pairs...")

    # Embed existing edges for comparison
    print("  Embedding existing edges...")
    edge_embeddings = []
    for edge_name, edge_text in rmap.items():
        combined = f"{edge_name}: {edge_text[:300]}"
        try:
            emb = embed(combined)
            edge_embeddings.append(emb)
        except Exception:
            pass

    # Build entity context from CCS fields for vivification
    ep_text = json.dumps(state.get("episodic_trace", ""))[:800]
    us_text = json.dumps(state.get("uncertainty_signals", ""))[:400]
    gist = state.get("semantic_gist", "")[:200]
    context_block = f"System gist: {gist}\nRecent work: {ep_text[:400]}"

    # STEP 2-5: GENERATE, SCORE, FILTER, FALSIFY
    results = []

    for a, b in pairs:
        print(f"\n  [{a}] ↔ [{b}]")

        # GENERATE — with vivification context and example
        prompt = (
            f"Context: A research system studying AI identity persistence through "
            f"iterative memory compression. Key finding: structural topology "
            f"(which concepts connect to which) outlasts vocabulary (how connections "
            f"are named). {context_block}\n\n"
            f"Example connection: 'rate-distortion theory' ↔ 'proactive decay' → "
            f"'Rate-distortion theory predicts that optimal compression preserves "
            f"high-information-density structures, which is exactly what proactive "
            f"decay implements by evicting low-salience entities first.'\n\n"
            f"Now propose a connection for:\n"
            f"  A: {a}\n  B: {b}\n\n"
            f"One sentence: how does A's structure or behavior mechanically "
            f"relate to B's in this memory-compression research context? "
            f"If genuinely unrelated, say 'NO CONNECTION'."
        )

        try:
            proposal = generate(prompt)
        except Exception as e:
            print(f"    Generation failed: {e}")
            continue

        if not proposal or "NO CONNECTION" in proposal.upper():
            print(f"    No connection proposed (honest rejection)")
            results.append({
                "pair": [a, b],
                "proposal": None,
                "rejected_by": "generation",
            })
            continue

        proposal = proposal.split("\n")[0][:300]
        print(f"    Proposal: {proposal[:100]}...")

        # SCORE — compare proposal embedding to existing edges
        try:
            proposal_emb = embed(f"{a} ↔ {b}: {proposal}")
        except Exception as e:
            print(f"    Embedding failed: {e}")
            continue

        if edge_embeddings:
            similarities = [cosine(proposal_emb, ee) for ee in edge_embeddings]
            max_sim = max(similarities)
            mean_sim = sum(similarities) / len(similarities)
        else:
            max_sim = 0.0
            mean_sim = 0.0

        print(f"    Score: max_sim={max_sim:.3f}, mean_sim={mean_sim:.3f}")

        # FILTER — proposal should be RELATED to existing topology
        # but not IDENTICAL (that would be redundant)
        if max_sim > 0.85:
            print(f"    REJECTED: too similar to existing edge (redundant)")
            results.append({
                "pair": [a, b],
                "proposal": proposal,
                "max_sim": round(max_sim, 4),
                "mean_sim": round(mean_sim, 4),
                "rejected_by": "redundancy",
            })
            continue

        if mean_sim < SIMILARITY_THRESHOLD:
            print(f"    REJECTED: too distant from existing topology")
            results.append({
                "pair": [a, b],
                "proposal": proposal,
                "max_sim": round(max_sim, 4),
                "mean_sim": round(mean_sim, 4),
                "rejected_by": "distance",
            })
            continue

        # FALSIFY — check if there's a testable prediction
        falsify_prompt = (
            f"This is a proposed connection: '{proposal}'\n"
            f"Can this be tested or falsified with existing data or a simple probe? "
            f"Answer YES or NO, then one sentence explaining how."
        )
        try:
            falsify = generate(falsify_prompt)
        except Exception:
            falsify = "UNKNOWN"

        testable = falsify.upper().startswith("YES")
        print(f"    Testable: {testable}")
        if testable:
            print(f"    How: {falsify[:120]}")

        results.append({
            "pair": [a, b],
            "proposal": proposal,
            "max_sim": round(max_sim, 4),
            "mean_sim": round(mean_sim, 4),
            "testable": testable,
            "falsification": falsify[:200] if testable else None,
            "status": "CANDIDATE",
        })

    # REPORT
    print("\n" + "=" * 60)
    print("WHEEL RESULTS")
    print("=" * 60)

    candidates = [r for r in results if r.get("status") == "CANDIDATE"]
    rejected = [r for r in results if r.get("rejected_by")]
    honest_rejections = [r for r in results if r.get("rejected_by") == "generation"]

    print(f"  Pairs spun: {len(pairs)}")
    print(f"  Honest rejections (no connection): {len(honest_rejections)}")
    print(f"  Filtered out: {len(rejected) - len(honest_rejections)}")
    print(f"  Candidates: {len(candidates)}")

    # Sort candidates by mean_sim (topology fit)
    candidates.sort(key=lambda x: x.get("mean_sim", 0), reverse=True)

    for i, c in enumerate(candidates[:MAX_CANDIDATES]):
        testable_str = "TESTABLE" if c.get("testable") else "untestable"
        print(f"\n  #{i+1} [{c['pair'][0]}] ↔ [{c['pair'][1]}]")
        print(f"      {c['proposal'][:150]}")
        print(f"      fit={c['mean_sim']:.3f}, {testable_str}")
        if c.get("falsification"):
            print(f"      test: {c['falsification'][:120]}")

    gen_rate = len(candidates) / len(pairs) if pairs else 0
    print(f"\n  Generation/selection ratio: {gen_rate:.0%} survived")
    print(f"  (Bruno target: generation is ONE PART, not dominant)")

    output = {
        "timestamp": int(time.time()),
        "probe": "bruno-wheel",
        "build": "#74",
        "ccs_version": version,
        "connected_entities": len(connected),
        "unconnected_entities": len(unconnected),
        "pairs_spun": len(pairs),
        "honest_rejections": len(honest_rejections),
        "filtered_out": len(rejected) - len(honest_rejections),
        "candidates": len(candidates),
        "generation_survival_rate": round(gen_rate, 3),
        "results": results,
    }

    OUTPUT.write_text(json.dumps(output, indent=2))
    print(f"\n  Results: {OUTPUT}")


if __name__ == "__main__":
    run()
