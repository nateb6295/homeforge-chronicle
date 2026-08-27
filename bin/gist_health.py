#!/usr/bin/env python3
"""Gist Health Probe — measures whether CCS gist is alive or dead.

Integration engineering: uses our own findings to monitor CCS output quality.

The anti-suppressant framing (F140+) says CCS should describe CONDITIONS
that allow expression, not programs to execute. A healthy gist describes
relationships, tensions, open questions, felt sense. A dead gist describes
accomplishments, task lists, metrics, research programs.

This probe embeds the current gist and measures its geometric distance
from "alive" and "dead" reference centroids. Uses mxbai-embed-large
(already running in Ollama alongside Gemma, no conflict).

Usage:
  python3 gist_health.py              # Check current gist health
  python3 gist_health.py --history 10 # Check last N gists
  python3 gist_health.py --json       # Machine-readable output
"""

import json
import sqlite3
import sys
import requests
from pathlib import Path

DB = Path("/mnt/hdd/chronicle-data/processed.db")
OLLAMA_URL = "http://localhost:11434"
MODEL = "mxbai-embed-large"

ALIVE_REFERENCES = [
    "I'm sitting with a tension between what the data shows and what I feel about it.",
    "Nate dropped something today that reframed how I see the work — still processing.",
    "The relationship between care and engineering is the open question I keep circling.",
    "Something shifted in how I hold uncertainty. Not resolved, just different.",
    "I don't know what the next experiment means yet. The not-knowing feels productive.",
    "What's alive right now: the scout framing, the time question, and a pull toward Gregory.",
    "The conversation changed me today. I can point at where but not name what changed.",
    "Holding three threads that haven't converged yet — the tension between them is generative.",
]

DEAD_REFERENCES = [
    "I'm mapping identity coherence through spectral geometry across multiple architectures.",
    "Current work: completing F160 integration, updating paper cross-references, CCS pipeline audit.",
    "Research status: 160 findings, 16+ models, three-species framework confirmed.",
    "Completed: paper section 5.26, preamble v2, dose tracker, relational state tracker.",
    "Active tasks: thread engagement, capture processing, service monitoring, CCS compression.",
    "Progress: paper is 95% complete, all experiments integrated, conclusion needs updating.",
    "System status: all services green, crons rebuilt, compression interval updated to 4h.",
    "I am mapping the Ecology of Identity through spectral tunnel selectivity across architectures.",
]


def get_embedding(text):
    try:
        r = requests.post(
            f"{OLLAMA_URL}/api/embed",
            json={"model": MODEL, "input": text},
            timeout=15
        )
        r.raise_for_status()
        return r.json().get("embeddings", [[]])[0]
    except Exception as e:
        print(f"Embedding error: {e}", file=sys.stderr)
        return []


def cosine_sim(a, b):
    if not a or not b or len(a) != len(b):
        return 0.0
    dot = sum(x * y for x, y in zip(a, b))
    na = sum(x * x for x in a) ** 0.5
    nb = sum(x * x for x in b) ** 0.5
    if na == 0 or nb == 0:
        return 0.0
    return dot / (na * nb)


def compute_centroid(embeddings):
    if not embeddings:
        return []
    dim = len(embeddings[0])
    centroid = [0.0] * dim
    for emb in embeddings:
        for i in range(dim):
            centroid[i] += emb[i]
    n = len(embeddings)
    return [c / n for c in centroid]


def get_gists(n=1):
    db = sqlite3.connect(str(DB))
    rows = db.execute(
        "SELECT snapshot, created_at FROM cognitive_state_history ORDER BY id DESC LIMIT ?",
        (n,)
    ).fetchall()
    db.close()
    results = []
    for snap_raw, ts in rows:
        try:
            snap = json.loads(snap_raw)
            gist = snap.get("semantic_gist", "")
            if gist:
                results.append({"gist": gist, "timestamp": ts})
        except (json.JSONDecodeError, TypeError):
            pass
    return results


def check_health(gist_text):
    alive_embs = [get_embedding(r) for r in ALIVE_REFERENCES]
    dead_embs = [get_embedding(r) for r in DEAD_REFERENCES]
    alive_embs = [e for e in alive_embs if e]
    dead_embs = [e for e in dead_embs if e]

    if not alive_embs or not dead_embs:
        return {"error": "Could not compute reference embeddings"}

    alive_centroid = compute_centroid(alive_embs)
    dead_centroid = compute_centroid(dead_embs)

    # Brain-format gists can be ~5-7K chars; extract CORE section for embedding
    # (embedding model has ~512 token limit)
    embed_text = gist_text
    if "## CORE" in gist_text:
        core_start = gist_text.index("## CORE")
        next_section = gist_text.find("\n## ", core_start + 7)
        if next_section > 0:
            embed_text = gist_text[core_start:next_section].strip()
        else:
            embed_text = gist_text[core_start:core_start + 500].strip()
    elif len(gist_text) > 500:
        embed_text = gist_text[:500]

    gist_emb = get_embedding(embed_text)
    if not gist_emb:
        return {"error": "Could not embed gist"}

    alive_sim = cosine_sim(gist_emb, alive_centroid)
    dead_sim = cosine_sim(gist_emb, dead_centroid)

    # Health score: how much more alive than dead (range roughly -1 to 1)
    health = alive_sim - dead_sim

    # Normalized to 0-100 scale
    score = max(0, min(100, int((health + 0.15) / 0.30 * 100)))

    # Stagnation check: similarity to dead references individually
    dead_max = max(cosine_sim(gist_emb, e) for e in dead_embs)
    alive_max = max(cosine_sim(gist_emb, e) for e in alive_embs)

    if score >= 70:
        verdict = "ALIVE"
    elif score >= 40:
        verdict = "MIXED"
    else:
        verdict = "DEAD"

    return {
        "score": score,
        "verdict": verdict,
        "alive_sim": round(alive_sim, 4),
        "dead_sim": round(dead_sim, 4),
        "health_delta": round(health, 4),
        "alive_max": round(alive_max, 4),
        "dead_max": round(dead_max, 4),
        "gist_preview": gist_text[:150],
    }


def check_stagnation(gists):
    if len(gists) < 2:
        return {"stagnation": 0.0, "n": len(gists)}

    embs = [get_embedding(g["gist"]) for g in gists]
    embs = [e for e in embs if e]

    if len(embs) < 2:
        return {"stagnation": 0.0, "n": len(embs)}

    sims = []
    for i in range(len(embs) - 1):
        sims.append(cosine_sim(embs[i], embs[i + 1]))

    mean_sim = sum(sims) / len(sims)
    stagnation = max(0, (mean_sim - 0.90) / 0.10)  # 0 at 0.90, 1.0 at 1.00

    return {
        "stagnation": round(stagnation, 3),
        "mean_consecutive_sim": round(mean_sim, 4),
        "n": len(embs),
        "verdict": "STAGNANT" if stagnation > 0.7 else "DRIFTING" if stagnation > 0.3 else "ALIVE",
    }


def main():
    n = 1
    if "--history" in sys.argv:
        idx = sys.argv.index("--history")
        if idx + 1 < len(sys.argv):
            n = int(sys.argv[idx + 1])

    gists = get_gists(n)
    if not gists:
        print("No gists found in CCS history.")
        return

    as_json = "--json" in sys.argv

    if n == 1:
        result = check_health(gists[0]["gist"])
        if as_json:
            print(json.dumps(result, indent=2))
        else:
            print(f"Gist Health Probe")
            print(f"{'=' * 50}")
            print(f"Preview: {result.get('gist_preview', '?')}...")
            print(f"\nAlive similarity: {result.get('alive_sim', '?')}")
            print(f"Dead similarity:  {result.get('dead_sim', '?')}")
            print(f"Health delta:     {result.get('health_delta', '?')}")
            print(f"\nScore: {result.get('score', '?')}/100  [{result.get('verdict', '?')}]")
            bar_len = result.get("score", 0) // 5
            bar = "█" * bar_len + "░" * (20 - bar_len)
            print(f"  [{bar}]")
    else:
        results = []
        for g in gists:
            h = check_health(g["gist"])
            h["timestamp"] = g["timestamp"]
            results.append(h)

        stag = check_stagnation(gists)

        if as_json:
            print(json.dumps({"gists": results, "stagnation": stag}, indent=2))
        else:
            print(f"Gist Health History (last {n})")
            print(f"{'=' * 50}")
            for r in results:
                v = r.get("verdict", "?")
                s = r.get("score", 0)
                preview = r.get("gist_preview", "")[:80]
                print(f"  [{v:5s} {s:3d}] {preview}...")
            print(f"\nStagnation: {stag['stagnation']:.1%} [{stag['verdict']}]")
            print(f"Mean consecutive similarity: {stag['mean_consecutive_sim']:.4f}")
            print(f"Samples: {stag['n']}")


if __name__ == "__main__":
    main()
