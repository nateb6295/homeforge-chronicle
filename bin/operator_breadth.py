#!/usr/bin/env python3
"""
operator_breadth.py — empirical test of the "constraints are operators with
wide application surface" hypothesis from thread #315 (#7129, #7134).

For each of the 5 constraints in the current CCS:
  1. Embed the constraint text.
  2. Embed a sample of activity_feed rows from the last N days.
  3. Count rows with cosine similarity >= threshold = that constraint's breadth.

Also compute pairwise constraint-to-constraint cosine. Hypothesis says:
  - Each constraint should cover a substantial fraction (operators apply widely).
  - Pairwise constraint-to-constraint cosine should be LOW (orthogonal axes).
  - Low-coverage constraints would indicate fact-shape, not operator-shape.
  - High pairwise similarity would indicate redundancy, not orthogonality.
"""
import json
import math
import sqlite3
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

DB = Path("/mnt/hdd/chronicle-data/processed.db")
OLLAMA_URL = "http://192.168.1.11:11434/api/embeddings"
EMBED_MODEL = "mxbai-embed-large"
MATCH_THRESHOLD = 0.55  # same as coherence_watch calibration
WINDOW_DAYS = 7
SAMPLE_SIZE = 200  # random rows from the window


def embed(text: str, retries: int = 3, backoff: float = 0.5):
    payload = json.dumps({"model": EMBED_MODEL, "prompt": text[:4000]}).encode()
    for attempt in range(retries):
        req = urllib.request.Request(
            OLLAMA_URL, data=payload, headers={"Content-Type": "application/json"}
        )
        try:
            with urllib.request.urlopen(req, timeout=20) as resp:
                return json.loads(resp.read()).get("embedding")
        except (urllib.error.HTTPError, urllib.error.URLError, TimeoutError, OSError) as e:
            if attempt < retries - 1:
                time.sleep(backoff * (2 ** attempt))
            else:
                print(f"embed failed after {retries} tries: {e}", file=sys.stderr)
                return None
    return None


def cosine(a, b):
    dot = sum(x * y for x, y in zip(a, b))
    na = math.sqrt(sum(x * x for x in a))
    nb = math.sqrt(sum(y * y for y in b))
    return dot / (na * nb) if na and nb else 0.0


def main():
    con = sqlite3.connect(DB)

    row = con.execute(
        "SELECT id, snapshot FROM cognitive_state_history ORDER BY id DESC LIMIT 1"
    ).fetchone()
    snap_id, snap = row
    ccs = json.loads(snap)
    constraints = [c for c in ccs.get("constraints", []) if isinstance(c, str)]
    print(f"# Operator-Breadth Probe")
    print(f"CCS snapshot: {snap_id}")
    print(f"Constraints: {len(constraints)}")
    for i, c in enumerate(constraints):
        print(f"  {i+1}. {c}")
    print()

    # Embed constraints
    print("Embedding constraints...")
    cvecs = [embed(c) for c in constraints]

    # Pairwise constraint similarity
    print("\n## Pairwise constraint cosine (low = orthogonal):")
    print("     " + "  ".join(f"C{j+1}   " for j in range(len(constraints))))
    for i in range(len(constraints)):
        row = []
        for j in range(len(constraints)):
            if i == j:
                row.append(" 1.000 ")
            else:
                row.append(f"{cosine(cvecs[i], cvecs[j]):+.3f} ")
        print(f"  C{i+1} " + " ".join(row))

    # Sample activity rows
    print(f"\nSampling {SAMPLE_SIZE} rows from last {WINDOW_DAYS}d...")
    rows = con.execute(
        "SELECT source, activity_type, title, content FROM activity_feed "
        "WHERE created_at > strftime('%s','now','-{} days') "
        "ORDER BY RANDOM() LIMIT ?".format(WINDOW_DAYS),
        (SAMPLE_SIZE,),
    ).fetchall()
    con.close()

    # Build blobs, filter too-short
    blobs = []
    for src, at, ti, c in rows:
        blob = " ".join(str(x) for x in (src, at, ti, c) if x).strip()
        if len(blob) >= 20:
            blobs.append(blob)
    print(f"Rows with enough content to embed: {len(blobs)}")

    # Embed activity rows (paced to avoid overwhelming the endpoint)
    print("Embedding sample...")
    rvecs = []
    for i, b in enumerate(blobs):
        v = embed(b)
        rvecs.append(v)
        if (i + 1) % 50 == 0:
            print(f"  {i+1}/{len(blobs)} embedded", file=sys.stderr)
    ok = sum(1 for v in rvecs if v)
    print(f"  embedded {ok}/{len(rvecs)} successfully")

    # Per-constraint breadth
    print(f"\n## Operator breadth per constraint (cosine >= {MATCH_THRESHOLD})")
    for i, c in enumerate(constraints):
        sims = [cosine(cvecs[i], rv) for rv in rvecs if rv]
        if not sims:
            continue
        above = [s for s in sims if s >= MATCH_THRESHOLD]
        print(f"  C{i+1}: hits={len(above):3d}/{len(sims)} ({len(above)/len(sims):.1%})  "
              f"max={max(sims):.3f}  mean={sum(sims)/len(sims):.3f}")
        print(f"      {c[:80]}")


if __name__ == "__main__":
    main()
