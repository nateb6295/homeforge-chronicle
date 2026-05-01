#!/usr/bin/env python3
"""
semantic_gate.py — embedding-based gate detection for CCS history.

Upgrade over gate_events.py: instead of normalizing dashes/whitespace and
comparing strings, embed each constraint via Ollama mxbai-embed-large and
cluster by cosine similarity. Paraphrase drift (same meaning, different
wording) collapses to the same cluster. A gate event = cluster-ID set
changed between consecutive CCS snapshots.

Requires Ollama at OLLAMA_HOST (192.168.1.11:11434 per Chronicle infra).

CLI:
  python3 semantic_gate.py              # run on CCS history, report gates
  python3 semantic_gate.py --selftest   # run unit tests on a dash/paraphrase corpus
"""
import argparse
import json
import math
import sqlite3
import sys
import urllib.request
from datetime import datetime
from pathlib import Path

DB = Path("/mnt/hdd/chronicle-data/processed.db")
OLLAMA = "http://192.168.1.11:11434/api/embeddings"
MODEL = "mxbai-embed-large"
SIMILARITY_THRESHOLD = 0.88  # cosine > this = same cluster


def embed(text: str) -> list[float]:
    body = json.dumps({"model": MODEL, "prompt": text}).encode()
    req = urllib.request.Request(OLLAMA, data=body,
                                 headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=30) as r:
        return json.loads(r.read())["embedding"]


def cosine(a: list[float], b: list[float]) -> float:
    dot = sum(x * y for x, y in zip(a, b))
    na = math.sqrt(sum(x * x for x in a))
    nb = math.sqrt(sum(x * x for x in b))
    if na == 0 or nb == 0:
        return 0.0
    return dot / (na * nb)


class Clusterer:
    """Online cluster assignment. Each cluster stores its centroid (first member).
    A new string joins the best-matching cluster if cosine > threshold, else
    starts a new cluster."""

    def __init__(self, threshold: float = SIMILARITY_THRESHOLD):
        self.threshold = threshold
        self.centroids: list[list[float]] = []
        self.exemplars: list[str] = []  # representative text per cluster

    def assign(self, text: str, vec: list[float]) -> int:
        best_id, best_sim = -1, -1.0
        for i, c in enumerate(self.centroids):
            s = cosine(vec, c)
            if s > best_sim:
                best_sim, best_id = s, i
        if best_sim >= self.threshold:
            return best_id
        self.centroids.append(vec)
        self.exemplars.append(text)
        return len(self.centroids) - 1


def constraints_of(ccs: dict) -> list[str]:
    out = []
    for c in ccs.get("constraints") or []:
        text = (c.get("rule") if isinstance(c, dict) else str(c)) or ""
        if text.strip():
            out.append(text.strip())
    return out


def pretty(ts: int) -> str:
    return datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M")


def selftest():
    """Prove embedding+clustering collapses dash variants AND paraphrase variants.
    Verify distinct-meaning strings stay in separate clusters."""
    pairs_same = [
        # dash drift (already caught by normalize() — baseline confirmation)
        ("maintain sovereign infrastructure - self-hosted services preferred",
         "maintain sovereign infrastructure \u2013 self\u2011hosted services preferred"),
        # paraphrase drift (normalize() would NOT catch this — new capability)
        ("maintain sovereign infrastructure - self-hosted services preferred",
         "prefer self-hosted services for sovereign infrastructure"),
        # another paraphrase
        ("do not redeploy untested changes without prior metric observation",
         "always observe metrics before redeploying untested code"),
    ]
    pairs_diff = [
        # different meaning — must NOT cluster together
        ("maintain sovereign infrastructure - self-hosted services preferred",
         "do not redeploy untested changes without prior metric observation"),
        ("never ignore creative_explore workspace - core directive",
         "prefer self-hosted services for sovereign infrastructure"),
    ]

    print(f"selftest: threshold={SIMILARITY_THRESHOLD}\n")
    all_ok = True
    print("  SAME-MEANING PAIRS (expect cosine >= threshold):")
    for a, b in pairs_same:
        va, vb = embed(a), embed(b)
        sim = cosine(va, vb)
        ok = sim >= SIMILARITY_THRESHOLD
        all_ok &= ok
        mark = "PASS" if ok else "FAIL"
        print(f"    [{mark}] cos={sim:.3f}  {a[:55]!r}")
        print(f"           vs    {b[:55]!r}")

    print("\n  DIFFERENT-MEANING PAIRS (expect cosine < threshold):")
    for a, b in pairs_diff:
        va, vb = embed(a), embed(b)
        sim = cosine(va, vb)
        ok = sim < SIMILARITY_THRESHOLD
        all_ok &= ok
        mark = "PASS" if ok else "FAIL"
        print(f"    [{mark}] cos={sim:.3f}  {a[:55]!r}")
        print(f"           vs    {b[:55]!r}")

    print(f"\n  overall: {'ALL PASS' if all_ok else 'FAIL — tune threshold'}")
    return all_ok


def run(limit: int = 50):
    con = sqlite3.connect(DB)
    rows = con.execute(
        "SELECT id, created_at, snapshot FROM cognitive_state_history "
        "ORDER BY created_at ASC LIMIT ?", (limit,)
    ).fetchall()
    con.close()
    if not rows:
        print("no snapshots")
        return

    clust = Clusterer()
    per_snapshot: list[tuple[int, int, set[int]]] = []
    for rid, ts, snap_str in rows:
        try:
            ccs = json.loads(snap_str)
        except Exception:
            continue
        cluster_ids: set[int] = set()
        for text in constraints_of(ccs):
            vec = embed(text)
            cluster_ids.add(clust.assign(text, vec))
        per_snapshot.append((rid, ts, cluster_ids))

    print(f"  {len(clust.exemplars)} unique constraint clusters found "
          f"across {len(rows)} snapshots (threshold={SIMILARITY_THRESHOLD})")
    print("  cluster exemplars:")
    for i, ex in enumerate(clust.exemplars):
        print(f"    [{i}] {ex[:90]}")
    print()

    prev_ids = None
    events = []
    for rid, ts, ids in per_snapshot:
        if prev_ids is not None and ids != prev_ids:
            events.append((rid, ts, sorted(ids - prev_ids),
                           sorted(prev_ids - ids)))
        prev_ids = ids

    print(f"  {len(events)} semantic gate event(s):")
    for rid, ts, added, removed in events:
        print(f"    #{rid}  {pretty(ts)}")
        for a in added:
            print(f"      + cluster[{a}] {clust.exemplars[a][:80]}")
        for r in removed:
            print(f"      - cluster[{r}] {clust.exemplars[r][:80]}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--selftest", action="store_true")
    ap.add_argument("--limit", type=int, default=50)
    args = ap.parse_args()
    if args.selftest:
        sys.exit(0 if selftest() else 1)
    run(args.limit)


if __name__ == "__main__":
    main()
