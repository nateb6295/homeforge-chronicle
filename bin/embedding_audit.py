#!/usr/bin/env python3
"""
embedding_audit.py — Audit the embedding space for retrieval drift.

The embedding layer has the highest governance-load in Chronicle's architecture
(g=2.531 per Tallam framework). This tool provides observability into what the
embedding space surfaces for canonical queries over time.

Usage:
  python3 embedding_audit.py              # Run audit with default probes
  python3 embedding_audit.py --history    # Show drift over time
  python3 embedding_audit.py --probe "query"  # Ad-hoc probe
"""
import argparse
import json
import math
import sqlite3
import struct
import time
import urllib.request
from pathlib import Path

DB = "/mnt/hdd/chronicle-data/processed.db"
OLLAMA = "http://192.168.1.11:11434/api/embeddings"
MODEL = "nomic-embed-text"  # Must match model used for capsule_embeddings
RESULTS_DIR = Path.home() / "chronicle" / "experiments" / "embedding_audit"

# Canonical probe queries — stable anchors for drift detection
CANONICAL_PROBES = [
    "thread 318 calibration beats effort navigation CCS",
    "Nate family homeforge sovereignty partnership",
    "compact CCS compression telegraphic cognitive state",
    "Discord presence heartbeat monitoring",
    "identity decay constraints invariant rotation",
    "embedding retrieval capsule search memory",
    "Gemma Hermes agent mesh infrastructure",
    "grokking phase transition training dynamics",
]


def embed(text, timeout=60):
    text = text[:800]
    body = json.dumps({"model": MODEL, "prompt": text}).encode()
    req = urllib.request.Request(
        OLLAMA, data=body, headers={"Content-Type": "application/json"}
    )
    resp = urllib.request.urlopen(req, timeout=timeout)
    return json.loads(resp.read())["embedding"]


def cosine(a, b):
    dot = sum(x * y for x, y in zip(a, b))
    na = math.sqrt(sum(x * x for x in a))
    nb = math.sqrt(sum(x * x for x in b))
    if na == 0 or nb == 0:
        return 0.0
    return dot / (na * nb)


def load_embeddings(db_path=DB, limit=None):
    """Load capsule embeddings from local DB."""
    db = sqlite3.connect(db_path)
    query = "SELECT ce.capsule_id, ce.embedding, kc.restatement, kc.topic "
    query += "FROM capsule_embeddings ce "
    query += "JOIN knowledge_capsules kc ON ce.capsule_id = kc.id "
    if limit:
        query += f"ORDER BY ce.created_at DESC LIMIT {limit}"
    rows = db.execute(query).fetchall()
    db.close()

    capsules = []
    for cid, emb_blob, content, topic in rows:
        # Decode embedding from blob (float32 array)
        n_floats = len(emb_blob) // 4
        emb = list(struct.unpack(f'{n_floats}f', emb_blob))
        capsules.append({
            "id": cid,
            "embedding": emb,
            "content": content[:200] if content else "",
            "topic": topic or "",
        })
    return capsules


def search(query_emb, capsules, top_k=5):
    """Find top-k most similar capsules to query embedding."""
    scored = []
    for cap in capsules:
        sim = cosine(query_emb, cap["embedding"])
        scored.append((sim, cap))
    scored.sort(key=lambda x: x[0], reverse=True)
    return scored[:top_k]


def run_audit(probes=None, top_k=5):
    """Run a full audit pass with canonical probes."""
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)

    if probes is None:
        probes = CANONICAL_PROBES

    print(f"Loading embeddings...")
    capsules = load_embeddings()
    print(f"Loaded {len(capsules)} capsule embeddings")
    print()

    audit_results = {}

    for query in probes:
        print(f"Probe: {query[:60]}...")
        q_emb = embed(query)
        results = search(q_emb, capsules, top_k=top_k)

        hits = []
        for sim, cap in results:
            hits.append({
                "capsule_id": cap["id"],
                "similarity": round(sim, 4),
                "topic": cap["topic"],
                "content_preview": cap["content"][:100],
            })
            print(f"  {sim:.4f}  #{cap['id']} [{cap['topic']}] {cap['content'][:60]}...")

        audit_results[query] = {
            "hits": hits,
            "top_similarity": hits[0]["similarity"] if hits else 0,
            "similarity_range": round(hits[0]["similarity"] - hits[-1]["similarity"], 4) if len(hits) > 1 else 0,
        }
        print()

    # Summary statistics
    top_sims = [r["top_similarity"] for r in audit_results.values()]
    ranges = [r["similarity_range"] for r in audit_results.values()]

    print("=" * 60)
    print("AUDIT SUMMARY")
    print(f"  Probes: {len(probes)}")
    print(f"  Capsules searched: {len(capsules)}")
    print(f"  Mean top similarity: {sum(top_sims)/len(top_sims):.4f}")
    print(f"  Mean similarity range (top-k spread): {sum(ranges)/len(ranges):.4f}")
    print()

    # Check for coverage gaps (probes where top hit is weak)
    weak = [(q, r["top_similarity"]) for q, r in audit_results.items() if r["top_similarity"] < 0.5]
    if weak:
        print("COVERAGE GAPS (top hit < 0.5):")
        for q, s in weak:
            print(f"  {s:.4f}  {q[:60]}")
    else:
        print("No coverage gaps detected (all probes have top hit >= 0.5)")

    # Save
    timestamp = int(time.time())
    result = {
        "timestamp": timestamp,
        "n_capsules": len(capsules),
        "n_probes": len(probes),
        "probes": audit_results,
        "mean_top_similarity": round(sum(top_sims)/len(top_sims), 4),
        "mean_range": round(sum(ranges)/len(ranges), 4),
    }
    out = RESULTS_DIR / f"audit_{time.strftime('%Y%m%d_%H%M%S')}.json"
    out.write_text(json.dumps(result, indent=2))
    print(f"\nSaved: {out}")

    return result


def show_history():
    """Show drift over time by comparing audit results."""
    files = sorted(RESULTS_DIR.glob("audit_*.json"))
    if not files:
        print("No audit history found. Run an audit first.")
        return

    print(f"{'Date':<20} {'Capsules':>10} {'Mean Top Sim':>14} {'Mean Range':>12}")
    print("-" * 60)
    for f in files:
        data = json.loads(f.read_text())
        ts = time.strftime('%Y-%m-%d %H:%M', time.localtime(data["timestamp"]))
        print(f"{ts:<20} {data['n_capsules']:>10} {data['mean_top_similarity']:>14.4f} "
              f"{data['mean_range']:>12.4f}")

    if len(files) >= 2:
        first = json.loads(files[0].read_text())
        last = json.loads(files[-1].read_text())
        # Compare top capsule IDs for each probe
        probe_drift = 0
        common_probes = set(first["probes"].keys()) & set(last["probes"].keys())
        for probe in common_probes:
            first_ids = {h["capsule_id"] for h in first["probes"][probe]["hits"]}
            last_ids = {h["capsule_id"] for h in last["probes"][probe]["hits"]}
            overlap = len(first_ids & last_ids)
            total = max(len(first_ids), len(last_ids))
            if total > 0:
                probe_drift += 1 - (overlap / total)
        if common_probes:
            mean_drift = probe_drift / len(common_probes)
            print(f"\nMean retrieval drift: {mean_drift:.2%}")
            if mean_drift < 0.2:
                print("Stable — same capsules surfacing for canonical probes")
            elif mean_drift < 0.5:
                print("Moderate drift — review what's changing")
            else:
                print("HIGH DRIFT — embedding space behavior has shifted significantly")


def main():
    parser = argparse.ArgumentParser(description="Embedding space audit")
    parser.add_argument("--history", action="store_true", help="Show drift history")
    parser.add_argument("--probe", type=str, help="Ad-hoc probe query")
    parser.add_argument("--top-k", type=int, default=5, help="Number of results per probe")
    args = parser.parse_args()

    if args.history:
        show_history()
    elif args.probe:
        run_audit(probes=[args.probe], top_k=args.top_k)
    else:
        run_audit(top_k=args.top_k)


if __name__ == "__main__":
    main()
