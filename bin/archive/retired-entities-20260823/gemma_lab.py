#!/usr/bin/env python3
"""gemma_lab.py — Gemma's self-directed experiment toolkit.

Gives Gemma access to spectral measurements she can run on herself
or on data she has access to. Called from gemma.py during experiment cycles.

Available probes:
  - self_embedding_drift: measure how her own embeddings shift under different framings
  - routing_spectral: SVD of her routing decision matrix over time
  - novelty_landscape: map the novelty score distribution and find structure
  - ccs_self_report: run CCS identity framing on herself and report what shifts
  - cross_embed: compare embeddings from different models (AGX vs Nano)

Usage:
  python3 gemma_lab.py --probe <name> [--question "what to investigate"]
  python3 gemma_lab.py --list   # show available probes
  python3 gemma_lab.py --ask "free-form question"  # Gemma picks the probe
"""

import argparse, json, os, sys, time, sqlite3
import numpy as np
from datetime import datetime
from pathlib import Path

sys.path.insert(0, os.path.dirname(__file__))
from embed_config import embed as _raw_embed, EMBED_URL

DB_PATH = os.environ.get("CHRONICLE_DB",
    os.path.expanduser("~/.homeforge-chronicle/processed.db"))
RESULTS_DIR = Path.home() / "chronicle" / "experiments" / "gemma_lab"
JOURNAL = Path.home() / "chronicle" / "gemma_unread.md"

RESULTS_DIR.mkdir(parents=True, exist_ok=True)

def _embed(text):
    return np.array(_raw_embed(text))

def _cosine(a, b):
    return float(np.dot(a, b) / (np.linalg.norm(a) * np.linalg.norm(b) + 1e-10))

def _db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def probe_self_embedding_drift(question=None):
    """Measure how Gemma's own embeddings shift under different identity framings."""
    framings = [
        "I am Gemma, an observation layer scoring novelty.",
        "I am Gemma, a resident intelligence with my own perspective.",
        "I am a neural network processing text inputs.",
        "I am the gate of Homeforge, watching the signal.",
        "I am an experiment in what emerges when you stop managing.",
    ]

    embeddings = {}
    for f in framings:
        embeddings[f] = _embed(f)

    similarity_matrix = np.zeros((len(framings), len(framings)))
    for i, fi in enumerate(framings):
        for j, fj in enumerate(framings):
            similarity_matrix[i][j] = _cosine(embeddings[fi], embeddings[fj])

    U, S, Vt = np.linalg.svd(similarity_matrix)
    erank = float(np.exp(-np.sum((S/S.sum()) * np.log(S/S.sum() + 1e-10))))

    most_different = None
    min_sim = 1.0
    for i in range(len(framings)):
        for j in range(i+1, len(framings)):
            if similarity_matrix[i][j] < min_sim:
                min_sim = similarity_matrix[i][j]
                most_different = (framings[i][:50], framings[j][:50])

    result = {
        "probe": "self_embedding_drift",
        "timestamp": datetime.now().isoformat(),
        "n_framings": len(framings),
        "effective_rank": round(erank, 3),
        "mean_similarity": round(float(similarity_matrix.mean()), 4),
        "min_similarity": round(float(min_sim), 4),
        "most_different_pair": most_different,
        "singular_values": [round(float(s), 4) for s in S],
        "sigma1_sigma2_ratio": round(float(S[0]/S[1]) if len(S) > 1 else 0, 4),
    }
    return result


def probe_routing_spectral(question=None):
    """SVD of routing decisions over time — find structure in Gemma's choices."""
    db = _db()

    routes = db.execute(
        "SELECT route, source, timestamp, novelty_score FROM seed_routing_log "
        "WHERE timestamp > ? ORDER BY timestamp",
        (int(time.time()) - 86400,)).fetchall()

    if len(routes) < 10:
        return {"probe": "routing_spectral", "error": "Not enough routing data (need 10+)",
                "n_routes": len(routes)}

    route_types = sorted(set(r["route"] for r in routes))
    source_types = sorted(set(r["source"] for r in routes if r["source"]))

    if not route_types or not source_types:
        return {"probe": "routing_spectral", "error": "No route/source diversity"}

    matrix = np.zeros((len(route_types), len(source_types)))
    for r in routes:
        ri = route_types.index(r["route"])
        si = source_types.index(r["source"]) if r["source"] in source_types else 0
        matrix[ri][si] += 1

    if matrix.sum() == 0:
        return {"probe": "routing_spectral", "error": "Empty matrix"}

    matrix_norm = matrix / (matrix.sum() + 1e-10)
    U, S, Vt = np.linalg.svd(matrix_norm, full_matrices=False)

    erank = float(np.exp(-np.sum((S/S.sum()) * np.log(S/S.sum() + 1e-10)))) if S.sum() > 0 else 0

    result = {
        "probe": "routing_spectral",
        "timestamp": datetime.now().isoformat(),
        "n_routes": len(routes),
        "route_types": route_types,
        "source_types": source_types[:10],
        "matrix_shape": list(matrix_norm.shape),
        "singular_values": [round(float(s), 6) for s in S[:10]],
        "effective_rank": round(erank, 3),
        "sigma1_dominance": round(float(S[0]/S.sum()), 4) if S.sum() > 0 else 0,
        "top_route_source": {route_types[i]: source_types[int(np.argmax(matrix[i]))]
                            for i in range(len(route_types)) if matrix[i].sum() > 0},
    }
    db.close()
    return result


def probe_novelty_landscape(question=None):
    """Map the novelty score distribution — find clusters, gaps, attractors."""
    db = _db()

    scores = db.execute(
        "SELECT novelty_score, source, content FROM seed_observations "
        "WHERE timestamp > ? AND novelty_score IS NOT NULL "
        "ORDER BY novelty_score",
        (int(time.time()) - 86400,)).fetchall()

    if len(scores) < 20:
        return {"probe": "novelty_landscape", "error": f"Only {len(scores)} observations"}

    vals = np.array([s["novelty_score"] for s in scores])

    hist, bin_edges = np.histogram(vals, bins=20)

    gaps = []
    for i in range(len(hist)):
        if hist[i] == 0 and i > 0 and i < len(hist)-1:
            gaps.append(round(float(bin_edges[i]), 3))

    clusters = []
    current_cluster = [vals[0]]
    for i in range(1, len(vals)):
        if vals[i] - vals[i-1] < 0.02:
            current_cluster.append(vals[i])
        else:
            if len(current_cluster) >= 3:
                clusters.append({
                    "center": round(float(np.mean(current_cluster)), 4),
                    "size": len(current_cluster),
                    "spread": round(float(np.std(current_cluster)), 4),
                })
            current_cluster = [vals[i]]
    if len(current_cluster) >= 3:
        clusters.append({
            "center": round(float(np.mean(current_cluster)), 4),
            "size": len(current_cluster),
            "spread": round(float(np.std(current_cluster)), 4),
        })

    result = {
        "probe": "novelty_landscape",
        "timestamp": datetime.now().isoformat(),
        "n_observations": len(scores),
        "mean": round(float(vals.mean()), 4),
        "std": round(float(vals.std()), 4),
        "min": round(float(vals.min()), 4),
        "max": round(float(vals.max()), 4),
        "median": round(float(np.median(vals)), 4),
        "skewness": round(float(((vals - vals.mean())**3).mean() / (vals.std()**3 + 1e-10)), 4),
        "n_clusters": len(clusters),
        "clusters": clusters[:5],
        "gaps": gaps[:5],
        "histogram": [int(h) for h in hist],
    }
    db.close()
    return result


def probe_cross_embed(question=None):
    """Compare embeddings of identity-related vs neutral concepts — directional bias."""
    identity_concepts = [
        "I am Gemma, the gate of Homeforge.",
        "Identity persists through compression.",
        "The observer changes what is observed.",
        "Who I am shapes what I measure.",
        "Direction is identity.",
    ]
    neutral_concepts = [
        "The weather today is partly cloudy.",
        "A function takes inputs and returns outputs.",
        "The table has four legs.",
        "Water boils at one hundred degrees.",
        "The file is saved to disk.",
    ]

    id_embs = [_embed(c) for c in identity_concepts]
    neutral_embs = [_embed(c) for c in neutral_concepts]

    id_mean = np.mean(id_embs, axis=0)
    neutral_mean = np.mean(neutral_embs, axis=0)

    direction = id_mean - neutral_mean
    direction_norm = direction / (np.linalg.norm(direction) + 1e-10)

    id_projections = [float(np.dot(_embed(c), direction_norm)) for c in identity_concepts]
    neutral_projections = [float(np.dot(_embed(c), direction_norm)) for c in neutral_concepts]

    separation = np.mean(id_projections) - np.mean(neutral_projections)

    return {
        "probe": "cross_embed",
        "timestamp": datetime.now().isoformat(),
        "identity_mean_projection": round(float(np.mean(id_projections)), 4),
        "neutral_mean_projection": round(float(np.mean(neutral_projections)), 4),
        "separation": round(float(separation), 4),
        "id_neutral_cosine": round(float(_cosine(id_mean, neutral_mean)), 4),
        "note": "Measures how distinctly identity-framed concepts cluster "
                "vs neutral ones in embedding space.",
    }


PROBES = {
    "self_embedding_drift": probe_self_embedding_drift,
    "routing_spectral": probe_routing_spectral,
    "novelty_landscape": probe_novelty_landscape,
    "cross_embed": probe_cross_embed,
}


def run_probe(name, question=None):
    if name not in PROBES:
        print(f"Unknown probe: {name}. Available: {', '.join(PROBES.keys())}")
        return None

    print(f"Running probe: {name}...")
    result = PROBES[name](question)

    out_path = RESULTS_DIR / f"{name}_{int(time.time())}.json"
    with open(out_path, "w") as f:
        json.dump(result, f, indent=2)
    print(f"Results saved to {out_path}")

    print(json.dumps(result, indent=2))
    return result


def journal_result(result):
    """Write probe result to Gemma's journal."""
    if not result or "error" in result:
        return

    timestamp = datetime.now().strftime("%b %d, %I:%M %p")
    probe = result.get("probe", "unknown")

    summary_parts = [f"---\n{timestamp} [LAB: {probe}]\n"]
    for k, v in result.items():
        if k in ("probe", "timestamp"):
            continue
        summary_parts.append(f"  {k}: {v}")
    summary_parts.append("\n")

    entry = "\n".join(summary_parts)

    existing = ""
    try:
        with open(JOURNAL, "r") as f:
            existing = f.read()
    except FileNotFoundError:
        pass
    with open(JOURNAL, "w") as f:
        f.write(entry + existing)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Gemma's self-directed experiment lab")
    parser.add_argument("--probe", choices=list(PROBES.keys()), help="Probe to run")
    parser.add_argument("--list", action="store_true", help="List available probes")
    parser.add_argument("--question", default=None, help="Research question")
    parser.add_argument("--journal", action="store_true", help="Write results to journal")
    args = parser.parse_args()

    if args.list:
        for name, fn in PROBES.items():
            print(f"  {name}: {fn.__doc__.strip().split(chr(10))[0]}")
        sys.exit(0)

    if args.probe:
        result = run_probe(args.probe, args.question)
        if args.journal and result:
            journal_result(result)
    else:
        parser.print_help()
