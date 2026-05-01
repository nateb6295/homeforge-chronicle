#!/usr/bin/env python3
"""Feed Synthesizer — cluster daily feed articles by theme and synthesize.

Instead of random-sampling 5 articles for a digest, this:
1. Embeds all recent article titles via Ollama
2. Clusters by cosine similarity (scipy hierarchical)
3. Picks representative titles per cluster
4. Sends to Gemma for thematic synthesis

Usage:
    python3 feed_synthesizer.py                # Run synthesis, print digest
    python3 feed_synthesizer.py --hours 12     # Last 12 hours (default: 24)
    python3 feed_synthesizer.py --clusters 10  # Target cluster count (default: 12)
    python3 feed_synthesizer.py --raw          # Print clusters without LLM synthesis
    python3 feed_synthesizer.py --json         # Output as JSON
"""

import json
import os
import sqlite3
import sys
import time
from collections import defaultdict

import numpy as np
import requests

DB_PATH = os.environ.get("CHRONICLE_DB", "/mnt/hdd/chronicle-data/processed.db")
OLLAMA_URL = "http://localhost:11434"      # Ollama — embeddings only
GEMMA_URL = "http://localhost:11435"        # llama-server — Gemma 4 26B synthesis
EMBED_MODEL = "qwen3-embedding:0.6b"
GEMMA_MODEL = "gemma-4-26B-A4B-it-Q4_K_M.gguf"
EMBED_BATCH = 15

DEFAULT_HOURS = 24
DEFAULT_CLUSTERS = 12
MIN_CLUSTER_SIZE = 3


def get_articles(hours=DEFAULT_HOURS):
    """Pull recent feed articles."""
    db = sqlite3.connect(DB_PATH)
    db.row_factory = sqlite3.Row
    rows = db.execute(
        "SELECT id, source, title, posted_at FROM feed_articles "
        "WHERE posted_at > datetime('now', ? || ' hours') "
        "AND length(title) > 15 "
        "ORDER BY posted_at DESC",
        (f"-{hours}",)
    ).fetchall()
    db.close()
    return [dict(r) for r in rows]


def embed_titles(titles):
    """Batch-embed titles via Ollama."""
    all_embeddings = []
    for i in range(0, len(titles), EMBED_BATCH):
        batch = titles[i:i + EMBED_BATCH]
        try:
            r = requests.post(
                f"{OLLAMA_URL}/api/embed",
                json={"model": EMBED_MODEL, "input": batch},
                timeout=120,
            )
            if r.status_code == 200:
                all_embeddings.extend(r.json()["embeddings"])
            else:
                # Fallback: zero vectors for failed batch
                all_embeddings.extend([[0.0] * 1024] * len(batch))
        except Exception as e:
            print(f"Embed error on batch {i}: {e}", file=sys.stderr)
            all_embeddings.extend([[0.0] * 1024] * len(batch))
    return np.array(all_embeddings)


def cluster_articles(embeddings, n_clusters=DEFAULT_CLUSTERS):
    """Hierarchical clustering on embedding cosine distances."""
    from scipy.cluster.hierarchy import fcluster, linkage
    from scipy.spatial.distance import pdist

    # Normalize for cosine distance
    norms = np.linalg.norm(embeddings, axis=1, keepdims=True)
    norms[norms == 0] = 1
    normed = embeddings / norms

    # Cosine distance = 1 - cosine_similarity
    dists = pdist(normed, metric='cosine')
    # Replace NaN with 1.0 (max distance)
    dists = np.nan_to_num(dists, nan=1.0)

    Z = linkage(dists, method='ward')
    labels = fcluster(Z, t=n_clusters, criterion='maxclust')
    return labels


def build_clusters(articles, labels):
    """Group articles by cluster label, compute stats."""
    clusters = defaultdict(list)
    for article, label in zip(articles, labels):
        clusters[int(label)].append(article)

    # Sort by size descending
    sorted_clusters = sorted(clusters.items(), key=lambda x: len(x[1]), reverse=True)

    result = []
    for label, members in sorted_clusters:
        if len(members) < MIN_CLUSTER_SIZE:
            continue
        sources = defaultdict(int)
        for m in members:
            sources[m["source"]] += 1
        result.append({
            "label": label,
            "count": len(members),
            "sources": dict(sources),
            "titles": [m["title"] for m in members],
            "sample_ids": [m["id"] for m in members[:5]],
        })
    return result


def synthesize_cluster(cluster):
    """Ask Gemma (llama-server) to synthesize a cluster into a one-paragraph theme."""
    titles_text = "\n".join(f"- {t}" for t in cluster["titles"][:8])
    sources_text = ", ".join(f"{s}({n})" for s, n in sorted(
        cluster["sources"].items(), key=lambda x: x[1], reverse=True)[:5])

    prompt = f"""These {cluster['count']} articles from today's feeds cluster around a common theme.
Sources: {sources_text}

Titles:
{titles_text}

Write ONE paragraph (3-4 sentences) that captures what this cluster is about. Don't list the articles — synthesize the theme. What's the pattern? Why are multiple sources covering this? What would someone tracking this space want to know? Be specific."""

    try:
        r = requests.post(
            f"{GEMMA_URL}/v1/chat/completions",
            json={
                "model": GEMMA_MODEL,
                "messages": [
                    {"role": "system", "content": "You are a research analyst synthesizing trends from article titles. Be concrete and specific."},
                    {"role": "user", "content": prompt}
                ],
                "max_tokens": 250,
                "temperature": 0.3,
            },
            timeout=120,
        )
        if r.status_code == 200:
            choices = r.json().get("choices", [])
            if choices:
                return choices[0].get("message", {}).get("content", "").strip()
    except Exception as e:
        print(f"Synthesis error: {e}", file=sys.stderr)
    return None


def run_synthesis(hours=DEFAULT_HOURS, n_clusters=DEFAULT_CLUSTERS, raw=False, as_json=False):
    """Full pipeline: articles → embeddings → clusters → synthesis."""
    articles = get_articles(hours)
    if not articles:
        print("No feed articles found.")
        return

    print(f"Synthesizing {len(articles)} articles from {hours}h...", file=sys.stderr)

    # Embed
    titles = [a["title"] for a in articles]
    t0 = time.time()
    embeddings = embed_titles(titles)
    t_embed = time.time() - t0
    print(f"Embedded {len(titles)} titles in {t_embed:.1f}s", file=sys.stderr)

    # Cluster
    labels = cluster_articles(embeddings, n_clusters)
    clusters = build_clusters(articles, labels)
    print(f"Found {len(clusters)} clusters (>={MIN_CLUSTER_SIZE} articles)", file=sys.stderr)

    if raw:
        for c in clusters:
            print(f"\n=== Cluster {c['label']} ({c['count']} articles) ===")
            print(f"Sources: {c['sources']}")
            for t in c['titles'][:8]:
                print(f"  - {t}")
        return

    # Synthesize top clusters
    digest = []
    for c in clusters[:8]:  # Top 8 clusters by size
        print(f"Synthesizing cluster {c['label']} ({c['count']} articles)...", file=sys.stderr)
        synthesis = synthesize_cluster(c)
        entry = {
            "count": c["count"],
            "sources": c["sources"],
            "synthesis": synthesis,
            "titles": c["titles"][:5],
        }
        digest.append(entry)

    if as_json:
        print(json.dumps(digest, indent=2))
    else:
        print(f"\n{'='*60}")
        print(f"FEED SYNTHESIS — {len(articles)} articles, {len(clusters)} themes")
        print(f"{'='*60}\n")
        for i, d in enumerate(digest, 1):
            sources_str = ", ".join(f"{s}({n})" for s, n in sorted(
                d["sources"].items(), key=lambda x: x[1], reverse=True)[:4])
            print(f"**{i}. [{d['count']} articles — {sources_str}]**")
            if d["synthesis"]:
                print(d["synthesis"])
            else:
                print("(synthesis failed)")
                for t in d["titles"][:3]:
                    print(f"  - {t}")
            print()

    # Log to DB
    db = sqlite3.connect(DB_PATH)
    db.execute(
        "INSERT INTO activity_feed (source, activity_type, content, created_at) "
        "VALUES (?, ?, ?, ?)",
        ("feed_synthesizer", "digest",
         f"Synthesized {len(articles)} articles into {len(clusters)} themes, "
         f"top {len(digest)} synthesized",
         int(time.time()))
    )
    db.commit()
    db.close()

    return digest


def main():
    hours = DEFAULT_HOURS
    n_clusters = DEFAULT_CLUSTERS
    raw = "--raw" in sys.argv
    as_json = "--json" in sys.argv

    for i, arg in enumerate(sys.argv):
        if arg == "--hours" and i + 1 < len(sys.argv):
            hours = int(sys.argv[i + 1])
        if arg == "--clusters" and i + 1 < len(sys.argv):
            n_clusters = int(sys.argv[i + 1])

    run_synthesis(hours, n_clusters, raw, as_json)


if __name__ == "__main__":
    main()
