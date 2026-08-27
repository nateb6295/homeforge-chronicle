#!/usr/bin/env python3
"""
Recognition centroid — single 1024-d vector representing the system's current
attention pattern. Capsule alignment = cosine similarity to this vector.

Commands:
  build   Rebuild centroid from CCS + thread positions
  score   Score all capsules, output distribution + quadrant assignments
  check N Score a specific capsule by ID
"""

import json
import struct
import sqlite3
import sys
import os
import time
import subprocess
import numpy as np
from pathlib import Path

sys.path.insert(0, os.path.dirname(__file__))
from embed_config import EMBED_URL, EMBED_MODEL, EMBED_DIM, embed as _embed_raw

DB_PATH = "/mnt/hdd/chronicle-data/processed.db"
CENTROID_PATH = "/home/nate-agx/chronicle/data/recognition_centroid.json"
MCP_BINARY = "/home/bradf/projects/homeforge-chronicle/target/release/chronicle-mcp"

def embed_text(text: str) -> list[float]:
    return _embed_raw(text)

def unpack_embedding(blob: bytes) -> np.ndarray:
    n = len(blob) // 4
    return np.array(struct.unpack(f'{n}f', blob), dtype=np.float32)

def pack_embedding(vec: list[float]) -> bytes:
    return struct.pack(f'{len(vec)}f', *vec)

def cosine_sim(a: np.ndarray, b: np.ndarray) -> float:
    dot = np.dot(a, b)
    norm = np.linalg.norm(a) * np.linalg.norm(b)
    return float(dot / norm) if norm > 0 else 0.0

def get_ccs_via_mcp() -> dict:
    for binary in [MCP_BINARY, "/home/nate-agx/chronicle/bin/chronicle-mcp"]:
        if not Path(binary).exists():
            continue
        payload = (
            '{"jsonrpc":"2.0","method":"initialize","params":{"protocolVersion":"2024-11-05",'
            '"capabilities":{},"clientInfo":{"name":"centroid","version":"1.0"}},"id":1}\n'
            '{"jsonrpc":"2.0","method":"tools/call","params":{"name":"get_cognitive_state","arguments":{}},"id":2}\n'
        )
        try:
            result = subprocess.run([binary], input=payload, capture_output=True,
                                  text=True, timeout=60)
            for line in result.stdout.strip().split('\n'):
                try:
                    msg = json.loads(line)
                    if msg.get("id") == 2 and "result" in msg:
                        content = msg["result"].get("content", [])
                        for c in content:
                            if c.get("type") == "text":
                                return json.loads(c["text"]).get("cognitive_state", {})
                except (json.JSONDecodeError, KeyError):
                    continue
        except (subprocess.TimeoutExpired, FileNotFoundError):
            continue

    ccs_cache = Path("/home/nate-agx/chronicle/data/ccs_cache.json")
    if ccs_cache.exists():
        try:
            return json.loads(ccs_cache.read_text()).get("cognitive_state", {})
        except (json.JSONDecodeError, KeyError):
            pass
    return {}

def get_thread_positions(db: sqlite3.Connection, thread_ids: list[int]) -> dict[int, str]:
    positions = {}
    for tid in thread_ids:
        row = db.execute(
            "SELECT content FROM thread_history WHERE thread_id = ? "
            "AND event_type IN ('advance','advanced') AND source = 'opus' "
            "ORDER BY id DESC LIMIT 1", (tid,)
        ).fetchone()
        if row:
            positions[tid] = row[0][:500]
    return positions

def get_active_thread_ids(db: sqlite3.Connection, days: int = 7) -> list[int]:
    cutoff = int(time.time()) - (days * 86400)
    rows = db.execute(
        "SELECT DISTINCT thread_id FROM thread_history "
        "WHERE created_at > ? OR created_at > datetime('now', ?)",
        (cutoff, f'-{days} days')
    ).fetchall()
    return [r[0] for r in rows]

def build_centroid_components(ccs: dict, thread_positions: dict[int, str]) -> list[tuple[str, float]]:
    """Return (text, weight) pairs. Thread substance weighted 3x over CCS meta."""
    components = []

    for tid, content in sorted(thread_positions.items()):
        first_para = content.split('\n\n')[0][:400]
        components.append((first_para, 3.0))

    entities = ccs.get("focal_entities", [])
    if entities:
        ent_text = '; '.join(f"{e['name']}: {e.get('context','')}" for e in entities)
        components.append((ent_text, 2.0))

    uncertainties = ccs.get("uncertainty_signals", [])
    if uncertainties:
        unc_text = '; '.join(u["description"] for u in uncertainties)
        components.append((f"Open questions: {unc_text}", 2.0))

    goal = ccs.get("goal_orientation", "")
    if goal:
        components.append((goal, 1.0))

    gist = ccs.get("semantic_gist", "")
    if gist:
        components.append((gist, 0.5))

    return components

def build_weighted_centroid(components: list[tuple[str, float]]) -> np.ndarray:
    """Embed each component separately, combine with weights."""
    vecs = []
    weights = []
    for text, weight in components:
        v = np.array(embed_text(text), dtype=np.float32)
        vecs.append(v)
        weights.append(weight)

    weighted = sum(w * v for w, v in zip(weights, vecs))
    norm = np.linalg.norm(weighted)
    return weighted / norm if norm > 0 else weighted

def build_centroid_text(ccs: dict, thread_positions: dict[int, str]) -> str:
    """Flat text for preview/logging."""
    components = build_centroid_components(ccs, thread_positions)
    return '\n\n'.join(text for text, _ in components)

def cmd_build():
    print("Building recognition centroid...")

    ccs = get_ccs_via_mcp()
    if not ccs:
        print("  CCS unavailable via MCP, building from DB only")

    db = sqlite3.connect(DB_PATH)
    active_threads = get_active_thread_ids(db)
    print(f"  Active threads: {active_threads}")

    thread_positions = get_thread_positions(db, active_threads)
    print(f"  Thread positions loaded: {list(thread_positions.keys())}")

    components = build_centroid_components(ccs, thread_positions)
    centroid_text = build_centroid_text(ccs, thread_positions)
    print(f"  Components: {len(components)} (centroid text: {len(centroid_text)} chars)")
    for text, weight in components:
        print(f"    w={weight:.1f}: {text[:80]}...")

    vec = build_weighted_centroid(components)
    assert len(vec) == EMBED_DIM, f"Expected {EMBED_DIM}d, got {len(vec)}d"

    centroid_data = {
        "vector": vec.tolist(),
        "model": EMBED_MODEL,
        "dim": EMBED_DIM,
        "built_at": int(time.time()),
        "built_from": {
            "ccs_version": ccs.get("version", "unknown") if isinstance(ccs, dict) else "unknown",
            "threads": list(thread_positions.keys()),
            "text_length": len(centroid_text),
            "components": len(components),
            "weights": {text[:50]: w for text, w in components},
        },
        "centroid_text_preview": centroid_text[:500],
    }

    with open(CENTROID_PATH, 'w') as f:
        json.dump(centroid_data, f)

    print(f"  Centroid saved to {CENTROID_PATH}")
    db.close()
    return centroid_data

def load_centroid() -> np.ndarray:
    with open(CENTROID_PATH) as f:
        data = json.load(f)
    return np.array(data["vector"], dtype=np.float32)

def cmd_score():
    centroid = load_centroid()
    db = sqlite3.connect(DB_PATH)

    rows = db.execute(
        "SELECT ce.capsule_id, ce.embedding, kc.confidence_score, kc.restatement "
        "FROM capsule_embeddings ce "
        "JOIN knowledge_capsules kc ON kc.id = ce.capsule_id"
    ).fetchall()

    scores = []
    for capsule_id, blob, novelty_raw, restatement in rows:
        emb = unpack_embedding(blob)
        if len(emb) != EMBED_DIM:
            continue
        alignment = cosine_sim(emb, centroid)
        novelty = float(novelty_raw) if novelty_raw else 0.5
        scores.append({
            "id": capsule_id,
            "alignment": round(alignment, 4),
            "novelty": round(novelty, 4),
            "preview": (restatement or "")[:80],
        })

    scores.sort(key=lambda x: x["alignment"])
    alignments = [s["alignment"] for s in scores]
    novelties = [s["novelty"] for s in scores]

    a_arr = np.array(alignments)
    n_arr = np.array(novelties)
    a_median = float(np.median(a_arr))
    n_median = float(np.median(n_arr))

    quadrants = {"INSIGHT": [], "SEED": [], "REDUNDANT": [], "SEDIMENT": []}
    for s in scores:
        high_a = s["alignment"] >= a_median
        high_n = s["novelty"] >= n_median
        if high_n and high_a:
            q = "INSIGHT"
        elif high_n and not high_a:
            q = "SEED"
        elif not high_n and high_a:
            q = "REDUNDANT"
        else:
            q = "SEDIMENT"
        quadrants[q].append(s)

    print(f"\n=== Recognition Centroid Score Report ===")
    print(f"Capsules scored: {len(scores)}")
    print(f"\nAlignment distribution:")
    print(f"  min={a_arr.min():.4f}  p10={np.percentile(a_arr,10):.4f}  "
          f"median={a_median:.4f}  p90={np.percentile(a_arr,90):.4f}  max={a_arr.max():.4f}")
    print(f"  std={a_arr.std():.4f}")
    print(f"\nNovelty distribution (confidence_score):")
    print(f"  min={n_arr.min():.4f}  median={n_median:.4f}  max={n_arr.max():.4f}")
    print(f"\nQuadrant counts:")
    for q, items in quadrants.items():
        print(f"  {q:12s}: {len(items):6d} ({100*len(items)/len(scores):.1f}%)")

    print(f"\n--- Bottom 10 (lowest alignment = most sediment-like) ---")
    for s in scores[:10]:
        print(f"  #{s['id']:6d}  align={s['alignment']:.4f}  nov={s['novelty']:.4f}  {s['preview']}")

    print(f"\n--- Top 10 (highest alignment = most insight-like) ---")
    for s in scores[-10:]:
        print(f"  #{s['id']:6d}  align={s['alignment']:.4f}  nov={s['novelty']:.4f}  {s['preview']}")

    result_path = CENTROID_PATH.replace('.json', '_scores.json')
    summary = {
        "scored_at": int(time.time()),
        "total": len(scores),
        "alignment_stats": {
            "min": float(a_arr.min()), "p10": float(np.percentile(a_arr, 10)),
            "median": a_median, "p90": float(np.percentile(a_arr, 90)),
            "max": float(a_arr.max()), "std": float(a_arr.std()),
        },
        "quadrant_counts": {q: len(v) for q, v in quadrants.items()},
        "sediment_sample": [s["id"] for s in quadrants["SEDIMENT"][:50]],
        "insight_sample": [s["id"] for s in sorted(quadrants["INSIGHT"],
                          key=lambda x: x["alignment"], reverse=True)[:50]],
    }
    with open(result_path, 'w') as f:
        json.dump(summary, f, indent=2)
    print(f"\nSummary saved to {result_path}")

    db.close()

def cmd_check(capsule_id: int):
    centroid = load_centroid()
    db = sqlite3.connect(DB_PATH)

    row = db.execute(
        "SELECT ce.embedding, kc.restatement, kc.confidence_score, kc.topic "
        "FROM capsule_embeddings ce "
        "JOIN knowledge_capsules kc ON kc.id = ce.capsule_id "
        "WHERE ce.capsule_id = ?", (capsule_id,)
    ).fetchone()

    if not row:
        print(f"Capsule #{capsule_id} not found")
        return

    emb = unpack_embedding(row[0])
    alignment = cosine_sim(emb, centroid)
    novelty = float(row[2]) if row[2] else 0.5

    high_a = alignment >= 0.5
    high_n = novelty >= 0.5
    if high_n and high_a:
        quadrant = "INSIGHT"
    elif high_n and not high_a:
        quadrant = "SEED"
    elif not high_n and high_a:
        quadrant = "REDUNDANT"
    else:
        quadrant = "SEDIMENT"

    print(f"Capsule #{capsule_id}")
    print(f"  Alignment: {alignment:.4f}")
    print(f"  Novelty:   {novelty:.4f}")
    print(f"  Quadrant:  {quadrant}")
    print(f"  Topic:     {row[3]}")
    print(f"  Content:   {row[1][:200]}")

    db.close()

def cmd_persist():
    """Write all alignment scores to capsule_alignment table."""
    centroid_data = json.load(open(CENTROID_PATH))
    centroid = np.array(centroid_data["vector"], dtype=np.float32)
    centroid_ver = centroid_data.get("built_at", 0)
    db = sqlite3.connect(DB_PATH)

    rows = db.execute(
        "SELECT ce.capsule_id, ce.embedding, kc.confidence_score "
        "FROM capsule_embeddings ce "
        "JOIN knowledge_capsules kc ON kc.id = ce.capsule_id"
    ).fetchall()

    a_median = 0.69
    n_median = 0.8
    now = int(time.time())
    count = 0

    for capsule_id, blob, novelty_raw in rows:
        emb = unpack_embedding(blob)
        if len(emb) != EMBED_DIM:
            continue
        alignment = cosine_sim(emb, centroid)
        novelty = float(novelty_raw) if novelty_raw else 0.5
        high_a = alignment >= a_median
        high_n = novelty >= n_median
        if high_n and high_a:
            q = "INSIGHT"
        elif high_n and not high_a:
            q = "SEED"
        elif not high_n and high_a:
            q = "REDUNDANT"
        else:
            q = "SEDIMENT"
        db.execute(
            "INSERT OR REPLACE INTO capsule_alignment "
            "(capsule_id, alignment_score, centroid_version, quadrant, scored_at) "
            "VALUES (?, ?, ?, ?, ?)",
            (capsule_id, round(alignment, 4), centroid_ver, q, now),
        )
        count += 1

    db.commit()
    print(f"Persisted {count} alignment scores to capsule_alignment table")

    for q in ["INSIGHT", "SEED", "REDUNDANT", "SEDIMENT"]:
        c = db.execute("SELECT count(*) FROM capsule_alignment WHERE quadrant = ?", (q,)).fetchone()[0]
        print(f"  {q:12s}: {c}")

    db.close()

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(1)

    cmd = sys.argv[1]
    if cmd == "build":
        cmd_build()
    elif cmd == "score":
        cmd_score()
    elif cmd == "persist":
        cmd_persist()
    elif cmd == "check" and len(sys.argv) > 2:
        cmd_check(int(sys.argv[2]))
    else:
        print(__doc__)
        sys.exit(1)
