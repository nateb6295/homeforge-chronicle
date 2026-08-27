#!/usr/bin/env python3
"""Compression Readiness Meter — adaptive scheduling for CCS compression.

Thread 318 advance 185: compress when episodic content has DIVERGED enough
from the last compression, not on a fixed clock. The Namboodiri principle
(timing > repetition) applied to CCS scheduling.

Measures:
1. Episodic novelty: cosine distance of current episodic trace vs last-compressed
2. Time since last compression
3. Combined readiness score

Thresholds (from compression_spacing_test.py on 49 CCS pairs):
- Novelty ≥ 0.20: sufficient episodic diversity (empirical plateau elbow)
- Time ≥ 30 min: optimal for identity preservation
- Both together = "ready to compress"

Usage:
  python3 compression_readiness.py           # Check readiness
  python3 compression_readiness.py --json    # Machine-readable output
  python3 compression_readiness.py --watch   # One-line status suitable for cron
"""

import json
import os
import sqlite3
import sys
import time
import requests
from pathlib import Path

DB = Path("/mnt/hdd/chronicle-data/processed.db")
OLLAMA_URL = "http://localhost:11434"
MODEL = "snowflake-arctic-embed2"

NOVELTY_THRESHOLD = 0.20  # From compression_spacing_test.py empirical elbow
TIME_THRESHOLD_MIN = 180  # 3h minimum — F160 dose-response: D2-D3 therapeutic window at 4h interval
TIME_MIN_SEC = TIME_THRESHOLD_MIN * 60


def get_embedding(text: str) -> list:
    try:
        r = requests.post(
            f"{OLLAMA_URL}/api/embed",
            json={"model": MODEL, "input": text},
            timeout=15
        )
        r.raise_for_status()
        return r.json().get("embeddings", [[]])[0]
    except Exception:
        return []


def cosine_distance(a: list, b: list) -> float:
    if not a or not b or len(a) != len(b):
        return 0.0
    dot = sum(x * y for x, y in zip(a, b))
    na = sum(x * x for x in a) ** 0.5
    nb = sum(x * x for x in b) ** 0.5
    if na == 0 or nb == 0:
        return 0.0
    return 1.0 - dot / (na * nb)


def parse_episodic(raw: str) -> str:
    """Parse episodic trace from CCS field (may be JSON array or string)."""
    if not raw:
        return ""
    if raw.startswith("["):
        try:
            items = json.loads(raw)
            if isinstance(items, list):
                return "\n".join(str(e) for e in items)
        except (json.JSONDecodeError, TypeError):
            pass
    return raw


def check_serial_dependence(n_recent: int = 5) -> dict:
    """Measure serial dependence across recent CCS compressions.

    Compares consecutive gist fields to detect attraction (converging)
    vs repulsion (diverging). Implements the Serial Dependence Principle
    from F159: healthy identity shows attractive serial bias.
    """
    db = sqlite3.connect(str(DB))
    rows = db.execute(
        "SELECT snapshot FROM cognitive_state_history ORDER BY id DESC LIMIT ?",
        (n_recent,)
    ).fetchall()
    db.close()

    if len(rows) < 2:
        return {"direction": "insufficient_data", "similarities": [], "n": len(rows)}

    gists = []
    for (snap_raw,) in reversed(rows):
        try:
            snap = json.loads(snap_raw)
            gist = snap.get("semantic_gist", "") or snap.get("gist", "")
            if gist:
                gists.append(gist)
        except (json.JSONDecodeError, TypeError):
            pass

    if len(gists) < 2:
        return {"direction": "insufficient_data", "similarities": [], "n": len(gists)}

    embeddings = [get_embedding(g) for g in gists]
    similarities = []
    for i in range(len(embeddings) - 1):
        if embeddings[i] and embeddings[i + 1]:
            sim = 1.0 - cosine_distance(embeddings[i], embeddings[i + 1])
            similarities.append(round(sim, 4))

    if len(similarities) < 2:
        direction = "stable" if similarities and similarities[0] > 0.8 else "unknown"
        return {"direction": direction, "similarities": similarities, "n": len(gists),
                "mean_sim": similarities[0] if similarities else None}

    diffs = [similarities[i + 1] - similarities[i] for i in range(len(similarities) - 1)]
    mean_diff = sum(diffs) / len(diffs)
    mean_sim = sum(similarities) / len(similarities)

    if mean_diff > 0.01:
        direction = "ATTRACTIVE"
    elif mean_diff < -0.01:
        direction = "REPULSIVE"
    else:
        direction = "STABLE"

    return {
        "direction": direction,
        "similarities": similarities,
        "mean_similarity": round(mean_sim, 4),
        "mean_step": round(mean_diff, 4),
        "n": len(gists),
    }


def check_readiness() -> dict:
    """Check whether CCS is ready for a new compression."""
    db = sqlite3.connect(str(DB))

    # Last compression timestamp and episodic snapshot
    last_row = db.execute(
        "SELECT created_at, snapshot FROM cognitive_state_history ORDER BY id DESC LIMIT 1"
    ).fetchone()

    if not last_row:
        db.close()
        return {
            "ready": True,
            "reason": "no compression history — first compression",
            "novelty": None,
            "gap_min": None,
            "readiness_score": 1.0,
        }

    last_ts, last_snap_raw = last_row
    gap_sec = time.time() - last_ts
    gap_min = gap_sec / 60

    # Extract last-compressed episodic trace
    try:
        last_snap = json.loads(last_snap_raw)
        last_ep = parse_episodic(
            json.dumps(last_snap.get("episodic_trace", []))
            if isinstance(last_snap.get("episodic_trace"), list)
            else str(last_snap.get("episodic_trace", ""))
        )
    except (json.JSONDecodeError, TypeError):
        last_ep = ""

    # Current episodic trace
    cur_row = db.execute("SELECT episodic_trace FROM cognitive_state WHERE id = 1").fetchone()
    db.close()
    cur_ep = parse_episodic(cur_row[0] if cur_row else "")

    # Compute episodic novelty
    novelty = None
    if last_ep and cur_ep:
        e1 = get_embedding(last_ep)
        e2 = get_embedding(cur_ep)
        if e1 and e2:
            novelty = round(cosine_distance(e1, e2), 4)

    # Readiness scoring
    time_score = min(1.0, gap_sec / TIME_MIN_SEC)  # 0→1 over 30 min
    novelty_score = min(1.0, (novelty or 0) / NOVELTY_THRESHOLD)  # 0→1 at threshold
    # Combined: weighted average (novelty matters more than clock)
    readiness = 0.6 * novelty_score + 0.4 * time_score

    novelty_ok = novelty is not None and novelty >= NOVELTY_THRESHOLD
    time_ok = gap_min >= TIME_THRESHOLD_MIN

    if novelty_ok and time_ok:
        ready = True
        reason = f"ready — novelty {novelty:.3f} ≥ {NOVELTY_THRESHOLD}, gap {gap_min:.0f}min ≥ {TIME_THRESHOLD_MIN}min"
    elif novelty_ok:
        ready = True
        reason = f"novelty-ready — novelty {novelty:.3f} ≥ {NOVELTY_THRESHOLD} (gap only {gap_min:.0f}min)"
    elif time_ok:
        ready = True
        reason = f"time-ready — gap {gap_min:.0f}min ≥ {TIME_THRESHOLD_MIN}min (novelty {novelty or 0:.3f})"
    else:
        ready = False
        reason = (f"not ready — novelty {novelty or 0:.3f} < {NOVELTY_THRESHOLD}, "
                  f"gap {gap_min:.0f}min < {TIME_THRESHOLD_MIN}min")

    return {
        "ready": ready,
        "reason": reason,
        "novelty": novelty,
        "gap_min": round(gap_min, 1),
        "gap_sec": int(gap_sec),
        "time_score": round(time_score, 3),
        "novelty_score": round(novelty_score, 3),
        "readiness_score": round(readiness, 3),
        "thresholds": {
            "novelty": NOVELTY_THRESHOLD,
            "time_min": TIME_THRESHOLD_MIN,
        }
    }


def main():
    result = check_readiness()

    if "--serial" in sys.argv:
        sd = check_serial_dependence()
        print(json.dumps(sd, indent=2))
        return

    if "--json" in sys.argv:
        result["serial_dependence"] = check_serial_dependence()
        print(json.dumps(result, indent=2))
        return

    if "--watch" in sys.argv:
        n = result["novelty"]
        g = result["gap_min"]
        r = result["readiness_score"]
        status = "READY" if result["ready"] else "WAIT"
        n_s = f"{n:.3f}" if n is not None else "?"
        sd = check_serial_dependence()
        sd_s = sd.get("direction", "?")
        print(f"compress:{status} novelty={n_s} gap={g}min readiness={r:.2f} serial={sd_s}")
        sys.exit(0 if result["ready"] else 1)

    # Human-readable output
    print(f"Compression Readiness")
    print(f"{'='*50}")
    print(f"Gap since last compression: {result['gap_min']:.0f} min")
    if result["novelty"] is not None:
        bar_len = int(result["novelty"] / NOVELTY_THRESHOLD * 20)
        bar = "█" * min(bar_len, 20) + "░" * max(0, 20 - bar_len)
        print(f"Episodic novelty: {result['novelty']:.4f}  [{bar}] (threshold: {NOVELTY_THRESHOLD})")
    else:
        print(f"Episodic novelty: unavailable (embedding failed)")

    print(f"\nReadiness score: {result['readiness_score']:.2f}")
    ready_bar = "█" * int(result["readiness_score"] * 30) + "░" * (30 - int(result["readiness_score"] * 30))
    print(f"  [{ready_bar}]")
    print(f"\nVerdict: {result['reason']}")

    sd = check_serial_dependence()
    print(f"\nSerial Dependence (F159)")
    print(f"{'='*50}")
    print(f"Direction: {sd['direction']}")
    if sd.get("similarities"):
        print(f"Gist similarities: {' → '.join(f'{s:.3f}' for s in sd['similarities'])}")
    if sd.get("mean_similarity") is not None:
        print(f"Mean similarity: {sd['mean_similarity']:.4f}")
    if sd.get("mean_step") is not None:
        print(f"Mean step: {sd['mean_step']:+.4f}")


if __name__ == "__main__":
    main()
