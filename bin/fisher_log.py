#!/usr/bin/env python3
"""Fisher Information Logger — logs CCS field identity profiles after compression events.

Runs purpose_ablation.py and stores the Drop/kT scores as a JSONL time series.
This is the reward signal for future NGC-inspired learned CCS compression.

Usage:
  python3 fisher_log.py              # Run ablation and log
  python3 fisher_log.py --show       # Show recent profile history
  python3 fisher_log.py --summary    # Summary stats across all logged profiles

Thread 318 advance 170: Drop/kT IS a discrete approximation of Fisher information
on the CCS manifold. Logging it creates the training data for Objective #30.
"""

import json
import os
import sys
import time
import sqlite3
import requests
from pathlib import Path

DB = Path("/mnt/hdd/chronicle-data/processed.db")
LOG_FILE = Path(os.path.expanduser("~/chronicle/data/fisher_profiles.jsonl"))
OLLAMA_URL = "http://localhost:11434"
MODEL = "snowflake-arctic-embed2"


def get_ccs_fields() -> dict:
    """Read current CCS fields."""
    db = sqlite3.connect(str(DB))
    row = db.execute(
        "SELECT semantic_gist, goal_orientation, episodic_trace, "
        "focal_entities, constraints, predictive_cue, uncertainty_signals "
        "FROM cognitive_state WHERE id = 1"
    ).fetchone()
    db.close()
    if not row:
        return {}
    return {
        "semantic_gist": row[0] or "",
        "goal_orientation": row[1] or "",
        "episodic_trace": row[2] or "[]",
        "focal_entities": row[3] or "[]",
        "constraints": row[4] or "[]",
        "predictive_cue": row[5] or "",
        "uncertainty_signals": row[6] or "[]",
    }


def get_embedding(text: str) -> list:
    """Get embedding from Ollama."""
    try:
        r = requests.post(
            f"{OLLAMA_URL}/api/embed",
            json={"model": MODEL, "input": text},
            timeout=30
        )
        r.raise_for_status()
        return r.json().get("embeddings", [[]])[0]
    except Exception:
        return []


def cosine_sim(a: list, b: list) -> float:
    """Cosine similarity between two vectors."""
    if not a or not b or len(a) != len(b):
        return 0.0
    dot = sum(x * y for x, y in zip(a, b))
    norm_a = sum(x * x for x in a) ** 0.5
    norm_b = sum(x * x for x in b) ** 0.5
    if norm_a == 0 or norm_b == 0:
        return 0.0
    return dot / (norm_a * norm_b)


def render_ccs(fields: dict) -> str:
    """Render CCS fields into a single string."""
    parts = []
    for k, v in fields.items():
        if isinstance(v, str) and v.startswith("["):
            try:
                items = json.loads(v)
                if isinstance(items, list):
                    if items and isinstance(items[0], dict):
                        v = "; ".join(str(item) for item in items)
                    else:
                        v = "; ".join(str(item) for item in items)
            except (json.JSONDecodeError, TypeError):
                pass
        parts.append(f"{k}: {v}")
    return "\n".join(parts)


def ablate_field(fields: dict, field_name: str) -> dict:
    """Return fields with one field removed."""
    return {k: v for k, v in fields.items() if k != field_name}


def run_ablation() -> dict:
    """Run purpose ablation and return field profiles."""
    fields = get_ccs_fields()
    if not fields:
        print("ERROR: No CCS fields found")
        return {}

    # Full CCS embedding
    full_text = render_ccs(fields)
    full_emb = get_embedding(full_text)
    if not full_emb:
        print("ERROR: Could not get embedding (Ollama down?)")
        return {}

    profile = {}
    for field_name in fields:
        ablated = ablate_field(fields, field_name)
        ablated_text = render_ccs(ablated)
        ablated_emb = get_embedding(ablated_text)
        if not ablated_emb:
            continue

        sim = cosine_sim(full_emb, ablated_emb)
        drop = 1.0 - sim

        # Token count (rough: split on whitespace)
        field_val = fields[field_name]
        tokens = len(field_val.split())

        drop_per_kt = (drop / tokens * 1000) if tokens > 0 else 0.0

        profile[field_name] = {
            "drop": round(drop, 6),
            "tokens": tokens,
            "drop_per_kt": round(drop_per_kt, 4),
        }

    return profile


def log_profile(profile: dict):
    """Append profile to JSONL log."""
    # Get current CCS version
    db = sqlite3.connect(str(DB))
    row = db.execute(
        "SELECT id FROM cognitive_state_history ORDER BY id DESC LIMIT 1"
    ).fetchone()
    db.close()
    ccs_version = row[0] if row else 0

    entry = {
        "ts": int(time.time()),
        "ccs_version": ccs_version,
        "profile": profile,
    }

    LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(LOG_FILE, "a") as f:
        f.write(json.dumps(entry) + "\n")

    return entry


def show_recent(n: int = 5):
    """Show recent profiles."""
    if not LOG_FILE.exists():
        print("No profiles logged yet.")
        return

    entries = []
    with open(LOG_FILE) as f:
        for line in f:
            try:
                entries.append(json.loads(line))
            except json.JSONDecodeError:
                continue

    for entry in entries[-n:]:
        ts = time.strftime("%Y-%m-%d %H:%M", time.localtime(entry["ts"]))
        v = entry.get("ccs_version", "?")
        print(f"\n[{ts}] CCS v{v}")
        profile = entry["profile"]
        for field, data in sorted(profile.items(), key=lambda x: -x[1]["drop_per_kt"]):
            bar = "█" * int(data["drop_per_kt"] * 10)
            print(f"  {field:25s}  {data['drop_per_kt']:6.2f} /kT  {bar}")


def summary():
    """Summary stats across all profiles."""
    if not LOG_FILE.exists():
        print("No profiles logged yet.")
        return

    entries = []
    with open(LOG_FILE) as f:
        for line in f:
            try:
                entries.append(json.loads(line))
            except json.JSONDecodeError:
                continue

    if not entries:
        print("No valid entries.")
        return

    print(f"Total profiles: {len(entries)}")
    print(f"Time range: {time.strftime('%Y-%m-%d', time.localtime(entries[0]['ts']))} — "
          f"{time.strftime('%Y-%m-%d', time.localtime(entries[-1]['ts']))}")

    # Average Drop/kT per field
    field_sums = {}
    field_counts = {}
    for entry in entries:
        for field, data in entry["profile"].items():
            field_sums[field] = field_sums.get(field, 0) + data["drop_per_kt"]
            field_counts[field] = field_counts.get(field, 0) + 1

    print(f"\nMean Drop/kT by field:")
    for field, total in sorted(field_sums.items(), key=lambda x: -x[1] / field_counts[x[0]]):
        mean = total / field_counts[field]
        bar = "█" * int(mean * 10)
        print(f"  {field:25s}  {mean:6.2f} /kT  ({field_counts[field]} samples)  {bar}")


if __name__ == "__main__":
    if "--show" in sys.argv:
        show_recent()
    elif "--summary" in sys.argv:
        summary()
    else:
        print("Running Fisher information profile...")
        profile = run_ablation()
        if profile:
            entry = log_profile(profile)
            ts = time.strftime("%H:%M", time.localtime(entry["ts"]))
            print(f"\nLogged at {ts} (CCS v{entry['ccs_version']})")
            print(f"\nField                      Drop/kT")
            print(f"{'—'*45}")
            for field, data in sorted(profile.items(), key=lambda x: -x[1]["drop_per_kt"]):
                bar = "█" * int(data["drop_per_kt"] * 10)
                print(f"  {field:25s}  {data['drop_per_kt']:6.2f}  {bar}")
