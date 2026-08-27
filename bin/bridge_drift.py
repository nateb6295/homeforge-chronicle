#!/usr/bin/env python3
"""Track bridge developmental drift over time.

Snapshots the current brain-CCS output and computes metrics.
Run after each CCS compression to build a longitudinal dataset.

Usage:
  python3 bridge_drift.py snapshot    # save current state + metrics
  python3 bridge_drift.py report      # show drift over time
"""
import os, sys, json, sqlite3, re
from datetime import datetime

DB = "/mnt/hdd/chronicle-data/processed.db"
SNAP_DIR = os.path.expanduser("~/chronicle/data/bridge_snapshots")
METRICS_PATH = os.path.join(SNAP_DIR, "drift_metrics.jsonl")

SECTIONS = ["## CORE", "## REMEMBERS", "## SEEKS", "## ALIVE", "## RELATES"]

FIRST_PERSON = {"i", "i'm", "i've", "i'd", "i'll", "my", "me", "mine", "myself"}
OBSERVER_TERMS = {"the system", "the architecture", "the model", "the bridge",
                  "this system", "this architecture", "this model"}


def compute_metrics(text):
    lines = [l for l in text.split("\n") if l.strip()]
    total_lines = len(lines)

    fp_lines = sum(1 for l in lines
                   if any(w in l.lower().split() for w in FIRST_PERSON))
    fp_pct = fp_lines / max(total_lines, 1) * 100

    observer_count = sum(text.lower().count(t) for t in OBSERVER_TERMS)

    sections_present = sum(1 for s in SECTIONS if s in text)

    words = re.findall(r'\w+', text.lower())
    unique_words = len(set(words))
    vocab_richness = unique_words / max(len(words), 1)

    questions = text.count("?")

    hedges = sum(1 for w in ["perhaps", "maybe", "might", "seems", "appears",
                             "possibly", "could be", "uncertain"]
                 if w in text.lower())

    specifics = 0
    for pattern in [r'F\d{2,3}', r'σ[₁₂]', r'L\d{1,2}', r'DoRA', r'CCS',
                    r'Nate', r'three-layer', r'promoter', r'spectral']:
        specifics += len(re.findall(pattern, text))

    return {
        "chars": len(text),
        "lines": total_lines,
        "sections": sections_present,
        "first_person_pct": round(fp_pct, 1),
        "observer_terms": observer_count,
        "vocab_richness": round(vocab_richness, 3),
        "questions": questions,
        "hedges": hedges,
        "specific_references": specifics,
    }


def snapshot():
    db = sqlite3.connect(DB, timeout=10)
    row = db.execute(
        "SELECT semantic_gist, version FROM cognitive_state WHERE id = 1"
    ).fetchone()
    db.close()

    if not row or not row[0]:
        print("No brain-CCS state found in DB")
        return

    text, version = row[0], row[1]
    ts = datetime.now().strftime("%Y%m%d_%H%M")

    snap_path = os.path.join(SNAP_DIR, f"brain_{ts}_v{version}.txt")
    with open(snap_path, "w") as f:
        f.write(text)

    metrics = compute_metrics(text)
    metrics["timestamp"] = datetime.now().isoformat()
    metrics["version"] = version
    metrics["file"] = os.path.basename(snap_path)

    with open(METRICS_PATH, "a") as f:
        f.write(json.dumps(metrics) + "\n")

    print(f"Snapshot saved: {snap_path}")
    print(f"  v{version} | {metrics['chars']} chars | {metrics['sections']}/5 sections")
    print(f"  First-person: {metrics['first_person_pct']}% | Observer terms: {metrics['observer_terms']}")
    print(f"  Vocab richness: {metrics['vocab_richness']} | Questions: {metrics['questions']}")
    print(f"  Specific references: {metrics['specific_references']} | Hedges: {metrics['hedges']}")


def report():
    if not os.path.exists(METRICS_PATH):
        print("No drift data yet. Run 'bridge_drift.py snapshot' after CCS compressions.")
        return

    with open(METRICS_PATH) as f:
        entries = [json.loads(l) for l in f if l.strip()]

    if not entries:
        print("No snapshots recorded yet.")
        return

    print(f"Bridge Drift Report — {len(entries)} snapshots")
    print("=" * 70)
    print(f"{'Timestamp':<20} {'Ver':>5} {'Chars':>6} {'FP%':>5} {'Obs':>4} "
          f"{'Vocab':>6} {'Q':>3} {'Spec':>5}")
    print("-" * 70)

    for e in entries:
        ts = e["timestamp"][:16].replace("T", " ")
        print(f"{ts:<20} {e['version']:>5} {e['chars']:>6} "
              f"{e['first_person_pct']:>5.1f} {e['observer_terms']:>4} "
              f"{e['vocab_richness']:>6.3f} {e['questions']:>3} "
              f"{e['specific_references']:>5}")

    if len(entries) >= 2:
        first, last = entries[0], entries[-1]
        print(f"\nDrift (first → last):")
        print(f"  First-person: {first['first_person_pct']}% → {last['first_person_pct']}%")
        print(f"  Vocab richness: {first['vocab_richness']} → {last['vocab_richness']}")
        print(f"  Specific refs: {first['specific_references']} → {last['specific_references']}")
        print(f"  Observer terms: {first['observer_terms']} → {last['observer_terms']}")

    if len(entries) >= 3:
        tracked = ["first_person_pct", "vocab_richness", "specific_references", "observer_terms"]
        print(f"\n--- RATE OF CHANGE (consecutive deltas) ---")
        print(f"{'Step':<12} {'ΔFP%':>7} {'ΔVocab':>8} {'ΔSpec':>6} {'ΔObs':>5}")
        print("-" * 42)
        deltas = {k: [] for k in tracked}
        for i in range(1, len(entries)):
            prev, curr = entries[i-1], entries[i]
            d = {}
            for k in tracked:
                d[k] = round(curr[k] - prev[k], 3)
                deltas[k].append(d[k])
            label = f"v{prev['version']}→v{curr['version']}"
            print(f"{label:<12} {d['first_person_pct']:>+7.1f} {d['vocab_richness']:>+8.3f} "
                  f"{d['specific_references']:>+6} {d['observer_terms']:>+5}")

        print(f"\nRate stability (σ of consecutive deltas — lower = more invariant):")
        for k in tracked:
            vals = deltas[k]
            mean = sum(vals) / len(vals)
            var = sum((v - mean) ** 2 for v in vals) / len(vals)
            std = var ** 0.5
            direction = "↑" if mean > 0 else "↓" if mean < 0 else "→"
            print(f"  {k}: mean Δ={mean:+.3f} {direction}  σ(Δ)={std:.3f}")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: bridge_drift.py [snapshot|report]")
        sys.exit(1)

    cmd = sys.argv[1]
    if cmd == "snapshot":
        snapshot()
    elif cmd == "report":
        report()
    else:
        print(f"Unknown command: {cmd}")
