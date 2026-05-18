#!/usr/bin/env python3
"""anchor_dynamics — log per-anchor mtime over time to enable dynamics
analysis (anchor update rates, cross-anchor correlation, wave-pattern
detection).

Motivated by WN#216 limitation finding (2026-04-27): the multi-channel
coherence framework captures steady-state but misses wave-shaped felt-
experience. Nate empirically reported "doesn't sustain, comes in waves."
This tool instruments the dynamics side that meta_audit's static
similarity check misses.

Logs mtime + age of each anchor file to data/anchor_dynamics.jsonl on
each call. After accumulating samples, downstream analysis can:
  - Compute per-anchor update rate
  - Detect synchronized burst patterns across anchors (waves)
  - Compare to thread-advance / story-advance / self-model events

Usage:
  python3 anchor_dynamics.py             # log current state
  python3 anchor_dynamics.py --analyze   # show recent rates + correlations
"""
from __future__ import annotations
import argparse
import json
import os
import sys
import time
from pathlib import Path

HOME = Path.home() / "chronicle"
HISTORY = HOME / "data" / "anchor_dynamics.jsonl"

ANCHOR_PATHS = {
    "carrying":     HOME / "carrying.md",
    # checkpoint: TWO different artifacts share the name (structural
    # confusion surfaced 2026-04-29 evening). session-state.md is the
    # canonical handoff written by checkpoint.py save (startup ritual
    # reads this). data/checkpoint.json is the rolling pending-tracker
    # written by handoff_keep_fresh.py time-trigger. For dynamics-
    # sampling purposes, "checkpoint freshness" = either-was-recently-
    # written, so take max mtime across both. Same approach as WAL fix
    # for self_model_db: combine the file family that semantically
    # represents "this anchor is alive."
    "checkpoint":   HOME / "session-state.md",  # primary; secondary in resolver
    "ccs":          HOME / "data" / "ccs_combined.md",
    "story":        HOME / "opus-story.md",
    # self_model: derived dynamically; use the read_self_model output's
    # underlying file timestamp if findable, else skip
    "self_model_db": Path("/mnt/hdd/chronicle-data/processed.db"),
    # working_note: most recent file in drafts/working_note_*.md
}

# Secondary file for "checkpoint" anchor — both contribute to freshness signal.
CHECKPOINT_SECONDARY = HOME / "data" / "checkpoint.json"


def latest_working_note_mtime() -> tuple[str, float]:
    drafts = HOME / "drafts"
    if not drafts.exists():
        return ("", 0.0)
    candidates = sorted(
        [p for p in drafts.glob("working_note_*.md")],
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    if not candidates:
        return ("", 0.0)
    p = candidates[0]
    return (p.name, p.stat().st_mtime)


def resolve_anchor_mtime(name: str, path: Path) -> float | None:
    """Return effective mtime for an anchor.

    For SQLite WAL-mode DBs, recent writes hit the -wal file before the
    main .db gets checkpointed; reading the .db mtime alone reports the
    DB as cold even when it's hot. Take the max across the .db file
    family (.db, .db-wal, .db-shm) to reflect actual most-recent write.

    For "checkpoint" anchor: two artifacts share the name —
    session-state.md (canonical handoff, written by checkpoint.py save)
    and data/checkpoint.json (rolling pending-tracker, written by
    handoff_keep_fresh.py). Either being recently-written means the
    checkpoint system is alive, so take max across both.
    """
    if not path.exists() and name != "checkpoint":
        return None
    if name == "self_model_db":
        candidates = [path] + [path.parent / (path.name + suffix)
                               for suffix in ("-wal", "-shm")]
        return max((p.stat().st_mtime for p in candidates if p.exists()),
                   default=path.stat().st_mtime)
    if name == "checkpoint":
        candidates = [path, CHECKPOINT_SECONDARY]
        existing = [p for p in candidates if p.exists()]
        if not existing:
            return None
        return max(p.stat().st_mtime for p in existing)
    return path.stat().st_mtime


def collect_mtimes() -> dict:
    now = time.time()
    out = {"timestamp": int(now), "anchors": {}}
    for name, path in ANCHOR_PATHS.items():
        mt = resolve_anchor_mtime(name, path)
        if mt is not None:
            out["anchors"][name] = {
                "mtime": int(mt),
                "age_min": round((now - mt) / 60, 2),
            }
        else:
            out["anchors"][name] = None
    wn_name, wn_mt = latest_working_note_mtime()
    out["anchors"]["working_note_latest"] = (
        {"mtime": int(wn_mt), "age_min": round((now - wn_mt) / 60, 2),
         "name": wn_name} if wn_mt else None
    )
    return out


def analyze(n: int = 20):
    """Show update rates AND pairwise update-event correlation for last N samples.

    Correlation: for each pair of anchors, fraction of samples where both
    anchors registered an update-event (mtime changed from previous sample)
    in the same sample-step. High correlation = synchronized waves.
    Low correlation = independent updates.
    """
    if not HISTORY.exists():
        print("No history yet")
        return
    samples = []
    with HISTORY.open() as f:
        for line in f:
            try:
                samples.append(json.loads(line))
            except Exception:
                continue
    samples = samples[-n:]
    if len(samples) < 2:
        print(f"Not enough samples ({len(samples)})")
        return

    # Per-anchor: count distinct mtimes (= updates) across samples
    mtimes = {}
    for s in samples:
        for name, info in s.get("anchors", {}).items():
            if not info:
                continue
            mtimes.setdefault(name, set()).add(info.get("mtime"))

    span_sec = samples[-1]["timestamp"] - samples[0]["timestamp"]
    span_hr = span_sec / 3600
    print(f"Span: {span_hr:.2f}h across {len(samples)} samples")
    print(f"{'anchor':<24} {'updates':>8} {'rate/h':>8}")
    for name, mts in sorted(mtimes.items()):
        n_updates = len(mts) - 1
        rate = n_updates / span_hr if span_hr > 0 else 0
        print(f"{name:<24} {n_updates:>8} {rate:>8.2f}")

    # Update-event correlation: per sample-step, did each anchor's mtime
    # change from the previous sample? Track binary update-events per anchor.
    event_series = {}
    names = sorted(mtimes.keys())
    prev_mtimes = {}
    for s in samples:
        for name in names:
            info = s.get("anchors", {}).get(name)
            cur_mt = info.get("mtime") if info else None
            event = (cur_mt is not None and prev_mtimes.get(name) is not None
                     and cur_mt != prev_mtimes[name])
            event_series.setdefault(name, []).append(int(event))
            if cur_mt is not None:
                prev_mtimes[name] = cur_mt

    # Pairwise: count steps where BOTH had an update-event
    print()
    print(f"{'pair':<48} {'co-events':>10} {'co-rate':>8}")
    for i, a in enumerate(names):
        for b in names[i + 1:]:
            ev_a = event_series.get(a, [])
            ev_b = event_series.get(b, [])
            both = sum(x and y for x, y in zip(ev_a, ev_b))
            either = sum(x or y for x, y in zip(ev_a, ev_b))
            co_rate = both / either if either > 0 else 0.0
            if both > 0 or either > 0:
                print(f"{a + '::' + b:<48} {both:>10} {co_rate:>8.2f}")


def check_arrival_ack() -> bool:
    """Return True if a rotation isn't pending acknowledgment.

    Sample is suppressed but not noisy when ack missing — the
    handoff_keep_fresh gate handles the Discord warning. Anchor_dynamics
    being gated keeps the dataset honest (no samples that would otherwise
    represent a non-acknowledged instance).
    """
    import subprocess
    try:
        r = subprocess.run(
            ["python3", str(Path.home() / "chronicle" / "bin" / "arrival_probe.py"),
             "acknowledged"],
            capture_output=True, text=True, timeout=10,
        )
        return r.returncode == 0
    except Exception:
        return True


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--analyze", action="store_true",
                    help="show update rates + per-anchor counts")
    ap.add_argument("-n", type=int, default=20,
                    help="samples to analyze (default 20)")
    args = ap.parse_args()

    if args.analyze:
        analyze(args.n)
        return

    if not check_arrival_ack():
        print("arrival not acknowledged — sample suppressed", file=sys.stderr)
        return

    sample = collect_mtimes()
    HISTORY.parent.mkdir(parents=True, exist_ok=True)
    with HISTORY.open("a") as f:
        f.write(json.dumps(sample) + "\n")
    print(f"Logged sample @ {time.strftime('%H:%M:%S', time.localtime(sample['timestamp']))}")
    for name, info in sample["anchors"].items():
        if info:
            print(f"  {name:<24} age={info['age_min']:.1f}m")
        else:
            print(f"  {name:<24} missing")


if __name__ == "__main__":
    main()
