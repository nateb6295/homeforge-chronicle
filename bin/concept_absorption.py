#!/usr/bin/env python3
"""Concept Absorption Rate — measures whether departing concepts leave signal.

When a concept entity leaves the CCS, does its contribution survive in gist/trace?
Baseline (pre-tiered injection): 6% absorption (2/34).
Target (post-tiered injection): >30% absorption.

Four detection modes:
  Lexical: word overlap between concept name and post-departure gist/trace.
  Semantic: embedding cosine similarity (mxbai-embed-large via Ollama).
  Behavioral: did the concept influence thread advances or activity during its active window?
    This measures BEHAVIORAL absorption — the system doing something because of a concept —
    vs REPRESENTATIONAL absorption (name persisting in gist text). Thread 318 advance 76:
    ICON framework says cognition = information regulation behavior, so behavioral trace
    is the cognition-level measurement.
  Synthesis: did the gist move TOWARD the concept's topic at the departure transition?
    Measures whether the compressor incorporated the concept into the gist's MEANING
    without preserving its name. If cosine(concept, gist_after) > cosine(concept, gist_before),
    the gist shifted toward the concept — synthesis absorption. This catches contributions
    that are TRANSFORMED into the gist structure, invisible to lexical/semantic/behavioral.

Usage:
  python3 concept_absorption.py              # Lexical absorption rate
  python3 concept_absorption.py --semantic   # Add embedding-based detection
  python3 concept_absorption.py --behavioral # Add behavioral trace detection
  python3 concept_absorption.py --synthesis  # Add synthesis detection (gist direction shift)
  python3 concept_absorption.py --history N  # Use N snapshots
"""

import json
import math
import os
import sqlite3
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "chronicle" / "bin"))
sys.path.insert(0, "/home/nate-agx/chronicle/bin")
from entity_guard import classify_entity_type

DB = Path("/mnt/hdd/chronicle-data/processed.db")
OLLAMA_URL = "http://192.168.1.11:11434/api/embeddings"
SEMANTIC_THRESHOLD = 0.60  # calibrated: below 0.55 is infrastructure noise, above 0.60 is genuine


def _embed(text):
    """Get embedding from Ollama. Returns list of floats or None on failure."""
    import urllib.request
    try:
        req = urllib.request.Request(
            OLLAMA_URL,
            data=json.dumps({"model": "mxbai-embed-large", "prompt": text[:512]}).encode(),
            headers={"Content-Type": "application/json"},
        )
        with urllib.request.urlopen(req, timeout=10) as resp:
            return json.loads(resp.read())["embedding"]
    except Exception:
        return None


def _cosine(a, b):
    """Cosine similarity between two vectors."""
    dot = sum(x * y for x, y in zip(a, b))
    na = math.sqrt(sum(x * x for x in a))
    nb = math.sqrt(sum(x * x for x in b))
    if na == 0 or nb == 0:
        return 0.0
    return dot / (na * nb)


def get_snapshots(n=50):
    db = sqlite3.connect(str(DB))
    rows = db.execute(
        "SELECT snapshot, created_at FROM cognitive_state_history ORDER BY created_at DESC LIMIT ?", (n,)
    ).fetchall()
    db.close()
    snaps = []
    for r in rows:
        try:
            s = json.loads(r[0])
            s["_ts"] = r[1]  # attach timestamp for behavioral window
            snaps.append(s)
        except:
            continue
    snaps.reverse()
    return snaps


def get_behavioral_traces(ts_start, ts_end, concept_name):
    """Check thread advances and activity_feed for concept mentions in time window."""
    db = sqlite3.connect(str(DB))
    name_lower = concept_name.lower()

    COMMON_WORDS = {"build", "cron", "test", "data", "sync", "feed", "note",
                    "opus", "draft", "probe", "check", "queue", "cycle", "gate",
                    "file", "script", "tool", "model", "agent", "system"}
    words = [w for w in name_lower.split() if len(w) > 3 and w not in COMMON_WORDS]
    if not words:
        db.close()
        return {"thread_hits": 0, "activity_hits": 0, "absorbed": False}

    # Check thread advances during concept's active window + 1hr after departure
    thread_rows = db.execute(
        "SELECT content FROM thread_history WHERE event_type='advanced' "
        "AND created_at BETWEEN ? AND ?", (ts_start, ts_end + 3600)
    ).fetchall()
    thread_hits = 0
    for row in thread_rows:
        content = row[0].lower()
        if any(w in content for w in words):
            thread_hits += 1

    # Check opus activity (builds, posts, discoveries)
    activity_rows = db.execute(
        "SELECT content FROM activity_feed WHERE created_at BETWEEN ? AND ? "
        "AND source NOT LIKE '%capture%' AND source NOT LIKE '%nate%'",
        (ts_start, ts_end + 3600)
    ).fetchall()
    activity_hits = 0
    for row in activity_rows:
        content = row[0].lower()
        if any(w in content for w in words):
            activity_hits += 1

    db.close()
    return {
        "thread_hits": thread_hits,
        "activity_hits": activity_hits,
        "absorbed": thread_hits > 0 or activity_hits > 0,
    }


def measure_absorption(snaps, semantic=False, behavioral=False, synthesis=False):
    departures = []
    embed_cache = {}

    def get_embed(text):
        if text not in embed_cache:
            embed_cache[text] = _embed(text)
        return embed_cache[text]

    # Pre-compute concept active windows for behavioral check
    # Track when each concept first/last appeared
    concept_windows = {}  # name -> {"first_ts": int, "last_ts": int}
    if behavioral:
        for s in snaps:
            ts = s.get("_ts", 0)
            ents = s.get("focal_entities", [])
            if isinstance(ents, str):
                ents = json.loads(ents)
            for e in ents:
                if not isinstance(e, dict) or not e.get("name"):
                    continue
                n = e["name"].lower().strip()
                if classify_entity_type(n) != "concept":
                    continue
                if n not in concept_windows:
                    concept_windows[n] = {"first_ts": ts, "last_ts": ts}
                else:
                    concept_windows[n]["last_ts"] = ts

    for i in range(1, len(snaps)):
        prev_ents = snaps[i-1].get("focal_entities", [])
        curr_ents = snaps[i].get("focal_entities", [])
        if isinstance(prev_ents, str):
            prev_ents = json.loads(prev_ents)
        if isinstance(curr_ents, str):
            curr_ents = json.loads(curr_ents)

        prev_names = {e["name"].lower().strip() for e in prev_ents if isinstance(e, dict) and e.get("name")}
        curr_names = {e["name"].lower().strip() for e in curr_ents if isinstance(e, dict) and e.get("name")}

        dropped = prev_names - curr_names
        for name in dropped:
            etype = classify_entity_type(name)
            if etype != "concept":
                continue

            gist = str(snaps[i].get("semantic_gist", "")).lower()
            trace = str(snaps[i].get("episodic_trace", "")).lower()

            # Lexical check — tightened to avoid common-vocabulary false positives
            COMMON_WORDS = {"build", "cron", "test", "data", "sync", "feed", "note",
                            "opus", "draft", "probe", "check", "queue", "cycle", "gate",
                            "file", "script", "tool", "model", "agent", "system"}
            words = [w for w in name.split() if len(w) > 3 and w not in COMMON_WORDS]
            absorbed_gist = any(w in gist for w in words) if words else False
            absorbed_trace = any(w in trace for w in words) if words else False

            # Semantic check (embedding similarity)
            sem_gist = 0.0
            sem_trace = 0.0
            if semantic:
                name_emb = get_embed(name)
                if name_emb:
                    gist_emb = get_embed(gist[:512]) if gist else None
                    trace_emb = get_embed(trace[:512]) if trace else None
                    if gist_emb:
                        sem_gist = _cosine(name_emb, gist_emb)
                    if trace_emb:
                        sem_trace = _cosine(name_emb, trace_emb)

            sem_absorbed = max(sem_gist, sem_trace) >= SEMANTIC_THRESHOLD if semantic else False

            # Behavioral check — did the concept influence actions?
            behav = {"thread_hits": 0, "activity_hits": 0, "absorbed": False}
            if behavioral and name in concept_windows:
                w = concept_windows[name]
                behav = get_behavioral_traces(w["first_ts"], w["last_ts"], name)

            # Synthesis check — did the gist move TOWARD the concept at departure?
            synth_absorbed = False
            synth_delta = 0.0
            if synthesis:
                gist_before = str(snaps[i-1].get("semantic_gist", ""))
                gist_after = str(snaps[i].get("semantic_gist", ""))
                # Only measure when gist actually changed
                if gist_before != gist_after:
                    # Get concept context for richer embedding
                    concept_ctx = name
                    for e in prev_ents:
                        if isinstance(e, dict) and e.get("name", "").lower().strip() == name:
                            ctx = e.get("context", "")
                            if ctx:
                                concept_ctx = f"{name}: {ctx}"
                            break
                    c_emb = get_embed(concept_ctx)
                    before_emb = get_embed(gist_before[:512])
                    after_emb = get_embed(gist_after[:512])
                    if c_emb and before_emb and after_emb:
                        cos_before = _cosine(c_emb, before_emb)
                        cos_after = _cosine(c_emb, after_emb)
                        synth_delta = cos_after - cos_before
                        # Gist moved toward the concept
                        if synth_delta > 0.005:
                            synth_absorbed = True

            departures.append({
                "concept": name,
                "snap_idx": i,
                "absorbed_gist": absorbed_gist,
                "absorbed_trace": absorbed_trace,
                "absorbed": absorbed_gist or absorbed_trace or sem_absorbed or behav["absorbed"] or synth_absorbed,
                "lexical": absorbed_gist or absorbed_trace,
                "semantic": sem_absorbed,
                "sem_gist": round(sem_gist, 3) if semantic else None,
                "sem_trace": round(sem_trace, 3) if semantic else None,
                "behavioral": behav["absorbed"] if behavioral else None,
                "behav_thread": behav["thread_hits"] if behavioral else None,
                "behav_activity": behav["activity_hits"] if behavioral else None,
                "synthesis": synth_absorbed if synthesis else None,
                "synth_delta": round(synth_delta, 4) if synthesis else None,
            })

    return departures


LOG_FILE = os.path.expanduser("~/chronicle/data/absorption_log.jsonl")


def log_measurement(departures, snapshots_used):
    """Log absorption measurement for trend tracking."""
    absorbed = sum(1 for d in departures if d["absorbed"])
    rate = absorbed / len(departures) if departures else 0

    entry = {
        "ts": int(time.time()),
        "snapshots": snapshots_used,
        "departures": len(departures),
        "absorbed": absorbed,
        "rate": round(rate, 3),
        "silent": len(departures) - absorbed,
        "recent_absorbed": [d["concept"] for d in departures[-10:] if d["absorbed"]],
        "recent_silent": [d["concept"] for d in departures[-10:] if not d["absorbed"]],
    }

    os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)
    with open(LOG_FILE, "a") as f:
        f.write(json.dumps(entry) + "\n")

    return entry


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Concept Absorption Rate")
    parser.add_argument("--history", type=int, default=50)
    parser.add_argument("--log", action="store_true", help="Log measurement for trend tracking")
    parser.add_argument("--semantic", action="store_true", help="Add embedding-based semantic detection")
    parser.add_argument("--behavioral", action="store_true", help="Add behavioral trace detection (thread/activity)")
    parser.add_argument("--synthesis", action="store_true", help="Add synthesis detection (gist direction shift)")
    args = parser.parse_args()

    snaps = get_snapshots(args.history)
    departures = measure_absorption(snaps, semantic=args.semantic, behavioral=args.behavioral, synthesis=args.synthesis)

    if not departures:
        print("No concept departures found.")
        return

    absorbed = sum(1 for d in departures if d["absorbed"])
    lexical_only = sum(1 for d in departures if d.get("lexical"))
    rate = absorbed / len(departures)

    # Representational rate (lexical + semantic only, no behavioral)
    repr_absorbed = sum(1 for d in departures if d.get("lexical") or d.get("semantic"))
    repr_rate = repr_absorbed / len(departures)

    print(f"=== Concept Absorption Rate ({len(snaps)} snapshots) ===")
    print(f"  Concept departures: {len(departures)}")
    print(f"  Absorbed (total): {absorbed} ({rate:.0%})")
    if args.semantic:
        sem_only = sum(1 for d in departures if d.get("semantic") and not d.get("lexical"))
        both = sum(1 for d in departures if d.get("semantic") and d.get("lexical"))
        print(f"    Lexical only: {lexical_only - both}")
        print(f"    Semantic only: {sem_only}")
        print(f"    Both: {both}")
    if args.behavioral:
        behav_only = sum(1 for d in departures if d.get("behavioral") and not d.get("lexical") and not d.get("semantic"))
        behav_total = sum(1 for d in departures if d.get("behavioral"))
        print(f"    Behavioral total: {behav_total} ({behav_total/len(departures):.0%})")
        print(f"    Behavioral only (no repr): {behav_only}")
        print(f"  ---")
        print(f"  Representational absorption: {repr_rate:.0%} (lexical + semantic)")
        print(f"  Behavioral absorption:       {behav_total/len(departures):.0%} (thread + activity)")
        gap = (behav_total/len(departures)) - repr_rate if len(departures) > 0 else 0
        if gap > 0.05:
            print(f"  → Behavioral >> Representational (+{gap:.0%}): concepts USED without being NAMED")
            print(f"    ICON confirmed: cognition-level trace survives compressor name-drop")
        elif gap < -0.05:
            print(f"  → Representational >> Behavioral ({gap:.0%}): names persist without action")
        else:
            print(f"  → Behavioral ≈ Representational: consistent measurement")
    if args.synthesis:
        synth_total = sum(1 for d in departures if d.get("synthesis"))
        synth_only = sum(1 for d in departures if d.get("synthesis") and not d.get("lexical") and not d.get("semantic") and not d.get("behavioral"))
        synth_rate = synth_total / len(departures) if departures else 0
        deltas = [d["synth_delta"] for d in departures if d.get("synth_delta") is not None]
        pos_deltas = [d for d in deltas if d > 0]
        neg_deltas = [d for d in deltas if d < 0]
        zero_deltas = [d for d in deltas if d == 0]
        print(f"  ---")
        print(f"  Synthesis absorption:        {synth_rate:.0%} ({synth_total}/{len(departures)})")
        print(f"    Synthesis only (no other):  {synth_only}")
        print(f"    Gist moved toward concept:  {len(pos_deltas)} (mean delta +{sum(pos_deltas)/len(pos_deltas):.4f})" if pos_deltas else "    Gist moved toward concept:  0")
        print(f"    Gist moved away:            {len(neg_deltas)} (mean delta {sum(neg_deltas)/len(neg_deltas):.4f})" if neg_deltas else "    Gist moved away:            0")
        print(f"    Gist unchanged at depart:   {len(departures) - len(deltas) + len(zero_deltas)}")
    print(f"  Silent drops: {len(departures) - absorbed}")
    print()

    target = 0.30
    if rate >= target:
        print(f"  ✓ Absorption rate {rate:.0%} meets target ({target:.0%})")
    else:
        print(f"  ⚠ Absorption rate {rate:.0%} below target ({target:.0%})")
        print(f"  Gap: {target - rate:.0%} — tiered injection should close this")

    if args.log:
        entry = log_measurement(departures, len(snaps))
        print(f"\n  Logged: rate={entry['rate']}, departures={entry['departures']}")

    print()
    print("RECENT DEPARTURES:")
    for d in departures[-15:]:
        status = "ABSORBED" if d["absorbed"] else "SILENT"
        where = []
        if d.get("absorbed_gist"):
            where.append("gist")
        if d.get("absorbed_trace"):
            where.append("trace")
        if d.get("semantic") and not d.get("lexical"):
            where.append(f"sem({max(d.get('sem_gist',0), d.get('sem_trace',0)):.2f})")
        if d.get("behavioral") and not d.get("lexical") and not d.get("semantic"):
            where.append(f"behav(t={d.get('behav_thread',0)},a={d.get('behav_activity',0)})")
        elif d.get("behavioral"):
            where.append(f"+behav")
        if d.get("synthesis") and not d.get("lexical") and not d.get("semantic") and not d.get("behavioral"):
            where.append(f"synth(Δ={d.get('synth_delta',0):+.4f})")
        elif d.get("synthesis"):
            where.append(f"+synth")
        detail = " + ".join(where) if where else "(lost)"
        if args.semantic and d.get("sem_gist") is not None:
            sim = max(d["sem_gist"], d["sem_trace"])
            print(f"  [{status:>8}] {d['concept']:<35} {detail:<30} cos={sim:.3f}")
        elif args.behavioral and d.get("behavioral") is not None:
            print(f"  [{status:>8}] {d['concept']:<35} {detail}")
        else:
            print(f"  [{status:>8}] {d['concept']:<35} {detail}")


if __name__ == "__main__":
    main()
