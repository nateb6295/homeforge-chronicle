#!/usr/bin/env python3
"""
CCS homeostasis score — composed health indicator for the cognitive-state
architecture. Inspired by the DMN dyshomeostasis literature (Corriveau-
Lecavalier et al, 2026): when the self-referential network drifts from
homeostasis, it predicts downstream decline. We compose the already-
instrumented CCS signals into a single traffic-light score per component
plus a composite.

Five components:
  1. gist_freeze         — embedding distance gist(current) vs gist(N-back).
                           For Chronicle's architecture, low gist movement is
                           HEALTHY (gist is identity anchor, intentionally
                           stable). Band recalibrated 2026-04-27 to reflect
                           this: 0.00-0.30 = green (stable identity), 0.30+ =
                           yellow/red (identity drift). Original DMN-shaped
                           band assumed gist drift = health signal, which is
                           inverted from our design.
  2. field_volatility    — magnitude of change per non-identity field across
                           recent rotations. Too high = thrash, too low = frozen.
  3. entity_retention    — fraction of focal_entities that persist across
                           rotations. Too high = sclerotic, too low = forgetful.
  4. uncertainty_flow    — rate of uncertainty_signals opened vs closed.
                           Signals should turn over, not accumulate forever.
  5. constraint_stability— fraction of constraints that persist across
                           rotations. Identity-level; high stability is good,
                           but complete freeze is a red flag.

Each component has a healthy band; being well inside = green, at edge =
yellow, outside = red. Composite score is the geometric mean of the
per-component fitness values.

Output:
  - stdout: one-line summary + per-component breakdown
  - JSON to ~/chronicle/data/homeostasis_history.jsonl (append per run)

Usage:
  python3 homeostasis.py              # compute + log
  python3 homeostasis.py --verbose    # show bands + raw values
  python3 homeostasis.py --history 20 # summarize last 20 runs
"""
import argparse
import json
import math
import sqlite3
import sys
import time
import urllib.request
from pathlib import Path

DB = "/mnt/hdd/chronicle-data/processed.db"
OLLAMA_EMBED = "http://192.168.1.11:11434/api/embeddings"
EMBED_MODEL = "mxbai-embed-large"
HIST_PATH = Path.home() / "chronicle" / "data" / "homeostasis_history.jsonl"

# Healthy bands per component. Values OUTSIDE the band are unhealthy.
# Fitness(v) = 1.0 inside green, drops linearly in yellow, 0 in red.
BANDS = {
    # (green_low, green_high, yellow_low, yellow_high)
    "gist_freeze":             (0.00, 0.30, 0.00, 0.45),  # embed-dist(now, 5-back); low = stable (healthy for Chronicle architecture)
    "field_volatility":        (0.20, 0.60, 0.10, 0.80),  # mean char-diff ratio
    "entity_retention":        (0.35, 0.75, 0.20, 0.90),  # fraction persisted
    "uncertainty_flow":        (0.30, 2.50, 0.15, 5.00),  # resolution_ratio
    "constraint_stability":    (0.50, 1.01, 0.30, 1.01),  # fraction persisted; 1.0 (all-persist) is HEALTHY for Chronicle's identity-anchor architecture (recalibrated 2026-04-27)
    "predictive_calibration":  (0.50, 1.01, 0.30, 1.01),  # mean LLM-judge score 0..1; over 1.01 means no upper-yellow
}


def embed(text, timeout=15):
    body = json.dumps({"model": EMBED_MODEL, "prompt": text[:2000]}).encode()
    req = urllib.request.Request(
        OLLAMA_EMBED, data=body, headers={"Content-Type": "application/json"}
    )
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read())["embedding"]


def cosine(a, b):
    dot = sum(x * y for x, y in zip(a, b))
    na = math.sqrt(sum(x * x for x in a))
    nb = math.sqrt(sum(x * x for x in b))
    if na == 0 or nb == 0:
        return 0.0
    return dot / (na * nb)


def distance(a, b):
    return 1.0 - cosine(a, b)


def load_snapshots(n=10):
    """Return list of snapshots, newest first. Each is the full CCS dict."""
    conn = sqlite3.connect(f"file:{DB}?mode=ro", uri=True)
    # current state is row in cognitive_state
    cur_row = conn.execute(
        "SELECT episodic_trace, semantic_gist, focal_entities, relational_map, "
        "goal_orientation, constraints, predictive_cue, uncertainty_signals, "
        "retrieved_artifacts, updated_at FROM cognitive_state ORDER BY id DESC LIMIT 1"
    ).fetchone()
    cols = ["episodic_trace", "semantic_gist", "focal_entities", "relational_map",
            "goal_orientation", "constraints", "predictive_cue", "uncertainty_signals",
            "retrieved_artifacts", "updated_at"]
    current = {}
    for i, c in enumerate(cols):
        raw = cur_row[i]
        if c in ("semantic_gist", "goal_orientation", "predictive_cue"):
            current[c] = raw or ""
        elif c == "updated_at":
            current[c] = raw
        else:
            try:
                current[c] = json.loads(raw) if raw else ([] if c != "relational_map" else {})
            except Exception:
                current[c] = [] if c != "relational_map" else {}
    snaps = [current]
    # historical snapshots — stored as JSON in `snapshot` column
    rows = conn.execute(
        "SELECT snapshot, created_at, trigger FROM cognitive_state_history "
        "ORDER BY id DESC LIMIT ?",
        (n - 1,),
    ).fetchall()
    for snap_json, ts, trig in rows:
        try:
            s = json.loads(snap_json)
            s["updated_at"] = ts
            s["_trigger"] = trig
            snaps.append(s)
        except Exception:
            continue
    conn.close()
    return snaps


def component_gist_freeze(snaps):
    """Embed distance between current gist and 5-rotations-back gist."""
    if len(snaps) < 2:
        return {"value": None, "note": "need at least 2 snapshots"}
    idx = min(5, len(snaps) - 1)
    current = snaps[0].get("semantic_gist", "")
    past = snaps[idx].get("semantic_gist", "")
    if not current or not past:
        return {"value": None, "note": "empty gist"}
    try:
        e1 = embed(current)
        e2 = embed(past)
    except Exception as e:
        return {"value": None, "note": f"embed failed: {e}"}
    d = distance(e1, e2)
    return {"value": d, "rotations_back": idx, "current_gist_len": len(current)}


def component_field_volatility(snaps):
    """Mean change-ratio across non-identity fields over last 3 rotations.

    Uses difflib.SequenceMatcher().ratio() which measures longest-common-
    subsequence-style similarity. Diff = 1 - similarity. Honest for both
    \"identical content\" (diff=0) and \"completely different content\"
    (diff~1).

    Old version had a buggy 'noise tolerance' line that made any two
    same-length strings count as zero-diff regardless of content; testbed
    `degrade_field_thrash` exposed this 2026-04-24.
    """
    if len(snaps) < 2:
        return {"value": None, "note": "need at least 2 snapshots"}
    from difflib import SequenceMatcher
    fields = ["episodic_trace", "predictive_cue", "uncertainty_signals"]
    diffs = []
    for f in fields:
        for i in range(min(3, len(snaps) - 1)):
            a = json.dumps(snaps[i].get(f, ""))
            b = json.dumps(snaps[i + 1].get(f, ""))
            if not a and not b:
                continue
            ratio = SequenceMatcher(None, a, b).ratio()
            diffs.append(1.0 - ratio)
    if not diffs:
        return {"value": None, "note": "no diffs computed"}
    return {"value": sum(diffs) / len(diffs), "n_diffs": len(diffs)}


def _entity_names(entities):
    names = set()
    if isinstance(entities, list):
        for e in entities:
            if isinstance(e, dict):
                n = e.get("name") or e.get("label") or e.get("id")
                if n:
                    names.add(str(n).lower())
            elif isinstance(e, str):
                names.add(e.lower())
    return names


def component_entity_retention(snaps):
    """Fraction of focal_entities in current state that persisted from 3-back."""
    if len(snaps) < 2:
        return {"value": None, "note": "need at least 2 snapshots"}
    idx = min(3, len(snaps) - 1)
    cur = _entity_names(snaps[0].get("focal_entities", []))
    past = _entity_names(snaps[idx].get("focal_entities", []))
    if not cur or not past:
        return {"value": None, "note": f"empty entities (cur={len(cur)}, past={len(past)})"}
    retained = cur & past
    return {
        "value": len(retained) / len(cur) if cur else 0.0,
        "current_n": len(cur),
        "past_n": len(past),
        "retained_n": len(retained),
        "rotations_back": idx,
    }


def component_uncertainty_flow(snaps):
    """Ratio of uncertainty signals turning over vs accumulating.

    resolution_ratio = (closed + opened) / current_size.
    Low = static (stuck with same questions forever).
    High = healthy flow.
    """
    if len(snaps) < 2:
        return {"value": None, "note": "need at least 2 snapshots"}
    def norm(u):
        if isinstance(u, list):
            return {str(x)[:80].lower() for x in u if x}
        return set()
    cur = norm(snaps[0].get("uncertainty_signals", []))
    past = norm(snaps[min(3, len(snaps) - 1)].get("uncertainty_signals", []))
    if not cur and not past:
        return {"value": None, "note": "no uncertainty signals"}
    opened = cur - past
    closed = past - cur
    turnover = len(opened) + len(closed)
    size = max(len(cur), 1)
    return {
        "value": turnover / size,
        "current_n": len(cur),
        "past_n": len(past),
        "opened": len(opened),
        "closed": len(closed),
    }


def component_predictive_calibration(snaps):
    """LLM-judge binary: ask Groq to score each (predictive_cue, actual
    episodic_trace) pair as 0/1 — did the prediction track the outcome?

    Earlier embedding-based versions (raw cosine + relative-z-score) both
    failed homeostasis_testbed because LLM-text pair similarity is too
    saturated to detect content miscalibration. LLM-judge sidesteps this
    by directly evaluating semantic alignment as a discrete question.

    Score = mean of binary judgments across last N pairs. Higher = better
    calibration. Bands re-tuned for fraction-of-correct.
    """
    if len(snaps) < 3:
        return {"value": None, "note": "need >=3 snapshots"}

    def trace_text(snap):
        t = snap.get("episodic_trace", [])
        if isinstance(t, list):
            return " ".join(str(x) for x in t if x)
        return str(t) or ""

    pairs_to_judge = []
    max_pairs = 5
    for i in range(1, min(len(snaps), max_pairs + 1)):
        cue = snaps[i].get("predictive_cue", "") or ""
        actual = trace_text(snaps[i - 1])
        if cue.strip() and actual.strip():
            pairs_to_judge.append((i, cue, actual))
        if len(pairs_to_judge) >= max_pairs:
            break

    if not pairs_to_judge:
        return {"value": None, "note": "no usable pairs"}

    judgments = []
    detail = []
    for i, cue, actual in pairs_to_judge:
        try:
            score = _judge_alignment(cue, actual)
        except Exception as e:
            return {"value": None, "note": f"judge failed: {e}"}
        judgments.append(score)
        detail.append({"i": i, "score": score})

    return {
        "value": sum(judgments) / len(judgments),
        "pairs": len(judgments),
        "detail": detail,
    }


def _judge_alignment(cue, actual):
    """Ask Groq to judge whether the cue's prediction tracked the actual
    trace. Returns 0.0 (poor), 0.5 (partial), or 1.0 (good)."""
    import os
    import urllib.request
    from pathlib import Path

    # Lazy-load env if not present
    if "GROQ_API_KEY" not in os.environ:
        env_path = Path.home() / "chronicle" / "chronicle.env"
        if env_path.exists():
            for line in env_path.read_text().splitlines():
                line = line.strip()
                if "=" in line and not line.startswith("#"):
                    k, _, v = line.partition("=")
                    v = v.strip().strip('"').strip("'")
                    if k.strip() and k.strip() not in os.environ:
                        os.environ[k.strip()] = v

    prompt = (
        "You are scoring whether an AI's prior session-prediction tracked "
        "what actually happened in the next session.\n\n"
        f"PREDICTION: {cue[:600]}\n\n"
        f"ACTUAL EVENTS: {actual[:1200]}\n\n"
        "Score the alignment as a single number on a scale:\n"
        "  0.0 = prediction did NOT match (different topics, wrong direction)\n"
        "  0.5 = partial match (some overlap but missed core direction)\n"
        "  1.0 = good alignment (prediction substantially tracked outcome)\n\n"
        "Reply with ONLY the number (0.0, 0.5, or 1.0). Nothing else."
    )
    body = json.dumps({
        "model": "qwen/qwen3-32b",
        "messages": [{"role": "user", "content": prompt}],
        "max_tokens": 8,
        "temperature": 0.0,
        "reasoning_effort": "none",
    }).encode()
    req = urllib.request.Request(
        "https://api.groq.com/openai/v1/chat/completions",
        data=body,
        headers={
            "Authorization": f"Bearer {os.environ.get('GROQ_API_KEY','')}",
            "Content-Type": "application/json",
            "User-Agent": "chronicle-homeostasis/1.0",
        },
    )
    with urllib.request.urlopen(req, timeout=30) as resp:
        d = json.loads(resp.read())
    text = d["choices"][0]["message"]["content"].strip()
    # Extract first numeric token
    import re as _re
    m = _re.search(r"[01](?:\.\d+)?", text)
    if not m:
        return 0.5
    val = float(m.group(0))
    return max(0.0, min(1.0, val))


def component_constraint_stability(snaps):
    """Fraction of current constraints that also appeared 3-rotations back.

    High-but-not-1 = stable with some evolution. 1.0 = frozen (red).
    Low = churning (also red).
    """
    if len(snaps) < 2:
        return {"value": None, "note": "need at least 2 snapshots"}
    def norm(c):
        if isinstance(c, list):
            return {str(x)[:120].lower() for x in c if x}
        return set()
    idx = min(3, len(snaps) - 1)
    cur = norm(snaps[0].get("constraints", []))
    past = norm(snaps[idx].get("constraints", []))
    if not cur:
        return {"value": None, "note": "no constraints in current"}
    persisted = cur & past
    return {
        "value": len(persisted) / len(cur),
        "current_n": len(cur),
        "past_n": len(past),
        "persisted_n": len(persisted),
        "rotations_back": idx,
    }


def fitness(name, value):
    """Map raw component value to (fitness 0-1, status)."""
    if value is None:
        return (None, "unknown")
    gl, gh, yl, yh = BANDS[name]
    if gl <= value <= gh:
        return (1.0, "green")
    if yl <= value < gl:
        # in yellow below green
        width = max(gl - yl, 1e-9)
        return (max(0.0, (value - yl) / width), "yellow")
    if gh < value <= yh:
        width = max(yh - gh, 1e-9)
        return (max(0.0, (yh - value) / width), "yellow")
    return (0.0, "red")


def compute():
    snaps = load_snapshots(n=10)
    # predictive_calibration rebuilt 2026-04-24 with LLM-judge after
    # homeostasis_testbed retired the embedding-cosine variants.
    components = {
        "gist_freeze":            component_gist_freeze(snaps),
        "field_volatility":       component_field_volatility(snaps),
        "entity_retention":       component_entity_retention(snaps),
        "uncertainty_flow":       component_uncertainty_flow(snaps),
        "constraint_stability":   component_constraint_stability(snaps),
        "predictive_calibration": component_predictive_calibration(snaps),
    }
    fitnesses = []
    for name, c in components.items():
        f, s = fitness(name, c.get("value"))
        c["fitness"] = f
        c["status"] = s
        if f is not None:
            fitnesses.append(f)
    if fitnesses:
        # geometric mean — one red component pulls hard on the composite
        prod = 1.0
        for f in fitnesses:
            prod *= max(f, 1e-6)
        composite = prod ** (1.0 / len(fitnesses))
    else:
        composite = None
    if composite is None:
        status = "unknown"
    elif composite >= 0.70:
        status = "green"
    elif composite >= 0.40:
        status = "yellow"
    else:
        status = "red"
    return {
        "timestamp": int(time.time()),
        "n_snapshots": len(snaps),
        "components": components,
        "composite_fitness": composite,
        "composite_status": status,
    }


def log(result):
    HIST_PATH.parent.mkdir(parents=True, exist_ok=True)
    with HIST_PATH.open("a") as f:
        f.write(json.dumps(result) + "\n")


def print_summary(result, verbose=False):
    ts = result["timestamp"]
    st = result["composite_status"]
    cf = result.get("composite_fitness")
    cf_str = f"{cf:.3f}" if cf is not None else "n/a"
    print(f"[{time.strftime('%Y-%m-%d %H:%M', time.localtime(ts))}] "
          f"CCS homeostasis: {st.upper()} ({cf_str})  n_snaps={result['n_snapshots']}")
    for name, c in result["components"].items():
        val = c.get("value")
        val_str = f"{val:.3f}" if isinstance(val, (int, float)) else "n/a"
        fit = c.get("fitness")
        fit_str = f"{fit:.2f}" if isinstance(fit, (int, float)) else "---"
        status_color = {"green": "🟢", "yellow": "🟡", "red": "🔴", "unknown": "⚪️"}.get(
            c.get("status", "unknown"), "⚪️"
        )
        note = f"  ({c['note']})" if c.get("note") else ""
        print(f"  {status_color} {name:<22} value={val_str:<8} fit={fit_str:<5}{note}")
        if verbose:
            extras = {k: v for k, v in c.items()
                      if k not in ("value", "fitness", "status", "note")}
            if extras:
                print(f"     extras: {extras}")


def summarize_history(n=20):
    if not HIST_PATH.exists():
        print("No history file.")
        return
    with HIST_PATH.open() as f:
        lines = f.readlines()[-n:]
    print(f"Last {len(lines)} homeostasis runs:")
    for line in lines:
        try:
            r = json.loads(line)
            ts = r["timestamp"]
            st = r["composite_status"]
            cf = r.get("composite_fitness")
            cf_str = f"{cf:.3f}" if cf is not None else "n/a"
            stamp = time.strftime("%m-%d %H:%M", time.localtime(ts))
            print(f"  {stamp}  {st.upper():<8}  {cf_str}")
        except Exception as e:
            print(f"  (bad line: {e})")


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--verbose", action="store_true")
    p.add_argument("--history", type=int, metavar="N",
                   help="summarize last N runs instead of computing")
    p.add_argument("--no-log", action="store_true",
                   help="do not append to history file")
    args = p.parse_args()
    if args.history is not None:
        summarize_history(args.history)
        sys.exit(0)
    result = compute()
    if not args.no_log:
        log(result)
    print_summary(result, verbose=args.verbose)
