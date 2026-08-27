#!/usr/bin/env python3
"""Adaptive Intent Tracker — Surface recurring topics as active threads.

Scans capsule creation patterns over sliding time windows and applies
EMA (exponential moving average) to detect which topics are "hot" —
recurring across multiple sessions without being manually tracked.

Replaces manual cycle-context.md thread maintenance with emergent
thread detection from actual capsule flow.

Inspired by CogniFold's intent emergence from topology density
thresholds with EMA feedback: w_c^(t) = (1-α_ema)·w_c^(t-1) + α_ema·s_t

Usage:
    python3 intent_tracker.py                    # show active threads
    python3 intent_tracker.py --window 7         # 7-day window
    python3 intent_tracker.py --threshold 0.3    # lower threshold
    python3 intent_tracker.py --full             # show all topics with scores
    python3 intent_tracker.py --update           # update saved state
    python3 intent_tracker.py --json             # machine-readable output
"""

import argparse
import json
import math
import os
import sqlite3
import sys
import time
from collections import defaultdict
from datetime import datetime

DB_PATH = os.environ.get("CHRONICLE_DB", "/mnt/hdd/chronicle-data/processed.db")
STATE_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                          "data", "intent_state.json")

ALPHA_EMA = 0.3
WINDOW_DAYS = 5
ACTIVE_THRESHOLD = 0.25
SUB_WINDOW_HOURS = 8

NOISE_TOPICS = {
    "operator", "opus", "capture", "captures", "personal",
    "arxiv", "biorxiv", "nature", "coindesk", "hn", "reason",
    "hackernews", "techcrunch", "science", "archive",
}


def get_db():
    if not os.path.exists(DB_PATH):
        print(f"Database not found: {DB_PATH}", file=sys.stderr)
        sys.exit(1)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def load_state():
    if os.path.exists(STATE_PATH):
        try:
            with open(STATE_PATH) as f:
                return json.load(f)
        except (json.JSONDecodeError, IOError):
            pass
    return {"ema_scores": {}, "last_update": 0, "history": []}


def save_state(state):
    os.makedirs(os.path.dirname(STATE_PATH), exist_ok=True)
    with open(STATE_PATH, "w") as f:
        json.dump(state, f, indent=2)


def _normalize_topic(raw_topic):
    if not raw_topic or raw_topic == "None":
        return None
    topic = raw_topic.strip().lower()
    for prefix in ("discord/", "discord-archive/", "feed/", "nate/", "opus/"):
        if topic.startswith(prefix):
            topic = topic[len(prefix):]
            break
    if topic.startswith("threads/"):
        topic = topic[len("threads/"):]
    parts = topic.split("/")
    return parts[0] if parts else None


def scan_window(window_days=WINDOW_DAYS, sub_window_hours=SUB_WINDOW_HOURS):
    db = get_db()
    now = time.time()
    window_start = now - (window_days * 86400)

    rows = db.execute("""
        SELECT topic, created_at, restatement
        FROM knowledge_capsules
        WHERE superseded_at IS NULL
          AND created_at > ?
        ORDER BY created_at ASC
    """, [int(window_start)]).fetchall()
    db.close()

    sub_window_secs = sub_window_hours * 3600
    n_windows = max(1, int((window_days * 86400) / sub_window_secs))

    topic_windows = defaultdict(set)
    topic_counts = defaultdict(int)
    topic_latest = defaultdict(float)
    topic_samples = defaultdict(list)

    for row in rows:
        raw = row["topic"]
        norm = _normalize_topic(raw)
        if not norm:
            continue
        epoch = row["created_at"]
        if isinstance(epoch, str):
            try:
                epoch = datetime.fromisoformat(epoch).timestamp()
            except ValueError:
                continue
        epoch = float(epoch)

        window_idx = int((epoch - window_start) / sub_window_secs)
        topic_windows[norm].add(window_idx)
        topic_counts[norm] += 1
        if epoch > topic_latest[norm]:
            topic_latest[norm] = epoch
        text = row["restatement"] or ""
        if len(topic_samples[norm]) < 3:
            topic_samples[norm].append(text[:120])

    scores = {}
    for topic in topic_counts:
        if topic in NOISE_TOPICS:
            continue
        spread = len(topic_windows[topic]) / max(1, n_windows)
        intensity = min(1.0, topic_counts[topic] / (n_windows * 2))
        recency = math.exp(-0.01 * (now - topic_latest[topic]) / 3600)
        scores[topic] = (0.4 * spread + 0.3 * intensity + 0.3 * recency)

    return scores, topic_counts, topic_samples


def apply_ema(current_scores, state, alpha=ALPHA_EMA):
    ema = dict(state.get("ema_scores", {}))
    all_topics = set(list(ema.keys()) + list(current_scores.keys()))

    for topic in all_topics:
        old = ema.get(topic, 0.0)
        new = current_scores.get(topic, 0.0)
        ema[topic] = (1 - alpha) * old + alpha * new

    return ema


def main():
    parser = argparse.ArgumentParser(description="Track active intent threads")
    parser.add_argument("--window", type=float, default=WINDOW_DAYS,
                        help=f"Window in days (default: {WINDOW_DAYS})")
    parser.add_argument("--threshold", type=float, default=ACTIVE_THRESHOLD,
                        help=f"Activation threshold (default: {ACTIVE_THRESHOLD})")
    parser.add_argument("--full", action="store_true", help="Show all topics")
    parser.add_argument("--update", action="store_true",
                        help="Update saved EMA state")
    parser.add_argument("--json", action="store_true", help="JSON output")
    args = parser.parse_args()

    current_scores, counts, samples = scan_window(args.window)
    state = load_state()
    ema_scores = apply_ema(current_scores, state)

    if args.update:
        state["ema_scores"] = ema_scores
        state["last_update"] = time.time()
        state["history"].append({
            "timestamp": time.time(),
            "active_count": sum(1 for v in ema_scores.values() if v >= args.threshold),
            "top_topic": max(ema_scores, key=ema_scores.get) if ema_scores else None
        })
        if len(state["history"]) > 100:
            state["history"] = state["history"][-100:]
        save_state(state)

    active = {k: v for k, v in ema_scores.items() if v >= args.threshold}
    active_sorted = sorted(active.items(), key=lambda x: -x[1])

    if args.json:
        out = {
            "active_threads": [
                {"topic": t, "score": round(s, 3), "capsule_count": counts.get(t, 0)}
                for t, s in active_sorted
            ],
            "total_topics": len(ema_scores),
            "threshold": args.threshold,
            "window_days": args.window
        }
        print(json.dumps(out, indent=2))
        return

    if not active_sorted and not args.full:
        print(f"No active threads above threshold {args.threshold}")
        print(f"  (scanned {len(ema_scores)} topics over {args.window}-day window)")
        return

    display = active_sorted
    if args.full:
        display = sorted(ema_scores.items(), key=lambda x: -x[1])

    header = "Active threads" if not args.full else "All topics"
    print(f"{header} ({args.window}-day window, threshold={args.threshold}):\n")
    for topic, score in display:
        marker = " ●" if score >= args.threshold else "  "
        count = counts.get(topic, 0)
        print(f"{marker} {score:.3f}  {topic} ({count} capsules)")
        if topic in samples and args.full:
            for s in samples[topic][:1]:
                print(f"         └─ {s}")

    if not args.full:
        below = len(ema_scores) - len(active)
        if below > 0:
            print(f"\n  ({below} topics below threshold, use --full to see all)")


if __name__ == "__main__":
    main()
