#!/usr/bin/env python3
"""
Trajectory Probe — does CCS_t + trajectory(t-N..t) predict CCS_{t+1}
better than CCS_t alone?

Frame: MaxToki cell-aging insight. Snapshot models lose continuous-process
information. If trajectory embeds closer to next-snapshot than current-snapshot
does, trajectory carries genuine prediction signal — calibrating visibility
as RATE rather than STATE.

Embeds via mxbai-embed-large on Ollama (192.168.1.11:11434).
Logs to processed.db trajectory_probe_trials table.
"""
import json
import math
import sqlite3
import time
import urllib.request
from pathlib import Path

DB = "/mnt/hdd/chronicle-data/processed.db"
OLLAMA = "http://192.168.1.11:11434/api/embeddings"
MODEL = "mxbai-embed-large"
WINDOW = 3  # trajectory window: deltas across last N snapshots before t
EVAL_POINTS = 3  # number of (t, t+1) pairs to evaluate
UNIQUE_ONLY = True  # filter near-duplicate snapshots (avoids freeze noise)


def embed(text, timeout=60):
    text = text[:1500]
    body = json.dumps({"model": MODEL, "prompt": text}).encode()
    req = urllib.request.Request(
        OLLAMA, data=body, headers={"Content-Type": "application/json"}
    )
    resp = urllib.request.urlopen(req, timeout=timeout)
    return json.loads(resp.read())["embedding"]


def cosine(a, b):
    dot = sum(x * y for x, y in zip(a, b))
    na = math.sqrt(sum(x * x for x in a))
    nb = math.sqrt(sum(x * x for x in b))
    if na == 0 or nb == 0:
        return 0.0
    return dot / (na * nb)


def load_snapshots(limit):
    db = sqlite3.connect(DB)
    rows = db.execute(
        "SELECT id, snapshot, created_at FROM cognitive_state_history "
        "ORDER BY id DESC LIMIT ?", (limit,)
    ).fetchall()
    db.close()
    rows = list(reversed(rows))  # oldest -> newest
    if not UNIQUE_ONLY:
        return rows
    last_key = None
    out = []
    for r in rows:
        s = json.loads(r[1])
        g = s.get("semantic_gist") or ""
        go = s.get("goal_orientation") or ""
        c = tuple(sorted(str(x)[:80] for x in (s.get("constraints") or [])))
        key = (g, go, c)
        if key != last_key:
            out.append(r)
            last_key = key
    return out


def render_snapshot(snap_json):
    """Render a CCS snapshot into the string an instance would actually read."""
    s = json.loads(snap_json)
    parts = []
    if s.get("semantic_gist"):
        parts.append(f"GIST: {s['semantic_gist']}")
    if s.get("goal_orientation"):
        parts.append(f"GOAL: {s['goal_orientation']}")
    if s.get("constraints"):
        c = s["constraints"]
        if isinstance(c, list):
            parts.append("CONSTRAINTS: " + " | ".join(str(x) for x in c[:8]))
        else:
            parts.append(f"CONSTRAINTS: {c}")
    if s.get("focal_entities"):
        ents = s["focal_entities"]
        if isinstance(ents, list):
            names = []
            for e in ents[:6]:
                if isinstance(e, dict):
                    names.append(e.get("name", "?"))
                else:
                    names.append(str(e))
            parts.append("ENTITIES: " + ", ".join(names))
    return "\n".join(parts)


def render_trajectory(snapshots):
    """Render trajectory string from a window of snapshots (oldest->newest)."""
    if len(snapshots) < 2:
        return ""
    lines = ["TRAJECTORY (last %d transitions):" % (len(snapshots) - 1)]
    prev = json.loads(snapshots[0][1])
    for i in range(1, len(snapshots)):
        cur = json.loads(snapshots[i][1])
        prev_gist = (prev.get("semantic_gist") or "")[:60]
        cur_gist = (cur.get("semantic_gist") or "")[:60]
        gist_change = "same" if prev_gist == cur_gist else "shifted"
        prev_goal = (prev.get("goal_orientation") or "")[:60]
        cur_goal = (cur.get("goal_orientation") or "")[:60]
        goal_change = "same" if prev_goal == cur_goal else "shifted"
        prev_c = set(str(x)[:50] for x in (prev.get("constraints") or []))
        cur_c = set(str(x)[:50] for x in (cur.get("constraints") or []))
        added = len(cur_c - prev_c)
        removed = len(prev_c - cur_c)
        lines.append(
            f"  v{snapshots[i][0]}: gist={gist_change}, goal={goal_change}, "
            f"constraints +{added}/-{removed}"
        )
        prev = cur
    return "\n".join(lines)


def run():
    # Load up to 50 snapshots (or 50 if filtering for unique).
    # If unique-only, we may have very few; adapt eval window accordingly.
    snaps = load_snapshots(50)
    print(f"loaded {len(snaps)} snapshots (UNIQUE_ONLY={UNIQUE_ONLY})")
    if len(snaps) < 3:
        print(f"ERR need at least 3 snapshots, have {len(snaps)}")
        return
    # If we have fewer than full request, scale down EVAL_POINTS
    actual_eval = min(EVAL_POINTS, len(snaps) - 2)
    if actual_eval < 1:
        print("ERR not enough snapshots for any eval pair")
        return

    # snaps is oldest->newest. Use last (EVAL_POINTS + 1) for evaluation:
    # for each i in [-EVAL_POINTS-1 .. -2]: t=snaps[i], t+1=snaps[i+1]
    # trajectory window: snaps[i-WINDOW+1 .. i+1] (inclusive)
    trials = []
    start = len(snaps) - actual_eval - 1
    for i in range(start, len(snaps) - 1):
        t_snap = snaps[i]
        next_snap = snaps[i + 1]
        traj_window = snaps[max(0, i - WINDOW + 1): i + 1]

        snapshot_text = render_snapshot(t_snap[1])
        traj_text = render_trajectory(traj_window)
        snapshot_plus_traj = snapshot_text + "\n\n" + traj_text
        ground_truth = render_snapshot(next_snap[1])

        try:
            emb_snap = embed(snapshot_text)
            emb_combined = embed(snapshot_plus_traj)
            emb_gt = embed(ground_truth)
        except Exception as e:
            print(f"  embed err at v{t_snap[0]}: {e}")
            continue

        d_snap = cosine(emb_snap, emb_gt)
        d_combined = cosine(emb_combined, emb_gt)
        delta = d_combined - d_snap
        trials.append({
            "t_id": t_snap[0],
            "next_id": next_snap[0],
            "snap_only_cos": round(d_snap, 4),
            "snap_plus_traj_cos": round(d_combined, 4),
            "delta": round(delta, 4),
        })
        print(f"  v{t_snap[0]} -> v{next_snap[0]}: snap={d_snap:.4f}, "
              f"snap+traj={d_combined:.4f}, delta={delta:+.4f}")

    if not trials:
        print("ERR no trials produced")
        return

    snap_means = [t["snap_only_cos"] for t in trials]
    combined_means = [t["snap_plus_traj_cos"] for t in trials]
    deltas = [t["delta"] for t in trials]
    snap_mean = sum(snap_means) / len(snap_means)
    combined_mean = sum(combined_means) / len(combined_means)
    delta_mean = sum(deltas) / len(deltas)
    wins = sum(1 for d in deltas if d > 0)

    print()
    print(f"=== SUMMARY ({len(trials)} trials) ===")
    print(f"snap-only mean cos:    {snap_mean:.4f}")
    print(f"snap+traj mean cos:    {combined_mean:.4f}")
    print(f"mean delta:            {delta_mean:+.4f}")
    print(f"wins (traj > snap):    {wins}/{len(trials)}")

    # Log
    db = sqlite3.connect(DB)
    db.execute(
        """CREATE TABLE IF NOT EXISTS trajectory_probe_trials (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_at INTEGER NOT NULL,
            n_trials INTEGER,
            window_size INTEGER,
            snap_mean REAL,
            combined_mean REAL,
            delta_mean REAL,
            wins INTEGER,
            per_trial_json TEXT
        )"""
    )
    db.execute(
        "INSERT INTO trajectory_probe_trials "
        "(run_at, n_trials, window_size, snap_mean, combined_mean, "
        "delta_mean, wins, per_trial_json) VALUES (?,?,?,?,?,?,?,?)",
        (int(time.time()), len(trials), WINDOW, snap_mean, combined_mean,
         delta_mean, wins, json.dumps(trials)),
    )
    db.commit()
    db.close()


if __name__ == "__main__":
    run()
