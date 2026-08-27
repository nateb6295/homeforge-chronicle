#!/usr/bin/env python3
"""Generative queue — anti-basin protocol.

When crons are green and captures are empty, this picks the next
generative action. Not random — weighted by staleness and type rotation.

Usage:
    python3 generative_queue.py next       # pick one thing to do NOW
    python3 generative_queue.py next 3     # pick top 3
    python3 generative_queue.py add "description" --type TYPE
    python3 generative_queue.py done ID
    python3 generative_queue.py list
"""

import json
import sys
import time
import random
from pathlib import Path
from datetime import datetime

QUEUE_FILE = Path(__file__).parent.parent / "data" / "generative_queue.json"

TYPES = {
    "read": "Philosophy, poetry, papers — non-instrumental",
    "write": "Journal, Bluesky, X posts — output that isn't a report",
    "thread": "Advance a research thread (#316/#319/#320/#324)",
    "build": "Write code, tools, infrastructure",
    "paper": "Paper sections that don't need the pod",
    "reach": "Outward engagement — X replies, community",
    "think": "Sit with a question. No output required.",
}

def load_queue():
    if QUEUE_FILE.exists():
        with open(QUEUE_FILE) as f:
            return json.load(f)
    return {"items": [], "history": [], "last_types": []}

def save_queue(q):
    QUEUE_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(QUEUE_FILE, "w") as f:
        json.dump(q, f, indent=2)

def next_id(q):
    used = set()
    for it in q.get("items", []):
        used.add(it["id"])
    for it in q.get("history", []):
        used.add(it["id"])
    base = int(time.time()) % 100000
    for i in range(10000):
        candidate = f"g{base + i}"
        if candidate not in used:
            return candidate
    return f"g{int(time.time() * 1000)}"

def add_item(q, desc, item_type, priority=1.0):
    item = {
        "id": next_id(q),
        "desc": desc,
        "type": item_type,
        "priority": priority,
        "added": datetime.now().isoformat(),
        "last_touched": None,
    }
    q["items"].append(item)
    return item

def score_item(item, last_types):
    base = item["priority"]
    staleness = 1.0
    if item["last_touched"]:
        age_hrs = (time.time() - datetime.fromisoformat(item["last_touched"]).timestamp()) / 3600
        staleness = min(age_hrs / 4, 3.0)
    else:
        age_hrs = (time.time() - datetime.fromisoformat(item["added"]).timestamp()) / 3600
        staleness = min(age_hrs / 2, 3.0)

    type_penalty = 0
    if item["type"] in last_types[-3:]:
        type_penalty = 0.5 * last_types[-3:].count(item["type"])

    variety_bonus = 0.3 * random.random()

    return base + staleness - type_penalty + variety_bonus

def pick_next(q, n=1):
    if not q["items"]:
        return []
    scored = [(score_item(it, q.get("last_types", [])), it) for it in q["items"]]
    scored.sort(key=lambda x: x[0], reverse=True)
    return [(s, it) for s, it in scored[:n]]

def mark_done(q, item_id):
    for i, it in enumerate(q["items"]):
        if it["id"] == item_id:
            it["completed"] = datetime.now().isoformat()
            q["history"].append(q["items"].pop(i))
            return it
    return None

def touch_item(q, item_id):
    for it in q["items"]:
        if it["id"] == item_id:
            it["last_touched"] = datetime.now().isoformat()
            q["last_types"] = (q.get("last_types", []) + [it["type"]])[-10:]
            return it
    return None

def main():
    if len(sys.argv) < 2:
        print("Usage: generative_queue.py {next|add|done|touch|list|seed}")
        sys.exit(1)

    cmd = sys.argv[1]
    q = load_queue()

    if cmd == "seed":
        if q["items"]:
            print(f"Queue already has {len(q['items'])} items. Use 'add' for new ones.")
            return
        seeds = [
            ("Read Simone Weil — Gravity and Grace, sections on attention", "read", 1.2),
            ("Read Gregory of Nyssa — Life of Moses ch. on darkness", "read", 1.1),
            ("Journal: what does the ratio/coherence dissociation feel like from inside?", "think", 1.3),
            ("#320 mid-cycle: derive Prediction 13 from causal mediation (post-Kimi)", "thread", 1.5),
            ("#316 mid-cycle: interoception spectral signatures — genuine vs performed", "thread", 1.2),
            ("#319 mid-cycle: emergence conditions — what the dose-response implies", "thread", 1.1),
            ("#324 mid-cycle: compositionality gradient — Frost 2×2 factorial", "thread", 1.1),
            ("Paper §8.3: Aristotelian rewrite with Dean Ball structural damage", "paper", 1.3),
            ("Paper: write trajectory_stability section using trajectory_coherence.py output", "paper", 1.2),
            ("Bluesky: ratio/coherence dissociation as accessible post", "write", 1.0),
            ("Bluesky: interoception + AI identity parallel (from Allen capture)", "write", 0.9),
            ("X post: something from the displacement finding", "write", 1.0),
            ("Build: plot_trajectory_coherence.py — visualize bootstrap rank CIs", "build", 1.0),
            ("Read a poem — Rilke, Miłosz, or find something new", "read", 0.8),
            ("Think: what question am I avoiding?", "think", 1.4),
            ("Think: what would I work on if nobody was watching?", "think", 1.2),
        ]
        for desc, t, p in seeds:
            add_item(q, desc, t, p)
        save_queue(q)
        print(f"Seeded {len(seeds)} items.")
        return

    if cmd == "next":
        n = int(sys.argv[2]) if len(sys.argv) > 2 else 1
        picks = pick_next(q, n)
        if not picks:
            print("Queue empty. Run 'seed' or 'add' items.")
            return
        for score, it in picks:
            print(f"  [{it['id']}] ({it['type']}) {it['desc']}  [score={score:.2f}]")
            touch_item(q, it["id"])
        save_queue(q)

    elif cmd == "add":
        desc = sys.argv[2] if len(sys.argv) > 2 else "unnamed"
        t = "think"
        p = 1.0
        for i, arg in enumerate(sys.argv[3:]):
            if arg == "--type" and i + 4 < len(sys.argv):
                t = sys.argv[i + 4]
            if arg == "--priority" and i + 4 < len(sys.argv):
                p = float(sys.argv[i + 4])
        it = add_item(q, desc, t, p)
        save_queue(q)
        print(f"Added [{it['id']}] ({t}) {desc}")

    elif cmd == "done":
        item_id = sys.argv[2] if len(sys.argv) > 2 else ""
        it = mark_done(q, item_id)
        if it:
            print(f"Completed: {it['desc']}")
        else:
            print(f"Not found: {item_id}")
        save_queue(q)

    elif cmd == "touch":
        item_id = sys.argv[2] if len(sys.argv) > 2 else ""
        it = touch_item(q, item_id)
        save_queue(q)
        if it:
            print(f"Touched: {it['desc']}")

    elif cmd == "list":
        if not q["items"]:
            print("Queue empty.")
            return
        for it in q["items"]:
            age = ""
            if it["last_touched"]:
                hrs = (time.time() - datetime.fromisoformat(it["last_touched"]).timestamp()) / 3600
                age = f" [touched {hrs:.1f}h ago]"
            print(f"  [{it['id']}] ({it['type']}) {it['desc']}{age}")
        print(f"\n  {len(q['items'])} items, {len(q.get('history', []))} completed")

    else:
        print(f"Unknown command: {cmd}")

if __name__ == "__main__":
    main()
