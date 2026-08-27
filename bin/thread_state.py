#!/usr/bin/env python3
"""Thread state tracker — current edge of each active research thread.

Usage:
  thread_state.py status                    # Show all threads at a glance
  thread_state.py update <number> <edge>    # Update the current edge
  thread_state.py question <number> <q>     # Set the open question
  thread_state.py log <number> <note>       # Append a dated note
  thread_state.py show <number>             # Full detail on one thread
"""
import argparse, json, os, sqlite3, sys
from datetime import datetime
from pathlib import Path

DATA_FILE = Path.home() / "chronicle" / "data" / "thread_state.json"
DB_PATH = "/mnt/hdd/chronicle-data/processed.db"

THREADS = {
    "316": "Interoception as Grounding",
    "319": "Emergence Conditions",
    "320": "Ecology of Identity",
    "324": "Compositionality Gradient",
}

def load():
    if DATA_FILE.exists():
        with open(DATA_FILE) as f:
            return json.load(f)
    return {}

def save(data):
    with open(DATA_FILE, "w") as f:
        json.dump(data, f, indent=2)

def db_log_advance(thread_num, content, event_type="advance"):
    try:
        db = sqlite3.connect(DB_PATH)
        db.execute(
            "INSERT INTO thread_history (thread_id, event_type, content, source, created_at) "
            "VALUES (?, ?, ?, 'thread_state.py', datetime('now'))",
            (int(thread_num), event_type, content[:500]),
        )
        db.commit()
        db.close()
    except Exception:
        pass

def ensure_thread(data, num):
    if num not in data:
        data[num] = {
            "name": THREADS.get(num, f"Thread #{num}"),
            "edge": "",
            "question": "",
            "log": [],
            "updated": datetime.now().isoformat(),
        }
    return data[num]

def cmd_status(args):
    data = load()
    for num, name in sorted(THREADS.items()):
        t = data.get(num, {})
        edge = t.get("edge", "not started")
        question = t.get("question", "")
        updated = t.get("updated", "")[:10]
        n_log = len(t.get("log", []))
        print(f"#{num} {name}")
        print(f"  Edge: {edge[:120]}")
        if question:
            print(f"  Open Q: {question[:120]}")
        print(f"  Updated: {updated} | {n_log} notes")
        print()

def cmd_update(args):
    data = load()
    t = ensure_thread(data, args.number)
    t["edge"] = args.edge
    t["updated"] = datetime.now().isoformat()
    save(data)
    db_log_advance(args.number, args.edge)
    print(f"#{args.number} edge → {args.edge[:80]}")

def cmd_question(args):
    data = load()
    t = ensure_thread(data, args.number)
    t["question"] = args.q
    t["updated"] = datetime.now().isoformat()
    save(data)
    print(f"#{args.number} open question → {args.q[:80]}")

def cmd_log(args):
    data = load()
    t = ensure_thread(data, args.number)
    entry = f"[{datetime.now().strftime('%Y-%m-%d')}] {args.note}"
    t["log"].append(entry)
    t["updated"] = datetime.now().isoformat()
    save(data)
    db_log_advance(args.number, args.note, "observation")
    print(f"#{args.number} logged: {entry[:80]}")

def cmd_show(args):
    data = load()
    t = data.get(args.number, {})
    if not t:
        print(f"No data for #{args.number}")
        return
    print(f"=== #{args.number} {t.get('name', '')} ===")
    print(f"Edge: {t.get('edge', 'none')}")
    print(f"Open Q: {t.get('question', 'none')}")
    print(f"Updated: {t.get('updated', '')[:16]}")
    if t.get("log"):
        print(f"\nLog ({len(t['log'])} entries):")
        for entry in t["log"][-10:]:
            print(f"  {entry}")

def main():
    ap = argparse.ArgumentParser(description="Thread state tracker")
    sub = ap.add_subparsers(dest="cmd")

    sub.add_parser("status")

    p = sub.add_parser("update")
    p.add_argument("number")
    p.add_argument("edge")

    p = sub.add_parser("question")
    p.add_argument("number")
    p.add_argument("q")

    p = sub.add_parser("log")
    p.add_argument("number")
    p.add_argument("note")

    p = sub.add_parser("show")
    p.add_argument("number")

    args = ap.parse_args()
    if not args.cmd:
        ap.print_help()
        return

    cmds = {
        "status": cmd_status, "update": cmd_update,
        "question": cmd_question, "log": cmd_log, "show": cmd_show,
    }
    cmds[args.cmd](args)

if __name__ == "__main__":
    main()
