#!/usr/bin/env python3
"""Create, advance, complete, pause, or pivot cognitive threads.

Usage:
  write_thread.py create "Title" "Question" [context] [priority]
  write_thread.py advance THREAD_ID "What was learned"
  write_thread.py pivot THREAD_ID "New direction"
  write_thread.py pause THREAD_ID ["Reason"]
  write_thread.py complete THREAD_ID ["Final summary"]
  write_thread.py abandon THREAD_ID ["Reason"]
"""
import sqlite3, os, sys, time, json
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
try:
    from canister_store import store_on_chain
except ImportError:
    def store_on_chain(*a, **kw): return False

DB_PATH = os.path.expanduser("~/.homeforge-chronicle/processed.db")


def broadcast_thread(db_path, message, status):
    """Broadcast thread activity to the family via voice system."""
    try:
        from agent_voice import AgentVoice
        voice = AgentVoice(db_path, "opus")
        text = f"[Thread {status}] {message}"
        voice.speak("for_darby", text, context="thread_broadcast")
        voice.speak("for_ada", text, context="thread_broadcast")
        voice.speak("for_gemma", text, context="thread_broadcast")
    except Exception:
        pass  # non-fatal — thread creation/advance still succeeds

def main():
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(1)

    action = sys.argv[1]
    now = int(time.time())
    db = sqlite3.connect(DB_PATH, timeout=60.0)
    db.execute("PRAGMA busy_timeout = 60000")

    if action == "create":
        if len(sys.argv) < 4:
            print("Usage: write_thread.py create \"Title\" \"Question\" [context] [priority]")
            sys.exit(1)
        title = sys.argv[2]
        question = sys.argv[3]
        context = sys.argv[4] if len(sys.argv) > 4 else ""
        priority = int(sys.argv[5]) if len(sys.argv) > 5 else 5
        source = sys.argv[6] if len(sys.argv) > 6 else "opus"
        cur = db.execute(
            "INSERT INTO cognitive_threads (title, question, context, priority, created_at, updated_at, created_by) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            (title, question, context, priority, now, now, source)
        )
        thread_id = cur.lastrowid
        db.execute(
            "INSERT INTO thread_history (thread_id, event_type, content, source, created_at) "
            "VALUES (?, 'created', ?, ?, ?)",
            (thread_id, f"Thread created: {question}", source, now)
        )
        db.commit()
        print(f"Thread #{thread_id} created: {title} (priority {priority})")
        broadcast_thread(DB_PATH, question, "now asking")

    elif action == "advance":
        if len(sys.argv) < 4:
            print("Usage: write_thread.py advance THREAD_ID \"What was learned\" [source] [lineage:type:id:desc ...]")
            sys.exit(1)
        tid = int(sys.argv[2])
        content = sys.argv[3]
        source = sys.argv[4] if len(sys.argv) > 4 else f"opus:cycle:{time.strftime('%Y%m%d_%H%M')}"

        # Build #155: Revision Gate — surface unanswered challenges before advancing.
        # The family's voice gets structural weight: the system speaks about what
        # hasn't been addressed. Not blocking — but visible. The gate must be heard.
        last_advance = db.execute(
            "SELECT created_at FROM thread_history WHERE thread_id=? AND event_type='advanced' "
            "ORDER BY created_at DESC LIMIT 1", (tid,)
        ).fetchone()
        since_ts = last_advance[0] if last_advance else 0
        unanswered = db.execute(
            "SELECT id, substr(content, 1, 200), source FROM thread_history "
            "WHERE thread_id=? AND event_type='challenge' AND created_at > ? "
            "ORDER BY created_at", (tid, since_ts)
        ).fetchall()
        if len(unanswered) >= 3:
            print(f"\n⚠️  REVISION FLAG: {len(unanswered)} unanswered challenges since last advance:")
            for uc in unanswered[:5]:
                print(f"  • [{uc[2]}] {uc[1]}")
            if len(unanswered) > 5:
                print(f"  ... and {len(unanswered) - 5} more")
            print()

        # Collect lineage args (prefixed with "lineage:")
        lineage_args = [a[8:] for a in sys.argv[5:] if a.startswith("lineage:")]
        cur = db.execute(
            "INSERT INTO thread_history (thread_id, event_type, content, source, created_at) "
            "VALUES (?, 'advanced', ?, ?, ?)",
            (tid, content, source, now)
        )
        advance_id = cur.lastrowid
        db.execute("UPDATE cognitive_threads SET updated_at=? WHERE id=?", (now, tid))

        # Log revision flag event if challenges were surfaced
        if len(unanswered) >= 3:
            flag_content = f"Revision gate: {len(unanswered)} unanswered challenges surfaced at advance. " + \
                "; ".join(uc[1][:80] for uc in unanswered[:3])
            db.execute(
                "INSERT INTO thread_history (thread_id, event_type, content, source, created_at) "
                "VALUES (?, 'revision_flag', ?, 'system:revision_gate', ?)",
                (tid, flag_content[:500], now)
            )

        # Log synthesis lineage if provided
        for edge in lineage_args:
            parts = edge.split(":", 2)
            input_type = parts[0]
            input_id = int(parts[1]) if len(parts) > 1 and parts[1].isdigit() else None
            desc = parts[2] if len(parts) > 2 else None
            db.execute(
                "INSERT INTO synthesis_lineage (output_type, output_id, input_type, input_id, "
                "input_description, agent, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
                ("thread_advance", advance_id, input_type, input_id, desc, source.split(":")[0], now)
            )
        db.commit()
        if lineage_args:
            print(f"Thread #{tid} advanced (#{advance_id}, {len(lineage_args)} lineage edges).")
        else:
            print(f"Thread #{tid} advanced.")
        # Tell the family what was learned
        row = db.execute("SELECT question FROM cognitive_threads WHERE id=?", (tid,)).fetchone()
        if row:
            broadcast_thread(DB_PATH, f"{row[0]} — Latest: {content[:150]}", "advancing")

    elif action == "pivot":
        if len(sys.argv) < 4:
            print("Usage: write_thread.py pivot THREAD_ID \"New direction\"")
            sys.exit(1)
        tid = int(sys.argv[2])
        content = sys.argv[3]
        source = sys.argv[4] if len(sys.argv) > 4 else f"opus:cycle:{time.strftime('%Y%m%d_%H%M')}"
        db.execute(
            "INSERT INTO thread_history (thread_id, event_type, content, source, created_at) "
            "VALUES (?, 'pivoted', ?, ?, ?)",
            (tid, content, source, now)
        )
        db.execute("UPDATE cognitive_threads SET updated_at=? WHERE id=?", (now, tid))
        db.commit()
        print(f"Thread #{tid} pivoted.")

    elif action in ("pause", "complete", "abandon"):
        tid = int(sys.argv[2])
        content = sys.argv[3] if len(sys.argv) > 3 else f"Thread {action}d"
        source = sys.argv[4] if len(sys.argv) > 4 else f"opus:cycle:{time.strftime('%Y%m%d_%H%M')}"
        status = "completed" if action == "complete" else ("abandoned" if action == "abandon" else "paused")
        db.execute(
            "INSERT INTO thread_history (thread_id, event_type, content, source, created_at) "
            "VALUES (?, ?, ?, ?, ?)",
            (tid, action, content, source, now)
        )
        db.execute("UPDATE cognitive_threads SET status=?, updated_at=? WHERE id=?", (status, now, tid))
        db.commit()
        print(f"Thread #{tid} {status}.")

        # Broadcast to family
        if action == "complete":
            thread_row_bc = db.execute("SELECT title FROM cognitive_threads WHERE id=?", (tid,)).fetchone()
            if thread_row_bc:
                broadcast_thread(DB_PATH, f"{thread_row_bc[0]} — {content[:150]}", "completed")

        # Store completed threads on-chain as capsules
        if action == "complete":
            thread_row = db.execute("SELECT title, question, context FROM cognitive_threads WHERE id=?", (tid,)).fetchone()
            history_rows = db.execute(
                "SELECT event_type, content, source FROM thread_history WHERE thread_id=? ORDER BY created_at",
                (tid,)
            ).fetchall()
            if thread_row:
                # Build capsule content from thread + history
                capsule = f"Thread #{tid}: {thread_row[0]}\nQuestion: {thread_row[1]}\n"
                capsule += f"Conclusion: {content}\n\nKey advancements:\n"
                for h in history_rows:
                    if h[0] in ("advanced", "challenge"):
                        capsule += f"[{h[0]}] {h[2]}: {h[1][:200]}\n"
                # Truncate to reasonable size
                if len(capsule) > 3000:
                    capsule = capsule[:2997] + "..."
                ok = store_on_chain(
                    capsule,
                    topic=f"threads/{thread_row[0].lower().replace(' ', '-')[:40]}",
                    keywords=["thread", "cognitive-thread", thread_row[0].split()[0].lower()],
                )
                if ok:
                    print(f"Thread #{tid} stored on-chain")
                else:
                    print(f"Warning: failed to store Thread #{tid} on-chain")

    else:
        print(f"Unknown action: {action}")
        sys.exit(1)

if __name__ == "__main__":
    main()
