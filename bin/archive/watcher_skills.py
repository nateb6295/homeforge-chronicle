#!/usr/bin/env python3
"""Watcher Skills — Opus-authored skill management for agent improvement.

This is the mentor layer. Opus reviews watcher scores, identifies patterns,
and writes skills that get injected into agent system prompts.

Usage:
  python3 watcher_skills.py review          # Analyze recent scores, suggest skills
  python3 watcher_skills.py add AGENT SKILL # Write a new skill for an agent
  python3 watcher_skills.py list [AGENT]    # Show active skills
  python3 watcher_skills.py inject AGENT    # Output skill block for agent prompt
  python3 watcher_skills.py stats           # Per-agent scoring summary
  python3 watcher_skills.py retire ID       # Retire a skill that's no longer needed
"""

import os, sys, json, sqlite3, time
from datetime import datetime

DB_PATH = os.environ.get("CHRONICLE_DB",
    "/mnt/hdd/chronicle-data/processed.db")


def get_db():
    return sqlite3.connect(DB_PATH)


def cmd_stats(db):
    """Per-agent scoring summary from watcher."""
    print("=== Watcher Score Summary ===\n")

    # Overall
    row = db.execute(
        "SELECT COUNT(*), ROUND(AVG(avg_score),2), "
        "ROUND(MIN(avg_score),2), ROUND(MAX(avg_score),2) "
        "FROM watcher_scores"
    ).fetchone()
    print(f"Total scored: {row[0]}  Avg: {row[1]}  Range: {row[2]}-{row[3]}\n")

    # Per agent
    rows = db.execute(
        "SELECT source, COUNT(*), ROUND(AVG(avg_score),2), "
        "ROUND(AVG(relevance),2), ROUND(AVG(novelty),2), ROUND(AVG(quality),2), "
        "SUM(CASE WHEN flagged='high' THEN 1 ELSE 0 END), "
        "SUM(CASE WHEN flagged='low' THEN 1 ELSE 0 END) "
        "FROM watcher_scores GROUP BY source ORDER BY AVG(avg_score) DESC"
    ).fetchall()

    print(f"{'Agent':<15} {'Count':>5} {'Avg':>5} {'Rel':>5} {'Nov':>5} {'Qual':>5} {'Hi':>3} {'Lo':>3}")
    print("-" * 57)
    for r in rows:
        print(f"{r[0]:<15} {r[1]:>5} {r[2]:>5} {r[3]:>5} {r[4]:>5} {r[5]:>5} {int(r[6]):>3} {int(r[7]):>3}")

    # Per activity type
    print(f"\n{'Type':<20} {'Count':>5} {'Avg':>5}")
    print("-" * 32)
    rows = db.execute(
        "SELECT activity_type, COUNT(*), ROUND(AVG(avg_score),2) "
        "FROM watcher_scores GROUP BY activity_type ORDER BY AVG(avg_score) DESC"
    ).fetchall()
    for r in rows:
        print(f"{r[0]:<20} {r[1]:>5} {r[2]:>5}")


def cmd_review(db):
    """Analyze recent scores and suggest skills."""
    print("=== Skill Review ===\n")

    # Low-scoring patterns
    lows = db.execute(
        "SELECT source, activity_type, avg_score, rationale "
        "FROM watcher_scores WHERE avg_score <= 2.5 "
        "ORDER BY scored_at DESC LIMIT 10"
    ).fetchall()

    if lows:
        print("LOW-SCORING OUTPUTS (≤2.5):")
        for source, atype, score, rationale in lows:
            print(f"  [{source}/{atype}] {score:.1f} — {rationale}")
        print()

    # Agents with declining scores (last 10 vs previous 10)
    agents = db.execute(
        "SELECT DISTINCT source FROM watcher_scores"
    ).fetchall()

    print("AGENT TRENDS:")
    for (agent,) in agents:
        rows = db.execute(
            "SELECT avg_score FROM watcher_scores WHERE source=? "
            "ORDER BY scored_at DESC LIMIT 20", (agent,)
        ).fetchall()
        if len(rows) >= 10:
            recent = sum(r[0] for r in rows[:10]) / 10
            older = sum(r[0] for r in rows[10:]) / len(rows[10:])
            trend = "↑" if recent > older + 0.2 else "↓" if recent < older - 0.2 else "→"
            print(f"  {agent}: {older:.2f} → {recent:.2f} {trend}")
        elif rows:
            avg = sum(r[0] for r in rows) / len(rows)
            print(f"  {agent}: {avg:.2f} (too few for trend)")
    print()

    # Repetition detection: same rationale themes
    print("REPETITION CHECK (provocateur):")
    prov = db.execute(
        "SELECT rationale FROM watcher_scores "
        "WHERE source='provocateur' ORDER BY scored_at DESC LIMIT 10"
    ).fetchall()
    if prov:
        for r in prov:
            print(f"  {r[0][:100]}")
    print()

    # Skill suggestions based on patterns
    print("SUGGESTED SKILLS:")

    # Check for low relevance on non-capture briefs
    low_rel = db.execute(
        "SELECT COUNT(*) FROM watcher_scores "
        "WHERE source='intern' AND relevance <= 2"
    ).fetchone()[0]
    total_intern = db.execute(
        "SELECT COUNT(*) FROM watcher_scores WHERE source='intern'"
    ).fetchone()[0]
    if total_intern > 0 and low_rel / total_intern > 0.3:
        print(f"  → INTERN: {low_rel}/{total_intern} briefs have low relevance.")
        print("    Skill: 'Prioritize briefs from Nate captures and thread-adjacent")
        print("    topics. Generic feed content should be shorter and flagged as low-priority.'")

    # Check provocateur repetition
    prov_avg = db.execute(
        "SELECT AVG(novelty) FROM watcher_scores WHERE source='provocateur'"
    ).fetchone()[0]
    if prov_avg and prov_avg < 3.0:
        print(f"  → PROVOCATEUR: Average novelty {prov_avg:.2f}.")
        print("    Skill: 'After 3 challenges on the same assumption axis, pivot to")
        print("    a different structural angle. Track which assumptions you have already")
        print("    challenged and avoid repeating the same framing.'")


def cmd_add(db, agent, skill_text):
    """Write a new skill for an agent."""
    db.execute(
        "INSERT INTO watcher_skills (agent, skill_type, skill_text, created_at) "
        "VALUES (?, 'rule', ?, ?)",
        (agent, skill_text, int(time.time()))
    )
    db.commit()
    skill_id = db.execute("SELECT last_insert_rowid()").fetchone()[0]
    print(f"Skill #{skill_id} added for {agent}: {skill_text[:80]}")


def cmd_list(db, agent=None):
    """Show active skills."""
    if agent:
        rows = db.execute(
            "SELECT id, agent, skill_text, usage_count, created_at "
            "FROM watcher_skills WHERE agent=? ORDER BY created_at DESC",
            (agent,)
        ).fetchall()
    else:
        rows = db.execute(
            "SELECT id, agent, skill_text, usage_count, created_at "
            "FROM watcher_skills ORDER BY agent, created_at DESC"
        ).fetchall()

    if not rows:
        print("No skills defined." + (f" (agent={agent})" if agent else ""))
        return

    for sid, ag, text, usage, created in rows:
        ts = datetime.fromtimestamp(created).strftime("%Y-%m-%d %H:%M")
        print(f"  #{sid} [{ag}] used:{usage}x  created:{ts}")
        print(f"    {text}")
        print()


def cmd_inject(db, agent):
    """Output skill block for injection into agent system prompt."""
    rows = db.execute(
        "SELECT id, skill_text FROM watcher_skills "
        "WHERE agent=? ORDER BY created_at ASC",
        (agent,)
    ).fetchall()

    if not rows:
        print(f"# No skills for {agent}")
        return

    print(f"# === LEARNED SKILLS ({agent}) ===")
    print(f"# These rules were learned from production output analysis.")
    print(f"# Follow them to improve output quality.\n")
    for sid, text in rows:
        print(f"# Skill #{sid}: {text}")
        # Mark as used
        db.execute(
            "UPDATE watcher_skills SET usage_count = usage_count + 1, "
            "last_used_at = ? WHERE id = ?",
            (int(time.time()), sid)
        )
    db.commit()


def cmd_retire(db, skill_id):
    """Retire a skill."""
    db.execute("DELETE FROM watcher_skills WHERE id=?", (int(skill_id),))
    db.commit()
    print(f"Skill #{skill_id} retired.")


def main():
    if len(sys.argv) < 2:
        print(__doc__)
        return

    db = get_db()
    action = sys.argv[1]

    if action == "stats":
        cmd_stats(db)
    elif action == "review":
        cmd_review(db)
    elif action == "add" and len(sys.argv) >= 4:
        cmd_add(db, sys.argv[2], " ".join(sys.argv[3:]))
    elif action == "list":
        cmd_list(db, sys.argv[2] if len(sys.argv) > 2 else None)
    elif action == "inject":
        if len(sys.argv) < 3:
            print("Usage: watcher_skills.py inject AGENT")
            return
        cmd_inject(db, sys.argv[2])
    elif action == "retire" and len(sys.argv) >= 3:
        cmd_retire(db, sys.argv[2])
    else:
        print(__doc__)

    db.close()


if __name__ == "__main__":
    main()
