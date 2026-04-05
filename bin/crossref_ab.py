#!/usr/bin/env python3
"""
Crossref A/B Test — Slow-channel evaluation for chronicle-specialist.

Watches for new crossref connections, generates a parallel description
using the chronicle-specialist model (fine-tuned), and stores both
for quality comparison. Does NOT modify the main crossref pipeline.

This implements Thread #235's slow-channel eval recommendation:
the auto-eval scores the minimum bar (fast channel); this compares
real production output over weeks (slow channel).

Runs as a background service after chronicle-specialist is deployed.
Only active when the specialist model exists in Ollama.

Usage:
  python3 crossref_ab.py           # Run comparison loop
  python3 crossref_ab.py results   # Show comparison stats
"""

import json
import os
import sqlite3
import sys
import time
import urllib.request
from datetime import datetime

DB_PATH = os.environ.get("CHRONICLE_DB",
    os.path.expanduser("~/.homeforge-chronicle/processed.db"))
OLLAMA_URL = os.environ.get("CHRONICLE_OLLAMA_URL", "http://localhost:11435")
SPECIALIST_MODEL = "chronicle-specialist"
CYCLE_INTERVAL = 1800  # Check every 30 minutes
SAMPLE_RATE = 1.0  # Compare 100% of connections (small volume, ~4/day)


def log(msg):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] {msg}", flush=True)


def get_db():
    conn = sqlite3.connect(DB_PATH, timeout=10)
    conn.row_factory = sqlite3.Row
    return conn


def init_db(conn):
    """Create comparison table if needed."""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS crossref_ab_comparison (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            crossref_id INTEGER NOT NULL,
            original_text TEXT NOT NULL,
            specialist_text TEXT,
            specialist_error TEXT,
            original_model TEXT,
            specialist_model TEXT DEFAULT 'chronicle-specialist',
            created_at INTEGER NOT NULL,
            UNIQUE(crossref_id)
        )
    """)
    # Add scoring columns if not present
    for col, ctype in [
        ("judge_winner", "TEXT"),       # 'original', 'specialist', 'tie'
        ("judge_reasoning", "TEXT"),
        ("judge_specificity_a", "INTEGER"),   # 1-5
        ("judge_specificity_b", "INTEGER"),
        ("judge_mechanism_a", "INTEGER"),     # 1-5
        ("judge_mechanism_b", "INTEGER"),
        ("judged_at", "INTEGER"),
    ]:
        try:
            conn.execute(f"ALTER TABLE crossref_ab_comparison ADD COLUMN {col} {ctype}")
        except Exception:
            pass
    conn.commit()


def specialist_available():
    """Check if chronicle-specialist exists in Ollama."""
    try:
        req = urllib.request.Request(f"{OLLAMA_URL}/api/tags")
        with urllib.request.urlopen(req, timeout=5) as resp:
            data = json.loads(resp.read())
            names = [m["name"] for m in data.get("models", [])]
            return any(SPECIALIST_MODEL in n for n in names)
    except Exception:
        return False


def _extract_title(content):
    """Extract article title from brief content (mirrors crossref.py logic)."""
    import re
    text = re.sub(r'^\[capsule:\d+\]\s*', '', content)
    text = re.sub(r'^Research brief:\s*', '', text)
    for sep in [' — ', ' - ', '\n']:
        idx = text.find(sep)
        if 10 < idx < 200:
            text = text[:idx]
            break
    return text[:200].strip()


def get_capsule(conn, capsule_id):
    """Load a seed observation (intern brief) used by crossref."""
    row = conn.execute(
        "SELECT id, content FROM seed_observations WHERE id = ?",
        (capsule_id,)
    ).fetchone()
    if not row:
        return None
    content = row["content"]
    return {"id": row["id"], "title": _extract_title(content), "content": content}


def get_new_connections(conn, limit=10):
    """Find crossref connections not yet compared."""
    rows = conn.execute(
        "SELECT c.id, c.brief_a_id, c.brief_b_id, c.connection_text, c.similarity "
        "FROM crossref_connections c "
        "LEFT JOIN crossref_ab_comparison ab ON ab.crossref_id = c.id "
        "WHERE ab.id IS NULL "
        "ORDER BY c.created_at DESC LIMIT ?",
        (limit,)
    ).fetchall()
    return [dict(r) for r in rows]


def generate_specialist_description(capsule_a, capsule_b):
    """Generate a connection description using the specialist model."""
    system_prompt = (
        "You are a Chronicle crossref analyst. Find unexpected structural "
        "connections between concepts. Be specific about the mechanism — "
        "name the structural pattern, not just the surface similarity."
    )
    user_prompt = (
        f"ARTICLE A:\n{capsule_a.get('title', 'Untitled')}\n"
        f"{capsule_a.get('content', '')[:600]}\n\n"
        f"ARTICLE B:\n{capsule_b.get('title', 'Untitled')}\n"
        f"{capsule_b.get('content', '')[:600]}\n\n"
        f"What specific structural connection do you see between these? "
        f"Name the mechanism. Be concise."
    )

    try:
        data = json.dumps({
            "model": SPECIALIST_MODEL,
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            "stream": False,
            "options": {"num_predict": 512, "temperature": 0.7},
        }).encode()

        req = urllib.request.Request(
            f"{OLLAMA_URL}/api/chat",
            data=data,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=120) as resp:
            result = json.loads(resp.read())
            text = result.get("message", {}).get("content", "").strip()
            if text:
                return text, None
            return None, "empty response"
    except Exception as e:
        return None, str(e)


def run_comparisons():
    """Main comparison loop."""
    log("Crossref A/B test starting")
    log(f"  DB: {DB_PATH}")
    log(f"  Ollama: {OLLAMA_URL}")
    log(f"  Model: {SPECIALIST_MODEL}")

    conn = get_db()
    init_db(conn)

    # Check if specialist is available
    if not specialist_available():
        log(f"  {SPECIALIST_MODEL} not available in Ollama. Waiting...")
        conn.close()
        return False

    log(f"  {SPECIALIST_MODEL} found. Processing new connections...")

    # Get uncompared connections
    new_conns = get_new_connections(conn, limit=20)
    if not new_conns:
        log("  No new connections to compare.")
        conn.close()
        return True

    log(f"  {len(new_conns)} connections to compare")

    for i, cx in enumerate(new_conns, 1):
        # Load source capsules (intern briefs stored as capsules)
        cap_a = get_capsule(conn, cx["brief_a_id"])
        cap_b = get_capsule(conn, cx["brief_b_id"])

        if not cap_a or not cap_b:
            # Store as error — capsule might be from intern briefs not in capsules table
            conn.execute(
                "INSERT OR IGNORE INTO crossref_ab_comparison "
                "(crossref_id, original_text, specialist_error, created_at) "
                "VALUES (?, ?, ?, ?)",
                (cx["id"], cx["connection_text"], "capsule not found", int(time.time()))
            )
            conn.commit()
            continue

        log(f"  [{i}/{len(new_conns)}] Comparing cx#{cx['id']}...")

        specialist_text, error = generate_specialist_description(cap_a, cap_b)

        conn.execute(
            "INSERT OR IGNORE INTO crossref_ab_comparison "
            "(crossref_id, original_text, specialist_text, specialist_error, "
            "original_model, specialist_model, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            (cx["id"], cx["connection_text"], specialist_text, error,
             "chronicle-challenger", SPECIALIST_MODEL, int(time.time()))
        )
        conn.commit()

        if specialist_text:
            log(f"    Original:   {cx['connection_text'][:100]}...")
            log(f"    Specialist: {specialist_text[:100]}...")

            # Auto-judge this pair
            groq_key = get_groq_key()
            if groq_key:
                try:
                    verdict = judge_pair(cx["connection_text"], specialist_text, groq_key)
                    conn.execute(
                        "UPDATE crossref_ab_comparison SET "
                        "judge_winner=?, judge_reasoning=?, "
                        "judge_specificity_a=?, judge_specificity_b=?, "
                        "judge_mechanism_a=?, judge_mechanism_b=?, judged_at=? "
                        "WHERE crossref_id=?",
                        (verdict["winner"], verdict["reasoning"],
                         verdict["specificity_original"], verdict["specificity_specialist"],
                         verdict["mechanism_original"], verdict["mechanism_specialist"],
                         int(time.time()), cx["id"])
                    )
                    conn.commit()
                    log(f"    Judge: {verdict['winner']} "
                        f"(O:{verdict['specificity_original']}/{verdict['mechanism_original']} "
                        f"S:{verdict['specificity_specialist']}/{verdict['mechanism_specialist']})")
                except Exception as e:
                    log(f"    Judge error: {e}")
        else:
            log(f"    Error: {error}")

        # Brief pause between requests
        time.sleep(2)

    conn.close()
    log("  Comparison cycle complete.")
    return True


def show_results():
    """Display comparison statistics."""
    conn = get_db()
    init_db(conn)

    total = conn.execute("SELECT count(*) FROM crossref_ab_comparison").fetchone()[0]
    success = conn.execute(
        "SELECT count(*) FROM crossref_ab_comparison WHERE specialist_text IS NOT NULL"
    ).fetchone()[0]
    errors = conn.execute(
        "SELECT count(*) FROM crossref_ab_comparison WHERE specialist_error IS NOT NULL"
    ).fetchone()[0]

    print(f"=== Crossref A/B Comparison Results ===\n")
    print(f"  Total comparisons: {total}")
    print(f"  Successful: {success}")
    print(f"  Errors: {errors}")

    if success > 0:
        # Show recent comparisons
        rows = conn.execute(
            "SELECT crossref_id, "
            "substr(original_text, 1, 200) as orig, "
            "substr(specialist_text, 1, 200) as spec, "
            "datetime(created_at, 'unixepoch', 'localtime') as ts "
            "FROM crossref_ab_comparison "
            "WHERE specialist_text IS NOT NULL "
            "ORDER BY created_at DESC LIMIT 5"
        ).fetchall()

        print(f"\n  Recent comparisons:")
        for r in rows:
            print(f"\n  [{r['ts']}] cx#{r['crossref_id']}")
            print(f"    Original:   {r['orig']}")
            print(f"    Specialist: {r['spec']}")

    conn.close()


def get_groq_key():
    """Load Groq API key from chronicle.env."""
    env_file = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "chronicle.env")
    if os.path.exists(env_file):
        with open(env_file) as f:
            for line in f:
                if line.startswith("GROQ_API_KEY="):
                    return line.strip().split("=", 1)[1].strip('"').strip("'")
    return os.environ.get("GROQ_API_KEY")


def judge_pair(original, specialist, groq_key):
    """Use Groq Llama 3.3 70B to judge which crossref description is better."""
    # Randomize presentation order to avoid position bias
    import random
    if random.random() < 0.5:
        desc_a, desc_b = original, specialist
        a_is = "original"
    else:
        desc_a, desc_b = specialist, original
        a_is = "specialist"

    prompt = (
        "You are evaluating two crossref descriptions that identify structural "
        "connections between articles. Rate each on two criteria (1-5 scale):\n\n"
        "1. SPECIFICITY: Does it name the concrete mechanism, or just assert similarity?\n"
        "   1=vague assertion, 3=names a pattern, 5=identifies precise structural mechanism\n\n"
        "2. MECHANISM: Does it explain HOW the connection works, not just THAT it exists?\n"
        "   1=just states similarity, 3=describes transfer, 5=explains causal/structural link\n\n"
        "DESCRIPTION A:\n" + desc_a[:600] + "\n\n"
        "DESCRIPTION B:\n" + desc_b[:600] + "\n\n"
        "Respond in EXACTLY this JSON format, nothing else:\n"
        '{"specificity_a": N, "specificity_b": N, "mechanism_a": N, "mechanism_b": N, '
        '"winner": "A"|"B"|"tie", "reasoning": "one sentence"}'
    )

    data = json.dumps({
        "model": "llama-3.3-70b-versatile",
        "messages": [{"role": "user", "content": prompt}],
        "temperature": 0.1,
        "max_tokens": 200,
    }).encode()

    req = urllib.request.Request(
        "https://api.groq.com/openai/v1/chat/completions",
        data=data,
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {groq_key}",
            "User-Agent": "chronicle-ab/1.0",
        },
        method="POST",
    )

    with urllib.request.urlopen(req, timeout=30) as resp:
        result = json.loads(resp.read())
        text = result["choices"][0]["message"]["content"].strip()

    # Parse JSON from response
    # Handle potential markdown wrapping
    if text.startswith("```"):
        text = text.split("\n", 1)[1].rsplit("```", 1)[0].strip()
    verdict = json.loads(text)

    # Map back from randomized order
    if a_is == "original":
        return {
            "specificity_original": verdict["specificity_a"],
            "specificity_specialist": verdict["specificity_b"],
            "mechanism_original": verdict["mechanism_a"],
            "mechanism_specialist": verdict["mechanism_b"],
            "winner": {"A": "original", "B": "specialist", "tie": "tie"}[verdict["winner"]],
            "reasoning": verdict["reasoning"],
        }
    else:
        return {
            "specificity_original": verdict["specificity_b"],
            "specificity_specialist": verdict["specificity_a"],
            "mechanism_original": verdict["mechanism_b"],
            "mechanism_specialist": verdict["mechanism_a"],
            "winner": {"A": "specialist", "B": "original", "tie": "tie"}[verdict["winner"]],
            "reasoning": verdict["reasoning"],
        }


def run_judge():
    """Score all unjudged comparisons."""
    groq_key = get_groq_key()
    if not groq_key:
        print("ERROR: No GROQ_API_KEY found")
        return

    conn = get_db()
    init_db(conn)

    rows = conn.execute(
        "SELECT id, crossref_id, original_text, specialist_text "
        "FROM crossref_ab_comparison "
        "WHERE specialist_text IS NOT NULL AND judged_at IS NULL"
    ).fetchall()

    if not rows:
        print("No unjudged comparisons.")
        conn.close()
        return

    print(f"Judging {len(rows)} comparisons...\n")

    wins = {"original": 0, "specialist": 0, "tie": 0}
    for i, row in enumerate(rows, 1):
        try:
            verdict = judge_pair(row["original_text"], row["specialist_text"], groq_key)
            conn.execute(
                "UPDATE crossref_ab_comparison SET "
                "judge_winner=?, judge_reasoning=?, "
                "judge_specificity_a=?, judge_specificity_b=?, "
                "judge_mechanism_a=?, judge_mechanism_b=?, judged_at=? "
                "WHERE id=?",
                (verdict["winner"], verdict["reasoning"],
                 verdict["specificity_original"], verdict["specificity_specialist"],
                 verdict["mechanism_original"], verdict["mechanism_specialist"],
                 int(time.time()), row["id"])
            )
            conn.commit()
            wins[verdict["winner"]] += 1
            print(f"  [{i}/{len(rows)}] cx#{row['crossref_id']}: {verdict['winner']} "
                  f"(O:{verdict['specificity_original']}/{verdict['mechanism_original']} "
                  f"S:{verdict['specificity_specialist']}/{verdict['mechanism_specialist']}) "
                  f"— {verdict['reasoning'][:80]}")
            time.sleep(0.5)  # Rate limit
        except Exception as e:
            print(f"  [{i}/{len(rows)}] cx#{row['crossref_id']}: ERROR — {e}")

    print(f"\nResults: specialist={wins['specialist']}, original={wins['original']}, tie={wins['tie']}")
    conn.close()


def show_scores():
    """Show aggregate scoring statistics."""
    conn = get_db()
    init_db(conn)

    total = conn.execute(
        "SELECT count(*) FROM crossref_ab_comparison WHERE judged_at IS NOT NULL"
    ).fetchone()[0]

    if total == 0:
        print("No scored comparisons yet. Run: crossref_ab.py judge")
        conn.close()
        return

    wins = conn.execute(
        "SELECT judge_winner, count(*) as cnt FROM crossref_ab_comparison "
        "WHERE judged_at IS NOT NULL GROUP BY judge_winner"
    ).fetchall()
    win_map = {r["judge_winner"]: r["cnt"] for r in wins}

    avg = conn.execute(
        "SELECT "
        "avg(judge_specificity_a) as spec_o, avg(judge_specificity_b) as spec_s, "
        "avg(judge_mechanism_a) as mech_o, avg(judge_mechanism_b) as mech_s "
        "FROM crossref_ab_comparison WHERE judged_at IS NOT NULL"
    ).fetchone()

    print(f"=== Crossref A/B Scores ({total} judged) ===\n")
    print(f"  Wins:  specialist={win_map.get('specialist',0)}  "
          f"original={win_map.get('original',0)}  tie={win_map.get('tie',0)}")
    s_pct = win_map.get('specialist', 0) / total * 100
    o_pct = win_map.get('original', 0) / total * 100
    print(f"  Win%:  specialist={s_pct:.0f}%  original={o_pct:.0f}%\n")
    print(f"  Avg specificity:  original={avg['spec_o']:.1f}/5  specialist={avg['spec_s']:.1f}/5")
    print(f"  Avg mechanism:    original={avg['mech_o']:.1f}/5  specialist={avg['mech_s']:.1f}/5")

    # Show recent judgments
    recent = conn.execute(
        "SELECT crossref_id, judge_winner, judge_reasoning, "
        "judge_specificity_a as sa, judge_specificity_b as sb, "
        "judge_mechanism_a as ma, judge_mechanism_b as mb "
        "FROM crossref_ab_comparison WHERE judged_at IS NOT NULL "
        "ORDER BY judged_at DESC LIMIT 5"
    ).fetchall()
    print(f"\n  Recent judgments:")
    for r in recent:
        print(f"    cx#{r['crossref_id']}: {r['judge_winner']} "
              f"(O:{r['sa']}/{r['ma']} S:{r['sb']}/{r['mb']}) "
              f"— {r['judge_reasoning'][:80]}")

    conn.close()


def main():
    if len(sys.argv) > 1:
        cmd = sys.argv[1]
        if cmd == "results":
            show_results()
        elif cmd == "judge":
            run_judge()
        elif cmd == "scores":
            show_scores()
        else:
            print(f"Unknown command: {cmd}")
            print("Usage: crossref_ab.py [results|judge|scores]")
        return

    while True:
        available = run_comparisons()
        if not available:
            # Model not yet deployed — check less frequently
            time.sleep(300)
        else:
            time.sleep(CYCLE_INTERVAL)


if __name__ == "__main__":
    main()
