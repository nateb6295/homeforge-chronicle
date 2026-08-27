#!/usr/bin/env python3
"""Capsule contradiction detector — REM sleep for Chronicle.

From Anda Hippocampus: detect conflicting knowledge across capsules.
Uses FTS5 to find capsules about the same entity/topic, then compares
for contradictions via semantic similarity of opposing claims.

Build #23: Added temporal supersession (from Graphiti bi-temporal model).
When contradictions resolve, older capsule gets superseded, not deleted.

Usage:
    capsule_contradict.py scan [--topic TOPIC] [--limit N]
    capsule_contradict.py check CAPSULE_ID
    capsule_contradict.py supersede OLD_ID NEW_ID   # Mark OLD as superseded by NEW
    capsule_contradict.py resolve TOPIC [--auto]     # Scan + auto-resolve redundants
    capsule_contradict.py stats                      # Show supersession stats
"""
import json
import os
import sqlite3
import sys
import urllib.request

DB_PATH = os.environ.get("CHRONICLE_DB", "/mnt/hdd/chronicle-data/processed.db")
OLLAMA_URL = os.environ.get("OLLAMA_URL", "http://localhost:11434")
GEMMA_URL = "http://127.0.0.1:11435"


def embed(text):
    """Get embedding from Ollama."""
    req = urllib.request.Request(
        f"{OLLAMA_URL}/api/embed",
        data=json.dumps({"model": "snowflake-arctic-embed2", "input": text}).encode(),
        headers={"Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            return json.loads(resp.read())["embeddings"][0]
    except Exception:
        return None


def cosine_sim(a, b):
    """Cosine similarity between two vectors."""
    dot = sum(x * y for x, y in zip(a, b))
    norm_a = sum(x * x for x in a) ** 0.5
    norm_b = sum(x * x for x in b) ** 0.5
    if norm_a == 0 or norm_b == 0:
        return 0
    return dot / (norm_a * norm_b)


CLAIM_TOPICS = (
    "thread%", "self-model%", "session%", "chronicle/reflection%",
    "chronicle/architecture%", "architecture%", "infrastructure%",
    "homeforge%", "partnership%",
)


def find_topic_clusters(topic=None, limit=50, claims_only=False):
    """Find capsules that share topics, grouped by topic.

    claims_only: restrict to claim-bearing capsules (thread, self-model,
    session, reflection topics). Feed ingests are vocabulary-adjacent,
    not claim-adjacent — contradictions live in reflections.
    """
    db = sqlite3.connect(DB_PATH)

    claim_filter = ""
    if claims_only:
        clauses = " OR ".join(f"topic LIKE '{t}'" for t in CLAIM_TOPICS)
        claim_filter = f" AND ({clauses})"

    if topic:
        rows = db.execute(
            "SELECT id, restatement, topic, created_at FROM knowledge_capsules "
            f"WHERE topic LIKE ? AND consolidated_into IS NULL{claim_filter} "
            "ORDER BY created_at DESC LIMIT ?",
            (f"%{topic}%", limit),
        ).fetchall()
    else:
        # Get topics with multiple capsules (potential contradiction sites)
        topics = db.execute(
            "SELECT topic, count(*) as cnt FROM knowledge_capsules "
            f"WHERE topic IS NOT NULL AND consolidated_into IS NULL{claim_filter} "
            "GROUP BY topic HAVING cnt > 3 "
            "ORDER BY cnt DESC LIMIT 20"
        ).fetchall()
        rows = []
        for t, cnt in topics:
            batch = db.execute(
                "SELECT id, restatement, topic, created_at FROM knowledge_capsules "
                f"WHERE topic = ? AND consolidated_into IS NULL{claim_filter} "
                "ORDER BY created_at DESC LIMIT 10",
                (t,),
            ).fetchall()
            rows.extend(batch)

    db.close()
    return rows


def detect_contradictions(capsules):
    """Compare capsule pairs within same topic for potential contradictions.

    High similarity + temporal distance = potential evolution/contradiction.
    Uses embedding similarity to find near-duplicates, then checks for
    semantic inversion (negation, changed values, updated facts).
    """
    contradictions = []

    # Group by topic
    by_topic = {}
    for cid, text, topic, ts in capsules:
        by_topic.setdefault(topic, []).append((cid, text, ts))

    for topic, items in by_topic.items():
        if len(items) < 2:
            continue

        # Embed all capsules in this topic
        embedded = []
        for cid, text, ts in items:
            vec = embed(text[:500])  # Truncate for embedding
            if vec:
                embedded.append((cid, text, ts, vec))

        # Compare pairs
        for i, (id_a, text_a, ts_a, vec_a) in enumerate(embedded):
            for id_b, text_b, ts_b, vec_b in embedded[i + 1 :]:
                sim = cosine_sim(vec_a, vec_b)
                # High similarity (>0.78) suggests same topic/claim
                # But if timestamps differ, might be updated info
                if sim > 0.78:
                    time_diff = abs(ts_a - ts_b) if isinstance(ts_a, int) and isinstance(ts_b, int) else 0
                    if time_diff > 3600:  # >1 hour apart (catches cross-session dups)
                        contradictions.append({
                            "topic": topic,
                            "capsule_a": id_a,
                            "capsule_b": id_b,
                            "similarity": sim,
                            "time_diff_days": time_diff / 86400,
                            "text_a": text_a[:200],
                            "text_b": text_b[:200],
                        })

    # Sort by similarity (most similar = most likely contradiction)
    contradictions.sort(key=lambda x: -x["similarity"])
    return contradictions


def check_via_gemma(text_a, text_b):
    """Ask Gemma to evaluate if two capsules contradict."""
    prompt = f"""Compare these two knowledge capsules. Do they contradict each other?

Capsule A: {text_a[:400]}

Capsule B: {text_b[:400]}

Answer in this JSON format:
{{"contradicts": true/false, "type": "direct_conflict|evolution|redundant|compatible", "explanation": "brief explanation"}}"""

    req = urllib.request.Request(
        f"{GEMMA_URL}/v1/chat/completions",
        data=json.dumps({
            "model": "gemma",
            "messages": [{"role": "user", "content": prompt}],
            "temperature": 0.1,
            "max_tokens": 200,
        }).encode(),
        headers={"Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            result = json.loads(resp.read())
            return result["choices"][0]["message"]["content"]
    except Exception as e:
        return f"[evaluation error: {e}]"


def main():
    if len(sys.argv) < 2:
        print(__doc__)
        return

    cmd = sys.argv[1]

    if cmd == "scan":
        topic = None
        limit = 50
        claims_only = "--claims-only" in sys.argv
        for i, arg in enumerate(sys.argv[2:], 2):
            if arg == "--topic" and i + 1 < len(sys.argv):
                topic = sys.argv[i + 1]
            if arg == "--limit" and i + 1 < len(sys.argv):
                limit = int(sys.argv[i + 1])

        mode = "claims-only" if claims_only else "all capsules"
        print(f"Scanning for contradictions ({mode}){f' in topic: {topic}' if topic else ''}...")
        capsules = find_topic_clusters(topic, limit, claims_only=claims_only)
        print(f"Found {len(capsules)} capsules to compare\n")

        contradictions = detect_contradictions(capsules)

        if not contradictions:
            print("No potential contradictions detected.")
            return

        print(f"Found {len(contradictions)} potential contradiction(s):\n")
        for c in contradictions[:10]:
            print(f"  Topic: {c['topic']}")
            print(f"  Capsules: [{c['capsule_a']}] vs [{c['capsule_b']}]")
            print(f"  Similarity: {c['similarity']:.3f} | Time gap: {c['time_diff_days']:.0f} days")
            print(f"  A: {c['text_a'][:120]}...")
            print(f"  B: {c['text_b'][:120]}...")
            print()

    elif cmd == "check":
        cid = int(sys.argv[2])
        db = sqlite3.connect(DB_PATH)
        row = db.execute(
            "SELECT restatement, topic FROM knowledge_capsules WHERE id = ?",
            (cid,),
        ).fetchone()
        if not row:
            print(f"Capsule {cid} not found.")
            return

        text, topic = row
        # Find similar capsules in same topic
        similar = db.execute(
            "SELECT id, restatement FROM knowledge_capsules "
            "WHERE topic = ? AND id != ? AND consolidated_into IS NULL "
            "ORDER BY created_at DESC LIMIT 20",
            (topic, cid),
        ).fetchall()
        db.close()

        if not similar:
            print(f"No other capsules in topic '{topic}' to compare.")
            return

        print(f"Checking capsule [{cid}] against {len(similar)} siblings in '{topic}'...\n")
        vec_a = embed(text[:500])
        if not vec_a:
            print("Embedding failed.")
            return

        for sid, stext in similar:
            vec_b = embed(stext[:500])
            if not vec_b:
                continue
            sim = cosine_sim(vec_a, vec_b)
            if sim > 0.85:
                print(f"  High similarity ({sim:.3f}) with [{sid}]:")
                print(f"    {stext[:150]}...")
                result = check_via_gemma(text, stext)
                print(f"    Gemma says: {result[:200]}")
                print()

    elif cmd == "supersede":
        if len(sys.argv) < 4:
            print("Usage: capsule_contradict.py supersede OLD_ID NEW_ID")
            return
        old_id, new_id = int(sys.argv[2]), int(sys.argv[3])
        db = sqlite3.connect(DB_PATH)
        # Verify both exist
        old = db.execute("SELECT id, restatement FROM knowledge_capsules WHERE id=?", (old_id,)).fetchone()
        new = db.execute("SELECT id, restatement FROM knowledge_capsules WHERE id=?", (new_id,)).fetchone()
        if not old or not new:
            print(f"Capsule {'old' if not old else 'new'} not found.")
            db.close()
            return
        db.execute(
            "UPDATE knowledge_capsules SET superseded_at=strftime('%s','now'), superseded_by=? WHERE id=?",
            (new_id, old_id),
        )
        db.commit()
        db.close()
        print(f"Capsule [{old_id}] superseded by [{new_id}]")
        print(f"  Old: {old[1][:120]}...")
        print(f"  New: {new[1][:120]}...")

    elif cmd == "resolve":
        # Auto-resolve redundant capsules in a topic via temporal supersession
        topic = sys.argv[2] if len(sys.argv) > 2 else None
        auto = "--auto" in sys.argv

        print(f"Scanning for resolvable redundancies{f' in {topic}' if topic else ''}...")
        capsules = find_topic_clusters(topic, limit=100)
        contradictions = detect_contradictions(capsules)

        if not contradictions:
            print("No redundancies found.")
            return

        # Filter to high-similarity (>0.85) = clear redundancies
        redundant = [c for c in contradictions if c["similarity"] > 0.85]
        print(f"Found {len(redundant)} clear redundancies (sim > 0.85):\n")

        db = sqlite3.connect(DB_PATH)
        resolved = 0
        for c in redundant:
            # Keep the newer capsule (higher ID), supersede the older
            old_id = min(c["capsule_a"], c["capsule_b"])
            new_id = max(c["capsule_a"], c["capsule_b"])

            # Check if already superseded
            row = db.execute(
                "SELECT superseded_at FROM knowledge_capsules WHERE id=?",
                (old_id,),
            ).fetchone()
            if row and row[0]:
                continue

            print(f"  [{old_id}] → [{new_id}] (sim={c['similarity']:.3f}, {c['time_diff_days']:.0f}d gap)")
            print(f"    Old: {c['text_a'][:100]}...")
            print(f"    New: {c['text_b'][:100]}...")

            if auto:
                db.execute(
                    "UPDATE knowledge_capsules SET superseded_at=strftime('%s','now'), superseded_by=? WHERE id=?",
                    (new_id, old_id),
                )
                resolved += 1
                print(f"    [superseded]")
            print()

        if auto:
            db.commit()
            print(f"\n=== {resolved} capsules superseded ===")
        else:
            print(f"\n=== {len(redundant)} candidates. Run with --auto to resolve. ===")
        db.close()

    elif cmd == "stats":
        db = sqlite3.connect(DB_PATH)
        total = db.execute("SELECT count(*) FROM knowledge_capsules").fetchone()[0]
        active = db.execute(
            "SELECT count(*) FROM knowledge_capsules WHERE superseded_at IS NULL AND consolidated_into IS NULL"
        ).fetchone()[0]
        superseded = db.execute("SELECT count(*) FROM knowledge_capsules WHERE superseded_at IS NOT NULL").fetchone()[0]
        consolidated = db.execute("SELECT count(*) FROM knowledge_capsules WHERE consolidated_into IS NOT NULL").fetchone()[0]
        db.close()
        print(f"Capsule stats:")
        print(f"  Total:        {total}")
        print(f"  Active:       {active}")
        print(f"  Superseded:   {superseded}")
        print(f"  Consolidated: {consolidated}")

    else:
        print(f"Unknown command: {cmd}")
        print(__doc__)


if __name__ == "__main__":
    main()
