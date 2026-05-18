#!/usr/bin/env python3
"""consolidate.py — Active memory consolidation for Chronicle.

The gap this fills: operational sessions engage captures one at a time.
Cross-connections between captures are missed because each gets its own
analysis window. This script finds those connections after the fact.

Unlike crossref (which pairs random items and forces connections),
consolidation is selective — it only surfaces connections where embedding
similarity AND LLM judgment both agree something genuine links them.

Modes:
  scan     — Find today's capture clusters via embeddings, show them
  run      — Find clusters AND ask Hermes to identify genuine connections
  store    — Find, judge, and store synthesis capsules to Chronicle
  dry-run  — Full pipeline, print what would be stored, don't store

Usage:
  python3 consolidate.py scan
  python3 consolidate.py run [--window HOURS] [--min-sim 0.55]
  python3 consolidate.py store [--window HOURS]
  python3 consolidate.py dry-run
"""

import argparse
import json
import math
import os
import sqlite3
import sys
import time
import urllib.request
from datetime import datetime, timezone, timedelta
from itertools import combinations
from pathlib import Path

PDT = timezone(timedelta(hours=-7))
DB_PATH = "/mnt/hdd/chronicle-data/processed.db"
OLLAMA_URL = "http://192.168.1.11:11434"
EMBED_MODEL = "mxbai-embed-large"
HERMES_ENV = Path.home() / ".hermes" / ".env"
NOUS_URL = "https://inference-api.nousresearch.com/v1/chat/completions"
HERMES_MODEL = "nousresearch/hermes-4-70b"
LOG_FILE = Path.home() / "chronicle" / "data" / "consolidation_log.jsonl"
MCP_BIN = os.path.expanduser("~/.local/bin/chronicle-mcp")

FEED_SOURCES = {"feed/", "sentinel", "crossref/", "capsule-sync", "prediction_monitor"}
NOISE_SOURCES = {"eye", "hal", "gemma", "discord:reaction"}
KEEP_SOURCES = {"discord:opus", "discord:nate", "discord:capture", "operator:capture"}
MIN_CONTENT_LEN = 100
DEFAULT_WINDOW_HOURS = 18
DEFAULT_MIN_SIM = 0.55
CLUSTER_MIN_SIZE = 2
MAX_CLUSTERS = 8


def now_pdt():
    return datetime.now(PDT)


def load_nous_key():
    if "NOUS_API_KEY" in os.environ:
        return os.environ["NOUS_API_KEY"]
    if HERMES_ENV.is_file():
        for line in HERMES_ENV.read_text().splitlines():
            if line.startswith("NOUS_API_KEY="):
                return line.split("=", 1)[1].strip().strip("'\"")
    chronicle_env = Path.home() / "chronicle" / "chronicle.env"
    if chronicle_env.is_file():
        for line in chronicle_env.read_text().splitlines():
            if line.startswith("NOUS_API_KEY="):
                return line.split("=", 1)[1].strip().strip("'\"")
    return None


def embed(text):
    data = json.dumps({"model": EMBED_MODEL, "input": text[:500]}).encode()
    req = urllib.request.Request(
        f"{OLLAMA_URL}/api/embed",
        data=data,
        headers={"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=30) as resp:
        result = json.loads(resp.read())
    return result["embeddings"][0]


def cosine_sim(a, b):
    dot = sum(x * y for x, y in zip(a, b))
    na = math.sqrt(sum(x * x for x in a))
    nb = math.sqrt(sum(x * x for x in b))
    if na == 0 or nb == 0:
        return 0.0
    return dot / (na * nb)


def get_substantive_capsules(window_hours=DEFAULT_WINDOW_HOURS):
    """Pull capsules that represent substantive work, not feed items."""
    db = sqlite3.connect(DB_PATH, timeout=10)
    cutoff = int(time.time()) - (window_hours * 3600)
    rows = db.execute(
        "SELECT id, source, title, content, created_at FROM activity_feed "
        "WHERE created_at > ? ORDER BY created_at ASC",
        (cutoff,),
    ).fetchall()
    db.close()

    capsules = []
    seen_text = set()
    for row_id, source, title, content, ts in rows:
        if any(source.startswith(prefix) for prefix in FEED_SOURCES):
            continue
        if source in NOISE_SOURCES:
            continue
        if KEEP_SOURCES and not any(source.startswith(k) for k in KEEP_SOURCES):
            continue
        text = (content or "").strip()
        if len(text) < MIN_CONTENT_LEN:
            continue
        # Dedup near-identical content (Gemma think echoes, opus/operator mirrors)
        # Strip channel prefix for dedup: "[Discord #opus] Opus: X" and "[Discord #operator] Chronicle: X"
        dedup_text = text
        for prefix in ("[Discord #opus] Opus: ", "[Discord #operator] Chronicle: ",
                       "[Discord #opus] nate_home: ", "[Discord #operator] nate_home: ",
                       "[Discord #capture] nate_home: "):
            if dedup_text.startswith(prefix):
                dedup_text = dedup_text[len(prefix):]
                break
        sig = dedup_text[:120]
        if sig in seen_text:
            continue
        seen_text.add(sig)
        # Skip noise patterns
        if text.startswith("[think]") or "Home scene" in text[:30]:
            continue
        if "FTSO Prediction" in text:
            continue
        if "Heartbeat" in text[:40] or "heartbeat" in text[:40]:
            continue
        if "90-min heartbeat" in text.lower():
            continue
        if "🔵 **[Hermes:" in text:
            continue
        # Operator posts are narration/echoes of opus posts — skip for consolidation
        if "[Discord #operator] Chronicle:" in text:
            continue
        if "[Discord #operator] nate_home:" in text:
            continue
        label = (title or "").strip() or text[:100].split("\n", 1)[0]
        capsules.append({
            "id": row_id,
            "source": source,
            "label": label,
            "text": text[:600],
            "ts": datetime.fromtimestamp(ts, PDT).strftime("%H:%M"),
        })
    return capsules


def find_clusters(capsules, min_sim=DEFAULT_MIN_SIM):
    """Two-phase: merge same-topic items into topics, then find cross-topic bridges.

    Phase 1: Items with sim > 0.85 are the same topic — merge into one representative.
    Phase 2: Find pairs of DIFFERENT topics with sim in the bridge zone (min_sim to 0.85).
    These are the interesting connections: similar enough to share structure, different
    enough to be genuinely distinct captures.
    """
    if len(capsules) < 2:
        return []

    print(f"Embedding {len(capsules)} capsules...", flush=True)
    embeddings = {}
    for c in capsules:
        try:
            embeddings[c["id"]] = embed(c["text"])
        except Exception as e:
            print(f"  skip {c['id']}: {e}", file=sys.stderr)

    cap_map = {c["id"]: c for c in capsules}
    ids = list(embeddings.keys())

    # Phase 1: Merge same-topic items (sim > 0.85)
    SAME_TOPIC_THRESH = 0.85
    topic_groups = []  # list of sets of IDs
    assigned = {}  # id -> group index

    for i, j in combinations(range(len(ids)), 2):
        sim = cosine_sim(embeddings[ids[i]], embeddings[ids[j]])
        if sim >= SAME_TOPIC_THRESH:
            gi = assigned.get(ids[i])
            gj = assigned.get(ids[j])
            if gi is not None and gj is not None:
                if gi != gj:
                    # Merge groups
                    topic_groups[gi].update(topic_groups[gj])
                    for mid in topic_groups[gj]:
                        assigned[mid] = gi
                    topic_groups[gj] = set()
            elif gi is not None:
                topic_groups[gi].add(ids[j])
                assigned[ids[j]] = gi
            elif gj is not None:
                topic_groups[gj].add(ids[i])
                assigned[ids[i]] = gj
            else:
                idx = len(topic_groups)
                topic_groups.append({ids[i], ids[j]})
                assigned[ids[i]] = idx
                assigned[ids[j]] = idx

    # Singletons become their own topic
    for cid in ids:
        if cid not in assigned:
            idx = len(topic_groups)
            topic_groups.append({cid})
            assigned[cid] = idx

    # Build topic representatives (longest text in group)
    topics = []
    for group in topic_groups:
        if not group:
            continue
        members = [cap_map[cid] for cid in group if cid in cap_map]
        if not members:
            continue
        rep = max(members, key=lambda m: len(m["text"]))
        avg_emb = None
        embs = [embeddings[cid] for cid in group if cid in embeddings]
        if embs:
            avg_emb = [sum(vals) / len(vals) for vals in zip(*embs)]
        topics.append({
            "rep": rep,
            "members": members,
            "embedding": avg_emb,
            "ids": group,
        })

    print(f"  {len(capsules)} capsules → {len(topics)} topics (merged same-topic items)")

    # Phase 2: Find cross-topic bridges
    bridges = []
    for i, j in combinations(range(len(topics)), 2):
        if topics[i]["embedding"] is None or topics[j]["embedding"] is None:
            continue
        sim = cosine_sim(topics[i]["embedding"], topics[j]["embedding"])
        if min_sim <= sim < SAME_TOPIC_THRESH:
            bridges.append((i, j, sim))

    bridges.sort(key=lambda x: -x[2])

    clusters = []
    for ti, tj, sim in bridges[:MAX_CLUSTERS]:
        clusters.append({
            "members": [topics[ti]["rep"], topics[tj]["rep"]],
            "all_members": topics[ti]["members"] + topics[tj]["members"],
            "sims": [sim],
            "avg_sim": sim,
        })

    return clusters


def judge_connection(cluster):
    """Ask Hermes whether a cluster has a genuine connection."""
    key = load_nous_key()
    if not key:
        return None

    member_texts = []
    for i, m in enumerate(cluster["members"]):
        member_texts.append(f"[{i+1}] ({m['ts']}) {m['text'][:400]}")

    prompt = (
        "Below are captures from the same day that have embedding similarity. "
        "Your job: determine if a structural principle transfers between them — "
        "a shared mechanism, pattern, or constraint that operates in both domains.\n\n"
        + "\n\n".join(member_texts)
        + "\n\nA genuine connection means the SAME structural principle appears in "
        "DIFFERENT domains. Examples of genuine connections:\n"
        "- 'Both are cases where measurement tools simpler than the system miss "
        "load-bearing dynamics' (nonlinear brain analysis + force-free molecular dynamics)\n"
        "- 'Both show that communication pressure shapes structure more than ancestry' "
        "(marmoset vocal tracts + language evolution)\n"
        "- 'Both reveal hidden capacity that normal operating conditions mask' "
        "(neuronal fat reserves + impossible color perception)\n\n"
        "NOT genuine: same topic discussed twice, capture and its own analysis, "
        "shared vocabulary without shared mechanism, debate between opposing views "
        "on the same question.\n\n"
        "If there IS a transferable principle:\n"
        "- State it in one sentence\n"
        "- Name what transfers: the mechanism, constraint, or pattern\n"
        "- Rate confidence: high/medium/low\n\n"
        "If no principle transfers, say 'No structural transfer' in one sentence.\n"
        "Be concise — 3-5 sentences max either way."
    )

    payload = {
        "model": HERMES_MODEL,
        "messages": [
            {"role": "system", "content": "You are a research analyst. Be selective and skeptical."},
            {"role": "user", "content": prompt},
        ],
        "max_tokens": 600,
        "temperature": 0.3,
    }

    req = urllib.request.Request(
        NOUS_URL,
        data=json.dumps(payload).encode(),
        headers={
            "Authorization": f"Bearer {key}",
            "Content-Type": "application/json",
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            data = json.loads(resp.read())
        return data["choices"][0]["message"]["content"]
    except Exception as e:
        print(f"Hermes judge failed: {e}", file=sys.stderr)
        return None


def store_synthesis(connection_text, cluster):
    """Store a synthesis capsule via Chronicle MCP."""
    member_labels = [m["label"][:80] for m in cluster["members"]]
    content = (
        f"Consolidation synthesis ({now_pdt().strftime('%Y-%m-%d')}): {connection_text}\n"
        f"Source captures: {'; '.join(member_labels)}"
    )

    jsonrpc = (
        '{"jsonrpc":"2.0","method":"initialize","params":{"protocolVersion":"2024-11-05",'
        '"capabilities":{},"clientInfo":{"name":"consolidate","version":"1.0"}},"id":1}\n'
        '{"jsonrpc":"2.0","method":"tools/call","params":{"name":"store_memory","arguments":'
        + json.dumps({
            "content": content[:1500],
            "topic": "chronicle/consolidation",
            "keywords": ["consolidation", "synthesis", "cross-capture"],
            "memory_type": "synthesis",
        })
        + '},"id":2}\n'
    )

    import subprocess
    result = subprocess.run(
        [MCP_BIN],
        input=jsonrpc,
        capture_output=True,
        text=True,
        timeout=60,
    )
    return "store_memory" in result.stdout and "success" in result.stdout


def log_run(clusters, judgments):
    LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
    entry = {
        "ts": datetime.now(PDT).isoformat(),
        "clusters_found": len(clusters),
        "judgments": len(judgments),
        "genuine": sum(1 for j in judgments if j and "no genuine" not in j.lower() and "no structural transfer" not in j.lower()),
    }
    with open(LOG_FILE, "a") as f:
        f.write(json.dumps(entry) + "\n")


def cmd_scan(args):
    capsules = get_substantive_capsules(args.window)
    print(f"Found {len(capsules)} substantive capsules in last {args.window}h\n")

    clusters = find_clusters(capsules, args.min_sim)
    if not clusters:
        print("No clusters found above similarity threshold.")
        return

    for i, cluster in enumerate(clusters):
        print(f"\n--- Cluster {i+1} (avg sim: {cluster['avg_sim']:.3f}) ---")
        for m in cluster["members"]:
            print(f"  [{m['ts']}] {m['label'][:80]}")


def cmd_run(args):
    capsules = get_substantive_capsules(args.window)
    print(f"Found {len(capsules)} substantive capsules in last {args.window}h")

    clusters = find_clusters(capsules, args.min_sim)
    if not clusters:
        print("No clusters found.")
        return

    judgments = []
    for i, cluster in enumerate(clusters):
        print(f"\n--- Cluster {i+1} (avg sim: {cluster['avg_sim']:.3f}) ---")
        for m in cluster["members"]:
            print(f"  [{m['ts']}] {m['label'][:80]}")

        print("\nJudging connection...")
        judgment = judge_connection(cluster)
        judgments.append(judgment)
        if judgment:
            print(f"\n{judgment}")
        else:
            print("  (judgment failed)")

    log_run(clusters, judgments)


def cmd_store(args):
    capsules = get_substantive_capsules(args.window)
    clusters = find_clusters(capsules, args.min_sim)
    if not clusters:
        print("No clusters to consolidate.")
        return

    stored = 0
    for cluster in clusters:
        judgment = judge_connection(cluster)
        jl = (judgment or "").lower()
        if not judgment or "no genuine" in jl or "no structural transfer" in jl:
            continue

        if args.dry_run:
            print(f"\n[DRY RUN] Would store:")
            print(f"  Connection: {judgment[:200]}")
            for m in cluster["members"]:
                print(f"  Source: {m['label'][:60]}")
        else:
            ok = store_synthesis(judgment, cluster)
            if ok:
                stored += 1
                print(f"Stored synthesis from {len(cluster['members'])} captures")

    print(f"\n{'Would store' if args.dry_run else 'Stored'} {stored} syntheses")


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("mode", choices=["scan", "run", "store", "dry-run"],
                    help="Operation mode")
    ap.add_argument("--window", type=int, default=DEFAULT_WINDOW_HOURS,
                    help=f"Lookback window in hours (default: {DEFAULT_WINDOW_HOURS})")
    ap.add_argument("--min-sim", type=float, default=DEFAULT_MIN_SIM,
                    help=f"Minimum cosine similarity for clustering (default: {DEFAULT_MIN_SIM})")
    args = ap.parse_args()
    args.dry_run = args.mode == "dry-run"

    if args.mode == "scan":
        cmd_scan(args)
    elif args.mode in ("run", "dry-run"):
        cmd_run(args) if args.mode == "run" else cmd_store(args)
    elif args.mode == "store":
        cmd_store(args)


if __name__ == "__main__":
    main()
